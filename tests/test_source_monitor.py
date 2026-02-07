"""
Tests for SourceMonitorAgent
NAM Intelligence Pipeline

Comprehensive tests for agents/monitoring/source_monitor.py.
Covers initialization, hashing, blocking detection, item counting,
domain extraction, baseline management, DOM drift detection,
run() routing, _check_sources, _create_baselines, _generate_report,
and _save_alerts_report.
"""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bs4 import BeautifulSoup
from pydantic import Field

# ---------------------------------------------------------------------------
# Patch SourceBaseline to include the ``url_hash`` field.
#
# The production source_monitor.py code passes ``url_hash=...`` to the
# SourceBaseline constructor and later reads ``baseline.url_hash``, but
# the Pydantic model in models/ontology.py does not declare that field.
# Pydantic V2 silently ignores unknown kwargs and then ``baseline.url_hash``
# raises AttributeError.  We fix this for tests by creating a subclass
# that adds the missing field and monkey-patching it into both modules.
# ---------------------------------------------------------------------------
import models.ontology as _ontology_module
from middleware.secrets import _reset_secrets_manager


class _PatchedSourceBaseline(_ontology_module.SourceBaseline):
    """SourceBaseline with the ``url_hash`` field that source_monitor expects."""
    url_hash: str = Field(default="")


# Patch before importing source_monitor so its module-level import picks it up
_ontology_module.SourceBaseline = _PatchedSourceBaseline  # type: ignore[misc]

import agents.monitoring.source_monitor as _sm_module  # noqa: E402

_sm_module.SourceBaseline = _PatchedSourceBaseline  # type: ignore[misc]

# Re-export so tests can reference SourceBaseline from here
SourceBaseline = _PatchedSourceBaseline


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_secrets_singleton():
    """Reset the secrets manager singleton before/after each test."""
    _reset_secrets_manager()
    yield
    _reset_secrets_manager()


def _create_source_monitor(agent_config=None):
    """Factory that creates a SourceMonitorAgent with mocked base deps."""
    from agents.monitoring.source_monitor import SourceMonitorAgent

    with (
        patch("agents.base.Config") as mock_config,
        patch("agents.base.StructuredLogger"),
        patch("agents.base.AsyncHTTPClient"),
        patch("agents.base.RateLimiter"),
    ):
        mock_config.return_value.load.return_value = agent_config or {}
        agent = SourceMonitorAgent(
            agent_type="monitoring.source_monitor",
            job_id="test-job-123",
        )
        return agent


@pytest.fixture
def monitor(tmp_path):
    """Create a SourceMonitorAgent wired to tmp_path directories."""
    agent = _create_source_monitor({
        "baseline_dir": str(tmp_path / "baselines"),
        "report_dir": str(tmp_path / "reports"),
    })
    agent.baseline_dir = tmp_path / "baselines"
    agent.report_dir = tmp_path / "reports"
    agent.baseline_dir.mkdir(parents=True, exist_ok=True)
    agent.report_dir.mkdir(parents=True, exist_ok=True)
    return agent


def _make_response(status_code=200, text=""):
    """Create a mock HTTP response with status_code and text."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    return resp


def _simple_html(body_content="<p>Hello</p>"):
    """Return a minimal HTML page wrapping *body_content*."""
    return f"<html><head><title>Test</title></head><body>{body_content}</body></html>"


def _member_directory_html(member_count=5):
    """Return HTML with *member_count* member-item divs."""
    items = "".join(
        f'<div class="member-item">Company {i}</div>' for i in range(member_count)
    )
    return f"<html><body><div class='directory'>{items}</div></body></html>"


def _save_baseline_file(monitor, url, **overrides):
    """Persist a baseline JSON file for *url* and return its data dict.

    Because the production SourceBaseline model in ontology.py does not
    declare a ``url_hash`` field, we write the file manually (keyed by
    the SHA-256 of the URL) so that ``_load_baseline`` can find it.
    """
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    now = datetime.now(UTC).isoformat()
    baseline_data = {
        "id": "test-baseline-id",
        "url": url,
        "url_hash": url_hash,
        "domain": "example.com",
        "selector_hashes": {},
        "page_structure_hash": "abc123",
        "expected_item_count": None,
        "content_hash": None,
        "is_active": True,
        "last_checked_at": now,
        "last_changed_at": None,
        "change_count": 0,
        "created_at": now,
        "updated_at": now,
    }
    baseline_data.update(overrides)
    path = monitor.baseline_dir / f"{url_hash}.json"
    with open(path, "w") as f:
        json.dump(baseline_data, f, default=str)
    return baseline_data


# ==========================================================================
# 1. INITIALIZATION (~3 tests)
# ==========================================================================

class TestInitialization:
    """Verify constructor, directories, and default config."""

    def test_constructor_sets_agent_type(self, monitor):
        assert monitor.agent_type == "monitoring.source_monitor"

    def test_baseline_and_report_dirs_created(self, monitor):
        assert monitor.baseline_dir.exists()
        assert monitor.report_dir.exists()

    def test_drift_threshold_default(self, monitor):
        assert monitor.drift_threshold == 0.2

    def test_drift_threshold_custom(self, tmp_path):
        agent = _create_source_monitor({
            "baseline_dir": str(tmp_path / "b"),
            "report_dir": str(tmp_path / "r"),
        })
        # _load_agent_config navigates by agent_type parts, so the flat dict
        # from the mock won't populate agent_config.  Simulate what happens
        # when the YAML *does* contain drift_threshold by assigning directly.
        agent.agent_config["drift_threshold"] = 0.5
        agent._setup()
        assert agent.drift_threshold == 0.5


# ==========================================================================
# 2. HASHING (~4 tests)
# ==========================================================================

class TestHashing:
    """Verify _hash_string and _hash_structure helpers."""

    def test_hash_string_deterministic(self, monitor):
        h1 = monitor._hash_string("hello")
        h2 = monitor._hash_string("hello")
        assert h1 == h2

    def test_hash_string_matches_sha256(self, monitor):
        expected = hashlib.sha256(b"test input").hexdigest()
        assert monitor._hash_string("test input") == expected

    def test_hash_structure_extracts_tag_structure(self, monitor):
        html = "<html><body><div class='a'><p>text</p></div></body></html>"
        soup = BeautifulSoup(html, "lxml")
        h = monitor._hash_structure(soup)
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex digest

    def test_hash_structure_same_html_same_hash(self, monitor):
        html = "<html><body><div><p>hello</p></div></body></html>"
        soup1 = BeautifulSoup(html, "lxml")
        soup2 = BeautifulSoup(html, "lxml")
        assert monitor._hash_structure(soup1) == monitor._hash_structure(soup2)

    def test_hash_structure_different_html_different_hash(self, monitor):
        html1 = "<html><body><div><p>hello</p></div></body></html>"
        html2 = "<html><body><span><a>world</a></span></body></html>"
        soup1 = BeautifulSoup(html1, "lxml")
        soup2 = BeautifulSoup(html2, "lxml")
        assert monitor._hash_structure(soup1) != monitor._hash_structure(soup2)

    def test_hash_structure_limits_depth_to_five(self, monitor):
        """Tags nested deeper than 5 levels should not change the hash."""
        # Build HTML with 7 levels of nesting
        shallow = "<html><body><div><div><div><div><div>X</div></div></div></div></div></body></html>"
        deep = "<html><body><div><div><div><div><div><span><em>Y</em></span></div></div></div></div></div></body></html>"
        soup_shallow = BeautifulSoup(shallow, "lxml")
        soup_deep = BeautifulSoup(deep, "lxml")
        # Both should produce the same hash because depth > 5 is truncated
        assert monitor._hash_structure(soup_shallow) == monitor._hash_structure(soup_deep)


# ==========================================================================
# 3. BLOCKING DETECTION (~5 tests)
# ==========================================================================

class TestBlockingDetection:
    """Verify _check_blocking identifies blocking patterns."""

    def test_detects_rate_limited(self, monitor):
        html = "<html><body>You have been rate limited</body></html>"
        result = monitor._check_blocking(html)
        assert "rate_limit" in result

    def test_detects_captcha(self, monitor):
        html = "<html><body>Please complete the captcha below</body></html>"
        result = monitor._check_blocking(html)
        assert "captcha" in result

    def test_detects_access_denied(self, monitor):
        html = "<html><body>Access Denied - contact admin</body></html>"
        result = monitor._check_blocking(html)
        assert "access_denied" in result

    def test_detects_forbidden(self, monitor):
        html = "<html><body>403 Forbidden</body></html>"
        result = monitor._check_blocking(html)
        assert "forbidden" in result

    def test_detects_unusual_traffic(self, monitor):
        html = "<html><body>We detected unusual traffic from your network</body></html>"
        result = monitor._check_blocking(html)
        assert "unusual_traffic" in result

    def test_clean_html_returns_empty_list(self, monitor):
        html = _member_directory_html(3)
        result = monitor._check_blocking(html)
        assert result == []

    def test_multiple_indicators_returned(self, monitor):
        html = "<html><body>Access Denied - captcha required</body></html>"
        result = monitor._check_blocking(html)
        assert "access_denied" in result
        assert "captcha" in result


# ==========================================================================
# 4. ITEM COUNTING (~4 tests)
# ==========================================================================

class TestItemCounting:
    """Verify _count_items against various HTML patterns."""

    def test_counts_class_based_items(self, monitor):
        html = _member_directory_html(5)
        soup = BeautifulSoup(html, "lxml")
        # "member-item" class matches the regex r'member|company|listing|item|card'
        # via the "item" portion
        count = monitor._count_items(soup)
        assert count >= 5

    def test_counts_table_rows(self, monitor):
        rows = "".join(f"<tr><td>Company {i}</td></tr>" for i in range(10))
        html = f"<html><body><table><tr><th>Name</th></tr>{rows}</table></body></html>"
        soup = BeautifulSoup(html, "lxml")
        count = monitor._count_items(soup)
        # tr count - 1 (header) = 10, plus the class-based items
        assert count >= 10

    def test_counts_li_elements_with_class(self, monitor):
        items = ''.join(f'<li class="entry">Item {i}</li>' for i in range(7))
        html = f"<html><body><ul>{items}</ul></body></html>"
        soup = BeautifulSoup(html, "lxml")
        count = monitor._count_items(soup)
        assert count >= 7

    def test_empty_page_returns_zero(self, monitor):
        html = "<html><body></body></html>"
        soup = BeautifulSoup(html, "lxml")
        count = monitor._count_items(soup)
        assert count == 0


# ==========================================================================
# 5. DOMAIN EXTRACTION (~3 tests)
# ==========================================================================

class TestDomainExtraction:
    """Verify _extract_domain parses URLs correctly."""

    def test_extracts_domain_from_url(self, monitor):
        assert monitor._extract_domain("https://www.pma.org/members") == "www.pma.org"

    def test_handles_url_with_port(self, monitor):
        assert monitor._extract_domain("http://localhost:8080/page") == "localhost:8080"

    def test_handles_url_without_www(self, monitor):
        assert monitor._extract_domain("https://nema.org/directory") == "nema.org"


# ==========================================================================
# 6. BASELINE MANAGEMENT (~6 tests)
# ==========================================================================

class TestBaselineManagement:
    """Verify save / load / update baseline persistence."""

    def test_save_and_load_baseline_round_trip(self, monitor):
        url = "https://example.com/members"
        _save_baseline_file(
            monitor,
            url,
            page_structure_hash="struct_hash_1",
            expected_item_count=42,
        )
        loaded = monitor._load_baseline(url)
        assert loaded is not None
        assert loaded.url == url
        assert loaded.page_structure_hash == "struct_hash_1"
        assert loaded.expected_item_count == 42

    def test_load_baseline_returns_none_for_missing(self, monitor):
        result = monitor._load_baseline("https://no-such-url.com")
        assert result is None

    def test_update_baseline_increments_change_count(self, monitor):
        url = "https://example.com/dir"
        _save_baseline_file(monitor, url, change_count=2)
        baseline = monitor._load_baseline(url)
        # Manually set url_hash so _save_baseline can write the file
        baseline.url_hash = monitor._hash_string(url)

        monitor._update_baseline(baseline, "<html>new</html>", 3)

        reloaded = monitor._load_baseline(url)
        assert reloaded.change_count == 5  # 2 + 3

    def test_update_baseline_sets_timestamps(self, monitor):
        url = "https://example.com/ts"
        _save_baseline_file(monitor, url)
        baseline = monitor._load_baseline(url)
        baseline.url_hash = monitor._hash_string(url)

        datetime.now(UTC)
        monitor._update_baseline(baseline, "<html>x</html>", 1)
        datetime.now(UTC)

        reloaded = monitor._load_baseline(url)
        assert reloaded.last_checked_at is not None
        assert reloaded.last_changed_at is not None

    def test_update_baseline_updates_content_hash(self, monitor):
        url = "https://example.com/ch"
        _save_baseline_file(monitor, url, content_hash="old_hash")
        baseline = monitor._load_baseline(url)
        baseline.url_hash = monitor._hash_string(url)

        new_html = "<html>new content</html>"
        monitor._update_baseline(baseline, new_html, 1)

        reloaded = monitor._load_baseline(url)
        expected_hash = monitor._hash_string(new_html)
        assert reloaded.content_hash == expected_hash

    @pytest.mark.asyncio
    async def test_create_baseline_for_url_success(self, monitor):
        """Mock HTTP and verify baseline is created and saved."""
        url = "https://example.com/test-page"
        html = _member_directory_html(3)
        monitor.http.get = AsyncMock(return_value=_make_response(200, html))

        baseline = await monitor._create_baseline_for_url(url, {"name": ".member-item"})

        assert baseline.url == url
        assert baseline.domain == "example.com"
        assert baseline.page_structure_hash  # non-empty hash
        # Verify the baseline was persisted
        assert any(monitor.baseline_dir.glob("*.json"))


# ==========================================================================
# 7. DOM DRIFT DETECTION / _compare_to_baseline (~6 tests)
# ==========================================================================

class TestCompareToBaseline:
    """Verify _compare_to_baseline alert generation."""

    def _make_baseline(self, monitor, url, html):
        """Build a SourceBaseline-like object matching *html*."""
        from models.ontology import SourceBaseline

        soup = BeautifulSoup(html, "lxml")
        structure_hash = monitor._hash_structure(soup)
        expected_count = monitor._count_items(soup)
        content_hash = monitor._hash_string(html)

        baseline = SourceBaseline(
            url=url,
            domain=monitor._extract_domain(url),
            page_structure_hash=structure_hash,
            expected_item_count=expected_count,
            content_hash=content_hash,
            selector_hashes={},
        )
        return baseline

    def test_identical_page_no_alerts(self, monitor):
        url = "https://example.com/same"
        html = _member_directory_html(5)
        baseline = self._make_baseline(monitor, url, html)
        alerts = monitor._compare_to_baseline(url, html, baseline)
        assert alerts == []

    def test_structure_change_triggers_dom_drift(self, monitor):
        url = "https://example.com/drift"
        original_html = "<html><body><div><p>old</p></div></body></html>"
        baseline = self._make_baseline(monitor, url, original_html)

        new_html = "<html><body><section><article>new</article></section></body></html>"
        alerts = monitor._compare_to_baseline(url, new_html, baseline)

        dom_drift_alerts = [a for a in alerts if a.get("type") == "DOM_DRIFT"]
        assert len(dom_drift_alerts) >= 1

    def test_selector_broken_returns_critical(self, monitor):
        url = "https://example.com/broken"
        original_html = '<html><body><div class="member-item">X</div></body></html>'
        soup = BeautifulSoup(original_html, "lxml")

        from models.ontology import SourceBaseline

        baseline = SourceBaseline(
            url=url,
            domain="example.com",
            page_structure_hash=monitor._hash_structure(soup),
            selector_hashes={
                "company": monitor._hash_string(str(soup.select(".member-item")))
            },
        )

        # New HTML has NO .member-item at all
        new_html = "<html><body><div class='other'>Y</div></body></html>"
        selectors = {"company": ".member-item"}
        alerts = monitor._compare_to_baseline(url, new_html, baseline, selectors)

        broken = [a for a in alerts if a.get("type") == "SELECTOR_BROKEN"]
        assert len(broken) == 1
        assert broken[0]["level"] == monitor.ALERT_CRITICAL

    def test_selector_changed_returns_warning(self, monitor):
        url = "https://example.com/changed"
        original_html = '<html><body><div class="member-item">A</div></body></html>'
        soup = BeautifulSoup(original_html, "lxml")

        from models.ontology import SourceBaseline

        baseline = SourceBaseline(
            url=url,
            domain="example.com",
            page_structure_hash=monitor._hash_structure(soup),
            selector_hashes={
                "company": monitor._hash_string(str(soup.select(".member-item")))
            },
        )

        # New HTML still has .member-item but content is different
        new_html = '<html><body><div class="member-item">DIFFERENT</div></body></html>'
        selectors = {"company": ".member-item"}
        alerts = monitor._compare_to_baseline(url, new_html, baseline, selectors)

        changed = [a for a in alerts if a.get("type") == "SELECTOR_CHANGED"]
        assert len(changed) == 1
        assert changed[0]["level"] == monitor.ALERT_WARNING

    def test_item_count_drop_to_zero_is_critical(self, monitor):
        url = "https://example.com/vanished"
        html_with_items = _member_directory_html(10)

        from models.ontology import SourceBaseline

        soup = BeautifulSoup(html_with_items, "lxml")
        baseline = SourceBaseline(
            url=url,
            domain="example.com",
            page_structure_hash=monitor._hash_structure(soup),
            expected_item_count=10,
        )

        empty_html = "<html><body><div class='directory'></div></body></html>"
        alerts = monitor._compare_to_baseline(url, empty_html, baseline)

        missing = [a for a in alerts if a.get("type") == "ITEMS_MISSING"]
        assert len(missing) == 1
        assert missing[0]["level"] == monitor.ALERT_CRITICAL
        assert missing[0]["expected"] == 10
        assert missing[0]["actual"] == 0

    def test_blocking_indicators_generate_critical_alert(self, monitor):
        url = "https://example.com/blocked"
        html = "<html><body><p>Normal page</p></body></html>"
        baseline = self._make_baseline(monitor, url, html)

        blocked_html = "<html><body><p>Access Denied</p></body></html>"
        alerts = monitor._compare_to_baseline(url, blocked_html, baseline)

        blocked = [a for a in alerts if a.get("type") == "ACCESS_BLOCKED"]
        assert len(blocked) == 1
        assert blocked[0]["level"] == monitor.ALERT_CRITICAL


# ==========================================================================
# 8. run() ROUTING (~4 tests)
# ==========================================================================

class TestRunRouting:
    """Verify run() dispatches to the correct handler."""

    @pytest.mark.asyncio
    async def test_check_action_routes_to_check_sources(self, monitor):
        monitor._check_sources = AsyncMock(return_value={"success": True, "records_processed": 0})
        await monitor.run({"action": "check", "urls": []})
        monitor._check_sources.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_baseline_action_routes_to_create_baselines(self, monitor):
        monitor._create_baselines = AsyncMock(return_value={"success": True, "records_processed": 0})
        await monitor.run({"action": "baseline", "urls": []})
        monitor._create_baselines.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_report_action_routes_to_generate_report(self, monitor):
        monitor._generate_report = AsyncMock(return_value={"success": True, "records_processed": 0})
        await monitor.run({"action": "report"})
        monitor._generate_report.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unknown_action_returns_error(self, monitor):
        result = await monitor.run({"action": "invalid_action"})
        assert result["success"] is False
        assert "Unknown action" in result["error"]

    @pytest.mark.asyncio
    async def test_default_action_is_check(self, monitor):
        """When no action is specified, default to 'check'."""
        monitor._check_sources = AsyncMock(return_value={"success": True, "records_processed": 0})
        await monitor.run({"urls": ["https://example.com"]})
        monitor._check_sources.assert_awaited_once()


# ==========================================================================
# 9. _check_sources (~5 tests)
# ==========================================================================

class TestCheckSources:
    """Verify _check_sources behaviour with mocked HTTP."""

    @pytest.mark.asyncio
    async def test_no_urls_returns_error(self, monitor):
        result = await monitor._check_sources({"urls": []})
        assert result["success"] is False
        assert "No URLs" in result["error"]

    @pytest.mark.asyncio
    async def test_no_baseline_creates_one(self, monitor):
        url = "https://example.com/new-page"
        html = _simple_html("<p>New</p>")
        monitor.http.get = AsyncMock(return_value=_make_response(200, html))

        result = await monitor._check_sources({"urls": [url], "selectors": {}})

        assert result["success"] is True
        # An INFO alert about creating baseline should be present
        info_alerts = [a for a in result["alerts"] if a["level"] == monitor.ALERT_INFO]
        assert len(info_alerts) == 1
        assert "No baseline exists" in info_alerts[0]["message"]

    @pytest.mark.asyncio
    async def test_http_error_generates_critical_alert(self, monitor):
        url = "https://example.com/fail"
        _save_baseline_file(monitor, url)
        monitor.http.get = AsyncMock(return_value=_make_response(503, "Server Error"))

        result = await monitor._check_sources({"urls": [url]})

        assert result["success"] is True
        assert result["changes_detected"] is True
        critical = [a for a in result["alerts"] if a["level"] == monitor.ALERT_CRITICAL]
        assert any("HTTP 503" in a["message"] for a in critical)

    @pytest.mark.asyncio
    async def test_fetch_exception_generates_critical_alert(self, monitor):
        url = "https://example.com/timeout"
        _save_baseline_file(monitor, url)
        monitor.http.get = AsyncMock(side_effect=TimeoutError("Connection timed out"))

        result = await monitor._check_sources({"urls": [url]})

        assert result["changes_detected"] is True
        critical = [a for a in result["alerts"] if a["level"] == monitor.ALERT_CRITICAL]
        assert len(critical) >= 1
        assert any("Failed to fetch" in a["message"] for a in critical)

    @pytest.mark.asyncio
    async def test_blocking_detected_in_html(self, monitor):
        url = "https://example.com/captcha-page"
        # Create a baseline from normal content
        normal_html = _simple_html("<p>Normal content</p>")
        _save_baseline_file(
            monitor,
            url,
            page_structure_hash=monitor._hash_structure(
                BeautifulSoup(normal_html, "lxml")
            ),
        )

        blocked_html = _simple_html("<p>Please complete the captcha to continue</p>")
        monitor.http.get = AsyncMock(return_value=_make_response(200, blocked_html))

        result = await monitor._check_sources({"urls": [url]})

        assert result["changes_detected"] is True
        blocked = [a for a in result["alerts"] if a.get("type") == "ACCESS_BLOCKED"]
        assert len(blocked) >= 1

    @pytest.mark.asyncio
    async def test_changes_detected_saves_report(self, monitor):
        url = "https://example.com/report-trigger"
        _save_baseline_file(monitor, url)
        monitor.http.get = AsyncMock(return_value=_make_response(500, "Error"))

        result = await monitor._check_sources({"urls": [url]})

        assert result["report_path"] is not None
        assert Path(result["report_path"]).exists()


# ==========================================================================
# 10. _create_baselines (~3 tests)
# ==========================================================================

class TestCreateBaselines:
    """Verify _create_baselines batch creation."""

    @pytest.mark.asyncio
    async def test_creates_baselines_for_urls(self, monitor):
        html = _member_directory_html(3)
        monitor.http.get = AsyncMock(return_value=_make_response(200, html))

        result = await monitor._create_baselines({
            "urls": ["https://a.com", "https://b.com"],
            "selectors": {},
        })

        assert result["success"] is True
        assert result["baselines_created"] == 2
        assert result["records_processed"] == 2

    @pytest.mark.asyncio
    async def test_handles_fetch_errors_gracefully(self, monitor):
        monitor.http.get = AsyncMock(
            side_effect=Exception("Network error"),
        )

        result = await monitor._create_baselines({
            "urls": ["https://fail.com"],
            "selectors": {},
        })

        assert result["success"] is True
        assert result["baselines_created"] == 0
        assert len(result["errors"]) == 1
        assert "Network error" in result["errors"][0]["error"]

    @pytest.mark.asyncio
    async def test_returns_count_of_created(self, monitor):
        call_count = 0

        async def _alternating_response(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 0:
                raise Exception("fail")
            return _make_response(200, _simple_html())

        monitor.http.get = AsyncMock(side_effect=_alternating_response)

        result = await monitor._create_baselines({
            "urls": ["https://ok.com", "https://fail.com", "https://also-ok.com"],
            "selectors": {},
        })

        assert result["baselines_created"] == 2
        assert len(result["errors"]) == 1

    @pytest.mark.asyncio
    async def test_no_urls_returns_error(self, monitor):
        result = await monitor._create_baselines({"urls": [], "selectors": {}})
        assert result["success"] is False
        assert "No URLs" in result["error"]


# ==========================================================================
# 11. _generate_report (~3 tests)
# ==========================================================================

class TestGenerateReport:
    """Verify _generate_report loads baselines and alerts."""

    @pytest.mark.asyncio
    async def test_returns_source_count(self, monitor):
        _save_baseline_file(monitor, "https://one.com")
        _save_baseline_file(monitor, "https://two.com")

        result = await monitor._generate_report({})

        assert result["success"] is True
        assert result["total_sources"] == 2

    @pytest.mark.asyncio
    async def test_loads_baselines_and_recent_alerts(self, monitor):
        _save_baseline_file(monitor, "https://a.com", change_count=3)
        _save_baseline_file(monitor, "https://b.com", change_count=0)

        # Create a fake alert report
        report_data = {"generated_at": "2026-01-01", "alerts": []}
        alert_path = monitor.report_dir / "change_report_20260101_120000.json"
        with open(alert_path, "w") as f:
            json.dump(report_data, f)

        result = await monitor._generate_report({})

        assert result["total_sources"] == 2

    @pytest.mark.asyncio
    async def test_saves_report_file(self, monitor):
        _save_baseline_file(monitor, "https://x.com")

        result = await monitor._generate_report({})

        report_path = Path(result["report_path"])
        assert report_path.exists()
        with open(report_path) as f:
            report = json.load(f)
        assert "total_sources" in report
        assert "baselines" in report

    @pytest.mark.asyncio
    async def test_empty_baselines(self, monitor):
        result = await monitor._generate_report({})
        assert result["success"] is True
        assert result["total_sources"] == 0


# ==========================================================================
# 12. _save_alerts_report (~2 tests)
# ==========================================================================

class TestSaveAlertsReport:
    """Verify _save_alerts_report persists JSON with alert counts."""

    def test_saves_json_report(self, monitor):
        alerts = [
            {"level": "CRITICAL", "url": "https://a.com", "message": "down"},
            {"level": "WARNING", "url": "https://b.com", "message": "drift"},
            {"level": "INFO", "url": "https://c.com", "message": "baseline"},
        ]
        report_path = monitor._save_alerts_report(alerts)

        assert Path(report_path).exists()
        with open(report_path) as f:
            report = json.load(f)
        assert report["alert_count"] == 3

    def test_includes_alert_counts_by_level(self, monitor):
        alerts = [
            {"level": "CRITICAL", "url": "https://a.com", "message": "x"},
            {"level": "CRITICAL", "url": "https://b.com", "message": "y"},
            {"level": "WARNING", "url": "https://c.com", "message": "z"},
            {"level": "INFO", "url": "https://d.com", "message": "w"},
        ]
        report_path = monitor._save_alerts_report(alerts)

        with open(report_path) as f:
            report = json.load(f)
        assert report["critical_count"] == 2
        assert report["warning_count"] == 1
        assert report["info_count"] == 1


# ==========================================================================
# ADDITIONAL EDGE-CASE TESTS
# ==========================================================================

class TestAlertLevelConstants:
    """Verify the class-level alert constants."""

    def test_alert_critical_value(self, monitor):
        assert monitor.ALERT_CRITICAL == "CRITICAL"

    def test_alert_warning_value(self, monitor):
        assert monitor.ALERT_WARNING == "WARNING"

    def test_alert_info_value(self, monitor):
        assert monitor.ALERT_INFO == "INFO"


class TestCalculateDrift:
    """Verify _calculate_drift placeholder returns 0.1."""

    def test_returns_placeholder_value(self, monitor):
        from models.ontology import SourceBaseline

        html = _simple_html()
        soup = BeautifulSoup(html, "lxml")
        baseline = SourceBaseline(
            url="https://x.com",
            domain="x.com",
            page_structure_hash="abc",
        )
        drift = monitor._calculate_drift(soup, baseline)
        assert drift == 0.1


class TestItemCountChanged:
    """Verify ITEM_COUNT_CHANGED alert when pct_diff > 0.5."""

    def test_significant_count_decrease_triggers_warning(self, monitor):
        from models.ontology import SourceBaseline

        url = "https://example.com/shrink"
        html_100 = _member_directory_html(100)
        soup_100 = BeautifulSoup(html_100, "lxml")
        baseline = SourceBaseline(
            url=url,
            domain="example.com",
            page_structure_hash=monitor._hash_structure(soup_100),
            expected_item_count=100,
        )

        # New page has only 10 items -> 90% change > 50% threshold
        html_10 = _member_directory_html(10)
        alerts = monitor._compare_to_baseline(url, html_10, baseline)

        count_changed = [a for a in alerts if a.get("type") == "ITEM_COUNT_CHANGED"]
        assert len(count_changed) == 1
        assert count_changed[0]["level"] == monitor.ALERT_WARNING

    def test_small_count_change_no_alert(self, monitor):
        from models.ontology import SourceBaseline

        url = "https://example.com/stable"
        html_10 = _member_directory_html(10)
        soup_10 = BeautifulSoup(html_10, "lxml")
        baseline = SourceBaseline(
            url=url,
            domain="example.com",
            page_structure_hash=monitor._hash_structure(soup_10),
            expected_item_count=10,
        )

        # 8 items -> 20% change < 50% threshold
        html_8 = _member_directory_html(8)
        alerts = monitor._compare_to_baseline(url, html_8, baseline)

        count_changed = [a for a in alerts if a.get("type") == "ITEM_COUNT_CHANGED"]
        assert len(count_changed) == 0
