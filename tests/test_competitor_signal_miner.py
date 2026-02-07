"""
Competitor Signal Miner Agent Tests
NAM Intelligence Pipeline

Comprehensive tests for the CompetitorSignalMinerAgent and CompetitorReportGenerator.
Covers initialization, brand detection, classification, confidence scoring,
HTML processing, run(), scan_batch(), report generation, and error handling.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from middleware.secrets import _reset_secrets_manager
from models.ontology import COMPETITOR_ALIASES, CompetitorSignalType

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(autouse=True)
def reset_secrets_singleton():
    _reset_secrets_manager()
    yield
    _reset_secrets_manager()


def create_signal_miner(agent_config=None):
    """Create a CompetitorSignalMinerAgent with mocked dependencies.

    agent_config is the leaf config dict for the agent.  It gets nested
    under {"intelligence": {"competitor_signal_miner": ...}} so that
    _load_agent_config() can navigate the path correctly.
    """
    from agents.intelligence.competitor_signal_miner import CompetitorSignalMinerAgent

    nested = {"intelligence": {"competitor_signal_miner": agent_config or {}}}

    with (
        patch("agents.base.Config") as mock_config,
        patch("agents.base.StructuredLogger"),
        patch("agents.base.AsyncHTTPClient"),
        patch("agents.base.RateLimiter"),
    ):
        mock_config.return_value.load.return_value = nested
        agent = CompetitorSignalMinerAgent(
            agent_type="intelligence.competitor_signal_miner",
            job_id="test-job-123",
        )
        return agent


@pytest.fixture
def miner():
    return create_signal_miner()


@pytest.fixture
def miner_small_context():
    """Miner with a very small context window for easier assertion."""
    return create_signal_miner({"context_window": 20})


@pytest.fixture
def miner_max_signals():
    """Miner limited to 3 signals max."""
    return create_signal_miner({"max_signals": 3})


# =============================================================================
# 1. INITIALIZATION TESTS
# =============================================================================


class TestInitialization:
    """Tests for agent construction and configuration."""

    def test_constructor_creates_agent(self, miner):
        """Agent is successfully instantiated with correct type."""
        assert miner.agent_type == "intelligence.competitor_signal_miner"
        assert miner.job_id == "test-job-123"

    def test_default_config_values(self, miner):
        """Default max_signals and context_window are applied when config is empty."""
        assert miner.max_signals == 100
        assert miner.context_window == 150

    def test_custom_config_values(self):
        """Agent respects custom config for max_signals and context_window."""
        agent = create_signal_miner({"max_signals": 50, "context_window": 200})
        assert agent.max_signals == 50
        assert agent.context_window == 200

    def test_competitor_patterns_built(self, miner):
        """Competitor regex patterns are built from COMPETITOR_ALIASES."""
        assert len(miner.competitor_patterns) == len(COMPETITOR_ALIASES)
        for competitor in COMPETITOR_ALIASES:
            assert competitor in miner.competitor_patterns
            # Each value should be a compiled regex
            assert hasattr(miner.competitor_patterns[competitor], "finditer")


# =============================================================================
# 2. BRAND DETECTION TESTS
# =============================================================================


class TestBrandDetection:
    """Tests for competitor brand mention detection via regex."""

    def test_exact_match_lowercase(self, miner):
        """Exact lowercase alias is detected."""
        text = "We implemented epicor for our manufacturing line."
        from models.ontology import Provenance

        prov = Provenance(
            source_url="http://test.com",
            extracted_by="test",
        )
        signals = miner._mine_signals(text, "http://test.com", None, None, "PMA", prov)
        competitor_names = [s.competitor_normalized for s in signals]
        assert "epicor" in competitor_names

    def test_case_insensitive_match(self, miner):
        """Mixed-case brand names are matched case-insensitively."""
        text = "Our factory runs EPICOR ERP for production scheduling."
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner._mine_signals(text, "http://test.com", None, None, None, prov)
        competitor_names = [s.competitor_normalized for s in signals]
        assert "epicor" in competitor_names

    def test_no_match_returns_empty(self, miner):
        """Text without any competitor names yields zero signals."""
        text = "This is a completely generic manufacturing article about widgets."
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner._mine_signals(text, "http://test.com", None, None, None, prov)
        assert signals == []

    def test_multiple_brands_single_page(self, miner):
        """Multiple different competitor brands are all detected."""
        text = "We compared Epicor and SAP before choosing Infor for our ERP."
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner._mine_signals(text, "http://test.com", None, None, None, prov)
        normalized = {s.competitor_normalized for s in signals}
        assert "epicor" in normalized
        assert "sap" in normalized
        assert "infor" in normalized

    def test_context_extraction_around_mention(self, miner_small_context):
        """Context is extracted within the configured context_window around the match."""
        # Build text with a known brand surrounded by padding.
        # context_window=20, so context is +/-20 chars around the match.
        # Use " word" (leading space) on right side to preserve word boundary.
        padding_left = "word " * 40   # 200 chars, ends with space
        padding_right = " word" * 40  # 200 chars, starts with space
        text = padding_left + "Epicor" + padding_right
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner_small_context._mine_signals(
            text, "http://test.com", None, None, None, prov
        )
        assert len(signals) >= 1
        context = signals[0].context
        # Context should contain the brand plus surrounding text, trimmed by window
        assert "Epicor" in context
        # Context should be shorter than the full text (clipped by small window)
        assert len(context) < len(text)

    def test_word_boundary_prevents_partial_match(self, miner):
        """Word boundary \\b prevents matching brand inside unrelated words.

        E.g., 'SAP' should NOT match in 'sapping' because of \\b.
        """
        text = "The tree sapping process removes excess moisture from wood."
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner._mine_signals(text, "http://test.com", None, None, None, prov)
        sap_signals = [s for s in signals if s.competitor_normalized == "sap"]
        assert len(sap_signals) == 0

    def test_multiple_matches_same_competitor(self, miner):
        """Multiple mentions of the same competitor produce multiple signals."""
        text = "Epicor is great. We love Epicor. Epicor is the best ERP for manufacturing."
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner._mine_signals(text, "http://test.com", None, None, None, prov)
        epicor_signals = [s for s in signals if s.competitor_normalized == "epicor"]
        assert len(epicor_signals) == 3

    def test_alias_match(self, miner):
        """Aliases like 'Epicor Kinetic' are matched and attributed to parent competitor."""
        text = "We upgraded to Epicor Kinetic last quarter."
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner._mine_signals(text, "http://test.com", None, None, None, prov)
        # Should find at least the "epicor kinetic" alias match
        epicor_signals = [s for s in signals if s.competitor_normalized == "epicor"]
        assert len(epicor_signals) >= 1


# =============================================================================
# 3. CLASSIFICATION TESTS
# =============================================================================


class TestClassification:
    """Tests for _classify_signal_type method."""

    def test_sponsor_context_detected(self, miner):
        """Context containing sponsor keywords classifies as SPONSOR."""
        result = miner._classify_signal_type(
            "Sponsored by Epicor Corporation at the annual event",
            "full page text about the event",
        )
        assert result == CompetitorSignalType.SPONSOR

    def test_exhibitor_context_detected(self, miner):
        """Context with exhibitor/booth keywords classifies as EXHIBITOR."""
        result = miner._classify_signal_type(
            "Visit us at booth 142, exhibiting our newest products",
            "trade show information page",
        )
        assert result == CompetitorSignalType.EXHIBITOR

    def test_member_usage_context_detected(self, miner):
        """Context with 'using' or 'implementation' classifies as MEMBER_USAGE."""
        result = miner._classify_signal_type(
            "We have been using this erp system for two years with great success",
            "company profile page",
        )
        assert result == CompetitorSignalType.MEMBER_USAGE

    def test_speaker_bio_context_detected(self, miner):
        """Context with speaker/presenter keywords classifies as SPEAKER_BIO."""
        result = miner._classify_signal_type(
            "Our keynote speaker and presenter on digital transformation",
            "event schedule page",
        )
        assert result == CompetitorSignalType.SPEAKER_BIO

    def test_default_classification_when_no_pattern_matches(self, miner):
        """When no pattern matches, default to MEMBER_USAGE."""
        result = miner._classify_signal_type(
            "something completely unrelated",
            "also unrelated text",
        )
        assert result == CompetitorSignalType.MEMBER_USAGE


# =============================================================================
# 4. CONFIDENCE SCORING TESTS
# =============================================================================


class TestConfidenceScoring:
    """Tests for _calculate_confidence method."""

    def test_base_confidence_is_0_7(self, miner):
        """Unrecognized matched text with generic context yields base 0.7."""
        confidence = miner._calculate_confidence("xyznotanalias", "generic context here")
        assert confidence == 0.7

    def test_boost_for_exact_product_alias(self, miner):
        """Known alias in COMPETITOR_ALIASES gets a +0.1 boost."""
        # "epicor" is a known alias
        confidence = miner._calculate_confidence("epicor", "generic context here")
        assert confidence == pytest.approx(0.8)

    def test_boost_for_specific_indicators(self, miner):
        """Context containing 'erp', 'software', etc. gets +0.05 each."""
        # "xyznotanalias" -> no alias boost. Context has "erp" and "software" -> +0.10
        confidence = miner._calculate_confidence(
            "xyznotanalias", "our erp software platform"
        )
        assert confidence == pytest.approx(0.8)

    def test_confidence_capped_at_0_95(self, miner):
        """Even with many boosts, confidence never exceeds 0.95."""
        # Known alias (+0.1) plus context with all indicators
        confidence = miner._calculate_confidence(
            "epicor",
            "erp software system implementation using the platform every day",
        )
        assert confidence <= 0.95

    def test_multiple_indicators_accumulate(self, miner):
        """Each unique indicator adds its own +0.05 boost."""
        # No alias match; context has "erp" (+0.05), "software" (+0.05), "system" (+0.05)
        confidence = miner._calculate_confidence(
            "xyznotanalias", "erp software system"
        )
        # 0.7 + 0.05 + 0.05 + 0.05 = 0.85
        assert confidence == pytest.approx(0.85)


# =============================================================================
# 5. HTML PROCESSING TESTS
# =============================================================================


class TestHTMLProcessing:
    """Tests for HTML text extraction and processing."""

    @pytest.mark.asyncio
    async def test_html_text_extraction(self, miner):
        """HTML content is parsed via BeautifulSoup to extract text."""
        html = "<html><body><p>We use Epicor for our production line.</p></body></html>"
        result = await miner.run({"html": html, "association": "PMA"})
        assert result["success"] is True
        assert len(result["signals"]) >= 1

    @pytest.mark.asyncio
    async def test_empty_input_no_content(self, miner):
        """When URL fetch fails and no html/text provided, returns no-content error."""
        # Provide a URL but make the fetch fail, so html and text remain None
        miner.http.get = AsyncMock(side_effect=Exception("fetch failed"))
        result = await miner.run({"url": "https://example.com/empty"})
        assert result["success"] is False
        assert "No content" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_text_only_input(self, miner):
        """Plain text input (no HTML) is processed directly."""
        result = await miner.run({
            "text": "SAP Business One is widely used in this industry.",
            "association": "NEMA",
        })
        assert result["success"] is True
        assert len(result["signals"]) >= 1
        names = [s["competitor_normalized"] if isinstance(s, dict) else s.competitor_normalized
                 for s in result["signals"]]
        assert "sap" in names

    @pytest.mark.asyncio
    async def test_url_fetch_when_only_url_provided(self, miner):
        """When only URL is provided, agent fetches the page via HTTP."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Epicor ERP solution overview</body></html>"
        miner.http.get = AsyncMock(return_value=mock_response)

        result = await miner.run({
            "url": "https://example.com/page",
            "association": "PMA",
        })
        assert result["success"] is True
        miner.http.get.assert_awaited_once_with("https://example.com/page", timeout=30)


# =============================================================================
# 6. run() METHOD TESTS
# =============================================================================


class TestRunMethod:
    """Tests for the main run() entry point."""

    @pytest.mark.asyncio
    async def test_success_with_text_input(self, miner):
        """run() returns success with signals when given text containing competitors."""
        result = await miner.run({
            "text": "Infor CloudSuite is a leading ERP solution.",
            "association": "PMA",
        })
        assert result["success"] is True
        assert result["records_processed"] == 1
        assert len(result["signals"]) >= 1
        assert "records" in result

    @pytest.mark.asyncio
    async def test_success_with_html_input(self, miner):
        """run() extracts text from HTML and finds competitor mentions."""
        html = "<div><h1>Sponsors</h1><p>Sponsored by SAP and Oracle</p></div>"
        result = await miner.run({"html": html})
        assert result["success"] is True
        summary = result["competitor_summary"]
        assert "Sap" in summary or "SAP" in summary or "sap" in [k.lower() for k in summary]

    @pytest.mark.asyncio
    async def test_empty_input_returns_error(self, miner):
        """run() with no url, html, or text returns an error."""
        result = await miner.run({})
        assert result["success"] is False
        assert "required" in result["error"].lower()
        assert result["records_processed"] == 0

    @pytest.mark.asyncio
    async def test_url_fetch_error_returns_no_content(self, miner):
        """When URL fetch fails (network error), agent returns no-content error."""
        miner.http.get = AsyncMock(side_effect=ConnectionError("DNS resolution failed"))

        result = await miner.run({"url": "https://bad-domain.example.com"})
        assert result["success"] is False
        assert "No content" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_competitor_summary_in_response(self, miner):
        """Response includes competitor_summary dict with brand counts."""
        text = "Epicor is used here. Epicor again. And SAP too."
        result = await miner.run({"text": text})
        assert result["success"] is True
        summary = result["competitor_summary"]
        # Epicor should appear with title case
        epicor_key = [k for k in summary if k.lower() == "epicor"]
        assert len(epicor_key) == 1
        assert summary[epicor_key[0]] == 2

    @pytest.mark.asyncio
    async def test_provenance_source_url_set(self, miner):
        """Provenance uses the URL when provided, else 'text_input'."""
        result = await miner.run({
            "text": "Using Epicor for manufacturing",
            "url": "https://example.com/page",
        })
        assert result["success"] is True
        # Signals should have provenance pointing to the URL
        signal = result["signals"][0]
        if isinstance(signal, dict):
            provenance = signal.get("provenance", [])
            assert any(p.get("source_url") == "https://example.com/page" for p in provenance)

    @pytest.mark.asyncio
    async def test_provenance_text_input_fallback(self, miner):
        """When no URL, provenance source_url is 'text_input'."""
        result = await miner.run({"text": "Epicor implementation guide"})
        assert result["success"] is True
        signal = result["signals"][0]
        if isinstance(signal, dict):
            provenance = signal.get("provenance", [])
            assert any(p.get("source_url") == "text_input" for p in provenance)


# =============================================================================
# 7. scan_batch() TESTS
# =============================================================================


class TestScanBatch:
    """Tests for batch scanning of multiple pages."""

    @pytest.mark.asyncio
    async def test_batch_multiple_pages(self, miner):
        """scan_batch processes multiple pages and aggregates signals."""
        pages = [
            {"text": "Epicor is our ERP of choice.", "association": "PMA"},
            {"text": "We use SAP for production planning.", "association": "NEMA"},
        ]
        result = await miner.scan_batch({"pages": pages})
        assert result["success"] is True
        assert result["pages_scanned"] == 2
        assert result["records_processed"] == 2
        assert len(result["signals"]) >= 2
        summary = result["competitor_summary"]
        # Both competitors should appear
        lower_keys = {k.lower() for k in summary}
        assert "epicor" in lower_keys
        assert "sap" in lower_keys

    @pytest.mark.asyncio
    async def test_batch_empty_pages(self, miner):
        """scan_batch with empty pages list returns error."""
        result = await miner.scan_batch({"pages": []})
        assert result["success"] is False
        assert "No pages" in result["error"]
        assert result["records_processed"] == 0

    @pytest.mark.asyncio
    async def test_batch_aggregates_counts(self, miner):
        """Competitor counts from multiple pages are summed correctly."""
        pages = [
            {"text": "Epicor ERP is great. Epicor again.", "association": "PMA"},
            {"text": "Epicor used by our members.", "association": "NEMA"},
        ]
        result = await miner.scan_batch({"pages": pages})
        assert result["success"] is True
        # Total Epicor mentions: 2 from page1 (via "epicor" alias) + 1 from page2
        epicor_key = [k for k in result["competitor_summary"] if k.lower() == "epicor"]
        assert len(epicor_key) == 1
        assert result["competitor_summary"][epicor_key[0]] >= 3


# =============================================================================
# 8. COMPETITOR REPORT GENERATOR TESTS
# =============================================================================


class TestCompetitorReportGenerator:
    """Tests for the CompetitorReportGenerator static report method."""

    def _make_signal(
        self,
        competitor_name="Epicor",
        competitor_normalized="epicor",
        signal_type="MEMBER_USAGE",
        source_association=None,
        source_event_id=None,
        source_company_id=None,
    ):
        return {
            "competitor_name": competitor_name,
            "competitor_normalized": competitor_normalized,
            "signal_type": signal_type,
            "context": "some context",
            "confidence": 0.8,
            "source_association": source_association,
            "source_event_id": source_event_id,
            "source_company_id": source_company_id,
        }

    def test_empty_signals_returns_zero_total(self):
        """Empty signal list produces report with total_signals=0."""
        from agents.intelligence.competitor_signal_miner import CompetitorReportGenerator

        report = CompetitorReportGenerator.generate_report([])
        assert report["total_signals"] == 0
        assert "competitors" not in report

    def test_single_competitor_report(self):
        """Report for a single competitor has correct structure."""
        from agents.intelligence.competitor_signal_miner import CompetitorReportGenerator

        signals = [self._make_signal()]
        report = CompetitorReportGenerator.generate_report(signals)
        assert report["total_signals"] == 1
        assert "epicor" in report["competitors"]
        assert report["competitors"]["epicor"]["name"] == "Epicor"
        assert report["competitors"]["epicor"]["total_signals"] == 1

    def test_multiple_competitors_sorted_by_count(self):
        """Competitors are sorted by signal count descending."""
        from agents.intelligence.competitor_signal_miner import CompetitorReportGenerator

        signals = [
            self._make_signal("SAP", "sap"),
            self._make_signal("Epicor", "epicor"),
            self._make_signal("Epicor", "epicor"),
            self._make_signal("Epicor", "epicor"),
            self._make_signal("SAP", "sap"),
        ]
        report = CompetitorReportGenerator.generate_report(signals)
        assert report["total_signals"] == 5
        keys = list(report["competitors"].keys())
        # Epicor (3) should come before SAP (2)
        assert keys[0] == "epicor"
        assert keys[1] == "sap"

    def test_associations_events_companies_tracked(self):
        """Report tracks unique associations, events, and companies per competitor."""
        from agents.intelligence.competitor_signal_miner import CompetitorReportGenerator

        signals = [
            self._make_signal(
                source_association="PMA",
                source_event_id="evt-1",
                source_company_id="comp-1",
            ),
            self._make_signal(
                source_association="NEMA",
                source_event_id="evt-2",
                source_company_id="comp-1",
            ),
            self._make_signal(
                source_association="PMA",
                source_event_id="evt-1",
                source_company_id="comp-2",
            ),
        ]
        report = CompetitorReportGenerator.generate_report(signals)
        epicor = report["competitors"]["epicor"]
        assert set(epicor["associations_present"]) == {"PMA", "NEMA"}
        assert epicor["events_present"] == 2  # evt-1, evt-2
        assert epicor["companies_using"] == 2  # comp-1, comp-2

    def test_signal_types_counted(self):
        """Report counts signals by type per competitor."""
        from agents.intelligence.competitor_signal_miner import CompetitorReportGenerator

        signals = [
            self._make_signal(signal_type="SPONSOR"),
            self._make_signal(signal_type="SPONSOR"),
            self._make_signal(signal_type="EXHIBITOR"),
        ]
        report = CompetitorReportGenerator.generate_report(signals)
        epicor = report["competitors"]["epicor"]
        assert epicor["signal_types"]["SPONSOR"] == 2
        assert epicor["signal_types"]["EXHIBITOR"] == 1

    def test_generated_at_timestamp_present(self):
        """Report includes a generated_at ISO timestamp."""
        from agents.intelligence.competitor_signal_miner import CompetitorReportGenerator

        signals = [self._make_signal()]
        report = CompetitorReportGenerator.generate_report(signals)
        assert "generated_at" in report
        # Should be a parseable ISO string
        from datetime import datetime

        datetime.fromisoformat(report["generated_at"])


# =============================================================================
# 9. ERROR HANDLING TESTS
# =============================================================================


class TestErrorHandling:
    """Tests for error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_network_error_on_url_fetch(self, miner):
        """Network errors during URL fetch are handled gracefully."""
        miner.http.get = AsyncMock(side_effect=TimeoutError("Connection timed out"))

        result = await miner.run({"url": "https://slow-site.example.com"})
        # Should not raise; returns error response
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_no_content_after_failed_fetch_returns_error(self, miner):
        """When fetch fails and no html/text fallback, returns no-content error."""
        miner.http.get = AsyncMock(side_effect=Exception("Connection refused"))

        result = await miner.run({"url": "https://down.example.com"})
        assert result["success"] is False
        assert result["error"] == "No content to scan"
        assert result["records_processed"] == 0

    @pytest.mark.asyncio
    async def test_max_signals_limit_respected(self, miner_max_signals):
        """Signal collection stops at max_signals limit."""
        # Create text with many competitor mentions to exceed limit of 3
        text = " ".join(["Epicor"] * 10 + ["SAP"] * 10 + ["Infor"] * 10)
        result = await miner_max_signals.run({"text": text})
        assert result["success"] is True
        assert len(result["signals"]) <= 3

    @pytest.mark.asyncio
    async def test_non_200_response_yields_no_content(self, miner):
        """HTTP response with non-200 status does not set html content."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        miner.http.get = AsyncMock(return_value=mock_response)

        result = await miner.run({"url": "https://blocked.example.com"})
        assert result["success"] is False
        assert "No content" in result["error"]

    @pytest.mark.asyncio
    async def test_context_ellipsis_prefix_when_not_at_start(self, miner_small_context):
        """Context gets '...' prefix when match is not at the start of text."""
        padding_left = "word " * 40   # 200 chars, ends with space
        padding_right = " word" * 40  # 200 chars, starts with space
        text = padding_left + "Epicor" + padding_right
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner_small_context._mine_signals(
            text, "http://test.com", None, None, None, prov
        )
        assert len(signals) >= 1
        assert signals[0].context.startswith("...")

    @pytest.mark.asyncio
    async def test_context_ellipsis_suffix_when_not_at_end(self, miner_small_context):
        """Context gets '...' suffix when match is not at the end of text."""
        padding_left = "word " * 40   # 200 chars, ends with space
        padding_right = " word" * 40  # 200 chars, starts with space
        text = padding_left + "Epicor" + padding_right
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner_small_context._mine_signals(
            text, "http://test.com", None, None, None, prov
        )
        assert len(signals) >= 1
        assert signals[0].context.endswith("...")


# =============================================================================
# 10. ADDITIONAL EDGE CASE TESTS
# =============================================================================


class TestEdgeCases:
    """Additional edge case and integration-like tests."""

    @pytest.mark.asyncio
    async def test_source_company_and_event_ids_propagated(self, miner):
        """source_company_id and source_event_id are set on each signal."""
        result = await miner.run({
            "text": "Epicor is our ERP",
            "source_company_id": "comp-abc",
            "source_event_id": "evt-xyz",
            "association": "PMA",
        })
        assert result["success"] is True
        signal = result["signals"][0]
        if isinstance(signal, dict):
            assert signal["source_company_id"] == "comp-abc"
            assert signal["source_event_id"] == "evt-xyz"
            assert signal["source_association"] == "PMA"

    @pytest.mark.asyncio
    async def test_dynamics_365_alias_detected(self, miner):
        """The alias 'Dynamics 365' is detected under 'microsoft dynamics'."""
        result = await miner.run({"text": "Migrating to Dynamics 365 this year."})
        assert result["success"] is True
        normalized = {
            (s["competitor_normalized"] if isinstance(s, dict) else s.competitor_normalized)
            for s in result["signals"]
        }
        assert "microsoft dynamics" in normalized

    @pytest.mark.asyncio
    async def test_html_strips_tags_before_scanning(self, miner):
        """HTML tags are stripped, leaving only text for scanning."""
        html = '<p class="erp-note">We implemented <strong>SAP S/4HANA</strong> last year.</p>'
        result = await miner.run({"html": html})
        assert result["success"] is True
        sap_signals = [
            s for s in result["signals"]
            if (s["competitor_normalized"] if isinstance(s, dict) else s.competitor_normalized) == "sap"
        ]
        assert len(sap_signals) >= 1

    @pytest.mark.asyncio
    async def test_classification_from_full_text_fallback(self, miner):
        """When context has no signal-type keywords, full_text[:2000] is checked."""
        # Put the "sponsor" keyword far from the match but within first 2000 chars
        text = "Our sponsorship program includes Epicor as a key partner."
        from models.ontology import Provenance

        prov = Provenance(source_url="http://test.com", extracted_by="test")
        signals = miner._mine_signals(text, "http://test.com", None, None, None, prov)
        # The "sponsorship" keyword is in context, so should classify as SPONSOR
        sponsor_signals = [
            s for s in signals
            if s.signal_type == CompetitorSignalType.SPONSOR
        ]
        assert len(sponsor_signals) >= 1

    @pytest.mark.asyncio
    async def test_competitor_name_title_cased(self, miner):
        """competitor_name field is title-cased (e.g., 'Epicor', 'Sap')."""
        result = await miner.run({"text": "epicor erp implementation"})
        assert result["success"] is True
        signal = result["signals"][0]
        name = signal["competitor_name"] if isinstance(signal, dict) else signal.competitor_name
        assert name == name.title()
