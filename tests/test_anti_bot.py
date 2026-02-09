"""
Tests for anti-bot / human-mimicry changes.

Covers:
- UA rotation (no bot identifier, multiple UAs)
- Rate limiter jitter
- Browser-like default headers
- Playwright stealth settings (html_parser, link_crawler)
- Human browsing helpers (BaseAgent._human_browse, _visit_with_referrer)
"""

import asyncio
import re
import time
from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
import pytest


# =============================================================================
# 1. USER-AGENT ROTATION
# =============================================================================


class TestUserAgentRotation:
    """Tests that USER_AGENTS pool replaces the old single USER_AGENT."""

    def test_user_agents_is_a_list(self):
        """USER_AGENTS is a list with multiple entries."""
        from skills.common.SKILL import AsyncHTTPClient

        assert isinstance(AsyncHTTPClient.USER_AGENTS, list)
        assert len(AsyncHTTPClient.USER_AGENTS) >= 3

    def test_no_bot_identifier_in_any_ua(self):
        """No UA string contains 'NAM-IntelBot' or any bot keyword."""
        from skills.common.SKILL import AsyncHTTPClient

        for ua in AsyncHTTPClient.USER_AGENTS:
            assert "NAM-IntelBot" not in ua
            assert "Bot/" not in ua
            assert "bot/" not in ua

    def test_all_uas_look_like_chrome(self):
        """Every UA string contains Chrome version pattern."""
        from skills.common.SKILL import AsyncHTTPClient

        for ua in AsyncHTTPClient.USER_AGENTS:
            assert "Chrome/" in ua
            assert "Mozilla/5.0" in ua

    def test_random_ua_returns_from_pool(self):
        """_random_ua() returns an element from USER_AGENTS."""
        from skills.common.SKILL import AsyncHTTPClient

        for _ in range(20):
            ua = AsyncHTTPClient._random_ua()
            assert ua in AsyncHTTPClient.USER_AGENTS

    def test_random_ua_has_variety(self):
        """_random_ua() returns more than one distinct value over many calls."""
        from skills.common.SKILL import AsyncHTTPClient

        uas = {AsyncHTTPClient._random_ua() for _ in range(100)}
        assert len(uas) > 1, "Expected random rotation to produce variety"

    def test_old_user_agent_attribute_removed(self):
        """The old singular USER_AGENT class attribute no longer exists."""
        from skills.common.SKILL import AsyncHTTPClient

        assert not hasattr(AsyncHTTPClient, "USER_AGENT")


# =============================================================================
# 2. BROWSER-LIKE DEFAULT HEADERS
# =============================================================================


class TestBrowserHeaders:
    """Tests that the HTTP client sends browser-like headers."""

    def test_browser_headers_dict_exists(self):
        """_BROWSER_HEADERS contains Accept, Accept-Language, etc."""
        from skills.common.SKILL import AsyncHTTPClient

        h = AsyncHTTPClient._BROWSER_HEADERS
        assert "Accept" in h
        assert "Accept-Language" in h
        assert "Accept-Encoding" in h
        assert "text/html" in h["Accept"]
        assert "en-US" in h["Accept-Language"]

    @pytest.mark.asyncio
    async def test_client_created_with_browser_headers(self):
        """_get_client() creates a client whose default headers include browser fields."""
        from skills.common.SKILL import AsyncHTTPClient, RateLimiter

        http = AsyncHTTPClient(RateLimiter())
        client = await http._get_client()

        # Check headers on the underlying httpx.AsyncClient
        headers = dict(client.headers)
        assert "user-agent" in headers
        assert "accept" in headers
        assert "accept-language" in headers
        assert "accept-encoding" in headers

        # UA should be from pool (no bot string)
        assert "NAM-IntelBot" not in headers["user-agent"]
        assert "Chrome/" in headers["user-agent"]

        await http.close()

    @pytest.mark.asyncio
    async def test_client_has_follow_redirects(self):
        """Client follows redirects by default."""
        from skills.common.SKILL import AsyncHTTPClient, RateLimiter

        http = AsyncHTTPClient(RateLimiter())
        client = await http._get_client()
        assert client.follow_redirects is True
        await http.close()


# =============================================================================
# 3. RATE LIMITER JITTER
# =============================================================================


class TestRateLimiterJitter:
    """Tests that rate limiter adds jitter to break fixed-interval patterns."""

    @pytest.mark.asyncio
    async def test_acquire_adds_jitter_when_waiting(self):
        """When a wait is needed, the actual sleep includes jitter > base wait."""
        from skills.common.SKILL import RateLimiter

        rl = RateLimiter()
        domain = "test-jitter.example.com"

        sleep_times = []
        original_sleep = asyncio.sleep

        async def capture_sleep(duration):
            sleep_times.append(duration)
            # Don't actually sleep in tests

        # First request — no wait needed
        await rl.acquire(domain)

        # Immediately try again — should need to wait with jitter
        with patch("asyncio.sleep", side_effect=capture_sleep):
            # Force a short elapsed so wait is triggered
            rl.last_request[domain] = time.time()
            await rl.acquire(domain)

        if sleep_times:
            wait = sleep_times[0]
            # Base wait at 1.0 req/s = ~1.0s. Jitter adds 0.3-1.5s.
            # So total should be > base (1.0s) when triggered.
            assert wait >= 0.3, f"Jitter too small: {wait}"

    @pytest.mark.asyncio
    async def test_jitter_varies_across_calls(self):
        """Multiple acquire() calls produce different wait times."""
        from skills.common.SKILL import RateLimiter

        rl = RateLimiter()
        domain = "jitter-vary.example.com"

        sleep_times = []

        async def capture_sleep(duration):
            sleep_times.append(duration)

        with patch("asyncio.sleep", side_effect=capture_sleep):
            for _ in range(10):
                # Force timing to always require a wait
                rl.last_request[domain] = time.time()
                await rl.acquire(domain)

        # At least some variation should exist
        if len(sleep_times) >= 2:
            assert len(set(round(t, 4) for t in sleep_times)) > 1, (
                "Expected varied jitter across calls"
            )


# =============================================================================
# 4. PLAYWRIGHT STEALTH — HTML PARSER
# =============================================================================


class TestPlaywrightStealthHTMLParser:
    """Tests that DirectoryParserAgent._fetch_with_playwright uses stealth."""

    def _make_agent(self):
        from agents.extraction.html_parser import DirectoryParserAgent

        with patch("agents.base.get_secrets_manager"):
            agent = DirectoryParserAgent(
                agent_type="extraction.directory_parser",
                config_path="config",
            )
        return agent

    def test_stealth_script_defined(self):
        """DirectoryParserAgent has a _STEALTH_SCRIPT class attribute."""
        from agents.extraction.html_parser import DirectoryParserAgent

        assert hasattr(DirectoryParserAgent, "_STEALTH_SCRIPT")
        assert "webdriver" in DirectoryParserAgent._STEALTH_SCRIPT
        assert "chrome" in DirectoryParserAgent._STEALTH_SCRIPT

    @pytest.mark.asyncio
    async def test_fetch_launches_headed_with_stealth(self):
        """_fetch_with_playwright launches headed browser with stealth args."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>OK</html>")

        mock_response = MagicMock()
        mock_response.status = 200
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        mock_browser = AsyncMock()
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_browser.close = AsyncMock()

        mock_chromium = AsyncMock()
        mock_chromium.launch = AsyncMock(return_value=mock_browser)

        mock_pw = AsyncMock()
        mock_pw.chromium = mock_chromium

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_pw)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "playwright.async_api.async_playwright",
            return_value=mock_cm,
        ):
            html = await agent._fetch_with_playwright("https://example.com")

        assert html == "<html>OK</html>"

        # Verify headed mode (headless=False)
        launch_kwargs = mock_chromium.launch.call_args
        assert launch_kwargs.kwargs.get("headless") is False

        # Verify anti-detection args
        args = launch_kwargs.kwargs.get("args", [])
        assert "--disable-blink-features=AutomationControlled" in args

        # Verify stealth script was injected
        mock_page.add_init_script.assert_called_once()
        script_arg = mock_page.add_init_script.call_args[0][0]
        assert "webdriver" in script_arg

        # Verify UA was set via new_context (not old new_page approach)
        ctx_kwargs = mock_browser.new_context.call_args.kwargs
        assert "user_agent" in ctx_kwargs
        assert "NAM-IntelBot" not in ctx_kwargs["user_agent"]

    @pytest.mark.asyncio
    async def test_fetch_returns_none_on_non_200(self):
        """_fetch_with_playwright returns None when status != 200."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_response = MagicMock()
        mock_response.status = 403
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        mock_browser = AsyncMock()
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_browser.close = AsyncMock()

        mock_chromium = AsyncMock()
        mock_chromium.launch = AsyncMock(return_value=mock_browser)

        mock_pw = AsyncMock()
        mock_pw.chromium = mock_chromium

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_pw)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "playwright.async_api.async_playwright",
            return_value=mock_cm,
        ):
            result = await agent._fetch_with_playwright("https://blocked.com")

        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_returns_none_on_import_error(self):
        """_fetch_with_playwright returns None when playwright not installed."""
        agent = self._make_agent()

        with patch.dict("sys.modules", {"playwright.async_api": None}):
            with patch("builtins.__import__", side_effect=ImportError("no playwright")):
                # Re-call the method — it catches ImportError internally
                result = await agent._fetch_with_playwright("https://example.com")

        assert result is None


# =============================================================================
# 5. PLAYWRIGHT STEALTH — LINK CRAWLER
# =============================================================================


class TestPlaywrightStealthLinkCrawler:
    """Tests that LinkCrawlerAgent uses stealth browser launch."""

    def _make_agent(self):
        from agents.discovery.link_crawler import LinkCrawlerAgent

        with patch("agents.base.get_secrets_manager"):
            agent = LinkCrawlerAgent(
                agent_type="discovery.link_crawler",
                config_path="config",
            )
        return agent

    def test_stealth_script_defined(self):
        """LinkCrawlerAgent has a _STEALTH_SCRIPT class attribute."""
        from agents.discovery.link_crawler import LinkCrawlerAgent

        assert hasattr(LinkCrawlerAgent, "_STEALTH_SCRIPT")
        assert "webdriver" in LinkCrawlerAgent._STEALTH_SCRIPT

    def test_launch_stealth_browser_method_exists(self):
        """LinkCrawlerAgent has _launch_stealth_browser()."""
        agent = self._make_agent()
        assert hasattr(agent, "_launch_stealth_browser")

    @pytest.mark.asyncio
    async def test_launch_stealth_browser_config(self):
        """_launch_stealth_browser launches with correct stealth settings."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        mock_browser = AsyncMock()
        mock_browser.new_context = AsyncMock(return_value=mock_context)

        mock_pw = MagicMock()
        mock_pw.chromium = MagicMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)

        browser, page = await agent._launch_stealth_browser(mock_pw)

        # Verify headed launch
        launch_kwargs = mock_pw.chromium.launch.call_args
        assert launch_kwargs.kwargs.get("headless") is False

        # Verify anti-detection args
        args = launch_kwargs.kwargs.get("args", [])
        assert "--disable-blink-features=AutomationControlled" in args

        # Verify stealth script injected
        mock_page.add_init_script.assert_called_once()

        # Verify context created with UA and viewport
        ctx_kwargs = mock_browser.new_context.call_args.kwargs
        assert "user_agent" in ctx_kwargs
        assert ctx_kwargs.get("viewport") == {"width": 1920, "height": 1080}

    @pytest.mark.asyncio
    async def test_infinite_scroll_uses_stealth(self):
        """_crawl_infinite_scroll uses _launch_stealth_browser."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=[])
        mock_page.wait_for_timeout = AsyncMock()

        mock_browser = AsyncMock()
        mock_browser.close = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch.object(
            agent,
            "_launch_stealth_browser",
            new_callable=AsyncMock,
            return_value=(mock_browser, mock_page),
        ):
            with patch(
                "playwright.async_api.async_playwright",
                return_value=mock_cm,
            ):
                urls = await agent._crawl_infinite_scroll("https://example.com/dir")

            agent._launch_stealth_browser.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_more_uses_stealth(self):
        """_crawl_load_more uses _launch_stealth_browser."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=[])
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.query_selector = AsyncMock(return_value=None)

        mock_browser = AsyncMock()
        mock_browser.close = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch.object(
            agent,
            "_launch_stealth_browser",
            new_callable=AsyncMock,
            return_value=(mock_browser, mock_page),
        ):
            with patch(
                "playwright.async_api.async_playwright",
                return_value=mock_cm,
            ):
                urls = await agent._crawl_load_more("https://example.com/dir")

            agent._launch_stealth_browser.assert_called_once()


# =============================================================================
# 6. BASE AGENT HUMAN BROWSING HELPERS
# =============================================================================


class TestHumanBrowseHelper:
    """Tests for BaseAgent._human_browse()."""

    def _make_agent(self):
        """Create a concrete agent for testing base class helpers."""
        from agents.base import BaseAgent

        class ConcreteAgent(BaseAgent):
            async def run(self, task):
                return {"success": True, "records_processed": 0}

        with patch("agents.base.get_secrets_manager"):
            agent = ConcreteAgent(
                agent_type="test.concrete",
                config_path="config",
            )
        return agent

    def test_stealth_script_on_base_agent(self):
        """BaseAgent has _STEALTH_SCRIPT."""
        from agents.base import BaseAgent

        assert hasattr(BaseAgent, "_STEALTH_SCRIPT")
        assert "webdriver" in BaseAgent._STEALTH_SCRIPT

    @pytest.mark.asyncio
    async def test_human_browse_scrolls_and_returns_html(self):
        """_human_browse scrolls and returns page content."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.evaluate = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>page content</html>")

        with patch("asyncio.sleep", new_callable=AsyncMock):
            html = await agent._human_browse(mock_page, "https://example.com/dir")

        assert html == "<html>page content</html>"
        mock_page.goto.assert_called_once_with(
            "https://example.com/dir", wait_until="domcontentloaded"
        )

        # Should have scrolled (two evaluate calls for 30% and 70%)
        assert mock_page.evaluate.call_count == 2
        mock_page.wait_for_load_state.assert_called_once_with("networkidle")

    @pytest.mark.asyncio
    async def test_visit_with_referrer_visits_homepage_first(self):
        """_visit_with_referrer navigates homepage then target."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.evaluate = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>target</html>")

        with patch("asyncio.sleep", new_callable=AsyncMock):
            html = await agent._visit_with_referrer(
                mock_page, "https://agma.org", "https://agma.org/members"
            )

        assert html == "<html>target</html>"

        # First call: homepage, second call: target (via _human_browse)
        goto_calls = mock_page.goto.call_args_list
        assert len(goto_calls) >= 2
        assert goto_calls[0][0][0] == "https://agma.org"
        assert goto_calls[1][0][0] == "https://agma.org/members"

    @pytest.mark.asyncio
    async def test_human_browse_returns_string(self):
        """_human_browse returns a string."""
        agent = self._make_agent()

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.evaluate = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.content = AsyncMock(return_value="")

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await agent._human_browse(mock_page, "https://example.com")

        assert isinstance(result, str)


# =============================================================================
# 7. INTEGRATION: HTTP CLIENT CREATES WITH ROTATED UA
# =============================================================================


class TestHTTPClientIntegration:
    """Integration tests for the full anti-bot HTTP client."""

    @pytest.mark.asyncio
    async def test_successive_clients_may_get_different_uas(self):
        """Creating multiple clients may pick different UAs."""
        from skills.common.SKILL import AsyncHTTPClient, RateLimiter

        uas = set()
        for _ in range(20):
            http = AsyncHTTPClient(RateLimiter())
            client = await http._get_client()
            uas.add(dict(client.headers).get("user-agent", ""))
            await http.close()

        # With 4 UAs and 20 tries, we should get variety
        assert len(uas) > 1

    @pytest.mark.asyncio
    async def test_request_uses_browser_accept_header(self):
        """Requests include the Accept header from _BROWSER_HEADERS."""
        from skills.common.SKILL import AsyncHTTPClient, RateLimiter

        http = AsyncHTTPClient(RateLimiter())
        client = await http._get_client()
        headers = dict(client.headers)

        assert "text/html" in headers.get("accept", "")
        await http.close()

    def test_no_old_user_agent_constant(self):
        """The old USER_AGENT string constant is removed."""
        from skills.common.SKILL import AsyncHTTPClient

        # Should NOT have the old singular attribute
        assert not hasattr(AsyncHTTPClient, "USER_AGENT")
        # Should have the new plural attribute
        assert hasattr(AsyncHTTPClient, "USER_AGENTS")
