"""
Tests for AccessGatekeeperAgent
NAM Intelligence Pipeline

Tests for the access gatekeeper agent that verifies legal and ethical access
before crawling websites, checking robots.txt, ToS, and authentication requirements.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestAccessGatekeeperAgentInitialization:
    """Tests for AccessGatekeeperAgent initialization."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_default_crawl_delay(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test default crawl delay value."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        assert agent.default_crawl_delay == 2.0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_default_daily_limit(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test default daily limit value."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        assert agent.default_daily_limit == 1000

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_default_false(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test ToS checking disabled by default."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        assert agent.check_tos is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_custom_config_values(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test custom configuration values."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        # Set config directly and re-run setup
        agent.agent_config = {
            "default_crawl_delay": 5.0,
            "default_daily_limit": 500,
            "check_tos": True
        }
        agent._setup()

        assert agent.default_crawl_delay == 5.0
        assert agent.default_daily_limit == 500
        assert agent.check_tos is True


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestAccessGatekeeperAgentRun:
    """Tests for AccessGatekeeperAgent.run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_with_url_input(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test running with URL input."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({"url": "https://pma.org/members"})

        assert result["success"] is True
        assert "verdict" in result
        assert "is_allowed" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_with_domain_input(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test running with domain input."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({"domain": "pma.org"})

        assert result["success"] is True
        assert "pma.org" in result["verdict"]["domain"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_missing_url_and_domain(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test error when both URL and domain are missing."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({})

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_allowed_site(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test allowed site verdict."""
        mock_config.return_value.load.return_value = {}

        # robots.txt allows all
        robots_response = MagicMock()
        robots_response.status_code = 200
        robots_response.text = "User-agent: *\nAllow: /"

        # Page is public
        page_response = MagicMock()
        page_response.status_code = 200
        page_response.text = "<html><body><h1>Member Directory</h1></body></html>"

        mock_http.return_value.get = AsyncMock(
            side_effect=[robots_response, page_response]
        )

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": True
        })

        assert result["success"] is True
        assert result["is_allowed"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_blocked_by_robots(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test blocked by robots.txt verdict."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nDisallow: /members"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": False
        })

        assert result["success"] is True
        assert result["is_allowed"] is False
        assert any("robots.txt" in r for r in result["reasons"])

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_blocked_by_auth(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        login_required_page_html
    ):
        """Test blocked by authentication requirement."""
        mock_config.return_value.load.return_value = {}

        robots_response = MagicMock()
        robots_response.status_code = 200
        robots_response.text = "User-agent: *\nAllow: /"

        page_response = MagicMock()
        page_response.status_code = 200
        page_response.text = login_required_page_html

        mock_http.return_value.get = AsyncMock(
            side_effect=[robots_response, page_response]
        )

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": True
        })

        assert result["success"] is True
        assert result["is_allowed"] is False
        assert any("auth" in r.lower() for r in result["reasons"])

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_suggested_rate(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that suggested rate is returned."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /\nCrawl-delay: 5"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": False
        })

        assert "suggested_rate" in result
        # Rate should be 1/5 = 0.2 for 5 second crawl delay
        assert result["suggested_rate"] == 0.2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_daily_limit(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that daily limit is returned."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": False
        })

        assert "daily_limit" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_records_processed(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that records_processed is returned."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": False
        })

        assert result["records_processed"] == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_with_tos_check_enabled(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        tos_without_restrictions
    ):
        """Test running with ToS check enabled."""
        mock_config.return_value.load.return_value = {}

        robots_response = MagicMock()
        robots_response.status_code = 200
        robots_response.text = "User-agent: *\nAllow: /"

        tos_response = MagicMock()
        tos_response.status_code = 200
        tos_response.text = tos_without_restrictions

        mock_http.return_value.get = AsyncMock(
            side_effect=[robots_response, tos_response]
        )

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        # Enable ToS check via agent_config and re-setup
        agent.agent_config = {"check_tos": True}
        agent._setup()

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": False
        })

        assert result["success"] is True
        assert result["verdict"]["tos_reviewed"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_positive_reasons_when_allowed(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test positive reasons are included when allowed."""
        mock_config.return_value.load.return_value = {}

        robots_response = MagicMock()
        robots_response.status_code = 200
        robots_response.text = "User-agent: *\nAllow: /"

        page_response = MagicMock()
        page_response.status_code = 200
        page_response.text = "<html><body>Public content</body></html>"

        mock_http.return_value.get = AsyncMock(
            side_effect=[robots_response, page_response]
        )

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": True
        })

        assert result["is_allowed"] is True
        assert any("robots.txt allows" in r for r in result["reasons"])
        assert any("authentication" in r.lower() for r in result["reasons"])

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_no_robots_txt_assumes_allowed(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that missing robots.txt assumes allowed."""
        mock_config.return_value.load.return_value = {}

        robots_response = MagicMock()
        robots_response.status_code = 404

        page_response = MagicMock()
        page_response.status_code = 200
        page_response.text = "<html><body>Content</body></html>"

        mock_http.return_value.get = AsyncMock(
            side_effect=[robots_response, page_response]
        )

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "url": "https://pma.org/members",
            "check_page": True
        })

        assert result["is_allowed"] is True
        assert any("No robots.txt" in r for r in result["reasons"])


# =============================================================================
# TEST CHECK ROBOTS.TXT
# =============================================================================


class TestAccessGatekeeperAgentCheckRobotsTxt:
    """Tests for _check_robots_txt() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_success(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test successful robots.txt parsing."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /members\nDisallow: /private"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        assert result["exists"] is True
        assert result["allows"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_user_agent_specific(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test robots.txt with user agent specific rules that override wildcard."""
        mock_config.return_value.load.return_value = {}

        # Both NAM-IntelBot and * block /private, so the agent cannot access it
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
User-agent: NAM-IntelBot
Disallow: /private

User-agent: *
Disallow: /private
"""
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/private"
        )

        assert result["exists"] is True
        # Should be blocked - both NAM-IntelBot and * disallow /private
        assert result["allows"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_fallback_to_wildcard(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test falling back to * user agent when specific not found."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nDisallow: /admin"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        assert result["allows"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_crawl_delay(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test extracting crawl delay from robots.txt."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /\nCrawl-delay: 10"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        assert result["crawl_delay"] == 10

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_extracts_sitemaps(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test extracting sitemap URLs from robots.txt."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
User-agent: *
Allow: /
Sitemap: https://example.com/sitemap.xml
Sitemap: https://example.com/sitemap-news.xml
"""
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        assert len(result["sitemaps"]) == 2
        assert "https://example.com/sitemap.xml" in result["sitemaps"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_404_allows(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that 404 robots.txt allows access."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        assert result["exists"] is False
        assert result["allows"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_exception_allows(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that exceptions during fetch allow access."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(side_effect=Exception("Network error"))

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        assert result["exists"] is False
        assert result["allows"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_disallow_specific_path(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test disallowing specific path."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nDisallow: /members"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        assert result["exists"] is True
        assert result["allows"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_robots_txt_logs_result(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that robots.txt result is logged."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        await agent._check_robots_txt(
            "https://example.com",
            "https://example.com/members"
        )

        agent.log.debug.assert_called()


# =============================================================================
# TEST CHECK PAGE
# =============================================================================


class TestAccessGatekeeperAgentCheckPage:
    """Tests for _check_page() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_401_requires_auth(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting 401 Unauthorized."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/members")

        assert result["requires_auth"] is True
        assert result["auth_type"] == "http_auth"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_403_requires_auth(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting 403 Forbidden."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/members")

        assert result["requires_auth"] is True
        assert result["auth_type"] == "http_auth"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_login_form(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting login form on page."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <form action="/login">
                <input type="email" name="email">
                <input type="password" name="password">
            </form>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/members")

        assert result["requires_auth"] is True
        assert result["auth_type"] == "login"
        assert "login form" in result["auth_indicator"].lower()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_auth_text_indicators(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting auth from text indicators."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent

        test_cases = [
            "Please log in to continue",
            "Sign in to view this content",
            "Members only area",
            "Login required",
            "You must be logged in",
        ]

        for text in test_cases:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = f"<html><body><p>{text}</p></body></html>"
            mock_http.return_value.get = AsyncMock(return_value=mock_response)

            agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")
            result = await agent._check_page("https://example.com/members")

            assert result["requires_auth"] is True, f"Failed for: {text}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_paywall_indicators(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        paywall_page_html
    ):
        """Test detecting paywall indicators."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = paywall_page_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/premium")

        assert result["is_paywall"] is True
        assert result["auth_type"] == "paywall"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_public_content(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting public content (no auth required)."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <h1>Member Directory</h1>
            <div class="member-list">
                <div>Company A</div>
                <div>Company B</div>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/members")

        assert result["requires_auth"] is False
        assert result["is_paywall"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_tracks_status_code(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that status code is tracked."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Content</body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/members")

        assert result["status_code"] == 200

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_handles_exception(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling exceptions during page check."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(side_effect=Exception("Error"))

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/members")

        assert result["requires_auth"] is False
        assert result["is_paywall"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_page_non_200_returns_early(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that non-200/401/403 returns early."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_page("https://example.com/members")

        assert result["status_code"] == 500
        assert result["requires_auth"] is False


# =============================================================================
# TEST CHECK TOS
# =============================================================================


class TestAccessGatekeeperAgentCheckTos:
    """Tests for _check_tos() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_finds_tos_page(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        tos_without_restrictions
    ):
        """Test finding and reviewing ToS page."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = tos_without_restrictions
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_tos("https://example.com")

        assert result["reviewed"] is True
        assert result["allows_crawling"] is True
        assert result["restricts_crawling"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_detects_restriction(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        tos_with_scraping_restriction
    ):
        """Test detecting ToS with scraping restrictions."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = tos_with_scraping_restriction
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_tos("https://example.com")

        assert result["reviewed"] is True
        assert result["restricts_crawling"] is True
        assert result["allows_crawling"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_tries_multiple_patterns(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        tos_without_restrictions
    ):
        """Test trying multiple ToS URL patterns."""
        mock_config.return_value.load.return_value = {}

        # First few patterns return 404, last one succeeds
        not_found = MagicMock()
        not_found.status_code = 404

        found = MagicMock()
        found.status_code = 200
        found.text = tos_without_restrictions

        mock_http.return_value.get = AsyncMock(
            side_effect=[not_found, not_found, not_found, found]
        )

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_tos("https://example.com")

        assert result["reviewed"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_no_tos_found(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling when no ToS page is found."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_tos("https://example.com")

        assert result["reviewed"] is False
        assert result["allows_crawling"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_handles_exception(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling exceptions during ToS check."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(side_effect=Exception("Error"))

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_tos("https://example.com")

        assert result["reviewed"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_data_mining_restriction(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting data mining restriction."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body><p>Data mining is prohibited on this site.</p></body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_tos("https://example.com")

        assert result["restricts_crawling"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_automated_access_restriction(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting automated access restriction."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body><p>Automated access and collection is prohibited.</p></body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent._check_tos("https://example.com")

        assert result["restricts_crawling"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_check_tos_logs_warning_on_restriction(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        tos_with_scraping_restriction
    ):
        """Test that restrictions are logged as warnings."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = tos_with_scraping_restriction
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        await agent._check_tos("https://example.com")

        agent.log.warning.assert_called()


# =============================================================================
# TEST GET DAILY LIMIT
# =============================================================================


class TestAccessGatekeeperAgentGetDailyLimit:
    """Tests for _get_daily_limit() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_get_daily_limit_configured_domain(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test getting configured limit for specific domain."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        # Set daily_limits config directly
        agent.agent_config = {
            "daily_limits": {
                "pma.org": 500,
                "nema.org": 750
            }
        }

        limit = agent._get_daily_limit("pma.org")

        assert limit == 500

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_get_daily_limit_subdomain_matching(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test matching subdomain to parent domain limit."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        # Set daily_limits config directly
        agent.agent_config = {
            "daily_limits": {
                "pma.org": 500
            }
        }

        limit = agent._get_daily_limit("members.pma.org")

        assert limit == 500

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_get_daily_limit_default(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test getting default limit for unknown domain."""
        mock_config.return_value.load.return_value = {
            "default_daily_limit": 1000,
            "daily_limits": {"pma.org": 500}
        }

        from agents.discovery.access_gatekeeper import AccessGatekeeperAgent
        agent = AccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        limit = agent._get_daily_limit("unknown.com")

        assert limit == 1000


# =============================================================================
# TEST BATCH ACCESS GATEKEEPER
# =============================================================================


class TestBatchAccessGatekeeperAgentRun:
    """Tests for BatchAccessGatekeeperAgent.run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_with_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run with multiple URLs."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "urls": [
                "https://pma.org/members",
                "https://nema.org/directory"
            ],
            "check_pages": False
        })

        assert result["success"] is True
        assert len(result["verdicts"]) == 2
        assert "summary" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_with_domains(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run with domain input."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "domains": ["pma.org", "nema.org"],
            "check_pages": False
        })

        assert result["success"] is True
        assert len(result["verdicts"]) == 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_empty_input(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run with empty input."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({"urls": [], "domains": []})

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_summary_counts(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run produces correct summary counts."""
        mock_config.return_value.load.return_value = {}

        # First URL allowed, second blocked
        allowed_response = MagicMock()
        allowed_response.status_code = 200
        allowed_response.text = "User-agent: *\nAllow: /"

        blocked_response = MagicMock()
        blocked_response.status_code = 200
        blocked_response.text = "User-agent: *\nDisallow: /"

        mock_http.return_value.get = AsyncMock(
            side_effect=[allowed_response, blocked_response]
        )

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "urls": ["https://allowed.com", "https://blocked.com"],
            "check_pages": False
        })

        assert result["summary"]["total"] == 2
        assert result["summary"]["allowed"] == 1
        assert result["summary"]["blocked"] == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_allowed_urls_list(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run produces allowed URLs list."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "urls": ["https://pma.org", "https://nema.org"],
            "check_pages": False
        })

        assert "allowed_urls" in result
        assert len(result["allowed_urls"]) == 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_blocked_urls_with_reasons(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run includes reasons for blocked URLs."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nDisallow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "urls": ["https://blocked.com"],
            "check_pages": False
        })

        assert len(result["blocked_urls"]) == 1
        assert "url" in result["blocked_urls"][0]
        assert "reasons" in result["blocked_urls"][0]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_records_processed(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run returns correct records_processed count."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "urls": ["https://a.com", "https://b.com", "https://c.com"],
            "check_pages": False
        })

        assert result["records_processed"] == 3

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_logs_summary(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run logs summary."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nAllow: /"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        await agent.run({
            "urls": ["https://a.com", "https://b.com"],
            "check_pages": False
        })

        # Verify logging
        agent.log.info.assert_called()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_with_check_pages_enabled(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run with page checking enabled."""
        mock_config.return_value.load.return_value = {}

        robots_response = MagicMock()
        robots_response.status_code = 200
        robots_response.text = "User-agent: *\nAllow: /"

        page_response = MagicMock()
        page_response.status_code = 200
        page_response.text = "<html><body>Content</body></html>"

        mock_http.return_value.get = AsyncMock(
            side_effect=[robots_response, page_response, robots_response, page_response]
        )

        from agents.discovery.access_gatekeeper import BatchAccessGatekeeperAgent
        agent = BatchAccessGatekeeperAgent(agent_type="discovery.access_gatekeeper")

        result = await agent.run({
            "urls": ["https://a.com", "https://b.com"],
            "check_pages": True
        })

        assert result["success"] is True
        assert len(result["verdicts"]) == 2
