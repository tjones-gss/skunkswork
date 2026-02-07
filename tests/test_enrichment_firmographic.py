"""
Tests for agents/enrichment/firmographic.py - FirmographicAgent

Tests firmographic enrichment via Clearbit, Apollo, ZoomInfo APIs and website scraping.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestFirmographicAgentInitialization:
    """Tests for FirmographicAgent initialization."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_default_providers(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent loads default providers from config."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {
                    "providers": ["clearbit", "apollo"]
                }
            }
        }
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent.providers == ["clearbit", "apollo"]

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_batch_size(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent loads batch_size from config."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {
                    "batch_size": 100
                }
            }
        }
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent.batch_size == 100

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_skip_if_exists_default(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent defaults skip_if_exists to True."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent.skip_if_exists is True


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestFirmographicAgentRun:
    """Tests for run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_empty_records_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() returns error for empty records list."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent.run({"records": []})

        assert result["success"] is False
        assert "No records provided" in result["error"]
        assert result["records"] == []
        assert result["records_processed"] == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_missing_records_key(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() handles missing records key."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent.run({})

        assert result["success"] is False
        assert "No records provided" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_skips_already_enriched(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() skips records with existing employee_count_min."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [{"company_name": "Acme", "employee_count_min": 100}]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert result["records"][0]["employee_count_min"] == 100
        # match_rate should be 0 since skipped records don't count as matched
        assert result["match_rate"] == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_skips_missing_domain_and_company(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() skips records without domain or company_name."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [{"city": "Detroit", "state": "MI"}]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert len(result["records"]) == 1
        assert result["match_rate"] == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_enriches_records(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() enriches records with firmographic data."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {"providers": ["clearbit"]}
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "metrics": {"employees": 250},
            "foundedYear": 1985
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [
            {"company_name": "Acme", "website": "https://acme.com"}
        ]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert result["records"][0]["employee_count_min"] == 250
        assert result["match_rate"] == 1.0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_calculates_match_rate(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() correctly calculates match_rate."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {"providers": ["clearbit"]}
            }
        }

        # First call succeeds, second fails
        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.json.return_value = {"metrics": {"employees": 100}}

        mock_fail = MagicMock()
        mock_fail.status_code = 404

        mock_http.return_value.get = AsyncMock(
            side_effect=[mock_success, mock_fail]
        )

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [
            {"company_name": "Acme", "website": "https://acme.com"},
            {"company_name": "Beta", "website": "https://beta.com"},
        ]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert result["match_rate"] == 0.5  # 1 out of 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_merges_firmographic_data(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() merges firmographic data with existing record."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {"providers": ["clearbit"]}
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "metrics": {"employees": 250},
            "category": {"naicsCode": "332710"}
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [
            {"company_name": "Acme", "website": "https://acme.com", "city": "Detroit"}
        ]
        result = await agent.run({"records": records})

        # Original fields preserved
        assert result["records"][0]["city"] == "Detroit"
        assert result["records"][0]["company_name"] == "Acme"
        # New fields added
        assert result["records"][0]["employee_count_min"] == 250
        assert result["records"][0]["naics_code"] == "332710"


# =============================================================================
# TEST CLEARBIT API
# =============================================================================


class TestFirmographicAgentClearbit:
    """Tests for Clearbit API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_success_full_response(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        clearbit_response, mock_api_keys
    ):
        """Clearbit returns all firmographic fields on success."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {"providers": ["clearbit"]}
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = clearbit_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_clearbit("acme-mfg.com")

        assert result["employee_count_min"] == 250
        assert result["employee_count_max"] == 250
        assert result["year_founded"] == 1985
        assert result["naics_code"] == "332710"
        assert result["industry"] == "Manufacturing"
        assert result["firmographic_source"] == "clearbit"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_builds_linkedin_url(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        clearbit_response, mock_api_keys
    ):
        """Clearbit builds LinkedIn URL from handle."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = clearbit_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_clearbit("acme-mfg.com")

        assert result["linkedin_url"] == "https://linkedin.com/company/acme-mfg"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_missing_metrics(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Clearbit handles missing metrics gracefully."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "Acme",
            "foundedYear": 1985
            # No metrics key
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_clearbit("acme.com")

        assert result["employee_count_min"] is None
        assert result["year_founded"] == 1985

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Clearbit 404 returns None."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_clearbit("nonexistent.xyz")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """Clearbit returns None without API key."""
        mock_config.return_value.load.return_value = {}

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_clearbit("acme.com")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_exception_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Clearbit exception returns None."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(
            side_effect=Exception("Network error")
        )

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_clearbit("acme.com")

        assert result is None


# =============================================================================
# TEST APOLLO API
# =============================================================================


class TestFirmographicAgentApollo:
    """Tests for Apollo API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_success(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        apollo_organization_response, mock_api_keys
    ):
        """Apollo returns firmographic data on success."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = apollo_organization_response
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_apollo("acme-mfg.com")

        assert result["employee_count_min"] == 250
        assert result["employee_count_max"] == 250
        assert result["year_founded"] == 1985
        assert result["industry"] == "Manufacturing"
        assert result["firmographic_source"] == "apollo"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_extracts_linkedin(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        apollo_organization_response, mock_api_keys
    ):
        """Apollo extracts LinkedIn URL directly."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = apollo_organization_response
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_apollo("acme-mfg.com")

        assert result["linkedin_url"] == "https://linkedin.com/company/acme-mfg"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_missing_organization(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo handles missing organization key."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}  # No organization key
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_apollo("acme.com")

        assert result["employee_count_min"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """Apollo returns None without API key."""
        mock_config.return_value.load.return_value = {}

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_apollo("acme.com")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo 404 returns None."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_apollo("nonexistent.xyz")

        assert result is None


# =============================================================================
# TEST ZOOMINFO API
# =============================================================================


class TestFirmographicAgentZoomInfo:
    """Tests for ZoomInfo API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_success(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        zoominfo_response, mock_api_keys
    ):
        """ZoomInfo returns firmographic data on success."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = zoominfo_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_zoominfo("Acme Manufacturing", "acme-mfg.com")

        assert result["employee_count_min"] == 250
        assert result["employee_count_max"] == 250
        assert result["year_founded"] == 1985
        assert result["naics_code"] == "332710"
        assert result["firmographic_source"] == "zoominfo"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_converts_revenue(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        zoominfo_response, mock_api_keys
    ):
        """ZoomInfo converts revenue from millions to USD."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = zoominfo_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_zoominfo("Acme", "acme.com")

        # 25 million -> 25,000,000
        assert result["revenue_min_usd"] == 25_000_000

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_handles_empty_data(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo handles empty data array gracefully."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        # Empty data raises IndexError which is caught, returning None
        result = await agent._fetch_zoominfo("Unknown Company")

        # Agent catches exception and returns None for empty results
        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """ZoomInfo returns None without API key."""
        mock_config.return_value.load.return_value = {}

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_zoominfo("Acme")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_with_domain(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo includes domain in params when provided."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [{"employeeCount": 100}]}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        await agent._fetch_zoominfo("Acme", "acme.com")

        # Verify domain was included in params
        call_kwargs = mock_http.return_value.get.call_args
        assert "domain" in call_kwargs.kwargs["params"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_null_revenue(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo handles null revenue (multiplies by 0)."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"employeeCount": 50, "revenueInMillions": None}]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._fetch_zoominfo("Small Co")

        assert result["revenue_min_usd"] == 0


# =============================================================================
# TEST WEBSITE SCRAPING
# =============================================================================


class TestFirmographicAgentWebscraping:
    """Tests for website scraping."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_about_page_with_employees(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Scrapes employee count from About page."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <h1>About Us</h1>
            <p>We have over 250 employees worldwide.</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result["employee_count_min"] == 250
        assert result["firmographic_source"] == "website"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_about_page_team_pattern(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Scrapes employee count from 'team of' pattern."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <p>Our dedicated team of 150 professionals serves customers globally.</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result["employee_count_min"] == 150

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_about_page_with_founded_year(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Scrapes founded year from About page."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <p>Founded in 1985, we are a leading manufacturer.</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result["year_founded"] == 1985

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_about_page_established_pattern(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Scrapes year from 'established' pattern."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <p>Established 1972, we have been serving customers for 50+ years.</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result["year_founded"] == 1972

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_about_page_with_revenue(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Scrapes revenue from About page."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <p>With annual revenue of $50 million, we continue to grow.</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result["revenue_min_usd"] == 50_000_000

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_about_page_revenue_billion(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Scrapes billion-dollar revenue."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <p>A $1.5 billion global enterprise.</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result["revenue_min_usd"] == 1_500_000_000

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_tries_multiple_paths(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Tries multiple About page paths."""
        mock_config.return_value.load.return_value = {}

        mock_404 = MagicMock()
        mock_404.status_code = 404

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.text = "<html><body><p>100 employees</p></body></html>"

        # First path fails, second succeeds
        mock_http.return_value.get = AsyncMock(
            side_effect=[mock_404, mock_200]
        )

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result["employee_count_min"] == 100
        assert mock_http.return_value.get.call_count == 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_all_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Returns None when all paths return 404."""
        mock_config.return_value.load.return_value = {}

        mock_404 = MagicMock()
        mock_404.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_404)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_no_data_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Returns None when page has no extractable data."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body><p>Contact us today!</p></body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        result = await agent._scrape_website("acme.com")

        assert result is None


# =============================================================================
# TEST PARSE ABOUT PAGE
# =============================================================================


class TestFirmographicAgentParseAboutPage:
    """Tests for _parse_about_page method."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_employees_with_comma(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses employee count with comma separator."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        html = "<p>We employ 1,500 employees worldwide.</p>"
        result = agent._parse_about_page(html)

        assert result["employee_count_min"] == 1500

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_employees_with_plus(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses employee count with plus suffix."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        html = "<p>With 500+ employees, we serve clients globally.</p>"
        result = agent._parse_about_page(html)

        assert result["employee_count_min"] == 500

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_rejects_invalid_employee_count(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Rejects employee counts outside valid range."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        # 0 employees - too low
        html = "<p>We have 0 employees.</p>"
        result = agent._parse_about_page(html)
        assert result is None or "employee_count_min" not in result

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_year_range_pattern(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses year from range pattern (1985 - present)."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        html = "<p>Serving customers 1985 - present.</p>"
        result = agent._parse_about_page(html)

        assert result["year_founded"] == 1985

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_rejects_future_year(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Rejects years in the future."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        html = "<p>Founded in 2050, a company of the future.</p>"
        result = agent._parse_about_page(html)

        assert result is None or "year_founded" not in result


# =============================================================================
# TEST REVENUE PARSER
# =============================================================================


class TestFirmographicAgentRevenueParser:
    """Tests for _parse_revenue method."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_10m(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses $10M revenue."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._parse_revenue("$10M") == 10_000_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_range(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses revenue range (takes first number)."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._parse_revenue("$10M-$50M") == 10_000_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_billion(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses $1B revenue."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._parse_revenue("$1B") == 1_000_000_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_thousand(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses $500K revenue."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._parse_revenue("$500K") == 500_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_decimal(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses decimal revenue."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._parse_revenue("$1.5B") == 1_500_000_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Returns None for invalid input."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._parse_revenue(None) is None
        assert agent._parse_revenue("") is None

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_no_suffix(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses revenue without suffix."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._parse_revenue("100") == 100


# =============================================================================
# TEST LINKEDIN URL BUILDER
# =============================================================================


class TestFirmographicAgentLinkedInBuilder:
    """Tests for _build_linkedin_url method."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_builds_linkedin_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Builds LinkedIn URL from handle."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._build_linkedin_url("acme-mfg") == "https://linkedin.com/company/acme-mfg"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_returns_none_for_empty(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Returns None for empty/None handle."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        assert agent._build_linkedin_url(None) is None
        assert agent._build_linkedin_url("") is None


# =============================================================================
# TEST PROVIDER FALLBACK
# =============================================================================


class TestFirmographicAgentProviderFallback:
    """Tests for provider fallback behavior."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_tries_providers_in_order(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Tries providers in configured order."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {
                    "providers": ["clearbit", "apollo"]
                }
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"metrics": {"employees": 100}}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        await agent.run({"records": records})

        # Should call clearbit first (GET)
        assert mock_http.return_value.get.call_count == 1
        # Should not call apollo (POST) since clearbit succeeded
        assert mock_http.return_value.post.call_count == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_falls_back_on_failure(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Falls back to next provider on failure."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {
                    "providers": ["clearbit", "apollo"]
                }
            }
        }

        mock_fail = MagicMock()
        mock_fail.status_code = 404

        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.json.return_value = {
            "organization": {"estimated_num_employees": 200}
        }

        mock_http.return_value.get = AsyncMock(return_value=mock_fail)
        mock_http.return_value.post = AsyncMock(return_value=mock_success)

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        # Should fall back to apollo
        assert result["records"][0]["firmographic_source"] == "apollo"
        assert result["records"][0]["employee_count_min"] == 200

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_stops_after_success(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Stops trying providers after success."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "firmographic": {
                    "providers": ["clearbit", "apollo", "zoominfo"]
                }
            }
        }

        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.json.return_value = {"metrics": {"employees": 100}}

        mock_http.return_value.get = AsyncMock(return_value=mock_success)
        mock_http.return_value.post = AsyncMock()

        from agents.enrichment.firmographic import FirmographicAgent

        agent = FirmographicAgent(agent_type="enrichment.firmographic")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        # Should only call clearbit (first provider)
        assert mock_http.return_value.get.call_count == 1
        # Should not call apollo or zoominfo
        assert mock_http.return_value.post.call_count == 0
