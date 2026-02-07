"""
Tests for agents/extraction/api_client.py - APIClientAgent

Tests external API interactions with mocked HTTP responses.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestAPIClientAgentInitialization:
    """Tests for APIClientAgent initialization."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_providers_config(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent loads providers from config."""
        mock_config.return_value.load.return_value = {
            "extraction": {
                "api_client": {
                    "providers": {
                        "clearbit": {"enabled": True},
                        "builtwith": {"enabled": True}
                    }
                }
            }
        }
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        assert hasattr(agent, "providers")

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_empty_cache(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent starts with empty cache."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        assert agent._cache == {}


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestAPIClientAgentRun:
    """Tests for run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_requires_domain_or_company_name(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() fails without domain or company_name."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({"provider": "clearbit"})

        assert result["success"] is False
        assert "No domain or company_name" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_cached_result(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() returns cached result if available."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")
        agent._cache["clearbit:acme.com"] = {"employee_count_min": 100}

        result = await agent.run({
            "provider": "clearbit",
            "domain": "acme.com"
        })

        assert result["success"] is True
        assert result["cached"] is True
        assert result["data"]["employee_count_min"] == 100

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_unknown_provider(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() fails for unknown provider."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "unknown_provider",
            "domain": "acme.com"
        })

        assert result["success"] is False
        assert "Unknown provider" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_defaults_to_clearbit(
        self, mock_limiter, mock_http, mock_logger, mock_config, monkeypatch
    ):
        """run() defaults to clearbit provider."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.setenv("CLEARBIT_API_KEY", "test-key")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "Acme",
            "metrics": {"employees": 100}
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({"domain": "acme.com"})

        assert result["provider"] == "clearbit"


# =============================================================================
# TEST CLEARBIT API
# =============================================================================


class TestAPIClientAgentClearbit:
    """Tests for Clearbit API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_success(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        clearbit_response, mock_api_keys
    ):
        """Clearbit API returns enriched data on success."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = clearbit_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "clearbit",
            "domain": "acme-mfg.com"
        })

        assert result["success"] is True
        assert result["data"]["employee_count_min"] == 250
        assert result["data"]["firmographic_source"] == "clearbit"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_extracts_linkedin_url(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        clearbit_response, mock_api_keys
    ):
        """Clearbit extracts LinkedIn URL from handle."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = clearbit_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "clearbit",
            "domain": "acme-mfg.com"
        })

        assert result["data"]["linkedin_url"] == "https://linkedin.com/company/acme-mfg"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """Clearbit returns None when API key not set."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "clearbit",
            "domain": "acme.com"
        })

        assert result["success"] is False
        assert result["data"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Clearbit 404 returns None (company not found)."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "clearbit",
            "domain": "nonexistent-domain.xyz"
        })

        assert result["success"] is False
        assert result["data"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_429_rate_limit(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Clearbit 429 raises rate limit error."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "30"}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "clearbit",
            "domain": "acme.com"
        })

        assert result["success"] is False
        assert "Rate limited" in result["error"]
        assert result["retry_after"] == 30

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_clearbit_parses_revenue(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Clearbit parses revenue strings correctly."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "metrics": {
                "employees": 100,
                "estimatedAnnualRevenue": "$10M-$50M"
            }
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "clearbit",
            "domain": "acme.com"
        })

        assert result["data"]["revenue_min_usd"] == 10_000_000


# =============================================================================
# TEST BUILTWITH API
# =============================================================================


class TestAPIClientAgentBuiltWith:
    """Tests for BuiltWith API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_success(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        builtwith_response, mock_api_keys
    ):
        """BuiltWith returns tech stack on success."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = builtwith_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "builtwith",
            "domain": "acme-mfg.com"
        })

        assert result["success"] is True
        assert "SAP" in result["data"]["tech_stack"]
        assert result["data"]["tech_source"] == "builtwith"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_extracts_erp(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        builtwith_response, mock_api_keys
    ):
        """BuiltWith extracts ERP system from categories."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = builtwith_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "builtwith",
            "domain": "acme-mfg.com"
        })

        assert result["data"]["erp_system"] == "SAP"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_extracts_crm(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        builtwith_response, mock_api_keys
    ):
        """BuiltWith extracts CRM system from categories."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = builtwith_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "builtwith",
            "domain": "acme-mfg.com"
        })

        assert result["data"]["crm_system"] == "Salesforce"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """BuiltWith returns None when API key not set."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "builtwith",
            "domain": "acme.com"
        })

        assert result["success"] is False
        assert result["data"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_limits_tech_stack(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """BuiltWith limits tech stack to 20 items."""
        mock_config.return_value.load.return_value = {}

        # Response with 30 technologies
        many_techs = [{"Name": f"Tech{i}", "Categories": []} for i in range(30)]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Results": [{
                "Result": {
                    "Paths": [{
                        "Technologies": many_techs
                    }]
                }
            }]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "builtwith",
            "domain": "acme.com"
        })

        assert len(result["data"]["tech_stack"]) <= 20


# =============================================================================
# TEST APOLLO API
# =============================================================================


class TestAPIClientAgentApollo:
    """Tests for Apollo API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_success_with_domain(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        apollo_organization_response, mock_api_keys
    ):
        """Apollo returns data on success with domain."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = apollo_organization_response
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "apollo",
            "domain": "acme-mfg.com"
        })

        assert result["success"] is True
        assert result["data"]["employee_count_min"] == 250
        assert result["data"]["firmographic_source"] == "apollo"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_success_with_company_name(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        apollo_organization_response, mock_api_keys
    ):
        """Apollo accepts company_name instead of domain."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = apollo_organization_response
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "apollo",
            "company_name": "Acme Manufacturing"
        })

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_extracts_linkedin(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        apollo_organization_response, mock_api_keys
    ):
        """Apollo extracts LinkedIn URL."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = apollo_organization_response
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "apollo",
            "domain": "acme-mfg.com"
        })

        assert result["data"]["linkedin_url"] == "https://linkedin.com/company/acme-mfg"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """Apollo returns None when API key not set."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "apollo",
            "domain": "acme.com"
        })

        assert result["success"] is False
        assert result["data"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_429_rate_limit(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo 429 raises rate limit error."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "apollo",
            "domain": "acme.com"
        })

        assert result["success"] is False
        assert "Rate limited" in result["error"]


# =============================================================================
# TEST ZOOMINFO API
# =============================================================================


class TestAPIClientAgentZoomInfo:
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
        """ZoomInfo returns data on success."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = zoominfo_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "zoominfo",
            "company_name": "Acme Manufacturing",
            "domain": "acme-mfg.com"
        })

        assert result["success"] is True
        assert result["data"]["employee_count_min"] == 250
        assert result["data"]["firmographic_source"] == "zoominfo"

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

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "zoominfo",
            "company_name": "Acme Manufacturing"
        })

        # 25 million -> 25,000,000
        assert result["data"]["revenue_min_usd"] == 25_000_000

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_extracts_naics(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        zoominfo_response, mock_api_keys
    ):
        """ZoomInfo extracts NAICS code."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = zoominfo_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "zoominfo",
            "company_name": "Acme Manufacturing"
        })

        assert result["data"]["naics_code"] == "332710"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """ZoomInfo returns None when API key not set."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "zoominfo",
            "company_name": "Acme"
        })

        assert result["success"] is False
        assert result["data"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_429_rate_limit(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo 429 raises rate limit error."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "zoominfo",
            "company_name": "Acme"
        })

        assert result["success"] is False
        assert "Rate limited" in result["error"]


# =============================================================================
# TEST ERROR HANDLING
# =============================================================================


class TestAPIClientAgentErrors:
    """Tests for error handling."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_handles_http_500_error(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Handles 5xx server errors."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        result = await agent.run({
            "provider": "clearbit",
            "domain": "acme.com"
        })

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_network_error_propagates(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Network errors propagate (are not caught by run())."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(
            side_effect=ConnectionError("Network unreachable")
        )

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        # Network errors propagate up - they are re-raised in _fetch_clearbit
        with pytest.raises(ConnectionError, match="Network unreachable"):
            await agent.run({
                "provider": "clearbit",
                "domain": "acme.com"
            })

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_json_decode_error_propagates(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """JSON decode errors propagate (are not caught by run())."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        # JSON errors propagate up - they are re-raised in _fetch_clearbit
        with pytest.raises(ValueError, match="Invalid JSON"):
            await agent.run({
                "provider": "clearbit",
                "domain": "acme.com"
            })


# =============================================================================
# TEST HELPER METHODS
# =============================================================================


class TestAPIClientAgentHelpers:
    """Tests for helper methods."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_millions(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses revenue with M suffix."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        assert agent._parse_revenue("$10M") == 10_000_000
        assert agent._parse_revenue("10M") == 10_000_000
        assert agent._parse_revenue("$10M-$50M") == 10_000_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_billions(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses revenue with B suffix."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        assert agent._parse_revenue("$1B") == 1_000_000_000
        assert agent._parse_revenue("1.5B") == 1_500_000_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_thousands(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses revenue with K suffix."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        assert agent._parse_revenue("$500K") == 500_000

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_revenue_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Returns None for invalid/empty revenue."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        assert agent._parse_revenue(None) is None
        assert agent._parse_revenue("") is None

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_build_linkedin_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Builds LinkedIn URL from handle."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        assert agent._build_linkedin_url("acme-mfg") == "https://linkedin.com/company/acme-mfg"
        assert agent._build_linkedin_url(None) is None
        assert agent._build_linkedin_url("") is None


# =============================================================================
# TEST CACHING
# =============================================================================


class TestAPIClientAgentCaching:
    """Tests for response caching."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_caches_successful_response(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Successful responses are cached."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"metrics": {"employees": 100}}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        # First call
        await agent.run({"provider": "clearbit", "domain": "acme.com"})

        # Second call should use cache
        result = await agent.run({"provider": "clearbit", "domain": "acme.com"})

        assert result["cached"] is True
        # HTTP should only be called once
        assert mock_http.return_value.get.call_count == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_cache_key_includes_provider(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Cache key includes provider name."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"metrics": {"employees": 100}}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.extraction.api_client import APIClientAgent

        agent = APIClientAgent(agent_type="extraction.api_client")

        # Clearbit and Apollo for same domain should be separate cache entries
        await agent.run({"provider": "clearbit", "domain": "acme.com"})

        mock_response.json.return_value = {"organization": {"estimated_num_employees": 200}}
        await agent.run({"provider": "apollo", "domain": "acme.com"})

        # Both should make HTTP calls (different cache keys)
        assert "clearbit:acme.com" in agent._cache
        assert "apollo:acme.com" in agent._cache
