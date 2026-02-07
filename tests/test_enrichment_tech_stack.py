"""
Tests for agents/enrichment/tech_stack.py - TechStackAgent

Tests tech stack detection via BuiltWith API, website fingerprinting, and job posting analysis.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestTechStackAgentInitialization:
    """Tests for TechStackAgent initialization."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_default_methods(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent loads default detection methods."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "tech_stack": {
                    "methods": ["builtwith", "website_fingerprint"]
                }
            }
        }
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        assert agent.methods == ["builtwith", "website_fingerprint"]

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
                "tech_stack": {
                    "batch_size": 75
                }
            }
        }
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        assert agent.batch_size == 75

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_has_erp_keywords(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent has ERP keywords dictionary."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        assert "SAP" in agent.ERP_KEYWORDS
        assert "Epicor" in agent.ERP_KEYWORDS
        assert "Microsoft Dynamics" in agent.ERP_KEYWORDS

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_has_crm_keywords(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent has CRM keywords dictionary."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        assert "Salesforce" in agent.CRM_KEYWORDS
        assert "HubSpot" in agent.CRM_KEYWORDS


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestTechStackAgentRun:
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
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

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
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent.run({})

        assert result["success"] is False
        assert "No records provided" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_skips_already_detected(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() skips records with existing erp_system."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        records = [{"company_name": "Acme", "erp_system": "SAP"}]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert result["records"][0]["erp_system"] == "SAP"
        # detection_rate should be 0 since skipped
        assert result["detection_rate"] == 0

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
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        records = [{"city": "Detroit", "state": "MI"}]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert len(result["records"]) == 1
        assert result["detection_rate"] == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_detects_tech_stack(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() detects and adds tech stack."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "tech_stack": {"methods": ["builtwith"]}
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Results": [{
                "Result": {
                    "Paths": [{
                        "Technologies": [
                            {"Name": "SAP", "Categories": ["ERP Systems"]}
                        ]
                    }]
                }
            }]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert result["records"][0]["erp_system"] == "SAP"
        assert result["detection_rate"] == 1.0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_calculates_detection_rate(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() correctly calculates detection_rate."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "tech_stack": {"methods": ["builtwith"]}
            }
        }

        # First succeeds with ERP, second finds nothing
        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.json.return_value = {
            "Results": [{
                "Result": {
                    "Paths": [{
                        "Technologies": [
                            {"Name": "Epicor", "Categories": ["ERP Systems"]}
                        ]
                    }]
                }
            }]
        }

        mock_empty = MagicMock()
        mock_empty.status_code = 200
        mock_empty.json.return_value = {"Results": []}

        mock_http.return_value.get = AsyncMock(
            side_effect=[mock_success, mock_empty]
        )

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        records = [
            {"company_name": "Acme", "website": "https://acme.com"},
            {"company_name": "Beta", "website": "https://beta.com"},
        ]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert result["detection_rate"] == 0.5  # 1 out of 2


# =============================================================================
# TEST BUILTWITH API
# =============================================================================


class TestTechStackAgentBuiltWith:
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

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_builtwith("acme.com")

        assert "SAP" in result["tech_stack"]
        assert "Salesforce" in result["tech_stack"]
        assert result["tech_source"] == "builtwith"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_detects_erp(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        builtwith_response, mock_api_keys
    ):
        """BuiltWith extracts ERP system from categories."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = builtwith_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_builtwith("acme.com")

        assert result["erp_system"] == "SAP"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_detects_crm(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        builtwith_response, mock_api_keys
    ):
        """BuiltWith extracts CRM system from categories."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = builtwith_response
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_builtwith("acme.com")

        assert result["crm_system"] == "Salesforce"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """BuiltWith returns None without API key."""
        mock_config.return_value.load.return_value = {}

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_builtwith("acme.com")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """BuiltWith 404 returns None."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_builtwith("nonexistent.xyz")

        assert result is None

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

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_builtwith("acme.com")

        assert len(result["tech_stack"]) <= 20

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_builtwith_empty_results(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """BuiltWith handles empty results."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Results": []}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_builtwith("acme.com")

        assert result["tech_stack"] == []
        assert result["erp_system"] is None
        assert result["crm_system"] is None


# =============================================================================
# TEST WEBSITE FINGERPRINT
# =============================================================================


class TestTechStackAgentFingerprint:
    """Tests for website fingerprint detection."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_sap(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects SAP in HTML."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<script src="/sap-ui-core.js"></script>'
        mock_response.headers = {}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert result["erp_system"] == "SAP"
        assert "SAP" in result["tech_stack"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_salesforce(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects Salesforce in HTML."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<script src="https://app.salesforce.com/api.js"></script>'
        mock_response.headers = {}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert result["crm_system"] == "Salesforce"
        assert "Salesforce" in result["tech_stack"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_dynamics(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects Microsoft Dynamics."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<script src="https://app.dynamics.com/widget.js"></script>'
        mock_response.headers = {}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert result["erp_system"] == "Microsoft Dynamics"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_epicor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects Epicor."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<script src="https://kinetic.epicor.com/api.js"></script>'
        mock_response.headers = {}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert result["erp_system"] == "Epicor"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_wordpress(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects WordPress."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<link href="/wp-content/themes/theme/style.css">'
        mock_response.headers = {}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert "WordPress" in result["tech_stack"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_hubspot(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects HubSpot."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<script src="https://js.hs-scripts.com/1234.js"></script>'
        mock_response.headers = {}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert "HubSpot" in result["tech_stack"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_server_nginx(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects nginx from headers."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Hello</body></html>"
        mock_response.headers = {"server": "nginx/1.18.0"}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert "nginx" in result["tech_stack"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_server_apache(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects Apache from headers."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Hello</body></html>"
        mock_response.headers = {"server": "Apache/2.4"}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert "Apache" in result["tech_stack"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_detects_iis(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint detects IIS from headers."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Hello</body></html>"
        mock_response.headers = {"server": "Microsoft-IIS/10.0"}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert "Microsoft IIS" in result["tech_stack"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint returns None on 404."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("nonexistent.xyz")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_no_tech_detected(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint returns None when no tech detected."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Simple page</body></html>"
        mock_response.headers = {}
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fingerprint_exception_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Fingerprint returns None on exception and logs debug."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(
            side_effect=Exception("Network error")
        )

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_fingerprint("acme.com")

        assert result is None
        agent.log.debug.assert_called()
        call_args = agent.log.debug.call_args
        assert call_args[0][0] == "website_fingerprint_failed"


# =============================================================================
# TEST JOB POSTINGS DETECTION
# =============================================================================


class TestTechStackAgentIndeedFeatureFlag:
    """Tests for Indeed scraping feature flag."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_indeed_disabled_by_default(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Indeed scraping disabled by default returns None immediately."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        assert result is None
        # HTTP should NOT have been called
        mock_http.return_value.get.assert_not_called()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_indeed_enabled_proceeds_to_http(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Indeed scraping enabled proceeds to HTTP call."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "tech_stack": {"enable_indeed_scraping": True}
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body><p>No tech mentions</p></body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        await agent._detect_job_postings("Acme Manufacturing")

        # HTTP SHOULD have been called (Indeed URL)
        mock_http.return_value.get.assert_called_once()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_indeed_disabled_logs_warning(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Indeed scraping disabled emits warning log."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        await agent._detect_job_postings("Acme Manufacturing")

        agent.log.warning.assert_called_once()
        call_args = agent.log.warning.call_args
        assert call_args[0][0] == "indeed_scraping_disabled"


class TestTechStackAgentJobPostings:
    """Tests for job posting detection."""

    # All job posting tests below enable Indeed scraping via config
    INDEED_ENABLED_CONFIG = {
        "enrichment": {"tech_stack": {"enable_indeed_scraping": True}}
    }

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_job_postings_detects_erp(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Job postings detect ERP mentions."""
        mock_config.return_value.load.return_value = self.INDEED_ENABLED_CONFIG

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <div class="job">
                <h3>ERP Administrator</h3>
                <p>Experience with SAP required. SAP certification preferred.</p>
            </div>
            <div class="job">
                <h3>IT Manager</h3>
                <p>Manage our SAP implementation.</p>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        assert result["erp_system"] == "SAP"
        assert result["tech_source"] == "job_postings"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_job_postings_requires_two_mentions(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Job postings require at least 2 mentions for confidence."""
        mock_config.return_value.load.return_value = self.INDEED_ENABLED_CONFIG

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <div class="job">
                <p>Experience with SAP preferred.</p>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        # Only 1 mention, not enough
        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_job_postings_detects_crm(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Job postings detect CRM mentions."""
        mock_config.return_value.load.return_value = self.INDEED_ENABLED_CONFIG

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <div class="job">
                <h3>Salesforce Administrator</h3>
                <p>Manage our Salesforce instance. SFDC certification required.</p>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        assert result["crm_system"] == "Salesforce"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_job_postings_prefers_highest_count(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Job postings select ERP with highest mention count."""
        mock_config.return_value.load.return_value = self.INDEED_ENABLED_CONFIG

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <p>SAP experience, SAP certification, SAP B1</p>
            <p>Oracle experience helpful</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        assert result["erp_system"] == "SAP"  # 3 mentions vs 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_job_postings_no_results(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Job postings return None when no ERP/CRM found."""
        mock_config.return_value.load.return_value = self.INDEED_ENABLED_CONFIG

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <p>Looking for a marketing specialist.</p>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_job_postings_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Job postings return None on 404."""
        mock_config.return_value.load.return_value = self.INDEED_ENABLED_CONFIG

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        assert result is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_job_postings_exception_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Job postings return None on exception and logs debug."""
        mock_config.return_value.load.return_value = self.INDEED_ENABLED_CONFIG

        mock_http.return_value.get = AsyncMock(
            side_effect=Exception("Network error")
        )

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        result = await agent._detect_job_postings("Acme Manufacturing")

        assert result is None
        agent.log.debug.assert_called()
        call_args = agent.log.debug.call_args
        assert call_args[0][0] == "job_postings_scrape_failed"


# =============================================================================
# TEST ERP/CRM KEYWORDS
# =============================================================================


class TestTechStackAgentKeywords:
    """Tests for ERP and CRM keyword dictionaries."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_erp_keywords_coverage(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """ERP keywords cover major vendors."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        expected_erps = [
            "SAP", "Oracle", "Epicor", "Infor", "Microsoft Dynamics",
            "SYSPRO", "Plex", "Acumatica", "QAD", "IFS",
            "Global Shop Solutions", "Sage", "IQMS", "JobBOSS", "MAPICS"
        ]

        for erp in expected_erps:
            assert erp in agent.ERP_KEYWORDS, f"Missing ERP: {erp}"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_crm_keywords_coverage(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """CRM keywords cover major vendors."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        expected_crms = [
            "Salesforce", "HubSpot", "Microsoft Dynamics CRM",
            "Zoho", "Pipedrive", "SAP CRM"
        ]

        for crm in expected_crms:
            assert crm in agent.CRM_KEYWORDS, f"Missing CRM: {crm}"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_erp_keywords_have_variants(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """ERP keywords include product variants."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        # SAP variants
        assert "SAP S/4HANA" in agent.ERP_KEYWORDS["SAP"]
        assert "SAP Business One" in agent.ERP_KEYWORDS["SAP"]

        # Oracle variants
        assert "NetSuite" in agent.ERP_KEYWORDS["Oracle"]
        assert "JD Edwards" in agent.ERP_KEYWORDS["Oracle"]


# =============================================================================
# TEST METHOD FALLBACK
# =============================================================================


class TestTechStackAgentMethodFallback:
    """Tests for detection method fallback behavior."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_tries_methods_in_order(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Tries detection methods in configured order."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "tech_stack": {
                    "methods": ["builtwith", "website_fingerprint"]
                }
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Results": [{
                "Result": {
                    "Paths": [{
                        "Technologies": [
                            {"Name": "SAP", "Categories": ["ERP Systems"]}
                        ]
                    }]
                }
            }]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        await agent.run({"records": records})

        # Should call builtwith first
        assert mock_http.return_value.get.call_count == 1
        # Should contain builtwith URL
        call_args = mock_http.return_value.get.call_args
        assert "builtwith.com" in call_args.args[0]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_stops_after_erp_detected(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Stops trying methods after ERP is detected."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "tech_stack": {
                    "methods": ["builtwith", "website_fingerprint", "job_postings"]
                }
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Results": [{
                "Result": {
                    "Paths": [{
                        "Technologies": [
                            {"Name": "Epicor", "Categories": ["ERP Systems"]}
                        ]
                    }]
                }
            }]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        # Should only call builtwith (first method found ERP)
        assert mock_http.return_value.get.call_count == 1
        assert result["records"][0]["erp_system"] == "Epicor"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_falls_back_on_failure(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Falls back to next method on failure."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "tech_stack": {
                    "methods": ["builtwith", "website_fingerprint"]
                }
            }
        }

        # BuiltWith fails, fingerprint succeeds
        mock_fail = MagicMock()
        mock_fail.status_code = 404

        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.text = '<script src="/sap-ui-core.js"></script>'
        mock_success.headers = {}

        mock_http.return_value.get = AsyncMock(
            side_effect=[mock_fail, mock_success]
        )

        from agents.enrichment.tech_stack import TechStackAgent

        agent = TechStackAgent(agent_type="enrichment.tech_stack")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        assert result["records"][0]["erp_system"] == "SAP"
        assert result["records"][0]["tech_source"] == "website_fingerprint"
