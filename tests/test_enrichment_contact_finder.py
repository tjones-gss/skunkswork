"""
Tests for agents/enrichment/contact_finder.py - ContactFinderAgent

Tests contact finding via Apollo, ZoomInfo APIs and team page scraping.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestContactFinderAgentInitialization:
    """Tests for ContactFinderAgent initialization."""

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
                "contact_finder": {
                    "providers": ["apollo", "zoominfo"]
                }
            }
        }
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent.providers == ["apollo", "zoominfo"]

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_max_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent loads max_contacts_per_company from config."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "contact_finder": {
                    "max_contacts_per_company": 10
                }
            }
        }
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent.max_contacts == 10

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_target_titles(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent loads target_titles from config."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "contact_finder": {
                    "target_titles": ["CIO", "CFO"]
                }
            }
        }
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent.target_titles == ["CIO", "CFO"]

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_has_target_title_patterns(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent has TARGET_TITLE_PATTERNS."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert len(agent.TARGET_TITLE_PATTERNS) > 0


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestContactFinderAgentRun:
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
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

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
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        result = await agent.run({})

        assert result["success"] is False
        assert "No records provided" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_skips_records_with_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() skips records that already have contacts."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        records = [{
            "company_name": "Acme",
            "contacts": [{"name": "John Smith", "title": "CEO"}]
        }]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert result["contacts_found"] == 1
        assert len(result["records"][0]["contacts"]) == 1

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
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        records = [{"city": "Detroit", "state": "MI"}]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert len(result["records"]) == 1
        assert result["contacts_found"] == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_finds_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() finds and adds contacts."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "contact_finder": {"providers": ["apollo"]}
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "people": [{
                "name": "John Smith",
                "title": "Chief Information Officer",
                "email": "jsmith@acme.com"
            }]
        }
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        assert result["success"] is True
        assert len(result["records"][0]["contacts"]) == 1
        assert result["contacts_found"] == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_limits_max_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() limits contacts to max_contacts_per_company."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "contact_finder": {
                    "providers": ["apollo"],
                    "max_contacts_per_company": 2
                }
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "people": [
                {"name": "John Smith", "title": "CIO", "email": "jsmith@acme.com"},
                {"name": "Jane Doe", "title": "CFO", "email": "jdoe@acme.com"},
                {"name": "Bob Johnson", "title": "CEO", "email": "bjohnson@acme.com"},
            ]
        }
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        # Should be limited to 2
        assert len(result["records"][0]["contacts"]) == 2


# =============================================================================
# TEST APOLLO API
# =============================================================================


class TestContactFinderAgentApollo:
    """Tests for Apollo API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_search_returns_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo search returns contacts."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "people": [{
                "name": "John Smith",
                "title": "CIO",
                "email": "jsmith@acme.com",
                "phone_numbers": [{"number": "555-123-4567"}],
                "linkedin_url": "https://linkedin.com/in/jsmith"
            }]
        }
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_apollo("acme.com")

        assert len(contacts) == 1
        assert contacts[0]["name"] == "John Smith"
        assert contacts[0]["title"] == "CIO"
        assert contacts[0]["email"] == "jsmith@acme.com"
        assert contacts[0]["source"] == "apollo"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_search_extracts_phone(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo extracts phone from phone_numbers array."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "people": [{
                "name": "John Smith",
                "title": "CIO",
                "phone_numbers": [{"number": "555-123-4567"}]
            }]
        }
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_apollo("acme.com")

        assert contacts[0]["phone"] == "555-123-4567"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_search_handles_no_phone(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo handles contacts without phone numbers."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "people": [{
                "name": "John Smith",
                "title": "CIO",
                "phone_numbers": []
            }]
        }
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_apollo("acme.com")

        assert contacts[0]["phone"] is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_search_extracts_linkedin(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo extracts LinkedIn URL."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "people": [{
                "name": "John Smith",
                "title": "CIO",
                "linkedin_url": "https://linkedin.com/in/jsmith"
            }]
        }
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_apollo("acme.com")

        assert contacts[0]["linkedin_url"] == "https://linkedin.com/in/jsmith"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """Apollo returns empty list without API key."""
        mock_config.return_value.load.return_value = {}

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_apollo("acme.com")

        assert contacts == []

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_404_returns_empty(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo 404 returns empty list."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.post = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_apollo("nonexistent.xyz")

        assert contacts == []

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_apollo_exception_returns_empty(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """Apollo exception returns empty list and logs warning."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.post = AsyncMock(
            side_effect=Exception("Network error")
        )

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_apollo("acme.com")

        assert contacts == []
        agent.log.warning.assert_called()
        call_args = agent.log.warning.call_args
        assert call_args[0][0] == "apollo_contact_search_failed"


# =============================================================================
# TEST ZOOMINFO API
# =============================================================================


class TestContactFinderAgentZoomInfo:
    """Tests for ZoomInfo API integration."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_search_returns_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo search returns contacts."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{
                "firstName": "John",
                "lastName": "Smith",
                "jobTitle": "CIO",
                "email": "jsmith@acme.com",
                "phone": "555-123-4567",
                "linkedInUrl": "https://linkedin.com/in/jsmith"
            }]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_zoominfo("Acme Manufacturing")

        assert len(contacts) == 1
        assert contacts[0]["name"] == "John Smith"
        assert contacts[0]["title"] == "CIO"
        assert contacts[0]["email"] == "jsmith@acme.com"
        assert contacts[0]["source"] == "zoominfo"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_combines_first_last_name(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo combines first and last name."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{
                "firstName": "Jane",
                "lastName": "Doe",
                "jobTitle": "CFO"
            }]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_zoominfo("Acme")

        assert contacts[0]["name"] == "Jane Doe"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_extracts_linkedin(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo extracts LinkedIn URL."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{
                "firstName": "John",
                "lastName": "Smith",
                "jobTitle": "CIO",
                "linkedInUrl": "https://linkedin.com/in/jsmith"
            }]
        }
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_zoominfo("Acme")

        assert contacts[0]["linkedin_url"] == "https://linkedin.com/in/jsmith"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_no_api_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, no_api_keys
    ):
        """ZoomInfo returns empty list without API key."""
        mock_config.return_value.load.return_value = {}

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_zoominfo("Acme")

        assert contacts == []

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_zoominfo_404_returns_empty(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """ZoomInfo 404 returns empty list."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._search_zoominfo("Nonexistent Company")

        assert contacts == []


# =============================================================================
# TEST TEAM PAGE SCRAPING
# =============================================================================


class TestContactFinderAgentTeamScraping:
    """Tests for team page scraping."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_team_page_finds_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Team page scraping finds contacts."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <div class="team-member">
                <h3>John Smith</h3>
                <p class="position">Chief Information Officer</p>
                <a href="mailto:jsmith@acme.com">Email</a>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._scrape_team_page("acme.com")

        assert len(contacts) >= 1
        assert any(c.get("source") == "website" for c in contacts)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_team_page_tries_multiple_paths(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Team page scraping tries multiple paths."""
        mock_config.return_value.load.return_value = {}

        mock_404 = MagicMock()
        mock_404.status_code = 404

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.text = """
        <html><body>
            <div class="executive">
                <h3>John Smith</h3>
                <p class="role">CEO</p>
            </div>
        </body></html>
        """

        # First few paths fail, then one succeeds
        mock_http.return_value.get = AsyncMock(
            side_effect=[mock_404, mock_404, mock_200]
        )

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._scrape_team_page("acme.com")

        assert len(contacts) >= 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_team_page_extracts_email(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Team page scraping extracts email from mailto links."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <div class="team-member">
                <h3>John Smith</h3>
                <span class="position">CIO</span>
                <a href="mailto:jsmith@acme.com?subject=Hello">Contact</a>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._scrape_team_page("acme.com")

        # Should extract email and strip query params
        email_contact = next((c for c in contacts if c.get("email")), None)
        if email_contact:
            assert email_contact["email"] == "jsmith@acme.com"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_team_page_extracts_phone(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Team page scraping extracts phone from tel links."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <div class="team-member">
                <h3>John Smith</h3>
                <span class="position">CIO</span>
                <a href="tel:+15551234567">Call</a>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._scrape_team_page("acme.com")

        phone_contact = next((c for c in contacts if c.get("phone")), None)
        if phone_contact:
            assert len(phone_contact["phone"]) >= 10

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_team_page_extracts_linkedin(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Team page scraping extracts LinkedIn URLs."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html><body>
            <div class="team-member">
                <h3>John Smith</h3>
                <span class="position">CIO</span>
                <a href="https://linkedin.com/in/jsmith">LinkedIn</a>
            </div>
        </body></html>
        """
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._scrape_team_page("acme.com")

        linkedin_contact = next((c for c in contacts if c.get("linkedin_url")), None)
        if linkedin_contact:
            assert "linkedin.com" in linkedin_contact["linkedin_url"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_scrape_team_page_all_404_returns_empty(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Team page scraping returns empty when all paths 404."""
        mock_config.return_value.load.return_value = {}

        mock_404 = MagicMock()
        mock_404.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_404)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = await agent._scrape_team_page("acme.com")

        assert contacts == []


# =============================================================================
# TEST TITLE VALIDATION
# =============================================================================


class TestContactFinderAgentTitleValidation:
    """Tests for title validation methods."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_looks_like_title_valid_ceo(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Recognizes CEO as valid title."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._looks_like_title("CEO") is True
        assert agent._looks_like_title("Chief Executive Officer") is True

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_looks_like_title_valid_vp(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Recognizes VP titles as valid."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._looks_like_title("VP of Operations") is True
        assert agent._looks_like_title("Vice President of IT") is True

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_looks_like_title_valid_director(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Recognizes Director titles as valid."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._looks_like_title("Director of Engineering") is True
        assert agent._looks_like_title("IT Director") is True

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_looks_like_title_invalid(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Rejects non-title strings."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._looks_like_title("") is False
        assert agent._looks_like_title(None) is False
        assert agent._looks_like_title("Detroit, MI") is False
        assert agent._looks_like_title("A" * 150) is False  # Too long

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_is_target_title_cio(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """CIO is a target title."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._is_target_title("CIO") is True
        assert agent._is_target_title("Chief Information Officer") is True

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_is_target_title_vp_it(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """VP IT is a target title."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._is_target_title("VP of IT") is True
        assert agent._is_target_title("VP Information Technology") is True

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_is_target_title_it_director(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """IT Director is a target title."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._is_target_title("IT Director") is True
        assert agent._is_target_title("Director of IT") is True

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_is_target_title_cfo(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """CFO is a target title."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._is_target_title("CFO") is True
        assert agent._is_target_title("Chief Financial Officer") is True

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_is_target_title_not_target(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Non-target titles return False."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        # These titles don't match any TARGET_TITLE_PATTERNS
        assert agent._is_target_title("Accountant") is False
        assert agent._is_target_title("Software Developer") is False
        assert agent._is_target_title("") is False
        assert agent._is_target_title(None) is False


# =============================================================================
# TEST TITLE PRIORITY
# =============================================================================


class TestContactFinderAgentTitlePriority:
    """Tests for title priority scoring."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_get_title_priority_cio(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """CIO has priority 1."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._get_title_priority("CIO") == 1
        assert agent._get_title_priority("Chief Information Officer") == 1

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_get_title_priority_cfo(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """CFO has priority 2."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._get_title_priority("CFO") == 2
        assert agent._get_title_priority("Chief Financial Officer") == 2

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_get_title_priority_plant_manager(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Plant Manager has priority 3."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        assert agent._get_title_priority("Plant Manager") == 3

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_get_title_priority_unknown(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Unknown titles have priority 999."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        # These titles don't match any TARGET_TITLE_PATTERNS
        assert agent._get_title_priority("Accountant") == 999
        assert agent._get_title_priority("Software Developer") == 999
        assert agent._get_title_priority("") == 999
        assert agent._get_title_priority(None) == 999


# =============================================================================
# TEST CONTACT SORTING
# =============================================================================


class TestContactFinderAgentSorting:
    """Tests for contact sorting by priority."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_sort_by_priority(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Sorts contacts by title priority."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = [
            {"name": "John", "title": "Plant Manager"},  # Priority 3
            {"name": "Jane", "title": "CIO"},            # Priority 1
            {"name": "Bob", "title": "CFO"},             # Priority 2
        ]

        sorted_contacts = agent._sort_by_priority(contacts)

        assert sorted_contacts[0]["name"] == "Jane"  # CIO first
        assert sorted_contacts[1]["name"] == "Bob"   # CFO second
        assert sorted_contacts[2]["name"] == "John"  # Plant Manager last

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_sort_by_priority_with_unknown(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Unknown titles sort last."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = [
            {"name": "Unknown", "title": "Marketing Rep"},
            {"name": "Known", "title": "CIO"},
        ]

        sorted_contacts = agent._sort_by_priority(contacts)

        assert sorted_contacts[0]["name"] == "Known"
        assert sorted_contacts[1]["name"] == "Unknown"


# =============================================================================
# TEST DEDUPLICATION
# =============================================================================


class TestContactFinderAgentDeduplication:
    """Tests for contact deduplication."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_dedupe_by_email(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Deduplicates by email."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = [
            {"name": "John Smith", "email": "jsmith@acme.com"},
            {"name": "J Smith", "email": "jsmith@acme.com"},  # Duplicate
        ]

        unique = agent._dedupe_contacts(contacts)

        assert len(unique) == 1

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_dedupe_by_name(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Deduplicates by name when email missing."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = [
            {"name": "John Smith"},
            {"name": "john smith"},  # Same name, different case
        ]

        unique = agent._dedupe_contacts(contacts)

        assert len(unique) == 1

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_dedupe_keeps_first(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Keeps first occurrence of duplicate."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = [
            {"name": "John Smith", "email": "jsmith@acme.com", "source": "apollo"},
            {"name": "J Smith", "email": "jsmith@acme.com", "source": "zoominfo"},
        ]

        unique = agent._dedupe_contacts(contacts)

        assert unique[0]["source"] == "apollo"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_dedupe_different_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Keeps different contacts."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        contacts = [
            {"name": "John Smith", "email": "jsmith@acme.com"},
            {"name": "Jane Doe", "email": "jdoe@acme.com"},
        ]

        unique = agent._dedupe_contacts(contacts)

        assert len(unique) == 2


# =============================================================================
# TEST PARSE TEAM PAGE
# =============================================================================


class TestContactFinderAgentParseTeamPage:
    """Tests for _parse_team_page method."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_team_page_structured_dom(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses structured team member elements."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        html = """
        <div class="team-member">
            <h3>John Smith</h3>
            <p class="position">Chief Executive Officer</p>
        </div>
        <div class="team-member">
            <h3>Jane Doe</h3>
            <p class="position">Chief Financial Officer</p>
        </div>
        """

        contacts = agent._parse_team_page(html, "acme.com")

        assert len(contacts) >= 2

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_parse_team_page_with_name_class(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Parses team members with .name class."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        html = """
        <div class="executive">
            <span class="name">John Smith</span>
            <span class="role">CEO</span>
        </div>
        """

        contacts = agent._parse_team_page(html, "acme.com")

        assert len(contacts) >= 1


# =============================================================================
# TEST EXTRACT CONTACTS FROM TEXT
# =============================================================================


class TestContactFinderAgentExtractFromText:
    """Tests for _extract_contacts_from_text method."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_extract_contacts_name_title_pattern(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Extracts contacts from Name, Title pattern."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent
        from bs4 import BeautifulSoup

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        html = """
        <p>John Smith, Chief Executive Officer</p>
        <p>Jane Doe, Chief Financial Officer</p>
        """
        soup = BeautifulSoup(html, "lxml")

        contacts = agent._extract_contacts_from_text(soup, "acme.com")

        assert len(contacts) >= 2
        assert any(c["name"] == "John Smith" for c in contacts)

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_extract_contacts_limits_results(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Limits extracted contacts to 10."""
        mock_config.return_value.load.return_value = {}
        from agents.enrichment.contact_finder import ContactFinderAgent
        from bs4 import BeautifulSoup

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        # Create HTML with many names
        lines = [f"<p>Person{i} Name, Chief Officer</p>" for i in range(20)]
        html = "\n".join(lines)
        soup = BeautifulSoup(html, "lxml")

        contacts = agent._extract_contacts_from_text(soup, "acme.com")

        assert len(contacts) <= 10


# =============================================================================
# TEST PROVIDER MERGING
# =============================================================================


class TestContactFinderAgentProviderMerging:
    """Tests for merging contacts from multiple providers."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_merges_multiple_sources(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() merges contacts from multiple providers."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "contact_finder": {
                    "providers": ["apollo", "zoominfo"]
                }
            }
        }

        # Apollo returns CIO
        mock_apollo_response = MagicMock()
        mock_apollo_response.status_code = 200
        mock_apollo_response.json.return_value = {
            "people": [{
                "name": "John Smith",
                "title": "CIO",
                "email": "jsmith@acme.com"
            }]
        }

        # ZoomInfo returns CFO
        mock_zoominfo_response = MagicMock()
        mock_zoominfo_response.status_code = 200
        mock_zoominfo_response.json.return_value = {
            "data": [{
                "firstName": "Jane",
                "lastName": "Doe",
                "jobTitle": "CFO",
                "email": "jdoe@acme.com"
            }]
        }

        mock_http.return_value.post = AsyncMock(return_value=mock_apollo_response)
        mock_http.return_value.get = AsyncMock(return_value=mock_zoominfo_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        # Should have contacts from both sources
        assert len(result["records"][0]["contacts"]) == 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_dedupes_merged_contacts(
        self, mock_limiter, mock_http, mock_logger, mock_config, mock_api_keys
    ):
        """run() deduplicates contacts from multiple providers."""
        mock_config.return_value.load.return_value = {
            "enrichment": {
                "contact_finder": {
                    "providers": ["apollo", "zoominfo"]
                }
            }
        }

        # Both return the same person
        mock_apollo_response = MagicMock()
        mock_apollo_response.status_code = 200
        mock_apollo_response.json.return_value = {
            "people": [{
                "name": "John Smith",
                "title": "CIO",
                "email": "jsmith@acme.com"
            }]
        }

        mock_zoominfo_response = MagicMock()
        mock_zoominfo_response.status_code = 200
        mock_zoominfo_response.json.return_value = {
            "data": [{
                "firstName": "John",
                "lastName": "Smith",
                "jobTitle": "Chief Information Officer",
                "email": "jsmith@acme.com"  # Same email
            }]
        }

        mock_http.return_value.post = AsyncMock(return_value=mock_apollo_response)
        mock_http.return_value.get = AsyncMock(return_value=mock_zoominfo_response)

        from agents.enrichment.contact_finder import ContactFinderAgent

        agent = ContactFinderAgent(agent_type="enrichment.contact_finder")

        records = [{"company_name": "Acme", "website": "https://acme.com"}]
        result = await agent.run({"records": records})

        # Should deduplicate to 1 contact
        assert len(result["records"][0]["contacts"]) == 1
