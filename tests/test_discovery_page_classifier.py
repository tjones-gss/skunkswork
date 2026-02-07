"""
Tests for PageClassifierAgent
NAM Intelligence Pipeline

Tests for the page classifier agent that classifies web pages into ontology types
to determine the appropriate extraction strategy.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestPageClassifierAgentInitialization:
    """Tests for PageClassifierAgent initialization."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_default_min_confidence(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test default minimum confidence threshold."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        assert agent.min_confidence == 0.5

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_custom_min_confidence(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test custom minimum confidence threshold."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        # Set config directly and re-run setup
        agent.agent_config = {"min_confidence": 0.7}
        agent._setup()

        assert agent.min_confidence == 0.7


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestPageClassifierAgentRun:
    """Tests for PageClassifierAgent.run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_with_provided_html(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test classification with provided HTML."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "html": sample_member_directory_html
        })

        assert result["success"] is True
        assert "page_type" in result
        assert "confidence" in result
        assert result["records_processed"] == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_fetches_html_when_not_provided(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test that HTML is fetched when not provided."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "fetch": True
        })

        assert result["success"] is True
        mock_http.return_value.get.assert_called_once()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_missing_url_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test error when URL is missing."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({})

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_handles_fetch_failure(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling fetch failure gracefully."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "fetch": True
        })

        # Should still succeed but classify based on URL only
        assert result["success"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_handles_fetch_exception(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling fetch exception gracefully."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(side_effect=Exception("Network error"))

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "fetch": True
        })

        # Should still succeed but classify based on URL only
        assert result["success"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_classification_object(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test that classification object is returned."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "html": sample_member_directory_html
        })

        assert "classification" in result
        assert "url" in result["classification"]
        assert "page_type" in result["classification"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_recommended_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test that recommended extractor is returned."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "html": sample_member_directory_html
        })

        assert "recommended_extractor" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_signals(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test that classification signals are returned."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "html": sample_member_directory_html
        })

        assert "signals" in result
        assert "url" in result["signals"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_logs_classification(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test that classification is logged."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        await agent.run({
            "url": "https://pma.org/members",
            "html": sample_member_directory_html
        })

        agent.log.info.assert_called()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_url_only_classification(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classification based on URL only."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "url": "https://pma.org/members",
            "fetch": False  # Don't fetch HTML
        })

        assert result["success"] is True
        # URL-based classification only
        assert result["confidence"] > 0


# =============================================================================
# TEST CLASSIFY BY URL
# =============================================================================


class TestPageClassifierAgentClassifyByUrl:
    """Tests for _classify_by_url() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_member_directory_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying member directory URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/members",
            "https://example.com/directory",
            "https://example.com/member-directory",
            "https://example.com/find-a-member",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.MEMBER_DIRECTORY, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_member_detail_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying member detail URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/members/acme-corp",
            "https://example.com/company/acme-corp",
            "https://example.com/profile/acme-corp",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.MEMBER_DETAIL, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_events_list_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying events list URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/events",
            "https://example.com/calendar",
            "https://example.com/upcoming-events",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.EVENTS_LIST, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_event_detail_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying event detail URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/events/fabtech-2024",
            "https://example.com/event/annual-meeting",
            "https://example.com/conference/summit-2024",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.EVENT_DETAIL, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_exhibitors_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying exhibitors list URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/exhibitors",
            "https://example.com/exhibitor-list",
            "https://example.com/exhibitor-directory",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.EXHIBITORS_LIST, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_sponsors_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying sponsors list URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/sponsors",
            "https://example.com/sponsor-list",
            "https://example.com/our-sponsors",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.SPONSORS_LIST, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_participants_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying participants list URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/attendees",
            "https://example.com/speakers",
            "https://example.com/participants",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.PARTICIPANTS_LIST, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_association_home_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying association home URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/",
            "https://example.com/home",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.ASSOCIATION_HOME, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_resource_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying resource URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        test_urls = [
            "https://example.com/resources",
            "https://example.com/publications",
            "https://example.com/news",
            "https://example.com/blog",
        ]

        for url in test_urls:
            result = agent._classify_by_url(url)
            assert result["page_type"] == PageType.RESOURCE, f"Failed for: {url}"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_unknown_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying unknown URLs."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = agent._classify_by_url("https://example.com/random-page-xyz")

        assert result["page_type"] == PageType.OTHER
        assert result["confidence"] < 0.5

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_url_returns_source(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that URL classification includes source."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        result = agent._classify_by_url("https://example.com/members")

        assert result["source"] == "url"


# =============================================================================
# TEST CLASSIFY BY CONTENT
# =============================================================================


class TestPageClassifierAgentClassifyByContent:
    """Tests for _classify_by_content() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_content_by_title(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying by page title."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        html = "<html><head><title>Member Directory - PMA</title></head><body></body></html>"

        result = agent._classify_by_content(html)

        assert result["page_type"] == PageType.MEMBER_DIRECTORY

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_content_by_h1(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying by H1 tag content."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        html = "<html><body><h1>Upcoming Events</h1></body></html>"

        result = agent._classify_by_content(html)

        assert result["page_type"] == PageType.EVENTS_LIST

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_content_by_body_text(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test classifying by body text content."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        html = """
        <html><body>
            <p>Thank you to our sponsors for their generous support.</p>
            <div class="sponsor-section">
                <h2>Platinum Sponsors</h2>
            </div>
        </body></html>
        """

        result = agent._classify_by_content(html)

        assert result["page_type"] == PageType.SPONSORS_LIST

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_content_returns_confidence(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that content classification includes confidence."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        html = "<html><head><title>Member Directory</title></head><body><h1>Member Directory</h1></body></html>"

        result = agent._classify_by_content(html)

        assert "confidence" in result
        assert result["confidence"] > 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_classify_content_returns_signals(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that matched keywords are returned as signals."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        html = "<html><head><title>Our Members</title></head><body><h1>Member Directory</h1></body></html>"

        result = agent._classify_by_content(html)

        assert "signals" in result


# =============================================================================
# TEST ANALYZE STRUCTURE
# =============================================================================


class TestPageClassifierAgentAnalyzeStructure:
    """Tests for _analyze_structure() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_analyze_structure_many_list_items(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting directory from many list items."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType
        from bs4 import BeautifulSoup

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        items = "".join([f'<li>Item {i}</li>' for i in range(25)])
        html = f"<html><body><ul>{items}</ul></body></html>"
        soup = BeautifulSoup(html, "lxml")

        scores = agent._analyze_structure(soup)

        assert PageType.MEMBER_DIRECTORY in scores

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_analyze_structure_member_elements(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting directory from member class elements."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType
        from bs4 import BeautifulSoup

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        members = "".join([f'<div class="member-item">Company {i}</div>' for i in range(10)])
        html = f"<html><body>{members}</body></html>"
        soup = BeautifulSoup(html, "lxml")

        scores = agent._analyze_structure(soup)

        assert PageType.MEMBER_DIRECTORY in scores

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_analyze_structure_single_profile(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting member detail from single profile element."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType
        from bs4 import BeautifulSoup

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        html = '<html><body><div class="member-profile">Company Details</div></body></html>'
        soup = BeautifulSoup(html, "lxml")

        scores = agent._analyze_structure(soup)

        assert PageType.MEMBER_DETAIL in scores

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_analyze_structure_event_elements(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting events from event class elements."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType
        from bs4 import BeautifulSoup

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        events = "".join([f'<div class="event-item">Event {i}</div>' for i in range(5)])
        html = f"<html><body>{events}</body></html>"
        soup = BeautifulSoup(html, "lxml")

        scores = agent._analyze_structure(soup)

        assert PageType.EVENTS_LIST in scores

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_analyze_structure_sponsor_tiers(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sponsors_page_html
    ):
        """Test detecting sponsors from tier elements."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType
        from bs4 import BeautifulSoup

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        soup = BeautifulSoup(sponsors_page_html, "lxml")

        scores = agent._analyze_structure(soup)

        assert PageType.SPONSORS_LIST in scores

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_analyze_structure_exhibitor_elements(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        exhibitors_page_html
    ):
        """Test detecting exhibitors from booth elements."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType
        from bs4 import BeautifulSoup

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        soup = BeautifulSoup(exhibitors_page_html, "lxml")

        scores = agent._analyze_structure(soup)

        assert PageType.EXHIBITORS_LIST in scores

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_analyze_structure_registration_form(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting event detail from registration form."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType
        from bs4 import BeautifulSoup

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        html = '<html><body><form action="/register"><button>Register Now</button></form></body></html>'
        soup = BeautifulSoup(html, "lxml")

        scores = agent._analyze_structure(soup)

        assert PageType.EVENT_DETAIL in scores


# =============================================================================
# TEST COMBINE CLASSIFICATIONS
# =============================================================================


class TestPageClassifierAgentCombineClassifications:
    """Tests for _combine_classifications() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_combine_same_type_boosts_confidence(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that matching classifications boost confidence."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        url_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.6
        }
        content_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.5,
            "signals": []
        }

        page_type, confidence, signals = agent._combine_classifications(url_class, content_class)

        assert page_type == PageType.MEMBER_DIRECTORY
        assert confidence > 0.6  # Boosted

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_combine_different_types_prefers_higher_confidence(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that higher confidence wins when types differ."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        url_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.4
        }
        content_class = {
            "page_type": PageType.EVENTS_LIST,
            "confidence": 0.8,
            "signals": []
        }

        page_type, confidence, signals = agent._combine_classifications(url_class, content_class)

        assert page_type == PageType.EVENTS_LIST
        assert confidence == 0.8

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_combine_url_only_when_no_content(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test using URL classification when content is None."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        url_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.7
        }

        page_type, confidence, signals = agent._combine_classifications(url_class, None)

        assert page_type == PageType.MEMBER_DIRECTORY
        assert confidence == 0.7

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_combine_caps_confidence_at_098(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that combined confidence is capped at 0.98."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        url_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.8
        }
        content_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.9,
            "signals": []
        }

        page_type, confidence, signals = agent._combine_classifications(url_class, content_class)

        assert confidence <= 0.98

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_combine_returns_signals(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that combined result includes signals."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        url_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.6
        }
        content_class = {
            "page_type": PageType.MEMBER_DIRECTORY,
            "confidence": 0.5,
            "signals": ["title:member directory"]
        }

        page_type, confidence, signals = agent._combine_classifications(url_class, content_class)

        assert "url" in signals
        assert "content" in signals


# =============================================================================
# TEST EXTRACTOR MAPPING
# =============================================================================


class TestPageClassifierAgentExtractorMapping:
    """Tests for extractor mapping."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_member_directory_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test member directory maps to directory parser."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        extractor = agent.EXTRACTOR_MAPPING.get(PageType.MEMBER_DIRECTORY)
        assert extractor == "extraction.directory_parser"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_member_detail_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test member detail maps to HTML parser."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        extractor = agent.EXTRACTOR_MAPPING.get(PageType.MEMBER_DETAIL)
        assert extractor == "extraction.html_parser"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_events_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test events maps to event extractor."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        extractor = agent.EXTRACTOR_MAPPING.get(PageType.EVENTS_LIST)
        assert extractor == "extraction.event_extractor"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_exhibitors_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test exhibitors maps to event participant extractor."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        extractor = agent.EXTRACTOR_MAPPING.get(PageType.EXHIBITORS_LIST)
        assert extractor == "extraction.event_participant_extractor"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_sponsors_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test sponsors maps to event participant extractor."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        extractor = agent.EXTRACTOR_MAPPING.get(PageType.SPONSORS_LIST)
        assert extractor == "extraction.event_participant_extractor"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_association_home_no_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test association home has no extractor."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        extractor = agent.EXTRACTOR_MAPPING.get(PageType.ASSOCIATION_HOME)
        assert extractor is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_other_no_extractor(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test OTHER page type has no extractor."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import PageClassifierAgent
        from models.ontology import PageType

        agent = PageClassifierAgent(agent_type="discovery.page_classifier")

        extractor = agent.EXTRACTOR_MAPPING.get(PageType.OTHER)
        assert extractor is None


# =============================================================================
# TEST BATCH PAGE CLASSIFIER
# =============================================================================


class TestBatchPageClassifierAgentRun:
    """Tests for BatchPageClassifierAgent.run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_multiple_pages(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html, event_page_html
    ):
        """Test batch classification of multiple pages."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import BatchPageClassifierAgent
        agent = BatchPageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "pages": [
                {"url": "https://example.com/members", "html": sample_member_directory_html},
                {"url": "https://example.com/events", "html": event_page_html}
            ]
        })

        assert result["success"] is True
        assert len(result["classifications"]) == 2
        assert result["records_processed"] == 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_empty_pages(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run with empty pages list."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import BatchPageClassifierAgent
        agent = BatchPageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({"pages": []})

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_returns_type_distribution(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test batch run returns type distribution."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import BatchPageClassifierAgent
        agent = BatchPageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "pages": [
                {"url": "https://example.com/members", "html": sample_member_directory_html},
                {"url": "https://example.com/directory", "html": sample_member_directory_html}
            ]
        })

        assert "type_distribution" in result
        assert "MEMBER_DIRECTORY" in result["type_distribution"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_logs_summary(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Test batch run logs classification summary."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import BatchPageClassifierAgent
        agent = BatchPageClassifierAgent(agent_type="discovery.page_classifier")

        await agent.run({
            "pages": [
                {"url": "https://example.com/members", "html": sample_member_directory_html}
            ]
        })

        agent.log.info.assert_called()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_batch_run_handles_failed_classification(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test batch run handles individual classification failures."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.page_classifier import BatchPageClassifierAgent
        agent = BatchPageClassifierAgent(agent_type="discovery.page_classifier")

        result = await agent.run({
            "pages": [
                {"url": "https://example.com/members"},  # Missing URL - will fail
                {}  # Invalid page - missing url
            ]
        })

        # Should succeed overall but track failed classifications
        assert result["success"] is True
        # Some may not be classified due to missing URL
