"""
Tests for agents/extraction/html_parser.py - HTMLParserAgent & DirectoryParserAgent

Tests HTML extraction with mocked HTTP responses.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST HTML PARSER INITIALIZATION
# =============================================================================


class TestHTMLParserAgentInitialization:
    """Tests for HTMLParserAgent initialization."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_default_config(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent initializes with default configuration."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")

        assert hasattr(agent, "batch_size")
        assert hasattr(agent, "concurrent_requests")
        assert hasattr(agent, "default_schema")

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_uses_config_values(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent uses configuration values."""
        mock_config.return_value.load.return_value = {
            "extraction": {
                "html_parser": {
                    "batch_size": 50,
                    "concurrent_requests": 10,
                    "default_schema": "pma"
                }
            }
        }
        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")

        assert agent.batch_size == 50
        assert agent.concurrent_requests == 10
        assert agent.default_schema == "pma"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_empty_schema_cache(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent starts with empty schema cache."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")

        assert agent._schema_cache == {}


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestHTMLParserAgentRun:
    """Tests for run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_requires_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() fails without URLs."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")

        result = await agent.run({"schema": "pma"})

        assert result["success"] is False
        assert "No URLs provided" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_accepts_single_url(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_detail_html
    ):
        """run() accepts single URL via 'url' parameter."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_detail_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["success"] is True
        assert len(result["records"]) == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_accepts_multiple_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_detail_html
    ):
        """run() accepts multiple URLs via 'urls' parameter."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_detail_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]}
        }

        result = await agent.run({
            "urls": [
                "https://example.com/member/1",
                "https://example.com/member/2"
            ],
            "association": "PMA"
        })

        assert result["success"] is True
        assert result["records_processed"] == 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_handles_http_errors(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """run() handles HTTP errors gracefully."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {}

        result = await agent.run({
            "url": "https://example.com/not-found",
            "association": "PMA"
        })

        # Should succeed but with 0 records
        assert result["success"] is True
        assert result["records_processed"] == 0


# =============================================================================
# TEST FIELD EXTRACTION
# =============================================================================


class TestHTMLParserAgentFieldExtraction:
    """Tests for field extraction."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extracts_text_with_css_selector(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_detail_html
    ):
        """Extracts text content using CSS selector."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_detail_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["company_name"] == "Acme Manufacturing Inc."

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extracts_href_attribute(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_detail_html
    ):
        """Extracts href attribute from link."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_detail_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]},
            "website": {"selectors": ["a.website"], "extract": "href"}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["website"] == "https://acme-mfg.com"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_tries_multiple_selectors(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Tries multiple selectors until one matches."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><span class="name">Test Company</span></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {
                "selectors": [
                    "h1.company-name",  # Won't match
                    ".title",           # Won't match
                    "span.name"         # Will match
                ]
            }
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["company_name"] == "Test Company"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_applies_parser(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Applies parser to extracted value."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="company-name">acme corp</h1></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {
                "selectors": ["h1.company-name"],
                "parser": "title_case"
            }
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["company_name"] == "Acme Corp"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_applies_default_value(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Uses default value when selector doesn't match."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="company-name">Test Corp</h1></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]},
            "country": {
                "selectors": [".country"],
                "default": "United States"
            }
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["country"] == "United States"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skips_records_without_company_name(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Skips records that don't have company_name."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><div class="info">Some text</div></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]},  # Won't match
            "info": {"selectors": [".info"]}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records_processed"] == 0


# =============================================================================
# TEST DIRECTORY PARSER
# =============================================================================


class TestDirectoryParserAgent:
    """Tests for DirectoryParserAgent."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extracts_multiple_records(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Extracts multiple records from directory page."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {
            "list_container": ".directory",
            "list_item": ".member-item",
            "company_name": {"selectors": ["h3.company-name"]},
            "location": {"selectors": [".location"]}
        }

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert result["success"] is True
        assert result["records_processed"] == 3
        assert result["records"][0]["company_name"] == "Acme Manufacturing Inc."
        assert result["records"][1]["company_name"] == "Beta Industries LLC"
        assert result["records"][2]["company_name"] == "Gamma Systems Corp"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_requires_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """DirectoryParser requires URL."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        result = await agent.run({"schema": "pma"})

        assert result["success"] is False
        assert "No URL provided" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_handles_http_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """DirectoryParser handles HTTP errors."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert result["success"] is False
        assert "HTTP 500" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_uses_default_selectors(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """DirectoryParser uses default list_item selector."""
        mock_config.return_value.load.return_value = {}

        # HTML with table rows
        html = """
        <html><body>
            <table>
                <tr><td class="company-name">Company A</td></tr>
                <tr><td class="company-name">Company B</td></tr>
            </table>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {
            # No list_container or list_item specified
            "company_name": {"selectors": [".company-name"]}
        }

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        # Should find rows with default selectors
        assert result["success"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extracts_href_from_list_items(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """DirectoryParser extracts href from list items."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {
            "list_container": ".directory",
            "list_item": ".member-item",
            "company_name": {"selectors": ["h3.company-name"]},
            "website": {"selectors": ["a.website"], "extract": "href"}
        }

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert result["records"][0]["website"] == "https://acme-mfg.com"
        assert result["records"][1]["website"] == "https://beta-industries.com"


# =============================================================================
# TEST SCHEMA LOADING
# =============================================================================


class TestHTMLParserAgentSchemaLoading:
    """Tests for schema loading."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_caches_loaded_schemas(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """Schemas are cached after first load."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(
            agent_type="extraction.html_parser",
            config_path=str(tmp_path)
        )

        # Manually set schema in cache
        agent._schema_cache["pma"] = {"company_name": {"selectors": [".name"]}}

        # Load same schema
        schema = agent._load_schema("pma")

        assert schema == {"company_name": {"selectors": [".name"]}}

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_falls_back_to_default_schema(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Falls back to default schema when requested not found."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {"company_name": {"selectors": [".name"]}}

        # Load nonexistent schema
        schema = agent._load_schema("nonexistent")

        assert schema == {"company_name": {"selectors": [".name"]}}


# =============================================================================
# TEST ERROR HANDLING
# =============================================================================


class TestHTMLParserAgentErrors:
    """Tests for error handling."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_handles_network_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Handles network errors gracefully - swallowed in _extract_from_url."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(
            side_effect=ConnectionError("Network error")
        )

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {}

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        # Network errors are caught in _extract_from_url and logged,
        # returning None - they don't propagate to the errors list
        assert result["success"] is True
        assert result["records_processed"] == 0
        # Errors list is empty because _extract_from_url catches and logs
        assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_handles_invalid_html(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Handles malformed HTML gracefully."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body><div>Unclosed div"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["div"]}
        }

        # Should not raise exception
        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["success"] is True


# =============================================================================
# TEST METADATA
# =============================================================================


class TestHTMLParserAgentMetadata:
    """Tests for metadata in extracted records."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_includes_source_url(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_detail_html
    ):
        """Records include source URL."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_detail_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["source_url"] == "https://example.com/member/1"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_includes_association(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_detail_html
    ):
        """Records include association code."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_detail_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["association"] == "PMA"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_includes_extracted_at(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_detail_html
    ):
        """Records include extraction timestamp."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_detail_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1.company-name"]}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert "extracted_at" in result["records"][0]
        # Should be ISO format timestamp
        assert "T" in result["records"][0]["extracted_at"]


# =============================================================================
# TEST XPATH EXTRACTION
# =============================================================================


class TestHTMLParserXPathExtraction:
    """Tests for XPath selector extraction."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_basic_xpath_extraction(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Extracts text using XPath selector."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><div id="name">Acme Corp</div></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ['//div[@id="name"]']}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records_processed"] == 1
        assert result["records"][0]["company_name"] == "Acme Corp"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_xpath_attribute_extraction(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Extracts attribute using XPath with extract config."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1>Test Co</h1><a id="site" href="https://test.com">Visit</a></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1"]},
            "website": {
                "selectors": ['//a[@id="site"]'],
                "extract": "href"
            }
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["website"] == "https://test.com"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_xpath_lxml_import_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Falls back gracefully when lxml not installed."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="name">Acme Corp</h1></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {
                "selectors": [
                    "//div[@class='name']",  # XPath - will fail without lxml
                    "h1.name",               # CSS fallback
                ]
            }
        }

        # Mock lxml import to fail inside _extract_xpath
        with patch.dict("sys.modules", {"lxml": None, "lxml.etree": None}):
            result = await agent.run({
                "url": "https://example.com/member/1",
                "association": "PMA"
            })

        # Should fall through to CSS selector
        assert result["records_processed"] == 1
        assert result["records"][0]["company_name"] == "Acme Corp"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_xpath_malformed_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Malformed XPath returns None and tries next selector."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="name">Acme Corp</h1></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {
                "selectors": [
                    "//[invalid xpath!(",  # Invalid XPath
                    "h1.name",             # Valid CSS fallback
                ]
            }
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records_processed"] == 1
        assert result["records"][0]["company_name"] == "Acme Corp"


# =============================================================================
# TEST SCHEMA EXTENDS
# =============================================================================


class TestHTMLParserSchemaExtends:
    """Tests for schema 'extends' functionality."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_inherits_base_fields(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """Extended schema inherits fields from base."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.html_parser import HTMLParserAgent
        import yaml

        # Create schemas directory
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()

        # Create base schema
        base_schema = {
            "default": {
                "company_name": {"selectors": [".name"]},
                "city": {"selectors": [".city"]},
            }
        }
        with open(schemas_dir / "default.yaml", "w") as f:
            yaml.dump(base_schema, f)

        # Create extended schema
        ext_schema = {
            "pma": {
                "extends": "default",
                "phone": {"selectors": [".phone"]},
            }
        }
        with open(schemas_dir / "pma.yaml", "w") as f:
            yaml.dump(ext_schema, f)

        agent = HTMLParserAgent(
            agent_type="extraction.html_parser",
            config_path=str(tmp_path)
        )

        schema = agent._load_schema("pma")

        assert "company_name" in schema  # inherited
        assert "city" in schema           # inherited
        assert "phone" in schema          # added

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_overrides_base_fields(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """Extended schema overrides base fields."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.html_parser import HTMLParserAgent
        import yaml

        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()

        base_schema = {
            "default": {
                "company_name": {"selectors": [".name"]},
            }
        }
        with open(schemas_dir / "default.yaml", "w") as f:
            yaml.dump(base_schema, f)

        ext_schema = {
            "pma": {
                "extends": "default",
                "company_name": {"selectors": [".company-name", "h1"]},
            }
        }
        with open(schemas_dir / "pma.yaml", "w") as f:
            yaml.dump(ext_schema, f)

        agent = HTMLParserAgent(
            agent_type="extraction.html_parser",
            config_path=str(tmp_path)
        )

        schema = agent._load_schema("pma")
        # Override should win
        assert schema["company_name"]["selectors"] == [".company-name", "h1"]

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_schema_from_file(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """Loads schema from YAML file on disk."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.html_parser import HTMLParserAgent
        import yaml

        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()

        schema_data = {
            "custom": {
                "company_name": {"selectors": [".co-name"]},
                "website": {"selectors": ["a.site"], "extract": "href"},
            }
        }
        with open(schemas_dir / "custom.yaml", "w") as f:
            yaml.dump(schema_data, f)

        agent = HTMLParserAgent(
            agent_type="extraction.html_parser",
            config_path=str(tmp_path)
        )

        schema = agent._load_schema("custom")
        assert schema["company_name"]["selectors"] == [".co-name"]
        assert schema["website"]["extract"] == "href"


# =============================================================================
# TEST FIELD MAPPING AND ENUM
# =============================================================================


class TestHTMLParserFieldMapping:
    """Tests for field mapping and enum validation."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_mapping_translates_value(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        schema_with_mapping
    ):
        """Mapping config translates extracted values."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="company-name">Acme Corp</h1><span class="tier">P</span></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = schema_with_mapping

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["membership_tier"] == "Platinum"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_mapping_passthrough_unknown(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        schema_with_mapping
    ):
        """Mapping passes through unknown values unchanged."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="company-name">Acme Corp</h1><span class="tier">X</span></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = schema_with_mapping

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        # "X" not in mapping, passed through
        assert result["records"][0]["membership_tier"] == "X"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_enum_exact_match(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        schema_with_enum
    ):
        """Enum validation passes exact match."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="company-name">Acme Corp</h1><span class="state">MI</span></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = schema_with_enum

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["state"] == "MI"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_enum_case_insensitive_match(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        schema_with_enum
    ):
        """Enum validation does case-insensitive matching."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="company-name">Acme Corp</h1><span class="state">mi</span></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = schema_with_enum

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        # "mi" should be corrected to "MI"
        assert result["records"][0]["state"] == "MI"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_enum_no_match_keeps_value(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        schema_with_enum
    ):
        """Enum validation keeps value when no match found."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1 class="company-name">Acme Corp</h1><span class="state">CA</span></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = schema_with_enum

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        # "CA" not in enum list, kept as-is
        assert result["records"][0]["state"] == "CA"


# =============================================================================
# TEST CSS EXTRACT TYPES
# =============================================================================


class TestHTMLParserCSSExtractTypes:
    """Tests for _extract_css with various extract types."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_src(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Extracts src attribute from element."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1>Acme Corp</h1><img class="logo" src="https://acme.com/logo.png"></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1"]},
            "logo_url": {"selectors": ["img.logo"], "extract": "src"}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["logo_url"] == "https://acme.com/logo.png"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_custom_attribute(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Extracts custom data attribute from element."""
        mock_config.return_value.load.return_value = {}

        html = '<html><body><h1>Acme Corp</h1><div class="member" data-id="M-12345">Info</div></body></html>'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import HTMLParserAgent

        agent = HTMLParserAgent(agent_type="extraction.html_parser")
        agent._schema_cache["default"] = {
            "company_name": {"selectors": ["h1"]},
            "member_id": {"selectors": [".member"], "extract": "data-id"}
        }

        result = await agent.run({
            "url": "https://example.com/member/1",
            "association": "PMA"
        })

        assert result["records"][0]["member_id"] == "M-12345"


# =============================================================================
# TEST AUTO-EXTRACT MEMBERS
# =============================================================================


class TestDirectoryParserAutoExtract:
    """Tests for DirectoryParserAgent._auto_extract_members()."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extracts_external_links(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        inline_directory_html
    ):
        """Auto-extract finds external company links."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = inline_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {}  # Empty schema triggers auto-extract

        result = await agent.run({
            "url": "https://pma.org/members",
            "association": "PMA"
        })

        company_names = [r["company_name"] for r in result["records"]]
        assert "Acme Manufacturing Inc" in company_names
        assert "Gamma Systems Corp" in company_names
        assert "Delta Corp" in company_names

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skips_social_media_links(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        inline_directory_html
    ):
        """Auto-extract skips social media links."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = inline_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {}

        result = await agent.run({
            "url": "https://pma.org/members",
            "association": "PMA"
        })

        domains = [r.get("domain", "") for r in result["records"]]
        assert "www.facebook.com" not in domains
        assert "twitter.com" not in domains
        assert "www.linkedin.com" not in domains
        assert "www.youtube.com" not in domains

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skips_internal_links(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        inline_directory_html
    ):
        """Auto-extract skips links back to the association site."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = inline_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {}

        result = await agent.run({
            "url": "https://pma.org/members",
            "association": "PMA"
        })

        # pma.org links should be excluded
        domains = [r.get("domain", "") for r in result["records"]]
        assert "pma.org" not in domains

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_dedupes_by_domain(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        inline_directory_html
    ):
        """Auto-extract deduplicates by domain."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = inline_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {}

        result = await agent.run({
            "url": "https://pma.org/members",
            "association": "PMA"
        })

        # acme-mfg.com appears twice in the HTML, should only be extracted once
        acme_records = [r for r in result["records"] if "acme" in r.get("domain", "").lower()]
        assert len(acme_records) == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_strips_asterisks(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        inline_directory_html
    ):
        """Auto-extract strips trailing asterisks from company names."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = inline_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {}

        result = await agent.run({
            "url": "https://pma.org/members",
            "association": "PMA"
        })

        # "Beta Industries*" should become "Beta Industries"
        beta = [r for r in result["records"] if "Beta" in r.get("company_name", "")]
        assert len(beta) == 1
        assert beta[0]["company_name"] == "Beta Industries"


# =============================================================================
# TEST STRUCTURED ERROR DETAIL (Phase 7)
# =============================================================================


class TestDirectoryParserStructuredErrors:
    """Tests for structured error responses with url, attempted_methods, duration_ms."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_error_response_includes_url(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Error responses include 'url' field."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert result["url"] == "https://example.com/directory"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_error_response_includes_attempted_methods(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Error responses include 'attempted_methods' list."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert "attempted_methods" in result
        assert "httpx" in result["attempted_methods"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_error_response_includes_duration_ms(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Error responses include 'duration_ms' field."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert "duration_ms" in result
        assert isinstance(result["duration_ms"], int)
        assert result["duration_ms"] >= 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_success_response_includes_url(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Success responses also include url field."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {
            "list_container": ".directory",
            "list_item": ".member-item",
            "company_name": {"selectors": ["h3.company-name"]},
        }

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert result["url"] == "https://example.com/directory"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_success_response_includes_duration(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Success responses include duration_ms."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {
            "list_container": ".directory",
            "list_item": ".member-item",
            "company_name": {"selectors": ["h3.company-name"]},
        }

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert result["duration_ms"] >= 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_success_response_includes_attempted_methods(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        sample_member_directory_html
    ):
        """Success responses include attempted_methods."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_member_directory_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")
        agent._schema_cache["default"] = {
            "list_container": ".directory",
            "list_item": ".member-item",
            "company_name": {"selectors": ["h3.company-name"]},
        }

        result = await agent.run({
            "url": "https://example.com/directory",
            "association": "PMA"
        })

        assert result["attempted_methods"] == ["httpx"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_no_url_error_has_empty_methods(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """No URL error response has empty attempted_methods."""
        mock_config.return_value.load.return_value = {}

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        result = await agent.run({"schema": "pma"})

        assert result["attempted_methods"] == []
        assert result["duration_ms"] == 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_403_with_playwright_fallback_lists_both_methods(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """403 response with Playwright fallback lists both methods."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        # Mock Playwright to also fail
        with patch.object(agent, "_fetch_with_playwright", new_callable=AsyncMock, return_value=None):
            result = await agent.run({
                "url": "https://example.com/directory",
                "association": "PMA"
            })

        assert result["attempted_methods"] == ["httpx", "playwright"]
        assert result["success"] is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_connection_error_with_playwright_fallback(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Connection error tries playwright as fallback and lists both methods."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(
            side_effect=ConnectionError("Connection refused")
        )

        from agents.extraction.html_parser import DirectoryParserAgent

        agent = DirectoryParserAgent(agent_type="extraction.directory_parser")

        with patch.object(agent, "_fetch_with_playwright", new_callable=AsyncMock, return_value=None):
            result = await agent.run({
                "url": "https://example.com/directory",
                "association": "PMA"
            })

        assert "httpx" in result["attempted_methods"]
        assert "playwright" in result["attempted_methods"]
        assert result["success"] is False
