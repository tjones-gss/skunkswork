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
