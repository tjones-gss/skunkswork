"""
Tests for agents/extraction/pdf_parser.py - PDFParserAgent

Tests PDF extraction with mocked pdfplumber and HTTP responses.
"""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# HELPERS
# =============================================================================


def _make_agent():
    """Create a PDFParserAgent with mocked dependencies."""
    with patch("agents.base.Config") as mock_config, \
         patch("agents.base.StructuredLogger"), \
         patch("agents.base.AsyncHTTPClient") as mock_http, \
         patch("agents.base.RateLimiter"):
        mock_config.return_value.load.return_value = {}
        from agents.extraction.pdf_parser import PDFParserAgent
        agent = PDFParserAgent(agent_type="extraction.pdf_parser")
        return agent, mock_http


def _make_mock_pdfplumber(mock_pdf):
    """Create a mock pdfplumber module that returns the given mock_pdf.

    pdfplumber is imported at function-level inside _extract_from_pdf(),
    so we must inject it via sys.modules, not via patch on the module attribute.
    """
    mock_module = MagicMock()
    mock_module.open.return_value = mock_pdf
    return mock_module


# =============================================================================
# TEST PDF PARSER INITIALIZATION
# =============================================================================


class TestPDFParserInit:
    """Tests for PDFParserAgent initialization."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_with_defaults(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent initializes with default max_pages."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.pdf_parser import PDFParserAgent

        agent = PDFParserAgent(agent_type="extraction.pdf_parser")
        assert agent.max_pages == 500

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_uses_config_max_pages(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent uses configured max_pages."""
        mock_config.return_value.load.return_value = {
            "extraction": {
                "pdf_parser": {"max_pages": 100}
            }
        }
        from agents.extraction.pdf_parser import PDFParserAgent

        agent = PDFParserAgent(agent_type="extraction.pdf_parser")
        assert agent.max_pages == 100

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_setup_called(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """_setup is invoked during __init__."""
        mock_config.return_value.load.return_value = {}
        from agents.extraction.pdf_parser import PDFParserAgent

        agent = PDFParserAgent(agent_type="extraction.pdf_parser")
        assert hasattr(agent, "max_pages")


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestPDFParserRun:
    """Tests for run() method."""

    @pytest.mark.asyncio
    async def test_run_requires_input(self):
        """run() fails without pdf_url or pdf_path."""
        agent, _ = _make_agent()
        result = await agent.run({"association": "PMA"})

        assert result["success"] is False
        assert "No pdf_url or pdf_path provided" in result["error"]
        assert result["records"] == []

    @pytest.mark.asyncio
    async def test_run_with_url(self, mock_pdfplumber_pdf, sample_pdf_table):
        """run() downloads and parses PDF from URL."""
        agent, mock_http = _make_agent()

        mock_response = MagicMock()
        mock_response.content = b"fake-pdf-bytes"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        mock_pdf = mock_pdfplumber_pdf([{"tables": [sample_pdf_table]}])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            result = await agent.run({
                "pdf_url": "https://example.com/dir.pdf",
                "association": "PMA"
            })

        assert result["success"] is True
        assert result["pages_processed"] == 1
        assert len(result["records"]) == 3

    @pytest.mark.asyncio
    async def test_run_with_file_path(self, tmp_path, mock_pdfplumber_pdf, sample_pdf_table):
        """run() reads PDF from file path."""
        agent, _ = _make_agent()

        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"fake-pdf-bytes")

        mock_pdf = mock_pdfplumber_pdf([{"tables": [sample_pdf_table]}])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            result = await agent.run({
                "pdf_path": str(pdf_file),
                "association": "PMA"
            })

        assert result["success"] is True
        assert len(result["records"]) == 3

    @pytest.mark.asyncio
    async def test_run_handles_http_error(self):
        """run() handles HTTP download errors."""
        agent, mock_http = _make_agent()
        mock_http.return_value.get = AsyncMock(
            side_effect=ConnectionError("Network error")
        )

        result = await agent.run({
            "pdf_url": "https://example.com/dir.pdf",
            "association": "PMA"
        })

        assert result["success"] is False
        assert "Failed to load PDF" in result["error"]

    @pytest.mark.asyncio
    async def test_run_handles_file_not_found(self):
        """run() handles missing file path."""
        agent, _ = _make_agent()

        result = await agent.run({
            "pdf_path": "/nonexistent/path.pdf",
            "association": "PMA"
        })

        assert result["success"] is False
        assert "Failed to load PDF" in result["error"]

    @pytest.mark.asyncio
    async def test_run_handles_parse_error(self):
        """run() handles PDF parsing errors."""
        agent, mock_http = _make_agent()

        mock_response = MagicMock()
        mock_response.content = b"fake-pdf-bytes"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        mock_module = MagicMock()
        mock_module.open.side_effect = Exception("Corrupted PDF")

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            result = await agent.run({
                "pdf_url": "https://example.com/dir.pdf",
                "association": "PMA"
            })

        assert result["success"] is False
        assert "Failed to parse PDF" in result["error"]

    @pytest.mark.asyncio
    async def test_run_response_structure(self, mock_pdfplumber_pdf, sample_pdf_table):
        """run() returns correct response structure."""
        agent, mock_http = _make_agent()

        mock_response = MagicMock()
        mock_response.content = b"fake-pdf-bytes"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        mock_pdf = mock_pdfplumber_pdf([{"tables": [sample_pdf_table]}])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            result = await agent.run({
                "pdf_url": "https://example.com/dir.pdf",
                "association": "PMA"
            })

        assert "success" in result
        assert "records" in result
        assert "pages_processed" in result
        assert "records_processed" in result


# =============================================================================
# TEST _extract_from_pdf
# =============================================================================


class TestPDFParserExtractFromPDF:
    """Tests for _extract_from_pdf() method."""

    @pytest.mark.asyncio
    async def test_pdfplumber_not_installed(self):
        """Returns empty when pdfplumber not installed."""
        agent, _ = _make_agent()

        # Temporarily remove pdfplumber from sys.modules to trigger ImportError
        saved = sys.modules.pop("pdfplumber", None)
        try:
            with patch.dict(sys.modules, {"pdfplumber": None}):
                records, pages = await agent._extract_from_pdf(b"data", "PMA")
        finally:
            if saved is not None:
                sys.modules["pdfplumber"] = saved

        assert records == []
        assert pages == 0

    @pytest.mark.asyncio
    async def test_multi_page_pdf(self, mock_pdfplumber_pdf, sample_pdf_table):
        """Processes multiple pages."""
        agent, _ = _make_agent()

        table2 = [
            ["Company Name", "City", "State", "Phone"],
            ["Delta Corp", "Boston", "MA", "555-111-2222"],
        ]
        mock_pdf = mock_pdfplumber_pdf([
            {"tables": [sample_pdf_table]},
            {"tables": [table2]},
        ])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert pages == 2
        assert len(records) == 4  # 3 from page 1 + 1 from page 2

    @pytest.mark.asyncio
    async def test_max_pages_limit(self, mock_pdfplumber_pdf, sample_pdf_table):
        """Respects max_pages limit."""
        agent, _ = _make_agent()
        agent.max_pages = 1

        mock_pdf = mock_pdfplumber_pdf([
            {"tables": [sample_pdf_table]},
            {"tables": [sample_pdf_table]},
            {"tables": [sample_pdf_table]},
        ])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert pages == 1
        assert len(records) == 3  # Only first page

    @pytest.mark.asyncio
    async def test_table_first_priority(self, mock_pdfplumber_pdf):
        """Uses table extraction when tables are present (ignores text)."""
        agent, _ = _make_agent()

        table = [
            ["Company Name", "City", "State", "Phone"],
            ["Acme Inc", "Detroit", "MI", "555-123-4567"],
        ]
        mock_pdf = mock_pdfplumber_pdf([{
            "tables": [table],
            "text": "Some text that should be ignored"
        }])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert len(records) == 1
        assert records[0]["company_name"] == "Acme Inc"

    @pytest.mark.asyncio
    async def test_text_fallback(self, mock_pdfplumber_pdf):
        """Falls back to text extraction when no tables."""
        agent, _ = _make_agent()

        mock_pdf = mock_pdfplumber_pdf([{
            "tables": [],
            "text": "Acme Manufacturing\nDetroit, MI 48201\n555-123-4567"
        }])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert len(records) == 1
        assert records[0]["company_name"] == "Acme Manufacturing"

    @pytest.mark.asyncio
    async def test_empty_pdf(self, mock_pdfplumber_pdf):
        """Handles empty PDF (no pages)."""
        agent, _ = _make_agent()

        mock_pdf = mock_pdfplumber_pdf([])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert records == []
        assert pages == 0


# =============================================================================
# TEST _parse_table
# =============================================================================


class TestPDFParserParseTable:
    """Tests for _parse_table() method."""

    def test_parses_valid_table(self, sample_pdf_table):
        """Parses table with valid headers and rows."""
        agent, _ = _make_agent()
        records = agent._parse_table(sample_pdf_table, "PMA")

        assert len(records) == 3
        assert records[0]["company_name"] == "Acme Manufacturing Inc"
        assert records[0]["city"] == "Detroit"
        assert records[0]["state"] == "MI"
        assert records[0]["phone"] == "(555) 123-4567"
        assert records[0]["association"] == "PMA"

    def test_empty_table(self):
        """Returns empty for empty table."""
        agent, _ = _make_agent()
        assert agent._parse_table([], "PMA") == []

    def test_header_only_table(self):
        """Returns empty for table with only headers."""
        agent, _ = _make_agent()
        table = [["Company Name", "City", "State"]]
        assert agent._parse_table(table, "PMA") == []

    def test_skips_mismatched_row_length(self):
        """Skips rows with different column count than headers."""
        agent, _ = _make_agent()
        table = [
            ["Company Name", "City", "State", "Phone"],
            ["Acme Inc", "Detroit", "MI", "555-123-4567"],
            ["Short Row", "Only Two"],  # mismatched
            ["Beta LLC", "Chicago", "IL", "555-987-6543"],
        ]
        records = agent._parse_table(table, "PMA")
        assert len(records) == 2

    def test_skips_missing_company_name(self):
        """Skips rows where company_name is empty or None."""
        agent, _ = _make_agent()
        table = [
            ["Company Name", "City", "State"],
            ["", "Detroit", "MI"],
            [None, "Chicago", "IL"],
            ["Gamma Corp", "Cleveland", "OH"],
        ]
        records = agent._parse_table(table, "PMA")
        assert len(records) == 1
        assert records[0]["company_name"] == "Gamma Corp"

    def test_includes_metadata(self):
        """Records include association and extracted_at."""
        agent, _ = _make_agent()
        table = [
            ["Company Name", "City"],
            ["Acme Inc", "Detroit"],
        ]
        records = agent._parse_table(table, "NEMA")
        assert records[0]["association"] == "NEMA"
        assert "extracted_at" in records[0]

    def test_strips_whitespace(self):
        """Strips whitespace from values."""
        agent, _ = _make_agent()
        table = [
            ["Company Name", "City", "State"],
            ["  Acme Inc  ", "  Detroit  ", "  MI  "],
        ]
        records = agent._parse_table(table, "PMA")
        assert records[0]["company_name"] == "Acme Inc"
        assert records[0]["city"] == "Detroit"

    def test_skips_non_member_table(self):
        """Returns empty for table without member-relevant columns."""
        agent, _ = _make_agent()
        table = [
            ["Date", "Amount", "Description"],
            ["2024-01-15", "100.00", "Payment"],
        ]
        assert agent._parse_table(table, "PMA") == []


# =============================================================================
# TEST _normalize_header
# =============================================================================


class TestPDFParserNormalizeHeader:
    """Tests for _normalize_header() method."""

    def test_none_header(self):
        """Returns None for None input."""
        agent, _ = _make_agent()
        assert agent._normalize_header(None) is None

    def test_empty_header(self):
        """Returns None for empty string."""
        agent, _ = _make_agent()
        assert agent._normalize_header("") is None

    @pytest.mark.parametrize("header,expected", [
        ("company", "company_name"),
        ("Company Name", "company_name"),
        ("NAME", "company_name"),
        ("Member", "company_name"),
        ("Member Name", "company_name"),
        ("Organization", "company_name"),
        ("City", "city"),
        ("STATE", "state"),
        ("St", "state"),
        ("Province", "state"),
        ("Country", "country"),
        ("Phone", "phone"),
        ("Telephone", "phone"),
        ("Email", "email"),
        ("E-Mail", "email"),
        ("Website", "website"),
        ("Web", "website"),
        ("URL", "website"),
        ("Membership", "membership_tier"),
        ("Membership Type", "membership_tier"),
        ("Type", "membership_tier"),
        ("Joined", "member_since"),
        ("Member Since", "member_since"),
        ("Year Joined", "member_since"),
    ])
    def test_header_variations(self, header, expected):
        """Maps header variations to correct field names."""
        agent, _ = _make_agent()
        assert agent._normalize_header(header) == expected

    def test_unknown_header(self):
        """Returns None for unknown headers."""
        agent, _ = _make_agent()
        assert agent._normalize_header("Unknown Column") is None
        assert agent._normalize_header("Revenue") is None
        assert agent._normalize_header("Notes") is None


# =============================================================================
# TEST _is_member_table
# =============================================================================


class TestPDFParserIsMemberTable:
    """Tests for _is_member_table() method."""

    def test_requires_company_name(self):
        """Requires company_name column."""
        agent, _ = _make_agent()
        assert agent._is_member_table(["city", "state", "phone"]) is False

    def test_requires_relevant_column(self):
        """Requires at least one relevant column beyond company_name."""
        agent, _ = _make_agent()
        assert agent._is_member_table(["company_name"]) is False
        assert agent._is_member_table(["company_name", "membership_tier"]) is False

    def test_valid_minimal(self):
        """Accepts table with company_name and one relevant column."""
        agent, _ = _make_agent()
        assert agent._is_member_table(["company_name", "city"]) is True
        assert agent._is_member_table(["company_name", "phone"]) is True
        assert agent._is_member_table(["company_name", "email"]) is True

    def test_valid_full(self):
        """Accepts table with multiple relevant columns."""
        agent, _ = _make_agent()
        headers = ["company_name", "city", "state", "phone", "website"]
        assert agent._is_member_table(headers) is True


# =============================================================================
# TEST _parse_text
# =============================================================================


class TestPDFParserParseText:
    """Tests for _parse_text() method."""

    def test_splits_by_double_newline(self):
        """Splits text into blocks by double newlines."""
        agent, _ = _make_agent()
        text = "Acme Manufacturing\nDetroit, MI 48201\n\nBeta Industries\nChicago, IL 60601"
        records = agent._parse_text(text, "PMA")
        assert len(records) == 2

    def test_multiple_blocks(self):
        """Parses multiple company blocks."""
        agent, _ = _make_agent()
        text = (
            "Acme Manufacturing\nDetroit, MI 48201\n555-123-4567\n\n"
            "Beta Industries\nChicago, IL 60601\n555-987-6543\n\n"
            "Gamma Systems\nCleveland, OH 44101"
        )
        records = agent._parse_text(text, "PMA")
        assert len(records) == 3

    def test_skips_invalid_blocks(self):
        """Skips blocks that can't be parsed as companies."""
        agent, _ = _make_agent()
        text = "Single line only\n\nPage 1\nof the directory\n\nAcme Corp\nDetroit, MI"
        records = agent._parse_text(text, "PMA")
        # "Single line only" - only 1 line, skipped
        # "Page 1" block - header pattern, skipped
        # "Acme Corp" - valid
        assert len(records) == 1
        assert records[0]["company_name"] == "Acme Corp"

    def test_empty_text(self):
        """Returns empty for empty text."""
        agent, _ = _make_agent()
        assert agent._parse_text("", "PMA") == []


# =============================================================================
# TEST _parse_text_block
# =============================================================================


class TestPDFParserParseTextBlock:
    """Tests for _parse_text_block() method."""

    def test_extracts_company_name(self):
        """First line is company name."""
        agent, _ = _make_agent()
        block = "Acme Manufacturing\nDetroit, MI"
        record = agent._parse_text_block(block, "PMA")
        assert record["company_name"] == "Acme Manufacturing"

    def test_extracts_phone(self):
        """Extracts phone numbers (dash-separated format)."""
        agent, _ = _make_agent()
        block = "Acme Manufacturing\n555-123-4567"
        record = agent._parse_text_block(block, "PMA")
        assert record["phone"] == "5551234567"

    def test_extracts_email(self):
        """Extracts email addresses."""
        agent, _ = _make_agent()
        block = "Acme Manufacturing\ninfo@acme-mfg.com"
        record = agent._parse_text_block(block, "PMA")
        assert record["email"] == "info@acme-mfg.com"

    def test_extracts_website(self):
        """Extracts website URLs."""
        agent, _ = _make_agent()
        block = "Acme Manufacturing\nwww.acme-mfg.com"
        record = agent._parse_text_block(block, "PMA")
        assert record["website"] == "www.acme-mfg.com"

    def test_extracts_city_state(self):
        """Extracts city and state from 'City, ST' pattern."""
        agent, _ = _make_agent()
        block = "Acme Manufacturing\nDetroit, MI 48201"
        record = agent._parse_text_block(block, "PMA")
        assert record["city"] == "Detroit"
        assert record["state"] == "MI"

    def test_skips_header_blocks(self):
        """Skips blocks that look like headers."""
        agent, _ = _make_agent()
        assert agent._parse_text_block("Page 1\nof the directory", "PMA") is None
        assert agent._parse_text_block("Member Directory\nSection A", "PMA") is None
        assert agent._parse_text_block("Directory\nPage 2", "PMA") is None

    def test_skips_page_numbers(self):
        """Skips blocks starting with just a number."""
        agent, _ = _make_agent()
        assert agent._parse_text_block("42\nSome text below", "PMA") is None

    def test_single_line_returns_none(self):
        """Returns None for single-line blocks."""
        agent, _ = _make_agent()
        assert agent._parse_text_block("Acme Manufacturing", "PMA") is None

    def test_includes_metadata(self):
        """Includes association and extracted_at."""
        agent, _ = _make_agent()
        block = "Acme Manufacturing\nDetroit, MI"
        record = agent._parse_text_block(block, "AGMA")
        assert record["association"] == "AGMA"
        assert "extracted_at" in record


# =============================================================================
# TEST EDGE CASES
# =============================================================================


class TestPDFParserEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_corrupted_pdf_bytes(self):
        """Handles corrupted PDF data."""
        agent, mock_http = _make_agent()

        mock_response = MagicMock()
        mock_response.content = b"not-a-real-pdf"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        mock_module = MagicMock()
        mock_module.open.side_effect = Exception("Invalid PDF")

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            result = await agent.run({
                "pdf_url": "https://example.com/bad.pdf",
                "association": "PMA"
            })

        assert result["success"] is False
        assert "Failed to parse PDF" in result["error"]

    @pytest.mark.asyncio
    async def test_unicode_content(self, mock_pdfplumber_pdf):
        """Handles unicode characters in PDF content."""
        agent, _ = _make_agent()

        table = [
            ["Company Name", "City", "State"],
            ["\u00dcller GmbH & Co. KG", "M\u00fcnchen", "BY"],
        ]
        mock_pdf = mock_pdfplumber_pdf([{"tables": [table]}])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert pages == 1

    @pytest.mark.asyncio
    async def test_large_pdf_many_pages(self, mock_pdfplumber_pdf):
        """Handles PDF with many pages."""
        agent, _ = _make_agent()
        agent.max_pages = 3

        pages_data = []
        for i in range(10):
            table = [
                ["Company Name", "City"],
                [f"Company {i}", f"City {i}"],
            ]
            pages_data.append({"tables": [table]})

        mock_pdf = mock_pdfplumber_pdf(pages_data)
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert pages == 3  # Limited by max_pages

    @pytest.mark.asyncio
    async def test_mixed_table_and_text_pages(self, mock_pdfplumber_pdf):
        """Handles mix of table and text pages."""
        agent, _ = _make_agent()

        mock_pdf = mock_pdfplumber_pdf([
            {"tables": [[
                ["Company Name", "City", "State"],
                ["Acme Inc", "Detroit", "MI"],
            ]]},
            {"tables": [], "text": "Beta Industries\nChicago, IL 60601"},
        ])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            records, pages = await agent._extract_from_pdf(b"data", "PMA")

        assert pages == 2
        company_names = [r["company_name"] for r in records]
        assert "Acme Inc" in company_names
        assert "Beta Industries" in company_names

    def test_none_values_in_table(self):
        """Handles None values in table cells."""
        agent, _ = _make_agent()
        table = [
            ["Company Name", "City", "State", "Phone"],
            ["Acme Inc", None, "MI", "555-123-4567"],
            ["Beta LLC", "Chicago", None, None],
        ]
        records = agent._parse_table(table, "PMA")
        assert any(r["company_name"] == "Acme Inc" for r in records)
        acme = [r for r in records if r["company_name"] == "Acme Inc"][0]
        assert "city" not in acme  # None value skipped

    def test_default_association(self):
        """Uses 'unknown' when no association provided."""
        agent, _ = _make_agent()
        table = [
            ["Company Name", "City"],
            ["Acme Inc", "Detroit"],
        ]
        records = agent._parse_table(table, "unknown")
        assert records[0]["association"] == "unknown"

    @pytest.mark.asyncio
    async def test_run_default_association(self, mock_pdfplumber_pdf, sample_pdf_table):
        """run() uses 'unknown' when association not in task."""
        agent, mock_http = _make_agent()

        mock_response = MagicMock()
        mock_response.content = b"fake-pdf-bytes"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        mock_pdf = mock_pdfplumber_pdf([{"tables": [sample_pdf_table]}])
        mock_module = _make_mock_pdfplumber(mock_pdf)

        with patch.dict(sys.modules, {"pdfplumber": mock_module}):
            result = await agent.run({
                "pdf_url": "https://example.com/dir.pdf"
            })

        assert result["success"] is True
        assert result["records"][0]["association"] == "unknown"

    def test_website_not_confused_with_email(self):
        """Website regex doesn't match email addresses."""
        agent, _ = _make_agent()
        block = "Acme Manufacturing\ninfo@acme.com\nwww.acme.com"
        record = agent._parse_text_block(block, "PMA")
        assert record["email"] == "info@acme.com"
        assert record["website"] == "www.acme.com"

    def test_phone_various_formats(self):
        """Extracts phone from various formats."""
        agent, _ = _make_agent()

        # Dashes
        block1 = "Company A\n555-123-4567"
        r1 = agent._parse_text_block(block1, "PMA")
        assert r1["phone"] == "5551234567"

        # Dots
        block2 = "Company B\n555.123.4567"
        r2 = agent._parse_text_block(block2, "PMA")
        assert r2["phone"] == "5551234567"

        # Spaces
        block3 = "Company C\n555 123 4567"
        r3 = agent._parse_text_block(block3, "PMA")
        assert r3["phone"] == "5551234567"

    def test_city_state_without_zip(self):
        """Extracts city/state even without zip code."""
        agent, _ = _make_agent()
        block = "Acme Corp\nDetroit, MI"
        record = agent._parse_text_block(block, "PMA")
        assert record["city"] == "Detroit"
        assert record["state"] == "MI"

    def test_multiword_city(self):
        """Extracts multi-word city name."""
        agent, _ = _make_agent()
        block = "Acme Corp\nNew York, NY 10001"
        record = agent._parse_text_block(block, "PMA")
        assert record["city"] == "New York"
        assert record["state"] == "NY"
