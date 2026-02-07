"""
PDF Parser Agent
NAM Intelligence Pipeline

Extracts member data from PDF directories and annual reports.
"""

import io
import re
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional

from agents.base import BaseAgent


class PDFParserAgent(BaseAgent):
    """
    PDF Parser Agent - extracts data from PDF documents.

    Responsibilities:
    - Download PDFs from URLs
    - Extract tables from PDFs
    - Parse text content into records
    - Handle multi-page documents
    """

    def _setup(self, **kwargs):
        """Initialize PDF parser settings."""
        self.max_pages = self.agent_config.get("max_pages", 500)

    async def run(self, task: dict) -> dict:
        """
        Extract data from PDF.

        Args:
            task: {
                "pdf_url": "https://association.org/directory.pdf",
                or
                "pdf_path": "/path/to/directory.pdf",
                "association": "PMA"
            }

        Returns:
            {
                "success": True,
                "records": [{...}, ...],
                "pages_processed": 45
            }
        """
        pdf_url = task.get("pdf_url")
        pdf_path = task.get("pdf_path")
        association = task.get("association", "unknown")

        if not pdf_url and not pdf_path:
            return {
                "success": False,
                "error": "No pdf_url or pdf_path provided",
                "records": [],
                "records_processed": 0
            }

        # Get PDF content
        try:
            if pdf_url:
                self.log.info(f"Downloading PDF from {pdf_url}")
                response = await self.http.get(pdf_url, timeout=120)
                pdf_bytes = response.content
            else:
                pdf_bytes = Path(pdf_path).read_bytes()

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to load PDF: {str(e)}",
                "records": [],
                "records_processed": 0
            }

        # Extract records
        try:
            records, pages = await self._extract_from_pdf(pdf_bytes, association)
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to parse PDF: {str(e)}",
                "records": [],
                "records_processed": 0
            }

        self.log.info(f"Extracted {len(records)} records from {pages} pages")

        return {
            "success": True,
            "records": records,
            "pages_processed": pages,
            "records_processed": len(records)
        }

    async def _extract_from_pdf(
        self,
        pdf_bytes: bytes,
        association: str
    ) -> tuple[list[dict], int]:
        """Extract records from PDF bytes."""
        try:
            import pdfplumber
        except ImportError:
            self.log.error("pdfplumber not installed")
            return [], 0

        records = []
        pages_processed = 0

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages):
                if page_num >= self.max_pages:
                    break

                pages_processed += 1

                # Try table extraction first
                tables = page.extract_tables()

                if tables:
                    for table in tables:
                        table_records = self._parse_table(table, association)
                        records.extend(table_records)
                else:
                    # Fall back to text extraction
                    text = page.extract_text()
                    if text:
                        text_records = self._parse_text(text, association)
                        records.extend(text_records)

        return records, pages_processed

    def _parse_table(self, table: list[list], association: str) -> list[dict]:
        """Parse table rows into records."""
        if not table or len(table) < 2:
            return []

        # First row = headers
        raw_headers = table[0]
        headers = [self._normalize_header(h) for h in raw_headers]

        # Check if this looks like a member table
        if not self._is_member_table(headers):
            return []

        records = []

        for row in table[1:]:
            if len(row) != len(headers):
                continue

            record = {
                "association": association,
                "extracted_at": datetime.now(UTC).isoformat()
            }

            for header, value in zip(headers, row, strict=False):
                if header and value:
                    clean_value = str(value).strip()
                    if clean_value:
                        record[header] = clean_value

            if record.get("company_name"):
                records.append(record)

        return records

    def _normalize_header(self, header: str) -> Optional[str]:
        """Normalize table header to field name."""
        if not header:
            return None

        header = str(header).lower().strip()

        # Mapping of common header variations
        header_map = {
            "company": "company_name",
            "company name": "company_name",
            "name": "company_name",
            "member": "company_name",
            "member name": "company_name",
            "organization": "company_name",
            "city": "city",
            "state": "state",
            "st": "state",
            "province": "state",
            "country": "country",
            "phone": "phone",
            "telephone": "phone",
            "email": "email",
            "e-mail": "email",
            "website": "website",
            "web": "website",
            "url": "website",
            "membership": "membership_tier",
            "membership type": "membership_tier",
            "type": "membership_tier",
            "joined": "member_since",
            "member since": "member_since",
            "year joined": "member_since",
        }

        return header_map.get(header)

    def _is_member_table(self, headers: list[str]) -> bool:
        """Check if table appears to be a member listing."""
        # Must have company name column
        if "company_name" not in headers:
            return False

        # Should have at least one other relevant column
        relevant = ["city", "state", "phone", "email", "website"]
        return any(h in headers for h in relevant)

    def _parse_text(self, text: str, association: str) -> list[dict]:
        """Parse text content into records."""
        records = []

        # Try to identify company entries in text
        # Common patterns: "Company Name\nCity, State\nPhone"

        # Split into blocks (usually separated by double newlines)
        blocks = re.split(r'\n\s*\n', text)

        for block in blocks:
            record = self._parse_text_block(block, association)
            if record:
                records.append(record)

        return records

    def _parse_text_block(self, block: str, association: str) -> Optional[dict]:
        """Parse a text block that might be a company entry."""
        lines = block.strip().split('\n')

        if len(lines) < 2:
            return None

        record = {
            "association": association,
            "extracted_at": datetime.now(UTC).isoformat()
        }

        # First line is usually company name
        company_name = lines[0].strip()

        # Skip if it looks like a header or page number
        if re.match(r'^(page|member|directory|table)', company_name, re.I):
            return None
        if re.match(r'^\d+$', company_name):
            return None

        record["company_name"] = company_name

        # Parse remaining lines
        for line in lines[1:]:
            line = line.strip()

            # Phone pattern
            phone_match = re.search(r'[\(]?\d{3}[\)\-\.\s]?\d{3}[\-\.\s]?\d{4}', line)
            if phone_match:
                record["phone"] = re.sub(r'[^\d]', '', phone_match.group())
                continue

            # Email pattern
            email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', line)
            if email_match:
                record["email"] = email_match.group().lower()
                continue

            # Website pattern
            web_match = re.search(r'(?:https?://)?(?:www\.)?[\w\.-]+\.[a-z]{2,}', line, re.I)
            if web_match and '@' not in web_match.group():
                record["website"] = web_match.group()
                continue

            # City, State pattern
            city_state = re.match(r'^([A-Z][a-zA-Z\s]+),?\s+([A-Z]{2})(?:\s+\d{5})?$', line)
            if city_state:
                record["city"] = city_state.group(1).strip()
                record["state"] = city_state.group(2)

        return record if record.get("company_name") else None
