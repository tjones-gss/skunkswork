"""
HTML Parser Agent
NAM Intelligence Pipeline

Extracts structured company data from HTML pages using CSS/XPath selectors.
"""

import re
import time
from datetime import datetime, UTC
from typing import Any, Optional
from urllib.parse import urlparse

import yaml
from bs4 import BeautifulSoup

from agents.base import BaseAgent
from skills.common.SKILL import STATE_CODES, apply_parser


class HTMLParserAgent(BaseAgent):
    """
    HTML Parser Agent - extracts data from web pages.

    Responsibilities:
    - Load extraction schemas
    - Apply CSS/XPath selectors to extract data
    - Parse and normalize field values
    - Handle batch processing with rate limiting
    """

    def _setup(self, **kwargs):
        """Initialize parser settings."""
        self.batch_size = self.agent_config.get("batch_size", 100)
        self.concurrent_requests = self.agent_config.get("concurrent_requests", 5)
        self.default_schema = self.agent_config.get("default_schema", "default")
        self._schema_cache = {}

    async def run(self, task: dict) -> dict:
        """
        Extract data from URL(s) using schema.

        Args:
            task: {
                "url": "https://pma.org/member/12345",  # Single URL
                or
                "urls": ["https://...", ...],  # Multiple URLs
                "schema": "pma",
                "association": "PMA"
            }

        Returns:
            {
                "success": True,
                "records": [{...}, ...],
                "records_processed": 100
            }
        """
        schema_name = task.get("schema", self.default_schema)
        association = task.get("association", "unknown")

        # Handle single URL or batch
        if "url" in task:
            urls = [task["url"]]
        else:
            urls = task.get("urls", [])

        if not urls:
            return {
                "success": False,
                "error": "No URLs provided",
                "records": [],
                "records_processed": 0
            }

        # Load schema
        schema = self._load_schema(schema_name)

        self.log.info(
            f"Extracting from {len(urls)} URLs",
            schema=schema_name,
            association=association
        )

        records = []
        errors = []

        for i, url in enumerate(urls):
            try:
                record = await self._extract_from_url(url, schema, association)

                if record:
                    records.append(record)

                if (i + 1) % 100 == 0:
                    self.log.info(f"Extracted {i + 1}/{len(urls)} records")

            except Exception as e:
                errors.append({"url": url, "error": str(e)})
                self.log.warning(f"Failed to extract {url}: {e}")

        if errors:
            self.log.warning(f"Failed to extract {len(errors)} URLs")

        return {
            "success": True,
            "records": records,
            "errors": errors,
            "records_processed": len(records)
        }

    def _load_schema(self, schema_name: str) -> dict:
        """Load extraction schema from config."""
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]

        try:
            # Try loading schema file
            schema_path = self.config_path / "schemas" / f"{schema_name}.yaml"

            if schema_path.exists():
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema_config = yaml.safe_load(f)

                # Get the schema definition
                schema = schema_config.get(schema_name, {})

                # Handle extends
                if "extends" in schema:
                    base_schema = self._load_schema(schema["extends"])
                    merged = {**base_schema}
                    for key, value in schema.items():
                        if key != "extends":
                            merged[key] = value
                    schema = merged

                self._schema_cache[schema_name] = schema
                return schema

        except Exception as e:
            self.log.warning(f"Failed to load schema {schema_name}: {e}")

        # Load default schema
        return self._load_schema("default") if schema_name != "default" else {}

    async def _extract_from_url(
        self,
        url: str,
        schema: dict,
        association: str
    ) -> Optional[dict]:
        """Extract data from a single URL."""
        try:
            response = await self.http.get(url, timeout=30)

            if response.status_code != 200:
                self.log.warning(f"HTTP {response.status_code} for {url}")
                return None

            html = response.text
            soup = BeautifulSoup(html, "lxml")

        except Exception as e:
            self.log.warning(f"Failed to fetch {url}: {e}")
            return None

        # Extract record
        record = {
            "source_url": url,
            "association": association,
            "extracted_at": datetime.now(UTC).isoformat()
        }

        # Extract each field defined in schema
        for field_name, field_config in schema.items():
            if field_name.startswith("_") or field_name in ["extends", "list_container", "list_item"]:
                continue

            if not isinstance(field_config, dict):
                continue

            value = self._extract_field(soup, field_config)

            # Apply parser if specified
            parser_name = field_config.get("parser")
            if parser_name and value:
                value = apply_parser(value, parser_name)

            # Apply mapping if specified
            mapping = field_config.get("mapping")
            if mapping and value:
                value = mapping.get(value, value)

            # Apply enum validation
            enum_values = field_config.get("enum")
            if enum_values and value:
                if value not in enum_values:
                    # Try case-insensitive match
                    for ev in enum_values:
                        if ev.lower() == str(value).lower():
                            value = ev
                            break

            # Apply default
            if not value:
                value = field_config.get("default")

            if value:
                record[field_name] = value

        # Validate required fields
        company_name = record.get("company_name")
        if not company_name:
            self.log.warning(f"Missing company_name for {url}")
            return None

        return record

    def _extract_field(self, soup: BeautifulSoup, config: dict) -> Optional[str]:
        """Extract field value using selectors."""
        selectors = config.get("selectors", [])

        for selector in selectors:
            try:
                if selector.startswith("//"):
                    # XPath selector
                    value = self._extract_xpath(soup, selector, config)
                else:
                    # CSS selector
                    value = self._extract_css(soup, selector, config)

                if value:
                    return value

            except Exception:
                continue

        return None

    def _extract_css(
        self,
        soup: BeautifulSoup,
        selector: str,
        config: dict
    ) -> Optional[str]:
        """Extract using CSS selector."""
        element = soup.select_one(selector)

        if not element:
            return None

        extract_type = config.get("extract")

        if extract_type == "href":
            return element.get("href")
        elif extract_type == "src":
            return element.get("src")
        elif extract_type:
            return element.get(extract_type)
        else:
            return self._get_text(element)

    def _extract_xpath(
        self,
        soup: BeautifulSoup,
        xpath: str,
        config: dict
    ) -> Optional[str]:
        """Extract using XPath selector."""
        try:
            from lxml import etree

            tree = etree.HTML(str(soup))
            elements = tree.xpath(xpath)

            if not elements:
                return None

            element = elements[0]
            extract_type = config.get("extract")

            if extract_type:
                return element.get(extract_type)
            elif hasattr(element, "text"):
                return element.text
            else:
                return str(element)

        except ImportError:
            self.log.warning("lxml not installed, XPath not supported")
            return None
        except Exception:
            return None

    def _get_text(self, element) -> str:
        """Extract clean text from element."""
        if hasattr(element, "get_text"):
            text = element.get_text(strip=True)
        else:
            text = str(element)

        # Normalize whitespace
        text = " ".join(text.split())

        return text.strip()


class DirectoryParserAgent(HTMLParserAgent):
    """
    Directory Parser Agent - extracts multiple records from a directory page.

    Used when the directory shows all members on one page (or paginated pages)
    rather than individual profile pages.
    """

    async def _fetch_with_playwright(self, url: str) -> Optional[str]:
        """Fetch page using Playwright browser when httpx is blocked by WAF."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            self.log.warning("Playwright not installed, cannot bypass WAF")
            return None

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    )
                )
                response = await page.goto(url, wait_until="networkidle", timeout=30000)
                if response and response.status == 200:
                    html = await page.content()
                    await browser.close()
                    return html
                self.log.warning(f"Playwright got HTTP {response.status if response else 'None'} for {url}")
                await browser.close()
        except Exception as e:
            self.log.warning(f"Playwright fetch failed for {url}: {e}")

        return None

    async def run(self, task: dict) -> dict:
        """
        Extract all records from directory page(s).

        Args:
            task: {
                "url": "https://pma.org/directory/results.asp?n=2000",
                "schema": "pma",
                "association": "PMA"
            }
        """
        url = task.get("url")
        schema_name = task.get("schema", self.default_schema)
        association = task.get("association", "unknown")

        if not url:
            return {
                "success": False,
                "error": "No URL provided",
                "url": url,
                "attempted_methods": [],
                "duration_ms": 0,
                "records": [],
                "records_processed": 0,
            }

        # Load schema
        schema = self._load_schema(schema_name)

        self.log.info(
            f"Extracting directory from {url}",
            schema=schema_name,
            association=association,
        )

        start = time.monotonic()
        attempted_methods = []

        try:
            attempted_methods.append("httpx")
            response = await self.http.get(url, timeout=60)

            if response.status_code == 200:
                html = response.text
            elif response.status_code in (403, 503):
                # WAF or anti-bot block — fall back to Playwright
                self.log.info(f"HTTP {response.status_code}, retrying with Playwright: {url}")
                attempted_methods.append("playwright")
                html = await self._fetch_with_playwright(url)
                if not html:
                    duration_ms = int((time.monotonic() - start) * 1000)
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code} (Playwright fallback also failed)",
                        "url": url,
                        "attempted_methods": attempted_methods,
                        "duration_ms": duration_ms,
                        "records": [],
                        "records_processed": 0,
                    }
            else:
                duration_ms = int((time.monotonic() - start) * 1000)
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "url": url,
                    "attempted_methods": attempted_methods,
                    "duration_ms": duration_ms,
                    "records": [],
                    "records_processed": 0,
                }

            soup = BeautifulSoup(html, "lxml")

        except Exception as e:
            # Connection error — try Playwright as fallback
            self.log.info(f"HTTP request failed ({e}), trying Playwright: {url}")
            attempted_methods.append("playwright")
            html = await self._fetch_with_playwright(url)
            if not html:
                duration_ms = int((time.monotonic() - start) * 1000)
                return {
                    "success": False,
                    "error": str(e),
                    "url": url,
                    "attempted_methods": attempted_methods,
                    "duration_ms": duration_ms,
                    "records": [],
                    "records_processed": 0,
                }
            soup = BeautifulSoup(html, "lxml")

        # Find list container
        container_selector = schema.get("list_container", "body")
        container = soup.select_one(container_selector) or soup

        # Find list items
        item_selector = schema.get("list_item", "tr, li, .member-item")
        items = container.select(item_selector)

        records = []

        for item in items:
            record = self._extract_item(item, schema, association, url)
            if record and record.get("company_name"):
                records.append(record)

        # Also try auto-detection for inline directories (external company links)
        # Use whichever approach found more records
        auto_records = self._auto_extract_members(soup, association, url)
        if len(auto_records) > len(records):
            self.log.info(
                f"Auto-detection found {len(auto_records)} records vs "
                f"{len(records)} from schema, using auto-detected records"
            )
            records = auto_records

        duration_ms = int((time.monotonic() - start) * 1000)
        self.log.info(f"Extracted {len(records)} records from directory")

        return {
            "success": True,
            "records": records,
            "records_processed": len(records),
            "url": url,
            "attempted_methods": attempted_methods,
            "duration_ms": duration_ms,
        }

    def _extract_item(
        self,
        item: BeautifulSoup,
        schema: dict,
        association: str,
        source_url: str
    ) -> Optional[dict]:
        """Extract record from a single list item."""
        record = {
            "source_url": source_url,
            "association": association,
            "extracted_at": datetime.now(UTC).isoformat()
        }

        for field_name, field_config in schema.items():
            if field_name.startswith("_") or not isinstance(field_config, dict):
                continue

            if field_name in ["extends", "list_container", "list_item"]:
                continue

            value = self._extract_field(item, field_config)

            if field_config.get("parser") and value:
                value = apply_parser(value, field_config["parser"])

            if field_config.get("mapping") and value:
                value = field_config["mapping"].get(value, value)

            if not value:
                value = field_config.get("default")

            if value:
                record[field_name] = value

        return record if record.get("company_name") else None

    def _auto_extract_members(
        self,
        soup: BeautifulSoup,
        association: str,
        source_url: str
    ) -> list[dict]:
        """
        Auto-extract company entries from inline directory pages.

        Detects external company links (name + website) when no schema
        selectors are configured. Common for associations like SOCMA that
        list members as linked company names pointing to external websites.
        """
        source_domain = urlparse(source_url).netloc
        social_domains = {
            "facebook.com", "twitter.com", "linkedin.com", "youtube.com",
            "instagram.com", "x.com", "tiktok.com",
        }

        records = []
        seen_domains = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)

            # Skip empty, short, or navigation-style text
            if not text or len(text) < 2:
                continue

            # Skip non-HTTP links
            if href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue

            parsed = urlparse(href)

            # Must be an external link with a real domain
            if not parsed.netloc or not parsed.scheme.startswith("http"):
                continue

            # Skip links back to the association site
            bare_domain = parsed.netloc.lower().removeprefix("www.")
            source_bare = source_domain.lower().removeprefix("www.")
            if bare_domain == source_bare:
                continue

            # Skip social media links (exact domain match, not substring)
            if bare_domain in social_domains:
                continue

            # Deduplicate by domain
            if bare_domain in seen_domains:
                continue
            seen_domains.add(bare_domain)

            # Clean up company name (strip asterisks, extra whitespace)
            company_name = re.sub(r'[\*†‡]+$', '', text).strip()
            if not company_name:
                continue

            records.append({
                "company_name": company_name,
                "website": href,
                "domain": parsed.netloc,
                "association": association,
                "source_url": source_url,
                "extracted_at": datetime.now(UTC).isoformat(),
            })

        return records
