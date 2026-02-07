"""
Link Crawler Agent
NAM Intelligence Pipeline

Crawls member directory pages following pagination to discover all member URLs.
"""

import asyncio
import json
import re
from datetime import datetime, UTC
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

from bs4 import BeautifulSoup

from agents.base import BaseAgent


class LinkCrawlerAgent(BaseAgent):
    """
    Link Crawler Agent - discovers all member URLs from a directory.

    Responsibilities:
    - Follow pagination to collect all member profile/listing URLs
    - Handle different pagination types (query param, path, infinite scroll)
    - Respect rate limits
    - Deduplicate URLs
    """

    def _setup(self, **kwargs):
        """Initialize crawler settings."""
        self.max_pages = self.agent_config.get("max_pages", 200)
        self.batch_size = self.agent_config.get("batch_size", 50)
        self.concurrent_requests = self.agent_config.get("concurrent_requests", 3)

    async def run(self, task: dict) -> dict:
        """
        Crawl directory to discover all member URLs.

        Args:
            task: {
                "entry_url": "https://pma.org/directory/results.asp?n=2000",
                "pagination": {"type": "query_param", "param": "n"},
                "association": "PMA"
            }

        Returns:
            {
                "success": True,
                "member_urls": ["https://...", ...],
                "total_pages": 12,
                "total_urls": 1134
            }
        """
        entry_url = task["entry_url"]
        pagination = task.get("pagination", {"type": "none"})
        association = task.get("association", "unknown")

        self.log.info(
            f"Starting crawl",
            entry_url=entry_url,
            pagination_type=pagination.get("type"),
            association=association
        )

        member_urls = set()
        pages_crawled = 0

        # Handle different pagination types
        pagination_type = pagination.get("type", "none")

        if pagination_type == "infinite_scroll":
            # Use Playwright for JS-rendered pages
            urls = await self._crawl_infinite_scroll(entry_url)
            member_urls.update(urls)
            pages_crawled = 1

        elif pagination_type == "load_more":
            # Similar to infinite scroll
            urls = await self._crawl_load_more(entry_url)
            member_urls.update(urls)
            pages_crawled = 1

        elif pagination_type in ["query_param", "path_segment"]:
            # Standard pagination
            urls, pages = await self._crawl_paginated(entry_url, pagination)
            member_urls.update(urls)
            pages_crawled = pages

        else:
            # Single page - no pagination
            html = await self._fetch_page(entry_url)
            if html:
                urls = self._extract_member_urls(html, entry_url)
                member_urls.update(urls)
                pages_crawled = 1

        # Convert to sorted list
        member_urls = sorted(list(member_urls))

        # Save URLs to file
        output_path = f"data/raw/{association}/urls.jsonl"
        self._save_urls(member_urls, output_path)

        self.log.info(
            f"Crawl complete",
            total_urls=len(member_urls),
            pages_crawled=pages_crawled,
            association=association
        )

        return {
            "success": True,
            "member_urls": member_urls,
            "total_pages": pages_crawled,
            "total_urls": len(member_urls),
            "output_path": output_path,
            "records_processed": pages_crawled
        }

    async def _fetch_page(self, url: str) -> str:
        """Fetch a page with error handling."""
        try:
            response = await self.http.get(url, timeout=30)

            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                self.log.debug(f"Page not found: {url}")
                return None
            else:
                self.log.warning(f"HTTP {response.status_code} for {url}")
                return None

        except Exception as e:
            self.log.warning(f"Failed to fetch {url}: {e}")
            return None

    async def _crawl_paginated(
        self,
        entry_url: str,
        pagination: dict
    ) -> tuple[set, int]:
        """Crawl paginated directory."""
        member_urls = set()
        page = 1
        consecutive_empty = 0

        while page <= self.max_pages and consecutive_empty < 3:
            # Build page URL
            page_url = self._build_page_url(entry_url, pagination, page)

            self.log.debug(f"Fetching page {page}: {page_url}")

            html = await self._fetch_page(page_url)

            if not html:
                consecutive_empty += 1
                page += 1
                continue

            # Extract URLs from page
            new_urls = self._extract_member_urls(html, entry_url)

            if not new_urls:
                consecutive_empty += 1
            else:
                consecutive_empty = 0
                member_urls.update(new_urls)

            self.log.info(
                f"Page {page}: found {len(new_urls)} URLs, total: {len(member_urls)}"
            )

            # Check for next page
            if not self._has_next_page(html, pagination, page):
                self.log.debug(f"No more pages after page {page}")
                break

            page += 1

        return member_urls, page

    def _build_page_url(self, base_url: str, pagination: dict, page: int) -> str:
        """Build URL for specific page number."""
        pag_type = pagination.get("type")
        param = pagination.get("param", "page")
        param_type = pagination.get("param_type", "page")

        if pag_type == "query_param":
            parsed = urlparse(base_url)
            query = parse_qs(parsed.query)

            # Calculate value based on param type
            if param_type == "offset":
                value = (page - 1) * 100  # Assume 100 per page
            elif param_type == "count":
                value = page * 100
            else:
                value = page

            query[param] = [str(value)]

            new_query = urlencode(query, doseq=True)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"

        elif pag_type == "path_segment":
            pattern = pagination.get("pattern", "/page/{n}")
            suffix = pattern.replace("{n}", str(page))

            # Remove existing page suffix if present
            base = re.sub(r'/page[-/]?\d+', '', base_url)
            return base.rstrip('/') + suffix

        return base_url

    def _has_next_page(self, html: str, pagination: dict, current_page: int) -> bool:
        """Check if there's a next page."""
        soup = BeautifulSoup(html, "lxml")

        # Check for "Next" link
        next_link = soup.find("a", string=re.compile(r'next|›|»', re.I))
        if next_link and next_link.get("href"):
            return True

        # Check for numbered pagination
        pagination_elem = soup.find(class_=re.compile(r'pagination|pager', re.I))
        if pagination_elem:
            # Look for page number higher than current
            page_links = pagination_elem.find_all("a", string=re.compile(r'^\d+$'))
            for link in page_links:
                try:
                    page_num = int(link.get_text())
                    if page_num > current_page:
                        return True
                except ValueError:
                    continue

        # Check for disabled "next" button (indicates last page)
        disabled_next = soup.find(
            ["a", "button", "span"],
            class_=re.compile(r'disabled|inactive', re.I),
            string=re.compile(r'next', re.I)
        )
        if disabled_next:
            return False

        # Default: assume there's more if we found content
        return True

    def _extract_member_urls(self, html: str, base_url: str) -> set:
        """Extract member profile/listing URLs from HTML."""
        soup = BeautifulSoup(html, "lxml")
        urls = set()
        base_domain = urlparse(base_url).netloc

        # Common patterns for member links
        patterns = [
            # URL path patterns
            re.compile(r'/member/|/company/|/profile/|/organization/', re.I),
            # Class patterns
            re.compile(r'member|company|profile|listing', re.I),
        ]

        # Find links with matching href patterns
        for link in soup.find_all("a", href=True):
            href = link["href"]

            # Skip non-content links
            if self._should_skip_url(href):
                continue

            # Check if href matches member patterns
            if any(p.search(href) for p in patterns[:1]):
                url = urljoin(base_url, href)
                if urlparse(url).netloc == base_domain:
                    urls.add(url)
                continue

            # Check if link has member-related class
            link_class = " ".join(link.get("class", []))
            if patterns[1].search(link_class):
                url = urljoin(base_url, href)
                if urlparse(url).netloc == base_domain:
                    urls.add(url)

        # Find links in member containers
        containers = soup.find_all(
            class_=re.compile(r'member-|company-|directory-|listing-|result-', re.I)
        )
        for container in containers:
            for link in container.find_all("a", href=True):
                href = link["href"]
                if not self._should_skip_url(href):
                    url = urljoin(base_url, href)
                    if urlparse(url).netloc == base_domain:
                        urls.add(url)

        # Find links in tables (common for member directories)
        for row in soup.find_all("tr"):
            links = row.find_all("a", href=True)
            if links:
                # Usually first link in row is the member link
                href = links[0]["href"]
                if not self._should_skip_url(href):
                    url = urljoin(base_url, href)
                    if urlparse(url).netloc == base_domain:
                        urls.add(url)

        return urls

    def _should_skip_url(self, url: str) -> bool:
        """Check if URL should be skipped."""
        skip_patterns = [
            r'^#',  # Anchor links
            r'^javascript:',
            r'^mailto:',
            r'^tel:',
            r'\.(pdf|doc|docx|xls|xlsx|zip)$',
            r'/login',
            r'/register',
            r'/contact',
            r'/about',
            r'/faq',
            r'/privacy',
            r'/terms',
            r'/search',
            r'/join',
            r'/membership$',  # Membership page itself
            r'facebook\.com',
            r'twitter\.com',
            r'linkedin\.com/company',
            r'youtube\.com',
        ]

        url_lower = url.lower()
        return any(re.search(p, url_lower) for p in skip_patterns)

    async def _crawl_infinite_scroll(self, url: str) -> set:
        """Crawl infinite scroll page using Playwright."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            self.log.warning("Playwright not installed, falling back to static scrape")
            html = await self._fetch_page(url)
            return self._extract_member_urls(html, url) if html else set()

        member_urls = set()

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()

                await page.goto(url, wait_until="networkidle")

                prev_count = 0
                no_change = 0
                max_scrolls = 50

                for _ in range(max_scrolls):
                    # Extract current URLs
                    links = await page.evaluate('''
                        () => Array.from(document.querySelectorAll('a'))
                            .map(a => a.href)
                    ''')

                    for href in links:
                        if not self._should_skip_url(href):
                            if urlparse(href).netloc == urlparse(url).netloc:
                                member_urls.add(href)

                    # Check for new content
                    if len(member_urls) == prev_count:
                        no_change += 1
                        if no_change >= 3:
                            break
                    else:
                        no_change = 0

                    prev_count = len(member_urls)

                    # Scroll down
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await page.wait_for_timeout(1500)

                await browser.close()

        except Exception as e:
            self.log.error(f"Playwright crawl failed: {e}")

        return member_urls

    async def _crawl_load_more(self, url: str) -> set:
        """Crawl page with Load More button using Playwright."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            self.log.warning("Playwright not installed, falling back to static scrape")
            html = await self._fetch_page(url)
            return self._extract_member_urls(html, url) if html else set()

        member_urls = set()

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()

                await page.goto(url, wait_until="networkidle")

                max_clicks = 50

                for _ in range(max_clicks):
                    # Extract current URLs
                    links = await page.evaluate('''
                        () => Array.from(document.querySelectorAll('a'))
                            .map(a => a.href)
                    ''')

                    for href in links:
                        if not self._should_skip_url(href):
                            if urlparse(href).netloc == urlparse(url).netloc:
                                member_urls.add(href)

                    # Try to click Load More button
                    try:
                        load_more = await page.query_selector(
                            'button:has-text("Load More"), '
                            'button:has-text("Show More"), '
                            'a:has-text("Load More"), '
                            'a:has-text("Show More")'
                        )

                        if load_more:
                            await load_more.click()
                            await page.wait_for_timeout(2000)
                        else:
                            break

                    except Exception:
                        break

                await browser.close()

        except Exception as e:
            self.log.error(f"Playwright crawl failed: {e}")

        return member_urls

    def _save_urls(self, urls: list, output_path: str):
        """Save discovered URLs to JSONL file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            for url in urls:
                record = {
                    "url": url,
                    "discovered_at": datetime.now(UTC).isoformat()
                }
                f.write(json.dumps(record) + "\n")

        self.log.info(f"Saved {len(urls)} URLs to {output_path}")
