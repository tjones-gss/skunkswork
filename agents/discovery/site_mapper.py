"""
Site Mapper Agent
NAM Intelligence Pipeline

Analyzes association websites to find member directories, detect pagination patterns,
and estimate member counts.
"""

import re
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

from bs4 import BeautifulSoup

from agents.base import BaseAgent


class SiteMapperAgent(BaseAgent):
    """
    Site Mapper Agent - discovers member directory structure.

    Responsibilities:
    - Check robots.txt compliance
    - Find directory URLs from common patterns or sitemap
    - Detect pagination type (query param, path segment, infinite scroll, etc.)
    - Estimate member count
    """

    DEFAULT_PATTERNS = [
        "/members",
        "/directory",
        "/member-list",
        "/member-directory",
        "/membership/members",
        "/membership/directory",
        "/membership/member-directory",
        "/membership/member-list",
        "/our-members",
        "/find-a-member",
        "/membership-directory",
        "/membership-list",
    ]

    def _setup(self, **kwargs):
        """Initialize site mapper specific settings."""
        self.max_depth = self.agent_config.get("max_depth", 3)
        self.patterns = self.agent_config.get("directory_patterns", self.DEFAULT_PATTERNS)

    async def run(self, task: dict) -> dict:
        """
        Map an association website to find member directory.

        Args:
            task: {
                "base_url": "https://pma.org",
                "directory_url": Optional[str],  # If known
                "association": "PMA"
            }

        Returns:
            {
                "success": True,
                "directory_url": "https://pma.org/directory/results.asp?n=2000",
                "pagination": {"type": "query_param", "param": "n"},
                "estimated_members": 1134,
                "auth_required": False
            }
        """
        base_url = task["base_url"]
        known_directory = task.get("directory_url")
        association = task.get("association", "unknown")

        self.log.info(f"Mapping site: {base_url}", association=association)

        # Step 1: Check robots.txt
        robots_parser = await self._fetch_robots_txt(base_url)

        # Step 2: Find directory URL
        if known_directory:
            directory_url = known_directory
            self.log.info(f"Using known directory URL: {directory_url}")
        else:
            directory_url = await self._find_directory_url(base_url, robots_parser)

        if not directory_url:
            return {
                "success": False,
                "error": f"Could not find member directory for {base_url}",
                "records_processed": 0
            }

        # Step 3: Check if directory is allowed by robots.txt
        if robots_parser and not robots_parser.can_fetch("NAM-IntelBot", directory_url):
            self.log.warning(f"robots.txt blocks access to {directory_url}")
            return {
                "success": False,
                "error": f"robots.txt blocks access to directory",
                "records_processed": 0
            }

        # Step 4: Fetch directory page and analyze
        try:
            response = await self.http.get(directory_url, timeout=30)

            if response.status_code != 200:
                return {
                    "success": False,
                    "error": f"Directory returned HTTP {response.status_code}",
                    "records_processed": 0
                }

            html = response.text
            soup = BeautifulSoup(html, "lxml")

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to fetch directory: {str(e)}",
                "records_processed": 0
            }

        # Step 5: Detect pagination
        pagination = self._detect_pagination(soup, directory_url)

        # Step 6: Estimate member count
        estimated_members = self._estimate_members(soup, html)

        # Step 7: Check for auth requirements
        auth_required = self._check_auth_required(soup)

        self.log.info(
            f"Site mapped successfully",
            directory_url=directory_url,
            pagination_type=pagination.get("type"),
            estimated_members=estimated_members,
            auth_required=auth_required
        )

        return {
            "success": True,
            "directory_url": directory_url,
            "pagination": pagination,
            "estimated_members": estimated_members,
            "auth_required": auth_required,
            "records_processed": 1
        }

    async def _fetch_robots_txt(self, base_url: str) -> RobotFileParser:
        """Fetch and parse robots.txt."""
        robots_url = f"{base_url.rstrip('/')}/robots.txt"

        try:
            response = await self.http.get(robots_url, timeout=10, retries=1)

            if response.status_code == 200:
                parser = RobotFileParser()
                parser.parse(response.text.splitlines())
                self.log.debug(f"robots.txt loaded from {robots_url}")
                return parser

        except Exception as e:
            self.log.debug(f"No robots.txt found: {e}")

        return None

    async def _find_directory_url(
        self,
        base_url: str,
        robots_parser: RobotFileParser
    ) -> str:
        """Find member directory URL using common patterns."""

        # Try common patterns
        for pattern in self.patterns:
            url = urljoin(base_url, pattern)

            # Check robots.txt
            if robots_parser and not robots_parser.can_fetch("NAM-IntelBot", url):
                continue

            try:
                response = await self.http.get(url, timeout=10, retries=1)

                if response.status_code == 200:
                    # Verify it looks like a directory page
                    if self._looks_like_directory(response.text):
                        self.log.info(f"Found directory at: {url}")
                        return url

            except Exception:
                continue

        # Try sitemap
        sitemap_url = await self._check_sitemap(base_url)
        if sitemap_url:
            return sitemap_url

        return None

    async def _check_sitemap(self, base_url: str) -> str:
        """Check sitemap.xml for directory URLs."""
        sitemap_url = f"{base_url.rstrip('/')}/sitemap.xml"

        try:
            response = await self.http.get(sitemap_url, timeout=10, retries=1)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "xml")

                # Look for URLs containing member-related keywords
                keywords = ["member", "directory", "company", "companies"]

                for loc in soup.find_all("loc"):
                    url = loc.get_text()
                    if any(kw in url.lower() for kw in keywords):
                        self.log.info(f"Found directory in sitemap: {url}")
                        return url

        except Exception:
            pass

        return None

    def _looks_like_directory(self, html: str) -> bool:
        """Check if HTML looks like a member directory."""
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text().lower()

        # Check for directory indicators
        indicators = [
            "member" in text and ("directory" in text or "list" in text),
            len(soup.find_all("a", href=True)) > 20,  # Many links
            soup.find(class_=re.compile(r"member|company|directory", re.I)) is not None,
            len(soup.find_all(["li", "tr", "div"], class_=True)) > 10,
        ]

        return sum(indicators) >= 2

    def _detect_pagination(self, soup: BeautifulSoup, current_url: str) -> dict:
        """Detect pagination type from page HTML."""

        # Check for query parameter pagination: ?page=2, ?n=100, ?p=2
        query_patterns = [
            (r'\?(page|p)=(\d+)', "page"),
            (r'\?(n|count|limit)=(\d+)', "count"),
            (r'\?(offset)=(\d+)', "offset"),
        ]

        for pattern, param_type in query_patterns:
            links = soup.find_all("a", href=re.compile(pattern))
            if links:
                match = re.search(pattern, links[0]["href"])
                if match:
                    param = match.group(1)
                    return {
                        "type": "query_param",
                        "param": param,
                        "param_type": param_type
                    }

        # Check for path-based pagination: /page/2, /page-2
        path_links = soup.find_all("a", href=re.compile(r'/page[-/]?\d+'))
        if path_links:
            return {
                "type": "path_segment",
                "pattern": "/page/{n}"
            }

        # Check for infinite scroll
        if soup.find(attrs={"data-infinite-scroll": True}):
            return {"type": "infinite_scroll"}

        if soup.find(attrs={"data-next-page": True}):
            return {"type": "infinite_scroll"}

        # Check for "Load More" button
        load_more = soup.find(
            ["button", "a"],
            string=re.compile(r'load\s*more|show\s*more|view\s*more', re.I)
        )
        if load_more:
            return {"type": "load_more"}

        # Check for "Next" link
        next_link = soup.find("a", string=re.compile(r'^next$|^>$|^>>$', re.I))
        if next_link:
            href = next_link.get("href", "")
            if "page" in href.lower():
                match = re.search(r'[?&](page|p)=', href)
                if match:
                    return {
                        "type": "query_param",
                        "param": match.group(1)
                    }

        # Check for pagination element
        pagination = soup.find(class_=re.compile(r'pagination|pager', re.I))
        if pagination:
            page_links = pagination.find_all("a", href=True)
            if page_links:
                # Analyze first pagination link
                href = page_links[0]["href"]
                match = re.search(r'[?&](\w+)=\d+', href)
                if match:
                    return {
                        "type": "query_param",
                        "param": match.group(1)
                    }

        # No pagination detected - single page
        return {"type": "none"}

    def _estimate_members(self, soup: BeautifulSoup, html: str) -> int:
        """Estimate number of members from page content."""

        # Look for explicit count in text
        text = soup.get_text()
        count_patterns = [
            r'(\d{1,5})\s*members?',
            r'showing\s*(\d+)',
            r'total[:\s]+(\d+)',
            r'(\d+)\s*results?',
            r'(\d+)\s*companies',
        ]

        for pattern in count_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                count = int(match.group(1))
                if 10 <= count <= 50000:  # Reasonable range
                    return count

        # Count list items that look like members
        member_containers = soup.find_all(
            class_=re.compile(r'member|company|listing|result', re.I)
        )
        if len(member_containers) >= 10:
            return len(member_containers)

        # Count table rows (excluding header)
        rows = soup.find_all("tr")
        if len(rows) > 5:
            return len(rows) - 1  # Exclude header

        # Count list items
        items = soup.find_all("li", class_=True)
        if len(items) >= 10:
            return len(items)

        return 0

    def _check_auth_required(self, soup: BeautifulSoup) -> bool:
        """Check if page requires authentication."""

        # Check for login form
        if soup.find("form", attrs={"action": re.compile(r'login|signin', re.I)}):
            return True

        # Check for login-related text
        text = soup.get_text().lower()
        auth_indicators = [
            "please log in",
            "sign in to view",
            "members only",
            "login required",
        ]

        return any(ind in text for ind in auth_indicators)
