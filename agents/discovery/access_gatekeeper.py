"""
Access Gatekeeper Agent
NAM Intelligence Pipeline

Verifies legal and ethical access before crawling any website.
Checks robots.txt compliance, detects ToS restrictions,
identifies paywalls/login requirements.
"""

import re
from datetime import UTC, datetime
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from middleware.policy import crawler_only
from models.ontology import AccessVerdict


class AccessGatekeeperAgent(BaseAgent):
    """
    Access Gatekeeper Agent - verifies crawling permissions.

    Responsibilities:
    - Fetch and parse robots.txt
    - Detect Terms of Service restrictions
    - Identify login/paywall requirements
    - Output access verdicts with rate limits
    """

    # User agent for robots.txt compliance
    USER_AGENT = "NAM-IntelBot"

    # Auth indicators
    AUTH_INDICATORS = [
        "please log in",
        "sign in to view",
        "members only",
        "login required",
        "authentication required",
        "access denied",
        "subscription required",
        "premium content",
        "you must be logged in",
        "register to view",
        "create an account",
    ]

    # Paywall indicators
    PAYWALL_INDICATORS = [
        "subscribe to continue",
        "premium access",
        "unlock this content",
        "purchase required",
        "buy now to read",
        "upgrade your plan",
    ]

    # ToS patterns that may restrict crawling
    TOS_RESTRICTION_PATTERNS = [
        r"scraping.*(prohibited|forbidden|not allowed)",
        r"automated.*(access|collection).*(prohibited|forbidden)",
        r"data mining.*(prohibited|forbidden)",
        r"no.*(crawling|scraping|harvesting)",
        r"robots?.*(prohibited|forbidden)",
    ]

    def _setup(self, **kwargs):
        """Initialize gatekeeper settings."""
        self.default_crawl_delay = self.agent_config.get("default_crawl_delay", 2.0)
        self.default_daily_limit = self.agent_config.get("default_daily_limit", 1000)
        self.check_tos = self.agent_config.get("check_tos", False)  # ToS checking is optional

    async def run(self, task: dict) -> dict:
        """
        Check access permissions for a URL or domain.

        Args:
            task: {
                "url": "https://pma.org/members",
                or
                "domain": "pma.org",
                "check_page": True,  # Optional: also fetch and check the actual page
            }

        Returns:
            {
                "success": True,
                "verdict": AccessVerdict,
                "is_allowed": True/False,
                "reasons": ["robots.txt allows", ...],
            }
        """
        url = task.get("url")
        domain = task.get("domain")
        check_page = task.get("check_page", True)

        if not url and not domain:
            return {
                "success": False,
                "error": "Either 'url' or 'domain' must be provided",
                "records_processed": 0
            }

        # Build base URL from domain if only domain provided
        if domain and not url:
            url = f"https://{domain}"

        # Parse URL components
        parsed = urlparse(url)
        domain = parsed.netloc
        base_url = f"{parsed.scheme}://{domain}"

        self.log.info(f"Checking access for: {domain}")

        # Initialize verdict
        verdict = AccessVerdict(
            url=url,
            domain=domain,
            is_allowed=True,
            reasons=[]
        )

        # Step 1: Check robots.txt
        robots_result = await self._check_robots_txt(base_url, url)
        verdict.robots_txt_exists = robots_result["exists"]
        verdict.robots_txt_allows = robots_result["allows"]
        verdict.crawl_delay = robots_result.get("crawl_delay")

        if not robots_result["allows"]:
            verdict.is_allowed = False
            verdict.reasons.append(f"robots.txt disallows: {url}")

        if robots_result.get("crawl_delay"):
            verdict.suggested_rate = 1.0 / robots_result["crawl_delay"]
        else:
            verdict.suggested_rate = 1.0 / self.default_crawl_delay

        # Step 2: Check the actual page for auth requirements
        if check_page:
            page_result = await self._check_page(url)

            if page_result.get("requires_auth"):
                verdict.requires_auth = True
                verdict.auth_type = page_result.get("auth_type", "login")
                verdict.is_allowed = False
                verdict.reasons.append(f"Page requires authentication: {page_result.get('auth_indicator')}")

            if page_result.get("is_paywall"):
                verdict.requires_auth = True
                verdict.auth_type = "paywall"
                verdict.is_allowed = False
                verdict.reasons.append("Page is behind paywall")

        # Step 3: Check ToS if enabled
        if self.check_tos:
            tos_result = await self._check_tos(base_url)
            verdict.tos_reviewed = tos_result["reviewed"]
            verdict.tos_allows_crawling = tos_result.get("allows_crawling")

            if tos_result.get("restricts_crawling"):
                verdict.is_allowed = False
                verdict.reasons.append(f"ToS may restrict crawling: {tos_result.get('restriction')}")

        # Set daily limit based on domain
        verdict.daily_limit = self._get_daily_limit(domain)

        # Add positive reasons if allowed
        if verdict.is_allowed:
            if verdict.robots_txt_exists:
                verdict.reasons.append("robots.txt allows crawling")
            else:
                verdict.reasons.append("No robots.txt found (assuming allowed)")

            if not verdict.requires_auth:
                verdict.reasons.append("No authentication required")

        verdict.checked_at = datetime.now(UTC)

        self.log.info(
            f"Access verdict for {domain}",
            is_allowed=verdict.is_allowed,
            reasons=verdict.reasons
        )

        return {
            "success": True,
            "verdict": verdict.model_dump(),
            "is_allowed": verdict.is_allowed,
            "reasons": verdict.reasons,
            "suggested_rate": verdict.suggested_rate,
            "daily_limit": verdict.daily_limit,
            "records_processed": 1
        }

    @crawler_only
    async def _check_robots_txt(self, base_url: str, target_url: str) -> dict:
        """Fetch and parse robots.txt for the domain."""
        robots_url = f"{base_url.rstrip('/')}/robots.txt"

        result = {
            "exists": False,
            "allows": True,  # Default to allow if no robots.txt
            "crawl_delay": None,
            "sitemaps": []
        }

        try:
            response = await self.http.get(robots_url, timeout=10, retries=1)

            if response.status_code == 200:
                result["exists"] = True

                # Parse robots.txt
                parser = RobotFileParser()
                parser.parse(response.text.splitlines())

                # Check if our user agent can fetch the target URL
                result["allows"] = parser.can_fetch(self.USER_AGENT, target_url)

                # Also check generic * user agent
                if not result["allows"]:
                    result["allows"] = parser.can_fetch("*", target_url)

                # Get crawl delay
                crawl_delay = parser.crawl_delay(self.USER_AGENT)
                if crawl_delay is None:
                    crawl_delay = parser.crawl_delay("*")
                result["crawl_delay"] = crawl_delay

                # Extract sitemaps
                for line in response.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sitemap_url = line.split(":", 1)[1].strip()
                        result["sitemaps"].append(sitemap_url)

                self.log.debug(
                    "robots.txt parsed",
                    allows=result["allows"],
                    crawl_delay=result["crawl_delay"]
                )

            elif response.status_code == 404:
                self.log.debug(f"No robots.txt found at {robots_url}")
            else:
                self.log.warning(f"robots.txt returned status {response.status_code}")

        except Exception as e:
            self.log.debug(f"Error fetching robots.txt: {e}")

        return result

    @crawler_only
    async def _check_page(self, url: str) -> dict:
        """Check page for authentication requirements."""
        result = {
            "requires_auth": False,
            "auth_type": None,
            "auth_indicator": None,
            "is_paywall": False,
            "status_code": None
        }

        try:
            response = await self.http.get(url, timeout=15, retries=1)
            result["status_code"] = response.status_code

            # 401/403 indicates auth required
            if response.status_code in (401, 403):
                result["requires_auth"] = True
                result["auth_type"] = "http_auth"
                result["auth_indicator"] = f"HTTP {response.status_code}"
                return result

            if response.status_code != 200:
                return result

            html = response.text.lower()
            soup = BeautifulSoup(response.text, "lxml")

            # Check for login forms
            login_forms = soup.find_all("form", attrs={
                "action": re.compile(r'login|signin|authenticate', re.I)
            })
            if login_forms:
                result["requires_auth"] = True
                result["auth_type"] = "login"
                result["auth_indicator"] = "login form detected"
                return result

            # Check for auth indicators in text
            for indicator in self.AUTH_INDICATORS:
                if indicator in html:
                    result["requires_auth"] = True
                    result["auth_type"] = "login"
                    result["auth_indicator"] = indicator
                    return result

            # Check for paywall indicators
            for indicator in self.PAYWALL_INDICATORS:
                if indicator in html:
                    result["is_paywall"] = True
                    result["requires_auth"] = True
                    result["auth_type"] = "paywall"
                    result["auth_indicator"] = indicator
                    return result

        except Exception as e:
            self.log.warning(f"Error checking page {url}: {e}")

        return result

    @crawler_only
    async def _check_tos(self, base_url: str) -> dict:
        """Check Terms of Service for crawling restrictions."""
        result = {
            "reviewed": False,
            "allows_crawling": None,
            "restricts_crawling": False,
            "restriction": None
        }

        # Common ToS URL patterns
        tos_patterns = [
            "/terms",
            "/terms-of-service",
            "/terms-of-use",
            "/tos",
            "/legal/terms",
            "/legal",
        ]

        for pattern in tos_patterns:
            tos_url = urljoin(base_url, pattern)

            try:
                response = await self.http.get(tos_url, timeout=10, retries=1)

                if response.status_code == 200:
                    result["reviewed"] = True
                    html_lower = response.text.lower()

                    # Check for restriction patterns
                    for pattern in self.TOS_RESTRICTION_PATTERNS:
                        match = re.search(pattern, html_lower)
                        if match:
                            result["restricts_crawling"] = True
                            result["allows_crawling"] = False
                            result["restriction"] = match.group()
                            self.log.warning(
                                f"ToS may restrict crawling: {match.group()}"
                            )
                            return result

                    # If reviewed but no restrictions found
                    result["allows_crawling"] = True
                    return result

            except Exception:
                continue

        return result

    def _get_daily_limit(self, domain: str) -> int:
        """Get daily request limit for domain."""
        # Check configured limits
        limits = self.agent_config.get("daily_limits", {})

        if domain in limits:
            return limits[domain]

        # Check for association domains
        for assoc_domain, limit in limits.items():
            if domain.endswith(assoc_domain):
                return limit

        return self.default_daily_limit


class BatchAccessGatekeeperAgent(AccessGatekeeperAgent):
    """
    Batch version of Access Gatekeeper for checking multiple URLs.
    """

    async def run(self, task: dict) -> dict:
        """
        Check access for multiple URLs or domains.

        Args:
            task: {
                "urls": ["https://...", ...],
                or
                "domains": ["pma.org", ...],
                "check_pages": False,  # Skip individual page checks
            }
        """
        urls = task.get("urls", [])
        domains = task.get("domains", [])
        check_pages = task.get("check_pages", False)

        # Build URL list from domains
        if domains:
            urls.extend([f"https://{d}" for d in domains])

        if not urls:
            return {
                "success": False,
                "error": "No URLs or domains provided",
                "records_processed": 0
            }

        self.log.info(f"Checking access for {len(urls)} URLs")

        verdicts = []
        allowed = []
        blocked = []

        for url in urls:
            result = await super().run({
                "url": url,
                "check_page": check_pages
            })

            verdict = result.get("verdict", {})
            verdicts.append(verdict)

            if result.get("is_allowed"):
                allowed.append(url)
            else:
                blocked.append({
                    "url": url,
                    "reasons": result.get("reasons", [])
                })

        self.log.info(
            "Access check complete",
            total=len(urls),
            allowed=len(allowed),
            blocked=len(blocked)
        )

        return {
            "success": True,
            "verdicts": verdicts,
            "allowed_urls": allowed,
            "blocked_urls": blocked,
            "summary": {
                "total": len(urls),
                "allowed": len(allowed),
                "blocked": len(blocked)
            },
            "records_processed": len(urls)
        }
