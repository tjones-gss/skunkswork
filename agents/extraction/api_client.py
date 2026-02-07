"""
API Client Agent
NAM Intelligence Pipeline

Fetches data from external APIs (Clearbit, BuiltWith, Apollo, ZoomInfo, etc.)
"""

import os
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from agents.base import BaseAgent


class APIClientAgent(BaseAgent):
    """
    API Client Agent - fetches data from external enrichment APIs.

    Responsibilities:
    - Query Clearbit for firmographic data
    - Query BuiltWith for technology detection
    - Query Apollo/ZoomInfo for contact data
    - Handle rate limits and quotas
    - Cache responses to reduce API calls
    """

    def _setup(self, **kwargs):
        """Initialize API client settings."""
        self.providers = self.agent_config.get("providers", {})
        self._cache = {}

    async def run(self, task: dict) -> dict:
        """
        Fetch data from specified API.

        Args:
            task: {
                "provider": "clearbit",
                "domain": "acme.com",
                or
                "company_name": "Acme Inc."
            }

        Returns:
            {
                "success": True,
                "data": {...},
                "provider": "clearbit"
            }
        """
        provider = task.get("provider", "clearbit")
        domain = task.get("domain")
        company_name = task.get("company_name")

        if not domain and not company_name:
            return {
                "success": False,
                "error": "No domain or company_name provided",
                "records_processed": 0
            }

        # Check cache
        cache_key = f"{provider}:{domain or company_name}"
        if cache_key in self._cache:
            return {
                "success": True,
                "data": self._cache[cache_key],
                "provider": provider,
                "cached": True,
                "records_processed": 1
            }

        # Fetch from provider
        data = None

        try:
            if provider == "clearbit":
                data = await self._fetch_clearbit(domain)
            elif provider == "builtwith":
                data = await self._fetch_builtwith(domain)
            elif provider == "apollo":
                data = await self._fetch_apollo(domain, company_name)
            elif provider == "zoominfo":
                data = await self._fetch_zoominfo(company_name, domain)
            else:
                return {
                    "success": False,
                    "error": f"Unknown provider: {provider}",
                    "records_processed": 0
                }

        except RateLimitError as e:
            return {
                "success": False,
                "error": f"Rate limited by {provider}",
                "retry_after": e.retry_after,
                "records_processed": 0
            }
        except APIError as e:
            return {
                "success": False,
                "error": str(e),
                "records_processed": 0
            }

        if data:
            self._cache[cache_key] = data

        return {
            "success": data is not None,
            "data": data,
            "provider": provider,
            "records_processed": 1 if data else 0
        }

    async def _fetch_clearbit(self, domain: str) -> Optional[dict]:
        """Fetch company data from Clearbit API."""
        api_key = os.getenv("CLEARBIT_API_KEY")

        if not api_key:
            self.log.warning("CLEARBIT_API_KEY not set")
            return None

        try:
            response = await self.http.get(
                "https://company.clearbit.com/v2/companies/find",
                params={"domain": domain},
                headers={"Authorization": f"Bearer {api_key}"}
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "employee_count_min": data.get("metrics", {}).get("employees"),
                    "employee_count_max": data.get("metrics", {}).get("employees"),
                    "revenue_min_usd": self._parse_revenue(
                        data.get("metrics", {}).get("estimatedAnnualRevenue")
                    ),
                    "year_founded": data.get("foundedYear"),
                    "naics_code": data.get("category", {}).get("naicsCode"),
                    "industry": data.get("category", {}).get("industry"),
                    "linkedin_url": self._build_linkedin_url(
                        data.get("linkedin", {}).get("handle")
                    ),
                    "description": data.get("description"),
                    "firmographic_source": "clearbit"
                }

            elif response.status_code == 404:
                return None
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RateLimitError("clearbit", retry_after)
            else:
                raise APIError("clearbit", response.status_code)

        except Exception as e:
            if not isinstance(e, (RateLimitError, APIError)):
                self.log.error(f"Clearbit error: {e}")
            raise

    async def _fetch_builtwith(self, domain: str) -> Optional[dict]:
        """Fetch technology stack from BuiltWith API."""
        api_key = os.getenv("BUILTWITH_API_KEY")

        if not api_key:
            self.log.warning("BUILTWITH_API_KEY not set")
            return None

        try:
            response = await self.http.get(
                "https://api.builtwith.com/v21/api.json",
                params={"KEY": api_key, "LOOKUP": domain}
            )

            if response.status_code == 200:
                data = response.json()
                technologies = []
                erp_system = None
                crm_system = None

                for result in data.get("Results", []):
                    for path in result.get("Result", {}).get("Paths", []):
                        for tech in path.get("Technologies", []):
                            name = tech.get("Name")
                            categories = tech.get("Categories", [])
                            technologies.append(name)

                            if any("ERP" in c for c in categories):
                                erp_system = name
                            if any("CRM" in c for c in categories):
                                crm_system = name

                return {
                    "tech_stack": technologies[:20],
                    "erp_system": erp_system,
                    "crm_system": crm_system,
                    "tech_source": "builtwith"
                }

            elif response.status_code == 429:
                raise RateLimitError("builtwith", 60)

        except Exception as e:
            if not isinstance(e, RateLimitError):
                self.log.error(f"BuiltWith error: {e}")
            raise

        return None

    async def _fetch_apollo(
        self,
        domain: str = None,
        company_name: str = None
    ) -> Optional[dict]:
        """Fetch data from Apollo API."""
        api_key = os.getenv("APOLLO_API_KEY")

        if not api_key:
            self.log.warning("APOLLO_API_KEY not set")
            return None

        try:
            # Company enrichment
            response = await self.http.post(
                "https://api.apollo.io/v1/organizations/enrich",
                headers={"X-Api-Key": api_key},
                json={"domain": domain} if domain else {"name": company_name}
            )

            if response.status_code == 200:
                data = response.json().get("organization", {})
                return {
                    "employee_count_min": data.get("estimated_num_employees"),
                    "employee_count_max": data.get("estimated_num_employees"),
                    "year_founded": data.get("founded_year"),
                    "industry": data.get("industry"),
                    "linkedin_url": data.get("linkedin_url"),
                    "firmographic_source": "apollo"
                }

            elif response.status_code == 429:
                raise RateLimitError("apollo", 60)

        except Exception as e:
            if not isinstance(e, RateLimitError):
                self.log.error(f"Apollo error: {e}")
            raise

        return None

    async def _fetch_zoominfo(
        self,
        company_name: str,
        domain: str = None
    ) -> Optional[dict]:
        """Fetch data from ZoomInfo API."""
        api_key = os.getenv("ZOOMINFO_API_KEY")

        if not api_key:
            self.log.warning("ZOOMINFO_API_KEY not set")
            return None

        try:
            params = {"companyName": company_name}
            if domain:
                params["domain"] = domain

            response = await self.http.get(
                "https://api.zoominfo.com/search/company",
                params=params,
                headers={"Authorization": f"Bearer {api_key}"}
            )

            if response.status_code == 200:
                data = response.json().get("data", [{}])[0]
                return {
                    "employee_count_min": data.get("employeeCount"),
                    "employee_count_max": data.get("employeeCount"),
                    "revenue_min_usd": (data.get("revenueInMillions") or 0) * 1_000_000,
                    "year_founded": data.get("yearFounded"),
                    "naics_code": data.get("naicsCode"),
                    "firmographic_source": "zoominfo"
                }

            elif response.status_code == 429:
                raise RateLimitError("zoominfo", 60)

        except Exception as e:
            if not isinstance(e, RateLimitError):
                self.log.error(f"ZoomInfo error: {e}")
            raise

        return None

    def _parse_revenue(self, revenue_str: str) -> Optional[int]:
        """Parse revenue string to integer."""
        if not revenue_str:
            return None

        # Handle ranges like "$10M-$50M"
        match = re.search(r'\$?(\d+(?:\.\d+)?)\s*([MBK])?', revenue_str, re.I)
        if match:
            value = float(match.group(1))
            multiplier = match.group(2)

            if multiplier:
                if multiplier.upper() == 'B':
                    value *= 1_000_000_000
                elif multiplier.upper() == 'M':
                    value *= 1_000_000
                elif multiplier.upper() == 'K':
                    value *= 1_000

            return int(value)

        return None

    def _build_linkedin_url(self, handle: str) -> Optional[str]:
        """Build LinkedIn URL from handle."""
        if handle:
            return f"https://linkedin.com/company/{handle}"
        return None


class RateLimitError(Exception):
    """Rate limit exceeded error."""

    def __init__(self, provider: str, retry_after: int = 60):
        self.provider = provider
        self.retry_after = retry_after
        super().__init__(f"Rate limited by {provider}, retry after {retry_after}s")


class APIError(Exception):
    """API error."""

    def __init__(self, provider: str, status_code: int):
        self.provider = provider
        self.status_code = status_code
        super().__init__(f"{provider} API error: HTTP {status_code}")
