"""
Firmographic Agent
NAM Intelligence Pipeline

Enriches company records with firmographic data (employee count, revenue, industry, etc.)
from third-party providers and website scraping.
"""

import asyncio
import os
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from skills.common.SKILL import extract_domain


class FirmographicAgent(BaseAgent):
    """
    Firmographic Agent - adds company size and revenue data.

    Responsibilities:
    - Query Clearbit, ZoomInfo, Apollo APIs for firmographic data
    - Scrape company websites for About/Company pages
    - Parse employee count and revenue ranges
    - Track match rates
    """

    def _setup(self, **kwargs):
        """Initialize firmographic settings."""
        self.providers = self.agent_config.get("providers", ["clearbit", "apollo"])
        self.batch_size = self.agent_config.get("batch_size", 50)
        self.skip_if_exists = self.agent_config.get("skip_if_exists", True)

    async def run(self, task: dict) -> dict:
        """
        Enrich records with firmographic data.

        Args:
            task: {
                "records": [{...}, ...]
            }

        Returns:
            {
                "success": True,
                "records": [{...enriched...}, ...],
                "match_rate": 0.82
            }
        """
        records = task.get("records", [])

        if not records:
            return {
                "success": False,
                "error": "No records provided",
                "records": [],
                "records_processed": 0
            }

        self.log.info(f"Enriching {len(records)} records with firmographic data")

        enriched_records = []
        matched = 0

        for i, record in enumerate(records):
            # Skip if already enriched
            if self.skip_if_exists and record.get("employee_count_min"):
                enriched_records.append(record)
                continue

            domain = extract_domain(record.get("website", ""))
            company_name = record.get("company_name", "")

            if not domain and not company_name:
                enriched_records.append(record)
                continue

            # Try providers in order
            firmographic_data = None

            for provider in self.providers:
                try:
                    if provider == "clearbit" and domain:
                        firmographic_data = await self._fetch_clearbit(domain)
                    elif provider == "apollo" and domain:
                        firmographic_data = await self._fetch_apollo(domain)
                    elif provider == "zoominfo" and company_name:
                        firmographic_data = await self._fetch_zoominfo(company_name, domain)
                    elif provider == "website" and domain:
                        firmographic_data = await self._scrape_website(domain)

                    if firmographic_data:
                        matched += 1
                        break

                except Exception as e:
                    self.log.warning(f"{provider} error for {company_name}: {e}")
                    await asyncio.sleep(1)  # Brief pause on error

            # Merge data
            if firmographic_data:
                record = {**record, **firmographic_data}

            enriched_records.append(record)

            if (i + 1) % 100 == 0:
                self.log.info(f"Enriched {i + 1}/{len(records)} records")

        match_rate = matched / len(records) if records else 0

        self.log.info(
            f"Firmographic enrichment complete",
            matched=matched,
            total=len(records),
            match_rate=f"{match_rate:.1%}"
        )

        return {
            "success": True,
            "records": enriched_records,
            "match_rate": match_rate,
            "records_processed": len(records)
        }

    async def _fetch_clearbit(self, domain: str) -> Optional[dict]:
        """Fetch from Clearbit API."""
        api_key = os.getenv("CLEARBIT_API_KEY")
        if not api_key:
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
                    "firmographic_source": "clearbit"
                }

        except Exception:
            pass

        return None

    async def _fetch_apollo(self, domain: str) -> Optional[dict]:
        """Fetch from Apollo API."""
        api_key = os.getenv("APOLLO_API_KEY")
        if not api_key:
            return None

        try:
            response = await self.http.post(
                "https://api.apollo.io/v1/organizations/enrich",
                headers={"X-Api-Key": api_key},
                json={"domain": domain}
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

        except Exception:
            pass

        return None

    async def _fetch_zoominfo(self, company_name: str, domain: str = None) -> Optional[dict]:
        """Fetch from ZoomInfo API."""
        api_key = os.getenv("ZOOMINFO_API_KEY")
        if not api_key:
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

        except Exception:
            pass

        return None

    async def _scrape_website(self, domain: str) -> Optional[dict]:
        """Scrape company website for firmographic data."""
        about_paths = ["/about", "/about-us", "/company", "/who-we-are"]

        for path in about_paths:
            try:
                url = f"https://{domain}{path}"
                response = await self.http.get(url, timeout=15, retries=1)

                if response.status_code == 200:
                    data = self._parse_about_page(response.text)
                    if data:
                        data["firmographic_source"] = "website"
                        return data

            except Exception:
                continue

        return None

    def _parse_about_page(self, html: str) -> Optional[dict]:
        """Parse About page for firmographic data."""
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text()

        data = {}

        # Employee count patterns
        emp_patterns = [
            r'(\d{1,3}(?:,\d{3})*)\+?\s*employees?',
            r'team of\s*(\d+)',
            r'workforce of\s*(\d+)',
            r'over\s*(\d+)\s*(?:employees?|people|staff)',
        ]

        for pattern in emp_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                count = int(match.group(1).replace(',', ''))
                if 1 <= count <= 500000:
                    data["employee_count_min"] = count
                    data["employee_count_max"] = count
                    break

        # Year founded patterns
        founded_patterns = [
            r'(?:founded|established|since)\s*(?:in\s*)?(\d{4})',
            r'(\d{4})\s*(?:-|–)\s*present',
        ]

        for pattern in founded_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                year = int(match.group(1))
                if 1800 <= year <= datetime.now().year:
                    data["year_founded"] = year
                    break

        # Revenue patterns
        revenue_patterns = [
            r'\$(\d+(?:\.\d+)?)\s*(million|billion|M|B)',
            r'revenue\s*(?:of\s*)?\$(\d+(?:\.\d+)?)\s*(million|billion|M|B)?',
        ]

        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                value = float(match.group(1))
                multiplier = match.group(2) if len(match.groups()) > 1 else None

                if multiplier:
                    if multiplier.lower() in ['billion', 'b']:
                        value *= 1_000_000_000
                    elif multiplier.lower() in ['million', 'm']:
                        value *= 1_000_000

                if value >= 100000:  # At least $100k
                    data["revenue_min_usd"] = int(value)
                    break

        return data if data else None

    def _parse_revenue(self, revenue_str: str) -> Optional[int]:
        """Parse revenue string to integer."""
        if not revenue_str:
            return None

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
