"""
Firmographic Agent
NAM Intelligence Pipeline

Enriches company records with firmographic data (employee count, revenue, industry, etc.)
from third-party providers and website scraping.
"""

import asyncio
import re
from datetime import datetime

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

    # SEC EDGAR API base URL and required User-Agent header
    EDGAR_BASE = "https://efts.sec.gov/LATEST"
    EDGAR_DATA = "https://data.sec.gov"
    EDGAR_USER_AGENT = "NAM-Intel-Pipeline support@example.com"

    def _setup(self, **kwargs):
        """Initialize firmographic settings."""
        self.providers = self.agent_config.get("providers", ["clearbit", "apollo", "sec_edgar", "website"])
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
                    elif provider == "sec_edgar" and company_name:
                        firmographic_data = await self._fetch_sec_edgar(company_name)
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
            "Firmographic enrichment complete",
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

    async def _fetch_clearbit(self, domain: str) -> dict | None:
        """Fetch from Clearbit API."""
        api_key = self.get_secret("CLEARBIT_API_KEY")
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

        except Exception as e:
            self.log.warning(
                "clearbit_fetch_failed",
                provider="clearbit",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )

        return None

    async def _fetch_apollo(self, domain: str) -> dict | None:
        """Fetch from Apollo API."""
        api_key = self.get_secret("APOLLO_API_KEY")
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

        except Exception as e:
            self.log.warning(
                "apollo_fetch_failed",
                provider="apollo",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )

        return None

    async def _fetch_zoominfo(self, company_name: str, domain: str = None) -> dict | None:
        """Fetch from ZoomInfo API."""
        api_key = self.get_secret("ZOOMINFO_API_KEY")
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

        except Exception as e:
            self.log.warning(
                "zoominfo_fetch_failed",
                provider="zoominfo",
                company_name=company_name,
                error=str(e),
                error_type=type(e).__name__,
            )

        return None

    async def _fetch_sec_edgar(self, company_name: str) -> dict | None:
        """Fetch free firmographic data from SEC EDGAR (public companies only).

        SEC EDGAR provides authoritative financial data for publicly traded companies.
        Rate limit: 10 req/sec. Requires User-Agent with contact info.
        """
        try:
            # Step 1: Search for company by name using EDGAR full-text search
            search_url = f"{self.EDGAR_BASE}/search-index"
            params = {
                "q": f'"{company_name}"',
                "dateRange": "custom",
                "startdt": "2024-01-01",
                "forms": "10-K",
            }
            headers = {"User-Agent": self.EDGAR_USER_AGENT}

            response = await self.http.get(
                search_url, params=params, headers=headers, timeout=15, retries=1
            )

            if response.status_code != 200:
                return None

            data = response.json()
            hits = data.get("hits", {}).get("hits", [])

            if not hits:
                return None

            # Extract CIK from first result
            first_hit = hits[0].get("_source", {})
            entity_name = first_hit.get("entity_name", "")
            cik_raw = first_hit.get("entity_id")

            if not cik_raw:
                return None

            # Step 2: Get company submission data from EDGAR
            cik_padded = str(cik_raw).zfill(10)
            submission_url = f"{self.EDGAR_DATA}/submissions/CIK{cik_padded}.json"

            sub_response = await self.http.get(
                submission_url, headers=headers, timeout=15, retries=1
            )

            if sub_response.status_code != 200:
                return None

            sub_data = sub_response.json()

            sic_code = sub_data.get("sic")
            sic_description = sub_data.get("sicDescription")
            state_of_inc = sub_data.get("stateOfIncorporation")
            fiscal_year_end = sub_data.get("fiscalYearEnd")

            # Extract address
            addresses = sub_data.get("addresses", {})
            business_addr = addresses.get("business", {})
            city = business_addr.get("city")
            state = business_addr.get("stateOrCountry")

            result = {
                "firmographic_source": "sec_edgar",
                "sic_code": sic_code,
            }

            if sic_description:
                result["industry"] = sic_description
            if city:
                result["city"] = city.title()
            if state and len(state) == 2:
                result["state"] = state

            return result

        except Exception as e:
            self.log.warning(
                "sec_edgar_fetch_failed",
                provider="sec_edgar",
                company_name=company_name,
                error=str(e),
                error_type=type(e).__name__,
            )

        return None

    def _extract_schema_org(self, html: str) -> dict | None:
        """Extract company data from schema.org Organization JSON-LD in page source."""
        import json as json_mod

        soup = BeautifulSoup(html, "lxml")
        scripts = soup.find_all("script", type="application/ld+json")

        for script in scripts:
            try:
                ld_data = json_mod.loads(script.string or "")
            except (json_mod.JSONDecodeError, TypeError):
                continue

            # Handle @graph arrays
            items = []
            if isinstance(ld_data, list):
                items = ld_data
            elif isinstance(ld_data, dict):
                if ld_data.get("@graph"):
                    items = ld_data["@graph"]
                else:
                    items = [ld_data]

            for item in items:
                item_type = item.get("@type", "")
                if isinstance(item_type, list):
                    item_type = " ".join(item_type)

                if "Organization" not in item_type and "Corporation" not in item_type:
                    continue

                data = {}

                # Employee count
                employees = item.get("numberOfEmployees")
                if isinstance(employees, dict):
                    emp_val = employees.get("value")
                    if emp_val and str(emp_val).isdigit():
                        data["employee_count_min"] = int(emp_val)
                        data["employee_count_max"] = int(emp_val)
                elif isinstance(employees, (int, float)):
                    data["employee_count_min"] = int(employees)
                    data["employee_count_max"] = int(employees)

                # Founding date
                founded = item.get("foundingDate")
                if founded:
                    year_match = re.search(r"(\d{4})", str(founded))
                    if year_match:
                        year = int(year_match.group(1))
                        if 1800 <= year <= datetime.now().year:
                            data["year_founded"] = year

                # Address
                address = item.get("address")
                if isinstance(address, dict):
                    if address.get("addressLocality"):
                        data["city"] = address["addressLocality"]
                    if address.get("addressRegion"):
                        data["state"] = address["addressRegion"]

                if data:
                    data["firmographic_source"] = "schema_org"
                    return data

        return None

    async def _scrape_website(self, domain: str) -> dict | None:
        """Scrape company website for firmographic data."""
        about_paths = [
            "/about", "/about-us", "/company", "/who-we-are",
            "/our-story", "/company-profile", "/about/company",
        ]

        # First try the homepage for schema.org JSON-LD
        try:
            homepage = await self.http.get(f"https://{domain}", timeout=15, retries=1)
            if homepage.status_code == 200:
                schema_data = self._extract_schema_org(homepage.text)
                if schema_data:
                    return schema_data
        except Exception:
            pass

        for path in about_paths:
            try:
                url = f"https://{domain}{path}"
                response = await self.http.get(url, timeout=15, retries=1)

                if response.status_code == 200:
                    # Try schema.org first, then regex
                    schema_data = self._extract_schema_org(response.text)
                    if schema_data:
                        return schema_data

                    data = self._parse_about_page(response.text)
                    if data:
                        data["firmographic_source"] = "website"
                        return data

            except Exception as e:
                self.log.debug(
                    "website_scrape_failed",
                    provider="website",
                    domain=domain,
                    path=path,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                continue

        return None

    def _parse_about_page(self, html: str) -> dict | None:
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

    def _parse_revenue(self, revenue_str: str) -> int | None:
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

    def _build_linkedin_url(self, handle: str) -> str | None:
        """Build LinkedIn URL from handle."""
        if handle:
            return f"https://linkedin.com/company/{handle}"
        return None
