"""
Tech Stack Agent
NAM Intelligence Pipeline

Detects ERP, CRM, MES, and other business software used by companies.
"""

import os
from urllib.parse import quote

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from skills.common.SKILL import extract_domain


class TechStackAgent(BaseAgent):
    """
    Tech Stack Agent - detects enterprise software.

    Responsibilities:
    - Query BuiltWith API for technology detection
    - Scrape job postings for ERP/software mentions
    - Analyze website source for technology fingerprints
    - Track detection rates
    """

    # ERP keywords by vendor
    ERP_KEYWORDS = {
        "SAP": ["SAP", "SAP S/4HANA", "SAP ECC", "SAP Business One", "SAP B1", "SAPUI5"],
        "Oracle": ["Oracle ERP", "Oracle Cloud", "JD Edwards", "NetSuite", "Oracle NetSuite"],
        "Epicor": ["Epicor", "Epicor Kinetic", "Epicor Prophet 21", "Epicor Eclipse"],
        "Infor": ["Infor", "Infor CloudSuite", "Infor M3", "Infor LN", "SyteLine", "Infor SyteLine"],
        "Microsoft Dynamics": ["Dynamics 365", "Dynamics AX", "Dynamics NAV", "Business Central", "D365"],
        "SYSPRO": ["SYSPRO"],
        "Plex": ["Plex", "Plex Manufacturing", "Plex MES"],
        "Acumatica": ["Acumatica"],
        "QAD": ["QAD", "QAD Adaptive ERP"],
        "IFS": ["IFS", "IFS Applications", "IFS Cloud"],
        "Global Shop Solutions": ["Global Shop Solutions", "GSS ERP"],
        "Sage": ["Sage", "Sage X3", "Sage Intacct", "Sage 100", "Sage 300"],
        "IQMS": ["IQMS", "DELMIAworks"],
        "JobBOSS": ["JobBOSS", "E2 Shop System"],
        "MAPICS": ["MAPICS"],
    }

    # CRM keywords
    CRM_KEYWORDS = {
        "Salesforce": ["Salesforce", "SFDC", "force.com"],
        "HubSpot": ["HubSpot"],
        "Microsoft Dynamics CRM": ["Dynamics CRM", "Dynamics 365 Sales"],
        "Zoho": ["Zoho CRM"],
        "Pipedrive": ["Pipedrive"],
        "SAP CRM": ["SAP CRM", "SAP C/4HANA"],
    }

    def _setup(self, **kwargs):
        """Initialize tech stack settings."""
        self.methods = self.agent_config.get("methods", ["builtwith", "website_fingerprint", "job_postings"])
        self.batch_size = self.agent_config.get("batch_size", 50)
        self.skip_if_exists = self.agent_config.get("skip_if_exists", True)
        self.enable_indeed_scraping = self.agent_config.get("enable_indeed_scraping", False)

    async def run(self, task: dict) -> dict:
        """
        Detect technology stack for records.

        Args:
            task: {
                "records": [{...}, ...]
            }

        Returns:
            {
                "success": True,
                "records": [{...enriched...}, ...],
                "detection_rate": 0.31
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

        self.log.info(f"Detecting tech stack for {len(records)} records")

        enriched_records = []
        detected = 0

        for i, record in enumerate(records):
            # Skip if already has ERP detected
            if self.skip_if_exists and record.get("erp_system"):
                enriched_records.append(record)
                continue

            domain = extract_domain(record.get("website", ""))
            company_name = record.get("company_name", "")

            if not domain and not company_name:
                enriched_records.append(record)
                continue

            # Try detection methods
            tech_data = None

            for method in self.methods:
                try:
                    if method == "builtwith" and domain:
                        tech_data = await self._detect_builtwith(domain)
                    elif method == "website_fingerprint" and domain:
                        tech_data = await self._detect_fingerprint(domain)
                    elif method == "job_postings" and company_name:
                        tech_data = await self._detect_job_postings(company_name)

                    if tech_data and tech_data.get("erp_system"):
                        detected += 1
                        break

                except Exception as e:
                    self.log.warning(f"{method} error for {company_name}: {e}")

            # Merge data
            if tech_data:
                record = {**record, **tech_data}

            enriched_records.append(record)

            if (i + 1) % 100 == 0:
                self.log.info(f"Processed {i + 1}/{len(records)} records")

        detection_rate = detected / len(records) if records else 0

        self.log.info(
            "Tech stack detection complete",
            detected=detected,
            total=len(records),
            detection_rate=f"{detection_rate:.1%}"
        )

        return {
            "success": True,
            "records": enriched_records,
            "detection_rate": detection_rate,
            "records_processed": len(records)
        }

    async def _detect_builtwith(self, domain: str) -> dict | None:
        """Detect tech stack using BuiltWith API."""
        api_key = os.getenv("BUILTWITH_API_KEY")
        if not api_key:
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

        except Exception as e:
            self.log.warning(
                "builtwith_detect_failed",
                provider="builtwith",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )

        return None

    async def _detect_fingerprint(self, domain: str) -> dict | None:
        """Detect tech stack by analyzing website source."""
        try:
            response = await self.http.get(
                f"https://{domain}",
                timeout=15,
                retries=1
            )

            if response.status_code != 200:
                return None

            html = response.text.lower()
            headers = dict(response.headers)

        except Exception as e:
            self.log.debug(
                "website_fingerprint_failed",
                provider="website_fingerprint",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

        tech_stack = []
        erp_system = None
        crm_system = None

        # Check for SAP indicators
        if any(s in html for s in ["sap-ui-core.js", "sapui5", "/sap/", "sap.ui."]):
            erp_system = "SAP"
            tech_stack.append("SAP")

        # Check for Salesforce
        if "salesforce.com" in html or "force.com" in html or ".sfdc" in html:
            crm_system = "Salesforce"
            tech_stack.append("Salesforce")

        # Check for Microsoft Dynamics
        if any(s in html for s in ["dynamics.com", "dynamics365", "d365"]):
            if not erp_system:
                erp_system = "Microsoft Dynamics"
            tech_stack.append("Microsoft Dynamics 365")

        # Check for Epicor
        if any(s in html for s in ["epicor", "kinetic.epicor"]):
            erp_system = "Epicor"
            tech_stack.append("Epicor")

        # Check for common web technologies
        tech_patterns = {
            "WordPress": ["/wp-content/", "/wp-includes/", "wordpress"],
            "Drupal": ["drupal", "/sites/default/"],
            "Shopify": ["shopify", "myshopify.com"],
            "HubSpot": ["hubspot", "hs-scripts.com"],
            "Google Analytics": ["google-analytics.com", "gtag", "ga.js"],
            "Google Tag Manager": ["googletagmanager.com", "gtm.js"],
            "React": ["react", "reactdom"],
            "Vue.js": ["vue.js", "vue.min.js"],
            "jQuery": ["jquery"],
            "Bootstrap": ["bootstrap"],
        }

        for tech_name, patterns in tech_patterns.items():
            if any(p in html for p in patterns):
                tech_stack.append(tech_name)

        # Check headers for tech clues
        server = headers.get("server", "").lower()
        if "nginx" in server:
            tech_stack.append("nginx")
        elif "apache" in server:
            tech_stack.append("Apache")
        elif "iis" in server:
            tech_stack.append("Microsoft IIS")

        if erp_system or crm_system or tech_stack:
            return {
                "tech_stack": list(set(tech_stack))[:20],
                "erp_system": erp_system,
                "crm_system": crm_system,
                "tech_source": "website_fingerprint"
            }

        return None

    async def _detect_job_postings(self, company_name: str) -> dict | None:
        """Detect ERP by analyzing job postings."""
        if not self.enable_indeed_scraping:
            self.log.warning("indeed_scraping_disabled", msg="Indeed scraping disabled by config; skipping")
            return None

        try:
            # Search Indeed for company jobs
            search_url = f"https://www.indeed.com/jobs?q={quote(company_name)}&l="
            response = await self.http.get(search_url, timeout=15, retries=1)

            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "lxml")
            job_text = soup.get_text().lower()

        except Exception as e:
            self.log.debug(
                "job_postings_scrape_failed",
                provider="job_postings",
                company_name=company_name,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

        # Count ERP mentions
        detected_erp = None
        max_count = 0

        for erp_name, keywords in self.ERP_KEYWORDS.items():
            count = 0
            for keyword in keywords:
                count += job_text.count(keyword.lower())

            if count > max_count:
                detected_erp = erp_name
                max_count = count

        # Need at least 2 mentions to be confident
        if max_count >= 2:
            return {
                "erp_system": detected_erp,
                "tech_source": "job_postings"
            }

        # Also check for CRM mentions
        detected_crm = None
        max_crm_count = 0

        for crm_name, keywords in self.CRM_KEYWORDS.items():
            count = 0
            for keyword in keywords:
                count += job_text.count(keyword.lower())

            if count > max_crm_count:
                detected_crm = crm_name
                max_crm_count = count

        if max_crm_count >= 2:
            return {
                "crm_system": detected_crm,
                "tech_source": "job_postings"
            }

        return None
