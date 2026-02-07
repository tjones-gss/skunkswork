"""
Contact Finder Agent
NAM Intelligence Pipeline

Identifies key decision-makers at target companies.
"""

import os
import re

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from skills.common.SKILL import extract_domain


class ContactFinderAgent(BaseAgent):
    """
    Contact Finder Agent - finds decision-maker contacts.

    Responsibilities:
    - Query Apollo/ZoomInfo for contacts
    - Scrape company Team/Leadership pages
    - Filter by target titles (CIO, VP IT, etc.)
    - Validate and deduplicate contacts
    """

    # Target title patterns (by priority)
    TARGET_TITLE_PATTERNS = [
        # Priority 1: Direct ERP decision makers
        (r"chief information officer|cio", 1),
        (r"vp?\s*(of\s*)?information technology|vp?\s*(of\s*)?it", 1),
        (r"it director|director.*\bit\b", 1),
        (r"erp manager|erp administrator", 1),
        # Priority 1: Key operational stakeholders
        (r"chief operating officer|coo", 1),
        (r"vp?\s*(of\s*)?operations", 1),
        # Priority 2: Budget authority
        (r"chief financial officer|cfo", 2),
        (r"controller|comptroller", 2),
        # Priority 2: Final approval
        (r"ceo|chief executive", 2),
        (r"president", 2),
        (r"owner", 2),
        # Priority 3: End user champions
        (r"plant manager", 3),
        (r"vp?\s*(of\s*)?manufacturing", 3),
        (r"director.*manufacturing", 3),
        (r"director.*operations", 3),
    ]

    def _setup(self, **kwargs):
        """Initialize contact finder settings."""
        self.providers = self.agent_config.get("providers", ["apollo", "website"])
        self.target_titles = self.agent_config.get("target_titles", [])
        self.max_contacts = self.agent_config.get("max_contacts_per_company", 5)
        self.batch_size = self.agent_config.get("batch_size", 50)

    async def run(self, task: dict) -> dict:
        """
        Find contacts for records.

        Args:
            task: {
                "records": [{...}, ...]
            }

        Returns:
            {
                "success": True,
                "records": [{...enriched...}, ...],
                "contacts_found": 156
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

        self.log.info(f"Finding contacts for {len(records)} records")

        enriched_records = []
        total_contacts = 0

        for i, record in enumerate(records):
            # Skip if already has contacts
            if record.get("contacts"):
                enriched_records.append(record)
                total_contacts += len(record["contacts"])
                continue

            domain = extract_domain(record.get("website", ""))
            company_name = record.get("company_name", "")

            if not domain and not company_name:
                enriched_records.append(record)
                continue

            # Collect contacts from all sources
            all_contacts = []

            for provider in self.providers:
                try:
                    if provider == "apollo" and domain:
                        contacts = await self._search_apollo(domain)
                        all_contacts.extend(contacts)
                    elif provider == "zoominfo" and company_name:
                        contacts = await self._search_zoominfo(company_name)
                        all_contacts.extend(contacts)
                    elif provider == "website" and domain:
                        contacts = await self._scrape_team_page(domain)
                        all_contacts.extend(contacts)

                except Exception as e:
                    self.log.warning(f"{provider} error for {company_name}: {e}")

            # Deduplicate contacts
            contacts = self._dedupe_contacts(all_contacts)

            # Filter to target titles
            contacts = [c for c in contacts if self._is_target_title(c.get("title", ""))]

            # Sort by title priority
            contacts = self._sort_by_priority(contacts)

            # Limit contacts
            record["contacts"] = contacts[:self.max_contacts]
            total_contacts += len(record["contacts"])

            enriched_records.append(record)

            if (i + 1) % 100 == 0:
                self.log.info(f"Processed {i + 1}/{len(records)} records")

        self.log.info(
            "Contact finding complete",
            total_contacts=total_contacts,
            companies_with_contacts=sum(1 for r in enriched_records if r.get("contacts"))
        )

        return {
            "success": True,
            "records": enriched_records,
            "contacts_found": total_contacts,
            "records_processed": len(records)
        }

    async def _search_apollo(self, domain: str) -> list[dict]:
        """Search Apollo for contacts."""
        api_key = os.getenv("APOLLO_API_KEY")
        if not api_key:
            return []

        try:
            response = await self.http.post(
                "https://api.apollo.io/v1/mixed_people/search",
                headers={"X-Api-Key": api_key},
                json={
                    "q_organization_domains": domain,
                    "person_titles": self.target_titles or [
                        "CIO", "VP IT", "IT Director", "COO", "CFO", "CEO"
                    ],
                    "page": 1,
                    "per_page": 10
                }
            )

            if response.status_code == 200:
                contacts = []
                for person in response.json().get("people", []):
                    contacts.append({
                        "name": person.get("name"),
                        "title": person.get("title"),
                        "email": person.get("email"),
                        "phone": person.get("phone_numbers", [{}])[0].get("number") if person.get("phone_numbers") else None,
                        "linkedin_url": person.get("linkedin_url"),
                        "source": "apollo"
                    })
                return contacts

        except Exception as e:
            self.log.warning(
                "apollo_contact_search_failed",
                provider="apollo",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )

        return []

    async def _search_zoominfo(self, company_name: str) -> list[dict]:
        """Search ZoomInfo for contacts."""
        api_key = os.getenv("ZOOMINFO_API_KEY")
        if not api_key:
            return []

        try:
            response = await self.http.get(
                "https://api.zoominfo.com/search/contact",
                params={
                    "companyName": company_name,
                    "jobTitle": "|".join(self.target_titles or ["CIO", "CFO", "CEO"]),
                },
                headers={"Authorization": f"Bearer {api_key}"}
            )

            if response.status_code == 200:
                contacts = []
                for person in response.json().get("data", []):
                    contacts.append({
                        "name": f"{person.get('firstName', '')} {person.get('lastName', '')}".strip(),
                        "title": person.get("jobTitle"),
                        "email": person.get("email"),
                        "phone": person.get("phone"),
                        "linkedin_url": person.get("linkedInUrl"),
                        "source": "zoominfo"
                    })
                return contacts

        except Exception as e:
            self.log.warning(
                "zoominfo_contact_search_failed",
                provider="zoominfo",
                company_name=company_name,
                error=str(e),
                error_type=type(e).__name__,
            )

        return []

    async def _scrape_team_page(self, domain: str) -> list[dict]:
        """Scrape company Team/Leadership page for contacts."""
        team_paths = [
            "/about/team", "/about/leadership", "/about-us/team",
            "/team", "/leadership", "/management", "/our-team",
            "/about/management", "/about/executives"
        ]

        for path in team_paths:
            try:
                url = f"https://{domain}{path}"
                response = await self.http.get(url, timeout=15, retries=1)

                if response.status_code == 200:
                    contacts = self._parse_team_page(response.text, domain)
                    if contacts:
                        return contacts

            except Exception as e:
                self.log.debug(
                    "team_page_scrape_failed",
                    provider="website",
                    domain=domain,
                    path=path,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                continue

        return []

    def _parse_team_page(self, html: str, domain: str) -> list[dict]:
        """Parse Team page for contact information."""
        soup = BeautifulSoup(html, "lxml")
        contacts = []

        # Look for team member cards/sections
        member_selectors = [
            ".team-member", ".executive", ".leader", ".staff-member",
            ".person", ".bio", "[class*='team']", "[class*='leadership']"
        ]

        for selector in member_selectors:
            members = soup.select(selector)
            if members:
                for member in members:
                    contact = self._extract_contact_from_element(member, domain)
                    if contact and contact.get("name"):
                        contacts.append(contact)

                if contacts:
                    break

        # If no structured data, try to find names and titles
        if not contacts:
            contacts = self._extract_contacts_from_text(soup, domain)

        return contacts

    def _extract_contact_from_element(self, element, domain: str) -> dict | None:
        """Extract contact info from a DOM element."""
        contact = {"source": "website"}

        # Extract name
        name_selectors = ["h3", "h4", ".name", ".title", "strong"]
        for sel in name_selectors:
            name_elem = element.select_one(sel)
            if name_elem:
                name = name_elem.get_text(strip=True)
                if name and len(name.split()) >= 2 and len(name) < 50:
                    contact["name"] = name
                    break

        # Extract title
        title_selectors = [".position", ".role", ".job-title", "p", "span"]
        for sel in title_selectors:
            title_elem = element.select_one(sel)
            if title_elem and title_elem != element.select_one(name_selectors[0] if name_selectors else ""):
                title = title_elem.get_text(strip=True)
                if title and self._looks_like_title(title):
                    contact["title"] = title
                    break

        # Extract email
        email_link = element.select_one("a[href^='mailto:']")
        if email_link:
            email = email_link.get("href", "").replace("mailto:", "").split("?")[0]
            if "@" in email:
                contact["email"] = email.lower()

        # Extract phone
        phone_link = element.select_one("a[href^='tel:']")
        if phone_link:
            phone = re.sub(r'[^\d+]', '', phone_link.get("href", ""))
            if len(phone) >= 10:
                contact["phone"] = phone

        # Extract LinkedIn
        linkedin_link = element.select_one("a[href*='linkedin.com']")
        if linkedin_link:
            contact["linkedin_url"] = linkedin_link.get("href")

        return contact if contact.get("name") else None

    def _extract_contacts_from_text(self, soup, domain: str) -> list[dict]:
        """Extract contacts from unstructured text."""
        contacts = []
        text = soup.get_text()

        # Pattern: Name\nTitle or Name, Title
        patterns = [
            r'([A-Z][a-z]+ [A-Z][a-z]+)\s*[,\n]\s*((?:Chief|VP|Vice President|Director|CEO|CFO|CIO|COO|President|Owner)[^,\n]{0,50})',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text)
            for name, title in matches:
                if self._looks_like_title(title):
                    contacts.append({
                        "name": name.strip(),
                        "title": title.strip(),
                        "source": "website"
                    })

        return contacts[:10]  # Limit

    def _looks_like_title(self, title: str) -> bool:
        """Check if string looks like a job title."""
        if not title or len(title) > 100:
            return False

        title_lower = title.lower()

        # Must contain title-like words
        title_words = [
            "president", "ceo", "cfo", "cio", "coo", "cto",
            "chief", "vice president", "vp", "director",
            "manager", "owner", "founder", "partner"
        ]

        return any(word in title_lower for word in title_words)

    def _is_target_title(self, title: str) -> bool:
        """Check if title matches target patterns."""
        if not title:
            return False

        title_lower = title.lower()
        return any(re.search(pattern, title_lower) for pattern, _ in self.TARGET_TITLE_PATTERNS)

    def _get_title_priority(self, title: str) -> int:
        """Get priority score for title (lower is better)."""
        if not title:
            return 999

        title_lower = title.lower()
        for pattern, priority in self.TARGET_TITLE_PATTERNS:
            if re.search(pattern, title_lower):
                return priority

        return 999

    def _sort_by_priority(self, contacts: list[dict]) -> list[dict]:
        """Sort contacts by title priority."""
        return sorted(contacts, key=lambda c: self._get_title_priority(c.get("title", "")))

    def _dedupe_contacts(self, contacts: list[dict]) -> list[dict]:
        """Deduplicate contacts by email or name."""
        seen = set()
        unique = []

        for contact in contacts:
            key = contact.get("email", "").lower() or contact.get("name", "").lower()
            if key and key not in seen:
                seen.add(key)
                unique.append(contact)

        return unique
