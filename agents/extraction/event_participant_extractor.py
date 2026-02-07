"""
Event Participant Extractor Agent
NAM Intelligence Pipeline

Extracts sponsor lists, exhibitor directories, attendee rosters,
and speaker information from event pages.
"""

import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from models.ontology import EventParticipant, ParticipantType, SponsorTier, Provenance
from middleware.policy import enforce_provenance, validate_json_output, auth_pages_flagged


class EventParticipantExtractorAgent(BaseAgent):
    """
    Event Participant Extractor Agent - extracts participants from event pages.

    Responsibilities:
    - Extract sponsor tiers (Platinum, Gold, Silver, Bronze)
    - Extract exhibitor details (booth numbers, categories)
    - Handle attendee/speaker rosters
    - Flag authentication requirements
    """

    # Sponsor tier patterns
    SPONSOR_TIER_PATTERNS = {
        SponsorTier.PLATINUM: ['platinum', 'diamond', 'premier', 'presenting', 'title'],
        SponsorTier.GOLD: ['gold'],
        SponsorTier.SILVER: ['silver'],
        SponsorTier.BRONZE: ['bronze'],
        SponsorTier.PARTNER: ['partner', 'strategic', 'associate'],
        SponsorTier.MEDIA: ['media', 'press'],
    }

    # Participant type indicators
    PARTICIPANT_INDICATORS = {
        ParticipantType.SPONSOR: ['sponsor', 'sponsors', 'sponsorship'],
        ParticipantType.EXHIBITOR: ['exhibitor', 'exhibitors', 'booth', 'vendor'],
        ParticipantType.SPEAKER: ['speaker', 'speakers', 'presenter', 'panelist'],
        ParticipantType.ATTENDEE: ['attendee', 'attendees', 'participant', 'registered'],
    }

    def _setup(self, **kwargs):
        """Initialize extractor settings."""
        self.max_participants = self.agent_config.get("max_participants", 500)

    @enforce_provenance
    @validate_json_output
    @auth_pages_flagged
    async def run(self, task: dict) -> dict:
        """
        Extract participants from an event page.

        Args:
            task: {
                "url": "https://pma.org/events/2024/sponsors",
                "html": "<html>...",  # Optional
                "event_id": "uuid",  # Optional: link to parent event
                "page_type": "SPONSORS_LIST" or "EXHIBITORS_LIST"
                "association": "PMA"
            }

        Returns:
            {
                "success": True,
                "participants": [EventParticipant, ...],
                "records_processed": 50
            }
        """
        url = task.get("url")
        html = task.get("html")
        event_id = task.get("event_id")
        page_type = task.get("page_type", "PARTICIPANTS_LIST")
        association = task.get("association", "unknown")

        if not url:
            return {
                "success": False,
                "error": "URL is required",
                "records": [],
                "records_processed": 0
            }

        # Fetch page if needed
        if not html:
            try:
                response = await self.http.get(url, timeout=30)
                if response.status_code != 200:
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}",
                        "records": [],
                        "records_processed": 0
                    }
                html = response.text
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e),
                    "records": [],
                    "records_processed": 0
                }

        soup = BeautifulSoup(html, "lxml")

        # Create provenance
        provenance = Provenance(
            source_url=url,
            source_type="web",
            extracted_by=self.agent_type,
            association_code=association,
            job_id=self.job_id
        )

        # Extract based on page type
        participants = []

        if page_type == "SPONSORS_LIST":
            participants = self._extract_sponsors(soup, url, event_id, provenance)
        elif page_type == "EXHIBITORS_LIST":
            participants = self._extract_exhibitors(soup, url, event_id, provenance)
        elif page_type == "PARTICIPANTS_LIST":
            # Auto-detect type
            page_text = soup.get_text().lower()
            if any(ind in page_text for ind in self.PARTICIPANT_INDICATORS[ParticipantType.SPONSOR]):
                participants = self._extract_sponsors(soup, url, event_id, provenance)
            elif any(ind in page_text for ind in self.PARTICIPANT_INDICATORS[ParticipantType.EXHIBITOR]):
                participants = self._extract_exhibitors(soup, url, event_id, provenance)
            elif any(ind in page_text for ind in self.PARTICIPANT_INDICATORS[ParticipantType.SPEAKER]):
                participants = self._extract_speakers(soup, url, event_id, provenance)
            else:
                # Try all extraction methods
                participants.extend(self._extract_sponsors(soup, url, event_id, provenance))
                participants.extend(self._extract_exhibitors(soup, url, event_id, provenance))

        self.log.info(
            f"Extracted {len(participants)} participants from {url}",
            page_type=page_type,
            association=association
        )

        # Convert to dicts
        participant_dicts = [p.model_dump() if hasattr(p, 'model_dump') else p for p in participants]

        return {
            "success": True,
            "records": participant_dicts,
            "participants": participant_dicts,
            "records_processed": len(participants)
        }

    def _extract_sponsors(
        self,
        soup: BeautifulSoup,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> list[EventParticipant]:
        """Extract sponsors organized by tier."""
        sponsors = []

        # Find sponsor sections by tier
        for tier, keywords in self.SPONSOR_TIER_PATTERNS.items():
            tier_sponsors = self._find_tier_sponsors(soup, tier, keywords, url, event_id, provenance)
            sponsors.extend(tier_sponsors)

        # If no tiered sponsors found, try generic extraction
        if not sponsors:
            sponsors = self._extract_generic_sponsors(soup, url, event_id, provenance)

        return sponsors[:self.max_participants]

    def _find_tier_sponsors(
        self,
        soup: BeautifulSoup,
        tier: SponsorTier,
        keywords: list[str],
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> list[EventParticipant]:
        """Find sponsors for a specific tier."""
        sponsors = []

        for keyword in keywords:
            # Find section headers
            headers = soup.find_all(
                ["h1", "h2", "h3", "h4", "h5", "div", "span"],
                string=re.compile(rf'\b{keyword}\b', re.I)
            )

            for header in headers:
                # Find sponsor logos/names following the header
                container = header.find_parent(["section", "div"])
                if not container:
                    container = header.find_next_sibling(["div", "ul", "section"])

                if container:
                    tier_sponsors = self._extract_sponsors_from_container(
                        container, tier, url, event_id, provenance
                    )
                    sponsors.extend(tier_sponsors)

            # Also check for elements with tier class
            tier_elements = soup.find_all(class_=re.compile(keyword, re.I))
            for elem in tier_elements:
                tier_sponsors = self._extract_sponsors_from_container(
                    elem, tier, url, event_id, provenance
                )
                sponsors.extend(tier_sponsors)

        return sponsors

    def _extract_sponsors_from_container(
        self,
        container,
        tier: SponsorTier,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> list[EventParticipant]:
        """Extract sponsors from a container element."""
        sponsors = []

        # Look for logos with alt text
        images = container.find_all("img")
        for img in images:
            alt = img.get("alt", "")
            if alt and len(alt) > 2:
                company_name = self._clean_company_name(alt)
                if company_name:
                    website = self._extract_website_from_link(img)
                    sponsors.append(EventParticipant(
                        event_id=event_id or "unknown",
                        participant_type=ParticipantType.SPONSOR,
                        company_name=company_name,
                        company_website=website,
                        sponsor_tier=tier,
                        provenance=[provenance]
                    ))

        # Look for linked text
        links = container.find_all("a")
        for link in links:
            text = link.get_text(strip=True)
            if text and len(text) > 2 and not text.startswith(("http", "www")):
                # Avoid duplicates from image alt
                if not any(s.company_name.lower() == text.lower() for s in sponsors):
                    website = link.get("href")
                    if website and not website.startswith("http"):
                        website = urljoin(url, website)

                    sponsors.append(EventParticipant(
                        event_id=event_id or "unknown",
                        participant_type=ParticipantType.SPONSOR,
                        company_name=text,
                        company_website=website,
                        sponsor_tier=tier,
                        provenance=[provenance]
                    ))

        return sponsors

    def _extract_generic_sponsors(
        self,
        soup: BeautifulSoup,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> list[EventParticipant]:
        """Extract sponsors without tier information."""
        sponsors = []

        # Find sponsor section
        sponsor_section = soup.find(class_=re.compile(r'sponsor', re.I))
        if not sponsor_section:
            sponsor_section = soup.find(id=re.compile(r'sponsor', re.I))

        if sponsor_section:
            return self._extract_sponsors_from_container(
                sponsor_section, SponsorTier.OTHER, url, event_id, provenance
            )

        return sponsors

    def _extract_exhibitors(
        self,
        soup: BeautifulSoup,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> list[EventParticipant]:
        """Extract exhibitors from an exhibitor list page."""
        exhibitors = []

        # Try table format
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                exhibitor = self._extract_exhibitor_from_row(row, url, event_id, provenance)
                if exhibitor:
                    exhibitors.append(exhibitor)

        if exhibitors:
            return exhibitors[:self.max_participants]

        # Try list format
        exhibitor_lists = soup.find_all(class_=re.compile(r'exhibitor', re.I))
        for exhibitor_list in exhibitor_lists:
            items = exhibitor_list.find_all(["li", "div", "article"])
            for item in items:
                exhibitor = self._extract_exhibitor_from_item(item, url, event_id, provenance)
                if exhibitor:
                    exhibitors.append(exhibitor)

        # Try card format
        if not exhibitors:
            cards = soup.find_all(class_=re.compile(r'exhibitor[-_]?card|vendor[-_]?card|booth[-_]?info', re.I))
            for card in cards:
                exhibitor = self._extract_exhibitor_from_item(card, url, event_id, provenance)
                if exhibitor:
                    exhibitors.append(exhibitor)

        return exhibitors[:self.max_participants]

    def _extract_exhibitor_from_row(
        self,
        row,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> Optional[EventParticipant]:
        """Extract exhibitor from a table row."""
        cells = row.find_all(["td", "th"])
        if len(cells) < 1:
            return None

        # First cell is usually company name
        company_name = cells[0].get_text(strip=True)
        if not company_name or len(company_name) < 2:
            return None

        # Look for booth number
        booth_number = None
        for cell in cells:
            text = cell.get_text(strip=True)
            booth_match = re.search(r'(?:booth|stand)\s*#?\s*(\w+)', text, re.I)
            if booth_match:
                booth_number = booth_match.group(1)
                break
            # Check for standalone booth number
            if re.match(r'^[A-Z]?\d{1,4}[A-Z]?$', text):
                booth_number = text
                break

        # Look for category
        category = None
        if len(cells) > 2:
            category = cells[-1].get_text(strip=True)
            if len(category) > 100:
                category = None

        # Look for website
        website = None
        link = row.find("a", href=True)
        if link:
            href = link.get("href")
            if href and "http" in href:
                website = href

        return EventParticipant(
            event_id=event_id or "unknown",
            participant_type=ParticipantType.EXHIBITOR,
            company_name=company_name,
            company_website=website,
            booth_number=booth_number,
            booth_category=category,
            provenance=[provenance]
        )

    def _extract_exhibitor_from_item(
        self,
        item,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> Optional[EventParticipant]:
        """Extract exhibitor from a list item or card."""
        # Extract company name
        name_elem = item.find(["h3", "h4", "h5", "strong", "b"])
        if not name_elem:
            name_elem = item.find("a")

        if not name_elem:
            return None

        company_name = name_elem.get_text(strip=True)
        if not company_name or len(company_name) < 2:
            return None

        # Extract booth number
        booth_number = None
        text = item.get_text()
        booth_match = re.search(r'(?:booth|stand)\s*#?\s*[:\-]?\s*(\w+)', text, re.I)
        if booth_match:
            booth_number = booth_match.group(1)

        # Extract category
        category = None
        cat_elem = item.find(class_=re.compile(r'category|type', re.I))
        if cat_elem:
            category = cat_elem.get_text(strip=True)

        # Extract website
        website = None
        link = item.find("a", href=re.compile(r'^https?://'))
        if link:
            website = link.get("href")

        return EventParticipant(
            event_id=event_id or "unknown",
            participant_type=ParticipantType.EXHIBITOR,
            company_name=company_name,
            company_website=website,
            booth_number=booth_number,
            booth_category=category,
            provenance=[provenance]
        )

    def _extract_speakers(
        self,
        soup: BeautifulSoup,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> list[EventParticipant]:
        """Extract speakers from a speakers page."""
        speakers = []

        # Find speaker cards/items
        speaker_elements = soup.find_all(class_=re.compile(r'speaker[-_]?card|presenter', re.I))

        if not speaker_elements:
            speaker_elements = soup.find_all(["article", "div"], class_=re.compile(r'speaker', re.I))

        for elem in speaker_elements[:self.max_participants]:
            speaker = self._extract_speaker_from_element(elem, url, event_id, provenance)
            if speaker:
                speakers.append(speaker)

        return speakers

    def _extract_speaker_from_element(
        self,
        elem,
        url: str,
        event_id: Optional[str],
        provenance: Provenance
    ) -> Optional[EventParticipant]:
        """Extract speaker from an element."""
        # Extract name
        name_elem = elem.find(class_=re.compile(r'name|title', re.I))
        if not name_elem:
            name_elem = elem.find(["h3", "h4", "h5"])

        if not name_elem:
            return None

        speaker_name = name_elem.get_text(strip=True)
        if not speaker_name or len(speaker_name) < 2:
            return None

        # Extract title/company
        title_elem = elem.find(class_=re.compile(r'title|position|role', re.I))
        speaker_title = title_elem.get_text(strip=True) if title_elem else None

        company_elem = elem.find(class_=re.compile(r'company|organization', re.I))
        company_name = company_elem.get_text(strip=True) if company_elem else None

        # If no company found, try to extract from title
        if not company_name and speaker_title and " at " in speaker_title:
            parts = speaker_title.split(" at ")
            speaker_title = parts[0].strip()
            company_name = parts[1].strip()

        if not company_name:
            company_name = "Unknown"

        # Extract presentation title
        pres_elem = elem.find(class_=re.compile(r'presentation|session|topic', re.I))
        presentation_title = pres_elem.get_text(strip=True) if pres_elem else None

        return EventParticipant(
            event_id=event_id or "unknown",
            participant_type=ParticipantType.SPEAKER,
            company_name=company_name,
            speaker_name=speaker_name,
            speaker_title=speaker_title,
            presentation_title=presentation_title,
            provenance=[provenance]
        )

    def _clean_company_name(self, name: str) -> Optional[str]:
        """Clean and validate company name."""
        if not name:
            return None

        # Remove common noise
        name = re.sub(r'\s*(logo|image|sponsor|partner)\s*', '', name, flags=re.I)
        name = name.strip()

        # Validate
        if len(name) < 2 or len(name) > 200:
            return None

        if name.lower() in ['logo', 'sponsor', 'partner', 'image', 'photo']:
            return None

        return name

    def _extract_website_from_link(self, element) -> Optional[str]:
        """Extract website URL from element or parent link."""
        # Check parent link
        parent = element.find_parent("a")
        if parent and parent.get("href"):
            href = parent.get("href")
            if href.startswith("http"):
                return href

        return None
