"""
Event Extractor Agent
NAM Intelligence Pipeline

Extracts structured event data from association websites.
Handles conferences, trade shows, webinars, and member events.
"""

import re

from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from agents.base import BaseAgent
from middleware.policy import auth_pages_flagged, enforce_provenance, validate_json_output
from models.ontology import Event, EventType, Provenance


class EventExtractorAgent(BaseAgent):
    """
    Event Extractor Agent - extracts event data from web pages.

    Responsibilities:
    - Extract event title, dates, location, type, organizer
    - Handle calendar and list layouts
    - Parse multiple date formats
    - Normalize locations
    """

    # Event type keywords
    EVENT_TYPE_KEYWORDS = {
        EventType.CONFERENCE: ['conference', 'summit', 'symposium', 'congress'],
        EventType.TRADE_SHOW: ['trade show', 'expo', 'exhibition', 'show'],
        EventType.WEBINAR: ['webinar', 'online event', 'virtual event', 'web conference'],
        EventType.WORKSHOP: ['workshop', 'training', 'seminar', 'course'],
        EventType.NETWORKING: ['networking', 'mixer', 'meet and greet', 'social'],
        EventType.ANNUAL_MEETING: ['annual meeting', 'annual conference', 'annual event'],
        EventType.TRAINING: ['training', 'certification', 'education'],
    }

    # Date patterns
    DATE_PATTERNS = [
        # "January 15, 2024" or "Jan 15, 2024"
        r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',
        # "15 January 2024"
        r'(\d{1,2})\s+(\w+)\s+(\d{4})',
        # "2024-01-15" or "01/15/2024" or "01-15-2024"
        r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',
        r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',
    ]

    # Date range patterns
    DATE_RANGE_PATTERNS = [
        # "January 15-17, 2024"
        r'(\w+)\s+(\d{1,2})\s*[-–]\s*(\d{1,2}),?\s+(\d{4})',
        # "January 15 - February 2, 2024"
        r'(\w+)\s+(\d{1,2})\s*[-–]\s*(\w+)\s+(\d{1,2}),?\s+(\d{4})',
        # "Jan 15, 2024 - Jan 17, 2024"
        r'(\w+)\s+(\d{1,2}),?\s+(\d{4})\s*[-–]\s*(\w+)\s+(\d{1,2}),?\s+(\d{4})',
    ]

    def _setup(self, **kwargs):
        """Initialize extractor settings."""
        self.extract_list = self.agent_config.get("extract_list", True)
        self.max_events = self.agent_config.get("max_events", 100)

    @enforce_provenance
    @validate_json_output
    @auth_pages_flagged
    async def run(self, task: dict) -> dict:
        """
        Extract events from a page.

        Args:
            task: {
                "url": "https://pma.org/events",
                "html": "<html>...",  # Optional: pre-fetched HTML
                "association": "PMA",
                "page_type": "EVENTS_LIST" or "EVENT_DETAIL"
            }

        Returns:
            {
                "success": True,
                "events": [Event, ...],
                "records_processed": 10
            }
        """
        url = task.get("url")
        html = task.get("html")
        association = task.get("association", "unknown")
        page_type = task.get("page_type", "EVENTS_LIST")

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
        if page_type == "EVENT_DETAIL":
            events = [self._extract_single_event(soup, url, provenance)]
            events = [e for e in events if e]
        else:
            events = self._extract_event_list(soup, url, provenance)

        self.log.info(
            f"Extracted {len(events)} events from {url}",
            association=association
        )

        # Convert to dicts
        event_dicts = [e.model_dump() if hasattr(e, 'model_dump') else e for e in events]

        return {
            "success": True,
            "records": event_dicts,
            "events": event_dicts,
            "records_processed": len(events)
        }

    def _extract_single_event(
        self,
        soup: BeautifulSoup,
        url: str,
        provenance: Provenance
    ) -> Event | None:
        """Extract a single event from a detail page."""
        try:
            # Extract title
            title = self._extract_title(soup)
            if not title:
                self.log.warning(f"No title found for event at {url}")
                return None

            # Extract dates
            start_date, end_date = self._extract_dates(soup)

            # Extract location
            venue, city, state, country, is_virtual = self._extract_location(soup)

            # Extract description
            description = self._extract_description(soup)

            # Determine event type
            event_type = self._determine_event_type(title, description or "")

            # Extract registration URL
            registration_url = self._extract_registration_url(soup, url)

            event = Event(
                title=title,
                event_type=event_type,
                description=description,
                start_date=start_date,
                end_date=end_date,
                venue=venue,
                city=city,
                state=state,
                country=country or "United States",
                is_virtual=is_virtual,
                event_url=url,
                registration_url=registration_url,
                organizer_association=provenance.association_code,
                provenance=[provenance]
            )

            return event

        except Exception as e:
            self.log.warning(f"Error extracting event from {url}: {e}")
            return None

    def _extract_event_list(
        self,
        soup: BeautifulSoup,
        url: str,
        provenance: Provenance
    ) -> list[Event]:
        """Extract multiple events from a list page."""
        events = []

        # Find event containers
        containers = self._find_event_containers(soup)

        for container in containers[:self.max_events]:
            try:
                event = self._extract_event_from_container(container, url, provenance)
                if event:
                    events.append(event)
            except Exception as e:
                self.log.debug(f"Error extracting event container: {e}")
                continue

        return events

    def _find_event_containers(self, soup: BeautifulSoup) -> list:
        """Find event containers in the page."""
        # Try common patterns
        patterns = [
            {"class_": re.compile(r'event[-_]?item|event[-_]?card|event[-_]?listing', re.I)},
            {"class_": re.compile(r'calendar[-_]?item|calendar[-_]?event', re.I)},
            {"attrs": {"data-event": True}},
            {"attrs": {"itemtype": re.compile(r'Event', re.I)}},
        ]

        for pattern in patterns:
            containers = soup.find_all(["div", "article", "li"], **pattern)
            if len(containers) >= 2:
                return containers

        # Try table rows
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) >= 3:  # Header + at least 2 events
                return rows[1:]  # Skip header

        # Try list items
        lists = soup.find_all(["ul", "ol"], class_=re.compile(r'event|calendar', re.I))
        for list_elem in lists:
            items = list_elem.find_all("li")
            if len(items) >= 2:
                return items

        return []

    def _extract_event_from_container(
        self,
        container,
        base_url: str,
        provenance: Provenance
    ) -> Event | None:
        """Extract event from a container element."""
        # Extract title
        title_elem = container.find(["h1", "h2", "h3", "h4", "a"], class_=re.compile(r'title|name', re.I))
        if not title_elem:
            title_elem = container.find(["h1", "h2", "h3", "h4"])
        if not title_elem:
            title_elem = container.find("a")

        if not title_elem:
            return None

        title = title_elem.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Extract event URL
        event_url = None
        link = title_elem if title_elem.name == "a" else container.find("a")
        if link and link.get("href"):
            href = link.get("href")
            if not href.startswith("http"):
                from urllib.parse import urljoin
                href = urljoin(base_url, href)
            event_url = href

        # Extract dates
        date_text = container.get_text()
        start_date, end_date = self._parse_dates_from_text(date_text)

        # Extract location
        location_elem = container.find(class_=re.compile(r'location|venue|place', re.I))
        location_text = location_elem.get_text(strip=True) if location_elem else ""
        venue, city, state, country, is_virtual = self._parse_location(location_text)

        # Determine event type
        event_type = self._determine_event_type(title, date_text)

        return Event(
            title=title,
            event_type=event_type,
            start_date=start_date,
            end_date=end_date,
            venue=venue,
            city=city,
            state=state,
            country=country or "United States",
            is_virtual=is_virtual,
            event_url=event_url,
            organizer_association=provenance.association_code,
            provenance=[provenance]
        )

    def _extract_title(self, soup: BeautifulSoup) -> str | None:
        """Extract event title from page."""
        # Try structured data first
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                import json
                data = json.loads(json_ld.string)
                if isinstance(data, dict) and data.get("name"):
                    return data["name"]
            except Exception:
                pass

        # Try meta tags
        meta_title = soup.find("meta", property="og:title")
        if meta_title and meta_title.get("content"):
            return meta_title["content"]

        # Try H1
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)

        # Try page title
        if soup.title:
            title = soup.title.string
            # Remove common suffixes
            title = re.sub(r'\s*[-|]\s*.+$', '', title)
            return title.strip()

        return None

    def _extract_dates(self, soup: BeautifulSoup) -> tuple:
        """Extract start and end dates from page."""
        # Try structured data
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                import json
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    start = data.get("startDate")
                    end = data.get("endDate")
                    if start:
                        return (
                            date_parser.parse(start) if start else None,
                            date_parser.parse(end) if end else None
                        )
            except Exception:
                pass

        # Try meta tags
        for prop in ["event:start_time", "startDate"]:
            meta = soup.find("meta", property=prop)
            if meta and meta.get("content"):
                try:
                    start = date_parser.parse(meta["content"])
                    end_meta = soup.find("meta", property=prop.replace("start", "end"))
                    end = date_parser.parse(end_meta["content"]) if end_meta else None
                    return start, end
                except Exception:
                    pass

        # Try date elements
        date_elem = soup.find(class_=re.compile(r'date|when|time', re.I))
        if date_elem:
            return self._parse_dates_from_text(date_elem.get_text())

        # Search full page text
        return self._parse_dates_from_text(soup.get_text())

    def _parse_dates_from_text(self, text: str) -> tuple:
        """Parse dates from text content."""
        if not text:
            return None, None

        text = text[:2000]  # Limit text to search

        # Try date range patterns first
        for pattern in self.DATE_RANGE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 4:
                        # "January 15-17, 2024"
                        month, start_day, end_day, year = groups
                        start = date_parser.parse(f"{month} {start_day}, {year}")
                        end = date_parser.parse(f"{month} {end_day}, {year}")
                        return start, end
                    elif len(groups) == 5:
                        # "January 15 - February 2, 2024"
                        start_month, start_day, end_month, end_day, year = groups
                        start = date_parser.parse(f"{start_month} {start_day}, {year}")
                        end = date_parser.parse(f"{end_month} {end_day}, {year}")
                        return start, end
                except Exception:
                    pass

        # Try single date patterns
        for pattern in self.DATE_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                try:
                    dates = []
                    for match in matches[:2]:  # Get up to 2 dates
                        date_str = " ".join(match) if isinstance(match, tuple) else match
                        dates.append(date_parser.parse(date_str))

                    if len(dates) == 2:
                        return dates[0], dates[1]
                    elif len(dates) == 1:
                        return dates[0], None
                except Exception:
                    pass

        return None, None

    def _extract_location(self, soup: BeautifulSoup) -> tuple:
        """Extract location information."""
        # Try structured data
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                import json
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    location = data.get("location", {})
                    if isinstance(location, dict):
                        address = location.get("address", {})
                        return (
                            location.get("name"),
                            address.get("addressLocality"),
                            address.get("addressRegion"),
                            address.get("addressCountry"),
                            "virtual" in str(location).lower()
                        )
            except Exception:
                pass

        # Try location elements
        location_elem = soup.find(class_=re.compile(r'location|venue|place|address', re.I))
        if location_elem:
            return self._parse_location(location_elem.get_text())

        return None, None, None, None, False

    def _parse_location(self, text: str) -> tuple:
        """Parse location from text."""
        if not text:
            return None, None, None, None, False

        text = text.strip()

        # Check for virtual
        is_virtual = any(kw in text.lower() for kw in ['virtual', 'online', 'webinar'])

        # Try to parse city, state pattern
        from skills.common.SKILL import STATE_CODES

        # Pattern: "City, ST" or "City, State"
        for _, state_code in STATE_CODES.items():
            pattern = rf'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*{state_code}\b'
            match = re.search(pattern, text)
            if match:
                return None, match.group(1), state_code, "United States", is_virtual

        # Full state names
        for state_name, state_code in STATE_CODES.items():
            if state_name.title() in text:
                return None, None, state_code, "United States", is_virtual

        return text if len(text) < 100 else None, None, None, None, is_virtual

    def _extract_description(self, soup: BeautifulSoup) -> str | None:
        """Extract event description."""
        # Try meta description
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return meta["content"][:500]

        # Try common description elements
        desc_elem = soup.find(class_=re.compile(r'description|summary|about', re.I))
        if desc_elem:
            return desc_elem.get_text(strip=True)[:500]

        # Try first paragraph after title
        h1 = soup.find("h1")
        if h1:
            p = h1.find_next("p")
            if p:
                return p.get_text(strip=True)[:500]

        return None

    def _extract_registration_url(self, soup: BeautifulSoup, base_url: str) -> str | None:
        """Extract registration URL."""
        patterns = ['register', 'sign up', 'rsvp', 'attend', 'buy ticket']

        for pattern in patterns:
            link = soup.find("a", string=re.compile(pattern, re.I))
            if link and link.get("href"):
                href = link["href"]
                if not href.startswith("http"):
                    from urllib.parse import urljoin
                    href = urljoin(base_url, href)
                return href

        return None

    def _determine_event_type(self, title: str, description: str) -> EventType:
        """Determine event type from title and description."""
        text = f"{title} {description}".lower()

        for event_type, keywords in self.EVENT_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    return event_type

        return EventType.OTHER
