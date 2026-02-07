"""
Page Classifier Agent
NAM Intelligence Pipeline

Classifies fetched web pages into ontology types to determine
the appropriate extraction strategy.
"""

import re
from datetime import datetime, UTC
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from models.ontology import PageType, PageClassification
from middleware.policy import validate_json_output, ontology_labels_required


class PageClassifierAgent(BaseAgent):
    """
    Page Classifier Agent - classifies pages into ontology types.

    Responsibilities:
    - Analyze URL structure, title, H1, content
    - Classify into 11 ontology page types
    - Assign confidence scores
    - Route to appropriate extractor
    """

    # URL patterns for classification
    URL_PATTERNS = {
        PageType.MEMBER_DIRECTORY: [
            r'/members?/?$',
            r'/directory/?$',
            r'/member-directory',
            r'/member-list',
            r'/find-a-member',
            r'/membership/members',
            r'/companies/?$',
        ],
        PageType.MEMBER_DETAIL: [
            r'/members?/[\w-]+/?$',
            r'/directory/[\w-]+/?$',
            r'/company/[\w-]+',
            r'/profile/[\w-]+',
            r'/member-profile',
        ],
        PageType.EVENTS_LIST: [
            r'/events/?$',
            r'/calendar/?$',
            r'/upcoming-events',
            r'/event-calendar',
            r'/conferences/?$',
        ],
        PageType.EVENT_DETAIL: [
            r'/events?/[\w-]+/?$',
            r'/event/[\w-]+',
            r'/conference/[\w-]+',
            r'/annual-meeting',
            r'/expo/?$',
        ],
        PageType.EXHIBITORS_LIST: [
            r'/exhibitors?/?$',
            r'/exhibitor-list',
            r'/exhibitor-directory',
            r'/booth-list',
        ],
        PageType.SPONSORS_LIST: [
            r'/sponsors?/?$',
            r'/sponsor-list',
            r'/sponsorship',
            r'/our-sponsors',
        ],
        PageType.PARTICIPANTS_LIST: [
            r'/attendees?/?$',
            r'/participants?/?$',
            r'/registered',
            r'/speakers?/?$',
        ],
        PageType.ASSOCIATION_HOME: [
            r'^/?$',
            r'/home/?$',
            r'/index',
        ],
        PageType.ASSOCIATION_DIRECTORY: [
            r'/about/?$',
            r'/about-us',
            r'/who-we-are',
        ],
        PageType.RESOURCE: [
            r'/resources?/?$',
            r'/publications?',
            r'/downloads?',
            r'/library',
            r'/news/?$',
            r'/blog/?$',
        ],
    }

    # Content keywords for classification
    CONTENT_KEYWORDS = {
        PageType.MEMBER_DIRECTORY: [
            'member directory', 'member list', 'our members', 'find a member',
            'company directory', 'browse members', 'member search',
        ],
        PageType.MEMBER_DETAIL: [
            'company profile', 'member profile', 'about the company',
            'contact information', 'company details',
        ],
        PageType.EVENTS_LIST: [
            'upcoming events', 'event calendar', 'conferences', 'trade shows',
            'webinars', 'networking events',
        ],
        PageType.EVENT_DETAIL: [
            'event details', 'register now', 'registration', 'conference agenda',
            'event schedule', 'keynote speakers',
        ],
        PageType.EXHIBITORS_LIST: [
            'exhibitors', 'exhibitor list', 'booth information', 'exhibitor directory',
            'floor plan', 'exhibit hall',
        ],
        PageType.SPONSORS_LIST: [
            'our sponsors', 'thank you to our sponsors', 'sponsorship levels',
            'platinum sponsors', 'gold sponsors', 'silver sponsors',
        ],
        PageType.PARTICIPANTS_LIST: [
            'attendees', 'participants', 'registered attendees', 'speakers',
            'presenters', 'panelists',
        ],
    }

    # Extractor mapping
    EXTRACTOR_MAPPING = {
        PageType.MEMBER_DIRECTORY: "extraction.directory_parser",
        PageType.MEMBER_DETAIL: "extraction.html_parser",
        PageType.EVENTS_LIST: "extraction.event_extractor",
        PageType.EVENT_DETAIL: "extraction.event_extractor",
        PageType.EXHIBITORS_LIST: "extraction.event_participant_extractor",
        PageType.SPONSORS_LIST: "extraction.event_participant_extractor",
        PageType.PARTICIPANTS_LIST: "extraction.event_participant_extractor",
        PageType.ASSOCIATION_HOME: None,  # No extraction needed
        PageType.ASSOCIATION_DIRECTORY: None,
        PageType.RESOURCE: None,
        PageType.OTHER: None,
    }

    def _setup(self, **kwargs):
        """Initialize classifier settings."""
        self.min_confidence = self.agent_config.get("min_confidence", 0.5)

    @validate_json_output
    async def run(self, task: dict) -> dict:
        """
        Classify a page into ontology types.

        Args:
            task: {
                "url": "https://pma.org/members",
                "html": "<html>...",  # Optional: pre-fetched HTML
                "fetch": True,  # Whether to fetch if HTML not provided
            }

        Returns:
            {
                "success": True,
                "classification": PageClassification,
                "page_type": "MEMBER_DIRECTORY",
                "confidence": 0.85,
                "recommended_extractor": "extraction.directory_parser"
            }
        """
        url = task.get("url")
        html = task.get("html")
        should_fetch = task.get("fetch", True)

        if not url:
            return {
                "success": False,
                "error": "URL is required",
                "records_processed": 0
            }

        # Fetch page if needed
        if not html and should_fetch:
            try:
                response = await self.http.get(url, timeout=30)
                if response.status_code == 200:
                    html = response.text
                else:
                    self.log.warning(f"Failed to fetch {url}: HTTP {response.status_code}")
            except Exception as e:
                self.log.warning(f"Failed to fetch {url}: {e}")

        # Classify based on URL
        url_classification = self._classify_by_url(url)

        # Classify based on content if HTML available
        content_classification = None
        if html:
            content_classification = self._classify_by_content(html)

        # Combine classifications
        page_type, confidence, signals = self._combine_classifications(
            url_classification, content_classification
        )

        # Get recommended extractor
        extractor = self.EXTRACTOR_MAPPING.get(page_type)

        classification = PageClassification(
            url=url,
            page_type=page_type,
            confidence=confidence,
            signals=signals,
            recommended_extractor=extractor,
            classified_at=datetime.now(UTC)
        )

        self.log.info(
            f"Classified page",
            url=url,
            page_type=page_type.value,
            confidence=confidence,
            extractor=extractor
        )

        return {
            "success": True,
            "classification": classification.model_dump(),
            "page_type": page_type.value,
            "confidence": confidence,
            "recommended_extractor": extractor,
            "signals": signals,
            "records_processed": 1
        }

    def _classify_by_url(self, url: str) -> dict:
        """Classify page based on URL patterns."""
        parsed = urlparse(url)
        path = parsed.path.lower()

        matches = {}

        for page_type, patterns in self.URL_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, path, re.IGNORECASE):
                    matches[page_type] = matches.get(page_type, 0) + 0.4
                    break

        # Find best match
        if matches:
            best_type = max(matches, key=matches.get)
            return {
                "page_type": best_type,
                "confidence": min(matches[best_type], 0.8),
                "source": "url"
            }

        return {
            "page_type": PageType.OTHER,
            "confidence": 0.1,
            "source": "url"
        }

    def _classify_by_content(self, html: str) -> dict:
        """Classify page based on content analysis."""
        soup = BeautifulSoup(html, "lxml")

        # Extract signals
        title = soup.title.string.lower() if soup.title else ""
        h1_tags = [h.get_text().lower() for h in soup.find_all("h1")]
        h1_text = " ".join(h1_tags)

        # Get body text (limited for performance)
        body_text = soup.get_text()[:5000].lower()

        matches = {}
        signals = {}

        for page_type, keywords in self.CONTENT_KEYWORDS.items():
            score = 0
            matched_keywords = []

            for keyword in keywords:
                # Check title (highest weight)
                if keyword in title:
                    score += 0.3
                    matched_keywords.append(f"title:{keyword}")

                # Check H1 (high weight)
                if keyword in h1_text:
                    score += 0.25
                    matched_keywords.append(f"h1:{keyword}")

                # Check body (lower weight)
                if keyword in body_text:
                    score += 0.1
                    matched_keywords.append(f"body:{keyword}")

            if score > 0:
                matches[page_type] = score
                signals[page_type.value] = matched_keywords

        # Additional structural signals
        structural_signals = self._analyze_structure(soup)
        for page_type, score in structural_signals.items():
            matches[page_type] = matches.get(page_type, 0) + score

        # Find best match
        if matches:
            best_type = max(matches, key=matches.get)
            return {
                "page_type": best_type,
                "confidence": min(matches[best_type], 0.95),
                "source": "content",
                "signals": signals.get(best_type.value, [])
            }

        return {
            "page_type": PageType.OTHER,
            "confidence": 0.1,
            "source": "content",
            "signals": []
        }

    def _analyze_structure(self, soup: BeautifulSoup) -> dict:
        """Analyze page structure for additional signals."""
        scores = {}

        # Check for list structures
        tables = soup.find_all("table")
        lists = soup.find_all(["ul", "ol"])
        list_items = soup.find_all("li")

        # Many list items suggests directory/list page
        if len(list_items) > 20:
            scores[PageType.MEMBER_DIRECTORY] = 0.2

        # Check for member/company class names
        member_elements = soup.find_all(class_=re.compile(
            r'member|company|listing|result|profile-card', re.I
        ))
        if len(member_elements) > 5:
            scores[PageType.MEMBER_DIRECTORY] = scores.get(PageType.MEMBER_DIRECTORY, 0) + 0.25

        # Single profile card suggests detail page
        if len(member_elements) == 1:
            scores[PageType.MEMBER_DETAIL] = 0.3

        # Check for event indicators
        event_elements = soup.find_all(class_=re.compile(
            r'event|conference|webinar|calendar', re.I
        ))
        if event_elements:
            if len(event_elements) > 3:
                scores[PageType.EVENTS_LIST] = 0.25
            else:
                scores[PageType.EVENT_DETAIL] = 0.2

        # Check for sponsor tier elements
        sponsor_tiers = soup.find_all(class_=re.compile(
            r'platinum|gold|silver|bronze|sponsor', re.I
        ))
        if sponsor_tiers:
            scores[PageType.SPONSORS_LIST] = 0.3

        # Check for exhibitor/booth elements
        exhibitor_elements = soup.find_all(class_=re.compile(
            r'exhibitor|booth', re.I
        ))
        if exhibitor_elements:
            scores[PageType.EXHIBITORS_LIST] = 0.3

        # Check for registration forms (event detail)
        reg_forms = soup.find_all("form", attrs={
            "action": re.compile(r'register|signup|rsvp', re.I)
        })
        if reg_forms:
            scores[PageType.EVENT_DETAIL] = scores.get(PageType.EVENT_DETAIL, 0) + 0.2

        return scores

    def _combine_classifications(
        self,
        url_class: dict,
        content_class: Optional[dict]
    ) -> tuple:
        """Combine URL and content classifications."""
        signals = {
            "url": {
                "type": url_class["page_type"].value,
                "confidence": url_class["confidence"]
            }
        }

        if content_class is None:
            return url_class["page_type"], url_class["confidence"], signals

        signals["content"] = {
            "type": content_class["page_type"].value,
            "confidence": content_class["confidence"],
            "matched_keywords": content_class.get("signals", [])
        }

        # If both agree, boost confidence
        if url_class["page_type"] == content_class["page_type"]:
            combined_confidence = min(
                url_class["confidence"] + content_class["confidence"],
                0.98
            )
            return url_class["page_type"], combined_confidence, signals

        # If they disagree, prefer higher confidence
        if url_class["confidence"] >= content_class["confidence"]:
            return url_class["page_type"], url_class["confidence"], signals
        else:
            return content_class["page_type"], content_class["confidence"], signals


class BatchPageClassifierAgent(PageClassifierAgent):
    """
    Batch version of Page Classifier for processing multiple pages.
    """

    async def run(self, task: dict) -> dict:
        """
        Classify multiple pages.

        Args:
            task: {
                "pages": [
                    {"url": "https://...", "html": "..."},
                    ...
                ]
            }
        """
        pages = task.get("pages", [])

        if not pages:
            return {
                "success": False,
                "error": "No pages provided",
                "records_processed": 0
            }

        self.log.info(f"Classifying {len(pages)} pages")

        classifications = []
        type_counts = {}

        for page in pages:
            result = await super().run(page)

            if result.get("success"):
                classification = result.get("classification", {})
                classifications.append(classification)

                page_type = result.get("page_type", "OTHER")
                type_counts[page_type] = type_counts.get(page_type, 0) + 1

        self.log.info(
            f"Classification complete",
            total=len(pages),
            type_distribution=type_counts
        )

        return {
            "success": True,
            "classifications": classifications,
            "type_distribution": type_counts,
            "records_processed": len(classifications)
        }
