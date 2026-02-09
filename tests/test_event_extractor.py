"""
Event Extractor Agent Tests
NAM Intelligence Pipeline

Comprehensive tests for the EventExtractorAgent covering initialization,
event type determination, date parsing, title/description/location extraction,
registration URL extraction, event container finding, and the run() method.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from middleware.secrets import _reset_secrets_manager
from models.ontology import EventType

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(autouse=True)
def reset_secrets_singleton():
    _reset_secrets_manager()
    yield
    _reset_secrets_manager()


def _create_event_extractor(agent_config=None):
    from agents.extraction.event_extractor import EventExtractorAgent

    nested = {"extraction": {"event_extractor": agent_config or {}}}
    with (
        patch("agents.base.Config") as mock_config,
        patch("agents.base.StructuredLogger"),
        patch("agents.base.AsyncHTTPClient"),
        patch("agents.base.RateLimiter"),
    ):
        mock_config.return_value.load.return_value = nested
        agent = EventExtractorAgent(
            agent_type="extraction.event_extractor",
            job_id="test-job-123",
        )
        return agent


@pytest.fixture
def extractor():
    return _create_event_extractor()


def _soup(html):
    """Helper to create a BeautifulSoup object from HTML string."""
    return BeautifulSoup(html, "lxml")


# =============================================================================
# HTML FIXTURES
# =============================================================================

EVENTS_LIST_HTML = """
<html>
<body>
<div class="event-item">
    <h3 class="title"><a href="/events/conf-2024">Annual Conference 2024</a></h3>
    <span class="date">January 15-17, 2024</span>
    <span class="location">Chicago, IL</span>
</div>
<div class="event-item">
    <h3 class="title"><a href="/events/expo-2024">Manufacturing Expo 2024</a></h3>
    <span class="date">March 5, 2024</span>
    <span class="location">Detroit, MI</span>
</div>
</body>
</html>
"""

EVENT_DETAIL_HTML = """
<html>
<head>
    <title>Annual Manufacturing Conference 2024 | PMA</title>
    <meta name="description" content="Join us for the premier manufacturing event of the year.">
    <meta property="og:title" content="Annual Manufacturing Conference 2024">
</head>
<body>
<h1>Annual Manufacturing Conference 2024</h1>
<p>The premier gathering for manufacturing professionals.</p>
<div class="date">January 15-17, 2024</div>
<div class="location">McCormick Place, Chicago, IL</div>
<a href="/events/conf-2024/register">Register Now</a>
</body>
</html>
"""

JSON_LD_EVENT_HTML = """
<html>
<head>
<script type="application/ld+json">
{
    "name": "Industry Summit 2024",
    "startDate": "2024-03-15",
    "endDate": "2024-03-17",
    "location": {
        "name": "Convention Center",
        "address": {
            "addressLocality": "Las Vegas",
            "addressRegion": "NV",
            "addressCountry": "United States"
        }
    }
}
</script>
</head>
<body>
<h1>Industry Summit 2024</h1>
</body>
</html>
"""


# =============================================================================
# 1. INITIALIZATION TESTS
# =============================================================================


class TestInitialization:
    """Tests for agent construction and configuration."""

    def test_default_config(self, extractor):
        """Default config has extract_list=True and max_events=100."""
        assert extractor.extract_list is True
        assert extractor.max_events == 100

    def test_custom_config_extract_list(self):
        """Custom extract_list is respected."""
        agent = _create_event_extractor({"extract_list": False})
        assert agent.extract_list is False

    def test_custom_config_max_events(self):
        """Custom max_events is respected."""
        agent = _create_event_extractor({"max_events": 50})
        assert agent.max_events == 50

    def test_agent_type(self, extractor):
        """Agent type is set correctly."""
        assert extractor.agent_type == "extraction.event_extractor"

    def test_job_id(self, extractor):
        """Job ID is set correctly."""
        assert extractor.job_id == "test-job-123"


# =============================================================================
# 2. DETERMINE EVENT TYPE TESTS
# =============================================================================


class TestDetermineEventType:
    """Tests for _determine_event_type method."""

    def test_conference_keyword(self, extractor):
        assert extractor._determine_event_type("Annual Conference 2024", "") == EventType.CONFERENCE

    def test_summit_keyword(self, extractor):
        assert extractor._determine_event_type("Manufacturing Summit", "") == EventType.CONFERENCE

    def test_symposium_keyword(self, extractor):
        assert extractor._determine_event_type("Technical Symposium", "") == EventType.CONFERENCE

    def test_congress_keyword(self, extractor):
        assert extractor._determine_event_type("World Congress on Manufacturing", "") == EventType.CONFERENCE

    def test_trade_show_keyword(self, extractor):
        assert extractor._determine_event_type("Annual Trade Show", "") == EventType.TRADE_SHOW

    def test_expo_keyword(self, extractor):
        assert extractor._determine_event_type("Manufacturing Expo 2024", "") == EventType.TRADE_SHOW

    def test_exhibition_keyword(self, extractor):
        assert extractor._determine_event_type("Industrial Exhibition", "") == EventType.TRADE_SHOW

    def test_webinar_keyword(self, extractor):
        assert extractor._determine_event_type("Webinar: Industry Trends", "") == EventType.WEBINAR

    def test_online_event_keyword(self, extractor):
        assert extractor._determine_event_type("Online Event Series", "") == EventType.WEBINAR

    def test_virtual_event_keyword(self, extractor):
        assert extractor._determine_event_type("Virtual Event 2024", "") == EventType.WEBINAR

    def test_workshop_keyword(self, extractor):
        assert extractor._determine_event_type("Lean Manufacturing Workshop", "") == EventType.WORKSHOP

    def test_seminar_keyword(self, extractor):
        assert extractor._determine_event_type("Quality Assurance Seminar", "") == EventType.WORKSHOP

    def test_networking_keyword(self, extractor):
        assert extractor._determine_event_type("Industry Networking Night", "") == EventType.NETWORKING

    def test_mixer_keyword(self, extractor):
        assert extractor._determine_event_type("Holiday Mixer", "") == EventType.NETWORKING

    def test_annual_meeting_keyword(self, extractor):
        assert extractor._determine_event_type("PMA Annual Meeting", "") == EventType.ANNUAL_MEETING

    def test_training_keyword_in_description(self, extractor):
        """Keywords in description also match."""
        assert extractor._determine_event_type("Some Event", "certification program available") == EventType.TRAINING

    def test_default_other_for_unknown(self, extractor):
        assert extractor._determine_event_type("Board Dinner", "enjoy food") == EventType.OTHER

    def test_case_insensitive(self, extractor):
        assert extractor._determine_event_type("ANNUAL CONFERENCE", "") == EventType.CONFERENCE

    def test_keyword_priority_order(self, extractor):
        """Earlier entries in EVENT_TYPE_KEYWORDS dict win."""
        # "conference" appears before "annual meeting" in the dict iteration
        result = extractor._determine_event_type("Annual Conference Meeting", "")
        assert result == EventType.CONFERENCE


# =============================================================================
# 3. PARSE DATES FROM TEXT TESTS
# =============================================================================


class TestParseDatesFromText:
    """Tests for _parse_dates_from_text method."""

    def test_date_range_same_month(self, extractor):
        """'January 15-17, 2024' parses start and end in same month."""
        start, end = extractor._parse_dates_from_text("January 15-17, 2024")
        assert start is not None
        assert end is not None
        assert start.month == 1
        assert start.day == 15
        assert end.day == 17
        assert start.year == 2024

    def test_date_range_cross_month(self, extractor):
        """'January 15 - February 2, 2024' parses cross-month range."""
        start, end = extractor._parse_dates_from_text("January 15 - February 2, 2024")
        assert start is not None
        assert end is not None
        assert start.month == 1
        assert start.day == 15
        assert end.month == 2
        assert end.day == 2

    def test_single_date(self, extractor):
        """'January 15, 2024' parses single date."""
        start, end = extractor._parse_dates_from_text("January 15, 2024")
        assert start is not None
        assert start.month == 1
        assert start.day == 15
        assert start.year == 2024

    def test_iso_format(self, extractor):
        """'2024-01-15' parses ISO date."""
        start, end = extractor._parse_dates_from_text("2024-01-15")
        assert start is not None
        assert start.year == 2024

    def test_us_format(self, extractor):
        """'01/15/2024' parses US date."""
        start, end = extractor._parse_dates_from_text("01/15/2024")
        assert start is not None

    def test_empty_text(self, extractor):
        """Empty text returns (None, None)."""
        assert extractor._parse_dates_from_text("") == (None, None)

    def test_none_text(self, extractor):
        """None text returns (None, None)."""
        assert extractor._parse_dates_from_text(None) == (None, None)

    def test_no_dates(self, extractor):
        """Text with no dates returns (None, None)."""
        assert extractor._parse_dates_from_text("No date information here.") == (None, None)

    def test_very_long_text_truncated(self, extractor):
        """Text with date within first 2000 chars is found."""
        long_text = "A" * 1980 + " January 15, 2024"  # Date fits within 2000 chars
        start, end = extractor._parse_dates_from_text(long_text)
        assert start is not None

    def test_very_long_text_date_beyond_limit(self, extractor):
        """Date beyond 2000 chars is not found."""
        long_text = "A" * 2001 + " January 15, 2024"
        start, end = extractor._parse_dates_from_text(long_text)
        assert start is None

    def test_date_range_with_en_dash(self, extractor):
        """En-dash separator also works."""
        start, end = extractor._parse_dates_from_text("January 15\u201317, 2024")
        assert start is not None
        assert end is not None


# =============================================================================
# 4. EXTRACT TITLE TESTS
# =============================================================================


class TestExtractTitle:
    """Tests for _extract_title method."""

    def test_json_ld_with_name(self, extractor):
        """JSON-LD structured data provides title."""
        soup = _soup(JSON_LD_EVENT_HTML)
        title = extractor._extract_title(soup)
        assert title == "Industry Summit 2024"

    def test_og_title_meta_tag(self, extractor):
        """og:title meta tag is used when no JSON-LD."""
        html = '<html><head><meta property="og:title" content="My Event"></head><body></body></html>'
        title = extractor._extract_title(_soup(html))
        assert title == "My Event"

    def test_h1_element(self, extractor):
        """H1 element is used as fallback."""
        html = "<html><body><h1>Great Event 2024</h1></body></html>"
        title = extractor._extract_title(_soup(html))
        assert title == "Great Event 2024"

    def test_page_title_with_suffix_removal(self, extractor):
        """Page title with ' | Site' suffix is cleaned."""
        html = "<html><head><title>My Event | PMA Website</title></head><body></body></html>"
        title = extractor._extract_title(_soup(html))
        assert title == "My Event"

    def test_page_title_with_dash_suffix(self, extractor):
        """Page title with ' - Site' suffix is cleaned."""
        html = "<html><head><title>My Event - Association Site</title></head><body></body></html>"
        title = extractor._extract_title(_soup(html))
        assert title == "My Event"

    def test_no_title_found(self, extractor):
        """No title anywhere returns None."""
        html = "<html><body><p>Some content.</p></body></html>"
        title = extractor._extract_title(_soup(html))
        assert title is None

    def test_json_ld_invalid_json(self, extractor):
        """Invalid JSON-LD falls through to other methods."""
        html = '<html><head><script type="application/ld+json">invalid json</script></head><body><h1>Fallback Title</h1></body></html>'
        title = extractor._extract_title(_soup(html))
        assert title == "Fallback Title"

    def test_json_ld_no_name_field(self, extractor):
        """JSON-LD without name field falls through."""
        html = '<html><head><script type="application/ld+json">{"@type": "Event"}</script></head><body><h1>H1 Title</h1></body></html>'
        title = extractor._extract_title(_soup(html))
        assert title == "H1 Title"


# =============================================================================
# 5. EXTRACT DESCRIPTION TESTS
# =============================================================================


class TestExtractDescription:
    """Tests for _extract_description method."""

    def test_meta_description(self, extractor):
        """Meta description tag is extracted."""
        html = '<html><head><meta name="description" content="A great event for all."></head><body></body></html>'
        desc = extractor._extract_description(_soup(html))
        assert desc == "A great event for all."

    def test_description_class_element(self, extractor):
        """Element with description class is extracted."""
        html = '<html><body><div class="description">Event details here.</div></body></html>'
        desc = extractor._extract_description(_soup(html))
        assert desc == "Event details here."

    def test_summary_class_element(self, extractor):
        """Element with summary class is extracted."""
        html = '<html><body><div class="summary">Summary of the event.</div></body></html>'
        desc = extractor._extract_description(_soup(html))
        assert desc == "Summary of the event."

    def test_paragraph_after_h1(self, extractor):
        """First paragraph after H1 is used as fallback."""
        html = "<html><body><h1>Event</h1><p>First paragraph content.</p></body></html>"
        desc = extractor._extract_description(_soup(html))
        assert desc == "First paragraph content."

    def test_no_description(self, extractor):
        """No description returns None."""
        html = "<html><body><div>Just a div.</div></body></html>"
        desc = extractor._extract_description(_soup(html))
        assert desc is None

    def test_description_truncation(self, extractor):
        """Long description is truncated to 500 chars."""
        long_desc = "A" * 600
        html = f'<html><head><meta name="description" content="{long_desc}"></head><body></body></html>'
        desc = extractor._extract_description(_soup(html))
        assert len(desc) == 500


# =============================================================================
# 6. EXTRACT LOCATION TESTS
# =============================================================================


class TestExtractLocation:
    """Tests for _extract_location method."""

    def test_json_ld_with_location(self, extractor):
        """JSON-LD location with address is extracted."""
        soup = _soup(JSON_LD_EVENT_HTML)
        venue, city, state, country, is_virtual = extractor._extract_location(soup)
        assert venue == "Convention Center"
        assert city == "Las Vegas"
        assert state == "NV"
        assert country == "United States"
        assert is_virtual is False

    def test_location_class_element(self, extractor):
        """Location class element is parsed."""
        html = '<html><body><div class="location">Chicago, IL</div></body></html>'
        venue, city, state, country, is_virtual = extractor._extract_location(_soup(html))
        assert city == "Chicago"
        assert state == "IL"

    def test_virtual_event_detection(self, extractor):
        """Virtual keyword in JSON-LD location is detected."""
        html = """
        <html><head><script type="application/ld+json">
        {"location": {"name": "Virtual Event Space", "address": {}}}
        </script></head><body></body></html>
        """
        venue, city, state, country, is_virtual = extractor._extract_location(_soup(html))
        assert is_virtual is True

    def test_no_location(self, extractor):
        """No location returns defaults."""
        html = "<html><body><p>No location here.</p></body></html>"
        result = extractor._extract_location(_soup(html))
        assert result == (None, None, None, None, False)


# =============================================================================
# 7. PARSE LOCATION TESTS
# =============================================================================


class TestParseLocation:
    """Tests for _parse_location method."""

    def test_city_state_abbreviation(self, extractor):
        """'Chicago, IL' parses city and state."""
        venue, city, state, country, is_virtual = extractor._parse_location("Chicago, IL")
        assert city == "Chicago"
        assert state == "IL"
        assert country == "United States"

    def test_virtual_keyword(self, extractor):
        """Virtual keyword sets is_virtual=True."""
        _, _, _, _, is_virtual = extractor._parse_location("Virtual Event")
        assert is_virtual is True

    def test_online_keyword(self, extractor):
        """Online keyword sets is_virtual=True."""
        _, _, _, _, is_virtual = extractor._parse_location("Online Webinar")
        assert is_virtual is True

    def test_empty_text(self, extractor):
        """Empty text returns all None."""
        result = extractor._parse_location("")
        assert result == (None, None, None, None, False)

    def test_none_text(self, extractor):
        """None text returns all None."""
        result = extractor._parse_location(None)
        assert result == (None, None, None, None, False)

    def test_full_state_name(self, extractor):
        """Full state name is recognized."""
        _, _, state, country, _ = extractor._parse_location("Event in California")
        assert state == "CA"
        assert country == "United States"

    def test_long_text_returns_none_venue(self, extractor):
        """Text longer than 100 chars returns None for venue."""
        long_location = "A" * 101
        venue, city, state, country, is_virtual = extractor._parse_location(long_location)
        assert venue is None

    def test_short_text_is_venue(self, extractor):
        """Short unrecognized text is treated as venue."""
        venue, city, state, country, is_virtual = extractor._parse_location("Grand Ballroom")
        assert venue == "Grand Ballroom"

    def test_city_state_with_venue(self, extractor):
        """City, ST pattern extracts city and state code."""
        venue, city, state, country, is_virtual = extractor._parse_location("Detroit, MI")
        assert city == "Detroit"
        assert state == "MI"


# =============================================================================
# 8. EXTRACT REGISTRATION URL TESTS
# =============================================================================


class TestExtractRegistrationUrl:
    """Tests for _extract_registration_url method."""

    def test_register_link(self, extractor):
        """'Register' link is found."""
        html = '<html><body><a href="https://example.com/register">Register Now</a></body></html>'
        url = extractor._extract_registration_url(_soup(html), "https://example.com")
        assert url == "https://example.com/register"

    def test_sign_up_link(self, extractor):
        """'Sign Up' link is found."""
        html = '<html><body><a href="https://example.com/signup">Sign Up Here</a></body></html>'
        url = extractor._extract_registration_url(_soup(html), "https://example.com")
        assert url == "https://example.com/signup"

    def test_rsvp_link(self, extractor):
        """'RSVP' link is found."""
        html = '<html><body><a href="/rsvp">RSVP Today</a></body></html>'
        url = extractor._extract_registration_url(_soup(html), "https://example.com")
        assert url == "https://example.com/rsvp"

    def test_relative_url_joined(self, extractor):
        """Relative URL is joined with base URL."""
        html = '<html><body><a href="/events/register">Register</a></body></html>'
        url = extractor._extract_registration_url(_soup(html), "https://example.com/events/conf")
        assert url.startswith("https://example.com")
        assert "register" in url

    def test_no_registration_link(self, extractor):
        """No registration link returns None."""
        html = '<html><body><a href="/about">About Us</a></body></html>'
        url = extractor._extract_registration_url(_soup(html), "https://example.com")
        assert url is None


# =============================================================================
# 9. FIND EVENT CONTAINERS TESTS
# =============================================================================


class TestFindEventContainers:
    """Tests for _find_event_containers method."""

    def test_event_item_class(self, extractor):
        """Elements with class 'event-item' are found."""
        html = """
        <html><body>
        <div class="event-item"><h3>Event 1</h3></div>
        <div class="event-item"><h3>Event 2</h3></div>
        </body></html>
        """
        containers = extractor._find_event_containers(_soup(html))
        assert len(containers) == 2

    def test_event_card_class(self, extractor):
        """Elements with class 'event-card' are found."""
        html = """
        <html><body>
        <div class="event-card"><h3>Event 1</h3></div>
        <div class="event-card"><h3>Event 2</h3></div>
        </body></html>
        """
        containers = extractor._find_event_containers(_soup(html))
        assert len(containers) == 2

    def test_calendar_item_class(self, extractor):
        """Elements with class 'calendar-item' are found."""
        html = """
        <html><body>
        <div class="calendar-item"><h3>Event 1</h3></div>
        <div class="calendar-item"><h3>Event 2</h3></div>
        </body></html>
        """
        containers = extractor._find_event_containers(_soup(html))
        assert len(containers) == 2

    def test_data_event_attribute(self, extractor):
        """Elements with data-event attribute are found."""
        html = """
        <html><body>
        <div data-event="1"><h3>Event 1</h3></div>
        <div data-event="2"><h3>Event 2</h3></div>
        </body></html>
        """
        containers = extractor._find_event_containers(_soup(html))
        assert len(containers) == 2

    def test_table_rows_skip_header(self, extractor):
        """Table rows found, header skipped."""
        html = """
        <html><body>
        <table>
            <tr><th>Event</th><th>Date</th></tr>
            <tr><td>Event 1</td><td>Jan 1</td></tr>
            <tr><td>Event 2</td><td>Jan 2</td></tr>
            <tr><td>Event 3</td><td>Jan 3</td></tr>
        </table>
        </body></html>
        """
        containers = extractor._find_event_containers(_soup(html))
        assert len(containers) == 3  # 3 data rows (header skipped)

    def test_list_items_in_event_list(self, extractor):
        """List items in ul/ol with event class found."""
        html = """
        <html><body>
        <ul class="event-list">
            <li>Event 1</li>
            <li>Event 2</li>
        </ul>
        </body></html>
        """
        containers = extractor._find_event_containers(_soup(html))
        assert len(containers) == 2

    def test_no_containers_found(self, extractor):
        """No event containers returns empty list."""
        html = "<html><body><p>Just text.</p></body></html>"
        containers = extractor._find_event_containers(_soup(html))
        assert containers == []

    def test_minimum_two_containers_required(self, extractor):
        """Single container is not enough for pattern match."""
        html = """
        <html><body>
        <div class="event-item"><h3>Only One Event</h3></div>
        </body></html>
        """
        containers = extractor._find_event_containers(_soup(html))
        assert len(containers) == 0


# =============================================================================
# 10. EXTRACT EVENT FROM CONTAINER TESTS
# =============================================================================


class TestExtractEventFromContainer:
    """Tests for _extract_event_from_container method."""

    def _make_provenance(self):
        from models.ontology import Provenance
        return Provenance(
            source_url="https://example.com/events",
            source_type="web",
            extracted_by="extraction.event_extractor",
            association_code="PMA",
            job_id="test-job-123",
        )

    def test_container_with_title_link_and_dates(self, extractor):
        """Container with title link and date span extracts event."""
        html = """
        <div class="event-item">
            <h3 class="title"><a href="/events/conf-2024">Annual Conference 2024</a></h3>
            <span class="date">January 15-17, 2024</span>
            <span class="location">Chicago, IL</span>
        </div>
        """
        container = _soup(html).find("div")
        event = extractor._extract_event_from_container(
            container, "https://example.com", self._make_provenance()
        )
        assert event is not None
        assert event.title == "Annual Conference 2024"
        assert event.event_url == "https://example.com/events/conf-2024"
        assert event.city == "Chicago"
        assert event.state == "IL"

    def test_container_with_h2_title(self, extractor):
        """Container with h2 title extracts event."""
        html = """
        <div class="event-item">
            <h2>Manufacturing Workshop</h2>
            <span class="location">Detroit, MI</span>
        </div>
        """
        container = _soup(html).find("div")
        event = extractor._extract_event_from_container(
            container, "https://example.com", self._make_provenance()
        )
        assert event is not None
        assert event.title == "Manufacturing Workshop"
        assert event.event_type == EventType.WORKSHOP

    def test_container_with_no_title(self, extractor):
        """Container without any title element returns None."""
        html = '<div class="event-item"><span>Some text</span></div>'
        container = _soup(html).find("div")
        event = extractor._extract_event_from_container(
            container, "https://example.com", self._make_provenance()
        )
        assert event is None

    def test_container_with_short_title(self, extractor):
        """Container with title shorter than 3 chars returns None."""
        html = '<div class="event-item"><h3>AB</h3></div>'
        container = _soup(html).find("div")
        event = extractor._extract_event_from_container(
            container, "https://example.com", self._make_provenance()
        )
        assert event is None

    def test_container_with_absolute_url(self, extractor):
        """Container with absolute URL preserves it."""
        html = """
        <div class="event-item">
            <a href="https://other.com/events/summit">Industry Summit 2024</a>
        </div>
        """
        container = _soup(html).find("div")
        event = extractor._extract_event_from_container(
            container, "https://example.com", self._make_provenance()
        )
        assert event is not None
        assert event.event_url == "https://other.com/events/summit"


# =============================================================================
# 11. EXTRACT SINGLE EVENT TESTS
# =============================================================================


class TestExtractSingleEvent:
    """Tests for _extract_single_event method."""

    def _make_provenance(self):
        from models.ontology import Provenance
        return Provenance(
            source_url="https://example.com/events/conf",
            source_type="web",
            extracted_by="extraction.event_extractor",
            association_code="PMA",
            job_id="test-job-123",
        )

    def test_full_event_page(self, extractor):
        """Full event detail page extracts all fields."""
        soup = _soup(EVENT_DETAIL_HTML)
        event = extractor._extract_single_event(
            soup, "https://example.com/events/conf", self._make_provenance()
        )
        assert event is not None
        assert "Conference" in event.title or "Manufacturing" in event.title
        assert event.event_type == EventType.CONFERENCE
        assert event.event_url == "https://example.com/events/conf"

    def test_page_with_no_title(self, extractor):
        """Page with no extractable title returns None."""
        html = "<html><body><p>No event here.</p></body></html>"
        event = extractor._extract_single_event(
            _soup(html), "https://example.com", self._make_provenance()
        )
        assert event is None

    def test_json_ld_event(self, extractor):
        """JSON-LD event extracts structured data."""
        soup = _soup(JSON_LD_EVENT_HTML)
        event = extractor._extract_single_event(
            soup, "https://example.com/summit", self._make_provenance()
        )
        assert event is not None
        assert event.title == "Industry Summit 2024"
        assert event.start_date is not None
        assert event.end_date is not None
        assert event.city == "Las Vegas"
        assert event.state == "NV"

    def test_registration_url_extracted(self, extractor):
        """Registration URL is found on detail page."""
        soup = _soup(EVENT_DETAIL_HTML)
        event = extractor._extract_single_event(
            soup, "https://example.com/events/conf", self._make_provenance()
        )
        assert event is not None
        assert event.registration_url is not None
        assert "register" in event.registration_url.lower()

    def test_provenance_attached(self, extractor):
        """Provenance is attached to the event."""
        soup = _soup(EVENT_DETAIL_HTML)
        prov = self._make_provenance()
        event = extractor._extract_single_event(
            soup, "https://example.com/events/conf", prov
        )
        assert event is not None
        assert len(event.provenance) == 1
        assert event.provenance[0].association_code == "PMA"


# =============================================================================
# 12. EXTRACT DATES TESTS
# =============================================================================


class TestExtractDates:
    """Tests for _extract_dates method."""

    def test_json_ld_dates(self, extractor):
        """JSON-LD startDate and endDate are parsed."""
        soup = _soup(JSON_LD_EVENT_HTML)
        start, end = extractor._extract_dates(soup)
        assert start is not None
        assert end is not None
        assert start.month == 3
        assert start.day == 15

    def test_date_element(self, extractor):
        """Date class element is parsed."""
        html = '<html><body><div class="date">March 10, 2024</div></body></html>'
        start, end = extractor._extract_dates(_soup(html))
        assert start is not None
        assert start.month == 3
        assert start.day == 10

    def test_no_dates_found(self, extractor):
        """No dates on page returns (None, None)."""
        html = "<html><body><p>No dates here.</p></body></html>"
        start, end = extractor._extract_dates(_soup(html))
        assert start is None
        assert end is None


# =============================================================================
# 13. RUN METHOD TESTS
# =============================================================================


class TestRun:
    """Tests for the async run() method."""

    @pytest.mark.asyncio
    async def test_success_events_list(self, extractor):
        """EVENTS_LIST page type extracts multiple events."""
        task = {
            "url": "https://example.com/events",
            "html": EVENTS_LIST_HTML,
            "association": "PMA",
            "page_type": "EVENTS_LIST",
        }
        result = await extractor.run(task)
        assert result["success"] is True
        assert result["records_processed"] >= 2
        assert len(result["events"]) >= 2

    @pytest.mark.asyncio
    async def test_success_event_detail(self, extractor):
        """EVENT_DETAIL page type extracts single event."""
        task = {
            "url": "https://example.com/events/conf",
            "html": EVENT_DETAIL_HTML,
            "association": "PMA",
            "page_type": "EVENT_DETAIL",
        }
        result = await extractor.run(task)
        assert result["success"] is True
        assert result["records_processed"] >= 1

    @pytest.mark.asyncio
    async def test_no_url_returns_error(self, extractor):
        """Missing URL returns error response."""
        task = {"html": "<html></html>", "association": "PMA"}
        result = await extractor.run(task)
        assert result["success"] is False
        assert "URL is required" in result["error"]
        assert result["records_processed"] == 0

    @pytest.mark.asyncio
    async def test_http_fetch_failure(self, extractor):
        """HTTP fetch exception returns error."""
        extractor.http.get = AsyncMock(side_effect=Exception("Connection refused"))
        task = {"url": "https://example.com/events", "association": "PMA"}
        result = await extractor.run(task)
        assert result["success"] is False
        assert "Connection refused" in result["error"]

    @pytest.mark.asyncio
    async def test_http_non_200(self, extractor):
        """Non-200 status code returns error."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        extractor.http.get = AsyncMock(return_value=mock_response)
        task = {"url": "https://example.com/events", "association": "PMA"}
        result = await extractor.run(task)
        assert result["success"] is False
        assert "404" in result["error"]

    @pytest.mark.asyncio
    async def test_empty_html_no_events(self, extractor):
        """Empty HTML extracts 0 events but still succeeds."""
        task = {
            "url": "https://example.com/events",
            "html": "<html><body></body></html>",
            "association": "PMA",
            "page_type": "EVENTS_LIST",
        }
        result = await extractor.run(task)
        assert result["success"] is True
        assert result["records_processed"] == 0
        assert len(result["events"]) == 0

    @pytest.mark.asyncio
    async def test_multiple_events_extracted(self, extractor):
        """Multiple event containers produce multiple events."""
        html = """
        <html><body>
        <div class="event-item">
            <h3 class="title"><a href="/e/1">Conference Alpha</a></h3>
            <span class="date">January 10, 2024</span>
        </div>
        <div class="event-item">
            <h3 class="title"><a href="/e/2">Expo Beta</a></h3>
            <span class="date">February 20, 2024</span>
        </div>
        <div class="event-item">
            <h3 class="title"><a href="/e/3">Workshop Gamma</a></h3>
            <span class="date">March 5, 2024</span>
        </div>
        </body></html>
        """
        task = {
            "url": "https://example.com/events",
            "html": html,
            "association": "PMA",
            "page_type": "EVENTS_LIST",
        }
        result = await extractor.run(task)
        assert result["success"] is True
        assert result["records_processed"] == 3

    @pytest.mark.asyncio
    async def test_http_fetch_when_no_html_provided(self, extractor):
        """When no HTML in task, fetches via HTTP."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = EVENT_DETAIL_HTML
        extractor.http.get = AsyncMock(return_value=mock_response)
        task = {
            "url": "https://example.com/events/conf",
            "association": "PMA",
            "page_type": "EVENT_DETAIL",
        }
        result = await extractor.run(task)
        assert result["success"] is True
        extractor.http.get.assert_called_once_with("https://example.com/events/conf", timeout=30)

    @pytest.mark.asyncio
    async def test_default_association_unknown(self, extractor):
        """Default association is 'unknown' when not provided."""
        task = {
            "url": "https://example.com/events",
            "html": "<html><body></body></html>",
        }
        result = await extractor.run(task)
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_default_page_type_events_list(self, extractor):
        """Default page_type is EVENTS_LIST."""
        task = {
            "url": "https://example.com/events",
            "html": EVENTS_LIST_HTML,
            "association": "PMA",
        }
        result = await extractor.run(task)
        assert result["success"] is True
        assert result["records_processed"] >= 2

    @pytest.mark.asyncio
    async def test_events_returned_as_dicts(self, extractor):
        """Events are converted to dicts (not Pydantic models)."""
        task = {
            "url": "https://example.com/events/conf",
            "html": EVENT_DETAIL_HTML,
            "association": "PMA",
            "page_type": "EVENT_DETAIL",
        }
        result = await extractor.run(task)
        assert result["success"] is True
        if result["events"]:
            assert isinstance(result["events"][0], dict)

    @pytest.mark.asyncio
    async def test_max_events_limit(self):
        """max_events config limits the number of extracted events."""
        agent = _create_event_extractor({"max_events": 1})
        html = """
        <html><body>
        <div class="event-item">
            <h3 class="title"><a href="/e/1">Event One</a></h3>
        </div>
        <div class="event-item">
            <h3 class="title"><a href="/e/2">Event Two</a></h3>
        </div>
        <div class="event-item">
            <h3 class="title"><a href="/e/3">Event Three</a></h3>
        </div>
        </body></html>
        """
        task = {
            "url": "https://example.com/events",
            "html": html,
            "association": "PMA",
            "page_type": "EVENTS_LIST",
        }
        result = await agent.run(task)
        assert result["success"] is True
        assert result["records_processed"] <= 1
