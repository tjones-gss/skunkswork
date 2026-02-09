"""
Tests for EventParticipantExtractorAgent
NAM Intelligence Pipeline

Covers initialization, company name cleaning, website extraction,
sponsor tier detection, exhibitor extraction, speaker extraction,
and the async run() entry point.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from middleware.secrets import _reset_secrets_manager
from models.ontology import ParticipantType, Provenance, SponsorTier

# ---------------------------------------------------------------------------
# Factory / fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_secrets_singleton():
    _reset_secrets_manager()
    yield
    _reset_secrets_manager()


def _create_participant_extractor(agent_config=None):
    from agents.extraction.event_participant_extractor import EventParticipantExtractorAgent

    nested = {"extraction": {"event_participant_extractor": agent_config or {}}}
    with (
        patch("agents.base.Config") as mock_config,
        patch("agents.base.StructuredLogger"),
        patch("agents.base.AsyncHTTPClient"),
        patch("agents.base.RateLimiter"),
    ):
        mock_config.return_value.load.return_value = nested
        agent = EventParticipantExtractorAgent(
            agent_type="extraction.event_participant_extractor",
            job_id="test-job-123",
        )
        return agent


@pytest.fixture
def extractor():
    return _create_participant_extractor()


def _make_provenance(url="https://example.com/sponsors"):
    return Provenance(
        source_url=url,
        source_type="web",
        extracted_by="extraction.event_participant_extractor",
        association_code="TEST",
        job_id="test-job-123",
    )


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

SPONSORS_HTML = """\
<html><body>
<section>
<h2>Platinum Sponsors</h2>
<div class="platinum">
    <a href="https://acme.com"><img alt="Acme Corp" src="logo.png"></a>
    <a href="https://beta.com">Beta Industries</a>
</div>
</section>
<section>
<h2>Gold Sponsors</h2>
<div class="gold">
    <a href="https://gamma.com">Gamma Systems</a>
</div>
</section>
</body></html>
"""

EXHIBITORS_TABLE_HTML = """\
<html><body>
<table>
<tr><th>Company</th><th>Booth</th><th>Category</th></tr>
<tr><td>Acme Corp</td><td>Booth #101</td><td>CNC Machines</td></tr>
<tr><td>Beta Inc</td><td>A202</td><td>Welding</td></tr>
</table>
</body></html>
"""

EXHIBITORS_LIST_HTML = """\
<html><body>
<div class="exhibitor-list">
    <div class="exhibitor-item">
        <h3>Acme Corp</h3>
        <span>Booth #201</span>
        <span class="category">CNC Machines</span>
        <a href="https://acme.com">Website</a>
    </div>
    <div class="exhibitor-item">
        <h3>Beta Inc</h3>
        <span>Booth #202</span>
    </div>
</div>
</body></html>
"""

EXHIBITORS_CARD_HTML = """\
<html><body>
<div class="exhibitor-card">
    <strong>Delta Corp</strong>
    <span>Booth #301</span>
    <span class="category">Welding</span>
    <a href="https://delta.com">Visit</a>
</div>
<div class="vendor-card">
    <h3>Epsilon Ltd</h3>
</div>
</body></html>
"""

SPEAKERS_HTML = """\
<html><body>
<div class="speaker-card">
    <h3 class="name">John Smith</h3>
    <span class="title">VP of Engineering at Acme Corp</span>
    <span class="presentation">Automation in Manufacturing</span>
</div>
<div class="speaker-card">
    <span class="name">Jane Doe</span>
    <span class="title">CTO</span>
    <span class="company">Beta Industries</span>
</div>
</body></html>
"""

SPEAKERS_DIV_HTML = """\
<html><body>
<div class="speaker">
    <h3>Alice Wong</h3>
    <span class="role">Director of Ops</span>
    <span class="organization">Gamma Systems</span>
    <span class="topic">Lean Manufacturing</span>
</div>
</body></html>
"""

GENERIC_SPONSORS_HTML_BY_CLASS = """\
<html><body>
<div class="sponsor-section">
    <a href="https://acme.com"><img alt="Acme Corp" src="logo.png"></a>
    <a href="https://beta.com">Beta Industries</a>
</div>
</body></html>
"""

GENERIC_SPONSORS_HTML_BY_ID = """\
<html><body>
<div id="sponsors-panel">
    <a href="https://gamma.com">Gamma Systems</a>
</div>
</body></html>
"""


# ===================================================================
# TestInitialization
# ===================================================================


class TestInitialization:
    def test_default_max_participants(self, extractor):
        assert extractor.max_participants == 500

    def test_custom_max_participants(self):
        agent = _create_participant_extractor({"max_participants": 100})
        assert agent.max_participants == 100


# ===================================================================
# TestCleanCompanyName
# ===================================================================


class TestCleanCompanyName:
    def test_normal_name(self, extractor):
        assert extractor._clean_company_name("Acme Corp") == "Acme Corp"

    def test_name_with_logo(self, extractor):
        result = extractor._clean_company_name("Acme Corp logo")
        assert result is not None
        assert "logo" not in result.lower()

    def test_name_with_sponsor(self, extractor):
        result = extractor._clean_company_name("Acme sponsor Corp")
        assert result is not None
        assert "sponsor" not in result.lower()

    def test_name_with_image(self, extractor):
        result = extractor._clean_company_name("image Acme Corp")
        assert result is not None
        assert "image" not in result.lower()

    def test_empty_string(self, extractor):
        assert extractor._clean_company_name("") is None

    def test_none_input(self, extractor):
        assert extractor._clean_company_name(None) is None

    def test_too_short(self, extractor):
        assert extractor._clean_company_name("A") is None

    def test_too_long(self, extractor):
        assert extractor._clean_company_name("X" * 201) is None

    def test_exact_noise_word_logo(self, extractor):
        assert extractor._clean_company_name("logo") is None

    def test_exact_noise_word_sponsor(self, extractor):
        assert extractor._clean_company_name("sponsor") is None

    def test_exact_noise_word_partner(self, extractor):
        assert extractor._clean_company_name("partner") is None

    def test_exact_noise_word_image(self, extractor):
        assert extractor._clean_company_name("image") is None

    def test_exact_noise_word_photo(self, extractor):
        assert extractor._clean_company_name("photo") is None


# ===================================================================
# TestExtractWebsiteFromLink
# ===================================================================


class TestExtractWebsiteFromLink:
    def test_image_inside_parent_link(self, extractor):
        html = '<a href="https://acme.com"><img alt="Acme" src="logo.png"></a>'
        soup = _soup(html)
        img = soup.find("img")
        assert extractor._extract_website_from_link(img) == "https://acme.com"

    def test_no_parent_link(self, extractor):
        html = '<div><img alt="Acme" src="logo.png"></div>'
        soup = _soup(html)
        img = soup.find("img")
        assert extractor._extract_website_from_link(img) is None

    def test_parent_link_without_href(self, extractor):
        html = '<a><img alt="Acme" src="logo.png"></a>'
        soup = _soup(html)
        img = soup.find("img")
        assert extractor._extract_website_from_link(img) is None

    def test_relative_url(self, extractor):
        html = '<a href="/companies/acme"><img alt="Acme" src="logo.png"></a>'
        soup = _soup(html)
        img = soup.find("img")
        assert extractor._extract_website_from_link(img) is None


# ===================================================================
# TestSponsorTierDetection
# ===================================================================


class TestSponsorTierDetection:
    """Tests that _find_tier_sponsors matches each tier's keywords."""

    def _make_html(self, keyword, company="Test Corp"):
        return f"""\
<html><body>
<section>
<h2>{keyword.title()} Sponsors</h2>
<div>
    <a href="https://test.com">{company}</a>
</div>
</section>
</body></html>
"""

    @pytest.mark.parametrize("keyword", ["platinum", "diamond", "premier", "presenting", "title"])
    def test_platinum_tier(self, extractor, keyword):
        soup = _soup(self._make_html(keyword))
        prov = _make_provenance()
        results = extractor._find_tier_sponsors(
            soup, SponsorTier.PLATINUM,
            [keyword], "https://example.com", None, prov,
        )
        assert len(results) >= 1
        assert results[0].sponsor_tier == SponsorTier.PLATINUM

    def test_gold_tier(self, extractor):
        soup = _soup(self._make_html("gold"))
        prov = _make_provenance()
        results = extractor._find_tier_sponsors(
            soup, SponsorTier.GOLD, ["gold"], "https://example.com", None, prov,
        )
        assert len(results) >= 1
        assert results[0].sponsor_tier == SponsorTier.GOLD

    def test_silver_tier(self, extractor):
        soup = _soup(self._make_html("silver"))
        prov = _make_provenance()
        results = extractor._find_tier_sponsors(
            soup, SponsorTier.SILVER, ["silver"], "https://example.com", None, prov,
        )
        assert len(results) >= 1
        assert results[0].sponsor_tier == SponsorTier.SILVER

    def test_bronze_tier(self, extractor):
        soup = _soup(self._make_html("bronze"))
        prov = _make_provenance()
        results = extractor._find_tier_sponsors(
            soup, SponsorTier.BRONZE, ["bronze"], "https://example.com", None, prov,
        )
        assert len(results) >= 1
        assert results[0].sponsor_tier == SponsorTier.BRONZE

    @pytest.mark.parametrize("keyword", ["partner", "strategic", "associate"])
    def test_partner_tier(self, extractor, keyword):
        soup = _soup(self._make_html(keyword))
        prov = _make_provenance()
        results = extractor._find_tier_sponsors(
            soup, SponsorTier.PARTNER,
            [keyword], "https://example.com", None, prov,
        )
        assert len(results) >= 1
        assert results[0].sponsor_tier == SponsorTier.PARTNER

    @pytest.mark.parametrize("keyword", ["media", "press"])
    def test_media_tier(self, extractor, keyword):
        soup = _soup(self._make_html(keyword))
        prov = _make_provenance()
        results = extractor._find_tier_sponsors(
            soup, SponsorTier.MEDIA,
            [keyword], "https://example.com", None, prov,
        )
        assert len(results) >= 1
        assert results[0].sponsor_tier == SponsorTier.MEDIA


# ===================================================================
# TestExtractSponsorsFromContainer
# ===================================================================


class TestExtractSponsorsFromContainer:
    def test_container_with_images(self, extractor):
        html = """\
<div>
    <a href="https://acme.com"><img alt="Acme Corp" src="logo.png"></a>
    <a href="https://beta.com"><img alt="Beta Inc" src="logo2.png"></a>
</div>"""
        container = _soup(html).find("div")
        prov = _make_provenance()
        results = extractor._extract_sponsors_from_container(
            container, SponsorTier.GOLD, "https://example.com", None, prov,
        )
        assert len(results) == 2
        names = {s.company_name for s in results}
        assert "Acme Corp" in names
        assert "Beta Inc" in names

    def test_container_with_linked_text(self, extractor):
        html = '<div><a href="https://gamma.com">Gamma Systems</a></div>'
        container = _soup(html).find("div")
        prov = _make_provenance()
        results = extractor._extract_sponsors_from_container(
            container, SponsorTier.SILVER, "https://example.com", None, prov,
        )
        assert len(results) == 1
        assert results[0].company_name == "Gamma Systems"
        assert results[0].company_website == "https://gamma.com"

    def test_duplicate_avoidance(self, extractor):
        html = """\
<div>
    <a href="https://acme.com"><img alt="Acme Corp" src="logo.png"></a>
    <a href="https://acme.com">Acme Corp</a>
</div>"""
        container = _soup(html).find("div")
        prov = _make_provenance()
        results = extractor._extract_sponsors_from_container(
            container, SponsorTier.GOLD, "https://example.com", None, prov,
        )
        names = [s.company_name for s in results]
        assert names.count("Acme Corp") == 1

    def test_short_alt_text_ignored(self, extractor):
        html = '<div><img alt="AB" src="logo.png"></div>'
        container = _soup(html).find("div")
        prov = _make_provenance()
        results = extractor._extract_sponsors_from_container(
            container, SponsorTier.GOLD, "https://example.com", None, prov,
        )
        assert len(results) == 0

    def test_link_starting_with_http_skipped_as_company_name(self, extractor):
        html = '<div><a href="https://acme.com">https://acme.com</a></div>'
        container = _soup(html).find("div")
        prov = _make_provenance()
        results = extractor._extract_sponsors_from_container(
            container, SponsorTier.GOLD, "https://example.com", None, prov,
        )
        assert len(results) == 0

    def test_link_starting_with_www_skipped_as_company_name(self, extractor):
        html = '<div><a href="https://acme.com">www.acme.com</a></div>'
        container = _soup(html).find("div")
        prov = _make_provenance()
        results = extractor._extract_sponsors_from_container(
            container, SponsorTier.GOLD, "https://example.com", None, prov,
        )
        assert len(results) == 0

    def test_relative_href_resolved(self, extractor):
        html = '<div><a href="/sponsors/delta">Delta Corp</a></div>'
        container = _soup(html).find("div")
        prov = _make_provenance()
        results = extractor._extract_sponsors_from_container(
            container, SponsorTier.GOLD, "https://example.com", None, prov,
        )
        assert len(results) == 1
        assert results[0].company_website == "https://example.com/sponsors/delta"


# ===================================================================
# TestExtractGenericSponsors
# ===================================================================


class TestExtractGenericSponsors:
    def test_sponsor_section_by_class(self, extractor):
        soup = _soup(GENERIC_SPONSORS_HTML_BY_CLASS)
        prov = _make_provenance()
        results = extractor._extract_generic_sponsors(soup, "https://example.com", None, prov)
        assert len(results) >= 1

    def test_sponsor_section_by_id(self, extractor):
        soup = _soup(GENERIC_SPONSORS_HTML_BY_ID)
        prov = _make_provenance()
        results = extractor._extract_generic_sponsors(soup, "https://example.com", None, prov)
        assert len(results) >= 1

    def test_no_sponsor_section(self, extractor):
        soup = _soup("<html><body><p>Nothing here</p></body></html>")
        prov = _make_provenance()
        results = extractor._extract_generic_sponsors(soup, "https://example.com", None, prov)
        assert results == []


# ===================================================================
# TestExtractSponsors
# ===================================================================


class TestExtractSponsors:
    def test_tiered_sponsors_found(self, extractor):
        soup = _soup(SPONSORS_HTML)
        prov = _make_provenance()
        results = extractor._extract_sponsors(soup, "https://example.com", "evt-1", prov)
        assert len(results) >= 3
        tiers = {s.sponsor_tier for s in results}
        assert SponsorTier.PLATINUM in tiers
        assert SponsorTier.GOLD in tiers

    def test_no_tiered_falls_back_to_generic(self, extractor):
        soup = _soup(GENERIC_SPONSORS_HTML_BY_CLASS)
        prov = _make_provenance()
        results = extractor._extract_sponsors(soup, "https://example.com", None, prov)
        assert len(results) >= 1
        assert all(s.sponsor_tier == SponsorTier.OTHER for s in results)

    def test_max_participants_limit(self):
        agent = _create_participant_extractor({"max_participants": 2})
        soup = _soup(SPONSORS_HTML)
        prov = _make_provenance()
        results = agent._extract_sponsors(soup, "https://example.com", None, prov)
        assert len(results) <= 2


# ===================================================================
# TestExtractExhibitors
# ===================================================================


class TestExtractExhibitors:
    def test_table_format(self, extractor):
        soup = _soup(EXHIBITORS_TABLE_HTML)
        prov = _make_provenance()
        results = extractor._extract_exhibitors(soup, "https://example.com", "evt-1", prov)
        assert len(results) == 2
        names = {e.company_name for e in results}
        assert "Acme Corp" in names
        assert "Beta Inc" in names

    def test_list_format(self, extractor):
        soup = _soup(EXHIBITORS_LIST_HTML)
        prov = _make_provenance()
        results = extractor._extract_exhibitors(soup, "https://example.com", "evt-1", prov)
        assert len(results) >= 1
        assert any(e.company_name == "Acme Corp" for e in results)

    def test_card_format(self, extractor):
        soup = _soup(EXHIBITORS_CARD_HTML)
        prov = _make_provenance()
        results = extractor._extract_exhibitors(soup, "https://example.com", "evt-1", prov)
        assert len(results) >= 1
        names = {e.company_name for e in results}
        assert "Delta Corp" in names or "Epsilon Ltd" in names

    def test_max_participants_limit(self):
        agent = _create_participant_extractor({"max_participants": 1})
        soup = _soup(EXHIBITORS_TABLE_HTML)
        prov = _make_provenance()
        results = agent._extract_exhibitors(soup, "https://example.com", None, prov)
        assert len(results) <= 1


# ===================================================================
# TestExtractExhibitorFromRow
# ===================================================================


class TestExtractExhibitorFromRow:
    def test_row_with_all_fields(self, extractor):
        html = """\
<table><tr>
<td>Acme Corp</td>
<td>Booth #123</td>
<td>CNC Machines</td>
<td><a href="https://acme.com">Website</a></td>
</tr></table>"""
        row = _soup(html).find("tr")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_row(row, "https://example.com", "evt-1", prov)
        assert result is not None
        assert result.company_name == "Acme Corp"
        assert result.booth_number == "123"
        assert result.company_website == "https://acme.com"
        assert result.participant_type == ParticipantType.EXHIBITOR

    def test_standalone_booth_number(self, extractor):
        html = '<table><tr><td>Beta Inc</td><td>A101</td></tr></table>'
        row = _soup(html).find("tr")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_row(row, "https://example.com", None, prov)
        assert result is not None
        assert result.booth_number == "A101"

    def test_row_no_cells(self, extractor):
        html = "<table><tr></tr></table>"
        row = _soup(html).find("tr")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_row(row, "https://example.com", None, prov)
        assert result is None

    def test_short_company_name(self, extractor):
        html = "<table><tr><td>X</td></tr></table>"
        row = _soup(html).find("tr")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_row(row, "https://example.com", None, prov)
        assert result is None

    def test_long_category_ignored(self, extractor):
        long_cat = "X" * 101
        html = f"<table><tr><td>Acme Corp</td><td>101</td><td>{long_cat}</td></tr></table>"
        row = _soup(html).find("tr")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_row(row, "https://example.com", None, prov)
        assert result is not None
        assert result.booth_category is None

    def test_event_id_set(self, extractor):
        html = "<table><tr><td>Acme Corp</td></tr></table>"
        row = _soup(html).find("tr")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_row(row, "https://example.com", "evt-99", prov)
        assert result.event_id == "evt-99"

    def test_event_id_defaults_to_unknown(self, extractor):
        html = "<table><tr><td>Acme Corp</td></tr></table>"
        row = _soup(html).find("tr")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_row(row, "https://example.com", None, prov)
        assert result.event_id == "unknown"


# ===================================================================
# TestExtractExhibitorFromItem
# ===================================================================


class TestExtractExhibitorFromItem:
    def test_item_with_h3(self, extractor):
        html = '<li><h3>Acme Corp</h3><span>Booth #999</span></li>'
        item = _soup(html).find("li")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_item(item, "https://example.com", None, prov)
        assert result is not None
        assert result.company_name == "Acme Corp"
        assert result.booth_number == "999"

    def test_item_with_strong(self, extractor):
        html = '<div><strong>Beta Inc</strong></div>'
        item = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_item(item, "https://example.com", None, prov)
        assert result is not None
        assert result.company_name == "Beta Inc"

    def test_item_with_link_as_name(self, extractor):
        html = '<div><a href="https://gamma.com">Gamma Systems</a></div>'
        item = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_item(item, "https://example.com", None, prov)
        assert result is not None
        assert result.company_name == "Gamma Systems"

    def test_category_class(self, extractor):
        html = '<div><h3>Acme</h3><span class="category">CNC</span></div>'
        item = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_item(item, "https://example.com", None, prov)
        assert result.booth_category == "CNC"

    def test_website_link(self, extractor):
        html = '<div><h3>Acme</h3><a href="https://acme.com">Visit</a></div>'
        item = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_item(item, "https://example.com", None, prov)
        assert result.company_website == "https://acme.com"

    def test_no_name_element(self, extractor):
        html = "<div><span>Some text</span></div>"
        item = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_item(item, "https://example.com", None, prov)
        assert result is None

    def test_short_name(self, extractor):
        html = "<div><h3>X</h3></div>"
        item = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_exhibitor_from_item(item, "https://example.com", None, prov)
        assert result is None


# ===================================================================
# TestExtractSpeakers
# ===================================================================


class TestExtractSpeakers:
    def test_speaker_cards(self, extractor):
        soup = _soup(SPEAKERS_HTML)
        prov = _make_provenance()
        results = extractor._extract_speakers(soup, "https://example.com", "evt-1", prov)
        assert len(results) == 2
        names = {s.speaker_name for s in results}
        assert "John Smith" in names
        assert "Jane Doe" in names

    def test_speaker_divs(self, extractor):
        soup = _soup(SPEAKERS_DIV_HTML)
        prov = _make_provenance()
        results = extractor._extract_speakers(soup, "https://example.com", None, prov)
        assert len(results) == 1
        assert results[0].speaker_name == "Alice Wong"

    def test_at_pattern_splits_title_and_company(self, extractor):
        soup = _soup(SPEAKERS_HTML)
        prov = _make_provenance()
        results = extractor._extract_speakers(soup, "https://example.com", None, prov)
        john = next(s for s in results if s.speaker_name == "John Smith")
        assert john.company_name == "Acme Corp"
        assert john.speaker_title == "VP of Engineering"

    def test_company_from_class(self, extractor):
        soup = _soup(SPEAKERS_HTML)
        prov = _make_provenance()
        results = extractor._extract_speakers(soup, "https://example.com", None, prov)
        jane = next(s for s in results if s.speaker_name == "Jane Doe")
        assert jane.company_name == "Beta Industries"

    def test_no_company_defaults_to_unknown(self, extractor):
        html = """\
<html><body>
<div class="speaker-card">
    <h3 class="name">Solo Speaker</h3>
</div>
</body></html>"""
        soup = _soup(html)
        prov = _make_provenance()
        results = extractor._extract_speakers(soup, "https://example.com", None, prov)
        assert len(results) == 1
        assert results[0].company_name == "Unknown"

    def test_no_speaker_elements(self, extractor):
        soup = _soup("<html><body><p>No speakers</p></body></html>")
        prov = _make_provenance()
        results = extractor._extract_speakers(soup, "https://example.com", None, prov)
        assert results == []

    def test_presentation_title(self, extractor):
        soup = _soup(SPEAKERS_HTML)
        prov = _make_provenance()
        results = extractor._extract_speakers(soup, "https://example.com", None, prov)
        john = next(s for s in results if s.speaker_name == "John Smith")
        assert john.presentation_title == "Automation in Manufacturing"


# ===================================================================
# TestExtractSpeakerFromElement
# ===================================================================


class TestExtractSpeakerFromElement:
    def test_name_from_class(self, extractor):
        html = '<div><span class="name">John Smith</span></div>'
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result is not None
        assert result.speaker_name == "John Smith"

    def test_name_from_h3(self, extractor):
        html = "<div><h3>Alice Wong</h3></div>"
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result.speaker_name == "Alice Wong"

    def test_title_from_class(self, extractor):
        html = '<div><h3>John</h3><span class="position">CTO</span></div>'
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result.speaker_title == "CTO"

    def test_company_from_class(self, extractor):
        html = '<div><h3>John</h3><span class="organization">Acme</span></div>'
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result.company_name == "Acme"

    def test_company_from_at_pattern(self, extractor):
        html = '<div><span class="name">John</span><span class="title">CTO at BigCo</span></div>'
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result.company_name == "BigCo"
        assert result.speaker_title == "CTO"

    def test_presentation_from_class(self, extractor):
        html = '<div><h3>John</h3><span class="session">Keynote</span></div>'
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result.presentation_title == "Keynote"

    def test_no_name_element(self, extractor):
        html = "<div><span>Some text</span></div>"
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result is None

    def test_short_name(self, extractor):
        html = "<div><h3>X</h3></div>"
        elem = _soup(html).find("div")
        prov = _make_provenance()
        result = extractor._extract_speaker_from_element(elem, "https://example.com", None, prov)
        assert result is None


# ===================================================================
# TestRun
# ===================================================================


class TestRun:
    @pytest.mark.asyncio
    async def test_sponsors_list_page_type(self, extractor):
        extractor.http = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = SPONSORS_HTML
        extractor.http.get = AsyncMock(return_value=resp)

        result = await extractor.run({
            "url": "https://example.com/sponsors",
            "page_type": "SPONSORS_LIST",
            "association": "TEST",
        })
        assert result["success"] is True
        assert result["records_processed"] >= 1
        assert len(result["participants"]) >= 1

    @pytest.mark.asyncio
    async def test_exhibitors_list_page_type(self, extractor):
        extractor.http = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = EXHIBITORS_TABLE_HTML
        extractor.http.get = AsyncMock(return_value=resp)

        result = await extractor.run({
            "url": "https://example.com/exhibitors",
            "page_type": "EXHIBITORS_LIST",
            "association": "TEST",
        })
        assert result["success"] is True
        assert result["records_processed"] >= 1

    @pytest.mark.asyncio
    async def test_participants_list_auto_detect_sponsors(self, extractor):
        html_with_sponsor = """\
<html><body>
<p>Thank you to our sponsors!</p>
<div class="sponsor-section">
    <a href="https://acme.com"><img alt="Acme Corp" src="logo.png"></a>
</div>
</body></html>"""
        extractor.http = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = html_with_sponsor
        extractor.http.get = AsyncMock(return_value=resp)

        result = await extractor.run({
            "url": "https://example.com/page",
            "page_type": "PARTICIPANTS_LIST",
            "association": "TEST",
        })
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_participants_list_auto_detect_exhibitors(self, extractor):
        html_with_exhibitor = """\
<html><body>
<p>Our exhibitors include the following companies.</p>
<table>
<tr><th>Company</th></tr>
<tr><td>Acme Corp</td></tr>
</table>
</body></html>"""
        extractor.http = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = html_with_exhibitor
        extractor.http.get = AsyncMock(return_value=resp)

        result = await extractor.run({
            "url": "https://example.com/page",
            "page_type": "PARTICIPANTS_LIST",
            "association": "TEST",
        })
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_participants_list_auto_detect_speakers(self, extractor):
        extractor.http = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = SPEAKERS_HTML.replace("</body>", "<p>Our speakers</p></body>")
        extractor.http.get = AsyncMock(return_value=resp)

        result = await extractor.run({
            "url": "https://example.com/page",
            "page_type": "PARTICIPANTS_LIST",
            "association": "TEST",
        })
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_participants_list_fallback_tries_all(self, extractor):
        html_no_indicators = """\
<html><body>
<table>
<tr><th>Company</th></tr>
<tr><td>Acme Corp</td></tr>
</table>
</body></html>"""
        extractor.http = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = html_no_indicators
        extractor.http.get = AsyncMock(return_value=resp)

        result = await extractor.run({
            "url": "https://example.com/page",
            "page_type": "PARTICIPANTS_LIST",
            "association": "TEST",
        })
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_no_url_returns_error(self, extractor):
        result = await extractor.run({"association": "TEST"})
        assert result["success"] is False
        assert "URL is required" in result["error"]

    @pytest.mark.asyncio
    async def test_http_error(self, extractor):
        extractor.http = AsyncMock()
        resp = MagicMock()
        resp.status_code = 403
        extractor.http.get = AsyncMock(return_value=resp)

        result = await extractor.run({
            "url": "https://example.com/sponsors",
            "page_type": "SPONSORS_LIST",
            "association": "TEST",
        })
        assert result["success"] is False
        assert "403" in result["error"]

    @pytest.mark.asyncio
    async def test_http_exception(self, extractor):
        extractor.http = AsyncMock()
        extractor.http.get = AsyncMock(side_effect=ConnectionError("refused"))

        result = await extractor.run({
            "url": "https://example.com/sponsors",
            "page_type": "SPONSORS_LIST",
            "association": "TEST",
        })
        assert result["success"] is False
        assert "refused" in result["error"]

    @pytest.mark.asyncio
    async def test_with_pre_fetched_html(self, extractor):
        extractor.http = AsyncMock()

        result = await extractor.run({
            "url": "https://example.com/sponsors",
            "html": SPONSORS_HTML,
            "page_type": "SPONSORS_LIST",
            "association": "TEST",
        })
        assert result["success"] is True
        extractor.http.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_event_id_passed_through(self, extractor):
        extractor.http = AsyncMock()

        result = await extractor.run({
            "url": "https://example.com/sponsors",
            "html": SPONSORS_HTML,
            "page_type": "SPONSORS_LIST",
            "association": "TEST",
            "event_id": "evt-42",
        })
        assert result["success"] is True
        for p in result["participants"]:
            assert p["event_id"] == "evt-42"

    @pytest.mark.asyncio
    async def test_records_processed_count(self, extractor):
        result = await extractor.run({
            "url": "https://example.com/sponsors",
            "html": SPONSORS_HTML,
            "page_type": "SPONSORS_LIST",
            "association": "TEST",
        })
        assert result["records_processed"] == len(result["participants"])

    @pytest.mark.asyncio
    async def test_default_page_type_is_participants_list(self, extractor):
        html = '<html><body><p>Our sponsors are great</p><div class="sponsor-section"><a href="https://a.com"><img alt="Acme Corp" src="x.png"></a></div></body></html>'
        result = await extractor.run({
            "url": "https://example.com/page",
            "html": html,
            "association": "TEST",
        })
        assert result["success"] is True
