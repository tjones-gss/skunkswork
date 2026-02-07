"""
Ontology Models
NAM Intelligence Pipeline

Pydantic models defining the data schema for the pipeline.
Based on gss-research-engine/ontology.yaml definitions.
"""

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

# =============================================================================
# ENUMERATIONS
# =============================================================================

class PageType(StrEnum):
    """Classification of web page types discovered during crawling."""

    ASSOCIATION_HOME = "ASSOCIATION_HOME"
    ASSOCIATION_DIRECTORY = "ASSOCIATION_DIRECTORY"
    MEMBER_DIRECTORY = "MEMBER_DIRECTORY"
    MEMBER_DETAIL = "MEMBER_DETAIL"
    EVENTS_LIST = "EVENTS_LIST"
    EVENT_DETAIL = "EVENT_DETAIL"
    PARTICIPANTS_LIST = "PARTICIPANTS_LIST"
    EXHIBITORS_LIST = "EXHIBITORS_LIST"
    SPONSORS_LIST = "SPONSORS_LIST"
    RESOURCE = "RESOURCE"
    OTHER = "OTHER"


class EntityType(StrEnum):
    """Types of entities tracked in the pipeline."""

    ASSOCIATION = "Association"
    COMPANY = "Company"
    EVENT = "Event"
    PERSON = "Person"
    COMPETITOR = "Competitor"


class RelationshipType(StrEnum):
    """Types of relationships between entities."""

    ASSOCIATION_HAS_MEMBER = "ASSOCIATION_HAS_MEMBER"
    ASSOCIATION_HOSTS_EVENT = "ASSOCIATION_HOSTS_EVENT"
    EVENT_HAS_PARTICIPANT = "EVENT_HAS_PARTICIPANT"
    EVENT_HAS_EXHIBITOR = "EVENT_HAS_EXHIBITOR"
    EVENT_HAS_SPONSOR = "EVENT_HAS_SPONSOR"
    ENTITY_MENTIONED_COMPETITOR = "ENTITY_MENTIONED_COMPETITOR"
    COMPANY_USES_TECHNOLOGY = "COMPANY_USES_TECHNOLOGY"
    PERSON_WORKS_AT = "PERSON_WORKS_AT"


class ParticipantType(StrEnum):
    """Types of event participants."""

    SPONSOR = "SPONSOR"
    EXHIBITOR = "EXHIBITOR"
    ATTENDEE = "ATTENDEE"
    SPEAKER = "SPEAKER"
    ORGANIZER = "ORGANIZER"


class SponsorTier(StrEnum):
    """Sponsorship tier levels."""

    PLATINUM = "PLATINUM"
    GOLD = "GOLD"
    SILVER = "SILVER"
    BRONZE = "BRONZE"
    PARTNER = "PARTNER"
    MEDIA = "MEDIA"
    OTHER = "OTHER"


class CompetitorSignalType(StrEnum):
    """Types of competitor signals detected."""

    SPONSOR = "SPONSOR"
    EXHIBITOR = "EXHIBITOR"
    MEMBER_USAGE = "MEMBER_USAGE"
    SPEAKER_BIO = "SPEAKER_BIO"
    PARTNER_INTEGRATION = "PARTNER_INTEGRATION"
    JOB_POSTING = "JOB_POSTING"
    CASE_STUDY = "CASE_STUDY"
    PRESS_RELEASE = "PRESS_RELEASE"


class EventType(StrEnum):
    """Types of events."""

    CONFERENCE = "CONFERENCE"
    TRADE_SHOW = "TRADE_SHOW"
    WEBINAR = "WEBINAR"
    WORKSHOP = "WORKSHOP"
    NETWORKING = "NETWORKING"
    TRAINING = "TRAINING"
    ANNUAL_MEETING = "ANNUAL_MEETING"
    OTHER = "OTHER"


class QualityGrade(StrEnum):
    """Quality grade for records."""

    A = "A"  # 90-100
    B = "B"  # 80-89
    C = "C"  # 70-79
    D = "D"  # 60-69
    F = "F"  # Below 60


# =============================================================================
# BASE MODELS
# =============================================================================

class Provenance(BaseModel):
    """Tracks the origin and lineage of extracted data."""

    source_url: str = Field(..., description="URL where data was extracted from")
    source_type: str = Field(default="web", description="Type of source (web, api, pdf)")
    extracted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    extracted_by: str = Field(..., description="Agent that performed extraction")
    association_code: str | None = Field(default=None, description="Association code if applicable")
    job_id: str | None = Field(default=None, description="Pipeline job ID")
    page_type: PageType | None = Field(default=None, description="Classified page type")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="Extraction confidence")

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class Contact(BaseModel):
    """Contact information for a person."""

    full_name: str = Field(..., description="Full name of contact")
    first_name: str | None = None
    last_name: str | None = None
    title: str | None = None
    department: str | None = None
    seniority: str | None = None
    email: str | None = None
    email_verified: bool = False
    phone: str | None = None
    linkedin_url: str | None = None
    data_source: str | None = None
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)


# =============================================================================
# CORE ENTITY MODELS
# =============================================================================

class Company(BaseModel):
    """Company entity with all firmographic and enrichment data."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    # Core identification
    company_name: str = Field(..., description="Canonical company name")
    normalized_name: str | None = Field(default=None, description="Normalized name for matching")
    domain: str | None = Field(default=None, description="Primary website domain")
    website: str | None = Field(default=None, description="Full website URL")

    # Location
    city: str | None = None
    state: str | None = Field(default=None, max_length=50)
    country: str = Field(default="United States")
    full_address: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    # Firmographics
    employee_count_min: int | None = Field(default=None, ge=0)
    employee_count_max: int | None = Field(default=None, ge=0)
    revenue_min_usd: int | None = Field(default=None, ge=0)
    revenue_max_usd: int | None = Field(default=None, ge=0)
    year_founded: int | None = Field(default=None, ge=1800, le=2030)
    naics_code: str | None = None
    sic_code: str | None = None
    industry: str | None = None

    # Technology
    erp_system: str | None = None
    crm_system: str | None = None
    tech_stack: list[str] = Field(default_factory=list)

    # Associations and contacts
    associations: list[str] = Field(default_factory=list, description="Association codes")
    contacts: list[Contact] = Field(default_factory=list)

    # Quality metadata
    quality_score: int | None = Field(default=None, ge=0, le=100)
    quality_grade: QualityGrade | None = None
    data_sources: list[str] = Field(default_factory=list)

    # Provenance tracking
    provenance: list[Provenance] = Field(default_factory=list)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_verified_at: datetime | None = None

    @field_validator('normalized_name', mode='before')
    @classmethod
    def auto_normalize_name(cls, v, info):
        """Auto-generate normalized name if not provided."""
        if v is None and 'company_name' in info.data:
            from skills.common.SKILL import normalize_company_name
            return normalize_company_name(info.data['company_name'])
        return v

    def add_provenance(self, provenance: Provenance):
        """Add a provenance record."""
        self.provenance.append(provenance)
        self.updated_at = datetime.now(UTC)

    def merge_from(self, other: "Company", overwrite: bool = False):
        """Merge data from another company record."""
        for field_name, _field_info in self.model_fields.items():
            if field_name in ('id', 'created_at', 'provenance'):
                continue

            other_value = getattr(other, field_name, None)
            current_value = getattr(self, field_name, None)

            if other_value is None:
                continue

            # For lists, combine them
            if isinstance(current_value, list) and isinstance(other_value, list):
                combined = list(set(current_value + other_value))
                setattr(self, field_name, combined)
            # For other fields, overwrite if current is empty or overwrite flag is set
            elif not current_value or overwrite:
                setattr(self, field_name, other_value)

        # Always merge provenance
        self.provenance.extend(other.provenance)
        self.updated_at = datetime.now(UTC)


class Event(BaseModel):
    """Event entity for conferences, trade shows, webinars, etc."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    # Core identification
    title: str = Field(..., description="Event title")
    event_type: EventType = Field(default=EventType.OTHER)
    description: str | None = None

    # Dates
    start_date: datetime | None = None
    end_date: datetime | None = None
    registration_deadline: datetime | None = None

    # Location
    venue: str | None = None
    city: str | None = None
    state: str | None = None
    country: str = Field(default="United States")
    is_virtual: bool = Field(default=False)

    # URLs
    event_url: str | None = None
    registration_url: str | None = None

    # Organizer
    organizer_name: str | None = None
    organizer_association: str | None = Field(default=None, description="Association code")

    # Participants
    expected_attendees: int | None = None
    exhibitor_count: int | None = None
    sponsor_count: int | None = None

    # Provenance
    provenance: list[Provenance] = Field(default_factory=list)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class EventParticipant(BaseModel):
    """Participant in an event (sponsor, exhibitor, attendee, speaker)."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    # Links
    event_id: str = Field(..., description="Event this participant belongs to")
    company_id: str | None = Field(default=None, description="Linked company if resolved")

    # Participant info
    participant_type: ParticipantType
    company_name: str = Field(..., description="Company name as listed")
    company_website: str | None = None

    # Sponsor-specific
    sponsor_tier: SponsorTier | None = None

    # Exhibitor-specific
    booth_number: str | None = None
    booth_category: str | None = None

    # Speaker-specific
    speaker_name: str | None = None
    speaker_title: str | None = None
    presentation_title: str | None = None

    # Provenance
    provenance: list[Provenance] = Field(default_factory=list)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CompetitorSignal(BaseModel):
    """Signal indicating competitor presence or usage."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    # Competitor identification
    competitor_name: str = Field(..., description="Name of competitor product/company")
    competitor_normalized: str | None = None

    # Signal details
    signal_type: CompetitorSignalType
    context: str = Field(..., description="Text context where competitor was mentioned")
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)

    # Related entities
    source_company_id: str | None = None
    source_event_id: str | None = None
    source_association: str | None = None

    # Provenance
    provenance: list[Provenance] = Field(default_factory=list)

    # Timestamps
    detected_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# =============================================================================
# GRAPH MODELS
# =============================================================================

class GraphNode(BaseModel):
    """Node in the relationship graph."""

    id: str = Field(..., description="Unique node identifier")
    entity_type: EntityType
    name: str = Field(..., description="Display name")
    properties: dict[str, Any] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    """Edge in the relationship graph."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str = Field(..., description="Source node ID")
    target_id: str = Field(..., description="Target node ID")
    relationship_type: RelationshipType
    properties: dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    provenance: Provenance | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# =============================================================================
# OPERATIONAL MODELS
# =============================================================================

class PageClassification(BaseModel):
    """Result of page type classification."""

    url: str
    page_type: PageType
    confidence: float = Field(ge=0.0, le=1.0)
    signals: dict[str, Any] = Field(default_factory=dict, description="Classification signals")
    recommended_extractor: str | None = Field(default=None, description="Agent to handle extraction")
    classified_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class AccessVerdict(BaseModel):
    """Result of access gatekeeper check."""

    url: str
    domain: str
    is_allowed: bool = Field(..., description="Whether crawling is permitted")
    reasons: list[str] = Field(default_factory=list)

    # robots.txt
    robots_txt_exists: bool = False
    robots_txt_allows: bool = True
    crawl_delay: float | None = None

    # Authentication
    requires_auth: bool = False
    auth_type: str | None = None  # login, paywall, api_key

    # Rate limiting
    suggested_rate: float = Field(default=0.5, description="Requests per second")
    daily_limit: int | None = None

    # ToS
    tos_reviewed: bool = False
    tos_allows_crawling: bool | None = None

    checked_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SourceBaseline(BaseModel):
    """Baseline snapshot for DOM drift detection."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    url: str
    domain: str

    # DOM structure
    selector_hashes: dict[str, str] = Field(default_factory=dict)
    page_structure_hash: str = Field(..., description="Hash of overall page structure")

    # Content indicators
    expected_item_count: int | None = None
    content_hash: str | None = None

    # Status
    is_active: bool = True
    last_checked_at: datetime | None = None
    last_changed_at: datetime | None = None
    change_count: int = Field(default=0)

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# =============================================================================
# TARGET COMPETITORS
# =============================================================================

TARGET_COMPETITORS = [
    "Epicor",
    "Odoo",
    "Infor",
    "IQMS",
    "JobBOSS",
    "SYSPRO",
    "Acumatica",
    "Microsoft Dynamics",
    "Dynamics 365",
    "Aptean",
    "Fulcrum",
    "SAP",
    "Oracle",
    "NetSuite",
    "Plex",
    "SAGE",
    "QAD",
    "Global Shop Solutions",
]

# Normalized competitor names for matching
COMPETITOR_ALIASES = {
    "epicor": ["epicor", "epicor erp", "epicor prophet 21", "epicor kinetic"],
    "odoo": ["odoo", "odoo erp", "odoo manufacturing"],
    "infor": ["infor", "infor cloudsuite", "infor syteline", "infor m3", "infor ln"],
    "iqms": ["iqms", "iqms erp", "delmiaworks"],
    "jobboss": ["jobboss", "job boss", "jobboss erp"],
    "syspro": ["syspro", "syspro erp"],
    "acumatica": ["acumatica", "acumatica erp", "acumatica cloud erp"],
    "microsoft dynamics": ["dynamics", "dynamics 365", "microsoft dynamics", "d365", "nav", "navision", "ax", "axapta"],
    "aptean": ["aptean", "aptean industrial", "aptean erp"],
    "fulcrum": ["fulcrum", "fulcrum erp"],
    "sap": ["sap", "sap erp", "sap business one", "sap s/4hana", "sap b1"],
    "oracle": ["oracle", "oracle erp", "jd edwards", "jde"],
    "netsuite": ["netsuite", "oracle netsuite"],
    "plex": ["plex", "plex systems", "plex manufacturing cloud"],
    "sage": ["sage", "sage 100", "sage 300", "sage x3", "sage intacct"],
    "qad": ["qad", "qad erp"],
    "global shop solutions": ["global shop", "global shop solutions"],
}
