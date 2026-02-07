"""
SQLAlchemy ORM Models
NAM Intelligence Pipeline

Maps all database tables to SQLAlchemy 2.0 declarative models.
Uses UUID primary keys, JSONB-compatible columns, and relationship definitions.

For SQLite test compatibility, JSON type is used (auto-adapts to JSONB on PostgreSQL).
"""

import uuid
from datetime import datetime, UTC
from typing import Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON


def _uuid() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""
    pass


# ---------------------------------------------------------------------------
# Companies
# ---------------------------------------------------------------------------

class CompanyModel(Base):
    __tablename__ = "companies"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Core identification
    canonical_name: Mapped[str] = mapped_column(String(500), nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(500), nullable=False)
    domain: Mapped[Optional[str]] = mapped_column(String(255), unique=True, nullable=True)
    website: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Location
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    country: Mapped[str] = mapped_column(String(100), default="United States")
    full_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Firmographics
    employee_count_min: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    employee_count_max: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    revenue_min_usd: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    revenue_max_usd: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    year_founded: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    naics_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    sic_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    industry: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    # Technology
    erp_system: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    crm_system: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    tech_stack: Mapped[Optional[dict]] = mapped_column(JSON, default=list)

    # Metadata
    quality_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    quality_grade: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    data_sources: Mapped[Optional[dict]] = mapped_column(JSON, default=list)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)
    last_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    memberships = relationship("AssociationMembershipModel", back_populates="company", cascade="all, delete-orphan")
    contacts = relationship("ContactModel", back_populates="company", cascade="all, delete-orphan")
    audit_logs = relationship("QualityAuditLogModel", back_populates="company", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint("quality_score >= 0 AND quality_score <= 100", name="ck_quality_score_range"),
        Index("idx_companies_normalized_name", "normalized_name"),
        Index("idx_companies_domain", "domain"),
        Index("idx_companies_location", "state", "city"),
        Index("idx_companies_quality", quality_score.desc()),
        Index("idx_companies_erp", "erp_system"),
        Index("idx_companies_industry", "industry"),
    )


# ---------------------------------------------------------------------------
# Association Memberships
# ---------------------------------------------------------------------------

class AssociationMembershipModel(Base):
    __tablename__ = "association_memberships"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    company_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("companies.id", ondelete="CASCADE"), nullable=True
    )

    # Association info
    association_code: Mapped[str] = mapped_column(String(20), nullable=False)
    association_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    # Membership details
    membership_tier: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    membership_status: Mapped[str] = mapped_column(String(20), default="active")
    member_since: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Source
    profile_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Timestamps
    extracted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    # Relationships
    company = relationship("CompanyModel", back_populates="memberships")

    __table_args__ = (
        UniqueConstraint("company_id", "association_code", name="uq_membership"),
        Index("idx_memberships_association", "association_code"),
        Index("idx_memberships_company", "company_id"),
    )


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------

class ContactModel(Base):
    __tablename__ = "contacts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    company_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("companies.id", ondelete="CASCADE"), nullable=True
    )

    # Contact info
    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    first_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    department: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    seniority: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Communication
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    email_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    email_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    linkedin_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Metadata
    data_source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    confidence_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    # Relationships
    company = relationship("CompanyModel", back_populates="contacts")

    __table_args__ = (
        UniqueConstraint("company_id", "email", name="uq_contact_email"),
        Index("idx_contacts_company", "company_id"),
        Index("idx_contacts_email", "email"),
    )


# ---------------------------------------------------------------------------
# Extraction Jobs
# ---------------------------------------------------------------------------

class ExtractionJobModel(Base):
    __tablename__ = "extraction_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Job info
    job_type: Mapped[str] = mapped_column(String(50), nullable=False)
    association_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Status
    status: Mapped[str] = mapped_column(String(20), default="pending")

    # Progress
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    processed_items: Mapped[int] = mapped_column(Integer, default=0)
    created_items: Mapped[int] = mapped_column(Integer, default=0)
    updated_items: Mapped[int] = mapped_column(Integer, default=0)
    failed_items: Mapped[int] = mapped_column(Integer, default=0)
    skipped_items: Mapped[int] = mapped_column(Integer, default=0)

    # Timing
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Checkpoints
    last_checkpoint: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    checkpoint_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Errors
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    error_log: Mapped[Optional[dict]] = mapped_column(JSON, default=list)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    # Relationships
    audit_logs = relationship("QualityAuditLogModel", back_populates="job")
    url_queue = relationship("URLQueueModel", back_populates="job", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_jobs_status", "status"),
        Index("idx_jobs_association", "association_code"),
        Index("idx_jobs_created", created_at.desc()),
    )


# ---------------------------------------------------------------------------
# Quality Audit Log
# ---------------------------------------------------------------------------

class QualityAuditLogModel(Base):
    __tablename__ = "quality_audit_log"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    company_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("companies.id", ondelete="CASCADE"), nullable=True
    )
    job_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("extraction_jobs.id", ondelete="SET NULL"), nullable=True
    )

    # Change details
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    old_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Validation
    validation_result: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    validator_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    confidence_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Metadata
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    # Relationships
    company = relationship("CompanyModel", back_populates="audit_logs")
    job = relationship("ExtractionJobModel", back_populates="audit_logs")

    __table_args__ = (
        Index("idx_audit_company", "company_id"),
        Index("idx_audit_job", "job_id"),
        Index("idx_audit_created", created_at.desc()),
    )


# ---------------------------------------------------------------------------
# URL Queue
# ---------------------------------------------------------------------------

class URLQueueModel(Base):
    __tablename__ = "url_queue"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("extraction_jobs.id", ondelete="CASCADE"), nullable=True
    )

    # URL info
    url: Mapped[str] = mapped_column(String(2000), nullable=False)
    url_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    association_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Status
    status: Mapped[str] = mapped_column(String(20), default="pending")
    priority: Mapped[int] = mapped_column(Integer, default=0)

    # Metadata
    source_url: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    depth: Mapped[int] = mapped_column(Integer, default=0)

    # Processing
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    # Relationships
    job = relationship("ExtractionJobModel", back_populates="url_queue")

    __table_args__ = (
        UniqueConstraint("job_id", "url_hash", name="uq_url_per_job"),
        Index("idx_queue_job_status", "job_id", "status"),
        Index("idx_queue_priority", priority.desc(), created_at.asc()),
    )


# ---------------------------------------------------------------------------
# Duplicate Groups
# ---------------------------------------------------------------------------

class DuplicateGroupModel(Base):
    __tablename__ = "duplicate_groups"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Canonical record
    canonical_company_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )

    # Group members stored as JSON array of UUIDs (SQLite compatible)
    member_company_ids: Mapped[dict] = mapped_column(JSON, nullable=False)

    # Match details
    match_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    match_method: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Status
    status: Mapped[str] = mapped_column(String(20), default="merged")
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    __table_args__ = (
        Index("idx_duplicates_canonical", "canonical_company_id"),
    )


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

class EventModel(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Core identification
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), default="OTHER")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Dates
    start_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    end_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    registration_deadline: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Location
    venue: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    country: Mapped[str] = mapped_column(String(100), default="United States")
    is_virtual: Mapped[bool] = mapped_column(Boolean, default=False)

    # URLs
    event_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    registration_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Organizer
    organizer_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    organizer_association: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Participant counts
    expected_attendees: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    exhibitor_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sponsor_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Provenance
    source_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    extracted_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    # Relationships
    participants = relationship("EventParticipantModel", back_populates="event", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_events_dates", "start_date", "end_date"),
        Index("idx_events_type", "event_type"),
        Index("idx_events_association", "organizer_association"),
        Index("idx_events_location", "state", "city"),
    )


# ---------------------------------------------------------------------------
# Event Participants
# ---------------------------------------------------------------------------

class EventParticipantModel(Base):
    __tablename__ = "event_participants"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Links
    event_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("events.id", ondelete="CASCADE"), nullable=True
    )
    company_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )

    # Participant info
    participant_type: Mapped[str] = mapped_column(String(20), nullable=False)
    company_name: Mapped[str] = mapped_column(String(500), nullable=False)
    company_website: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Sponsor-specific
    sponsor_tier: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Exhibitor-specific
    booth_number: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    booth_category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Speaker-specific
    speaker_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    speaker_title: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    presentation_title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Provenance
    source_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    extracted_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    # Relationships
    event = relationship("EventModel", back_populates="participants")

    __table_args__ = (
        Index("idx_participants_event", "event_id"),
        Index("idx_participants_company", "company_id"),
        Index("idx_participants_type", "participant_type"),
        Index("idx_participants_tier", "sponsor_tier"),
    )


# ---------------------------------------------------------------------------
# Competitor Signals
# ---------------------------------------------------------------------------

class CompetitorSignalModel(Base):
    __tablename__ = "competitor_signals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Competitor identification
    competitor_name: Mapped[str] = mapped_column(String(100), nullable=False)
    competitor_normalized: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Signal details
    signal_type: Mapped[str] = mapped_column(String(50), nullable=False)
    context: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.80)

    # Related entities
    source_company_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    source_event_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("events.id", ondelete="SET NULL"), nullable=True
    )
    source_association: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Provenance
    source_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    extracted_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Timestamps
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    __table_args__ = (
        Index("idx_signals_competitor", "competitor_normalized"),
        Index("idx_signals_type", "signal_type"),
        Index("idx_signals_company", "source_company_id"),
        Index("idx_signals_event", "source_event_id"),
        Index("idx_signals_detected", detected_at.desc()),
    )


# ---------------------------------------------------------------------------
# Entity Relationships
# ---------------------------------------------------------------------------

class EntityRelationshipModel(Base):
    __tablename__ = "entity_relationships"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Relationship
    source_id: Mapped[str] = mapped_column(String(36), nullable=False)
    source_type: Mapped[str] = mapped_column(String(20), nullable=False)
    target_id: Mapped[str] = mapped_column(String(36), nullable=False)
    target_type: Mapped[str] = mapped_column(String(20), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(50), nullable=False)

    # Metadata
    properties: Mapped[Optional[dict]] = mapped_column(JSON, default=dict)
    confidence: Mapped[float] = mapped_column(Float, default=1.00)

    # Provenance
    source_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    extracted_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    __table_args__ = (
        UniqueConstraint("source_id", "target_id", "relationship_type", name="uq_relationship"),
        Index("idx_relationships_source", "source_id", "source_type"),
        Index("idx_relationships_target", "target_id", "target_type"),
        Index("idx_relationships_type", "relationship_type"),
    )


# ---------------------------------------------------------------------------
# Source Baselines
# ---------------------------------------------------------------------------

class SourceBaselineModel(Base):
    __tablename__ = "source_baselines"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # Source identification
    url: Mapped[str] = mapped_column(String(2000), nullable=False)
    url_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    domain: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # DOM structure
    selector_hashes: Mapped[Optional[dict]] = mapped_column(JSON, default=dict)
    page_structure_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Content indicators
    expected_item_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_changed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    change_count: Mapped[int] = mapped_column(Integer, default=0)

    # Alert configuration
    alert_on_change: Mapped[bool] = mapped_column(Boolean, default=True)
    alert_threshold: Mapped[int] = mapped_column(Integer, default=1)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        Index("idx_baselines_domain", "domain"),
        Index("idx_baselines_active", "is_active"),
    )
