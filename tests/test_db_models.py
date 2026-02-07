"""
Database ORM Model Tests
NAM Intelligence Pipeline

Tests all SQLAlchemy ORM models using SQLite in-memory for speed.
No PostgreSQL required.
"""

import uuid
from datetime import datetime, UTC

import pytest
import pytest_asyncio
from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.models import (
    Base,
    AssociationMembershipModel,
    CompanyModel,
    CompetitorSignalModel,
    ContactModel,
    DuplicateGroupModel,
    EntityRelationshipModel,
    EventModel,
    EventParticipantModel,
    ExtractionJobModel,
    QualityAuditLogModel,
    SourceBaselineModel,
    URLQueueModel,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine):
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as sess:
        yield sess


# ---------------------------------------------------------------------------
# Schema / Table Registration
# ---------------------------------------------------------------------------

class TestTableRegistration:
    """Verify all 12 tables register correctly."""

    def test_all_tables_registered(self):
        tables = set(Base.metadata.tables.keys())
        expected = {
            "companies",
            "association_memberships",
            "contacts",
            "extraction_jobs",
            "quality_audit_log",
            "url_queue",
            "duplicate_groups",
            "events",
            "event_participants",
            "competitor_signals",
            "entity_relationships",
            "source_baselines",
        }
        assert expected == tables

    def test_table_count(self):
        assert len(Base.metadata.tables) == 12

    @pytest.mark.asyncio
    async def test_create_all_succeeds(self, engine):
        """Verify all tables actually exist in the database."""
        async with engine.connect() as conn:
            table_names = await conn.run_sync(
                lambda c: inspect(c).get_table_names()
            )
        assert len(table_names) == 12


# ---------------------------------------------------------------------------
# CompanyModel
# ---------------------------------------------------------------------------

class TestCompanyModel:

    @pytest.mark.asyncio
    async def test_create_minimal_company(self, session):
        company = CompanyModel(
            canonical_name="Acme Manufacturing",
            normalized_name="acme manufacturing",
        )
        session.add(company)
        await session.commit()

        result = await session.get(CompanyModel, company.id)
        assert result is not None
        assert result.canonical_name == "Acme Manufacturing"
        assert result.country == "United States"  # default

    @pytest.mark.asyncio
    async def test_create_full_company(self, session):
        cid = str(uuid.uuid4())
        company = CompanyModel(
            id=cid,
            canonical_name="Beta Industries LLC",
            normalized_name="beta industries",
            domain="beta-ind.com",
            website="https://beta-ind.com",
            city="Chicago",
            state="IL",
            country="United States",
            employee_count_min=100,
            employee_count_max=500,
            revenue_min_usd=10_000_000,
            revenue_max_usd=50_000_000,
            year_founded=1990,
            naics_code="332710",
            industry="Manufacturing",
            erp_system="SAP",
            crm_system="Salesforce",
            tech_stack=["SAP", "AWS"],
            quality_score=85,
            quality_grade="A",
            data_sources=["PMA"],
        )
        session.add(company)
        await session.commit()

        result = await session.get(CompanyModel, cid)
        assert result.domain == "beta-ind.com"
        assert result.quality_score == 85
        assert result.tech_stack == ["SAP", "AWS"]

    @pytest.mark.asyncio
    async def test_uuid_auto_generated(self, session):
        company = CompanyModel(
            canonical_name="Test",
            normalized_name="test",
        )
        session.add(company)
        await session.commit()
        assert company.id is not None
        assert len(company.id) == 36  # UUID format

    @pytest.mark.asyncio
    async def test_timestamp_defaults(self, session):
        company = CompanyModel(
            canonical_name="Timestamped",
            normalized_name="timestamped",
        )
        session.add(company)
        await session.commit()
        assert company.created_at is not None
        assert company.updated_at is not None

    @pytest.mark.asyncio
    async def test_domain_unique_constraint(self, session):
        c1 = CompanyModel(
            canonical_name="First",
            normalized_name="first",
            domain="unique.com",
        )
        c2 = CompanyModel(
            canonical_name="Second",
            normalized_name="second",
            domain="unique.com",
        )
        session.add(c1)
        await session.flush()
        session.add(c2)
        with pytest.raises(Exception):  # IntegrityError
            await session.flush()

    @pytest.mark.asyncio
    async def test_null_domain_allowed(self, session):
        c1 = CompanyModel(canonical_name="NoDomain1", normalized_name="nodomain1")
        c2 = CompanyModel(canonical_name="NoDomain2", normalized_name="nodomain2")
        session.add_all([c1, c2])
        await session.commit()
        assert c1.domain is None
        assert c2.domain is None

    @pytest.mark.asyncio
    async def test_json_fields(self, session):
        company = CompanyModel(
            canonical_name="JsonTest",
            normalized_name="jsontest",
            tech_stack=["React", "Node", "PostgreSQL"],
            data_sources=["PMA", "NEMA"],
        )
        session.add(company)
        await session.commit()

        result = await session.get(CompanyModel, company.id)
        assert "React" in result.tech_stack
        assert len(result.data_sources) == 2


# ---------------------------------------------------------------------------
# ContactModel
# ---------------------------------------------------------------------------

class TestContactModel:

    @pytest.mark.asyncio
    async def test_create_contact(self, session):
        company = CompanyModel(
            canonical_name="Parent Co",
            normalized_name="parent co",
        )
        session.add(company)
        await session.flush()

        contact = ContactModel(
            company_id=company.id,
            full_name="John Smith",
            title="CTO",
            email="john@parent.com",
        )
        session.add(contact)
        await session.commit()

        result = await session.get(ContactModel, contact.id)
        assert result.full_name == "John Smith"
        assert result.company_id == company.id

    @pytest.mark.asyncio
    async def test_contact_without_company(self, session):
        contact = ContactModel(
            full_name="Orphan Contact",
            email="orphan@test.com",
        )
        session.add(contact)
        await session.commit()
        assert contact.company_id is None

    @pytest.mark.asyncio
    async def test_contact_email_defaults(self, session):
        contact = ContactModel(
            full_name="No Email",
        )
        session.add(contact)
        await session.commit()
        assert contact.email_verified is False

    @pytest.mark.asyncio
    async def test_company_contact_relationship(self, session):
        company = CompanyModel(
            canonical_name="RelTest",
            normalized_name="reltest",
        )
        session.add(company)
        await session.flush()

        c1 = ContactModel(company_id=company.id, full_name="Alice")
        c2 = ContactModel(company_id=company.id, full_name="Bob")
        session.add_all([c1, c2])
        await session.commit()

        # Query via relationship
        await session.refresh(company, ["contacts"])
        assert len(company.contacts) == 2


# ---------------------------------------------------------------------------
# AssociationMembershipModel
# ---------------------------------------------------------------------------

class TestAssociationMembershipModel:

    @pytest.mark.asyncio
    async def test_create_membership(self, session):
        company = CompanyModel(
            canonical_name="Member Co",
            normalized_name="member co",
        )
        session.add(company)
        await session.flush()

        membership = AssociationMembershipModel(
            company_id=company.id,
            association_code="PMA",
            association_name="Precision Metalforming Association",
            membership_tier="Gold",
        )
        session.add(membership)
        await session.commit()

        assert membership.membership_status == "active"  # default

    @pytest.mark.asyncio
    async def test_membership_unique_constraint(self, session):
        company = CompanyModel(
            canonical_name="DupeMember",
            normalized_name="dupemember",
        )
        session.add(company)
        await session.flush()

        m1 = AssociationMembershipModel(
            company_id=company.id,
            association_code="PMA",
        )
        m2 = AssociationMembershipModel(
            company_id=company.id,
            association_code="PMA",
        )
        session.add(m1)
        await session.flush()
        session.add(m2)
        with pytest.raises(Exception):
            await session.flush()


# ---------------------------------------------------------------------------
# ExtractionJobModel
# ---------------------------------------------------------------------------

class TestExtractionJobModel:

    @pytest.mark.asyncio
    async def test_create_job(self, session):
        job = ExtractionJobModel(
            job_type="full_extract",
            association_code="PMA",
        )
        session.add(job)
        await session.commit()

        assert job.status == "pending"
        assert job.total_items == 0
        assert job.error_count == 0

    @pytest.mark.asyncio
    async def test_update_job_progress(self, session):
        job = ExtractionJobModel(
            job_type="incremental",
            total_items=100,
        )
        session.add(job)
        await session.commit()

        job.processed_items = 50
        job.status = "running"
        await session.commit()

        result = await session.get(ExtractionJobModel, job.id)
        assert result.processed_items == 50
        assert result.status == "running"

    @pytest.mark.asyncio
    async def test_job_checkpoint_json(self, session):
        job = ExtractionJobModel(
            job_type="full_extract",
            last_checkpoint={"page": 5, "offset": 250},
        )
        session.add(job)
        await session.commit()

        result = await session.get(ExtractionJobModel, job.id)
        assert result.last_checkpoint["page"] == 5


# ---------------------------------------------------------------------------
# QualityAuditLogModel
# ---------------------------------------------------------------------------

class TestQualityAuditLogModel:

    @pytest.mark.asyncio
    async def test_create_audit_entry(self, session):
        company = CompanyModel(
            canonical_name="AuditCo",
            normalized_name="auditco",
        )
        session.add(company)
        await session.flush()

        audit = QualityAuditLogModel(
            company_id=company.id,
            field_name="erp_system",
            old_value=None,
            new_value="SAP",
            validation_result="passed",
            validator_name="crossref",
        )
        session.add(audit)
        await session.commit()

        assert audit.field_name == "erp_system"
        assert audit.validation_result == "passed"


# ---------------------------------------------------------------------------
# EventModel
# ---------------------------------------------------------------------------

class TestEventModel:

    @pytest.mark.asyncio
    async def test_create_event(self, session):
        event = EventModel(
            title="FABTECH 2024",
            event_type="TRADE_SHOW",
            city="Orlando",
            state="FL",
        )
        session.add(event)
        await session.commit()

        assert event.country == "United States"
        assert event.is_virtual is False

    @pytest.mark.asyncio
    async def test_event_participants_relationship(self, session):
        event = EventModel(title="TestEvent", event_type="CONFERENCE")
        session.add(event)
        await session.flush()

        p1 = EventParticipantModel(
            event_id=event.id,
            participant_type="SPONSOR",
            company_name="Sponsor Co",
            sponsor_tier="GOLD",
        )
        p2 = EventParticipantModel(
            event_id=event.id,
            participant_type="EXHIBITOR",
            company_name="Exhibitor Co",
            booth_number="101",
        )
        session.add_all([p1, p2])
        await session.commit()

        await session.refresh(event, ["participants"])
        assert len(event.participants) == 2


# ---------------------------------------------------------------------------
# CompetitorSignalModel
# ---------------------------------------------------------------------------

class TestCompetitorSignalModel:

    @pytest.mark.asyncio
    async def test_create_signal(self, session):
        signal = CompetitorSignalModel(
            competitor_name="Epicor",
            competitor_normalized="epicor",
            signal_type="MEMBER_USAGE",
            context="Company uses Epicor ERP",
            confidence=0.90,
        )
        session.add(signal)
        await session.commit()

        assert signal.confidence == 0.90


# ---------------------------------------------------------------------------
# EntityRelationshipModel
# ---------------------------------------------------------------------------

class TestEntityRelationshipModel:

    @pytest.mark.asyncio
    async def test_create_relationship(self, session):
        rel = EntityRelationshipModel(
            source_id=str(uuid.uuid4()),
            source_type="Association",
            target_id=str(uuid.uuid4()),
            target_type="Company",
            relationship_type="ASSOCIATION_HAS_MEMBER",
        )
        session.add(rel)
        await session.commit()

        assert rel.confidence == 1.0  # default

    @pytest.mark.asyncio
    async def test_unique_relationship_constraint(self, session):
        sid = str(uuid.uuid4())
        tid = str(uuid.uuid4())
        r1 = EntityRelationshipModel(
            source_id=sid,
            source_type="Company",
            target_id=tid,
            target_type="Event",
            relationship_type="EVENT_HAS_SPONSOR",
        )
        r2 = EntityRelationshipModel(
            source_id=sid,
            source_type="Company",
            target_id=tid,
            target_type="Event",
            relationship_type="EVENT_HAS_SPONSOR",
        )
        session.add(r1)
        await session.flush()
        session.add(r2)
        with pytest.raises(Exception):
            await session.flush()


# ---------------------------------------------------------------------------
# SourceBaselineModel
# ---------------------------------------------------------------------------

class TestSourceBaselineModel:

    @pytest.mark.asyncio
    async def test_create_baseline(self, session):
        baseline = SourceBaselineModel(
            url="https://pma.org/members",
            url_hash="abc123",
            domain="pma.org",
            is_active=True,
            selector_hashes={"h1": "hash1", "ul": "hash2"},
        )
        session.add(baseline)
        await session.commit()

        result = await session.get(SourceBaselineModel, baseline.id)
        assert result.selector_hashes["h1"] == "hash1"
        assert result.is_active is True


# ---------------------------------------------------------------------------
# URLQueueModel
# ---------------------------------------------------------------------------

class TestURLQueueModel:

    @pytest.mark.asyncio
    async def test_create_url_queue_entry(self, session):
        job = ExtractionJobModel(job_type="extract")
        session.add(job)
        await session.flush()

        entry = URLQueueModel(
            job_id=job.id,
            url="https://pma.org/members?page=1",
            url_hash="deadbeef",
            association_code="PMA",
            priority=10,
        )
        session.add(entry)
        await session.commit()

        assert entry.status == "pending"
        assert entry.attempts == 0

    @pytest.mark.asyncio
    async def test_url_queue_job_relationship(self, session):
        job = ExtractionJobModel(job_type="extract")
        session.add(job)
        await session.flush()

        u1 = URLQueueModel(job_id=job.id, url="https://a.com", url_hash="h1")
        u2 = URLQueueModel(job_id=job.id, url="https://b.com", url_hash="h2")
        session.add_all([u1, u2])
        await session.commit()

        await session.refresh(job, ["url_queue"])
        assert len(job.url_queue) == 2


# ---------------------------------------------------------------------------
# DuplicateGroupModel
# ---------------------------------------------------------------------------

class TestDuplicateGroupModel:

    @pytest.mark.asyncio
    async def test_create_duplicate_group(self, session):
        c1 = CompanyModel(canonical_name="Dup1", normalized_name="dup1")
        c2 = CompanyModel(canonical_name="Dup2", normalized_name="dup2")
        session.add_all([c1, c2])
        await session.flush()

        group = DuplicateGroupModel(
            canonical_company_id=c1.id,
            member_company_ids=[c1.id, c2.id],
            match_score=0.95,
            match_method="fuzzy_name",
        )
        session.add(group)
        await session.commit()

        result = await session.get(DuplicateGroupModel, group.id)
        assert len(result.member_company_ids) == 2
        assert result.status == "merged"


# ---------------------------------------------------------------------------
# Cascade Delete Tests
# ---------------------------------------------------------------------------

class TestCascadeDeletes:

    @pytest.mark.asyncio
    async def test_delete_company_cascades_contacts(self, session):
        company = CompanyModel(
            canonical_name="Cascade",
            normalized_name="cascade",
        )
        session.add(company)
        await session.flush()

        contact = ContactModel(company_id=company.id, full_name="Gone")
        session.add(contact)
        await session.commit()

        await session.delete(company)
        await session.commit()

        result = await session.get(ContactModel, contact.id)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_company_cascades_memberships(self, session):
        company = CompanyModel(
            canonical_name="CascadeMember",
            normalized_name="cascademember",
        )
        session.add(company)
        await session.flush()

        membership = AssociationMembershipModel(
            company_id=company.id,
            association_code="PMA",
        )
        session.add(membership)
        await session.commit()

        await session.delete(company)
        await session.commit()

        result = await session.get(AssociationMembershipModel, membership.id)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_event_cascades_participants(self, session):
        event = EventModel(title="CascadeEvent", event_type="OTHER")
        session.add(event)
        await session.flush()

        participant = EventParticipantModel(
            event_id=event.id,
            participant_type="SPONSOR",
            company_name="Deleted",
        )
        session.add(participant)
        await session.commit()

        await session.delete(event)
        await session.commit()

        result = await session.get(EventParticipantModel, participant.id)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_job_cascades_url_queue(self, session):
        job = ExtractionJobModel(job_type="extract")
        session.add(job)
        await session.flush()

        url = URLQueueModel(job_id=job.id, url="https://x.com", url_hash="x")
        session.add(url)
        await session.commit()

        await session.delete(job)
        await session.commit()

        result = await session.get(URLQueueModel, url.id)
        assert result is None
