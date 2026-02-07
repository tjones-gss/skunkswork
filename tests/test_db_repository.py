"""
Repository Layer Tests
NAM Intelligence Pipeline

Tests all repository CRUD operations using SQLite in-memory.
"""

import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.models import (
    Base,
    CompanyModel,
    ContactModel,
    CompetitorSignalModel,
    EventModel,
    EventParticipantModel,
    ExtractionJobModel,
    QualityAuditLogModel,
)
from db.repository import (
    AuditRepository,
    CompanyRepository,
    CompetitorSignalRepository,
    ContactRepository,
    EventParticipantRepository,
    EventRepository,
    ExtractionJobRepository,
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


async def _make_company(session, name="Test Co", domain=None, **kwargs):
    """Helper to create a company record quickly."""
    company = CompanyModel(
        canonical_name=name,
        normalized_name=name.lower(),
        domain=domain,
        **kwargs,
    )
    session.add(company)
    await session.flush()
    return company


# ---------------------------------------------------------------------------
# CompanyRepository
# ---------------------------------------------------------------------------

class TestCompanyRepository:

    @pytest.mark.asyncio
    async def test_get(self, session):
        company = await _make_company(session, "GetCo")
        result = await CompanyRepository.get(session, company.id)
        assert result is not None
        assert result.canonical_name == "GetCo"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, session):
        result = await CompanyRepository.get(session, str(uuid.uuid4()))
        assert result is None

    @pytest.mark.asyncio
    async def test_find_by_domain(self, session):
        await _make_company(session, "DomainCo", domain="domain.com")
        result = await CompanyRepository.find_by_domain(session, "domain.com")
        assert result is not None
        assert result.canonical_name == "DomainCo"

    @pytest.mark.asyncio
    async def test_find_by_domain_nonexistent(self, session):
        result = await CompanyRepository.find_by_domain(session, "nope.com")
        assert result is None

    @pytest.mark.asyncio
    async def test_find_by_name(self, session):
        await _make_company(session, "NameCo")
        result = await CompanyRepository.find_by_name(session, "NameCo")
        assert result is not None

    @pytest.mark.asyncio
    async def test_count(self, session):
        assert await CompanyRepository.count(session) == 0
        await _make_company(session, "One")
        await _make_company(session, "Two")
        assert await CompanyRepository.count(session) == 2

    @pytest.mark.asyncio
    async def test_upsert_insert(self, session):
        result = await CompanyRepository.upsert(session, {
            "canonical_name": "New Co",
            "normalized_name": "new co",
            "domain": "new.com",
        })
        await session.flush()
        assert result.canonical_name == "New Co"

    @pytest.mark.asyncio
    async def test_upsert_update_by_domain(self, session):
        await _make_company(session, "Original", domain="up.com", city="Detroit")

        result = await CompanyRepository.upsert(session, {
            "canonical_name": "Updated",
            "normalized_name": "updated",
            "domain": "up.com",
            "city": "Chicago",
        })
        await session.flush()
        assert result.city == "Chicago"
        assert result.canonical_name == "Updated"

    @pytest.mark.asyncio
    async def test_upsert_skip_none_values(self, session):
        await _make_company(session, "KeepCity", domain="keep.com", city="Detroit")

        result = await CompanyRepository.upsert(session, {
            "domain": "keep.com",
            "city": None,  # should NOT overwrite
        })
        await session.flush()
        assert result.city == "Detroit"

    @pytest.mark.asyncio
    async def test_bulk_upsert(self, session):
        records = [
            {"canonical_name": "A", "normalized_name": "a", "domain": "a.com"},
            {"canonical_name": "B", "normalized_name": "b", "domain": "b.com"},
            {"canonical_name": "C", "normalized_name": "c", "domain": "c.com"},
        ]
        count = await CompanyRepository.bulk_upsert(session, records)
        await session.flush()
        assert count == 3
        assert await CompanyRepository.count(session) == 3

    @pytest.mark.asyncio
    async def test_bulk_upsert_idempotent(self, session):
        records = [
            {"canonical_name": "Dup", "normalized_name": "dup", "domain": "dup.com"},
        ]
        await CompanyRepository.bulk_upsert(session, records)
        await session.flush()
        # Upsert same record again
        await CompanyRepository.bulk_upsert(session, records)
        await session.flush()
        assert await CompanyRepository.count(session) == 1

    @pytest.mark.asyncio
    async def test_delete(self, session):
        company = await _make_company(session, "DeleteMe")
        assert await CompanyRepository.delete(session, company.id) is True
        await session.flush()
        assert await CompanyRepository.get(session, company.id) is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, session):
        assert await CompanyRepository.delete(session, str(uuid.uuid4())) is False

    @pytest.mark.asyncio
    async def test_search_by_state(self, session):
        await _make_company(session, "MI Co", domain="mi.com", state="MI")
        await _make_company(session, "OH Co", domain="oh.com", state="OH")

        results = await CompanyRepository.search(session, state="MI")
        assert len(results) == 1
        assert results[0].state == "MI"

    @pytest.mark.asyncio
    async def test_search_by_erp(self, session):
        await _make_company(session, "SAP Co", domain="sap.com", erp_system="SAP")
        await _make_company(session, "Epicor Co", domain="epicor.com", erp_system="Epicor")

        results = await CompanyRepository.search(session, erp_system="SAP")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_search_by_min_quality(self, session):
        await _make_company(session, "High", domain="high.com", quality_score=90)
        await _make_company(session, "Low", domain="low.com", quality_score=40)

        results = await CompanyRepository.search(session, min_quality=80)
        assert len(results) == 1
        assert results[0].canonical_name == "High"

    @pytest.mark.asyncio
    async def test_search_with_limit(self, session):
        for i in range(10):
            await _make_company(session, f"Co{i}", domain=f"co{i}.com")

        results = await CompanyRepository.search(session, limit=3)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_search_with_offset(self, session):
        for i in range(5):
            await _make_company(session, f"Co{i:02d}", domain=f"co{i}.com")

        all_results = await CompanyRepository.search(session, limit=100)
        offset_results = await CompanyRepository.search(session, limit=100, offset=3)
        assert len(offset_results) == len(all_results) - 3

    @pytest.mark.asyncio
    async def test_search_combined_filters(self, session):
        await _make_company(
            session, "Match", domain="match.com",
            state="MI", erp_system="SAP", quality_score=90,
        )
        await _make_company(
            session, "NoMatch", domain="nomatch.com",
            state="MI", erp_system="Epicor", quality_score=90,
        )
        results = await CompanyRepository.search(
            session, state="MI", erp_system="SAP", min_quality=80,
        )
        assert len(results) == 1
        assert results[0].canonical_name == "Match"


# ---------------------------------------------------------------------------
# ContactRepository
# ---------------------------------------------------------------------------

class TestContactRepository:

    @pytest.mark.asyncio
    async def test_upsert_insert(self, session):
        company = await _make_company(session, "Parent")
        contact = await ContactRepository.upsert(session, {
            "company_id": company.id,
            "full_name": "Alice",
            "email": "alice@parent.com",
        })
        await session.flush()
        assert contact.full_name == "Alice"

    @pytest.mark.asyncio
    async def test_upsert_update_by_email(self, session):
        company = await _make_company(session, "Parent")
        await ContactRepository.upsert(session, {
            "company_id": company.id,
            "full_name": "Alice",
            "email": "alice@parent.com",
            "title": "Engineer",
        })
        await session.flush()

        # Upsert same company+email with updated title
        result = await ContactRepository.upsert(session, {
            "company_id": company.id,
            "email": "alice@parent.com",
            "title": "CTO",
        })
        await session.flush()
        assert result.title == "CTO"

    @pytest.mark.asyncio
    async def test_find_by_company(self, session):
        company = await _make_company(session, "MultiContact")
        for name in ["A", "B", "C"]:
            session.add(ContactModel(
                company_id=company.id,
                full_name=name,
            ))
        await session.flush()

        contacts = await ContactRepository.find_by_company(session, company.id)
        assert len(contacts) == 3

    @pytest.mark.asyncio
    async def test_find_by_email(self, session):
        session.add(ContactModel(full_name="Test", email="test@x.com"))
        await session.flush()

        result = await ContactRepository.find_by_email(session, "test@x.com")
        assert result is not None

    @pytest.mark.asyncio
    async def test_find_by_email_nonexistent(self, session):
        result = await ContactRepository.find_by_email(session, "nope@x.com")
        assert result is None

    @pytest.mark.asyncio
    async def test_bulk_upsert(self, session):
        company = await _make_company(session, "BulkContacts")
        records = [
            {"company_id": company.id, "full_name": "A", "email": "a@x.com"},
            {"company_id": company.id, "full_name": "B", "email": "b@x.com"},
        ]
        count = await ContactRepository.bulk_upsert(session, records)
        assert count == 2


# ---------------------------------------------------------------------------
# ExtractionJobRepository
# ---------------------------------------------------------------------------

class TestExtractionJobRepository:

    @pytest.mark.asyncio
    async def test_create_job(self, session):
        job = await ExtractionJobRepository.create(session, {
            "job_type": "full_extract",
            "association_code": "PMA",
        })
        await session.flush()
        assert job.status == "pending"

    @pytest.mark.asyncio
    async def test_get_job(self, session):
        job = await ExtractionJobRepository.create(session, {
            "job_type": "incremental",
        })
        await session.flush()

        result = await ExtractionJobRepository.get(session, job.id)
        assert result.job_type == "incremental"

    @pytest.mark.asyncio
    async def test_update_progress(self, session):
        job = await ExtractionJobRepository.create(session, {
            "job_type": "extract",
            "total_items": 100,
        })
        await session.flush()

        updated = await ExtractionJobRepository.update_progress(
            session, job.id,
            processed_items=50,
            created_items=45,
            failed_items=5,
        )
        assert updated.processed_items == 50
        assert updated.created_items == 45

    @pytest.mark.asyncio
    async def test_update_progress_nonexistent(self, session):
        result = await ExtractionJobRepository.update_progress(
            session, str(uuid.uuid4()), processed_items=10,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_complete_job(self, session):
        job = await ExtractionJobRepository.create(session, {
            "job_type": "extract",
        })
        await session.flush()

        completed = await ExtractionJobRepository.complete(session, job.id)
        assert completed.status == "completed"
        assert completed.completed_at is not None

    @pytest.mark.asyncio
    async def test_complete_job_with_status(self, session):
        job = await ExtractionJobRepository.create(session, {
            "job_type": "extract",
        })
        await session.flush()

        completed = await ExtractionJobRepository.complete(
            session, job.id, status="failed",
        )
        assert completed.status == "failed"

    @pytest.mark.asyncio
    async def test_list_by_status(self, session):
        for i in range(3):
            await ExtractionJobRepository.create(session, {
                "job_type": "extract",
                "status": "running",
            })
        await ExtractionJobRepository.create(session, {
            "job_type": "extract",
            "status": "completed",
        })
        await session.flush()

        running = await ExtractionJobRepository.list_by_status(session, "running")
        assert len(running) == 3

        completed = await ExtractionJobRepository.list_by_status(session, "completed")
        assert len(completed) == 1


# ---------------------------------------------------------------------------
# AuditRepository
# ---------------------------------------------------------------------------

class TestAuditRepository:

    @pytest.mark.asyncio
    async def test_log_change(self, session):
        company = await _make_company(session, "AuditCo")
        entry = await AuditRepository.log_change(session, {
            "company_id": company.id,
            "field_name": "city",
            "old_value": "Detroit",
            "new_value": "Chicago",
            "validation_result": "corrected",
        })
        await session.flush()
        assert entry.field_name == "city"

    @pytest.mark.asyncio
    async def test_get_history(self, session):
        company = await _make_company(session, "HistoryCo")
        for i in range(5):
            await AuditRepository.log_change(session, {
                "company_id": company.id,
                "field_name": f"field_{i}",
            })
        await session.flush()

        history = await AuditRepository.get_history(session, company.id)
        assert len(history) == 5

    @pytest.mark.asyncio
    async def test_get_history_limit(self, session):
        company = await _make_company(session, "LimitCo")
        for i in range(10):
            await AuditRepository.log_change(session, {
                "company_id": company.id,
                "field_name": f"field_{i}",
            })
        await session.flush()

        history = await AuditRepository.get_history(session, company.id, limit=3)
        assert len(history) == 3

    @pytest.mark.asyncio
    async def test_get_job_logs(self, session):
        job = await ExtractionJobRepository.create(session, {"job_type": "extract"})
        await session.flush()

        await AuditRepository.log_change(session, {
            "job_id": job.id,
            "field_name": "test",
        })
        await session.flush()

        logs = await AuditRepository.get_job_logs(session, job.id)
        assert len(logs) == 1


# ---------------------------------------------------------------------------
# EventRepository
# ---------------------------------------------------------------------------

class TestEventRepository:

    @pytest.mark.asyncio
    async def test_create_event(self, session):
        event = await EventRepository.create(session, {
            "title": "FABTECH",
            "event_type": "TRADE_SHOW",
        })
        await session.flush()
        assert event.title == "FABTECH"

    @pytest.mark.asyncio
    async def test_get_event(self, session):
        event = await EventRepository.create(session, {
            "title": "GetEvent",
            "event_type": "CONFERENCE",
        })
        await session.flush()

        result = await EventRepository.get(session, event.id)
        assert result.title == "GetEvent"

    @pytest.mark.asyncio
    async def test_list_by_association(self, session):
        for i in range(3):
            await EventRepository.create(session, {
                "title": f"PMA Event {i}",
                "event_type": "CONFERENCE",
                "organizer_association": "PMA",
            })
        await EventRepository.create(session, {
            "title": "NEMA Event",
            "event_type": "WEBINAR",
            "organizer_association": "NEMA",
        })
        await session.flush()

        pma_events = await EventRepository.list_by_association(session, "PMA")
        assert len(pma_events) == 3

    @pytest.mark.asyncio
    async def test_count(self, session):
        assert await EventRepository.count(session) == 0
        await EventRepository.create(session, {
            "title": "Counted",
            "event_type": "OTHER",
        })
        await session.flush()
        assert await EventRepository.count(session) == 1


# ---------------------------------------------------------------------------
# EventParticipantRepository
# ---------------------------------------------------------------------------

class TestEventParticipantRepository:

    @pytest.mark.asyncio
    async def test_create_participant(self, session):
        event = await EventRepository.create(session, {
            "title": "TestEvent",
            "event_type": "OTHER",
        })
        await session.flush()

        participant = await EventParticipantRepository.create(session, {
            "event_id": event.id,
            "participant_type": "SPONSOR",
            "company_name": "Sponsor Inc",
            "sponsor_tier": "PLATINUM",
        })
        await session.flush()
        assert participant.sponsor_tier == "PLATINUM"

    @pytest.mark.asyncio
    async def test_find_by_event(self, session):
        event = await EventRepository.create(session, {
            "title": "MultiPart",
            "event_type": "OTHER",
        })
        await session.flush()

        for i in range(4):
            await EventParticipantRepository.create(session, {
                "event_id": event.id,
                "participant_type": "EXHIBITOR",
                "company_name": f"Exhibitor {i}",
            })
        await session.flush()

        participants = await EventParticipantRepository.find_by_event(session, event.id)
        assert len(participants) == 4

    @pytest.mark.asyncio
    async def test_find_sponsors(self, session):
        event = await EventRepository.create(session, {
            "title": "SponsorEvent",
            "event_type": "OTHER",
        })
        await session.flush()

        await EventParticipantRepository.create(session, {
            "event_id": event.id,
            "participant_type": "SPONSOR",
            "company_name": "S1",
        })
        await EventParticipantRepository.create(session, {
            "event_id": event.id,
            "participant_type": "EXHIBITOR",
            "company_name": "E1",
        })
        await session.flush()

        sponsors = await EventParticipantRepository.find_sponsors(session, event.id)
        assert len(sponsors) == 1
        assert sponsors[0].company_name == "S1"


# ---------------------------------------------------------------------------
# CompetitorSignalRepository
# ---------------------------------------------------------------------------

class TestCompetitorSignalRepository:

    @pytest.mark.asyncio
    async def test_create_signal(self, session):
        signal = await CompetitorSignalRepository.create(session, {
            "competitor_name": "SAP",
            "competitor_normalized": "sap",
            "signal_type": "MEMBER_USAGE",
            "context": "Uses SAP ERP",
        })
        await session.flush()
        assert signal.competitor_name == "SAP"

    @pytest.mark.asyncio
    async def test_find_by_competitor(self, session):
        for i in range(3):
            await CompetitorSignalRepository.create(session, {
                "competitor_name": "Epicor",
                "competitor_normalized": "epicor",
                "signal_type": "MEMBER_USAGE",
                "context": f"Usage {i}",
            })
        await session.flush()

        signals = await CompetitorSignalRepository.find_by_competitor(session, "epicor")
        assert len(signals) == 3

    @pytest.mark.asyncio
    async def test_count_by_competitor(self, session):
        for name in ["epicor", "epicor", "sap", "sap", "sap"]:
            await CompetitorSignalRepository.create(session, {
                "competitor_name": name,
                "competitor_normalized": name,
                "signal_type": "MEMBER_USAGE",
                "context": "test",
            })
        await session.flush()

        counts = await CompetitorSignalRepository.count_by_competitor(session)
        assert counts["epicor"] == 2
        assert counts["sap"] == 3


# ---------------------------------------------------------------------------
# Connection Pool / Integration
# ---------------------------------------------------------------------------

class TestDatabasePool:

    @pytest.mark.asyncio
    async def test_pool_lifecycle(self):
        from db.connection import DatabasePool

        pool = DatabasePool("sqlite:///")
        await pool.init()

        assert pool.is_initialized
        assert await pool.health_check()

        async with pool.session() as session:
            result = await session.execute(
                __import__("sqlalchemy").text("SELECT 1")
            )
            assert result.scalar() == 1

        await pool.close()
        assert not pool.is_initialized

    @pytest.mark.asyncio
    async def test_pool_url_conversion(self):
        from db.connection import DatabasePool

        assert DatabasePool._to_async_url("postgresql://user:pass@host/db") == \
            "postgresql+asyncpg://user:pass@host/db"
        assert DatabasePool._to_async_url("postgres://user:pass@host/db") == \
            "postgresql+asyncpg://user:pass@host/db"
        assert DatabasePool._to_async_url("sqlite:///test.db") == \
            "sqlite+aiosqlite:///test.db"
        assert DatabasePool._to_async_url("mysql://x") == "mysql://x"  # unchanged

    @pytest.mark.asyncio
    async def test_pool_missing_url(self):
        from db.connection import DatabasePool
        import os
        os.environ.pop("DATABASE_URL", None)

        with pytest.raises(ValueError, match="database_url"):
            DatabasePool()

    @pytest.mark.asyncio
    async def test_session_rollback_on_error(self):
        from db.connection import DatabasePool

        pool = DatabasePool("sqlite:///")
        await pool.init()

        # Create tables
        async with pool.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        try:
            async with pool.session() as session:
                company = CompanyModel(
                    canonical_name="RollbackTest",
                    normalized_name="rollbacktest",
                )
                session.add(company)
                raise ValueError("force rollback")
        except ValueError:
            pass

        # Verify the company was NOT persisted (rolled back)
        async with pool.session() as session:
            count = await CompanyRepository.count(session)
            assert count == 0

        await pool.close()

    @pytest.mark.asyncio
    async def test_health_check_not_initialized(self):
        from db.connection import DatabasePool

        pool = DatabasePool("sqlite:///")
        assert await pool.health_check() is False


# ---------------------------------------------------------------------------
# Migrate helpers
# ---------------------------------------------------------------------------

class TestMigrateHelpers:

    @pytest.mark.asyncio
    async def test_create_and_drop_async(self):
        from db.migrate import create_all_async, drop_all_async

        eng = create_async_engine("sqlite+aiosqlite://", echo=False)
        await create_all_async(eng)

        async with eng.connect() as conn:
            tables = await conn.run_sync(
                lambda c: __import__("sqlalchemy").inspect(c).get_table_names()
            )
        assert len(tables) == 12

        await drop_all_async(eng)

        async with eng.connect() as conn:
            tables = await conn.run_sync(
                lambda c: __import__("sqlalchemy").inspect(c).get_table_names()
            )
        assert len(tables) == 0

        await eng.dispose()
