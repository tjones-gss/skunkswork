"""
Repository Layer — Data Access Objects
NAM Intelligence Pipeline

Provides async CRUD operations with bulk upsert support for all core entities.
All methods accept an AsyncSession and are designed for use within a
``DatabasePool.session()`` context manager.
"""

from datetime import datetime, UTC
from typing import Optional, Sequence

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
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
# Company Repository
# ---------------------------------------------------------------------------

class CompanyRepository:
    """CRUD operations for the companies table."""

    @staticmethod
    async def get(session: AsyncSession, company_id: str) -> Optional[CompanyModel]:
        return await session.get(CompanyModel, company_id)

    @staticmethod
    async def find_by_domain(session: AsyncSession, domain: str) -> Optional[CompanyModel]:
        result = await session.execute(
            select(CompanyModel).where(CompanyModel.domain == domain)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def find_by_name(session: AsyncSession, name: str) -> Optional[CompanyModel]:
        result = await session.execute(
            select(CompanyModel).where(CompanyModel.canonical_name == name)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def search(
        session: AsyncSession,
        *,
        state: Optional[str] = None,
        industry: Optional[str] = None,
        erp_system: Optional[str] = None,
        min_quality: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[CompanyModel]:
        q = select(CompanyModel)
        if state:
            q = q.where(CompanyModel.state == state)
        if industry:
            q = q.where(CompanyModel.industry == industry)
        if erp_system:
            q = q.where(CompanyModel.erp_system == erp_system)
        if min_quality is not None:
            q = q.where(CompanyModel.quality_score >= min_quality)
        q = q.order_by(CompanyModel.canonical_name).limit(limit).offset(offset)
        result = await session.execute(q)
        return result.scalars().all()

    @staticmethod
    async def count(session: AsyncSession) -> int:
        result = await session.execute(select(func.count(CompanyModel.id)))
        return result.scalar_one()

    @staticmethod
    async def upsert(session: AsyncSession, data: dict) -> CompanyModel:
        """Insert or update a company. Matches on domain (if provided) or id."""
        existing = None
        if data.get("domain"):
            existing = await CompanyRepository.find_by_domain(session, data["domain"])
        if existing is None and data.get("id"):
            existing = await session.get(CompanyModel, data["id"])

        if existing:
            for key, value in data.items():
                if key != "id" and value is not None:
                    setattr(existing, key, value)
            existing.updated_at = datetime.now(UTC)
            return existing

        company = CompanyModel(**data)
        session.add(company)
        return company

    @staticmethod
    async def bulk_upsert(session: AsyncSession, records: list[dict]) -> int:
        """Upsert a batch of company records. Returns count of upserted rows."""
        count = 0
        for record in records:
            await CompanyRepository.upsert(session, record)
            count += 1
        return count

    @staticmethod
    async def delete(session: AsyncSession, company_id: str) -> bool:
        company = await session.get(CompanyModel, company_id)
        if company:
            await session.delete(company)
            return True
        return False


# ---------------------------------------------------------------------------
# Contact Repository
# ---------------------------------------------------------------------------

class ContactRepository:
    """CRUD operations for the contacts table."""

    @staticmethod
    async def get(session: AsyncSession, contact_id: str) -> Optional[ContactModel]:
        return await session.get(ContactModel, contact_id)

    @staticmethod
    async def find_by_company(
        session: AsyncSession, company_id: str
    ) -> Sequence[ContactModel]:
        result = await session.execute(
            select(ContactModel).where(ContactModel.company_id == company_id)
        )
        return result.scalars().all()

    @staticmethod
    async def find_by_email(session: AsyncSession, email: str) -> Optional[ContactModel]:
        result = await session.execute(
            select(ContactModel).where(ContactModel.email == email)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def upsert(session: AsyncSession, data: dict) -> ContactModel:
        """Upsert a contact. Matches on (company_id, email) or id."""
        existing = None
        if data.get("company_id") and data.get("email"):
            result = await session.execute(
                select(ContactModel).where(
                    ContactModel.company_id == data["company_id"],
                    ContactModel.email == data["email"],
                )
            )
            existing = result.scalar_one_or_none()
        if existing is None and data.get("id"):
            existing = await session.get(ContactModel, data["id"])

        if existing:
            for key, value in data.items():
                if key != "id" and value is not None:
                    setattr(existing, key, value)
            existing.updated_at = datetime.now(UTC)
            return existing

        contact = ContactModel(**data)
        session.add(contact)
        return contact

    @staticmethod
    async def bulk_upsert(session: AsyncSession, records: list[dict]) -> int:
        count = 0
        for record in records:
            await ContactRepository.upsert(session, record)
            count += 1
        return count


# ---------------------------------------------------------------------------
# Extraction Job Repository
# ---------------------------------------------------------------------------

class ExtractionJobRepository:
    """CRUD operations for the extraction_jobs table."""

    @staticmethod
    async def get(session: AsyncSession, job_id: str) -> Optional[ExtractionJobModel]:
        return await session.get(ExtractionJobModel, job_id)

    @staticmethod
    async def create(session: AsyncSession, data: dict) -> ExtractionJobModel:
        job = ExtractionJobModel(**data)
        session.add(job)
        return job

    @staticmethod
    async def update_progress(
        session: AsyncSession,
        job_id: str,
        *,
        processed_items: Optional[int] = None,
        created_items: Optional[int] = None,
        updated_items: Optional[int] = None,
        failed_items: Optional[int] = None,
        skipped_items: Optional[int] = None,
    ) -> Optional[ExtractionJobModel]:
        job = await session.get(ExtractionJobModel, job_id)
        if not job:
            return None
        if processed_items is not None:
            job.processed_items = processed_items
        if created_items is not None:
            job.created_items = created_items
        if updated_items is not None:
            job.updated_items = updated_items
        if failed_items is not None:
            job.failed_items = failed_items
        if skipped_items is not None:
            job.skipped_items = skipped_items
        job.updated_at = datetime.now(UTC)
        return job

    @staticmethod
    async def complete(
        session: AsyncSession, job_id: str, *, status: str = "completed"
    ) -> Optional[ExtractionJobModel]:
        job = await session.get(ExtractionJobModel, job_id)
        if not job:
            return None
        job.status = status
        job.completed_at = datetime.now(UTC)
        job.updated_at = datetime.now(UTC)
        return job

    @staticmethod
    async def list_by_status(
        session: AsyncSession, status: str, limit: int = 50
    ) -> Sequence[ExtractionJobModel]:
        result = await session.execute(
            select(ExtractionJobModel)
            .where(ExtractionJobModel.status == status)
            .order_by(ExtractionJobModel.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()


# ---------------------------------------------------------------------------
# Audit Repository
# ---------------------------------------------------------------------------

class AuditRepository:
    """CRUD operations for the quality_audit_log table."""

    @staticmethod
    async def log_change(session: AsyncSession, data: dict) -> QualityAuditLogModel:
        entry = QualityAuditLogModel(**data)
        session.add(entry)
        return entry

    @staticmethod
    async def get_history(
        session: AsyncSession,
        company_id: str,
        limit: int = 100,
    ) -> Sequence[QualityAuditLogModel]:
        result = await session.execute(
            select(QualityAuditLogModel)
            .where(QualityAuditLogModel.company_id == company_id)
            .order_by(QualityAuditLogModel.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def get_job_logs(
        session: AsyncSession,
        job_id: str,
        limit: int = 500,
    ) -> Sequence[QualityAuditLogModel]:
        result = await session.execute(
            select(QualityAuditLogModel)
            .where(QualityAuditLogModel.job_id == job_id)
            .order_by(QualityAuditLogModel.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()


# ---------------------------------------------------------------------------
# Event Repository
# ---------------------------------------------------------------------------

class EventRepository:
    """CRUD operations for the events table."""

    @staticmethod
    async def get(session: AsyncSession, event_id: str) -> Optional[EventModel]:
        return await session.get(EventModel, event_id)

    @staticmethod
    async def create(session: AsyncSession, data: dict) -> EventModel:
        event = EventModel(**data)
        session.add(event)
        return event

    @staticmethod
    async def list_by_association(
        session: AsyncSession, association_code: str, limit: int = 100
    ) -> Sequence[EventModel]:
        result = await session.execute(
            select(EventModel)
            .where(EventModel.organizer_association == association_code)
            .order_by(EventModel.start_date.desc())
            .limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def count(session: AsyncSession) -> int:
        result = await session.execute(select(func.count(EventModel.id)))
        return result.scalar_one()


# ---------------------------------------------------------------------------
# Event Participant Repository
# ---------------------------------------------------------------------------

class EventParticipantRepository:
    """CRUD operations for the event_participants table."""

    @staticmethod
    async def create(session: AsyncSession, data: dict) -> EventParticipantModel:
        participant = EventParticipantModel(**data)
        session.add(participant)
        return participant

    @staticmethod
    async def find_by_event(
        session: AsyncSession, event_id: str
    ) -> Sequence[EventParticipantModel]:
        result = await session.execute(
            select(EventParticipantModel).where(
                EventParticipantModel.event_id == event_id
            )
        )
        return result.scalars().all()

    @staticmethod
    async def find_sponsors(
        session: AsyncSession, event_id: str
    ) -> Sequence[EventParticipantModel]:
        result = await session.execute(
            select(EventParticipantModel).where(
                EventParticipantModel.event_id == event_id,
                EventParticipantModel.participant_type == "SPONSOR",
            )
        )
        return result.scalars().all()


# ---------------------------------------------------------------------------
# Competitor Signal Repository
# ---------------------------------------------------------------------------

class CompetitorSignalRepository:
    """CRUD operations for the competitor_signals table."""

    @staticmethod
    async def create(session: AsyncSession, data: dict) -> CompetitorSignalModel:
        signal = CompetitorSignalModel(**data)
        session.add(signal)
        return signal

    @staticmethod
    async def find_by_competitor(
        session: AsyncSession, competitor_normalized: str, limit: int = 100
    ) -> Sequence[CompetitorSignalModel]:
        result = await session.execute(
            select(CompetitorSignalModel)
            .where(CompetitorSignalModel.competitor_normalized == competitor_normalized)
            .order_by(CompetitorSignalModel.detected_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def count_by_competitor(session: AsyncSession) -> dict[str, int]:
        """Return {competitor_normalized: count} dict."""
        result = await session.execute(
            select(
                CompetitorSignalModel.competitor_normalized,
                func.count(CompetitorSignalModel.id),
            ).group_by(CompetitorSignalModel.competitor_normalized)
        )
        return {row[0]: row[1] for row in result.all()}
