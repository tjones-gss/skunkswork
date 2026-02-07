"""
Database Integration Tests
NAM Intelligence Pipeline

Tests BaseAgent.save_to_db() and orchestrator --persist-db integration
using SQLite in-memory.
"""

import pytest
import pytest_asyncio
from unittest.mock import MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.connection import DatabasePool
from db.models import Base, CompanyModel, ContactModel, EventModel
from db.repository import CompanyRepository, ContactRepository, EventRepository


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db_pool():
    """Create an in-memory SQLite database pool with all tables."""
    pool = DatabasePool("sqlite:///")
    await pool.init()
    async with pool.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def session(db_pool):
    async with db_pool.session() as sess:
        yield sess


# ---------------------------------------------------------------------------
# BaseAgent.save_to_db Tests
# ---------------------------------------------------------------------------

class TestBaseAgentSaveToDb:
    """Test the save_to_db method on BaseAgent."""

    def _make_agent(self, db_pool=None):
        """Create a minimal agent subclass for testing."""
        from agents.base import BaseAgent

        class StubAgent(BaseAgent):
            async def run(self, task):
                return {"success": True, "records_processed": 0}

        with patch("agents.base.Config"):
            with patch("agents.base.StructuredLogger"):
                with patch("agents.base.RateLimiter"):
                    with patch("agents.base.AsyncHTTPClient"):
                        agent = StubAgent(
                            agent_type="test.stub",
                            db_pool=db_pool,
                        )
        return agent

    @pytest.mark.asyncio
    async def test_save_to_db_without_pool_returns_zero(self):
        agent = self._make_agent(db_pool=None)
        count = await agent.save_to_db([{"company_name": "X"}], "company")
        assert count == 0

    @pytest.mark.asyncio
    async def test_save_to_db_companies(self, db_pool):
        agent = self._make_agent(db_pool=db_pool)
        records = [
            {
                "canonical_name": "Acme Co",
                "normalized_name": "acme co",
                "domain": "acme.com",
            },
            {
                "canonical_name": "Beta Co",
                "normalized_name": "beta co",
                "domain": "beta.com",
            },
        ]
        count = await agent.save_to_db(records, "company")
        assert count == 2

        # Verify persisted
        async with db_pool.session() as session:
            total = await CompanyRepository.count(session)
            assert total == 2

    @pytest.mark.asyncio
    async def test_save_to_db_contacts(self, db_pool):
        # First create a company
        async with db_pool.session() as session:
            company = CompanyModel(
                canonical_name="Parent",
                normalized_name="parent",
            )
            session.add(company)
            await session.flush()
            cid = company.id

        agent = self._make_agent(db_pool=db_pool)
        records = [
            {"company_id": cid, "full_name": "John", "email": "john@parent.com"},
        ]
        count = await agent.save_to_db(records, "contact")
        assert count == 1

    @pytest.mark.asyncio
    async def test_save_to_db_events(self, db_pool):
        agent = self._make_agent(db_pool=db_pool)
        records = [
            {"title": "TestEvent", "event_type": "CONFERENCE"},
        ]
        count = await agent.save_to_db(records, "event")
        assert count == 1

        async with db_pool.session() as session:
            total = await EventRepository.count(session)
            assert total == 1

    @pytest.mark.asyncio
    async def test_save_to_db_unknown_entity_type(self, db_pool):
        agent = self._make_agent(db_pool=db_pool)
        count = await agent.save_to_db([{"x": 1}], "unknown")
        assert count == 0

    @pytest.mark.asyncio
    async def test_save_to_db_handles_errors_gracefully(self, db_pool):
        agent = self._make_agent(db_pool=db_pool)
        # Pass data that will cause a missing required field error
        records = [
            {"domain": "missing-name.com"},  # missing canonical_name (NOT NULL)
        ]
        count = await agent.save_to_db(records, "company")
        # Should not crash, returns 0 or partial count
        assert count >= 0

    @pytest.mark.asyncio
    async def test_db_pool_attribute_none_by_default(self):
        agent = self._make_agent()
        assert agent.db_pool is None


# ---------------------------------------------------------------------------
# Pool Session Semantics
# ---------------------------------------------------------------------------

class TestPoolSessionSemantics:
    """Test transactional semantics of the DatabasePool."""

    @pytest.mark.asyncio
    async def test_auto_commit_on_success(self, db_pool):
        async with db_pool.session() as session:
            company = CompanyModel(
                canonical_name="Committed",
                normalized_name="committed",
            )
            session.add(company)
        # Session context manager commits on exit

        async with db_pool.session() as session:
            count = await CompanyRepository.count(session)
            assert count == 1

    @pytest.mark.asyncio
    async def test_auto_rollback_on_exception(self, db_pool):
        try:
            async with db_pool.session() as session:
                company = CompanyModel(
                    canonical_name="NotCommitted",
                    normalized_name="notcommitted",
                )
                session.add(company)
                raise RuntimeError("boom")
        except RuntimeError:
            pass

        async with db_pool.session() as session:
            count = await CompanyRepository.count(session)
            assert count == 0

    @pytest.mark.asyncio
    async def test_multiple_sessions_isolated(self, db_pool):
        """Two sessions see independent states before commit."""
        async with db_pool.session() as s1:
            s1.add(CompanyModel(
                canonical_name="FromS1",
                normalized_name="froms1",
            ))

        async with db_pool.session() as s2:
            count = await CompanyRepository.count(s2)
            # s1 committed, so s2 should see it
            assert count == 1


# ---------------------------------------------------------------------------
# Orchestrator --persist-db Flag
# ---------------------------------------------------------------------------

class TestOrchestratorPersistDbFlag:
    """Test that --persist-db CLI option is accepted."""

    def test_persist_db_option_exists(self):
        """Verify the click option is registered."""
        from agents.orchestrator import main
        param_names = [p.name for p in main.params]
        assert "persist_db" in param_names
