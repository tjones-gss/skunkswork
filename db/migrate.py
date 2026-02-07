"""
Migration Wrapper
NAM Intelligence Pipeline

Convenience functions for running Alembic migrations programmatically.
Also provides ``create_all`` for quick schema creation (tests, dev).
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine

from db.models import Base


def create_all_sync(database_url: str) -> None:
    """Create all tables synchronously (useful for scripts and simple dev setup)."""
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    engine.dispose()


def drop_all_sync(database_url: str) -> None:
    """Drop all tables synchronously."""
    engine = create_engine(database_url)
    Base.metadata.drop_all(engine)
    engine.dispose()


async def create_all_async(engine: AsyncEngine) -> None:
    """Create all tables using an async engine (e.g., for tests)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_all_async(engine: AsyncEngine) -> None:
    """Drop all tables using an async engine."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
