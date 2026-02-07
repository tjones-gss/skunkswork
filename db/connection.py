"""
Async Database Connection Pool
NAM Intelligence Pipeline

Provides DatabasePool wrapping SQLAlchemy async engine with connection pooling,
health checks, and graceful shutdown.
"""

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Module-level singleton
_pool: Optional["DatabasePool"] = None


class DatabasePool:
    """
    Async connection pool wrapping SQLAlchemy async engine.

    Usage::

        pool = DatabasePool("postgresql+asyncpg://user:pass@localhost/db")
        await pool.init()

        async with pool.session() as session:
            result = await session.execute(text("SELECT 1"))

        await pool.close()
    """

    def __init__(
        self,
        database_url: str | None = None,
        *,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_recycle: int = 3600,
        echo: bool = False,
    ):
        self._url = database_url or os.getenv("DATABASE_URL", "")
        if not self._url:
            raise ValueError(
                "database_url must be provided or DATABASE_URL env var must be set"
            )

        # Convert sync URLs to async driver equivalents
        self._url = self._to_async_url(self._url)

        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._pool_recycle = pool_recycle
        self._echo = echo

    @staticmethod
    def _to_async_url(url: str) -> str:
        """Convert synchronous DB URLs to their async driver equivalents."""
        if url.startswith("postgresql://") or url.startswith("postgres://"):
            return url.replace("postgresql://", "postgresql+asyncpg://", 1).replace(
                "postgres://", "postgresql+asyncpg://", 1
            )
        if url.startswith("sqlite:///"):
            return url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
        return url

    async def init(self) -> None:
        """Initialize the engine and session factory."""
        if self._engine is not None:
            return

        engine_kwargs = {
            "echo": self._echo,
            "pool_recycle": self._pool_recycle,
        }

        # SQLite doesn't support pool_size/max_overflow
        if "sqlite" not in self._url:
            engine_kwargs["pool_size"] = self._pool_size
            engine_kwargs["max_overflow"] = self._max_overflow

        self._engine = create_async_engine(self._url, **engine_kwargs)
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """Provide a transactional async session scope."""
        if self._session_factory is None:
            raise RuntimeError("DatabasePool not initialized. Call init() first.")

        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def health_check(self) -> bool:
        """Check if the database is reachable."""
        if self._engine is None:
            return False
        try:
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Dispose the engine and release all connections."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None

    @property
    def engine(self) -> AsyncEngine | None:
        return self._engine

    @property
    def is_initialized(self) -> bool:
        return self._engine is not None


async def get_pool(database_url: str | None = None) -> DatabasePool:
    """Get or create the module-level DatabasePool singleton."""
    global _pool
    if _pool is None or not _pool.is_initialized:
        _pool = DatabasePool(database_url)
        await _pool.init()
    return _pool
