"""
LexSolv AI — Async SQLAlchemy database engine and session management.

Uses asyncpg as the PostgreSQL driver for fully async I/O.
Gracefully handles missing DATABASE_URL so the app can still start.
"""

from __future__ import annotations

import logging
import os
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database URL
# ---------------------------------------------------------------------------
# Accepts DATABASE_URL in standard postgres:// or postgresql+asyncpg:// form.
# Railway / Heroku typically provide postgres:// — we normalise it here.

_raw_url = os.getenv("DATABASE_URL", "")
DATABASE_URL: Optional[str] = None

if _raw_url:
    DATABASE_URL = _raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
    if "asyncpg" not in DATABASE_URL:
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------

async_engine: Optional[AsyncEngine] = None
async_session_factory: Optional[async_sessionmaker] = None

if DATABASE_URL:
    async_engine = create_async_engine(
        DATABASE_URL,
        echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )

    async_session_factory = async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    logger.info("Database engine created (URL configured)")
else:
    logger.warning(
        "DATABASE_URL is not set — database features are disabled. "
        "Add a PostgreSQL database and set the DATABASE_URL environment variable."
    )


# ---------------------------------------------------------------------------
# Base class for ORM models
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Dependency for FastAPI route injection
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Yield an async DB session for a single request, then close it.

    Usage in a route:
        @app.get("/example")
        async def example(db: AsyncSession = Depends(get_db)):
            ...
    """
    if async_session_factory is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL.")

    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
