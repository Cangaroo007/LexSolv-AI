"""
LexSolv AI — Async SQLAlchemy database engine and session management.

Uses asyncpg as the PostgreSQL driver for fully async I/O.
Falls back to SQLite (aiosqlite) for local development when DATABASE_URL is not set.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
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
# Falls back to a local SQLite database when DATABASE_URL is not set.

_raw_url = os.getenv("DATABASE_URL", "")
DATABASE_URL: Optional[str] = None
IS_SQLITE = False

if _raw_url:
    DATABASE_URL = _raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
    if "asyncpg" not in DATABASE_URL:
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
else:
    # Fall back to local SQLite for development / demo
    _db_path = Path(__file__).resolve().parent.parent / "lexsolv_local.db"
    DATABASE_URL = f"sqlite+aiosqlite:///{_db_path}"
    IS_SQLITE = True
    logger.info("No DATABASE_URL set — using local SQLite at %s", _db_path)

# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------

async_engine: Optional[AsyncEngine] = None
async_session_factory: Optional[async_sessionmaker] = None

if DATABASE_URL:
    engine_kwargs: dict = {
        "echo": os.getenv("SQL_ECHO", "false").lower() == "true",
    }
    if not IS_SQLITE:
        engine_kwargs["pool_size"] = 5
        engine_kwargs["max_overflow"] = 10
        engine_kwargs["pool_pre_ping"] = True

    async_engine = create_async_engine(DATABASE_URL, **engine_kwargs)

    async_session_factory = async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    logger.info("Database engine created (%s)", "SQLite" if IS_SQLITE else "PostgreSQL")


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
