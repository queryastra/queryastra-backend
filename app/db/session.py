"""Engine assincrono e sessionmaker do SQLAlchemy.

Le `DATABASE_URL` de `app/core/config.py` — nunca hardcoded. O driver esperado e `asyncpg`
(`postgresql+asyncpg://...`), compativel com o TimescaleDB descrito em `Docs/database-schema.md`.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings


def create_engine() -> AsyncEngine:
    """Cria o engine assincrono a partir da configuracao atual."""
    settings = get_settings()
    return create_async_engine(settings.database_url, pool_pre_ping=True, future=True)


engine: AsyncEngine = create_engine()

async_session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency FastAPI: entrega uma sessao por request, fecha ao final."""
    async with async_session_factory() as session:
        yield session
