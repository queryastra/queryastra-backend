"""Testes de `SystemSettingsRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.system_settings_repository import SystemSettingsRepository


async def test_get_returns_the_singleton_row(db_session: AsyncSession) -> None:
    repo = SystemSettingsRepository(db_session)
    row = await repo.get()
    assert row.id == 1
    assert row.blocking_min_duration_ms >= 0


async def test_update_blocking_min_duration_ms_persists_and_is_read_back(
    db_session: AsyncSession,
) -> None:
    repo = SystemSettingsRepository(db_session)
    try:
        updated = await repo.update_blocking_min_duration_ms(2500)
        assert updated.blocking_min_duration_ms == 2500

        reread = await repo.get()
        assert reread.blocking_min_duration_ms == 2500
    finally:
        await repo.update_blocking_min_duration_ms(0)


async def test_get_is_idempotent_across_calls(db_session: AsyncSession) -> None:
    repo = SystemSettingsRepository(db_session)
    first = await repo.get()
    second = await repo.get()
    assert first.blocking_min_duration_ms == second.blocking_min_duration_ms
