"""Testes de `BlockingEventsRepository.insert_batch` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedBlockingEvent
from app.repositories._tables import blocking_events_table
from app.repositories.blocking_events_repository import BlockingEventsRepository


async def test_insert_batch_persists_rows(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    events = [
        NormalizedBlockingEvent(
            time=datetime.now(UTC),
            instance_id=test_instance.id,
            blocker_session_id="1",
            blocked_session_id="2",
            wait_time_ms=100,
        )
    ]
    repo = BlockingEventsRepository(db_session)
    await repo.insert_batch(events)

    result = await db_session.execute(
        sa.select(blocking_events_table).where(
            blocking_events_table.c.instance_id == test_instance.id
        )
    )
    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0].blocker_session_id == "1"
    assert rows[0].blocked_session_id == "2"
    assert rows[0].wait_time_ms == 100


async def test_insert_batch_noop_for_empty_list(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingEventsRepository(db_session)
    await repo.insert_batch([])
