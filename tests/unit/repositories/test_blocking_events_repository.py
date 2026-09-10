"""Testes de `BlockingEventsRepository.insert_batch` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

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


async def test_list_latest_snapshot_ignores_older_resolved_blocks(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingEventsRepository(db_session)
    now = datetime.now(UTC)
    older_time = now - timedelta(minutes=10)
    await repo.insert_batch(
        [
            NormalizedBlockingEvent(
                time=older_time,
                instance_id=test_instance.id,
                blocker_session_id="resolved-blocker",
                blocked_session_id="resolved-blocked",
                wait_time_ms=500,
            )
        ]
    )
    await repo.insert_batch(
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="current-blocker",
                blocked_session_id="current-blocked",
                wait_time_ms=250,
            )
        ]
    )

    rows = await repo.list_latest_snapshot(
        test_instance.id, since=now - timedelta(minutes=30), limit=100
    )

    blocker_ids = {row.blocker_session_id for row in rows}
    assert blocker_ids == {"current-blocker"}
