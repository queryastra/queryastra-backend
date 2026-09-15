"""Testes de `BlockingIncidentsRepository` contra o banco real `ab_xql_pm_dev` — Etapa 10,
redesenho (log de incidente de bloqueio retido, ver `app/models/blocking_incident.py`)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedBlockingEvent
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository


async def test_upsert_batch_inserts_new_incident_with_started_at(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingIncidentsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="b1",
                blocked_session_id="w1",
                wait_time_ms=100,
            )
        ],
        now,
    )

    rows = await repo.list_open(test_instance.id, 10)
    assert len(rows) == 1
    assert rows[0].blocker_session_id == "b1"
    assert rows[0].blocked_session_id == "w1"
    assert abs((rows[0].started_at - now).total_seconds()) < 1
    assert rows[0].ended_at is None


async def test_upsert_batch_preserves_started_at_across_cycles(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingIncidentsRepository(db_session)
    first_observed = datetime.now(UTC) - timedelta(minutes=10)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=first_observed,
                instance_id=test_instance.id,
                blocker_session_id="b2",
                blocked_session_id="w2",
                wait_time_ms=100,
            )
        ],
        first_observed,
    )

    second_observed = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=second_observed,
                instance_id=test_instance.id,
                blocker_session_id="b2",
                blocked_session_id="w2",
                wait_time_ms=200,
            )
        ],
        second_observed,
    )

    rows = await repo.list_open(test_instance.id, 10)
    assert len(rows) == 1
    assert abs((rows[0].started_at - first_observed).total_seconds()) < 1
    assert abs((rows[0].last_seen_at - second_observed).total_seconds()) < 1
    assert rows[0].wait_time_ms == 200


async def test_upsert_batch_closes_incident_no_longer_observed(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingIncidentsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="b3",
                blocked_session_id="w3",
                wait_time_ms=100,
            )
        ],
        now,
    )

    closed_at = datetime.now(UTC)
    await repo.upsert_batch(test_instance.id, [], closed_at)

    assert await repo.list_open(test_instance.id, 10) == []
    all_incidents = await repo.list_incidents(test_instance.id, 10)
    assert len(all_incidents) == 1
    assert all_incidents[0].ended_at is not None


async def test_list_incidents_filters_by_database_name(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingIncidentsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="b4",
                blocked_session_id="w4",
                wait_time_ms=100,
                blocker_database_name="AB_COBRANCA",
                blocked_database_name="AB_COBRANCA",
            ),
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="b5",
                blocked_session_id="w5",
                wait_time_ms=100,
                blocker_database_name="OUTRO_BANCO",
                blocked_database_name="OUTRO_BANCO",
            ),
        ],
        now,
    )

    rows = await repo.list_incidents(test_instance.id, 10, database_name="AB_COBRANCA")
    assert len(rows) == 1
    assert rows[0].blocker_session_id == "b4"


async def test_find_involving_session_returns_both_roles(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingIncidentsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="chain-a",
                blocked_session_id="chain-b",
                wait_time_ms=100,
            ),
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="chain-b",
                blocked_session_id="chain-c",
                wait_time_ms=50,
            ),
        ],
        now,
    )

    rows = await repo.find_involving_session(test_instance.id, "chain-b", 10)
    pairs = {(r.blocker_session_id, r.blocked_session_id) for r in rows}
    assert pairs == {("chain-a", "chain-b"), ("chain-b", "chain-c")}


async def test_purge_ended_before_removes_only_old_ended_incidents(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BlockingIncidentsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="purge-b",
                blocked_session_id="purge-w",
                wait_time_ms=10,
            ),
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="keep-b",
                blocked_session_id="keep-w",
                wait_time_ms=10,
            ),
        ],
        now,
    )
    # Fecha so o par "purge-*" (mantendo "keep-*" em andamento).
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="keep-b",
                blocked_session_id="keep-w",
                wait_time_ms=10,
            )
        ],
        now,
    )

    # `purge_ended_before` age sobre a tabela inteira (sem filtro por instancia, por design) — o
    # banco de dev compartilhado pode ja ter outras linhas encerradas de sessoes anteriores,
    # entao so verificamos o estado desta instancia (via `list_incidents`), sem exigir uma
    # contagem exata de linhas removidas globalmente.
    await repo.purge_ended_before(datetime.now(UTC) + timedelta(seconds=1))

    remaining = await repo.list_incidents(test_instance.id, 10)
    assert [r.blocker_session_id for r in remaining] == ["keep-b"]
