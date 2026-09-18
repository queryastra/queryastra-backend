"""Testes de `QueryPlanSnapshotsRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.models.query_plan_snapshot import QueryPlanSnapshot
from app.normalizers.models import NormalizedPlanSnapshot
from app.repositories.query_plan_snapshots_repository import QueryPlanSnapshotsRepository


async def _cleanup(db_session: AsyncSession, instance_id) -> None:  # noqa: ANN001 - helper de teste
    await db_session.execute(
        delete(QueryPlanSnapshot).where(QueryPlanSnapshot.instance_id == instance_id)
    )
    await db_session.commit()


async def test_insert_if_changed_inserts_first_snapshot_seen(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QueryPlanSnapshotsRepository(db_session)
    snapshot = NormalizedPlanSnapshot(
        instance_id=test_instance.id,
        sql_hash="hash-a",
        plan_hash="plan-hash-1",
        plan_xml_masked="<ShowPlanXML>1</ShowPlanXML>",
        last_execution_time=datetime.now(UTC),
    )

    new_rows = await repo.insert_if_changed([snapshot])

    assert len(new_rows) == 1
    history = await repo.list_for_query(test_instance.id, "hash-a", limit=10)
    assert len(history) == 1
    assert history[0].plan_hash == "plan-hash-1"

    await _cleanup(db_session, test_instance.id)


async def test_insert_if_changed_skips_when_plan_hash_unchanged(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QueryPlanSnapshotsRepository(db_session)
    make_snapshot = lambda: NormalizedPlanSnapshot(  # noqa: E731 - helper de teste
        instance_id=test_instance.id,
        sql_hash="hash-b",
        plan_hash="same-plan-hash",
        plan_xml_masked="<ShowPlanXML>x</ShowPlanXML>",
        last_execution_time=None,
    )

    first = await repo.insert_if_changed([make_snapshot()])
    second = await repo.insert_if_changed([make_snapshot()])

    assert len(first) == 1
    assert second == []
    history = await repo.list_for_query(test_instance.id, "hash-b", limit=10)
    assert len(history) == 1

    await _cleanup(db_session, test_instance.id)


async def test_insert_if_changed_inserts_new_row_when_plan_hash_differs(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QueryPlanSnapshotsRepository(db_session)
    await repo.insert_if_changed(
        [
            NormalizedPlanSnapshot(
                instance_id=test_instance.id,
                sql_hash="hash-c",
                plan_hash="plan-hash-old",
                plan_xml_masked="<ShowPlanXML>old</ShowPlanXML>",
                last_execution_time=None,
            )
        ]
    )
    new_rows = await repo.insert_if_changed(
        [
            NormalizedPlanSnapshot(
                instance_id=test_instance.id,
                sql_hash="hash-c",
                plan_hash="plan-hash-new",
                plan_xml_masked="<ShowPlanXML>new</ShowPlanXML>",
                last_execution_time=None,
            )
        ]
    )

    assert len(new_rows) == 1
    history = await repo.list_for_query(test_instance.id, "hash-c", limit=10)
    # mais recente primeiro — o plano NOVO aparece antes do antigo, os dois retidos (historico).
    assert len(history) == 2
    assert history[0].plan_hash == "plan-hash-new"
    assert history[1].plan_hash == "plan-hash-old"

    await _cleanup(db_session, test_instance.id)


async def test_insert_if_changed_noop_for_empty_list(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QueryPlanSnapshotsRepository(db_session)
    assert await repo.insert_if_changed([]) == []


async def test_purge_older_than_removes_only_old_snapshots(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QueryPlanSnapshotsRepository(db_session)
    await repo.insert_if_changed(
        [
            NormalizedPlanSnapshot(
                instance_id=test_instance.id,
                sql_hash="hash-d",
                plan_hash="plan-hash-1",
                plan_xml_masked="<ShowPlanXML>1</ShowPlanXML>",
                last_execution_time=None,
            )
        ]
    )
    # `captured_at` tem `server_default=now()` — recua manualmente pra simular um snapshot antigo.
    await db_session.execute(
        QueryPlanSnapshot.__table__.update()
        .where(QueryPlanSnapshot.instance_id == test_instance.id)
        .values(captured_at=datetime.now(UTC) - timedelta(days=100))
    )
    await db_session.commit()

    purged = await repo.purge_older_than(datetime.now(UTC) - timedelta(days=90))

    assert purged == 1
    assert await repo.list_for_query(test_instance.id, "hash-d", limit=10) == []
