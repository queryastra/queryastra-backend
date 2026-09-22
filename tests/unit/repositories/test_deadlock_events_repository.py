"""Testes de `DeadlockEventsRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.deadlock_event import DeadlockEvent
from app.models.instance import Instance
from app.normalizers.models import NormalizedDeadlockEvent
from app.repositories.deadlock_events_repository import DeadlockEventsRepository


async def _cleanup(db_session: AsyncSession, instance_id) -> None:  # noqa: ANN001 - helper de teste
    await db_session.execute(delete(DeadlockEvent).where(DeadlockEvent.instance_id == instance_id))
    await db_session.commit()


async def test_insert_pending_creates_placeholder_row(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Deteccao instantanea (`PollingWorker`) — so `instance_id`/`time` preenchidos,
    `pending=True`, nenhum outro campo inventado."""
    repo = DeadlockEventsRepository(db_session)
    detected_at = datetime.now(UTC)

    try:
        row = await repo.insert_pending(test_instance.id, detected_at)

        assert row.pending is True
        assert row.time == detected_at
        assert row.victim_session_id is None
        assert row.database_name is None

        listed = await repo.list_for_instance(test_instance.id, limit=10)
        assert len(listed) == 1
        assert listed[0].id == row.id
    finally:
        await _cleanup(db_session, test_instance.id)


async def test_insert_new_reconciles_pending_row_instead_of_duplicating(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """O caminho principal desta feature: um registro pendente (deteccao instantanea) e'
    substituido no lugar quando o relatorio completo chega do `system_health`, minutos depois -
    nunca vira uma segunda linha."""
    repo = DeadlockEventsRepository(db_session)
    detected_at = datetime.now(UTC)
    # O timestamp REAL do XML quase sempre difere um pouco do instante em que o contador foi
    # percebido incrementado - simula 8 segundos de diferenca, dentro da janela de reconciliacao.
    real_event_time = detected_at + timedelta(seconds=8)

    try:
        pending = await repo.insert_pending(test_instance.id, detected_at)

        new_rows = await repo.insert_new(
            [
                NormalizedDeadlockEvent(
                    instance_id=test_instance.id,
                    time=real_event_time,
                    database_name="AB_TESTE_LOCK",
                    victim_session_id="61",
                    victim_sql_text_masked="UPDATE t SET x = ?",
                    survivor_session_id="62",
                    survivor_sql_text_masked="UPDATE t SET y = ?",
                    resource_description="keylock (AB_TESTE_LOCK.dbo.t)",
                    raw_xml_masked="<deadlock/>",
                    detail={"processes": [], "resources": []},
                )
            ]
        )

        assert len(new_rows) == 1
        assert new_rows[0].id == pending.id  # mesma linha, reconciliada - nao uma nova
        assert new_rows[0].pending is False
        assert new_rows[0].time == real_event_time
        assert new_rows[0].victim_session_id == "61"
        assert new_rows[0].survivor_session_id == "62"

        listed = await repo.list_for_instance(test_instance.id, limit=10)
        assert len(listed) == 1  # continua so 1 linha no total, nao 2
    finally:
        await _cleanup(db_session, test_instance.id)


async def test_insert_new_inserts_normally_when_no_pending_row_matches(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Sem deteccao instantanea previa (ex.: instancia com poll mais lento, ou o counter-diff nao
    disparou) - `insert_new` continua funcionando como antes, inserindo a linha direto."""
    repo = DeadlockEventsRepository(db_session)
    event_time = datetime.now(UTC)

    try:
        new_rows = await repo.insert_new(
            [
                NormalizedDeadlockEvent(
                    instance_id=test_instance.id,
                    time=event_time,
                    database_name="AB_TESTE_LOCK",
                    victim_session_id="61",
                    victim_sql_text_masked="UPDATE t SET x = ?",
                    survivor_session_id="62",
                    survivor_sql_text_masked="UPDATE t SET y = ?",
                    resource_description="keylock (AB_TESTE_LOCK.dbo.t)",
                    raw_xml_masked="<deadlock/>",
                    detail={"processes": [], "resources": []},
                )
            ]
        )

        assert len(new_rows) == 1
        assert new_rows[0].pending is False
    finally:
        await _cleanup(db_session, test_instance.id)


async def test_insert_new_ignores_pending_row_outside_reconciliation_window(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Um pendente MUITO antigo (fora da janela de 10min) nao e' reconciliado por engano - o
    evento real vira uma linha nova, e o pendente antigo continua pendente (sera' expurgado
    separadamente por idade, mesma politica dos demais)."""
    repo = DeadlockEventsRepository(db_session)
    old_pending_time = datetime.now(UTC) - timedelta(hours=2)
    real_event_time = datetime.now(UTC)

    try:
        pending = await repo.insert_pending(test_instance.id, old_pending_time)

        new_rows = await repo.insert_new(
            [
                NormalizedDeadlockEvent(
                    instance_id=test_instance.id,
                    time=real_event_time,
                    database_name="AB_TESTE_LOCK",
                    victim_session_id="61",
                    victim_sql_text_masked="UPDATE t SET x = ?",
                    survivor_session_id="62",
                    survivor_sql_text_masked="UPDATE t SET y = ?",
                    resource_description="keylock (AB_TESTE_LOCK.dbo.t)",
                    raw_xml_masked="<deadlock/>",
                    detail={"processes": [], "resources": []},
                )
            ]
        )

        assert len(new_rows) == 1
        assert new_rows[0].id != pending.id

        listed = await repo.list_for_instance(test_instance.id, limit=10)
        assert len(listed) == 2  # o pendente antigo (ainda pendente) + a linha nova
    finally:
        await _cleanup(db_session, test_instance.id)
