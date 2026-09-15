"""Testes de `TransactionEventsRepository` contra o banco real `ab_xql_pm_dev`.

Caso principal: `upsert_batch` reconcilia o conjunto inteiro de transacoes abertas por
instancia a cada chamada — upsert nas vistas, DELETE das que sumiram (ja fecharam).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedTransactionEvent
from app.repositories.transaction_events_repository import TransactionEventsRepository


async def test_upsert_batch_inserts_new_transactions(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = TransactionEventsRepository(db_session)
    begin_time = datetime.now(UTC) - timedelta(minutes=10)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-1",
                session_id="55",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=begin_time,
                state="ativa",
                last_error_code=None,
            ),
        ],
        datetime.now(UTC),
    )

    rows = await repo.list_for_instance(test_instance.id, 10)
    assert len(rows) == 1
    assert rows[0].transaction_identifier == "tx-1"
    assert rows[0].session_id == "55"
    assert rows[0].database_name == "AB_COBRANCA"


async def test_upsert_batch_removes_transactions_no_longer_open(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Reconciliacao: uma transacao vista num ciclo mas ausente no proximo (ja fechou) precisa
    sumir da tabela — nao pode ficar "presa" pra sempre como se ainda estivesse aberta."""
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-closes-soon",
                session_id="60",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
            ),
        ],
        now,
    )
    assert len(await repo.list_for_instance(test_instance.id, 10)) == 1

    # Proximo ciclo: a transacao ja fechou, nao aparece mais na lista de abertas.
    await repo.upsert_batch(test_instance.id, [], datetime.now(UTC))

    assert await repo.list_for_instance(test_instance.id, 10) == []


async def test_upsert_batch_preserves_first_seen_at_on_update(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = TransactionEventsRepository(db_session)
    first_observed = datetime.now(UTC) - timedelta(hours=1)
    begin_time = first_observed - timedelta(minutes=5)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-long-lived",
                session_id="70",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=begin_time,
                state="ativa",
                last_error_code=None,
            ),
        ],
        first_observed,
    )

    second_observed = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-long-lived",
                session_id="70",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=begin_time,
                state="ativa",
                last_error_code=None,
            ),
        ],
        second_observed,
    )

    row = await repo.get_by_identifier(test_instance.id, "tx-long-lived")
    assert row is not None
    assert abs((row.first_seen_at - first_observed).total_seconds()) < 1
    assert abs((row.last_seen_at - second_observed).total_seconds()) < 1


async def test_get_by_identifier_returns_none_when_not_found(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = TransactionEventsRepository(db_session)
    assert await repo.get_by_identifier(test_instance.id, "does-not-exist") is None


async def test_list_for_instance_orders_oldest_first(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-recent",
                session_id="80",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now - timedelta(minutes=1),
                state="ativa",
                last_error_code=None,
            ),
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-old",
                session_id="81",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now - timedelta(hours=2),
                state="ativa",
                last_error_code=None,
            ),
        ],
        now,
    )

    rows = await repo.list_for_instance(test_instance.id, 10)
    assert [row.transaction_identifier for row in rows] == ["tx-old", "tx-recent"]
