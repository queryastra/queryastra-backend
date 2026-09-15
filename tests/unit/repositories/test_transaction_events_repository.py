"""Testes de `TransactionEventsRepository` contra o banco real `ab_xql_pm_dev`.

Caso principal (redesenho pos-uso real — log de incidente retido): `upsert_batch` grava/atualiza
as transacoes FLAGGED do ciclo (`flag_reason` uniao de todos os motivos ja vistos) e FECHA
(`closed_at`, nunca deleta) qualquer transacao ja rastreada que nao esta mais entre as abertas do
ciclo (`open_transaction_identifiers`).
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
                flag_reason="orfa",
            ),
        ],
        {"tx-1"},
        datetime.now(UTC),
    )

    rows = await repo.list_for_instance(test_instance.id, 10)
    assert len(rows) == 1
    assert rows[0].transaction_identifier == "tx-1"
    assert rows[0].session_id == "55"
    assert rows[0].database_name == "AB_COBRANCA"
    assert rows[0].flag_reason == "orfa"
    assert rows[0].closed_at is None


async def test_upsert_batch_closes_transactions_no_longer_open(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Reconciliacao: uma transacao vista num ciclo mas ausente no proximo (ja fechou) precisa
    ganhar `closed_at` — NUNCA deletada, so deixa de aparecer na listagem `open_only` (padrao)."""
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
                flag_reason="bloqueio",
            ),
        ],
        {"tx-closes-soon"},
        now,
    )
    assert len(await repo.list_for_instance(test_instance.id, 10)) == 1

    # Proximo ciclo: a transacao ja fechou, nao aparece mais entre as abertas.
    closed_at = datetime.now(UTC)
    await repo.upsert_batch(test_instance.id, [], set(), closed_at)

    assert await repo.list_for_instance(test_instance.id, 10) == []
    row = await repo.get_by_identifier(test_instance.id, "tx-closes-soon")
    assert row is not None
    assert row.closed_at is not None
    assert row.state == "encerrada"
    # Continua acessivel via list_for_instance(open_only=False) e get_by_identifier.
    open_and_closed = await repo.list_for_instance(test_instance.id, 10, open_only=False)
    assert [row.transaction_identifier for row in open_and_closed] == ["tx-closes-soon"]


async def test_upsert_batch_does_not_close_transaction_still_open_but_not_reflagged(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Uma transacao ja flagged que continua aberta mas nao e re-flagged neste ciclo (ex.:
    bloqueio resolveu, ainda nao virou orfa) NAO deve ser fechada — so falta atualizacao ate ser
    re-flagged ou realmente fechar."""
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-still-open",
                session_id="61",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
                flag_reason="bloqueio",
            ),
        ],
        {"tx-still-open"},
        now,
    )

    # Proximo ciclo: nao re-flagged (sem eventos), mas ainda ABERTA (open_transaction_identifiers).
    await repo.upsert_batch(test_instance.id, [], {"tx-still-open"}, datetime.now(UTC))

    row = await repo.get_by_identifier(test_instance.id, "tx-still-open")
    assert row is not None
    assert row.closed_at is None


async def test_upsert_batch_merges_flag_reason_tags(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`flag_reason` e uma UNIAO de todas as tags ja vistas, nao uma sobrescrita — uma transacao
    que foi flagged por bloqueio num ciclo e por orfa depois preserva as duas."""
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-multi-reason",
                session_id="62",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
                flag_reason="bloqueio",
            ),
        ],
        {"tx-multi-reason"},
        now,
    )

    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-multi-reason",
                session_id="62",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
                flag_reason="orfa",
            ),
        ],
        {"tx-multi-reason"},
        datetime.now(UTC),
    )

    row = await repo.get_by_identifier(test_instance.id, "tx-multi-reason")
    assert row is not None
    assert set(row.flag_reason.split(",")) == {"bloqueio", "orfa"}


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
                flag_reason="orfa",
            ),
        ],
        {"tx-long-lived"},
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
                flag_reason="orfa",
            ),
        ],
        {"tx-long-lived"},
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
                flag_reason="orfa",
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
                flag_reason="orfa",
            ),
        ],
        {"tx-recent", "tx-old"},
        now,
    )

    rows = await repo.list_for_instance(test_instance.id, 10)
    assert [row.transaction_identifier for row in rows] == ["tx-old", "tx-recent"]


async def test_tag_open_transactions_by_session_adds_deadlock_tag(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Correlacao assincrona com deadlock: uma transacao aberta com `session_id` batendo ganha
    `"deadlock"` em `flag_reason`, sem perder a tag anterior."""
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-deadlock-victim",
                session_id="90",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
                flag_reason="bloqueio",
            ),
        ],
        {"tx-deadlock-victim"},
        now,
    )

    await repo.tag_open_transactions_by_session(test_instance.id, ["90"], "deadlock")

    row = await repo.get_by_identifier(test_instance.id, "tx-deadlock-victim")
    assert row is not None
    assert set(row.flag_reason.split(",")) == {"bloqueio", "deadlock"}


async def test_purge_closed_before_removes_only_old_closed_rows(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-old-closed",
                session_id="91",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
                flag_reason="orfa",
            ),
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-still-open-2",
                session_id="92",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
                flag_reason="orfa",
            ),
        ],
        {"tx-old-closed", "tx-still-open-2"},
        now,
    )
    # Fecha so "tx-old-closed" (mantendo "tx-still-open-2" aberta).
    await repo.upsert_batch(test_instance.id, [], {"tx-still-open-2"}, now)

    # `purge_closed_before` age sobre a tabela inteira (sem filtro por instancia, por design —
    # e um expurgo global por idade) — o banco de dev compartilhado pode ja ter outras linhas
    # fechadas de testes/sessoes anteriores, entao so verificamos que ESTAS duas linhas terminam
    # no estado esperado, sem exigir uma contagem exata de linhas removidas.
    await repo.purge_closed_before(datetime.now(UTC) + timedelta(seconds=1))

    assert await repo.get_by_identifier(test_instance.id, "tx-old-closed") is None
    assert await repo.get_by_identifier(test_instance.id, "tx-still-open-2") is not None
