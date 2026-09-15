"""Testes de integracao de `GET /api/v1/transactions/*` contra o banco real `ab_xql_pm_dev` —
Etapa 10, itens 2/3 (rastreamento de transacoes)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedTransactionEvent
from app.repositories.transaction_events_repository import TransactionEventsRepository


async def test_list_transactions_returns_empty_list_when_none(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/transactions/{test_instance.id}")

    assert response.status_code == 200
    assert response.json() == []


async def test_list_transactions_flags_old_transaction_as_orphan_candidate(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-orphan",
                session_id="90",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now - timedelta(hours=1),
                state="ativa",
                last_error_code=None,
                flag_reason="orfa",
            ),
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-fresh",
                session_id="91",
                database_name="AB_COBRANCA",
                transaction_type="local",
                transaction_uow=None,
                begin_time=now,
                state="ativa",
                last_error_code=None,
                flag_reason="bloqueio",
            ),
        ],
        {"tx-orphan", "tx-fresh"},
        now,
    )

    response = await client.get(f"/api/v1/transactions/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    by_id = {row["transaction_identifier"]: row for row in body}
    assert by_id["tx-orphan"]["is_orphan_candidate"] is True
    assert by_id["tx-fresh"]["is_orphan_candidate"] is False


async def test_get_transaction_returns_detail(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = TransactionEventsRepository(db_session)
    now = datetime.now(UTC)
    await repo.upsert_batch(
        test_instance.id,
        [
            NormalizedTransactionEvent(
                instance_id=test_instance.id,
                transaction_identifier="tx-detail",
                session_id="95",
                database_name="AB_COBRANCA",
                transaction_type="distributed",
                transaction_uow="11111111-1111-1111-1111-111111111111",
                begin_time=now,
                state="ativa",
                last_error_code=547,
                flag_reason="erro",
            ),
        ],
        {"tx-detail"},
        now,
    )

    response = await client.get(f"/api/v1/transactions/{test_instance.id}/tx-detail")

    assert response.status_code == 200
    body = response.json()
    assert body["transaction_identifier"] == "tx-detail"
    assert body["transaction_type"] == "distributed"
    assert body["transaction_uow"] == "11111111-1111-1111-1111-111111111111"
    assert body["last_error_code"] == 547


async def test_get_transaction_returns_404_when_not_found(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/transactions/{test_instance.id}/does-not-exist")

    assert response.status_code == 404


async def test_list_transactions_for_unknown_instance_returns_empty_list(
    client: AsyncClient,
) -> None:
    response = await client.get(f"/api/v1/transactions/{uuid4()}")

    assert response.status_code == 200
    assert response.json() == []
