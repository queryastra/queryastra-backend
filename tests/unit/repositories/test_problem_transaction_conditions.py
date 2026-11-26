"""O criterio de "transacao problematica no periodo" e UM so — tela de Transacoes e secao
"Envolvimento em eventos" do detalhe de query usam `problem_transaction_conditions`."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from app.models.transaction_event import TransactionEvent
from app.repositories.transaction_events_repository import problem_transaction_conditions


def _sql(conditions: list[sa.ColumnElement[bool]]) -> str:
    stmt = sa.select(TransactionEvent.id).where(*conditions)
    return str(stmt.compile(dialect=postgresql.dialect()))


def test_always_requires_flag_reason() -> None:
    sql = _sql(problem_transaction_conditions(uuid4()))
    assert "transaction_events.flag_reason IS NOT NULL" in sql
    assert "transaction_events.flag_reason !=" in sql
    assert "closed_at" not in sql
    assert "begin_time" not in sql


def test_period_is_overlap() -> None:
    now = datetime.now(UTC)
    sql = _sql(problem_transaction_conditions(uuid4(), since=now, until=now))
    assert "transaction_events.closed_at IS NULL OR transaction_events.closed_at >=" in sql
    assert "transaction_events.begin_time <=" in sql
