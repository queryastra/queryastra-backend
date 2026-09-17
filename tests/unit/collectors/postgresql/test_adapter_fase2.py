"""Testes de `PostgreSQLCollector` — Fase 2, item 2 (replicacao, freespace) e `run_scalar_query`
(item 2, `rule_type="custom_sql"`). Mesmo padrao de fake de conexao `asyncpg` de
`tests/unit/collectors/postgresql/test_adapter.py`.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import asyncpg
import pytest

from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.adapter import PostgreSQLCollector
from app.normalizers.metric_names import PG_REPLICA_REPLAY_LAG_SECONDS

_CONNECTION = ConnectionParams(host="localhost", port=5432, auth_method="encrypted_fallback")


class _FakeReplicationConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.closed = False

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        if "pg_stat_replication" in query:
            return self._rows
        raise AssertionError(f"fetch inesperado: {query}")

    async def close(self) -> None:
        self.closed = True


def _patch_connect(monkeypatch: pytest.MonkeyPatch, fake_conn: object) -> None:
    async def fake_connect(**kwargs: object) -> object:
        return fake_conn

    monkeypatch.setattr(asyncpg, "connect", fake_connect)


async def test_collect_replication_lag_metrics_returns_replay_lag_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = _FakeReplicationConnection(
        [{"application_name": "standby-1", "replay_lag_seconds": 3.5}]
    )
    _patch_connect(monkeypatch, fake_conn)

    instance_id = uuid4()
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=instance_id)
    samples = await collector.collect_replication_lag_metrics()

    assert len(samples) == 1
    assert samples[0].metric_name == PG_REPLICA_REPLAY_LAG_SECONDS
    assert samples[0].value == 3.5
    assert samples[0].labels == {"replica": "standby-1"}
    assert samples[0].target_id == instance_id
    assert fake_conn.closed


async def test_collect_replication_lag_metrics_empty_when_no_replica(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = _FakeReplicationConnection([])
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    samples = await collector.collect_replication_lag_metrics()

    assert samples == []


async def test_collect_freespace_metrics_always_empty_without_connecting() -> None:
    """PostgreSQL nao expoe espaco livre de disco via SQL puro - sempre lista vazia, sem
    sequer tentar conectar (mesma classe de limitacao ja documentada pra CPU/deadlock)."""
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    samples = await collector.collect_freespace_metrics()
    assert samples == []


class _FakeScalarConnection:
    def __init__(self, value: object) -> None:
        self._value = value
        self.closed = False

    async def fetchval(self, sql: str) -> object:
        del sql
        return self._value

    async def close(self) -> None:
        self.closed = True


async def test_run_scalar_query_returns_float_value(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = _FakeScalarConnection(10)
    _patch_connect(monkeypatch, fake_conn)
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())

    value = await collector.run_scalar_query("SELECT COUNT(*) FROM t", 5.0)

    assert value == 10.0
    assert fake_conn.closed


async def test_run_scalar_query_returns_none_when_null(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = _FakeScalarConnection(None)
    _patch_connect(monkeypatch, fake_conn)
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())

    value = await collector.run_scalar_query("SELECT COUNT(*) FROM t", 5.0)

    assert value is None
