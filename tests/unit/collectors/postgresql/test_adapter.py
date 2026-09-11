"""Testes de `PostgreSQLCollector.collect()` — `app/collectors/postgresql/adapter.py`.

Usa um fake de conexao `asyncpg` (sem I/O de rede real), via
`monkeypatch.setattr(asyncpg, "connect", fake_connect)`. As linhas fixas simulam
`pg_stat_activity`/`pg_blocking_pids`/`pg_stat_database`/`pg_stat_bgwriter`|`pg_stat_checkpointer`
conforme `Docs/collector-queries.md` §1, §2, §5.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import asyncpg
import pytest

from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.adapter import PostgreSQLCollector
from app.normalizers.metric_names import PG_CHECKPOINTS_TIMED

_CONNECTION = ConnectionParams(host="localhost", port=5432, auth_method="encrypted_fallback")

_DATABASE_STATS_ROW: dict[str, Any] = {
    "numbackends": 3,
    "xact_commit": 100,
    "xact_rollback": 1,
    "blks_read": 10,
    "blks_hit": 990,
    "tup_returned": 500,
    "tup_fetched": 400,
    "tup_inserted": 5,
    "tup_updated": 2,
    "tup_deleted": 1,
    "deadlocks": 0,
    "temp_files": 0,
    "temp_bytes": 0,
}

_BGWRITER_ROW: dict[str, Any] = {
    "checkpoints_timed": 7,
    "checkpoints_req": 1,
    "checkpoint_write_time": 123.0,
    "checkpoint_sync_time": 4.0,
    "buffers_checkpoint": 42,
}

_CHECKPOINTER_ROW: dict[str, Any] = {
    "num_timed": 8,
    "num_requested": 2,
    "write_time": 234.0,
    "sync_time": 5.0,
    "buffers_written": 43,
}


def _session_row(
    pid: int, query: str | None, query_start: datetime | None = None
) -> dict[str, Any]:
    return {
        "pid": pid,
        "usename": "monitoring_user",
        "datname": "app_db",
        "application_name": "psql",
        "client_addr": "10.0.0.5",
        "backend_start": datetime.now(UTC),
        "xact_start": None,
        "query_start": query_start,
        "state_change": datetime.now(UTC),
        "wait_event_type": "Lock",
        "wait_event": "relation",
        "state": "active",
        "backend_type": "client backend",
        "query": query,
    }


class _FakeConnection:
    """Fake de `asyncpg.Connection`: dispatcha por trecho da query, sem I/O real."""

    def __init__(
        self,
        version_num: int,
        session_rows: list[dict[str, Any]],
        blocking_rows: list[dict[str, Any]],
    ) -> None:
        self._version_num = version_num
        self._session_rows = session_rows
        self._blocking_rows = blocking_rows
        self.closed = False
        self.checkpointer_queried = False
        self.bgwriter_queried = False

    async def fetchval(self, query: str, *args: object) -> Any:
        if "server_version_num" in query:
            return self._version_num
        raise AssertionError(f"fetchval inesperado: {query}")

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        if "blocked_by" in query:
            return self._blocking_rows
        if "pg_stat_activity" in query:
            return self._session_rows
        raise AssertionError(f"fetch inesperado: {query}")

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any]:
        if "pg_stat_checkpointer" in query:
            self.checkpointer_queried = True
            return _CHECKPOINTER_ROW
        if "pg_stat_bgwriter" in query:
            self.bgwriter_queried = True
            return _BGWRITER_ROW
        if "pg_stat_database" in query:
            return _DATABASE_STATS_ROW
        raise AssertionError(f"fetchrow inesperado: {query}")

    async def close(self) -> None:
        self.closed = True


def _patch_connect(monkeypatch: pytest.MonkeyPatch, fake_conn: _FakeConnection) -> None:
    async def fake_connect(**kwargs: object) -> _FakeConnection:
        return fake_conn

    monkeypatch.setattr(asyncpg, "connect", fake_connect)


async def test_collect_uses_checkpointer_on_pg17_plus(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = _FakeConnection(version_num=170000, session_rows=[], blocking_rows=[])
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert fake_conn.checkpointer_queried is True
    assert fake_conn.bgwriter_queried is False
    assert fake_conn.closed is True
    checkpoints_timed = [s for s in result.metric_samples if s.metric_name == PG_CHECKPOINTS_TIMED]
    assert len(checkpoints_timed) == 1
    assert checkpoints_timed[0].value == 8.0
    # Regressao: versao exposta em CollectionResult pra PollingWorker persistir de volta em
    # instances.engine_version* (antes desta feature, nunca era persistida - corte de escopo
    # da Etapa 3, fechado agora).
    assert result.engine_version == "17.0"
    assert result.engine_version_major == 17


async def test_collect_uses_bgwriter_below_pg17(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = _FakeConnection(version_num=160003, session_rows=[], blocking_rows=[])
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert fake_conn.bgwriter_queried is True
    assert fake_conn.checkpointer_queried is False
    checkpoints_timed = [s for s in result.metric_samples if s.metric_name == PG_CHECKPOINTS_TIMED]
    assert len(checkpoints_timed) == 1
    assert checkpoints_timed[0].value == 7.0
    # 160003 -> major 16, minor 3 (nao "16.0"; server_version_num nao tem componente de patch
    # separado no PG10+, o minor completo ja esta nos ultimos 4 digitos).
    assert result.engine_version == "16.3"
    assert result.engine_version_major == 16


async def test_collect_masks_sql_literal_before_hashing(monkeypatch: pytest.MonkeyPatch) -> None:
    session_rows = [_session_row(pid=101, query="SELECT * FROM t WHERE x = 'segredo'")]
    fake_conn = _FakeConnection(version_num=170000, session_rows=session_rows, blocking_rows=[])
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.query_samples) == 1
    sample = result.query_samples[0]
    assert "segredo" not in sample.sql_text_masked
    assert sample.sql_text_masked == "SELECT * FROM t WHERE x = ?"

    import hashlib

    assert sample.sql_hash == hashlib.sha256(sample.sql_text_masked.encode("utf-8")).hexdigest()


async def test_collect_expands_multiple_blockers_into_multiple_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_rows = [
        _session_row(pid=200, query="UPDATE t SET a = 1", query_start=datetime.now(UTC)),
        _session_row(pid=201, query="SELECT 1"),
        _session_row(pid=202, query="SELECT 2"),
    ]
    blocking_rows = [{"pid": 200, "blocked_by": [201, 202], "query": "UPDATE t SET a = 1"}]
    fake_conn = _FakeConnection(
        version_num=170000, session_rows=session_rows, blocking_rows=blocking_rows
    )
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.blocking_events) == 2
    blocker_ids = {e.blocker_session_id for e in result.blocking_events}
    assert blocker_ids == {"201", "202"}
    for event in result.blocking_events:
        assert event.blocked_session_id == "200"
        assert event.wait_time_ms >= 0


async def test_collect_skips_row_with_null_query(monkeypatch: pytest.MonkeyPatch) -> None:
    session_rows = [
        _session_row(pid=300, query=None),
        _session_row(pid=301, query="SELECT 1"),
    ]
    fake_conn = _FakeConnection(version_num=170000, session_rows=session_rows, blocking_rows=[])
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.query_samples) == 1
    assert result.query_samples[0].session_id == "301"


async def test_collect_clamps_duration_ms_to_zero_when_query_start_is_ahead_of_collected_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regressao: `collected_at` e carimbado logo apos o fetch de `pg_stat_activity` (dentro de
    `collect()`), mas ainda pode ficar, por uma margem de rede residual, ligeiramente atras de um
    `query_start` observado pelo proprio servidor no mesmo instante do snapshot — o que sem clamp
    produziria `duration_ms` negativo (bug real encontrado em producao antes desta correcao)."""
    future_query_start = datetime.now(UTC) + timedelta(milliseconds=500)
    session_rows = [_session_row(pid=400, query="SELECT 1", query_start=future_query_start)]
    fake_conn = _FakeConnection(version_num=170000, session_rows=session_rows, blocking_rows=[])
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.query_samples) == 1
    assert result.query_samples[0].duration_ms == 0


async def test_collect_raises_value_error_without_instance_id() -> None:
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError, match="instance_id"):
        await collector.collect()
