"""Testes de `SQLServerCollector.collect()` — `app/collectors/sqlserver/adapter.py`.

Usa um fake de `pymssql.connect`/cursor (`as_dict=True`), sem I/O de rede real, via
`monkeypatch.setattr(pymssql, "connect", fake_connect)`.
"""

from __future__ import annotations

import hashlib
from typing import Any
from uuid import uuid4

import pymssql
import pytest

from app.collectors.connection import ConnectionParams
from app.collectors.sqlserver.adapter import SQLServerCollector, _parse_major_version
from app.normalizers.metric_names import SQLSERVER_PAGE_LIFE_EXPECTANCY

_CONNECTION = ConnectionParams(host="localhost", port=1433, auth_method="encrypted_fallback")


def _request_row(
    session_id: int,
    query_text: str | None,
    blocking_session_id: int | None = 0,
    wait_type: str | None = None,
    wait_time: int = 0,
    full_query_text: str | None = None,
) -> dict[str, Any]:
    """`query_text` simula o `SUBSTRING` ja fatiado pelo statement em execucao (o que a query
    real devolve); `full_query_text` simula `t.text` cru (o batch/procedure inteiro) — por
    padrao igual a `query_text`, quando o teste nao se importa com a distincao entre os dois."""
    return {
        "session_id": session_id,
        "status": "running",
        "command": "SELECT",
        "start_time": None,
        "elapsed_seconds": 1.0,
        "total_elapsed_time": 1000,
        "cpu_seconds": 0.5,
        "reads": 10,
        "writes": 0,
        "logical_reads": 20,
        "row_count": 5,
        "blocking_session_id": blocking_session_id,
        "wait_type": wait_type,
        "wait_time": wait_time,
        "login_name": "monitoring_user",
        "host_name": "app-server-1",
        "program_name": "sqlcmd",
        "database_name": "app_db",
        "query_text": query_text,
        "full_query_text": full_query_text if full_query_text is not None else query_text,
    }


class _FakeCursor:
    def __init__(self, version_row: dict[str, Any], session_rows: list, counter_rows: list) -> None:
        self._version_row = version_row
        self._session_rows = session_rows
        self._counter_rows = counter_rows
        self._pending: list[dict[str, Any]] = []

    def execute(self, query: str) -> None:
        if "SERVERPROPERTY" in query:
            self._pending = [self._version_row]
        elif "dm_exec_requests" in query:
            self._pending = self._session_rows
        elif "dm_os_performance_counters" in query:
            self._pending = self._counter_rows
        else:
            raise AssertionError(f"execute inesperado: {query}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._pending[0] if self._pending else None

    def fetchall(self) -> list[dict[str, Any]]:
        return self._pending

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self, version_row: dict[str, Any], session_rows: list, counter_rows: list) -> None:
        self._version_row = version_row
        self._session_rows = session_rows
        self._counter_rows = counter_rows
        self.closed = False

    def cursor(self, as_dict: bool = False) -> _FakeCursor:
        assert as_dict is True
        return _FakeCursor(self._version_row, self._session_rows, self._counter_rows)

    def close(self) -> None:
        self.closed = True


def _patch_connect(
    monkeypatch: pytest.MonkeyPatch,
    version_row: dict[str, Any],
    session_rows: list,
    counter_rows: list,
) -> _FakeConnection:
    fake_conn = _FakeConnection(version_row, session_rows, counter_rows)

    def fake_connect(**kwargs: object) -> _FakeConnection:
        return fake_conn

    monkeypatch.setattr(pymssql, "connect", fake_connect)
    return fake_conn


_COUNTER_ROWS = [
    {"counter_name": "Page life expectancy", "cntr_value": 900},
    {"counter_name": "Processes blocked", "cntr_value": 0},
    {"counter_name": "Target Server Memory (KB)", "cntr_value": 1024000},
    {"counter_name": "Total Server Memory (KB)", "cntr_value": 512000},
]


async def test_collect_treats_negative_blocking_session_id_as_no_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_rows = [
        _request_row(session_id=51, query_text="SELECT 1", blocking_session_id=-2),
    ]
    _patch_connect(
        monkeypatch,
        version_row={"product_version": "16.0.1000.6"},
        session_rows=session_rows,
        counter_rows=_COUNTER_ROWS,
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert result.blocking_events == []
    assert len(result.query_samples) == 1


async def test_collect_creates_blocking_event_for_positive_blocking_session_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_rows = [
        _request_row(session_id=60, query_text="UPDATE t SET a=1", blocking_session_id=0),
        _request_row(
            session_id=61, query_text="SELECT * FROM t", blocking_session_id=60, wait_time=250
        ),
    ]
    _patch_connect(
        monkeypatch,
        version_row={"product_version": "16.0.1000.6"},
        session_rows=session_rows,
        counter_rows=_COUNTER_ROWS,
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.blocking_events) == 1
    event = result.blocking_events[0]
    assert event.blocker_session_id == "60"
    assert event.blocked_session_id == "61"
    assert event.wait_time_ms == 250
    assert event.blocker_sql_hash is not None


async def test_collect_maps_raw_counters_correctly(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_connect(
        monkeypatch,
        version_row={"product_version": "16.0.1000.6"},
        session_rows=[],
        counter_rows=_COUNTER_ROWS,
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    page_life = [
        s for s in result.metric_samples if s.metric_name == SQLSERVER_PAGE_LIFE_EXPECTANCY
    ]
    assert len(page_life) == 1
    assert page_life[0].value == 900.0


@pytest.mark.parametrize(
    ("product_version", "expected_major"),
    [("12.0.2000.8", 12), ("16.0.1000.6", 16), ("10.50.6000.34", 10)],
)
def test_parse_major_version(product_version: str, expected_major: int) -> None:
    assert _parse_major_version(product_version) == expected_major


async def test_collect_skips_row_with_null_query_text(monkeypatch: pytest.MonkeyPatch) -> None:
    session_rows = [
        _request_row(session_id=70, query_text=None),
        _request_row(session_id=71, query_text="SELECT 1"),
    ]
    _patch_connect(
        monkeypatch,
        version_row={"product_version": "16.0.1000.6"},
        session_rows=session_rows,
        counter_rows=[],
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.query_samples) == 1
    assert result.query_samples[0].session_id == "71"


async def test_collect_uses_statement_text_for_hash_not_full_procedure_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regressao: `sql_hash`/`sql_text_masked` tem que vir do STATEMENT em execucao (o que a
    query real fatia via `statement_start_offset`/`statement_end_offset`), nao do texto cru de
    `sys.dm_exec_sql_text` — que, para uma sessao dentro de uma stored procedure, e a
    `CREATE PROCEDURE ...` inteira (bug real encontrado em producao). `sql_text_full_masked`
    continua guardando o contexto completo (a procedure inteira), so pra tela de detalhe."""
    full_procedure_text = (
        "CREATE PROCEDURE dbo.sp_example AS BEGIN EXEC dbo.sp_cdc_scan @maxtrans = 1 END"
    )
    session_rows = [
        _request_row(
            session_id=80,
            query_text="EXEC dbo.sp_cdc_scan @maxtrans = 1",
            full_query_text=full_procedure_text,
        ),
    ]
    _patch_connect(
        monkeypatch,
        version_row={"product_version": "16.0.1000.6"},
        session_rows=session_rows,
        counter_rows=[],
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.query_samples) == 1
    sample = result.query_samples[0]
    assert sample.sql_text_masked == "EXEC dbo.sp_cdc_scan @maxtrans = ?"
    assert "CREATE PROCEDURE" not in sample.sql_text_masked
    assert (
        sample.sql_text_full_masked
        == "CREATE PROCEDURE dbo.sp_example AS BEGIN EXEC dbo.sp_cdc_scan @maxtrans = ? END"
    )
    assert sample.sql_hash == hashlib.sha256(sample.sql_text_masked.encode("utf-8")).hexdigest()


async def test_collect_raises_value_error_without_instance_id() -> None:
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError, match="instance_id"):
        await collector.collect()
