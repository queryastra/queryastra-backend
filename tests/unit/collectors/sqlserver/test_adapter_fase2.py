"""Testes de `SQLServerCollector` — Fase 2, item 2 (freespace, replicacao) e `run_scalar_query`
(item 2, `rule_type="custom_sql"`). Mesmo padrao de fake de `pymssql.connect`/cursor de
`tests/unit/collectors/sqlserver/test_adapter.py`.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pymssql
import pytest

from app.collectors.connection import ConnectionParams
from app.collectors.sqlserver.adapter import SQLServerCollector
from app.normalizers.metric_names import (
    SQLSERVER_REPLICA_LOG_SEND_QUEUE_KB,
    SQLSERVER_REPLICA_REDO_QUEUE_KB,
    SQLSERVER_VOLUME_FREE_PERCENT,
)

_CONNECTION = ConnectionParams(host="localhost", port=1433, auth_method="encrypted_fallback")


class _FakeDictCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self._pending: list[dict[str, Any]] = []

    def execute(self, query: str, *args: object) -> None:
        del query, args
        self._pending = self._rows

    def fetchall(self) -> list[dict[str, Any]]:
        return self._pending

    def close(self) -> None:
        pass


class _FakeDictConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.closed = False

    def cursor(self, as_dict: bool = False) -> _FakeDictCursor:
        assert as_dict is True
        return _FakeDictCursor(self._rows)

    def close(self) -> None:
        self.closed = True


def _patch_dict_connect(
    monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]
) -> _FakeDictConnection:
    fake_conn = _FakeDictConnection(rows)
    monkeypatch.setattr(pymssql, "connect", lambda **kwargs: fake_conn)
    return fake_conn


async def test_collect_freespace_metrics_computes_percent_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch,
        [{"volume_mount_point": "C:\\", "total_bytes": 1000, "available_bytes": 250}],
    )
    instance_id = uuid4()
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=instance_id)

    samples = await collector.collect_freespace_metrics()

    assert len(samples) == 1
    assert samples[0].metric_name == SQLSERVER_VOLUME_FREE_PERCENT
    assert samples[0].value == 25.0
    assert samples[0].labels == {"volume": "C:\\"}
    assert samples[0].target_id == instance_id


async def test_collect_freespace_metrics_labels_null_volume_as_desconhecido(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`sys.dm_os_volume_stats.volume_mount_point` pode vir NULL (Microsoft documenta esse
    caso) - confirmado ao vivo contra Autklinux20. `str(None)` viraria a string literal "None"
    no label, que e pior do que rotular como desconhecido."""
    _patch_dict_connect(
        monkeypatch,
        [{"volume_mount_point": None, "total_bytes": 1000, "available_bytes": 500}],
    )
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    samples = await collector.collect_freespace_metrics()

    assert samples[0].labels == {"volume": "desconhecido"}


async def test_collect_freespace_metrics_raises_value_error_without_instance_id() -> None:
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError, match="instance_id"):
        await collector.collect_freespace_metrics()


async def test_collect_replication_lag_metrics_returns_log_and_redo_queue_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch,
        [
            {
                "replica_server_name": "AUTKLINUX20-SECONDARY",
                "database_name": "AB_COBRANCA",
                "log_send_queue_size": 128,
                "redo_queue_size": 64,
            }
        ],
    )
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    samples = await collector.collect_replication_lag_metrics()

    assert len(samples) == 2
    by_metric = {s.metric_name: s for s in samples}
    assert by_metric[SQLSERVER_REPLICA_LOG_SEND_QUEUE_KB].value == 128.0
    assert by_metric[SQLSERVER_REPLICA_REDO_QUEUE_KB].value == 64.0
    for sample in samples:
        assert sample.labels == {
            "replica": "AUTKLINUX20-SECONDARY",
            "database": "AB_COBRANCA",
        }


async def test_collect_replication_lag_metrics_empty_when_no_ag_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(monkeypatch, [])
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    samples = await collector.collect_replication_lag_metrics()

    assert samples == []


class _FakeScalarCursor:
    def __init__(self, row: tuple | None) -> None:
        self._row = row

    def execute(self, sql: str) -> None:
        del sql

    def fetchone(self) -> tuple | None:
        return self._row

    def close(self) -> None:
        pass


class _FakeScalarConnection:
    def __init__(self, row: tuple | None) -> None:
        self._row = row

    def cursor(self) -> _FakeScalarCursor:
        return _FakeScalarCursor(self._row)

    def close(self) -> None:
        pass


async def test_run_scalar_query_returns_float_from_first_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pymssql, "connect", lambda **kwargs: _FakeScalarConnection((42,)))
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    value = await collector.run_scalar_query("SELECT COUNT(*) FROM t", 5.0)

    assert value == 42.0


async def test_run_scalar_query_returns_none_when_no_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pymssql, "connect", lambda **kwargs: _FakeScalarConnection(None))
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    value = await collector.run_scalar_query("SELECT COUNT(*) FROM t", 5.0)

    assert value is None


async def test_run_scalar_query_returns_none_for_non_numeric_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pymssql, "connect", lambda **kwargs: _FakeScalarConnection(("abc",)))
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    value = await collector.run_scalar_query("SELECT status FROM t", 5.0)

    assert value is None


# --- scan_plan_compile_timeouts (Fase 2, item 5) ------------------------------------------------

_PLAN_XML_WITH_TIMEOUT = """<?xml version="1.0"?>
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementOptmEarlyAbortReason="TimeOutReached" />
  </Statements></Batch></BatchSequence>
</ShowPlanXML>"""

_PLAN_XML_WITHOUT_TIMEOUT = """<?xml version="1.0"?>
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementOptmEarlyAbortReason="GoodEnoughPlanFound" />
  </Statements></Batch></BatchSequence>
</ShowPlanXML>"""


async def test_scan_plan_compile_timeouts_finds_statement_with_timeout_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch,
        [
            {"query_plan": _PLAN_XML_WITH_TIMEOUT, "statement_text": "SELECT * FROM t WHERE x=1"},
            {"query_plan": _PLAN_XML_WITHOUT_TIMEOUT, "statement_text": "SELECT 1"},
        ],
    )
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    findings = await collector.scan_plan_compile_timeouts()

    assert len(findings) == 1
    assert findings[0].reason == "TimeOutReached"
    assert "x=?" in findings[0].statement_text_masked or "?" in findings[0].statement_text_masked


async def test_scan_plan_compile_timeouts_dedupes_by_sql_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch,
        [
            {"query_plan": _PLAN_XML_WITH_TIMEOUT, "statement_text": "SELECT * FROM t WHERE x=1"},
            {"query_plan": _PLAN_XML_WITH_TIMEOUT, "statement_text": "SELECT * FROM t WHERE x=2"},
        ],
    )
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    findings = await collector.scan_plan_compile_timeouts()

    # mascarado, os dois statements viram o mesmo hash (literal ?1/?2 -> ?)
    assert len(findings) == 1


async def test_scan_plan_compile_timeouts_returns_empty_when_no_reason_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch, [{"query_plan": _PLAN_XML_WITHOUT_TIMEOUT, "statement_text": "SELECT 1"}]
    )
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    findings = await collector.scan_plan_compile_timeouts()

    assert findings == []


# --- scan_plan_history (historico de plano de execucao) -----------------------------------------

_SIMPLE_PLAN_XML = """<?xml version="1.0"?>
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple />
  </Statements></Batch></BatchSequence>
</ShowPlanXML>"""

_DIFFERENT_PLAN_XML = """<?xml version="1.0"?>
<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
  <BatchSequence><Batch><Statements>
    <StmtSimple PhysicalOp="Index Scan" />
  </Statements></Batch></BatchSequence>
</ShowPlanXML>"""

_LAST_EXEC_TIME = datetime(2026, 9, 18, 12, 0, 0, tzinfo=UTC)


async def test_scan_plan_history_returns_snapshot_with_masked_plan_and_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch,
        [
            {
                "query_plan": _SIMPLE_PLAN_XML,
                "statement_text": "SELECT * FROM t WHERE x=1",
                "last_execution_time": _LAST_EXEC_TIME,
            },
        ],
    )
    instance_id = uuid4()
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=instance_id)

    snapshots = await collector.scan_plan_history()

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instance_id == instance_id
    assert snapshot.last_execution_time == _LAST_EXEC_TIME
    assert snapshot.plan_xml_masked
    assert (
        snapshot.plan_hash
        == hashlib.sha256(snapshot.plan_xml_masked.encode("utf-8")).hexdigest()
    )


async def test_scan_plan_history_dedupes_by_sql_hash_keeping_first_seen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch,
        [
            {
                "query_plan": _SIMPLE_PLAN_XML,
                "statement_text": "SELECT * FROM t WHERE x=1",
                "last_execution_time": None,
            },
            {
                "query_plan": _DIFFERENT_PLAN_XML,
                "statement_text": "SELECT * FROM t WHERE x=2",
                "last_execution_time": None,
            },
        ],
    )
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    snapshots = await collector.scan_plan_history()

    # mascarado, os dois statements viram o mesmo sql_hash (literal ?1/?2 -> ?)
    assert len(snapshots) == 1


async def test_scan_plan_history_distinguishes_different_plans_by_plan_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_dict_connect(
        monkeypatch,
        [
            {
                "query_plan": _SIMPLE_PLAN_XML,
                "statement_text": "SELECT * FROM a",
                "last_execution_time": None,
            },
            {
                "query_plan": _DIFFERENT_PLAN_XML,
                "statement_text": "SELECT * FROM b",
                "last_execution_time": None,
            },
        ],
    )
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())

    snapshots = await collector.scan_plan_history()

    assert len(snapshots) == 2
    assert snapshots[0].plan_hash != snapshots[1].plan_hash


async def test_scan_plan_history_raises_value_error_without_instance_id() -> None:
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError):
        await collector.scan_plan_history()
