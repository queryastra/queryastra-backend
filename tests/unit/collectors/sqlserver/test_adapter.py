"""Testes de `SQLServerCollector.collect()` — `app/collectors/sqlserver/adapter.py`.

Usa um fake de `pymssql.connect`/cursor (`as_dict=True`), sem I/O de rede real, via
`monkeypatch.setattr(pymssql, "connect", fake_connect)`.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
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
    cpu_time_ms: int = 500,
    wait_resource: str | None = None,
    transaction_id: int | None = None,
    prev_error: int = 0,
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
        "cpu_time_ms": cpu_time_ms,
        "reads": 10,
        "writes": 0,
        "logical_reads": 20,
        "row_count": 5,
        "blocking_session_id": blocking_session_id,
        "wait_type": wait_type,
        "wait_time": wait_time,
        "wait_resource": wait_resource,
        "transaction_id": transaction_id,
        "prev_error": prev_error,
        "login_name": "monitoring_user",
        "host_name": "app-server-1",
        "program_name": "sqlcmd",
        "database_name": "app_db",
        "query_text": query_text,
        "full_query_text": full_query_text if full_query_text is not None else query_text,
    }


class _FakeCursor:
    def __init__(
        self,
        version_row: dict[str, Any],
        session_rows: list,
        counter_rows: list,
        all_session_rows: list,
        blocked_object_rows: list,
        transaction_rows: list,
        transaction_database_rows: list,
    ) -> None:
        self._version_row = version_row
        self._session_rows = session_rows
        self._counter_rows = counter_rows
        self._all_session_rows = all_session_rows
        self._blocked_object_rows = blocked_object_rows
        self._transaction_rows = transaction_rows
        self._transaction_database_rows = transaction_database_rows
        self._pending: list[dict[str, Any]] = []

    def execute(self, query: str) -> None:
        if "SERVERPROPERTY" in query:
            self._pending = [self._version_row]
        elif "GETUTCDATE" in query:
            # Offset 0 (mesmo instante pros dois lados) - testes usam datetimes naive ja
            # tratados como UTC diretamente, sem diferenca de fuso a simular.
            _now = datetime(2026, 1, 1)
            self._pending = [{"utc_now": _now, "local_now": _now}]
        elif "dm_exec_requests" in query:
            self._pending = self._session_rows
        elif "dm_tran_locks" in query:
            self._pending = self._blocked_object_rows
        elif "dm_tran_database_transactions" in query:
            self._pending = self._transaction_database_rows
        elif "dm_tran_session_transactions" in query:
            self._pending = self._transaction_rows
        elif "dm_exec_sessions" in query:
            self._pending = self._all_session_rows
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
    def __init__(
        self,
        version_row: dict[str, Any],
        session_rows: list,
        counter_rows: list,
        all_session_rows: list,
        blocked_object_rows: list,
        transaction_rows: list,
        transaction_database_rows: list,
    ) -> None:
        self._version_row = version_row
        self._session_rows = session_rows
        self._counter_rows = counter_rows
        self._all_session_rows = all_session_rows
        self._blocked_object_rows = blocked_object_rows
        self._transaction_rows = transaction_rows
        self._transaction_database_rows = transaction_database_rows
        self.closed = False

    def cursor(self, as_dict: bool = False) -> _FakeCursor:
        assert as_dict is True
        return _FakeCursor(
            self._version_row,
            self._session_rows,
            self._counter_rows,
            self._all_session_rows,
            self._blocked_object_rows,
            self._transaction_rows,
            self._transaction_database_rows,
        )

    def close(self) -> None:
        self.closed = True


def _patch_connect(
    monkeypatch: pytest.MonkeyPatch,
    version_row: dict[str, Any],
    session_rows: list,
    counter_rows: list,
    all_session_rows: list | None = None,
    blocked_object_rows: list | None = None,
    transaction_rows: list | None = None,
    transaction_database_rows: list | None = None,
) -> _FakeConnection:
    # `all_session_rows` default: identidade equivalente a `session_rows` (converte cada linha de
    # request pras 4 colunas de `_ALL_SESSIONS_QUERY`) — suficiente pra maioria dos testes, que
    # nao focam no caso do bloqueador ocioso (sem request ativa).
    if all_session_rows is None:
        all_session_rows = [
            {
                "session_id": row["session_id"],
                "login_name": row["login_name"],
                "host_name": row["host_name"],
                "program_name": row["program_name"],
            }
            for row in session_rows
        ]
    fake_conn = _FakeConnection(
        version_row,
        session_rows,
        counter_rows,
        all_session_rows,
        blocked_object_rows if blocked_object_rows is not None else [],
        transaction_rows if transaction_rows is not None else [],
        transaction_database_rows if transaction_database_rows is not None else [],
    )

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
    # Regressao: versao exposta em CollectionResult pra PollingWorker persistir de volta em
    # instances.engine_version* (antes desta feature, nunca era persistida).
    assert result.engine_version == "16.0.1000.6"
    assert result.engine_version_major == 16


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


async def test_collect_enriches_idle_blocker_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Etapa 9 — um bloqueador OCIOSO (sem request ativa) nao aparece em
    `_SESSION_ACTIVITY_QUERY` (INNER JOIN com `sys.dm_exec_requests`), entao fica sem identidade
    se so olharmos `session_rows`. `_ALL_SESSIONS_QUERY` (`sys.dm_exec_sessions`, todas as
    sessoes conectadas) fecha esse gap."""
    session_rows = [
        _request_row(session_id=91, query_text="SELECT * FROM t", blocking_session_id=90),
    ]
    all_session_rows = [
        {
            "session_id": 91,
            "login_name": "monitoring_user",
            "host_name": "app-server-1",
            "program_name": "sqlcmd",
        },
        {
            "session_id": 90,
            "login_name": "idle_blocker_user",
            "host_name": "batch-host",
            "program_name": "batch_job.exe",
        },
    ]
    _patch_connect(
        monkeypatch,
        version_row={"product_version": "16.0.1000.6"},
        session_rows=session_rows,
        counter_rows=_COUNTER_ROWS,
        all_session_rows=all_session_rows,
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.blocking_events) == 1
    event = result.blocking_events[0]
    assert event.blocker_db_user == "idle_blocker_user"
    assert event.blocker_program == "batch_job.exe"
    assert event.blocker_machine == "batch-host"


async def test_collect_populates_blocked_object_from_dm_tran_locks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Etapa 9 — `blocked_object`/`blocked_wait_resource` vem de `wait_resource` (bruto, ja em
    `_SESSION_ACTIVITY_QUERY`) e `sys.dm_tran_locks`/`sys.partitions` (nome resolvido) — so pro
    lado BLOQUEADO."""
    session_rows = [
        _request_row(session_id=100, query_text="UPDATE t SET a=1", blocking_session_id=0),
        _request_row(
            session_id=101,
            query_text="SELECT * FROM t",
            blocking_session_id=100,
            wait_resource="KEY: 5:72057594051690496 (ffeb27d6a833)",
        ),
    ]
    blocked_object_rows = [{"session_id": 101, "resource_type": "KEY", "blocked_object": "dbo.t"}]
    _patch_connect(
        monkeypatch,
        version_row={"product_version": "16.0.1000.6"},
        session_rows=session_rows,
        counter_rows=_COUNTER_ROWS,
        blocked_object_rows=blocked_object_rows,
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.blocking_events) == 1
    event = result.blocking_events[0]
    assert event.blocked_object == "dbo.t"
    assert event.blocked_wait_resource == "KEY: 5:72057594051690496 (ffeb27d6a833)"


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


class _FakeMissingIndexCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self._pending: list[dict[str, Any]] = []

    def execute(self, query: str) -> None:
        if "dm_db_missing_index_group_stats" in query:
            self._pending = self._rows
        else:
            raise AssertionError(f"execute inesperado: {query}")

    def fetchall(self) -> list[dict[str, Any]]:
        return self._pending

    def close(self) -> None:
        pass


class _FakeMissingIndexConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.closed = False

    def cursor(self, as_dict: bool = False) -> _FakeMissingIndexCursor:
        assert as_dict is True
        return _FakeMissingIndexCursor(self._rows)

    def close(self) -> None:
        self.closed = True


def _patch_missing_index_connect(
    monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]
) -> _FakeMissingIndexConnection:
    fake_conn = _FakeMissingIndexConnection(rows)

    def fake_connect(**kwargs: object) -> _FakeMissingIndexConnection:
        return fake_conn

    monkeypatch.setattr(pymssql, "connect", fake_connect)
    return fake_conn


def _missing_index_row(
    database_name: str = "AB_COBRANCA",
    table_name: str = "[AB_COBRANCA].[dbo].[pedidos]",
    equality_columns: str | None = "[cliente_id]",
    inequality_columns: str | None = None,
    included_columns: str | None = "[valor]",
    avg_total_user_cost: float = 5.0,
    avg_user_impact: float = 80.0,
    user_seeks: int = 100,
    user_scans: int = 10,
) -> dict[str, Any]:
    impact_score = avg_total_user_cost * avg_user_impact * (user_seeks + user_scans)
    return {
        "database_name": database_name,
        "table_name": table_name,
        "impact_score": impact_score,
        "avg_total_user_cost": avg_total_user_cost,
        "avg_user_impact": avg_user_impact,
        "equality_columns": equality_columns,
        "inequality_columns": inequality_columns,
        "included_columns": included_columns,
        "user_seeks": user_seeks,
        "user_scans": user_scans,
    }


async def test_collect_missing_index_suggestions_builds_readable_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`sys.dm_db_missing_index_details` cobre a instancia inteira numa unica chamada — sem
    iterar por banco (ao contrario do PostgreSQL)."""
    row = _missing_index_row()
    _patch_missing_index_connect(monkeypatch, [row])

    instance_id = uuid4()
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=instance_id)
    suggestions = await collector.collect_missing_index_suggestions()

    assert len(suggestions) == 1
    suggestion = suggestions[0]
    assert suggestion.instance_id == instance_id
    assert suggestion.database_name == "AB_COBRANCA"
    assert suggestion.table_name == "[AB_COBRANCA].[dbo].[pedidos]"
    assert suggestion.impact_score == 5.0 * 80.0 * (100 + 10)
    assert "cliente_id" in suggestion.suggestion
    assert "valor" in suggestion.suggestion
    assert suggestion.detail["user_seeks"] == 100
    assert suggestion.detail["user_scans"] == 10
    assert suggestion.detail["avg_total_user_cost"] == 5.0
    assert suggestion.detail["avg_user_impact"] == 80.0


async def test_collect_missing_index_suggestions_handles_null_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`equality_columns`/`inequality_columns`/`included_columns` sao todos nullable — uma
    sugestao real quase sempre so preenche um subconjunto deles."""
    row = _missing_index_row(equality_columns=None, inequality_columns="[data_criacao]")
    _patch_missing_index_connect(monkeypatch, [row])

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    suggestions = await collector.collect_missing_index_suggestions()

    assert len(suggestions) == 1
    assert "data_criacao" in suggestions[0].suggestion
    assert suggestions[0].detail["equality_columns"] is None


async def test_collect_missing_index_suggestions_raises_value_error_without_instance_id() -> None:
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError, match="instance_id"):
        await collector.collect_missing_index_suggestions()


class _FakeBackupStatusCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self._pending: list[dict[str, Any]] = []

    def execute(self, query: str) -> None:
        if "msdb.dbo.backupset" in query:
            self._pending = self._rows
        else:
            raise AssertionError(f"execute inesperado: {query}")

    def fetchall(self) -> list[dict[str, Any]]:
        return self._pending

    def close(self) -> None:
        pass


class _FakeBackupStatusConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.closed = False

    def cursor(self, as_dict: bool = False) -> _FakeBackupStatusCursor:
        assert as_dict is True
        return _FakeBackupStatusCursor(self._rows)

    def close(self) -> None:
        self.closed = True


def _patch_backup_status_connect(
    monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]
) -> _FakeBackupStatusConnection:
    fake_conn = _FakeBackupStatusConnection(rows)

    def fake_connect(**kwargs: object) -> _FakeBackupStatusConnection:
        return fake_conn

    monkeypatch.setattr(pymssql, "connect", fake_connect)
    return fake_conn


async def test_collect_backup_status_returns_one_row_per_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`msdb.dbo.backupset` cobre a instancia inteira numa unica chamada — uma linha por banco,
    so backup completo (`type='D'`)."""
    last_backup = datetime(2026, 9, 10, 3, 0, 0)
    _patch_backup_status_connect(
        monkeypatch,
        [
            {"database_name": "AB_COBRANCA", "last_backup_at": last_backup},
            {"database_name": "master", "last_backup_at": last_backup},
        ],
    )

    instance_id = uuid4()
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=instance_id)
    rows = await collector.collect_backup_status()

    assert len(rows) == 2
    by_database = {row.database_name: row for row in rows}
    assert by_database["AB_COBRANCA"].instance_id == instance_id
    assert by_database["AB_COBRANCA"].last_backup_at == last_backup
    assert by_database["AB_COBRANCA"].detail == {"backup_type": "D"}
    assert by_database["master"].last_backup_at == last_backup


async def test_collect_backup_status_handles_database_with_no_backup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Um banco sem nenhum backup completo registrado ainda deve vir com `last_backup_at=None`
    (MAX() de um conjunto vazio no SQL sai NULL, nao levanta erro)."""
    _patch_backup_status_connect(
        monkeypatch, [{"database_name": "banco_novo", "last_backup_at": None}]
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    rows = await collector.collect_backup_status()

    assert len(rows) == 1
    assert rows[0].last_backup_at is None


async def test_collect_backup_status_raises_value_error_without_instance_id() -> None:
    collector = SQLServerCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError, match="instance_id"):
        await collector.collect_backup_status()


# --- fetch_execution_plan (Etapa 11 — busca sob demanda) --------------------------------------


def _masked_hash(statement_text: str) -> str:
    """Mesmo pipeline de `_hash_query_text` (mascara + SHA-256), pra montar o hash esperado nos
    fixtures de teste sem depender do simbolo privado do modulo."""
    from app.masking.sql_masking import mask_sql_literals

    return hashlib.sha256(mask_sql_literals(statement_text).encode("utf-8")).hexdigest()


class _FakeExecutionPlanCursor:
    def __init__(
        self, search_rows: list[dict[str, Any]], plan_xml_by_handle: dict[str, str]
    ) -> None:
        self._search_rows = search_rows
        self._plan_xml_by_handle = plan_xml_by_handle
        self._pending_rows: list[dict[str, Any]] = []
        self._pending_one: dict[str, Any] | None = None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        if "dm_exec_query_stats" in query:
            self._pending_rows = self._search_rows
        elif "dm_exec_query_plan" in query:
            assert params is not None
            plan_handle = params[0]
            xml = self._plan_xml_by_handle.get(plan_handle)
            self._pending_one = {"query_plan": xml} if xml is not None else None
        else:
            raise AssertionError(f"execute inesperado: {query}")

    def fetchall(self) -> list[dict[str, Any]]:
        return self._pending_rows

    def fetchone(self) -> dict[str, Any] | None:
        return self._pending_one

    def close(self) -> None:
        pass


class _FakeExecutionPlanConnection:
    def __init__(
        self, search_rows: list[dict[str, Any]], plan_xml_by_handle: dict[str, str]
    ) -> None:
        self._search_rows = search_rows
        self._plan_xml_by_handle = plan_xml_by_handle
        self.closed = False

    def cursor(self, as_dict: bool = False) -> _FakeExecutionPlanCursor:
        assert as_dict is True
        return _FakeExecutionPlanCursor(self._search_rows, self._plan_xml_by_handle)

    def close(self) -> None:
        self.closed = True


def _patch_execution_plan_connect(
    monkeypatch: pytest.MonkeyPatch,
    search_rows: list[dict[str, Any]],
    plan_xml_by_handle: dict[str, str] | None = None,
) -> _FakeExecutionPlanConnection:
    fake_conn = _FakeExecutionPlanConnection(search_rows, plan_xml_by_handle or {})

    def fake_connect(**kwargs: object) -> _FakeExecutionPlanConnection:
        return fake_conn

    monkeypatch.setattr(pymssql, "connect", fake_connect)
    return fake_conn


async def test_fetch_execution_plan_returns_xml_for_matching_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statement = "SELECT * FROM t WHERE id = 42"
    sql_hash = _masked_hash(statement)
    _patch_execution_plan_connect(
        monkeypatch,
        search_rows=[
            {"plan_handle": b"handle-a", "statement_text": "SELECT * FROM other WHERE x = 1"},
            {"plan_handle": b"handle-b", "statement_text": statement},
        ],
        plan_xml_by_handle={b"handle-b": "<ShowPlanXML>...</ShowPlanXML>"},
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    plan_xml = await collector.fetch_execution_plan(sql_hash)

    assert plan_xml == "<ShowPlanXML>...</ShowPlanXML>"


async def test_fetch_execution_plan_returns_none_when_no_cached_plan_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_execution_plan_connect(
        monkeypatch,
        search_rows=[
            {"plan_handle": b"handle-a", "statement_text": "SELECT * FROM other WHERE x = 1"},
        ],
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    plan_xml = await collector.fetch_execution_plan(_masked_hash("SELECT * FROM t WHERE id = 42"))

    assert plan_xml is None


async def test_fetch_execution_plan_returns_none_when_plan_evicted_from_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O `plan_handle` bate (statement encontrado), mas `sys.dm_exec_query_plan` devolve NULL —
    o plano ja saiu do cache entre a amostra original e esta busca. Nao e um erro, so `None`."""
    statement = "SELECT * FROM t WHERE id = 42"
    sql_hash = _masked_hash(statement)
    _patch_execution_plan_connect(
        monkeypatch,
        search_rows=[{"plan_handle": b"handle-a", "statement_text": statement}],
        plan_xml_by_handle={},  # sem entrada para handle-a -> query_plan NULL
    )

    collector = SQLServerCollector(connection=_CONNECTION, instance_id=uuid4())
    plan_xml = await collector.fetch_execution_plan(sql_hash)

    assert plan_xml is None
