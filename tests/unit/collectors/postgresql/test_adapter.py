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
        all_session_rows: list[dict[str, Any]] | None = None,
        blocked_object_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self._version_num = version_num
        self._session_rows = session_rows
        self._blocking_rows = blocking_rows
        # Default: mesma identidade de `session_rows` (suficiente pra a maioria dos testes, que
        # nao focam no caso do bloqueador ocioso). `blocked_object_rows` default vazio — sem
        # `pg_locks` correspondente, `blocked_object`/`wait_resource` ficam None.
        self._all_session_rows = all_session_rows if all_session_rows is not None else session_rows
        self._blocked_object_rows = blocked_object_rows if blocked_object_rows is not None else []
        self.closed = False
        self.checkpointer_queried = False
        self.bgwriter_queried = False

    async def fetchval(self, query: str, *args: object) -> Any:
        if "server_version_num" in query:
            return self._version_num
        raise AssertionError(f"fetchval inesperado: {query}")

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        if "pg_locks" in query:
            return self._blocked_object_rows
        if "blocked_by" in query:
            return self._blocking_rows
        if "WHERE state" in query:
            return self._session_rows
        if "pg_stat_activity" in query:
            return self._all_session_rows
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


async def test_collect_reduces_transitive_blockers_to_direct_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Etapa 9 — `pg_blocking_pids()` devolve o fecho TRANSITIVO: se A(500) bloqueia B(501) e
    B(501) bloqueia C(502), `blocked_by` de C ja vem como `[500, 501]`, nao so `[501]`. Sem a
    reducao (`_reduce_to_direct_edges`), C ficaria ligado tanto a A quanto a B na arvore —
    achatando a cadeia real A -> B -> C. Com a reducao, C so liga a B (o bloqueador mais
    proximo), e B liga a A."""
    session_rows = [
        _session_row(pid=500, query="UPDATE t SET a = 1"),
        _session_row(pid=501, query="UPDATE t SET a = 2"),
        _session_row(pid=502, query="SELECT 1"),
    ]
    blocking_rows = [
        {"pid": 501, "blocked_by": [500], "query": "UPDATE t SET a = 2"},
        {"pid": 502, "blocked_by": [500, 501], "query": "SELECT 1"},
    ]
    fake_conn = _FakeConnection(
        version_num=170000, session_rows=session_rows, blocking_rows=blocking_rows
    )
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    edges = {(e.blocker_session_id, e.blocked_session_id) for e in result.blocking_events}
    assert edges == {("500", "501"), ("501", "502")}


async def test_collect_enriches_idle_blocker_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Etapa 9 — um bloqueador OCIOSO (transacao aberta segurando lock, sem `state` ativo) nao
    aparece em `_SESSION_ACTIVITY_QUERY` (`WHERE state != 'idle'`), entao fica sem identidade se
    so olharmos `session_rows`. `_ALL_SESSIONS_QUERY` (todas as sessoes conectadas) fecha esse
    gap."""
    session_rows = [_session_row(pid=601, query="SELECT 1")]  # so o bloqueado tem request ativa
    blocking_rows = [{"pid": 601, "blocked_by": [600], "query": "SELECT 1"}]
    all_session_rows = [
        *session_rows,
        {
            "pid": 600,
            "usename": "idle_blocker_user",
            "application_name": "batch_job",
            "client_addr": "10.0.0.9",
            "datname": "app_db",
            "xact_start": None,
            "state": "idle in transaction",
        },
    ]
    fake_conn = _FakeConnection(
        version_num=170000,
        session_rows=session_rows,
        blocking_rows=blocking_rows,
        all_session_rows=all_session_rows,
    )
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.blocking_events) == 1
    event = result.blocking_events[0]
    assert event.blocker_db_user == "idle_blocker_user"
    assert event.blocker_program == "batch_job"
    assert event.blocker_machine == "10.0.0.9"


async def test_collect_populates_blocked_object_from_pg_locks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Etapa 9 — `blocked_object`/`blocked_wait_resource` vem de `pg_locks` (so pro lado
    BLOQUEADO, que e quem esta de fato esperando por um recurso)."""
    session_rows = [
        _session_row(pid=700, query="UPDATE t SET a = 1"),
        _session_row(pid=701, query="SELECT * FROM t"),
    ]
    blocking_rows = [{"pid": 701, "blocked_by": [700], "query": "SELECT * FROM t"}]
    blocked_object_rows = [
        {"pid": 701, "locktype": "relation", "mode": "AccessShareLock", "blocked_object": "t"}
    ]
    fake_conn = _FakeConnection(
        version_num=170000,
        session_rows=session_rows,
        blocking_rows=blocking_rows,
        blocked_object_rows=blocked_object_rows,
    )
    _patch_connect(monkeypatch, fake_conn)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    result = await collector.collect()

    assert len(result.blocking_events) == 1
    event = result.blocking_events[0]
    assert event.blocked_object == "t"
    assert event.blocked_wait_resource == "relation/AccessShareLock"


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


class _FakeMissingIndexConnection:
    """Fake de conexao para `collect_missing_index_suggestions()` — `fetch()` devolve linhas
    fixas conforme a query pedida (lista de bancos ou heuristica de indice ausente)."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.closed = False

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        if "pg_database" in query or "pg_stat_user_tables" in query:
            return self._rows
        raise AssertionError(f"fetch inesperado: {query}")

    async def close(self) -> None:
        self.closed = True


async def test_collect_missing_index_suggestions_iterates_per_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`pg_stat_user_tables` e escopada ao banco conectado — a checagem precisa iterar por banco
    do cluster (nao so o banco de manutencao "postgres" usado por `collect()`/
    `check_capabilities()`), ao contrario do resto do coletor, cluster-wide via `pg_stat_activity`/
    `pg_stat_database`."""
    bootstrap_conn = _FakeMissingIndexConnection(rows=[{"datname": "db_a"}, {"datname": "db_b"}])
    db_a_conn = _FakeMissingIndexConnection(
        rows=[
            {
                "schemaname": "public",
                "table_name": "pedidos",
                "seq_scan": 500,
                "idx_scan": 10,
                "n_live_tup": 100_000,
                "table_size_bytes": 20_000_000,
            }
        ]
    )
    db_b_conn = _FakeMissingIndexConnection(rows=[])

    async def fake_connect(**kwargs: object) -> _FakeMissingIndexConnection:
        database = kwargs["database"]
        if database == "postgres":
            return bootstrap_conn
        if database == "db_a":
            return db_a_conn
        if database == "db_b":
            return db_b_conn
        raise AssertionError(f"conexao inesperada para database={database}")

    monkeypatch.setattr(asyncpg, "connect", fake_connect)

    instance_id = uuid4()
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=instance_id)
    suggestions = await collector.collect_missing_index_suggestions()

    assert len(suggestions) == 1
    suggestion = suggestions[0]
    assert suggestion.instance_id == instance_id
    assert suggestion.database_name == "db_a"
    assert suggestion.table_name == "public.pedidos"
    assert suggestion.impact_score == 490.0
    assert suggestion.detail["seq_scan"] == 500
    assert bootstrap_conn.closed
    assert db_a_conn.closed
    assert db_b_conn.closed


async def test_collect_missing_index_suggestions_skips_database_that_fails_to_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Falha de conexao/permissao num banco especifico do cluster nao aborta a checagem dos
    demais bancos — so aquele banco fica sem sugestao."""
    bootstrap_conn = _FakeMissingIndexConnection(rows=[{"datname": "db_a"}, {"datname": "db_b"}])
    db_b_conn = _FakeMissingIndexConnection(
        rows=[
            {
                "schemaname": "public",
                "table_name": "clientes",
                "seq_scan": 200,
                "idx_scan": 0,
                "n_live_tup": 50_000,
                "table_size_bytes": 15_000_000,
            }
        ]
    )

    async def fake_connect(**kwargs: object) -> _FakeMissingIndexConnection:
        database = kwargs["database"]
        if database == "postgres":
            return bootstrap_conn
        if database == "db_a":
            raise asyncpg.InvalidAuthorizationSpecificationError("sem permissao")
        if database == "db_b":
            return db_b_conn
        raise AssertionError(f"conexao inesperada para database={database}")

    monkeypatch.setattr(asyncpg, "connect", fake_connect)

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    suggestions = await collector.collect_missing_index_suggestions()

    assert len(suggestions) == 1
    assert suggestions[0].database_name == "db_b"


async def test_collect_missing_index_suggestions_raises_value_error_without_instance_id() -> None:
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError, match="instance_id"):
        await collector.collect_missing_index_suggestions()


class _FakeArchiverConnection:
    """Fake de conexao para `collect_backup_status()` — `fetchrow()` devolve a linha fixa de
    `pg_stat_archiver` (sempre exatamente 1 linha, cluster-wide)."""

    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row
        self.closed = False

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any]:
        if "pg_stat_archiver" in query:
            return self._row
        raise AssertionError(f"fetchrow inesperado: {query}")

    async def close(self) -> None:
        self.closed = True


_DEFAULT_ARCHIVED_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _archiver_row(
    archived_count: int = 42,
    last_archived_wal: str | None = "000000010000000000000005",
    last_archived_time: datetime | None = _DEFAULT_ARCHIVED_TIME,
    failed_count: int = 0,
    last_failed_wal: str | None = None,
    last_failed_time: datetime | None = None,
    stats_reset: datetime | None = _DEFAULT_ARCHIVED_TIME,
) -> dict[str, Any]:
    """`last_archived_time`/`stats_reset` default a um timestamp fixo (nao `None`) — passar
    `None` explicitamente nos testes representa de verdade "arquivamento nunca rodou", nao "usa
    o padrao"."""
    return {
        "archived_count": archived_count,
        "last_archived_wal": last_archived_wal,
        "last_archived_time": last_archived_time,
        "failed_count": failed_count,
        "last_failed_wal": last_failed_wal,
        "last_failed_time": last_failed_time,
        "stats_reset": stats_reset,
    }


async def test_collect_backup_status_uses_last_archived_time_as_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`pg_stat_archiver` e cluster-wide (nao por banco) — 1 unica linha, `database_name=None`.
    `last_backup_at` e um PROXY de saude de WAL archiving, nao um backup de fato."""
    archived_at = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
    fake_conn = _FakeArchiverConnection(
        _archiver_row(archived_count=100, last_archived_time=archived_at)
    )
    _patch_connect(monkeypatch, fake_conn)  # type: ignore[arg-type]

    instance_id = uuid4()
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=instance_id)
    rows = await collector.collect_backup_status()

    assert len(rows) == 1
    row = rows[0]
    assert row.instance_id == instance_id
    assert row.database_name is None
    assert row.last_backup_at == archived_at
    assert row.detail["archived_count"] == 100
    assert fake_conn.closed


async def test_collect_backup_status_handles_null_last_archived_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archiving desabilitado/nunca rodou — `last_archived_time` pode ser `None`."""
    fake_conn = _FakeArchiverConnection(
        _archiver_row(archived_count=0, last_archived_wal=None, last_archived_time=None)
    )
    _patch_connect(monkeypatch, fake_conn)  # type: ignore[arg-type]

    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=uuid4())
    rows = await collector.collect_backup_status()

    assert rows[0].last_backup_at is None


async def test_collect_backup_status_raises_value_error_without_instance_id() -> None:
    collector = PostgreSQLCollector(connection=_CONNECTION, instance_id=None)
    with pytest.raises(ValueError, match="instance_id"):
        await collector.collect_backup_status()
