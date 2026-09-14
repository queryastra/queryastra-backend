"""Adaptador PostgreSQL. Fonte de verdade: `Docs/collector-queries.md`.

Usa `asyncpg` (Docs/database-schema.md, ARCHITECTURE_SPEC.md §4) como driver assincrono.

`check_capabilities()` abre uma conexao curta (timeout configuravel) e checa, via
`pg_has_role`, se o usuario de coleta tem as roles minimas confirmadas em
`Docs/collector-queries.md` §7 (`pg_read_all_stats`, `pg_read_all_settings`,
`pg_signal_backend`) — mandatorias — e a disponibilidade opcional de `pg_stat_statements`
("Top SQL"), que so degrada, nunca reprova o cadastro sozinha.

`collect()` executa um ciclo real de coleta somente leitura, com timeout por query
(`settings.collection_query_timeout_seconds`) e limite de linhas truncado em Python apos o
fetch (`settings.collection_max_rows`) — nunca DDL/DML:
    - `pg_stat_activity` (WHERE state != 'idle') — sessao/query ativa (collector-queries.md §1),
      normalizada em `NormalizedQuerySample` (mascarada via `mask_sql_literals` antes do hash).
    - `pg_blocking_pids(pid)` — deteccao de bloqueio (collector-queries.md §2); devolve o fecho
      TRANSITIVO de bloqueadores, reduzido pro(s) bloqueador(es) DIRETO(s) por
      `_reduce_to_direct_edges` antes de virar `NormalizedBlockingEvent` — a arvore completa
      (Etapa 9) e reconstruida no app encadeando essas arestas diretas de um mesmo snapshot
      (`app.api.v1.waits_blocking`), nao via CTE recursiva ao vivo. `wait_time_ms` e uma
      aproximacao (`collected_at - query_start` da sessao bloqueada, ou `0`) — Postgres nao expoe
      duracao nativa de espera de lock. `_ALL_SESSIONS_QUERY` identifica bloqueadores OCIOSOS
      (sem `state` ativo); `_BLOCKED_OBJECT_QUERY` (`pg_locks`) resolve o objeto bloqueado.
    - `pg_stat_database` (agregado via SUM, cluster inteiro — `ConnectionParams` nao carrega
      banco especifico) e `pg_stat_bgwriter` (< PG17) / `pg_stat_checkpointer` (>= PG17) —
      metricas de recurso/instancia (collector-queries.md §5). Versao e detectada a cada ciclo
      dentro de `collect()` (via `server_version_num`), nunca persistida em `Instance` (corte de
      escopo desta feature — `engine_version*` fica para o health-check de cadastro).
    - `pg_stat_statements` (extensao opcional) — Top SQL agregado: **nao implementado** em
      `collect()` nesta feature (so a capacidade e checada em `check_capabilities()`).
    - Detalhe de deadlock: **nao disponivel via SQL** neste motor — so
      `pg_stat_database.deadlocks` (contador agregado, coletado como metrica); detalhe fica em
      log do servidor (collector-queries.md §4) — `deadlock_events` fora de escopo (corte).

Permissao minima esperada: role `pg_read_all_stats`/`pg_read_all_settings`/`pg_signal_backend`
(ou superuser) — collector-queries.md §7.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    POSTGRESQL_CONFIGURATION_VISIBILITY,
    POSTGRESQL_SESSION_ACTIVITY,
    POSTGRESQL_SESSION_TERMINATION,
    POSTGRESQL_TOP_SQL,
)
from app.collectors.connection import ConnectionParams
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals
from app.normalizers.metric_names import (
    PG_ACTIVE_SESSION_COUNT,
    PG_BLKS_HIT,
    PG_BLKS_READ,
    PG_CHECKPOINT_BUFFERS_WRITTEN,
    PG_CHECKPOINT_SYNC_TIME_MS,
    PG_CHECKPOINT_WRITE_TIME_MS,
    PG_CHECKPOINTS_REQUESTED,
    PG_CHECKPOINTS_TIMED,
    PG_DEADLOCKS,
    PG_NUMBACKENDS,
    PG_TEMP_BYTES,
    PG_TEMP_FILES,
    PG_TUP_DELETED,
    PG_TUP_FETCHED,
    PG_TUP_INSERTED,
    PG_TUP_RETURNED,
    PG_TUP_UPDATED,
    PG_XACT_COMMIT,
    PG_XACT_ROLLBACK,
)
from app.normalizers.models import (
    NormalizedBackupStatus,
    NormalizedBlockingEvent,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedQuerySample,
)
from app.normalizers.wait_taxonomy import map_wait_event

logger = get_logger(__name__)

# `track_activity_query_size` minimo para nao truncar query capturada demais cedo
# (default do Postgres e 1024 — Docs/collector-queries.md §7).
_MIN_TRACK_ACTIVITY_QUERY_SIZE = 4096

# PG17 substitui `pg_stat_bgwriter.checkpoints_*` por `pg_stat_checkpointer` (colunas renomeadas
# `num_timed`/`num_requested`/`write_time`/`sync_time`/`buffers_written`).
_PG17_VERSION_NUM = 170000

_SESSION_ACTIVITY_QUERY = """
    SELECT pid, usename, datname, application_name, client_addr, backend_start, xact_start,
           query_start, state_change, wait_event_type, wait_event, state, backend_type, query
    FROM pg_stat_activity
    WHERE state != 'idle'
"""

_BLOCKING_QUERY = """
    SELECT pid, pg_blocking_pids(pid) AS blocked_by, query
    FROM pg_stat_activity
    WHERE cardinality(pg_blocking_pids(pid)) > 0
"""

# Etapa 9 (arvore completa de bloqueio) — identidade de TODAS as sessoes conectadas, sem o filtro
# `state != 'idle'` de `_SESSION_ACTIVITY_QUERY`. Necessaria pro "bloqueador ocioso": uma sessao
# com transacao aberta segurando lock sem estar rodando nada (`state = 'idle in transaction'` ou,
# no caso raro de lock preservado por outro motivo, mesmo `'idle'`) nao aparece em
# `_SESSION_ACTIVITY_QUERY` — sem isso, ficaria sem usuario/programa/maquina na arvore.
_ALL_SESSIONS_QUERY = """
    SELECT pid, usename, application_name, client_addr
    FROM pg_stat_activity
"""

# Etapa 9 — recurso/objeto bloqueado. `pg_locks` e uma view do sistema (sem role especial
# exigida) — `NOT granted` filtra so os PEDIDOS de lock ainda esperando (o pid bloqueado). `mode`
# e o modo de lock pedido (ex. "ShareLock"); `relation::regclass` resolve o nome qualificado da
# tabela quando o lock e sobre uma relacao (NULL pra locktype sem relacao associada, ex.
# transactionid/tuple/advisory — `blocked_object` fica NULL nesse caso, nao e erro).
_BLOCKED_OBJECT_QUERY = """
    SELECT pid, locktype, mode, relation::regclass::text AS blocked_object
    FROM pg_locks
    WHERE NOT granted
"""

_DATABASE_STATS_QUERY = """
    SELECT sum(numbackends) AS numbackends, sum(xact_commit) AS xact_commit,
           sum(xact_rollback) AS xact_rollback, sum(blks_read) AS blks_read,
           sum(blks_hit) AS blks_hit, sum(tup_returned) AS tup_returned,
           sum(tup_fetched) AS tup_fetched, sum(tup_inserted) AS tup_inserted,
           sum(tup_updated) AS tup_updated, sum(tup_deleted) AS tup_deleted,
           sum(deadlocks) AS deadlocks, sum(temp_files) AS temp_files,
           sum(temp_bytes) AS temp_bytes
    FROM pg_stat_database
"""

_BGWRITER_QUERY = """
    SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time,
           buffers_checkpoint
    FROM pg_stat_bgwriter
"""

_CHECKPOINTER_QUERY = """
    SELECT num_timed, num_requested, write_time, sync_time, buffers_written
    FROM pg_stat_checkpointer
"""

# `datallowconn=false` (ex.: templates) nunca aceitaria conexao; `datistemplate=false` exclui
# template0/template1 mesmo que `datallowconn` estivesse true por engano de configuracao.
_LIST_DATABASES_QUERY = """
    SELECT datname FROM pg_database
    WHERE datistemplate = false AND datallowconn = true
"""

# Heuristica (Docs/collector-queries.md §10) — sem DMV nativa equivalente ao
# `sys.dm_db_missing_index_details` do SQL Server. `pg_stat_user_tables` e escopada ao banco
# CONECTADO (ao contrario de `pg_stat_activity`/`pg_stat_database`, cluster-wide) — por isso
# `collect_missing_index_suggestions()` itera por banco em vez de rodar uma unica vez.
_MISSING_INDEX_HEURISTIC_QUERY = """
    SELECT schemaname, relname AS table_name,
           seq_scan, idx_scan, n_live_tup,
           pg_relation_size(relid) AS table_size_bytes
    FROM pg_stat_user_tables
    WHERE seq_scan > COALESCE(idx_scan, 0)
      AND pg_relation_size(relid) > $1
      AND seq_scan > $2
    ORDER BY seq_scan - COALESCE(idx_scan, 0) DESC
"""

# `pg_stat_archiver` sempre devolve exatamente 1 linha (cluster-wide, nao por banco) — mesmo com
# archiving desabilitado (colunas ficam NULL nesse caso). Docs/collector-queries.md §6.
_ARCHIVER_STATUS_QUERY = """
    SELECT archived_count, last_archived_wal, last_archived_time,
           failed_count, last_failed_wal, last_failed_time, stats_reset
    FROM pg_stat_archiver
"""


def _reduce_to_direct_edges(blocked_by_map: dict[int, set[int]]) -> dict[int, set[int]]:
    """Reduz o fecho transitivo de `pg_blocking_pids()` pro(s) bloqueador(es) DIRETO(s).

    `pg_blocking_pids(pid)` documenta devolver todo mundo "a frente" de `pid` na fila de espera,
    nao so quem bloqueia `pid` imediatamente — se A bloqueia B e B bloqueia C,
    `blocked_by_map[C] == {A, B}`, nao so `{B}`. Um bloqueador `b` e mantido em `blockers(w)` so
    se `b` NAO estiver, ele mesmo, no conjunto de bloqueadores de outro `b2` tambem presente em
    `blockers(w)` — nesse caso `b` so bloqueia `w` INDIRETAMENTE (via `b2`), entao e removido,
    sobrando so o(s) bloqueador(es) mais proximo(s). Sem essa reducao, a arvore reconstruida em
    `app.api.v1.waits_blocking` ligaria C tanto em A quanto em B, achatando/duplicando a cadeia
    real A -> B -> C.
    """
    direct: dict[int, set[int]] = {}
    for waiter, blockers in blocked_by_map.items():
        direct[waiter] = {
            b
            for b in blockers
            if not any(b in blocked_by_map.get(b2, ()) for b2 in blockers if b2 != b)
        }
    return direct


class PostgreSQLCollector:
    """Implementa `app.collectors.base.Collector` para instancias PostgreSQL."""

    def __init__(self, connection: ConnectionParams, instance_id: UUID | None = None) -> None:
        self._connection_params = connection
        self._instance_id = instance_id

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Conecta e checa roles (`pg_has_role`) e extensao `pg_stat_statements`.

        Conexao com timeout (`Docs/rules/02-safe-collection.md`); qualquer erro de
        conectividade vira uma unica `CapabilityResult(CONNECTIVITY, UNAVAILABLE, ...)` — nunca
        a mensagem bruta da excecao, so o nome da classe (pode conter host/senha no texto
        default do driver).
        """
        try:
            conn = await asyncio.wait_for(
                self._connect(), timeout=self._connection_params.connect_timeout_seconds
            )
        except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
            return [
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=f"Falha ao conectar: {type(exc).__name__}",
                )
            ]

        try:
            return await self._check_roles_and_extensions(conn)
        finally:
            await conn.close()

    async def _connect(self, database: str = "postgres") -> asyncpg.Connection:
        """`database` default ("postgres", banco de manutencao) serve pra `check_capabilities()`/
        `collect()`, que so leem views cluster-wide (`pg_stat_activity`/`pg_stat_database`) — o
        banco conectado nao importa pra elas. `collect_missing_index_suggestions()` passa um
        banco real explicito (`pg_stat_user_tables` e escopada ao banco conectado)."""
        params = self._connection_params
        connect_kwargs: dict[str, object] = {
            "host": params.host,
            "port": params.port,
            "user": params.username,
            "database": database,
            "timeout": params.connect_timeout_seconds,
        }
        # `auth_method == "integrated"`: nao passamos `password` — a conexao depende de
        # confianca do ambiente (ex.: `pg_hba.conf` com `trust`/peer, ou certificado de cliente
        # ja provisionado fora deste adaptador). Caminho ainda nao totalmente resolvido no MVP
        # (nao ha integracao Kerberos/GSSAPI aqui) mas nao deve quebrar: o `asyncpg.connect`
        # segue sem senha, e o servidor decide se aceita.
        if params.auth_method != "integrated":
            connect_kwargs["password"] = params.secret_value
        return await asyncpg.connect(**connect_kwargs)

    async def _check_roles_and_extensions(self, conn: asyncpg.Connection) -> list[CapabilityResult]:
        results: list[CapabilityResult] = []

        has_session_activity = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_read_all_stats', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_SESSION_ACTIVITY,
                status=CapabilityStatus.FULL
                if has_session_activity
                else CapabilityStatus.UNAVAILABLE,
                reason=None if has_session_activity else "role pg_read_all_stats ausente",
            )
        )

        has_configuration_visibility = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_read_all_settings', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_CONFIGURATION_VISIBILITY,
                status=(
                    CapabilityStatus.FULL
                    if has_configuration_visibility
                    else CapabilityStatus.UNAVAILABLE
                ),
                reason=None
                if has_configuration_visibility
                else "role pg_read_all_settings ausente",
            )
        )

        has_session_termination = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_signal_backend', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_SESSION_TERMINATION,
                status=(
                    CapabilityStatus.FULL
                    if has_session_termination
                    else CapabilityStatus.UNAVAILABLE
                ),
                reason=None if has_session_termination else "role pg_signal_backend ausente",
            )
        )

        results.append(await self._check_top_sql(conn))
        return results

    async def _check_top_sql(self, conn: asyncpg.Connection) -> CapabilityResult:
        has_extension = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')"
        )
        # `current_setting()` devolve o valor "bonito" com unidade (ex.: "4kB") para GUCs com
        # unidade de exibicao — `pg_settings.setting` sempre devolve o numero cru (bytes),
        # seguro para castar direto em inteiro.
        query_size_ok = await conn.fetchval(
            "SELECT setting::int >= $1 FROM pg_settings WHERE name = 'track_activity_query_size'",
            _MIN_TRACK_ACTIVITY_QUERY_SIZE,
        )

        if has_extension and query_size_ok:
            return CapabilityResult(capability=POSTGRESQL_TOP_SQL, status=CapabilityStatus.FULL)

        missing = []
        if not has_extension:
            missing.append("extensao pg_stat_statements nao instalada")
        if not query_size_ok:
            missing.append(f"track_activity_query_size menor que {_MIN_TRACK_ACTIVITY_QUERY_SIZE}")
        return CapabilityResult(
            capability=POSTGRESQL_TOP_SQL,
            status=CapabilityStatus.DEGRADED,
            reason="; ".join(missing),
        )

    async def collect(self) -> CollectionResult:
        """Executa um ciclo de coleta: sessao ativa, bloqueio (nivel direto) e metricas de recurso.

        Ver `Docs/collector-queries.md` §1 (sessao ativa), §2 (bloqueio), §5 (metricas de
        recurso). Versao do engine e detectada a cada ciclo pra decidir entre `pg_stat_bgwriter`
        (< PG17) e `pg_stat_checkpointer` (>= PG17); tambem exposta em
        `CollectionResult.engine_version*` pra `PollingWorker` persistir de volta em
        `instances.engine_version*`.

        Requer `instance_id` (levanta `ValueError` se `None` — usado em teste de conexao
        pre-cadastro, onde `collect()` nao se aplica).
        """
        if self._instance_id is None:
            raise ValueError(
                "PostgreSQLCollector.collect requer instance_id de uma instancia ja cadastrada."
            )

        start = time.perf_counter()
        settings = get_settings()

        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            version_num = await self._fetch_version(conn, settings.collection_query_timeout_seconds)
            # `server_version_num` (PG10+, unico suportado formalmente - Q20): major*10000+minor
            # direto, sem componente de patch separado (ex.: 170000 -> "17.0", 160003 -> "16.3") —
            # confirmado contra pgpedia.info/documentacao oficial. Derivado do que ja foi
            # buscado, sem round-trip extra.
            engine_version = f"{version_num // 10000}.{version_num % 10000}"

            session_rows = await self._fetch_rows(
                conn, _SESSION_ACTIVITY_QUERY, settings.collection_query_timeout_seconds
            )
            # Carimbado logo apos o fetch de `pg_stat_activity` (nao antes de abrir a conexao,
            # como era antes) - e o instante que corresponde de fato ao `query_start` que acabamos
            # de ler dali. Carimbar antes da conexao/`_fetch_version` (que juntos levam ~100-300ms)
            # fazia `duration_ms = collected_at - query_start` sair negativo sempre que uma sessao
            # comecava uma query nesse intervalo (query_start ficava depois de collected_at).
            collected_at = self._now()
            session_rows = session_rows[: settings.collection_max_rows]

            blocking_rows = await self._fetch_rows(
                conn, _BLOCKING_QUERY, settings.collection_query_timeout_seconds
            )
            blocking_rows = blocking_rows[: settings.collection_max_rows]

            all_session_rows = await self._fetch_rows(
                conn, _ALL_SESSIONS_QUERY, settings.collection_query_timeout_seconds
            )
            all_session_rows = all_session_rows[: settings.collection_max_rows]

            blocked_object_rows = await self._fetch_rows(
                conn, _BLOCKED_OBJECT_QUERY, settings.collection_query_timeout_seconds
            )
            blocked_object_rows = blocked_object_rows[: settings.collection_max_rows]

            database_stats_row = await asyncio.wait_for(
                conn.fetchrow(_DATABASE_STATS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )

            if version_num >= _PG17_VERSION_NUM:
                checkpoint_row = await asyncio.wait_for(
                    conn.fetchrow(_CHECKPOINTER_QUERY),
                    timeout=settings.collection_query_timeout_seconds,
                )
            else:
                checkpoint_row = await asyncio.wait_for(
                    conn.fetchrow(_BGWRITER_QUERY),
                    timeout=settings.collection_query_timeout_seconds,
                )
        finally:
            await conn.close()

        query_samples = self._build_query_samples(session_rows, collected_at)
        blocking_events = self._build_blocking_events(
            session_rows, blocking_rows, all_session_rows, blocked_object_rows, collected_at
        )
        metric_samples = self._build_metric_samples(
            session_rows,
            database_stats_row,
            checkpoint_row,
            version_num,
            collected_at,
        )

        duration_ms = int((time.perf_counter() - start) * 1000)
        return CollectionResult(
            instance_id=self._instance_id,
            collected_at=collected_at,
            duration_ms=duration_ms,
            metric_samples=metric_samples,
            query_samples=query_samples,
            blocking_events=blocking_events,
            engine_version=engine_version,
            engine_version_major=version_num // 10000,
        )

    async def _fetch_version(self, conn: asyncpg.Connection, timeout_seconds: float) -> int:
        return await asyncio.wait_for(
            conn.fetchval("SELECT current_setting('server_version_num')::int"),
            timeout=timeout_seconds,
        )

    async def _fetch_rows(
        self, conn: asyncpg.Connection, query: str, timeout_seconds: float
    ) -> list[asyncpg.Record]:
        return await asyncio.wait_for(conn.fetch(query), timeout=timeout_seconds)

    def _build_query_samples(
        self, session_rows: list[asyncpg.Record], collected_at: datetime
    ) -> list[NormalizedQuerySample]:
        assert self._instance_id is not None  # garantido por collect()
        instance_id: UUID = self._instance_id
        samples: list[NormalizedQuerySample] = []
        for row in session_rows:
            query_text = row["query"]
            if not query_text:
                continue
            masked = mask_sql_literals(query_text)
            sql_hash = hashlib.sha256(masked.encode("utf-8")).hexdigest()
            query_start = row["query_start"]
            duration_ms = (
                # `max(0, ...)`: rede residual entre o servidor executar o snapshot de
                # `pg_stat_activity` e este processo carimbar `collected_at` ainda pode deixar
                # uma query iniciada nesse intervalo alguns ms "no futuro" - clamp evita duracao
                # negativa nesse caso raro (a causa principal do problema, o carimbo tirado antes
                # de sequer abrir a conexao, foi corrigida acima, movendo `collected_at` pra logo
                # apos este fetch).
                max(0, int((collected_at - query_start).total_seconds() * 1000))
                if query_start is not None
                else None
            )
            client_addr = row["client_addr"]
            samples.append(
                NormalizedQuerySample(
                    time=collected_at,
                    instance_id=instance_id,
                    engine="postgresql",
                    sql_hash=sql_hash,
                    sql_text_masked=masked,
                    # `pg_stat_activity.query` ja e so o statement atual (sem a distincao
                    # batch/procedure do SQL Server) — os dois campos coincidem aqui.
                    sql_text_full_masked=masked,
                    wait_type=map_wait_event("postgresql", row["wait_event_type"]),
                    wait_event_native=row["wait_event"],
                    # Postgres nao expoe duracao de espera do backend diretamente.
                    wait_time_ms=None,
                    duration_ms=duration_ms,
                    session_id=str(row["pid"]),
                    db_user=row["usename"],
                    program=row["application_name"],
                    database_name=row["datname"],
                    machine=str(client_addr) if client_addr is not None else None,
                )
            )
        return samples

    def _build_blocking_events(
        self,
        session_rows: list[asyncpg.Record],
        blocking_rows: list[asyncpg.Record],
        all_session_rows: list[asyncpg.Record],
        blocked_object_rows: list[asyncpg.Record],
        collected_at: datetime,
    ) -> list[NormalizedBlockingEvent]:
        """Constroi um evento por par (bloqueador DIRETO, bloqueado).

        `pg_blocking_pids(pid)` devolve o fecho TRANSITIVO de bloqueadores (toda a fila a frente,
        nao so o bloqueador imediato — confirmado na documentacao oficial: se A bloqueia B e B
        bloqueia C, `pg_blocking_pids(C)` devolve `{A, B}`, nao so `{B}`). `_reduce_to_direct_edges`
        reduz cada `blocked_by` pro(s) bloqueador(es) mais proximo(s) antes de gerar eventos — sem
        isso, a arvore reconstruida em `app.api.v1.waits_blocking` ficaria achatada/duplicada (C
        apareceria bloqueado por A E por B, em vez de A -> B -> C).

        `wait_time_ms` e uma APROXIMACAO: Postgres nao expoe duracao nativa de espera de lock,
        entao usamos `collected_at - query_start` da sessao bloqueada quando disponivel, ou `0`
        quando `query_start` e `None`. Identidade (usuario/programa/maquina) vem PRIMEIRO de
        `session_rows` (sessao ativa) e cai pra `all_session_rows` (qualquer sessao conectada)
        pro caso do bloqueador ocioso — mesmo padrao do adaptador SQL Server.
        """
        assert self._instance_id is not None  # garantido por collect()
        instance_id: UUID = self._instance_id
        sessions_by_pid = {row["pid"]: row for row in session_rows}
        identity_by_pid = {row["pid"]: row for row in all_session_rows}
        blocked_object_by_pid = {row["pid"]: row for row in blocked_object_rows}

        def _identity(pid: int) -> tuple[str | None, str | None, str | None]:
            active_row = sessions_by_pid.get(pid)
            row = active_row if active_row is not None else identity_by_pid.get(pid)
            if row is None:
                return None, None, None
            client_addr = row["client_addr"]
            return (
                row["usename"],
                row["application_name"],
                str(client_addr) if client_addr is not None else None,
            )

        blocked_by_map = {row["pid"]: set(row["blocked_by"]) for row in blocking_rows}
        direct_edges = _reduce_to_direct_edges(blocked_by_map)

        events: list[NormalizedBlockingEvent] = []
        for row in blocking_rows:
            blocked_pid = row["pid"]
            blocked_session = sessions_by_pid.get(blocked_pid)
            blocked_sql_hash = self._hash_query(
                blocked_session["query"] if blocked_session is not None else None
            )
            query_start = blocked_session["query_start"] if blocked_session is not None else None
            wait_time_ms = (
                # `max(0, ...)`: mesma razao do clamp em `_build_query_samples` — residual de rede
                # entre o snapshot no servidor e o carimbo de `collected_at` neste processo.
                max(0, int((collected_at - query_start).total_seconds() * 1000))
                if query_start is not None
                else 0
            )
            blocked_login, blocked_program, blocked_machine = _identity(blocked_pid)
            blocked_object_row = blocked_object_by_pid.get(blocked_pid)
            for blocker_pid in direct_edges.get(blocked_pid, set()):
                blocker_session = sessions_by_pid.get(blocker_pid)
                blocker_sql_hash = self._hash_query(
                    blocker_session["query"] if blocker_session is not None else None
                )
                blocker_login, blocker_program, blocker_machine = _identity(blocker_pid)
                events.append(
                    NormalizedBlockingEvent(
                        time=collected_at,
                        instance_id=instance_id,
                        blocker_session_id=str(blocker_pid),
                        blocked_session_id=str(blocked_pid),
                        blocker_sql_hash=blocker_sql_hash,
                        blocked_sql_hash=blocked_sql_hash,
                        wait_time_ms=wait_time_ms,
                        blocker_db_user=blocker_login,
                        blocker_program=blocker_program,
                        blocker_machine=blocker_machine,
                        blocker_wait_event_native=(
                            blocker_session["wait_event"] if blocker_session is not None else None
                        ),
                        blocked_db_user=blocked_login,
                        blocked_program=blocked_program,
                        blocked_machine=blocked_machine,
                        blocked_wait_event_native=(
                            blocked_session["wait_event"] if blocked_session is not None else None
                        ),
                        blocked_wait_resource=(
                            f"{blocked_object_row['locktype']}/{blocked_object_row['mode']}"
                            if blocked_object_row is not None
                            else None
                        ),
                        blocked_object=(
                            blocked_object_row["blocked_object"]
                            if blocked_object_row is not None
                            else None
                        ),
                    )
                )
        return events

    @staticmethod
    def _hash_query(query_text: str | None) -> str | None:
        if not query_text:
            return None
        return hashlib.sha256(mask_sql_literals(query_text).encode("utf-8")).hexdigest()

    def _build_metric_samples(
        self,
        session_rows: list[asyncpg.Record],
        database_stats_row: asyncpg.Record,
        checkpoint_row: asyncpg.Record,
        version_num: int,
        collected_at: datetime,
    ) -> list[NormalizedMetricSample]:
        assert self._instance_id is not None  # garantido por collect()
        instance_id: UUID = self._instance_id
        labels: dict[str, str] = {}

        def sample(metric_name: str, value: float | None) -> NormalizedMetricSample | None:
            if value is None:
                return None
            return NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=metric_name,
                value=float(value),
                labels=labels,
            )

        candidates = [
            sample(PG_ACTIVE_SESSION_COUNT, len(session_rows)),
            sample(PG_NUMBACKENDS, database_stats_row["numbackends"]),
            sample(PG_XACT_COMMIT, database_stats_row["xact_commit"]),
            sample(PG_XACT_ROLLBACK, database_stats_row["xact_rollback"]),
            sample(PG_BLKS_READ, database_stats_row["blks_read"]),
            sample(PG_BLKS_HIT, database_stats_row["blks_hit"]),
            sample(PG_TUP_RETURNED, database_stats_row["tup_returned"]),
            sample(PG_TUP_FETCHED, database_stats_row["tup_fetched"]),
            sample(PG_TUP_INSERTED, database_stats_row["tup_inserted"]),
            sample(PG_TUP_UPDATED, database_stats_row["tup_updated"]),
            sample(PG_TUP_DELETED, database_stats_row["tup_deleted"]),
            sample(PG_DEADLOCKS, database_stats_row["deadlocks"]),
            sample(PG_TEMP_FILES, database_stats_row["temp_files"]),
            sample(PG_TEMP_BYTES, database_stats_row["temp_bytes"]),
        ]

        if version_num >= _PG17_VERSION_NUM:
            candidates.extend(
                [
                    sample(PG_CHECKPOINTS_TIMED, checkpoint_row["num_timed"]),
                    sample(PG_CHECKPOINTS_REQUESTED, checkpoint_row["num_requested"]),
                    sample(PG_CHECKPOINT_WRITE_TIME_MS, checkpoint_row["write_time"]),
                    sample(PG_CHECKPOINT_SYNC_TIME_MS, checkpoint_row["sync_time"]),
                    sample(PG_CHECKPOINT_BUFFERS_WRITTEN, checkpoint_row["buffers_written"]),
                ]
            )
        else:
            candidates.extend(
                [
                    sample(PG_CHECKPOINTS_TIMED, checkpoint_row["checkpoints_timed"]),
                    sample(PG_CHECKPOINTS_REQUESTED, checkpoint_row["checkpoints_req"]),
                    sample(PG_CHECKPOINT_WRITE_TIME_MS, checkpoint_row["checkpoint_write_time"]),
                    sample(PG_CHECKPOINT_SYNC_TIME_MS, checkpoint_row["checkpoint_sync_time"]),
                    sample(PG_CHECKPOINT_BUFFERS_WRITTEN, checkpoint_row["buffers_checkpoint"]),
                ]
            )

        return [c for c in candidates if c is not None]

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        """Heuristica de indice ausente (Docs/collector-queries.md §10) — itera por banco do
        cluster (`pg_stat_user_tables` e escopada ao banco conectado), nao so o banco de
        manutencao usado por `collect()`. Erro num banco especifico (permissao, timeout) e
        logado e pulado — nunca aborta a checagem dos demais bancos."""
        if self._instance_id is None:
            raise ValueError(
                "PostgreSQLCollector.collect_missing_index_suggestions requer instance_id de "
                "uma instancia ja cadastrada."
            )
        instance_id: UUID = self._instance_id
        settings = get_settings()

        bootstrap_conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            database_rows = await self._fetch_rows(
                bootstrap_conn, _LIST_DATABASES_QUERY, settings.collection_query_timeout_seconds
            )
        finally:
            await bootstrap_conn.close()

        suggestions: list[NormalizedMissingIndexSuggestion] = []
        for db_row in database_rows:
            database_name = db_row["datname"]
            try:
                db_conn = await asyncio.wait_for(
                    self._connect(database=database_name),
                    timeout=self._connection_params.connect_timeout_seconds,
                )
            except (OSError, asyncpg.PostgresError, TimeoutError):
                logger.warning(
                    "Falha ao conectar ao banco %s (instance_id=%s) para checagem de indice "
                    "ausente - pulando",
                    database_name,
                    instance_id,
                )
                continue
            try:
                rows = await asyncio.wait_for(
                    db_conn.fetch(
                        _MISSING_INDEX_HEURISTIC_QUERY,
                        settings.missing_index_pg_min_table_size_bytes,
                        settings.missing_index_pg_min_seq_scans,
                    ),
                    timeout=settings.collection_query_timeout_seconds,
                )
            except (asyncpg.PostgresError, TimeoutError):
                logger.warning(
                    "Falha ao rodar heuristica de indice ausente no banco %s "
                    "(instance_id=%s) - pulando",
                    database_name,
                    instance_id,
                )
                continue
            finally:
                await db_conn.close()

            for row in rows[: settings.collection_max_rows]:
                seq_scan = row["seq_scan"]
                idx_scan = row["idx_scan"] or 0
                suggestions.append(
                    NormalizedMissingIndexSuggestion(
                        instance_id=instance_id,
                        database_name=database_name,
                        table_name=f"{row['schemaname']}.{row['table_name']}",
                        suggestion=(
                            f"Candidato a indice (heuristica, nao indica coluna): "
                            f"{seq_scan} sequential scans vs {idx_scan} index scans"
                        ),
                        impact_score=float(seq_scan - idx_scan),
                        detail={
                            "seq_scan": seq_scan,
                            "idx_scan": row["idx_scan"],
                            "n_live_tup": row["n_live_tup"],
                            "table_size_bytes": row["table_size_bytes"],
                        },
                    )
                )
        return suggestions

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        """`pg_stat_archiver` (Docs/collector-queries.md §6) — cluster-wide, 1 unica linha,
        `database_name=None`. `last_backup_at = last_archived_time`: PROXY de saude de WAL
        archiving, NAO um backup de fato (Postgres nao tem catalogo nativo de backup completo
        como o `msdb` do SQL Server)."""
        if self._instance_id is None:
            raise ValueError(
                "PostgreSQLCollector.collect_backup_status requer instance_id de uma instancia "
                "ja cadastrada."
            )
        instance_id: UUID = self._instance_id
        settings = get_settings()

        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            row = await asyncio.wait_for(
                conn.fetchrow(_ARCHIVER_STATUS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedBackupStatus(
                instance_id=instance_id,
                database_name=None,
                last_backup_at=row["last_archived_time"],
                detail={
                    "archived_count": row["archived_count"],
                    "last_archived_wal": row["last_archived_wal"],
                    "failed_count": row["failed_count"],
                    "last_failed_wal": row["last_failed_wal"],
                    "last_failed_time": row["last_failed_time"].isoformat()
                    if row["last_failed_time"] is not None
                    else None,
                    "stats_reset": row["stats_reset"].isoformat()
                    if row["stats_reset"] is not None
                    else None,
                },
            )
        ]

    async def close(self) -> None:
        """Sem estado persistente entre chamadas (cada `check_capabilities` fecha sua conexao)."""
        logger.info("close chamado para instancia postgresql (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
