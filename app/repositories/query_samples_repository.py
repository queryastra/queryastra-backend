"""Repositorio da hypertable `query_samples`.

`query_samples` (`Docs/database-schema.md` §3) e hypertable, criada por
`migrations/versions/66ae4664300c_hypertables.py`. Antes de inserir, todo texto de SQL ja deve
ter passado por `app.masking.sql_masking.mask_sql_literals` (feito pelo coletor, ao normalizar
`NormalizedQuerySample`) e pela checagem de `sql_exclusions` (`Docs/rules/03-sql-privacy.md`:
"Aplicar exclusoes por hash, programa ou usuario antes de gravar `query_samples`."), feita aqui.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

import sqlalchemy as sa

from app.normalizers.models import NormalizedQuerySample
from app.repositories._tables import query_samples_table, sql_exclusions_table
from app.repositories.base import BaseRepository


class QuerySamplesRepository(BaseRepository):
    """Persistencia de `NormalizedQuerySample` na hypertable `query_samples`.

    Regras de `sql_exclusions`: `instance_id IS NULL` vale para todas as instancias; com
    `instance_id` preenchido, so para aquela instancia. Comparacao de `match_value` e sempre
    exata (sem regex/glob) contra `sql_hash`/`program`/`db_user` conforme `match_type`.
    """

    async def insert_batch(self, samples: list[NormalizedQuerySample]) -> None:
        if not samples:
            return

        instance_ids = {s.instance_id for s in samples}
        result = await self._session.execute(
            sa.select(
                sql_exclusions_table.c.instance_id,
                sql_exclusions_table.c.match_type,
                sql_exclusions_table.c.match_value,
            ).where(
                sa.or_(
                    sql_exclusions_table.c.instance_id.in_(instance_ids),
                    sql_exclusions_table.c.instance_id.is_(None),
                )
            )
        )
        exclusions = list(result.all())

        def is_excluded(sample: NormalizedQuerySample) -> bool:
            for instance_id, match_type, match_value in exclusions:
                if instance_id is not None and instance_id != sample.instance_id:
                    continue
                if match_type == "sql_hash" and sample.sql_hash == match_value:
                    return True
                if match_type == "program_name" and sample.program == match_value:
                    return True
                if match_type == "db_user" and sample.db_user == match_value:
                    return True
            return False

        rows = [
            {
                "time": s.time,
                "instance_id": s.instance_id,
                "sql_hash": s.sql_hash,
                "sql_name": s.sql_name,
                "sql_text_masked": s.sql_text_masked,
                "sql_text_encrypted": s.sql_text_encrypted,
                "wait_type": s.wait_type.value if s.wait_type is not None else None,
                "wait_event_native": s.wait_event_native,
                "wait_time_ms": s.wait_time_ms,
                "duration_ms": s.duration_ms,
                "session_id": s.session_id,
                "db_user": s.db_user,
                "program": s.program,
                "database_name": s.database_name,
                "machine": s.machine,
            }
            for s in samples
            if not is_excluded(s)
        ]
        if not rows:
            return
        await self._session.execute(sa.insert(query_samples_table), rows)
        await self._session.commit()

    async def list_active(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Amostras brutas de `query_samples` no periodo — leitura direta da hypertable, os
        continuous aggregates (`query_stats_hourly`) estao vazios e nunca atualizados."""
        stmt = (
            sa.select(query_samples_table)
            .where(
                query_samples_table.c.instance_id == instance_id,
                query_samples_table.c.time >= since,
                query_samples_table.c.time <= until,
            )
            .order_by(query_samples_table.c.time.desc())
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).fetchall())

    async def top_sql(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        database_name: str | None = None,
    ) -> list[sa.Row]:
        """Agrega `query_samples` bruto por `sql_hash`: execution_count/total/avg via GROUP BY;
        sql_name/sql_text_masked/database_name da linha mais recente por hash (DISTINCT ON); wait
        predominante = moda de wait_type por hash (window function). SQL literal via `sa.text`
        (mais legivel que expression builder para DISTINCT ON + moda).

        `database_name` (nivel 2 do drill-down hierarquico: Instancia -> Banco -> Query) filtra
        as amostras a um unico banco quando informado; `None` (default) mantem o comportamento
        historico de agregar todos os bancos juntos sob o mesmo `sql_hash` — o filtro e aplicado
        via `:database_name IS NULL OR database_name = :database_name` nas 3 CTEs, sempre com
        `database_name` no dict de params (valor `None` quando nao ha filtro), em vez de montar
        SQL condicionalmente."""
        stmt = sa.text("""
            WITH agg AS (
                SELECT sql_hash, count(*) AS execution_count,
                       coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms,
                       coalesce(avg(wait_time_ms), 0) AS avg_wait_time_ms
                FROM query_samples
                WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                    AND (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
                GROUP BY sql_hash
            ),
            latest AS (
                SELECT DISTINCT ON (sql_hash) sql_hash, sql_name, sql_text_masked, database_name
                FROM query_samples
                WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                    AND (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
                ORDER BY sql_hash, time DESC
            ),
            wait_counts AS (
                SELECT sql_hash, wait_type, count(*) AS wait_count
                FROM query_samples
                WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                    AND wait_type IS NOT NULL
                    AND (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
                GROUP BY sql_hash, wait_type
            ),
            wait_mode AS (
                SELECT sql_hash, wait_type,
                       row_number() OVER (PARTITION BY sql_hash ORDER BY wait_count DESC) AS rn
                FROM wait_counts
            )
            SELECT agg.sql_hash, latest.sql_name, latest.sql_text_masked, latest.database_name,
                   agg.execution_count, agg.total_wait_time_ms, agg.avg_wait_time_ms,
                   wait_mode.wait_type AS predominant_wait_type
            FROM agg
            JOIN latest ON latest.sql_hash = agg.sql_hash
            LEFT JOIN wait_mode ON wait_mode.sql_hash = agg.sql_hash AND wait_mode.rn = 1
            ORDER BY agg.total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "database_name": database_name,
            },
        )
        return list(result.fetchall())

    async def top_sql_by_database(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por `database_name` (nivel 1 do drill-down hierarquico:
        Instancia -> Banco -> Query). Mesmo padrao de CTE de `top_sql()`, mas agrupando por
        `database_name` em vez de `sql_hash`. Linhas com `database_name` NULL sao agrupadas sob a
        label 'desconhecido' (COALESCE), nunca descartadas."""
        stmt = sa.text("""
            SELECT
                COALESCE(database_name, 'desconhecido') AS database_name,
                count(*) AS execution_count,
                coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms,
                coalesce(avg(wait_time_ms), 0) AS avg_wait_time_ms
            FROM query_samples
            WHERE instance_id = :instance_id AND time >= :since AND time <= :until
            GROUP BY COALESCE(database_name, 'desconhecido')
            ORDER BY total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt, {"instance_id": instance_id, "since": since, "until": until, "limit": limit}
        )
        return list(result.fetchall())

    async def top_wait_types(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por `wait_event_native` — dimensao "Waits" do drill-down
        (irma de "Bancos de dados"/"SQL": `top_sql_by_database`/`top_sql`), folha da propria
        dimensao (um tipo de espera nao se decompoe em mais nada, ao contrario de banco/query, que
        se decompoem por wait nativo). `wait_event_native` NULL vira 'desconhecido' (COALESCE),
        nunca descartado."""
        stmt = sa.text("""
            SELECT
                COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                count(*) AS execution_count,
                coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms,
                coalesce(avg(wait_time_ms), 0) AS avg_wait_time_ms
            FROM query_samples
            WHERE instance_id = :instance_id AND time >= :since AND time <= :until
            GROUP BY COALESCE(wait_event_native, 'desconhecido')
            ORDER BY total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt, {"instance_id": instance_id, "since": since, "until": until, "limit": limit}
        )
        return list(result.fetchall())

    async def wait_breakdown_by_database(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`database_name`, `wait_event_native`) — decomposicao do
        wait time de cada banco (nivel 1 do drill-down) pelo tipo de espera NATIVO do engine (ex.:
        `PAGEIOLATCH_SH`, `ASYNC_NETWORK_IO` no SQL Server; nomes de `pg_stat_activity.wait_event`
        no PostgreSQL) — granularidade maior que os 9 buckets canonicos cross-engine de
        `wait_type` (usados em `wait_summary()`), a pedido explicito de ver o wait nativo por
        banco, nao so a categoria agregada. `database_name`/`wait_event_native`
        NULL viram 'desconhecido' (COALESCE), nunca descartados — mesmo padrao de
        `top_sql_by_database`."""
        stmt = sa.text("""
            SELECT
                COALESCE(database_name, 'desconhecido') AS database_name,
                COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms
            FROM query_samples
            WHERE instance_id = :instance_id AND time >= :since AND time <= :until
            GROUP BY
                COALESCE(database_name, 'desconhecido'),
                COALESCE(wait_event_native, 'desconhecido')
            ORDER BY database_name, total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt, {"instance_id": instance_id, "since": since, "until": until, "limit": limit}
        )
        return list(result.fetchall())

    async def wait_breakdown_by_sql_hash(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        database_name: str | None = None,
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`sql_hash`, `wait_event_native`) — decomposicao do wait
        time de cada query (nivel 2 do drill-down) pelo tipo de espera NATIVO do engine. Mesma
        ideia de `wait_breakdown_by_database`, granularidade `sql_hash` em vez de
        `database_name`; aceita o mesmo filtro opcional `database_name` de `top_sql()` (nivel 2
        so quer a decomposicao das queries daquele banco)."""
        stmt = sa.text("""
            SELECT
                sql_hash,
                COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms
            FROM query_samples
            WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                AND (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
            GROUP BY sql_hash, COALESCE(wait_event_native, 'desconhecido')
            ORDER BY sql_hash, total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "database_name": database_name,
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_database(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: Literal["day", "hour"] = "day",
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`bucket` de tempo, `database_name`) — visao historica
        "Tudo" do nivel 1 do drill-down quando `bucket="day"` (um dia calendario por barra,
        equivalente ao grafico multi-dia do "Top SQL" de ferramentas de referencia como o
        SolarWinds DPA), ou o detalhamento por hora DENTRO de um dia especifico quando
        `bucket="hour"` (mesmo `since`/`until` do filtro de um dia unico). `bucket` sempre em UTC
        (`time AT TIME ZONE 'UTC'`), independente do timezone de sessao do Postgres. O `AT TIME
        ZONE 'UTC'` externo (envolvendo o `date_trunc` inteiro) converte o timestamp NAIVE de
        volta pra `timestamptz` — sem isso, asyncpg devolve um datetime Python naive e o FastAPI
        serializa sem sufixo de fuso (`"2026-09-11T14:00:00"`, sem `Z`/offset), o que faz
        `new Date(...)` no navegador interpretar o instante como fuso LOCAL em vez de UTC (bug
        real encontrado ao implementar o `HourBarChart` do frontend — corrigido aqui, na fonte).
        Sem filtro de `since`/`until` restritivo o suficiente (bucket="day" tipicamente varre
        tudo), isto pode varrer o historico inteiro de `query_samples` (sem retention policy
        hoje) — aceitavel no volume atual, mas pode exigir paginacao/particionamento se o
        historico crescer muito."""
        stmt = sa.text("""
            SELECT
                (date_trunc(:bucket, time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS bucket_start,
                COALESCE(database_name, 'desconhecido') AS database_name,
                coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms
            FROM query_samples
            WHERE instance_id = :instance_id AND time >= :since AND time <= :until
            GROUP BY
                date_trunc(:bucket, time AT TIME ZONE 'UTC'),
                COALESCE(database_name, 'desconhecido')
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_sql_hash(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        database_name: str | None = None,
        bucket: Literal["day", "hour"] = "day",
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`bucket` de tempo, `sql_hash`) — visao historica "Tudo" do
        nivel 2 do drill-down (`bucket="day"`) ou detalhamento por hora dentro de um dia
        (`bucket="hour"`). Mesmas ressalvas de `wait_time_by_bucket_and_database` (`bucket_start`
        convertido de volta pra `timestamptz` via `AT TIME ZONE 'UTC'` externo, pra sair com `Z`
        no JSON); aceita o mesmo filtro opcional `database_name` de
        `top_sql()`/`wait_breakdown_by_sql_hash()`."""
        stmt = sa.text("""
            SELECT
                (date_trunc(:bucket, time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS bucket_start,
                sql_hash,
                coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms
            FROM query_samples
            WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                AND (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
            GROUP BY date_trunc(:bucket, time AT TIME ZONE 'UTC'), sql_hash
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "database_name": database_name,
                "bucket": bucket,
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_wait_type(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: Literal["day", "hour"] = "day",
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`bucket` de tempo, `wait_event_native`) — visao historica
        "Tudo" da dimensao "Waits" (`bucket="day"`) ou detalhamento por hora dentro de um dia
        (`bucket="hour"`). Mesmas ressalvas de `wait_time_by_bucket_and_database` (`bucket_start`
        convertido de volta pra `timestamptz` via `AT TIME ZONE 'UTC'` externo)."""
        stmt = sa.text("""
            SELECT
                (date_trunc(:bucket, time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS bucket_start,
                COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms
            FROM query_samples
            WHERE instance_id = :instance_id AND time >= :since AND time <= :until
            GROUP BY
                date_trunc(:bucket, time AT TIME ZONE 'UTC'),
                COALESCE(wait_event_native, 'desconhecido')
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
            },
        )
        return list(result.fetchall())

    async def query_detail(
        self, instance_id: UUID, sql_hash: str, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Amostras brutas (nao agregadas) de um `sql_hash` especifico — nivel 3 (folha) do
        drill-down hierarquico. Usado para mostrar o historico de execucoes de uma query."""
        stmt = (
            sa.select(query_samples_table)
            .where(
                query_samples_table.c.instance_id == instance_id,
                query_samples_table.c.sql_hash == sql_hash,
                query_samples_table.c.time >= since,
                query_samples_table.c.time <= until,
            )
            .order_by(query_samples_table.c.time.desc())
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).fetchall())

    async def wait_summary(
        self, instance_id: UUID, since: datetime, until: datetime
    ) -> list[sa.Row]:
        stmt = (
            sa.select(
                query_samples_table.c.wait_type,
                sa.func.coalesce(sa.func.sum(query_samples_table.c.wait_time_ms), 0).label(
                    "total_wait_time_ms"
                ),
            )
            .where(
                query_samples_table.c.instance_id == instance_id,
                query_samples_table.c.time >= since,
                query_samples_table.c.time <= until,
                query_samples_table.c.wait_type.is_not(None),
            )
            .group_by(query_samples_table.c.wait_type)
            .order_by(sa.desc("total_wait_time_ms"))
        )
        return list((await self._session.execute(stmt)).fetchall())

    async def latest_by_session(
        self, instance_id: UUID, session_ids: Sequence[str], since: datetime
    ) -> dict[str, sa.Row]:
        """Amostra mais recente por `session_id` — usada so para enriquecer nos da arvore de
        bloqueio com sql_text_masked/db_user (`blocking_events` nao guarda esses campos, so
        sql_hash)."""
        if not session_ids:
            return {}
        stmt = sa.text("""
            SELECT DISTINCT ON (session_id) session_id, sql_hash, sql_text_masked, db_user
            FROM query_samples
            WHERE instance_id = :instance_id AND session_id = ANY(:session_ids) AND time >= :since
            ORDER BY session_id, time DESC
        """)
        result = await self._session.execute(
            stmt,
            {"instance_id": instance_id, "session_ids": list(session_ids), "since": since},
        )
        return {row.session_id: row for row in result.fetchall()}
