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
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` bruto por `sql_hash`: execution_count/total/avg via GROUP BY;
        sql_name/sql_text_masked da linha mais recente por hash (DISTINCT ON); wait predominante
        = moda de wait_type por hash (window function). SQL literal via `sa.text` (mais legivel
        que expression builder para DISTINCT ON + moda)."""
        stmt = sa.text("""
            WITH agg AS (
                SELECT sql_hash, count(*) AS execution_count,
                       coalesce(sum(wait_time_ms), 0) AS total_wait_time_ms,
                       coalesce(avg(wait_time_ms), 0) AS avg_wait_time_ms
                FROM query_samples
                WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                GROUP BY sql_hash
            ),
            latest AS (
                SELECT DISTINCT ON (sql_hash) sql_hash, sql_name, sql_text_masked
                FROM query_samples
                WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                ORDER BY sql_hash, time DESC
            ),
            wait_counts AS (
                SELECT sql_hash, wait_type, count(*) AS wait_count
                FROM query_samples
                WHERE instance_id = :instance_id AND time >= :since AND time <= :until
                    AND wait_type IS NOT NULL
                GROUP BY sql_hash, wait_type
            ),
            wait_mode AS (
                SELECT sql_hash, wait_type,
                       row_number() OVER (PARTITION BY sql_hash ORDER BY wait_count DESC) AS rn
                FROM wait_counts
            )
            SELECT agg.sql_hash, latest.sql_name, latest.sql_text_masked,
                   agg.execution_count, agg.total_wait_time_ms, agg.avg_wait_time_ms,
                   wait_mode.wait_type AS predominant_wait_type
            FROM agg
            JOIN latest ON latest.sql_hash = agg.sql_hash
            LEFT JOIN wait_mode ON wait_mode.sql_hash = agg.sql_hash AND wait_mode.rn = 1
            ORDER BY agg.total_wait_time_ms DESC
            LIMIT :limit
        """)
        result = await self._session.execute(
            stmt, {"instance_id": instance_id, "since": since, "until": until, "limit": limit}
        )
        return list(result.fetchall())

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
