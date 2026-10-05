"""Repositorio da hypertable `metric_samples`.

`metric_samples` (`Docs/database-schema.md` §2) nao tem model ORM dedicado — hypertables e
continuous aggregates exigem TimescaleDB de verdade (`create_hypertable`,
`timescaledb.continuous`), criadas via `migrations/versions/66ae4664300c_hypertables.py`. Este
repositorio escreve via SQLAlchemy Core (`app/repositories/_tables.py`), apontando para a tabela
real ja migrada.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import sqlalchemy as sa

from app.normalizers.models import NormalizedMetricSample
from app.repositories._tables import metric_samples_table
from app.repositories.base import BaseRepository

# Etapa 10, item 4 (Estagio B) - `metric_samples_hourly`/`_daily` agora tem `add_continuous_
# aggregate_policy` de verdade (migration `b7c2f4e9a1d6`), entao `list_series` passa a ler dos
# aggregates quando a janela pedida e longa demais pra ser eficiente/correta contra a hypertable
# bruta. Limiares alinhados com a retencao ja configurada (30 dias bruto / 90 dias hora / 5 anos
# dia) - nunca pedir dado a um nivel que ja pode ter sido expurgado por retention:
#   - janela <= 2 dias: hypertable bruta (granularidade fina, o caso comum de "Dia"/"Por hora")
#   - 2 dias < janela <= 45 dias: `metric_samples_hourly` (bem dentro dos 90 dias de retencao dela)
#   - janela > 45 dias: `metric_samples_daily` (retencao de 5 anos, cobre o modo "Tudo")
_RAW_TABLE_MAX_WINDOW = timedelta(days=2)
_HOURLY_MAX_WINDOW = timedelta(days=45)


class MetricSamplesRepository(BaseRepository):
    """Persistencia e leitura de `NormalizedMetricSample` na hypertable `metric_samples`.

    `list_series` roteia pra `metric_samples`/`metric_samples_hourly`/`metric_samples_daily`
    conforme o tamanho da janela pedida (ver `_RAW_TABLE_MAX_WINDOW`/`_HOURLY_MAX_WINDOW`) - os
    demais metodos (telemetria de ciclo, baseline de anomaly detection, coverage) continuam so
    contra a hypertable bruta, que e onde esse dado sempre existe mais recente.
    """

    async def insert_batch(self, samples: list[NormalizedMetricSample]) -> None:
        if not samples:
            return
        rows = [
            {
                "time": s.time,
                "target_type": s.target_type,
                "target_id": s.target_id,
                "metric_name": s.metric_name,
                "labels": s.labels,
                "value": s.value,
            }
            for s in samples
        ]
        await self._session.execute(sa.insert(metric_samples_table), rows)
        await self._session.commit()

    async def list_series(
        self,
        target_type: str,
        target_id: UUID,
        metric_names: Sequence[str] | None,
        since: datetime,
        until: datetime,
        limit: int,
    ) -> list[sa.Row]:
        """Linhas brutas ordenadas por (metric_name, time) para agrupar em serie por
        metric_name na camada de API. `metric_names` None/vazio devolve todas as metricas do
        alvo no periodo.

        `limit` e aplicado POR METRIC_NAME (via `ROW_NUMBER() OVER (PARTITION BY metric_name ...)`),
        nao um LIMIT global — bug real encontrado em producao: um LIMIT global ordenado por
        `(metric_name, time)` faz as primeiras metricas em ordem alfabetica (ex.:
        `collector_cycle_duration_ms`, gravada a cada ciclo) consumirem o orcamento inteiro pra
        uma instancia de poll rapido (1s), deixando toda metrica de negocio real (sessoes ativas,
        Page Life Expectancy, etc.) sem NENHUM ponto na tela de "Dia inteiro" — nao e so volume de
        dado, e uma metrica inteira desaparecendo silenciosamente. Mantem as amostras mais
        RECENTES por metrica (`ORDER BY time DESC` dentro do particionamento), depois reordena
        cronologicamente pra exibicao.

        Fonte escolhida pelo tamanho da janela (Etapa 10, item 4, Estagio B) — ver
        `_RAW_TABLE_MAX_WINDOW`/`_HOURLY_MAX_WINDOW`: janela curta le a hypertable bruta
        (granularidade fina), janela media le `metric_samples_hourly`, janela longa le
        `metric_samples_daily` (`avg_value` no lugar de `value`, `bucket` no lugar de `time`) -
        os 2 aggregates agora agrupam por `labels` tambem (migration `b7c2f4e9a1d6`), entao uma
        metrica com `labels` distintos (ex.: freespace por volume) nunca e misturada numa unica
        media."""
        window = until - since
        if window <= _RAW_TABLE_MAX_WINDOW:
            source_table, time_column, value_column = "metric_samples", "time", "value"
        elif window <= _HOURLY_MAX_WINDOW:
            source_table, time_column, value_column = (
                "metric_samples_hourly",
                "bucket",
                "avg_value",
            )
        else:
            source_table, time_column, value_column = (
                "metric_samples_daily",
                "bucket",
                "avg_value",
            )

        stmt = sa.text(f"""
            WITH ranked AS (
                SELECT
                    {time_column} AS time, target_type, target_id, metric_name, labels,
                    {value_column} AS value,
                    ROW_NUMBER() OVER (PARTITION BY metric_name ORDER BY {time_column} DESC) AS rn
                FROM {source_table}
                WHERE target_type = :target_type AND target_id = :target_id
                  AND {time_column} >= :since AND {time_column} <= :until
                  AND (CAST(:metric_names AS text[]) IS NULL OR metric_name = ANY(:metric_names))
            )
            SELECT time, target_type, target_id, metric_name, labels, value
            FROM ranked
            WHERE rn <= :limit
            ORDER BY metric_name, time
        """)
        # source_table/time_column/value_column acima sao constantes internas fixas (nunca
        # input do usuario) - nao ha concatenacao de dado externo na query.
        result = await self._session.execute(
            stmt,
            {
                "target_type": target_type,
                "target_id": target_id,
                "since": since,
                "until": until,
                "metric_names": list(metric_names) if metric_names else None,
                "limit": limit,
            },
        )
        return list(result.fetchall())

    async def get_latest_value(
        self, target_type: str, target_id: UUID, metric_name: str
    ) -> sa.Row | None:
        """Ultima amostra de uma metrica pontual (telemetria de ciclo do collector)."""
        stmt = (
            sa.select(metric_samples_table.c.time, metric_samples_table.c.value)
            .where(
                metric_samples_table.c.target_type == target_type,
                metric_samples_table.c.target_id == target_id,
                metric_samples_table.c.metric_name == metric_name,
            )
            .order_by(metric_samples_table.c.time.desc())
            .limit(1)
        )
        return (await self._session.execute(stmt)).first()

    async def get_baseline_stats(
        self, target_type: str, target_id: UUID, metric_name: str, since: datetime
    ) -> sa.Row | None:
        """Media/desvio padrao amostral de uma metrica desde `since` — base do alerta de anomaly
        detection (Fase 2, item 1: `AlertingWorker._evaluate_anomaly`, warning=media+2*stddev,
        critical=media+3*stddev, mesmo modelo simples do SAM/Orion). `STDDEV_SAMP` (nao
        `STDDEV_POP`) porque estamos estimando o desvio padrao da POPULACAO a partir de uma
        AMOSTRA do historico, nao temos todos os valores possiveis. `sample_count` devolvido
        pra quem chama decidir se ha dado suficiente pra confiar no baseline (ver docstring do
        worker: menos de ~20 amostras e baseline instavel demais, resulta em nenhum alerta, nao
        um alerta inventado)."""
        stmt = sa.select(
            sa.func.avg(metric_samples_table.c.value).label("mean"),
            sa.func.stddev_samp(metric_samples_table.c.value).label("stddev"),
            sa.func.count().label("sample_count"),
        ).where(
            metric_samples_table.c.target_type == target_type,
            metric_samples_table.c.target_id == target_id,
            metric_samples_table.c.metric_name == metric_name,
            metric_samples_table.c.time >= since,
        )
        return (await self._session.execute(stmt)).first()

    async def get_daily_averages_for_forecast(
        self,
        target_type: str,
        target_id: UUID,
        metric_name: str,
        since: datetime,
        aggregate_fn: str = "AVG",
    ) -> list[tuple[str, float]]:
        """Médias (ou mínimos) diárias de uma métrica para regressão de capacidade.

        Lê de `metric_samples_daily` (continuous aggregate, retenção 5 anos). Quando
        `aggregate_fn="MIN"` (ex.: disco livre — queremos o pior volume), devolve o mínimo
        entre todos os labels naquele dia em vez da média. Retorna lista de (date_str, value).
        """
        # aggregate_fn é constante interna (nunca input do usuário) — seguro usar em f-string.
        agg = "MIN" if aggregate_fn == "MIN" else "AVG"
        stmt = sa.text(f"""
            SELECT
                DATE(bucket AT TIME ZONE 'UTC')::text AS date_str,
                {agg}(avg_value) AS value
            FROM metric_samples_daily
            WHERE target_type = :target_type
              AND target_id = :target_id
              AND metric_name = :metric_name
              AND bucket >= :since
            GROUP BY DATE(bucket AT TIME ZONE 'UTC')
            ORDER BY DATE(bucket AT TIME ZONE 'UTC')
        """)
        result = await self._session.execute(
            stmt,
            {
                "target_type": target_type,
                "target_id": str(target_id),
                "metric_name": metric_name,
                "since": since,
            },
        )
        return [(row.date_str, float(row.value)) for row in result.fetchall()]

    async def count_distinct_metrics_since(
        self,
        target_type: str,
        target_id: UUID,
        since: datetime,
        exclude_metric_names: Sequence[str],
    ) -> int:
        """Metricas distintas (fora da telemetria de ciclo) desde `since` — criterio simples de
        collection_coverage: 0 = 'unavailable', >0 = 'full'."""
        stmt = sa.select(sa.func.count(sa.distinct(metric_samples_table.c.metric_name))).where(
            metric_samples_table.c.target_type == target_type,
            metric_samples_table.c.target_id == target_id,
            metric_samples_table.c.time >= since,
            metric_samples_table.c.metric_name.notin_(exclude_metric_names),
        )
        return (await self._session.execute(stmt)).scalar_one()

    async def get_metric_trend_hourly(
        self,
        instance_id: UUID,
        metric_name: str,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        """Medias horarias de uma metrica de instancia a partir de `metric_samples_hourly`.

        Agrega todos os `labels` por bucket (AVG de avg_value, MIN de min_value, MAX de
        max_value) — metricas com multiplos labels (ex.: freespace por volume) sao colapsadas
        numa unica linha por hora, adequado para exibicao de tendencia agregada.

        Retorna lista de dicts `{bucket, avg_value, min_value, max_value}`.
        """
        stmt = sa.text("""
            SELECT
                bucket,
                AVG(avg_value) AS avg_value,
                MIN(min_value) AS min_value,
                MAX(max_value) AS max_value
            FROM metric_samples_hourly
            WHERE target_type = 'instance'
              AND target_id   = :target_id
              AND metric_name = :metric_name
              AND bucket >= :since
              AND bucket <  :until
            GROUP BY bucket
            ORDER BY bucket
        """)
        result = await self._session.execute(
            stmt,
            {
                "target_id": str(instance_id),
                "metric_name": metric_name,
                "since": since,
                "until": until,
            },
        )
        return [
            {
                "bucket": row.bucket,
                "avg_value": float(row.avg_value),
                "min_value": float(row.min_value),
                "max_value": float(row.max_value),
            }
            for row in result.fetchall()
        ]

    async def get_metric_trend_daily(
        self,
        instance_id: UUID,
        metric_name: str,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        """Medias diarias de uma metrica de instancia a partir de `metric_samples_daily`.

        Mesma logica de agregacao de `get_metric_trend_hourly` mas usando a view diaria
        (retencao de 5 anos — adequada para janelas longas ate 365 dias).
        """
        stmt = sa.text("""
            SELECT
                bucket,
                AVG(avg_value) AS avg_value,
                MIN(min_value) AS min_value,
                MAX(max_value) AS max_value
            FROM metric_samples_daily
            WHERE target_type = 'instance'
              AND target_id   = :target_id
              AND metric_name = :metric_name
              AND bucket >= :since
              AND bucket <  :until
            GROUP BY bucket
            ORDER BY bucket
        """)
        result = await self._session.execute(
            stmt,
            {
                "target_id": str(instance_id),
                "metric_name": metric_name,
                "since": since,
                "until": until,
            },
        )
        return [
            {
                "bucket": row.bucket,
                "avg_value": float(row.avg_value),
                "min_value": float(row.min_value),
                "max_value": float(row.max_value),
            }
            for row in result.fetchall()
        ]

    async def get_distinct_metric_names(
        self,
        target_type: str,
        target_id: UUID,
        since: datetime,
    ) -> list[str]:
        """Nomes de metricas distintos coletados para o alvo desde `since`.

        Lido da hypertable bruta (`metric_samples`) — garante visibilidade de metricas
        recentes mesmo quando os continuous aggregates ainda nao materializaram o ultimo bucket.
        """
        stmt = (
            sa.select(sa.distinct(metric_samples_table.c.metric_name))
            .where(
                metric_samples_table.c.target_type == target_type,
                metric_samples_table.c.target_id == target_id,
                metric_samples_table.c.time >= since,
            )
            .order_by(metric_samples_table.c.metric_name)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.fetchall()]
