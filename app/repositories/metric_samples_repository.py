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
