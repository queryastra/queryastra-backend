"""Modelo `MissingIndexCollectionRun` — quando foi a ultima coleta de indice ausente de uma
instancia, e desde quando os dados de uso acumulados (`sys.dm_db_missing_index_group_stats`/
`pg_stat_user_tables`) sao validos.

Sem isso, "quando foi a ultima coleta" so era derivavel de `MAX(last_seen_at)` entre as proprias
sugestoes — quebra quando a instancia genuinamente nao tem NENHUMA sugestao (ex.: banco pequeno
demais pro limiar), caso em que nao havia como distinguir "nunca coletou" de "coletou e nao achou
nada". `stats_since` (reinicio do motor) e uma pergunta DIFERENTE de `collected_at` (quando o
worker rodou): os 2 motores zeram os contadores cumulativos de uso no proprio restart do
servico, entao um tempdb reiniciado ha 1 ano acumulou muito mais historico (mais confiavel) que
um reiniciado ha 1 semana, mesmo com a mesma `collected_at` recente nos dois casos — pedido
explicito do usuario, item 4 desta rodada.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MissingIndexCollectionRun(Base):
    """1 linha por instancia (upsert) — estado da ultima checagem de indice ausente, exista ou
    nao alguma sugestao no momento."""

    __tablename__ = "missing_index_collection_runs"

    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), primary_key=True
    )
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # `None` quando o motor nao devolveu um valor (erro pontual de coleta) - nunca inventado.
    stats_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
