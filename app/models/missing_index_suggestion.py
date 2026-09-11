"""Modelo `MissingIndexSuggestion` — `Docs/database-schema.md` §3, ARCHITECTURE_SPEC.md Q18/Q21.

Ao contrario das hypertables de coleta continua (`query_samples`/`metric_samples`/
`blocking_events`), esta e uma tabela relacional comum, atualizada por UPSERT (nao serie
temporal bruta) — cada linha e o estado mais recente de UMA sugestao (`instance_id`,
`database_name`, `table_name`, `suggestion`), reavaliada por um worker de baixa frequencia
(`app/workers/missing_index_supervisor.py`), nao a cada poucos segundos como o `PollingWorker`.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class MissingIndexSuggestion(Base):
    """Estado mais recente de uma sugestao de indice ausente para uma instancia/banco/tabela.

    `database_name` e NULL quando o engine nao distingue banco na sugestao (nao se aplica hoje —
    SQL Server sempre preenche via `DB_NAME(mid.database_id)`, PostgreSQL preenche com o banco
    onde a heuristica rodou); mantido nullable para nao forcar um valor artificial se um futuro
    engine nao tiver esse conceito. `detail` guarda a estrutura bruta especifica do engine (ver
    `Docs/collector-queries.md` §10) — colunas de igualdade/desigualdade/incluidas no SQL Server,
    contadores de `pg_stat_user_tables` no PostgreSQL.
    """

    __tablename__ = "missing_index_suggestions"
    __table_args__ = (
        UniqueConstraint(
            "instance_id",
            "database_name",
            "table_name",
            "suggestion",
            name="uq_missing_index_suggestions_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    database_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    # Descricao legivel: colunas de igualdade/desigualdade/incluidas (SQL Server) ou resumo da
    # heuristica (PostgreSQL) — ver `Docs/collector-queries.md` §10.
    suggestion: Mapped[str] = mapped_column(Text, nullable=False)
    impact_score: Mapped[float | None] = mapped_column(nullable=True)
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
