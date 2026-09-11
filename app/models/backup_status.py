"""Modelo `BackupStatus` — `Docs/database-schema.md` §3, ARCHITECTURE_SPEC.md Q16.

Visibilidade de "tempo desde o último backup" — tabela relacional comum (estado mais recente,
não série temporal bruta), atualizada por UPSERT por um worker de baixa frequência
(`app/workers/backup_status_supervisor.py`), mesmo padrão de `MissingIndexSuggestion`.

SQL Server grava 1 linha por banco (`database_name` preenchido) — `last_backup_at` vem de
`MAX(backup_finish_date)` de `msdb.dbo.backupset` (`type='D'`, só backup completo/full).
PostgreSQL grava 1 única linha por instância (`database_name` NULL — `pg_stat_archiver` é
cluster-wide, não por banco) com `last_backup_at = last_archived_time` como PROXY de saúde de WAL
archiving, NÃO um backup de fato — Postgres não tem catálogo nativo de backup completo como o
`msdb` do SQL Server. A UI deve deixar essa diferença de semântica clara.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func, text

from app.db.base import Base


class BackupStatus(Base):
    """Estado mais recente de backup/archiving de uma instância (ou de um banco dela, no SQL
    Server) — ver docstring do módulo para a diferença de semântica entre engines."""

    __tablename__ = "backup_status"
    __table_args__ = (
        # 2 índices únicos parciais (não 1 UNIQUE simples): NULL não é tratado como valor
        # repetível em UNIQUE do Postgres — sem os parciais, cada checagem do Postgres criaria
        # uma linha nova em vez de atualizar a mesma (database_name sempre NULL nesse caso).
        Index(
            "uq_backup_status_instance_database",
            "instance_id",
            "database_name",
            unique=True,
            postgresql_where=text("database_name IS NOT NULL"),
        ),
        Index(
            "uq_backup_status_instance_only",
            "instance_id",
            unique=True,
            postgresql_where=text("database_name IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    database_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_backup_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
