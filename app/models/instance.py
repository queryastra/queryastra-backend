"""Modelos `Instance` e `InstanceMaintenanceWindow`.

Campos e constraints conforme `Docs/database-schema.md` §1. NUNCA adicionar coluna de senha
inline aqui (ver nota no schema — credencial nao deve ficar em coluna de cadastro).
Credencial vive so como referencia em `encrypted_credential_ref`
(`app/security/credential_reference.py`), nunca em texto puro.
"""

from __future__ import annotations

import uuid
from datetime import datetime, time

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Time,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.db.base import Base


class Instance(Base):
    """Instancia de banco de dados monitorada (PostgreSQL ou SQL Server)."""

    __tablename__ = "instances"
    __table_args__ = (
        CheckConstraint(
            "db_type IN ('postgresql', 'sqlserver', 'mysql', 'cockroachdb', 'aurora_postgresql', 'aurora_mysql', 'oracle', 'db2', 'redshift')",
            name="ck_instances_db_type",
        ),
        CheckConstraint(
            "cloud_provider IN ('aws_rds', 'azure', 'gcp') OR cloud_provider IS NULL",
            name="ck_instances_cloud_provider",
        ),
        CheckConstraint(
            "engine_support_tier IN ('formal', 'degraded', 'unsupported') "
            "OR engine_support_tier IS NULL",
            name="ck_instances_engine_support_tier",
        ),
        CheckConstraint("criticality IN ('critical', 'standard')", name="ck_instances_criticality"),
        CheckConstraint(
            "auth_method IN ('integrated', 'encrypted_fallback')",
            name="ck_instances_auth_method",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    db_type: Mapped[str] = mapped_column(String, nullable=False)
    host: Mapped[str] = mapped_column(String, nullable=False)
    port: Mapped[int] = mapped_column(Integer, nullable=False)

    # Detecção de nuvem gerenciada muda o que e coletavel (RDS nao expoe SO; Azure Flexible
    # Server tem catalogo de extensao restrito) — ver database-schema.md §1.
    cloud_provider: Mapped[str | None] = mapped_column(String, nullable=True)

    # Detectados/recalculados no health-check de cadastro e a cada reconexao, nunca digitados
    # manualmente (Docs/collector-queries.md §9, Docs/version-compatibility.md).
    engine_version: Mapped[str | None] = mapped_column(String, nullable=True)
    engine_version_major: Mapped[int | None] = mapped_column(Integer, nullable=True)
    engine_support_tier: Mapped[str | None] = mapped_column(String, nullable=True)

    criticality: Mapped[str] = mapped_column(String, nullable=False, server_default="standard")
    polling_interval_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="10"
    )

    auth_method: Mapped[str] = mapped_column(String, nullable=False)
    # Referencia opaca ao segredo (cofre ou criptografia com chave fora do banco) — NUNCA a
    encrypted_credential_ref: Mapped[str | None] = mapped_column(String, nullable=True)

    # Override ad-hoc do "Tempo mínimo para captura de Bloqueios (Locks)" só para esta instância
    # — a pedido explícito do usuário, pra poder afinar temporariamente uma investigação (ex.:
    # analisar um caso específico com um cliente) sem afetar as demais instâncias monitoradas. Se
    # NULL (padrão), usa o valor global de `system_settings.blocking_min_duration_ms`
    # (`app.models.system_settings`) — `PollingWorker._run_cycle` resolve isso a cada ciclo.
    min_lock_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Mesmo padrão do override acima, para "Transações em Sleeping" — NULL (padrão) usa
    # `system_settings.sleeping_min_duration_ms`.
    min_sleeping_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(nullable=True)

    maintenance_windows: Mapped[list[InstanceMaintenanceWindow]] = relationship(
        back_populates="instance", cascade="all, delete-orphan"
    )


class InstanceMaintenanceWindow(Base):
    """Janela de manutencao/blackout recorrente por instancia.

    Permite pausar coleta/alerta num horario recorrente
    sem desativar a instancia inteira (`instances.active`).
    """

    __tablename__ = "instance_maintenance_windows"
    __table_args__ = (
        CheckConstraint("day_of_week BETWEEN 0 AND 6", name="ck_maintenance_windows_day_of_week"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    day_of_week: Mapped[int] = mapped_column(SmallInteger, nullable=False)  # 0 = domingo
    start_time: Mapped[time] = mapped_column(Time, nullable=False)
    end_time: Mapped[time] = mapped_column(Time, nullable=False)
    suppress_polling: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)

    instance: Mapped[Instance] = relationship(back_populates="maintenance_windows")
