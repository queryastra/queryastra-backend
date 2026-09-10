"""Modelos `Server` e `InstanceServer` (vinculo N:N).

Conforme `Docs/database-schema.md` §1 — servidores/hosts monitorados via
node_exporter/windows_exporter, vinculados opcionalmente a instancias de banco (um host pode ter
varias instancias; uma instancia pode ser um cluster multi-host no futuro).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class Server(Base):
    """Servidor/host monitorado via node_exporter (Linux) ou windows_exporter (Windows)."""

    __tablename__ = "servers"
    __table_args__ = (
        CheckConstraint("os_type IN ('linux', 'windows')", name="ck_servers_os_type"),
        CheckConstraint("criticality IN ('critical', 'standard')", name="ck_servers_criticality"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    hostname: Mapped[str] = mapped_column(String, nullable=False)
    os_type: Mapped[str] = mapped_column(String, nullable=False)
    # host:port do node_exporter/windows_exporter, ou identificador do push remote_write.
    exporter_endpoint: Mapped[str] = mapped_column(String, nullable=False)
    criticality: Mapped[str] = mapped_column(String, nullable=False, server_default="standard")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)


class InstanceServer(Base):
    """Vinculo N:N entre instancia de banco e servidor/host fisico."""

    __tablename__ = "instance_servers"

    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), primary_key=True
    )
    server_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("servers.id", ondelete="CASCADE"), primary_key=True
    )
