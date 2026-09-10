"""Fabrica de `Collector` por `db_type` — unico ponto que decide qual adaptador instanciar.

Usado por `app/services/instance_service.py` (teste de conexao/cadastro) e
`app/services/health_check_service.py` (health-check periodico de instancia ja cadastrada).
"""

from __future__ import annotations

from uuid import UUID

from app.collectors.base import Collector
from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.adapter import PostgreSQLCollector
from app.collectors.sqlserver.adapter import SQLServerCollector


def create_collector(
    db_type: str, connection: ConnectionParams, instance_id: UUID | None = None
) -> Collector:
    """Instancia o `Collector` correto para `db_type` ("postgresql" | "sqlserver")."""
    if db_type == "postgresql":
        return PostgreSQLCollector(connection=connection, instance_id=instance_id)
    if db_type == "sqlserver":
        return SQLServerCollector(connection=connection, instance_id=instance_id)
    raise ValueError(f"db_type desconhecido: {db_type}")
