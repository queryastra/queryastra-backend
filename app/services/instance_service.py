"""Caso de uso de cadastro/gestao de instancias — stub.

Orquestra `InstancesRepository` + o `Collector` correto (`app.collectors.postgresql.adapter` ou
`app.collectors.sqlserver.adapter`, escolhido por `Instance.db_type`) para cadastro, teste de
conexao e health-check (`Docs/specs/02-backend-contract.md`).
"""

from __future__ import annotations

from uuid import UUID

from app.repositories.instances_repository import InstancesRepository


class InstanceService:
    """Casos de uso de cadastro, teste de conexao e health-check de instancias."""

    def __init__(self, repository: InstancesRepository) -> None:
        self._repository = repository

    async def register_instance(self, payload: dict) -> None:
        raise NotImplementedError("InstanceService.register_instance ainda nao implementado.")

    async def test_connection(self, instance_id: UUID) -> None:
        raise NotImplementedError("InstanceService.test_connection ainda nao implementado.")
