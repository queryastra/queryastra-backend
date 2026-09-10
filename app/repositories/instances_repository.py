"""Repositorio de `Instance`/`InstanceMaintenanceWindow` — stub.

Ver `app/models/instance.py` para os campos, `Docs/database-schema.md` §1 para o contrato de
persistencia. Metodos ainda nao implementados: apenas a assinatura esperada pela camada de
`services` (`app/services/instance_service.py`).
"""

from __future__ import annotations

from uuid import UUID

from app.models.instance import Instance
from app.repositories.base import BaseRepository


class InstancesRepository(BaseRepository):
    """Acesso a dados de instancias monitoradas."""

    async def get_by_id(self, instance_id: UUID) -> Instance | None:
        raise NotImplementedError("InstancesRepository.get_by_id ainda nao implementado.")

    async def list_active(self) -> list[Instance]:
        raise NotImplementedError("InstancesRepository.list_active ainda nao implementado.")

    async def create(self, instance: Instance) -> Instance:
        raise NotImplementedError("InstancesRepository.create ainda nao implementado.")
