"""Excecoes de servico compartilhadas entre `instance_service` e `health_check_service`."""

from __future__ import annotations

from uuid import UUID


class InstanceNotFoundError(Exception):
    """Nenhuma instancia cadastrada com o `instance_id` informado."""

    def __init__(self, instance_id: UUID) -> None:
        self.instance_id = instance_id
        super().__init__(f"Instancia nao encontrada: {instance_id}")
