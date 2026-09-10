"""Orquestracao de health-check de capacidade — `Docs/rules/04-engine-capabilities.md`.

"O health-check deve reportar capacidade completa, limitada ou indisponivel, sem bloquear o
cadastro quando a degradacao for aceitavel." Este servico chama
`Collector.check_capabilities()` do adaptador correto e resume o resultado numa unica
capacidade agregada para a instancia (`CapabilityStatus`), sem esconder o detalhe por
capacidade individual.
"""

from __future__ import annotations

from uuid import UUID

from app.collectors.base import CapabilityResult, CapabilityStatus
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.repositories.instances_repository import InstancesRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)
from app.services.exceptions import InstanceNotFoundError

__all__ = ["HealthCheckService", "InstanceNotFoundError"]


class HealthCheckService:
    """Agrega o resultado de `check_capabilities()` de um `Collector` num veredito por instancia."""

    def __init__(
        self, repository: InstancesRepository, credential_resolver: CredentialResolver
    ) -> None:
        self._repository = repository
        self._credential_resolver = credential_resolver

    async def check_instance(self, instance_id: UUID) -> list[CapabilityResult]:
        """Resolve a credencial (se houver) da instancia ja cadastrada e checa suas capacidades.

        Raises:
            InstanceNotFoundError: nenhuma instancia com esse id.
        """
        instance = await self._repository.get_by_id(instance_id)
        if instance is None:
            raise InstanceNotFoundError(instance_id)

        username: str | None = None
        secret_value: str | None = None
        if instance.auth_method == "encrypted_fallback":
            if instance.encrypted_credential_ref is None:
                raise CredentialResolutionError(
                    "encrypted_credential_ref ausente para auth_method='encrypted_fallback'"
                )
            resolved = await self._credential_resolver.resolve(
                CredentialReference(instance.encrypted_credential_ref)
            )
            username, secret_value = resolved.username, resolved.secret_value

        connection = ConnectionParams(
            host=instance.host,
            port=instance.port,
            auth_method=instance.auth_method,
            username=username,
            secret_value=secret_value,
        )
        collector = create_collector(
            instance.db_type, connection=connection, instance_id=instance.id
        )
        try:
            return await collector.check_capabilities()
        finally:
            await collector.close()

    @staticmethod
    def overall_status(capabilities: list[CapabilityResult]) -> CapabilityStatus:
        """Resume uma lista de `CapabilityResult` num unico `CapabilityStatus`.

        Regra: pior status vence (qualquer `UNAVAILABLE` domina; senao qualquer `DEGRADED`
        domina; so `FULL` em tudo resulta em `FULL`) — nunca bloqueia o cadastro, so informa o
        nivel de degradacao agregado.
        """
        statuses = {c.status for c in capabilities}
        if CapabilityStatus.UNAVAILABLE in statuses:
            return CapabilityStatus.UNAVAILABLE
        if CapabilityStatus.DEGRADED in statuses:
            return CapabilityStatus.DEGRADED
        return CapabilityStatus.FULL
