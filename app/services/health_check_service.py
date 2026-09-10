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


class HealthCheckService:
    """Agrega o resultado de `check_capabilities()` de um `Collector` num veredito por instancia."""

    async def check_instance(self, instance_id: UUID) -> list[CapabilityResult]:
        raise NotImplementedError("HealthCheckService.check_instance ainda nao implementado.")

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
