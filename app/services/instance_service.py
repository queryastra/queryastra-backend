"""Caso de uso de cadastro/gestao de instancias.

Orquestra `InstancesRepository` + o `Collector` correto (`app.collectors.factory.create_collector`,
escolhido por `db_type`) para cadastro e teste de conexao (`Docs/specs/02-backend-contract.md`).

Regra critica (Docs/rules/01-credential-security.md): a senha em texto puro so existe em memoria
pelo tempo minimo necessario para abrir a conexao de teste ou cifrar o fallback — nunca e
persistida em `instances`, nunca logada (nem via log do `payload` inteiro).
"""

from __future__ import annotations

from dataclasses import dataclass

from app.collectors.base import CapabilityResult
from app.collectors.capabilities import has_missing_mandatory_capability
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.models.instance import Instance
from app.repositories.instances_repository import InstancesRepository
from app.security.credential_encryption import encrypt_credential


@dataclass(frozen=True, slots=True)
class ConnectionTestInput:
    """Entrada do teste de conexao pontual — nao persiste nada."""

    db_type: str
    host: str
    port: int
    auth_method: str
    username: str | None = None
    password: str | None = None


@dataclass(frozen=True, slots=True)
class RegisterInstanceInput:
    """Entrada do cadastro de instancia — testa a conexao antes de persistir."""

    name: str
    db_type: str
    host: str
    port: int
    auth_method: str
    cloud_provider: str | None = None
    criticality: str = "standard"
    polling_interval_seconds: int = 10
    username: str | None = None
    password: str | None = None


class InstanceRegistrationRejected(Exception):
    """Cadastro reprovado: alguma capacidade mandatoria da engine ficou `UNAVAILABLE`."""

    def __init__(self, capabilities: list[CapabilityResult]) -> None:
        self.capabilities = capabilities
        super().__init__("Cadastro reprovado: capacidade mandatoria indisponivel")


class InstanceService:
    """Casos de uso de cadastro e teste de conexao de instancias."""

    def __init__(self, repository: InstancesRepository) -> None:
        self._repository = repository

    async def test_connection(
        self, connection_input: ConnectionTestInput
    ) -> list[CapabilityResult]:
        """Abre uma conexao pontual (sem persistir nada) e devolve a cobertura de capacidade."""
        connection = ConnectionParams(
            host=connection_input.host,
            port=connection_input.port,
            auth_method=connection_input.auth_method,
            username=connection_input.username,
            secret_value=connection_input.password,
        )
        collector = create_collector(connection_input.db_type, connection=connection)
        try:
            return await collector.check_capabilities()
        finally:
            await collector.close()

    async def register_instance(self, payload: RegisterInstanceInput) -> Instance:
        """Testa a conexao e, se as capacidades mandatorias estiverem OK, persiste a instancia.

        Raises:
            InstanceRegistrationRejected: alguma capacidade mandatoria da engine (ver
                `app.collectors.capabilities.MANDATORY_CAPABILITIES`) ficou `UNAVAILABLE`, ou a
                conexao nem abriu.
        """
        capabilities = await self.test_connection(
            ConnectionTestInput(
                db_type=payload.db_type,
                host=payload.host,
                port=payload.port,
                auth_method=payload.auth_method,
                username=payload.username,
                password=payload.password,
            )
        )
        if has_missing_mandatory_capability(payload.db_type, capabilities):
            raise InstanceRegistrationRejected(capabilities)

        encrypted_ref = None
        if payload.auth_method == "encrypted_fallback":
            encrypted_ref = encrypt_credential(payload.username or "", payload.password or "")

        # Verifica se existe instância soft-deletada com mesmo endpoint — se sim, restaura o UUID
        # original para que o histórico TimescaleDB fique acessível sem gaps de UUID.
        existing = await self._repository.find_soft_deleted(
            payload.host, payload.port, payload.db_type
        )
        if existing is not None:
            return await self._repository.restore(
                existing,
                name=payload.name,
                criticality=payload.criticality,
                polling_interval_seconds=payload.polling_interval_seconds,
                encrypted_credential_ref=encrypted_ref,
                cloud_provider=payload.cloud_provider,
            )

        instance = Instance(
            name=payload.name,
            db_type=payload.db_type,
            host=payload.host,
            port=payload.port,
            cloud_provider=payload.cloud_provider,
            criticality=payload.criticality,
            polling_interval_seconds=payload.polling_interval_seconds,
            auth_method=payload.auth_method,
            encrypted_credential_ref=encrypted_ref,
        )
        return await self._repository.create(instance)
