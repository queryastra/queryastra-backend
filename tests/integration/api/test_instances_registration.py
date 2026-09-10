"""Testes de integracao do cadastro de instancia (`POST /api/v1/instances`).

Usa `app.dependency_overrides` para injetar um repositorio fake em memoria (sem tocar o
TimescaleDB real) e monkeypatch em `app.services.instance_service.create_collector` para devolver
um coletor fake cujo `check_capabilities()` retorna listas fixas por caso de teste — sem tentar
conexao de rede real.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.v1.instances import get_instances_repository
from app.collectors.base import CapabilityResult, CapabilityStatus
from app.collectors.capabilities import (
    POSTGRESQL_CONFIGURATION_VISIBILITY,
    POSTGRESQL_SESSION_ACTIVITY,
    POSTGRESQL_SESSION_TERMINATION,
    POSTGRESQL_TOP_SQL,
)
from app.main import app
from app.models.instance import Instance

_ALL_MANDATORY_FULL = [
    CapabilityResult(POSTGRESQL_SESSION_ACTIVITY, CapabilityStatus.FULL),
    CapabilityResult(POSTGRESQL_CONFIGURATION_VISIBILITY, CapabilityStatus.FULL),
    CapabilityResult(POSTGRESQL_SESSION_TERMINATION, CapabilityStatus.FULL),
]

_MANDATORY_MISSING = [
    CapabilityResult(
        POSTGRESQL_SESSION_ACTIVITY, CapabilityStatus.UNAVAILABLE, "role pg_read_all_stats ausente"
    ),
    CapabilityResult(POSTGRESQL_CONFIGURATION_VISIBILITY, CapabilityStatus.FULL),
    CapabilityResult(POSTGRESQL_SESSION_TERMINATION, CapabilityStatus.FULL),
]

_OPTIONAL_DEGRADED = [
    *_ALL_MANDATORY_FULL,
    CapabilityResult(POSTGRESQL_TOP_SQL, CapabilityStatus.DEGRADED, "extensao ausente"),
]


class _FakeCollector:
    """Coletor fake: `check_capabilities()` devolve uma lista fixa, sem I/O de rede."""

    def __init__(self, capabilities: list[CapabilityResult]) -> None:
        self._capabilities = capabilities

    async def check_capabilities(self) -> list[CapabilityResult]:
        return self._capabilities

    async def collect(self):  # pragma: no cover - fora de escopo deste teste
        raise NotImplementedError

    async def close(self) -> None:
        return None


class _FakeInstancesRepository:
    """Repositorio fake em memoria — mesma interface de `InstancesRepository`, sem banco real."""

    def __init__(self) -> None:
        self.created: list[Instance] = []

    async def get_by_id(self, instance_id):  # noqa: ANN001 - fake, assinatura simples
        return next((i for i in self.created if i.id == instance_id), None)

    async def list_active(self) -> list[Instance]:
        return list(self.created)

    async def create(self, instance: Instance) -> Instance:
        instance.id = uuid4()
        instance.active = True
        instance.created_at = datetime.now(UTC)
        instance.updated_at = datetime.now(UTC)
        self.created.append(instance)
        return instance


@pytest.fixture
def fake_repository() -> _FakeInstancesRepository:
    return _FakeInstancesRepository()


@pytest.fixture
async def client_with_fake_repository(
    fake_repository: _FakeInstancesRepository,
) -> AsyncIterator[AsyncClient]:
    app.dependency_overrides[get_instances_repository] = lambda: fake_repository
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.pop(get_instances_repository, None)


def _payload(**overrides: object) -> dict:
    base = {
        "name": "meu-postgres",
        "db_type": "postgresql",
        "host": "127.0.0.1",
        "port": 5432,
        "criticality": "standard",
        "polling_interval_seconds": 10,
        "auth_method": "encrypted_fallback",
        "username": "monitoring_user",
        "password": "super-secret-password",
    }
    base.update(overrides)
    return base


async def test_register_instance_success_when_all_mandatory_full(
    client_with_fake_repository: AsyncClient,
    fake_repository: _FakeInstancesRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.services.instance_service.create_collector",
        lambda db_type, connection, instance_id=None: _FakeCollector(_ALL_MANDATORY_FULL),
    )

    response = await client_with_fake_repository.post("/api/v1/instances", json=_payload())

    assert response.status_code == 201
    body = response.json()
    assert "password" not in body
    assert "encrypted_credential_ref" not in body
    assert len(fake_repository.created) == 1
    # Documenta o comportamento: instancia recem-criada nunca tem telemetria ainda, entao o
    # status derivado no corpo do POST e sempre "unknown" (`UNKNOWN_STATUS`, sem consultar
    # `metric_samples`) — ver `app/api/v1/instances.py::create_instance`.
    assert body["health_status"] == "unknown"
    assert body["collection_coverage"] == "unavailable"
    assert body["last_collected_at"] is None


async def test_register_instance_rejected_when_mandatory_capability_unavailable(
    client_with_fake_repository: AsyncClient,
    fake_repository: _FakeInstancesRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.services.instance_service.create_collector",
        lambda db_type, connection, instance_id=None: _FakeCollector(_MANDATORY_MISSING),
    )

    response = await client_with_fake_repository.post("/api/v1/instances", json=_payload())

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any(c["status"] == "unavailable" for c in detail["capabilities"])
    assert fake_repository.created == []


async def test_register_instance_accepted_when_only_optional_capability_degraded(
    client_with_fake_repository: AsyncClient,
    fake_repository: _FakeInstancesRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.services.instance_service.create_collector",
        lambda db_type, connection, instance_id=None: _FakeCollector(_OPTIONAL_DEGRADED),
    )

    response = await client_with_fake_repository.post("/api/v1/instances", json=_payload())

    assert response.status_code == 201
    assert len(fake_repository.created) == 1


async def test_encrypted_credential_ref_never_contains_plaintext_password(
    client_with_fake_repository: AsyncClient,
    fake_repository: _FakeInstancesRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.services.instance_service.create_collector",
        lambda db_type, connection, instance_id=None: _FakeCollector(_ALL_MANDATORY_FULL),
    )

    response = await client_with_fake_repository.post(
        "/api/v1/instances", json=_payload(password="a-very-plain-password")
    )

    assert response.status_code == 201
    persisted = fake_repository.created[0]
    assert persisted.encrypted_credential_ref is not None
    assert persisted.encrypted_credential_ref != "a-very-plain-password"
    assert "a-very-plain-password" not in persisted.encrypted_credential_ref
