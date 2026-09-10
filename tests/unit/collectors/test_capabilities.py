"""Testes de `has_missing_mandatory_capability` — `app/collectors/capabilities.py`."""

from __future__ import annotations

from app.collectors.base import CapabilityResult, CapabilityStatus
from app.collectors.capabilities import (
    CONNECTIVITY,
    POSTGRESQL_CONFIGURATION_VISIBILITY,
    POSTGRESQL_SESSION_ACTIVITY,
    POSTGRESQL_SESSION_TERMINATION,
    POSTGRESQL_TOP_SQL,
    SQLSERVER_SERVER_STATE,
    SQLSERVER_SESSION_TERMINATION,
    has_missing_mandatory_capability,
)


def test_missing_mandatory_capability_rejects_postgresql() -> None:
    capabilities = [
        CapabilityResult(POSTGRESQL_SESSION_ACTIVITY, CapabilityStatus.UNAVAILABLE, "sem role"),
        CapabilityResult(POSTGRESQL_CONFIGURATION_VISIBILITY, CapabilityStatus.FULL),
        CapabilityResult(POSTGRESQL_SESSION_TERMINATION, CapabilityStatus.FULL),
    ]
    assert has_missing_mandatory_capability("postgresql", capabilities) is True


def test_only_optional_capability_missing_does_not_reject() -> None:
    capabilities = [
        CapabilityResult(POSTGRESQL_SESSION_ACTIVITY, CapabilityStatus.FULL),
        CapabilityResult(POSTGRESQL_CONFIGURATION_VISIBILITY, CapabilityStatus.FULL),
        CapabilityResult(POSTGRESQL_SESSION_TERMINATION, CapabilityStatus.FULL),
        CapabilityResult(POSTGRESQL_TOP_SQL, CapabilityStatus.DEGRADED, "sem extensao"),
    ]
    assert has_missing_mandatory_capability("postgresql", capabilities) is False


def test_all_full_does_not_reject() -> None:
    capabilities = [
        CapabilityResult(SQLSERVER_SERVER_STATE, CapabilityStatus.FULL),
        CapabilityResult(SQLSERVER_SESSION_TERMINATION, CapabilityStatus.FULL),
    ]
    assert has_missing_mandatory_capability("sqlserver", capabilities) is False


def test_missing_mandatory_capability_rejects_sqlserver() -> None:
    capabilities = [
        CapabilityResult(SQLSERVER_SERVER_STATE, CapabilityStatus.UNAVAILABLE, "sem permissao"),
    ]
    assert has_missing_mandatory_capability("sqlserver", capabilities) is True


def test_connectivity_unavailable_rejects_regardless_of_engine() -> None:
    capabilities = [
        CapabilityResult(CONNECTIVITY, CapabilityStatus.UNAVAILABLE, "Falha ao conectar: OSError"),
    ]
    assert has_missing_mandatory_capability("postgresql", capabilities) is True
    assert has_missing_mandatory_capability("sqlserver", capabilities) is True
