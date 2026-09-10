"""Testes da interface `Collector` e do modelo de capacidade — `app/collectors/base.py`."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult, Collector
from app.collectors.postgresql.adapter import PostgreSQLCollector
from app.collectors.sqlserver.adapter import SQLServerCollector


def test_postgresql_collector_satisfies_protocol() -> None:
    collector = PostgreSQLCollector(instance_id=uuid4())
    assert isinstance(collector, Collector)


def test_sqlserver_collector_satisfies_protocol() -> None:
    collector = SQLServerCollector(instance_id=uuid4())
    assert isinstance(collector, Collector)


def test_capability_status_values() -> None:
    assert {s.value for s in CapabilityStatus} == {"full", "degraded", "unavailable"}


def test_capability_result_is_immutable() -> None:
    result = CapabilityResult(capability="top_sql", status=CapabilityStatus.DEGRADED, reason="x")
    assert result.capability == "top_sql"
    assert result.status is CapabilityStatus.DEGRADED


def test_collection_result_defaults_to_empty_samples() -> None:
    result = CollectionResult(
        instance_id=uuid4(),
        collected_at=datetime.now(UTC),
        duration_ms=10,
    )
    assert result.metric_samples == []
    assert result.query_samples == []
    assert result.capabilities == []
