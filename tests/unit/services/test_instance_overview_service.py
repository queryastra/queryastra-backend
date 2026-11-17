"""Testes das partes puras do dashboard de visao geral."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from app.models.instance import Instance
from app.normalizers.metric_names import (
    COLLECTOR_CYCLE_DURATION_MS,
    COLLECTOR_CYCLE_ERROR_COUNT,
    PG_ACTIVE_SESSION_COUNT,
    PG_NUMBACKENDS,
    SQLSERVER_INSTANCE_CPU_PERCENT,
    SQLSERVER_USER_CONNECTIONS,
)
from app.services.instance_overview_service import (
    aggregate_status,
    build_card,
    sort_cards,
)

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)


def _instance(name: str = "db", db_type: str = "postgresql", active: bool = True) -> Instance:
    return Instance(
        id=uuid4(),
        name=name,
        db_type=db_type,
        host="h",
        port=5432,
        criticality="standard",
        active=active,
    )


def test_aggregate_status_precedence() -> None:
    assert aggregate_status("healthy", {}, active=False) == "paused"
    assert aggregate_status("unavailable", {"critical": 2}, active=True) == "down"
    assert aggregate_status("healthy", {"critical": 1, "warning": 3}, active=True) == "critical"
    assert aggregate_status("healthy", {"warning": 1}, active=True) == "warning"
    assert aggregate_status("healthy", {}, active=True) == "up"
    assert aggregate_status("unknown", {}, active=True) == "unknown"


def test_build_card_postgres_from_latest_values() -> None:
    inst = _instance()
    card = build_card(
        inst,
        latest={
            COLLECTOR_CYCLE_DURATION_MS: (120.0, NOW),
            COLLECTOR_CYCLE_ERROR_COUNT: (0.0, NOW),
            PG_NUMBACKENDS: (42.0, NOW),
            PG_ACTIVE_SESSION_COUNT: (5.0, NOW),
        },
        open_alerts={"warning": 2},
        deadlocks_last_hour=3,
    )
    assert card.health_status == "healthy"
    assert card.status == "warning"
    assert card.connections == 42.0
    assert card.active_sessions == 5.0
    assert card.cpu_percent is None
    assert card.deadlocks_last_hour == 3
    assert card.open_alerts_total == 2
    assert card.last_collected_at == NOW


def test_build_card_sqlserver_with_errors_is_down() -> None:
    inst = _instance(db_type="sqlserver")
    card = build_card(
        inst,
        latest={
            COLLECTOR_CYCLE_DURATION_MS: (100.0, NOW),
            COLLECTOR_CYCLE_ERROR_COUNT: (1.0, NOW),
            SQLSERVER_USER_CONNECTIONS: (10.0, NOW),
            SQLSERVER_INSTANCE_CPU_PERCENT: (37.5, NOW),
        },
        open_alerts={},
        deadlocks_last_hour=0,
    )
    assert card.status == "down"
    assert card.connections == 10.0
    assert card.cpu_percent == 37.5


def test_build_card_uses_fallback_health_without_recent_cycle() -> None:
    inst = _instance()
    old = datetime(2026, 11, 1, tzinfo=UTC)
    card = build_card(
        inst,
        latest={},
        open_alerts={},
        deadlocks_last_hour=None,
        fallback_health=("healthy", old),
    )
    assert card.health_status == "healthy"
    assert card.last_collected_at == old
    no_data = build_card(inst, latest={}, open_alerts={}, deadlocks_last_hour=None)
    assert no_data.status == "unknown"
    assert no_data.last_collected_at is None


def test_naive_cycle_time_is_made_utc() -> None:
    card = build_card(
        _instance(),
        latest={COLLECTOR_CYCLE_DURATION_MS: (1.0, datetime(2026, 11, 17, 12, 0))},
        open_alerts={},
        deadlocks_last_hour=None,
    )
    assert card.last_collected_at is not None
    assert card.last_collected_at.tzinfo is not None


def test_sort_cards_worst_first_then_name() -> None:
    def card(name: str, health: str, alerts: dict[str, int], active: bool = True):  # noqa: ANN202
        return build_card(
            _instance(name=name, active=active),
            latest={},
            open_alerts=alerts,
            deadlocks_last_hour=None,
            fallback_health=(health, None),
        )

    cards = [
        card("b-up", "healthy", {}),
        card("a-up", "healthy", {}),
        card("crit", "healthy", {"critical": 1}),
        card("down", "unavailable", {}),
        card("paused", "healthy", {}, active=False),
    ]
    assert [c.name for c in sort_cards(cards)] == ["down", "crit", "a-up", "b-up", "paused"]
