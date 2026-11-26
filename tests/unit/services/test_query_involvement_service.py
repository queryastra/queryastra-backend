"""Testes de `build_query_involvement` com repositórios falsos (sem banco)."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from app.services.query_involvement_service import build_query_involvement

_NOW = datetime(2026, 11, 26, 12, tzinfo=UTC)
_HASH = "abc123"


def _incident(**overrides: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "id": uuid4(),
        "blocker_session_id": "51",
        "blocked_session_id": "52",
        "blocker_sql_hash": None,
        "blocked_sql_hash": None,
        "blocker_database_name": "DB1",
        "blocked_database_name": "DB1",
        "blocker_machine": "app-1",
        "blocked_machine": "app-2",
        "blocker_transaction_id": None,
        "blocked_transaction_id": None,
        "blocker_linked_instance_id": None,
        "blocked_linked_instance_id": None,
        "started_at": _NOW,
        "last_seen_at": _NOW,
        "ended_at": None,
        "wait_time_ms": 1500,
        "blocked_object": "dbo.t",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _tx(identifier: str, session_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        transaction_identifier=identifier,
        session_id=session_id,
        database_name="DB1",
        begin_time=_NOW,
        closed_at=None,
        flag_reason="bloqueio",
    )


class _Repo:
    def __init__(self, **methods: Any) -> None:
        for name, value in methods.items():

            async def _fn(*_a: Any, _v: Any = value, **_k: Any) -> Any:
                return _v

            setattr(self, name, _fn)


@pytest.mark.asyncio
async def test_build_query_involvement_links_all_sections() -> None:
    linked_id = uuid4()
    incidents = [
        _incident(
            blocker_sql_hash=_HASH,
            blocker_transaction_id="T1",
            blocker_linked_instance_id=linked_id,
        ),
        # mesma query dos dois lados → duas linhas
        _incident(blocker_sql_hash=_HASH, blocked_sql_hash=_HASH),
    ]
    remote_instance = uuid4()
    result = await build_query_involvement(
        instance_id=uuid4(),
        sql_hash=_HASH,
        since=_NOW,
        until=_NOW,
        blocking_repository=_Repo(find_by_sql_hash=incidents),  # type: ignore[arg-type]
        transactions_repository=_Repo(  # type: ignore[arg-type]
            find_by_identifiers=[_tx("T1", "51")],
            # T1 também aparece na heurística → não duplica; T2 só heurística → probable
            find_overlapping_sql_hash=[(_tx("T1", "51"), 3, _NOW), (_tx("T2", "77"), 2, _NOW)],
        ),
        deadlocks_repository=_Repo(list_for_instance=[]),  # type: ignore[arg-type]
        query_samples_repository=_Repo(  # type: ignore[arg-type]
            machines_for_sql_hash=[
                SimpleNamespace(
                    machine="SQLPROD02.corp.local",
                    sample_count=9,
                    first_seen_at=_NOW,
                    last_seen_at=_NOW,
                ),
                SimpleNamespace(
                    machine="app-1", sample_count=4, first_seen_at=_NOW, last_seen_at=_NOW
                ),
            ]
        ),
        instances_repository=_Repo(  # type: ignore[arg-type]
            get_names_by_ids={linked_id: "SQLPROD01"},
            host_map_excluding={"sqlprod02": (remote_instance, "SQLPROD02")},
        ),
    )

    assert [(b.role, b.session_id) for b in result.blocking] == [
        ("blocker", "51"),
        ("blocker", "51"),
        ("blocked", "52"),
    ]
    assert result.blocking[0].linked_instance_name == "SQLPROD01"
    assert [(t.transaction_identifier, t.match) for t in result.transactions] == [
        ("T1", "exact"),
        ("T2", "probable"),
    ]
    assert result.transactions[1].sample_count == 2
    assert [(s.machine, s.instance_name) for s in result.linked_sources] == [
        ("SQLPROD02.corp.local", "SQLPROD02")
    ]
    assert result.deadlocks == []
