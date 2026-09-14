"""Testes de `NormalizedBlockingEvent` — `app/normalizers/models.py`."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.normalizers.models import NormalizedBlockingEvent


def _make_event(**overrides: object) -> NormalizedBlockingEvent:
    base: dict[str, object] = {
        "time": datetime.now(UTC),
        "instance_id": uuid4(),
        "blocker_session_id": "10",
        "blocked_session_id": "20",
        "wait_time_ms": 150,
    }
    base.update(overrides)
    return NormalizedBlockingEvent(**base)  # type: ignore[arg-type]


def test_blocking_event_is_immutable() -> None:
    event = _make_event()
    with pytest.raises(ValidationError):
        event.wait_time_ms = 999  # type: ignore[misc]


def test_blocking_event_required_fields_present() -> None:
    event = _make_event()
    assert event.blocker_session_id == "10"
    assert event.blocked_session_id == "20"
    assert event.wait_time_ms == 150
    assert event.blocker_sql_hash is None
    assert event.blocked_sql_hash is None


def test_blocking_event_missing_wait_time_ms_raises() -> None:
    with pytest.raises(ValidationError):
        NormalizedBlockingEvent(
            time=datetime.now(UTC),
            instance_id=uuid4(),
            blocker_session_id="1",
            blocked_session_id="2",
        )  # type: ignore[call-arg]


def test_blocking_event_optional_sql_hashes_can_be_set() -> None:
    event = _make_event(blocker_sql_hash="abc", blocked_sql_hash="def")
    assert event.blocker_sql_hash == "abc"
    assert event.blocked_sql_hash == "def"


def test_blocking_event_enrichment_fields_default_to_none() -> None:
    """Etapa 9 — todos os campos de enriquecimento (identidade/wait/recurso) sao opcionais,
    aditivos: um evento antigo (ou um coletor que nao os preencheu) fica so com None."""
    event = _make_event()
    assert event.blocker_db_user is None
    assert event.blocker_program is None
    assert event.blocker_machine is None
    assert event.blocker_wait_event_native is None
    assert event.blocked_db_user is None
    assert event.blocked_program is None
    assert event.blocked_machine is None
    assert event.blocked_wait_event_native is None
    assert event.blocked_wait_resource is None
    assert event.blocked_object is None


def test_blocking_event_enrichment_fields_can_be_set() -> None:
    event = _make_event(
        blocker_db_user="idle_user",
        blocker_program="batch_job",
        blocker_machine="batch-host",
        blocked_db_user="app_user",
        blocked_program="psql",
        blocked_machine="10.0.0.5",
        blocked_wait_event_native="relation",
        blocked_wait_resource="relation/AccessShareLock",
        blocked_object="t",
    )
    assert event.blocker_db_user == "idle_user"
    assert event.blocker_program == "batch_job"
    assert event.blocker_machine == "batch-host"
    assert event.blocked_db_user == "app_user"
    assert event.blocked_program == "psql"
    assert event.blocked_machine == "10.0.0.5"
    assert event.blocked_wait_event_native == "relation"
    assert event.blocked_wait_resource == "relation/AccessShareLock"
    assert event.blocked_object == "t"
