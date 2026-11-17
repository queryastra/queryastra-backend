"""Testes de `app.services.notification_service` e do filtro por usuario de `/ws/live`."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

from app.api.deps import CurrentUser
from app.api.v1.websocket import event_visible_to
from app.core.events import EventBus
from app.models.alert import Alert
from app.models.notification import Notification
from app.services.alert_events import build_alert_event
from app.services.notification_service import NotificationService, should_notify


class _FakeSession:
    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakeAccessRepo:
    def __init__(self, user_ids: set[int]) -> None:
        self.user_ids = user_ids
        self.queried: list[UUID] = []

    async def list_user_ids_with_access(self, instance_id: UUID) -> set[int]:
        self.queried.append(instance_id)
        return self.user_ids


class _FakeNotificationsRepo:
    def __init__(self) -> None:
        self.created: list[Notification] = []

    async def create_many(self, rows: list[Notification]) -> list[Notification]:
        for index, row in enumerate(rows, start=1):
            row.id = index
            row.created_at = datetime.now(UTC)
        self.created.extend(rows)
        return rows


class _FakeInstancesRepo:
    async def get_by_id(self, instance_id: UUID) -> SimpleNamespace:
        return SimpleNamespace(id=instance_id, name="prod-sql-01")


def _alert(severity: str = "critical") -> Alert:
    return Alert(
        id=uuid4(),
        alert_rule_id=uuid4(),
        target_type="instance",
        target_id=uuid4(),
        severity=severity,
        state="open",
    )


def _service(bus: EventBus, access: _FakeAccessRepo, notifications: _FakeNotificationsRepo):
    return NotificationService(
        lambda: _FakeSession(),  # type: ignore[arg-type,return-value]
        bus=bus,
        access_repository_factory=lambda _s: access,  # type: ignore[arg-type,return-value]
        notifications_repository_factory=lambda _s: notifications,  # type: ignore[arg-type,return-value]
        instances_repository_factory=lambda _s: _FakeInstancesRepo(),  # type: ignore[arg-type,return-value]
    )


def test_should_notify_rules() -> None:
    created_warning = build_alert_event(_alert("warning"), kind="triggered", rule_name="r")
    assert created_warning["type"] == "alert_created"
    assert should_notify(created_warning)
    assert should_notify(build_alert_event(_alert("critical"), kind="level_changed"))
    assert should_notify(build_alert_event(_alert("critical"), kind="escalated"))
    assert not should_notify(build_alert_event(_alert("warning"), kind="level_changed"))
    assert not should_notify(build_alert_event(_alert("critical"), kind="reminder"))
    assert not should_notify(build_alert_event(_alert("critical"), kind="recovered"))
    assert not should_notify({"type": "instance_collected", "instance_id": str(uuid4())})
    # recorrencia cria um Alert NOVO -> alert_created
    assert build_alert_event(_alert(), kind="recurrence_escalated")["type"] == "alert_created"


async def test_handle_event_creates_one_row_per_user_and_publishes() -> None:
    bus = EventBus()
    queue = bus.subscribe()
    access = _FakeAccessRepo({3, 1})
    notifications = _FakeNotificationsRepo()
    service = _service(bus, access, notifications)
    alert = _alert("critical")

    created = await service.handle_event(
        build_alert_event(alert, kind="triggered", rule_name="CPU alta")
    )

    assert [n.user_id for n in created] == [1, 3]
    assert access.queried == [alert.target_id]
    assert created[0].title == "CPU alta — prod-sql-01"
    assert created[0].severity == "critical"
    assert created[0].link == "/alerts"
    assert created[0].alert_id == alert.id
    published = [queue.get_nowait(), queue.get_nowait()]
    assert {e["user_id"] for e in published} == {1, 3}
    assert all(e["type"] == "notification_created" for e in published)
    assert published[0]["notification"]["instance_id"] == str(alert.target_id)


async def test_handle_event_ignores_non_notifiable_events() -> None:
    bus = EventBus()
    notifications = _FakeNotificationsRepo()
    service = _service(bus, _FakeAccessRepo({1}), notifications)

    result = await service.handle_event(build_alert_event(_alert(), kind="recovered"))

    assert result == []
    assert notifications.created == []


# --- filtro do WebSocket ---------------------------------------------------------------------


def test_ws_notification_only_to_its_user() -> None:
    event = {"type": "notification_created", "user_id": 5}
    assert event_visible_to(event, CurrentUser(5, "u", "viewer"), set())
    assert not event_visible_to(event, CurrentUser(6, "x", "admin"), None)


def test_ws_instance_events_filtered_by_access() -> None:
    allowed, denied = uuid4(), uuid4()
    viewer = CurrentUser(2, "v", "viewer")
    assert event_visible_to(
        {"type": "alert_created", "instance_id": str(allowed)}, viewer, {allowed}
    )
    assert not event_visible_to(
        {"type": "alert_created", "instance_id": str(denied)}, viewer, {allowed}
    )
    assert not event_visible_to(
        {"type": "instance_collected", "instance_id": str(denied)}, viewer, None
    )
    assert event_visible_to(
        {"type": "alert_created", "instance_id": str(denied)}, CurrentUser(1, "a", "admin"), None
    )
    assert event_visible_to({"type": "something_global"}, viewer, set())
