from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.deps import CurrentUser
from app.api.v1 import websocket as ws_module
from app.core.events import event_bus


def _app(monkeypatch) -> FastAPI:
    user = CurrentUser(user_id=1, username="admin", role="admin")
    monkeypatch.setattr(ws_module, "_user_from_token", lambda token: user if token else None)

    async def _active(_user_id: int) -> CurrentUser:
        return user

    monkeypatch.setattr(ws_module, "load_active_user", _active)
    app = FastAPI()
    app.include_router(ws_module.router)
    return app


def test_client_close_ends_handler_and_unsubscribes(monkeypatch):
    before = len(event_bus._subscribers)
    with TestClient(_app(monkeypatch)) as client:
        with client.websocket_connect("/ws/live?token=t") as socket:
            assert len(event_bus._subscribers) == before + 1
            socket.close()
    assert len(event_bus._subscribers) == before


def test_missing_token_is_rejected(monkeypatch):
    with TestClient(_app(monkeypatch)) as client:
        try:
            with client.websocket_connect("/ws/live"):
                raise AssertionError("connection should have been refused")
        except Exception as exc:  # noqa: BLE001
            assert getattr(exc, "code", None) == 1008
