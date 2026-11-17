"""CRUD de `/api/v1/annotations` contra o banco real (`ab_xql_pm_dev`, migration
`f7a8b9c0d1e2_annotations` aplicada). Usa anotacoes GLOBAIS (sem instancia) para nao depender de
fixture de instancia; cada teste apaga o que criou."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient

from app.api.deps import CurrentUser, get_current_user
from app.main import app


@pytest.fixture
def as_viewer() -> Iterator[None]:
    previous = app.dependency_overrides.get(get_current_user)
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(999, "viewer", "viewer")
    yield
    if previous is not None:
        app.dependency_overrides[get_current_user] = previous


async def test_annotation_crud_roundtrip(client: AsyncClient) -> None:
    occurred = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)
    created = await client.post(
        "/api/v1/annotations",
        json={
            "occurred_at": occurred.isoformat(),
            "title": "  deploy v2.3  ",
            "category": "deploy",
            "description": "rollout",
        },
    )
    assert created.status_code == 201, created.text
    body = created.json()
    annotation_id = body["id"]
    try:
        assert body["title"] == "deploy v2.3"
        assert body["instance_id"] is None
        assert body["category"] == "deploy"

        listed = await client.get(
            "/api/v1/annotations",
            params={
                "from": (occurred - timedelta(minutes=1)).isoformat(),
                "to": (occurred + timedelta(minutes=1)).isoformat(),
            },
        )
        assert listed.status_code == 200
        assert annotation_id in {a["id"] for a in listed.json()}

        outside = await client.get(
            "/api/v1/annotations",
            params={"from": (occurred + timedelta(minutes=5)).isoformat()},
        )
        assert annotation_id not in {a["id"] for a in outside.json()}

        patched = await client.patch(
            f"/api/v1/annotations/{annotation_id}",
            json={"title": "deploy v2.4", "category": "config"},
        )
        assert patched.status_code == 200
        assert patched.json()["title"] == "deploy v2.4"
        assert patched.json()["category"] == "config"

        fetched = await client.get(f"/api/v1/annotations/{annotation_id}")
        assert fetched.json()["description"] == "rollout"
    finally:
        deleted = await client.delete(f"/api/v1/annotations/{annotation_id}")
        assert deleted.status_code == 204
    gone = await client.get(f"/api/v1/annotations/{annotation_id}")
    assert gone.status_code == 404


async def test_annotation_validation(client: AsyncClient) -> None:
    now = datetime.now(UTC).isoformat()
    bad_category = await client.post(
        "/api/v1/annotations", json={"occurred_at": now, "title": "x", "category": "nope"}
    )
    assert bad_category.status_code == 422
    blank = await client.post("/api/v1/annotations", json={"occurred_at": now, "title": "   "})
    assert blank.status_code == 422


async def test_viewer_cannot_create(client: AsyncClient, as_viewer: None) -> None:
    response = await client.post(
        "/api/v1/annotations",
        json={"occurred_at": datetime.now(UTC).isoformat(), "title": "x"},
    )
    assert response.status_code == 403
