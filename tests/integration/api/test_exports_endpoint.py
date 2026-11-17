"""`POST /api/v1/exports/table-pdf` — PDF generico de tabela (sem banco)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from httpx import AsyncClient

from app.api.deps import CurrentUser, get_current_user
from app.api.v1.exports import safe_filename
from app.main import app


@pytest.fixture
def authenticated() -> Iterator[None]:
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(1, "tester", "viewer")
    yield
    app.dependency_overrides.pop(get_current_user, None)


def _payload(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "title": "Index health",
        "subtitle": "prod · appdb",
        "filename": "index health 2026/11/17",
        "columns": [
            {"key": "name", "label": "Index"},
            {"key": "size", "label": "Size", "align": "right"},
        ],
        "rows": [{"name": "ix_a", "size": 1.5}, {"name": "ix_b", "size": None}],
    }
    body.update(overrides)
    return body


async def test_requires_authentication(client: AsyncClient) -> None:
    # O conftest deste pacote injeta um admin em todos os testes — removido so aqui.
    stub = app.dependency_overrides.pop(get_current_user, None)
    try:
        response = await client.post("/api/v1/exports/table-pdf", json=_payload())
    finally:
        if stub is not None:
            app.dependency_overrides[get_current_user] = stub
    assert response.status_code == 401


async def test_returns_pdf(client: AsyncClient, authenticated: None) -> None:
    response = await client.post("/api/v1/exports/table-pdf", json=_payload())
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert 'filename="index_health_2026_11_17.pdf"' in response.headers["content-disposition"]
    assert response.content.startswith(b"%PDF-")


async def test_rejects_oversized_payloads(client: AsyncClient, authenticated: None) -> None:
    too_many_cols = [{"key": f"c{i}", "label": "x"} for i in range(31)]
    r1 = await client.post("/api/v1/exports/table-pdf", json=_payload(columns=too_many_cols))
    assert r1.status_code == 422
    huge_cell = [{"name": "x" * 2001, "size": 1}]
    r2 = await client.post("/api/v1/exports/table-pdf", json=_payload(rows=huge_cell))
    assert r2.status_code == 422
    r3 = await client.post("/api/v1/exports/table-pdf", json=_payload(columns=[]))
    assert r3.status_code == 422


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, "export.pdf"),
        ("report.pdf", "report.pdf"),
        ('a"; rm -rf /', "a_rm_-rf.pdf"),
        ("../../etc/passwd", "etc_passwd.pdf"),
    ],
)
def test_safe_filename(raw: str | None, expected: str) -> None:
    assert safe_filename(raw) == expected
