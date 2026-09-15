"""Testes de `SqlInsightClient` — sempre degrada para `available=False` sem propagar excecao,
independente do motivo (desligado, timeout, 5xx, resposta malformada)."""

from __future__ import annotations

import httpx

from app.services.sql_insight_client import SqlInsightClient


async def test_check_returns_unavailable_when_base_url_not_configured() -> None:
    client = SqlInsightClient(base_url=None, timeout_seconds=3.0)
    analysis = await client.check("SELECT ? FROM t")
    assert analysis.available is False
    assert analysis.rules == []


async def test_check_returns_rules_on_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/check"
        return httpx.Response(
            200,
            json={
                "rules": [
                    {
                        "id": "R001",
                        "description": "Evite SELECT *",
                        "details": "use colunas explicitas",
                    },
                    {"id": "R002", "description": "NOLOCK sem necessidade"},
                ]
            },
        )

    client = SqlInsightClient(
        base_url="http://autklinux4:9100/",
        timeout_seconds=3.0,
        transport=httpx.MockTransport(handler),
    )
    analysis = await client.check("SELECT * FROM t WITH (NOLOCK)")
    assert analysis.available is True
    assert [r.rule_id for r in analysis.rules] == ["R001", "R002"]
    assert analysis.rules[0].description == "Evite SELECT *"
    assert analysis.rules[0].details == "use colunas explicitas"
    assert analysis.rules[1].details is None


async def test_check_degrades_on_http_error_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    client = SqlInsightClient(
        base_url="http://autklinux4:9100",
        timeout_seconds=3.0,
        transport=httpx.MockTransport(handler),
    )
    analysis = await client.check("SELECT ? FROM t")
    assert analysis.available is False
    assert analysis.rules == []


async def test_check_degrades_on_timeout() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    client = SqlInsightClient(
        base_url="http://autklinux4:9100",
        timeout_seconds=3.0,
        transport=httpx.MockTransport(handler),
    )
    analysis = await client.check("SELECT ? FROM t")
    assert analysis.available is False
    assert analysis.rules == []


async def test_check_degrades_on_malformed_json_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = SqlInsightClient(
        base_url="http://autklinux4:9100",
        timeout_seconds=3.0,
        transport=httpx.MockTransport(handler),
    )
    analysis = await client.check("SELECT ? FROM t")
    assert analysis.available is False
    assert analysis.rules == []
