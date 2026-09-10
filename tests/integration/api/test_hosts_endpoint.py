"""Testes de integracao de `/api/v1/hosts` contra o banco real `ab_xql_pm_dev`.

Mesmo padrao de `tests/integration/api/test_metrics_endpoint.py`: `client` (ASGI transport,
`tests/conftest.py`) e `db_session` (`tests/integration/conftest.py`) apontam para a mesma
`DATABASE_URL`, entao os dados persistidos por uma requisicao do `client` sao visiveis via
`db_session` no mesmo teste.
"""

from __future__ import annotations

from uuid import uuid4

import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.server import Server
from app.repositories._tables import metric_samples_table


async def test_post_metrics_creates_server_and_metric_samples(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    hostname = f"test-host-{uuid4()}"
    server_id: str | None = None
    try:
        response = await client.post(
            f"/api/v1/hosts/{hostname}/metrics",
            json={
                "os_type": "windows",
                "metrics": [
                    {"name": "host_load1", "value": 1.5},
                    {"name": "host_mem_used_bytes", "value": 123.0, "labels": {"unit": "bytes"}},
                ],
            },
        )

        assert response.status_code == 202
        body = response.json()
        assert body["ingested"] == 2
        server_id = body["server_id"]

        server = await db_session.get(Server, server_id)
        assert server is not None
        assert server.hostname == hostname
        assert server.os_type == "windows"
        assert server.active is True

        rows = (
            await db_session.execute(
                sa.select(metric_samples_table).where(metric_samples_table.c.target_id == server.id)
            )
        ).fetchall()
        assert len(rows) == 2
        assert {row.metric_name for row in rows} == {"host_load1", "host_mem_used_bytes"}
        assert all(row.target_type == "server" for row in rows)
        labelled = next(row for row in rows if row.metric_name == "host_mem_used_bytes")
        assert labelled.labels == {"unit": "bytes"}
    finally:
        if server_id is not None:
            await db_session.execute(
                sa.delete(metric_samples_table).where(metric_samples_table.c.target_id == server_id)
            )
        await db_session.execute(sa.delete(Server).where(Server.hostname == hostname))
        await db_session.commit()


async def test_post_metrics_reuses_existing_server_on_second_call(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    hostname = f"test-host-{uuid4()}"
    server_id: str | None = None
    try:
        first = await client.post(
            f"/api/v1/hosts/{hostname}/metrics",
            json={"os_type": "linux", "metrics": [{"name": "host_load1", "value": 1.0}]},
        )
        second = await client.post(
            f"/api/v1/hosts/{hostname}/metrics",
            json={"os_type": "linux", "metrics": [{"name": "host_load1", "value": 2.0}]},
        )

        assert first.status_code == 202
        assert second.status_code == 202
        server_id = first.json()["server_id"]
        assert second.json()["server_id"] == server_id

        rows = (
            (await db_session.execute(sa.select(Server).where(Server.hostname == hostname)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
    finally:
        if server_id is not None:
            await db_session.execute(
                sa.delete(metric_samples_table).where(metric_samples_table.c.target_id == server_id)
            )
        await db_session.execute(sa.delete(Server).where(Server.hostname == hostname))
        await db_session.commit()


async def test_get_hosts_lists_active_servers(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    hostname = f"test-host-{uuid4()}"
    server_id: str | None = None
    try:
        post_response = await client.post(
            f"/api/v1/hosts/{hostname}/metrics",
            json={"os_type": "windows", "metrics": [{"name": "host_load1", "value": 1.0}]},
        )
        server_id = post_response.json()["server_id"]

        response = await client.get("/api/v1/hosts")

        assert response.status_code == 200
        hostnames = {host["hostname"] for host in response.json()}
        assert hostname in hostnames
    finally:
        if server_id is not None:
            await db_session.execute(
                sa.delete(metric_samples_table).where(metric_samples_table.c.target_id == server_id)
            )
        await db_session.execute(sa.delete(Server).where(Server.hostname == hostname))
        await db_session.commit()
