"""Teste de integracao de `GET /api/v1/topology` contra o banco real `ab_xql_pm_dev` — Mapa de
Dependencias, correlacao agentless via `query_samples.machine`
(ver `app.services.topology_service`)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedQuerySample
from app.repositories.query_samples_repository import QuerySamplesRepository


async def test_get_topology_detects_edge_between_two_registered_instances(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """`test_instance` (o alvo sendo consultado) recebe uma amostra cujo `machine` bate com o
    `host` de `caller` — deve virar uma aresta `caller -> test_instance`, e ambos devem aparecer
    como nós do tipo "instance". `caller` usa um host bem distintivo (não `127.0.0.1`, que
    colide com `demo-postgres-local`, uma instância real já cadastrada no dev DB compartilhado)."""
    caller = Instance(
        name=f"test-instance-{uuid4()}",
        db_type="sqlserver",
        host="10.250.99.201",
        port=1433,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
    )
    db_session.add(caller)
    await db_session.commit()
    await db_session.refresh(caller)

    try:
        repo = QuerySamplesRepository(db_session)
        await repo.insert_batch(
            [
                NormalizedQuerySample(
                    time=datetime.now(UTC),
                    instance_id=test_instance.id,
                    engine="postgresql",
                    sql_hash="hash-topology",
                    sql_text_masked="SELECT ? FROM t",
                    machine=caller.host,
                )
            ]
        )

        response = await client.get("/api/v1/topology")

        assert response.status_code == 200
        body = response.json()
        nodes_by_id = {node["id"]: node for node in body["nodes"]}
        assert nodes_by_id[str(caller.id)]["kind"] == "instance"
        assert nodes_by_id[str(test_instance.id)]["kind"] == "instance"

        edges = {(edge["source"], edge["target"]) for edge in body["edges"]}
        assert (str(caller.id), str(test_instance.id)) in edges
    finally:
        await db_session.delete(caller)
        await db_session.commit()


async def test_get_topology_unmatched_machine_becomes_external_node(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Uma `machine` que não bate com nenhuma instância/servidor cadastrado vira um nó "externo"
    rotulado com o valor bruto observado — nunca inventa uma identidade conhecida."""
    repo = QuerySamplesRepository(db_session)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=datetime.now(UTC),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-external",
                sql_text_masked="SELECT ? FROM t",
                machine="app-pod-xyz-12345",
            )
        ]
    )

    response = await client.get("/api/v1/topology")

    assert response.status_code == 200
    body = response.json()
    external_node = next(
        (n for n in body["nodes"] if n["id"] == "external:app-pod-xyz-12345"), None
    )
    assert external_node is not None
    assert external_node["kind"] == "external"
    assert external_node["label"] == "app-pod-xyz-12345"
