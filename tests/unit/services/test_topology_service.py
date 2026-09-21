from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from app.models.instance import Instance
from app.models.server import Server
from app.services.topology_service import TopologyService


@dataclass
class _FakeRow:
    instance_id: object
    machine: str | None
    sample_count: int


class _FakeQuerySamplesRepository:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    async def list_distinct_machines_by_instance(self, since: datetime) -> list[_FakeRow]:
        return self._rows


def _instance(*, host: str, db_type: str = "sqlserver") -> Instance:
    return Instance(id=uuid4(), name=host, db_type=db_type, host=host)


def _server(*, hostname: str) -> Server:
    return Server(id=uuid4(), hostname=hostname, os_type="linux", exporter_endpoint="x")


async def test_build_dependency_graph_resolves_instance_server_and_external_nodes() -> None:
    """3 desfechos possíveis pra uma `machine` observada: bate com outra instância cadastrada
    (aresta instância→instância), bate com um `Server` monitorado (aresta server→instância,
    identificado mesmo sem ser uma instância de banco), ou não bate com nada conhecido (nó
    "externo", rotulado com o valor bruto — nunca inventa identidade)."""
    caller_instance = _instance(host="10.0.0.1")
    target_instance = _instance(host="10.0.0.2")
    known_server = _server(hostname="app-server-01")

    rows = [
        _FakeRow(instance_id=target_instance.id, machine="10.0.0.1", sample_count=5),
        _FakeRow(instance_id=target_instance.id, machine="app-server-01", sample_count=3),
        _FakeRow(instance_id=target_instance.id, machine="10.9.9.9", sample_count=1),
    ]
    service = TopologyService(_FakeQuerySamplesRepository(rows))

    graph = await service.build_dependency_graph(
        since=datetime(2026, 1, 1, tzinfo=UTC),
        instances=[caller_instance, target_instance],
        servers=[known_server],
        health_status_by_instance_id={
            str(caller_instance.id): "healthy",
            str(target_instance.id): "unavailable",
        },
    )

    node_ids = {node.id for node in graph.nodes}
    assert str(caller_instance.id) in node_ids
    assert str(target_instance.id) in node_ids
    assert "server:app-server-01" in node_ids
    assert "external:10.9.9.9" in node_ids

    target_node = next(n for n in graph.nodes if n.id == str(target_instance.id))
    assert target_node.health_status == "unavailable"

    edges = {(edge.source, edge.target, edge.sample_count) for edge in graph.edges}
    assert edges == {
        (str(caller_instance.id), str(target_instance.id), 5),
        ("server:app-server-01", str(target_instance.id), 3),
        ("external:10.9.9.9", str(target_instance.id), 1),
    }


async def test_build_dependency_graph_ignores_self_loop() -> None:
    """Uma instância aparecendo como "cliente dela mesma" (ex.: uma ferramenta de admin rodando
    no próprio host do banco) não é uma dependência — não gera aresta nem duplica o nó."""
    instance = _instance(host="10.0.0.5")
    rows = [_FakeRow(instance_id=instance.id, machine="10.0.0.5", sample_count=7)]
    service = TopologyService(_FakeQuerySamplesRepository(rows))

    graph = await service.build_dependency_graph(
        since=datetime(2026, 1, 1, tzinfo=UTC),
        instances=[instance],
        servers=[],
        health_status_by_instance_id={str(instance.id): "healthy"},
    )

    assert graph.edges == []
    assert [node.id for node in graph.nodes] == [str(instance.id)]


async def test_build_dependency_graph_ignores_row_for_removed_instance() -> None:
    """Uma linha cujo `instance_id` não está mais entre as instâncias ativas (desativada/excluída
    depois da amostra) é ignorada — nunca inventa um nó pra uma instância que não existe mais."""
    rows = [_FakeRow(instance_id=uuid4(), machine="10.0.0.1", sample_count=2)]
    service = TopologyService(_FakeQuerySamplesRepository(rows))

    graph = await service.build_dependency_graph(
        since=datetime(2026, 1, 1, tzinfo=UTC),
        instances=[],
        servers=[],
        health_status_by_instance_id={},
    )

    assert graph.nodes == []
    assert graph.edges == []
