"""Serviço de "Mapa de Dependências" — `ARCHITECTURE_SPEC.md`/roadmap (Fase 3, "Topologia & Causa
Raiz"), item independente do "status agregado com drill-down" (esse depende do mapa existir
primeiro, adiado — ver decisão registrada nesta sessão).

Escopo desta primeira versão, deliberadamente honesto sobre suas limitações (mesmo espírito de
`comparativo-metodologico-solarwinds.md`): **não** é uma sonda de rede contínua medindo
latência/perda de pacote como o "Application Dependencies" do SolarWinds SAM — não existe nenhum
agente de rede neste projeto (`ab-xql-pm-agents` só expõe métricas de SO via
node_exporter/windows_exporter). É, em vez disso, uma correlação 100% agentless sobre dado JÁ
coletado: toda amostra de sessão ativa já grava `machine` (IP/hostname do cliente que abriu
aquela sessão — `client_addr` no PostgreSQL, `host_name` no SQL Server). Quando essa `machine`
bate com o `host` de OUTRA instância cadastrada, ou com o `hostname` de um `Server` monitorado, é
uma dependência real detectada (ex.: linked server, `dblink`, job rodando no mesmo host de outra
instância) — sem inventar nada, sem sonda nova. Quando não bate com nada conhecido, vira um nó
"externo" rotulado com o valor bruto observado (nunca fabrica um nome).

Limitações documentadas explicitamente (não são bugs):
    - Só detecta o que foi observado numa sessão ATIVA durante os ciclos de poll da janela `since`
      — mesma limitação de "foto periódica" de todo o resto da coleta.
    - Correlação por igualdade de string (case-insensitive) entre `machine` e `host`/`hostname` —
      sem resolução de DNS. Uma instância cadastrada por hostname que só aparece via IP como
      `machine` (ou vice-versa) não é correlacionada.
    - Se 2+ instâncias compartilharem o mesmo `host` cadastrado (ex.: duas instâncias locais de
      teste em `127.0.0.1`), a correlação resolve pra UMA delas arbitrariamente (a última do
      `dict` de lookup) — caso raro em produção real (instâncias distintas normalmente têm hosts
      distintos), não tratado no MVP.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from app.models.instance import Instance
from app.models.server import Server
from app.repositories.query_samples_repository import QuerySamplesRepository

TopologyNodeKind = Literal["instance", "server", "external"]


@dataclass(frozen=True, slots=True)
class TopologyNode:
    id: str
    kind: TopologyNodeKind
    label: str
    db_type: str | None = None
    health_status: str | None = None


@dataclass(frozen=True, slots=True)
class TopologyEdge:
    source: str
    target: str
    sample_count: int


@dataclass(frozen=True, slots=True)
class TopologyGraph:
    nodes: list[TopologyNode] = field(default_factory=list)
    edges: list[TopologyEdge] = field(default_factory=list)


class TopologyService:
    """Monta o grafo de dependências correlacionando `query_samples.machine` (já coletado) contra
    as instâncias/servidores cadastrados — ver docstring do módulo para o escopo/limitações."""

    def __init__(self, query_samples_repository: QuerySamplesRepository) -> None:
        self._query_samples_repository = query_samples_repository

    async def build_dependency_graph(
        self,
        since: datetime,
        instances: list[Instance],
        servers: list[Server],
        health_status_by_instance_id: dict[str, str],
    ) -> TopologyGraph:
        instance_by_host = {
            instance.host.strip().lower(): instance for instance in instances if instance.host
        }
        server_by_hostname = {
            server.hostname.strip().lower(): server for server in servers if server.hostname
        }

        nodes: dict[str, TopologyNode] = {
            str(instance.id): TopologyNode(
                id=str(instance.id),
                kind="instance",
                label=instance.name,
                db_type=instance.db_type,
                health_status=health_status_by_instance_id.get(str(instance.id), "unknown"),
            )
            for instance in instances
        }

        edge_counts: dict[tuple[str, str], int] = {}
        rows = await self._query_samples_repository.list_distinct_machines_by_instance(since)
        for row in rows:
            target_id = str(row.instance_id)
            if target_id not in nodes:
                continue  # instancia removida/desativada desde a amostra — nao inventa aresta
            machine = (row.machine or "").strip()
            if not machine:
                continue
            machine_key = machine.lower()

            source_instance = instance_by_host.get(machine_key)
            if source_instance is not None and str(source_instance.id) == target_id:
                continue  # "cliente" e a própria instância alvo — não é dependência
            if source_instance is not None:
                source_id = str(source_instance.id)
            else:
                server = server_by_hostname.get(machine_key)
                if server is not None:
                    source_id = f"server:{server.hostname}"
                    nodes.setdefault(
                        source_id,
                        TopologyNode(id=source_id, kind="server", label=server.hostname),
                    )
                else:
                    source_id = f"external:{machine}"
                    nodes.setdefault(
                        source_id, TopologyNode(id=source_id, kind="external", label=machine)
                    )

            key = (source_id, target_id)
            edge_counts[key] = edge_counts.get(key, 0) + int(row.sample_count)

        edges = [
            TopologyEdge(source=source, target=target, sample_count=count)
            for (source, target), count in edge_counts.items()
        ]
        return TopologyGraph(nodes=list(nodes.values()), edges=edges)
