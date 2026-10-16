"""Análise de plano de execução SQL Server (ShowPlanXML) — identifica operadores problemáticos
e gera recomendações acionáveis.

Trabalha sobre o XML bruto do plano (já mascarado — plan_xml_masked) armazenado em
query_plan_snapshots. Só aplicável a SQL Server (outros bancos retornam lista vazia).

Achados ordenados por severidade: critical → warning → info.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Literal

Severity = Literal["critical", "warning", "info"]

_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"

# Custo mínimo (% do total) para que um Sort/Scan seja reportado como problema.
_COSTLY_OP_THRESHOLD = 0.15
# Razão mínima entre actual e estimated rows para sinalizar erro de estimativa.
_ESTIMATE_RATIO_THRESHOLD = 10.0


@dataclass(frozen=True, slots=True)
class PlanFinding:
    finding_id: str
    severity: Severity
    description: str
    suggestion: str
    operator: str | None = None
    object_name: str | None = None


@dataclass
class _RelOpNode:
    physical_op: str | None
    logical_op: str | None
    estimated_cost: float | None      # operador isolado (sem subárvore)
    subtree_cost: float | None        # subtree (usado para % do total)
    estimate_rows: float | None
    actual_rows: float | None
    parallel: bool
    object_name: str | None
    warnings: list[str]
    children: list["_RelOpNode"] = field(default_factory=list)


def _tag(local: str) -> str:
    return f"{{{_NS}}}{local}"


def _float_attr(el: ET.Element, name: str) -> float | None:
    v = el.get(name)
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _collect_object_name(rel_op: ET.Element) -> str | None:
    """Primeiro elemento Object dentro do RelOp (não descendo em RelOp filhos)."""
    def _search(el: ET.Element, depth: int = 0) -> ET.Element | None:
        for child in el:
            if child.tag == _tag("RelOp") and depth > 0:
                continue
            if child.tag == _tag("Object"):
                return child
            found = _search(child, depth + 1)
            if found is not None:
                return found
        return None

    obj = _search(rel_op)
    if obj is None:
        return None
    parts = [
        (obj.get("Database") or "").strip("[]"),
        (obj.get("Schema") or "").strip("[]"),
        (obj.get("Table") or "").strip("[]"),
    ]
    index = (obj.get("Index") or "").strip("[]")
    name = ".".join(p for p in parts if p)
    return f"{name}.{index}" if index else (name or None)


def _collect_warnings(rel_op: ET.Element) -> list[str]:
    warnings: list[str] = []
    warnings_el = rel_op.find(_tag("Warnings"))
    if warnings_el is None:
        return warnings
    # Atributos booleanos diretos (NoJoinPredicate="true", etc.)
    for attr, val in warnings_el.attrib.items():
        if val.lower() == "true":
            warnings.append(attr)
    # Elementos filhos (SpillToTempDb, ColumnsWithNoStatistics, etc.)
    for child in warnings_el:
        warnings.append(child.tag.split("}")[-1])
    return warnings


def _collect_actual_rows(rel_op: ET.Element) -> float | None:
    rt = rel_op.find(_tag("RunTimeInformation"))
    if rt is None:
        return None
    counters = list(rt.findall(_tag("RunTimeCountersPerThread")))
    if not counters:
        return None
    non_zero = [c for c in counters if c.get("Thread") != "0"]
    relevant = non_zero if non_zero else counters
    total = 0.0
    any_found = False
    for c in relevant:
        v = c.get("ActualRows")
        if v is not None:
            try:
                total += float(v)
                any_found = True
            except ValueError:
                pass
    return total if any_found else None


def _parse_rel_op(el: ET.Element) -> _RelOpNode:
    """Parseia um RelOp e seus filhos diretos de forma recursiva."""
    child_rel_ops: list[ET.Element] = []

    def _collect_children(parent: ET.Element) -> None:
        for child in parent:
            if child.tag == _tag("RelOp"):
                child_rel_ops.append(child)
            else:
                _collect_children(child)

    _collect_children(el)

    node = _RelOpNode(
        physical_op=el.get("PhysicalOp"),
        logical_op=el.get("LogicalOp"),
        estimated_cost=_float_attr(el, "EstimatedOperatorCost"),
        subtree_cost=_float_attr(el, "EstimatedTotalSubtreeCost"),
        estimate_rows=_float_attr(el, "EstimateRows"),
        actual_rows=_collect_actual_rows(el),
        parallel=el.get("Parallel") == "1" or el.get("Parallel") == "true",
        object_name=_collect_object_name(el),
        warnings=_collect_warnings(el),
    )
    node.children = [_parse_rel_op(c) for c in child_rel_ops]
    return node


def _all_nodes(nodes: list[_RelOpNode]) -> list[_RelOpNode]:
    result: list[_RelOpNode] = []
    for n in nodes:
        result.append(n)
        result.extend(_all_nodes(n.children))
    return result


def analyze_plan(plan_xml: str) -> list[PlanFinding]:
    """Analisa o ShowPlanXML e retorna achados ordenados por severidade."""
    if not plan_xml or not plan_xml.strip():
        return []

    try:
        root = ET.fromstring(plan_xml)
    except ET.ParseError:
        return []

    # Coleta todos os RelOp raiz (pode haver múltiplos statements no plano)
    root_rel_ops: list[ET.Element] = []
    for el in root.iter(_tag("RelOp")):
        # É raiz se o pai imediato não for outro RelOp
        parent_map = {c: p for p in root.iter() for c in p}
        parent = parent_map.get(el)
        if parent is None or parent.tag != _tag("RelOp"):
            root_rel_ops.append(el)
        break  # Usamos iter() apenas para detectar o padrão; rebuilda abaixo

    # Rebuild mais correto: coleta RelOps que não são filhos de outro RelOp
    all_rel_ops_in_doc: list[ET.Element] = list(root.iter(_tag("RelOp")))
    rel_op_set = set(id(e) for e in all_rel_ops_in_doc)

    parent_ids: set[int] = set()
    for el in all_rel_ops_in_doc:
        def _mark_children(node: ET.Element) -> None:
            for child in node:
                if child.tag == _tag("RelOp"):
                    parent_ids.add(id(child))
                else:
                    _mark_children(child)
        _mark_children(el)

    root_rel_ops = [e for e in all_rel_ops_in_doc if id(e) not in parent_ids]
    if not root_rel_ops:
        return []

    parsed_roots = [_parse_rel_op(e) for e in root_rel_ops]
    all_nodes = _all_nodes(parsed_roots)

    # Custo máximo de subárvore (para calcular %)
    max_cost = max((n.subtree_cost or 0.0) for n in all_nodes) or 1.0

    findings: list[PlanFinding] = []
    seen_rule_ids: set[str] = set()

    def _add(f: PlanFinding) -> None:
        if f.finding_id not in seen_rule_ids:
            seen_rule_ids.add(f.finding_id)
            findings.append(f)

    has_actual = any(n.actual_rows is not None for n in all_nodes)

    for node in all_nodes:
        op = node.physical_op or node.logical_op or ""
        cost_pct = (node.subtree_cost or 0.0) / max_cost

        # P001 — Key Lookup (índice não covering)
        if op == "Key Lookup":
            _add(PlanFinding(
                finding_id="P001",
                severity="warning",
                description="Key Lookup detectado — o índice não cobre todas as colunas necessárias.",
                suggestion=(
                    "Crie um índice covering com as colunas projetadas no INCLUDE. "
                    "Key Lookup elimina o acesso direto à linha base (RID/clustered), "
                    "mas ainda faz uma leitura extra por linha."
                ),
                operator="Key Lookup",
                object_name=node.object_name,
            ))

        # P002 — Sort com custo alto
        if op == "Sort" and cost_pct >= _COSTLY_OP_THRESHOLD:
            pct_str = f"{cost_pct * 100:.0f}%"
            _add(PlanFinding(
                finding_id="P002",
                severity="warning",
                description=f"Sort com {pct_str} do custo total — dados não chegam na ordem desejada.",
                suggestion=(
                    "Crie um índice com as colunas do ORDER BY / GROUP BY na mesma ordem. "
                    "Sort explícito no plano significa que o otimizador não encontrou "
                    "um índice ordenado adequado."
                ),
                operator="Sort",
            ))

        # P003 — Table Scan ou Clustered Index Scan com custo alto
        if op in ("Table Scan", "Clustered Index Scan") and cost_pct >= _COSTLY_OP_THRESHOLD:
            pct_str = f"{cost_pct * 100:.0f}%"
            _add(PlanFinding(
                finding_id="P003",
                severity="warning",
                description=f"{op} com {pct_str} do custo total — varredura completa da tabela.",
                suggestion=(
                    "Avalie criar um índice nas colunas do filtro WHERE. "
                    f"Um {op} lê todas as linhas da tabela independente do filtro aplicado."
                ),
                operator=op,
                object_name=node.object_name,
            ))

        # P004 — Erro de estimativa de linhas (só planos actual)
        if has_actual and node.actual_rows is not None and node.estimate_rows:
            ratio = node.actual_rows / node.estimate_rows if node.estimate_rows > 0 else None
            if ratio is not None and (ratio > _ESTIMATE_RATIO_THRESHOLD or ratio < 1 / _ESTIMATE_RATIO_THRESHOLD):
                direction = "sub" if ratio > 1 else "super"
                _add(PlanFinding(
                    finding_id="P004",
                    severity="warning",
                    description=(
                        f"Erro de estimativa de cardinalidade: estimado {node.estimate_rows:.0f} linhas, "
                        f"real {node.actual_rows:.0f} ({direction}estimado {abs(ratio):.0f}x)."
                    ),
                    suggestion=(
                        "Estatísticas desatualizadas ou parameter sniffing. "
                        "Tente UPDATE STATISTICS com FULLSCAN ou adicione OPTION (RECOMPILE) "
                        "para forçar recompilação com os parâmetros reais."
                    ),
                    operator=op or None,
                    object_name=node.object_name,
                ))

        # P005 — SpillToTempDb (grant de memória insuficiente)
        if "SpillToTempDb" in node.warnings:
            _add(PlanFinding(
                finding_id="P005",
                severity="critical",
                description="SpillToTempDb — Sort ou Hash Match derramou dados no disco por falta de memória.",
                suggestion=(
                    "O grant de memória foi insuficiente (estimativa de linhas errada). "
                    "Corrija as estatísticas para melhorar o grant, ou ajuste max server memory / "
                    "Resource Governor se o servidor tiver memória disponível."
                ),
                operator=op or None,
            ))

        # P006 — NoJoinPredicate (produto cartesiano)
        if "NoJoinPredicate" in node.warnings:
            _add(PlanFinding(
                finding_id="P006",
                severity="critical",
                description="NoJoinPredicate — JOIN sem condição (produto cartesiano).",
                suggestion=(
                    "Verifique as condições de JOIN. Um JOIN sem predicado multiplica "
                    "todas as linhas de uma tabela pelas da outra."
                ),
                operator=op or None,
            ))

        # P007 — ColumnsWithNoStatistics
        if "ColumnsWithNoStatistics" in node.warnings:
            _add(PlanFinding(
                finding_id="P007",
                severity="info",
                description="Colunas sem estatísticas — otimizador usou estimativas padrão (possivelmente incorretas).",
                suggestion=(
                    "Crie estatísticas nas colunas filtradas: "
                    "CREATE STATISTICS stat_nome ON tabela(coluna) WITH FULLSCAN"
                ),
                operator=op or None,
                object_name=node.object_name,
            ))

        # P008 — Nested Loops com outer input grande (estimativa alta)
        if op == "Nested Loops" and node.children:
            outer = node.children[0]
            outer_rows = outer.estimate_rows or 0
            if outer_rows > 10_000:
                _add(PlanFinding(
                    finding_id="P008",
                    severity="info",
                    description=(
                        f"Nested Loops com ~{outer_rows:,.0f} linhas estimadas no outer input — "
                        "pode ser lento se o inner input não tiver seek eficiente."
                    ),
                    suggestion=(
                        "Verifique se o inner input do loop usa Index Seek. "
                        "Para grandes volumes, Hash Match ou Merge Join podem ser mais eficientes "
                        "— use OPTION (HASH JOIN) ou OPTION (MERGE JOIN) para comparar."
                    ),
                    operator="Nested Loops",
                ))

    # Ordena: critical → warning → info
    _order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: _order[f.severity])
    return findings


# ── PostgreSQL EXPLAIN JSON ───────────────────────────────────────────────────

def _pg_walk(node: dict, collected: list[dict]) -> None:
    collected.append(node)
    for child in node.get("Plans", []):
        _pg_walk(child, collected)


def analyze_plan_postgresql_json(plan_json_text: str) -> list[PlanFinding]:
    """Analisa saída de EXPLAIN (FORMAT JSON) do PostgreSQL e retorna achados."""
    import json

    if not plan_json_text or not plan_json_text.strip():
        return []
    try:
        data = json.loads(plan_json_text)
        root_plan = data[0]["Plan"] if isinstance(data, list) else data["Plan"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return []

    all_nodes: list[dict] = []
    _pg_walk(root_plan, all_nodes)

    max_cost = max((n.get("Total Cost", 0.0) for n in all_nodes), default=1.0) or 1.0
    findings: list[PlanFinding] = []
    seen: set[str] = set()

    def _add(f: PlanFinding) -> None:
        if f.finding_id not in seen:
            seen.add(f.finding_id)
            findings.append(f)

    for node in all_nodes:
        ntype = node.get("Node Type", "")
        total_cost = node.get("Total Cost", 0.0)
        cost_pct = total_cost / max_cost
        relation = node.get("Relation Name") or node.get("Alias") or None
        plan_rows = node.get("Plan Rows", 0)
        actual_rows = node.get("Actual Rows")

        # PG01 — Sequential Scan com custo significativo
        if ntype == "Seq Scan" and cost_pct >= _COSTLY_OP_THRESHOLD:
            _add(PlanFinding(
                finding_id="PG01",
                severity="warning",
                description=(
                    f"Seq Scan em '{relation}' representa {cost_pct:.0%} do custo total "
                    f"({plan_rows:,.0f} linhas estimadas) — varredura completa da tabela."
                ),
                suggestion=(
                    "Crie um índice nas colunas usadas no filtro (WHERE) ou JOIN. "
                    "Analise com EXPLAIN ANALYZE para confirmar o custo real."
                ),
                operator="Seq Scan",
                object_name=relation,
            ))

        # PG02 — Sort sem índice com custo alto
        if ntype == "Sort" and cost_pct >= _COSTLY_OP_THRESHOLD:
            sort_key = ", ".join(node.get("Sort Key", []))
            _add(PlanFinding(
                finding_id="PG02",
                severity="warning",
                description=(
                    f"Sort em memória/disco representa {cost_pct:.0%} do custo total. "
                    f"Chave: {sort_key or 'desconhecida'}."
                ),
                suggestion=(
                    "Crie um índice que cubra as colunas de ordenação. "
                    "Verifique work_mem se o sort estiver derramando para disco."
                ),
                operator="Sort",
            ))

        # PG03 — Hash Join ou Hash com linhas altas (pressão de memória)
        if ntype in ("Hash", "Hash Join") and plan_rows > 10_000:
            _add(PlanFinding(
                finding_id="PG03",
                severity="info",
                description=(
                    f"{ntype} com {plan_rows:,.0f} linhas estimadas pode pressionar work_mem."
                ),
                suggestion=(
                    "Se o plano mostrar 'Batches > 1', aumente work_mem ou adicione índice "
                    "para permitir Merge Join (que não precisa de hash table em memória)."
                ),
                operator=ntype,
            ))

        # PG04 — Erro de estimativa de cardinalidade (plano real com ANALYZE)
        if actual_rows is not None and plan_rows > 0:
            ratio = actual_rows / plan_rows if plan_rows else None
            if ratio is not None and (ratio > _ESTIMATE_RATIO_THRESHOLD or ratio < 1 / _ESTIMATE_RATIO_THRESHOLD):
                direction = "sub" if ratio > 1 else "super"
                _add(PlanFinding(
                    finding_id="PG04",
                    severity="warning",
                    description=(
                        f"Estimativa errada de cardinalidade: esperado {plan_rows:,.0f} linhas, "
                        f"real {actual_rows:,.0f} ({direction}estimado {abs(ratio):.0f}x)."
                    ),
                    suggestion=(
                        "Execute ANALYZE na tabela para atualizar estatísticas. "
                        "Considere aumentar default_statistics_target para colunas skewed."
                    ),
                    operator=ntype or None,
                    object_name=relation,
                ))

        # PG05 — Nested Loop com muitas linhas no outer
        if ntype == "Nested Loop" and node.get("Plans"):
            outer_rows = node["Plans"][0].get("Plan Rows", 0)
            if outer_rows > 10_000:
                _add(PlanFinding(
                    finding_id="PG05",
                    severity="info",
                    description=(
                        f"Nested Loop com ~{outer_rows:,.0f} linhas no outer input — "
                        "ineficiente se o inner input não usar Index Scan."
                    ),
                    suggestion=(
                        "Certifique-se de que o inner input do loop usa Index Scan. "
                        "Para grandes volumes, um Hash Join pode ser mais eficiente — "
                        "teste com SET enable_nestloop = off."
                    ),
                    operator="Nested Loop",
                ))

    _order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: _order[f.severity])
    return findings


# ── MySQL EXPLAIN FORMAT=JSON ─────────────────────────────────────────────────

def _mysql_walk(node: dict, collected: list[dict]) -> None:
    """Percorre recursivamente os nós do plano JSON do MySQL."""
    collected.append(node)
    for key in ("table", "duplicates_removal", "ordering_operation", "grouping_operation"):
        child = node.get(key)
        if isinstance(child, dict):
            _mysql_walk(child, collected)
    for child in node.get("nested_loop", []):
        if isinstance(child, dict):
            _mysql_walk(child.get("table", child), collected)


def analyze_plan_mysql_json(plan_json_text: str) -> list[PlanFinding]:
    """Analisa saída de EXPLAIN FORMAT=JSON do MySQL/MariaDB."""
    import json

    if not plan_json_text or not plan_json_text.strip():
        return []
    try:
        data = json.loads(plan_json_text)
        query_block = data.get("query_block", data)
    except (json.JSONDecodeError, TypeError):
        return []

    all_nodes: list[dict] = []
    _mysql_walk(query_block, all_nodes)

    findings: list[PlanFinding] = []
    seen: set[str] = set()

    def _add(f: PlanFinding) -> None:
        if f.finding_id not in seen:
            seen.add(f.finding_id)
            findings.append(f)

    for node in all_nodes:
        table_name = node.get("table_name") or node.get("alias") or None
        access_type = node.get("access_type", "")
        rows = node.get("rows_examined_per_scan") or node.get("rows_produced_per_join") or 0
        filtered = float(node.get("filtered", 100) or 100)
        key_used = node.get("key") or node.get("used_key_parts")
        extra = (node.get("message") or "").lower()

        # MY01 — Full Table Scan (access_type = ALL)
        if access_type == "ALL":
            _add(PlanFinding(
                finding_id="MY01",
                severity="warning",
                description=(
                    f"Full Table Scan em '{table_name}' ({rows:,} linhas examinadas, "
                    f"{filtered:.0f}% filtradas) — nenhum índice usado."
                ),
                suggestion=(
                    "Crie um índice nas colunas do WHERE/JOIN. "
                    "Use EXPLAIN para confirmar qual índice o otimizador escolheria."
                ),
                operator="ALL",
                object_name=table_name,
            ))

        # MY02 — Filesort (ordenação sem índice)
        if "filesort" in extra or node.get("using_filesort"):
            _add(PlanFinding(
                finding_id="MY02",
                severity="warning",
                description=(
                    f"Filesort em '{table_name}' — ordenação feita em memória/disco sem índice."
                ),
                suggestion=(
                    "Crie um índice composto que cubra as colunas do ORDER BY, "
                    "de preferência o mesmo usado no WHERE para evitar scan extra."
                ),
                operator="filesort",
                object_name=table_name,
            ))

        # MY03 — Tabela temporária
        if "temporary" in extra or node.get("using_temporary_table"):
            _add(PlanFinding(
                finding_id="MY03",
                severity="warning",
                description=(
                    "Tabela temporária criada internamente — indica GROUP BY/DISTINCT/UNION "
                    "sem índice adequado ou colunas TEXT/BLOB forçando disco."
                ),
                suggestion=(
                    "Revise os índices para as colunas de agrupamento/ordenação. "
                    "Aumente tmp_table_size e max_heap_table_size se a tabela estiver indo para disco."
                ),
                operator="temporary table",
            ))

        # MY04 — Index range scan com muitas linhas
        if access_type == "range" and rows > 50_000 and not key_used:
            _add(PlanFinding(
                finding_id="MY04",
                severity="info",
                description=(
                    f"Range scan em '{table_name}' lê ~{rows:,} linhas — "
                    "índice presente mas seletividade baixa."
                ),
                suggestion=(
                    "Considere um índice mais seletivo ou reescreva a consulta "
                    "para reduzir o intervalo escaneado."
                ),
                operator="range",
                object_name=table_name,
            ))

    _order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: _order[f.severity])
    return findings


# ── Oracle V$SQL_PLAN (texto formatado) ───────────────────────────────────────

def analyze_plan_oracle_text(plan_text: str) -> list[PlanFinding]:
    """Analisa plano Oracle formatado a partir de V$SQL_PLAN."""
    if not plan_text or not plan_text.strip():
        return []

    findings: list[PlanFinding] = []
    seen: set[str] = set()

    def _add(f: PlanFinding) -> None:
        if f.finding_id not in seen:
            seen.add(f.finding_id)
            findings.append(f)

    lines = plan_text.upper().splitlines()
    for line in lines:
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) < 3:
            continue
        operation = parts[1] if len(parts) > 1 else ""
        options = parts[2] if len(parts) > 2 else ""
        obj = parts[3] if len(parts) > 3 else None
        try:
            cost = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
            rows_est = int(parts[6]) if len(parts) > 6 and parts[6].isdigit() else 0
        except (ValueError, IndexError):
            cost, rows_est = 0, 0

        # OR01 — Full Table Scan
        if "TABLE ACCESS" in operation and "FULL" in options:
            _add(PlanFinding(
                finding_id="OR01",
                severity="warning",
                description=(
                    f"Full Table Scan em '{obj or 'tabela'}' "
                    f"(custo estimado: {cost:,}, linhas: {rows_est:,})."
                ),
                suggestion=(
                    "Crie um índice nas colunas do WHERE/JOIN. "
                    "Use INDEX hints ou verifique estatísticas com DBMS_STATS."
                ),
                operator="TABLE ACCESS FULL",
                object_name=obj,
            ))

        # OR02 — Sort
        if "SORT" in operation and cost > 0:
            _add(PlanFinding(
                finding_id="OR02",
                severity="warning",
                description=(
                    f"Operação de Sort com custo {cost:,} — "
                    "pode usar PGA excessiva ou derramar para disco (temp)."
                ),
                suggestion=(
                    "Crie um índice que cubra as colunas do ORDER BY/GROUP BY. "
                    "Monitore V$SORT_USAGE e ajuste PGA_AGGREGATE_TARGET se houver spill."
                ),
                operator="SORT",
            ))

        # OR03 — Nested Loops com muitas linhas
        if "NESTED LOOPS" in operation and rows_est > 10_000:
            _add(PlanFinding(
                finding_id="OR03",
                severity="info",
                description=(
                    f"Nested Loops com {rows_est:,} linhas estimadas — "
                    "ineficiente se o inner não usar Index Scan."
                ),
                suggestion=(
                    "Verifique se o inner do loop usa um índice. "
                    "Para grandes volumes, considere HASH JOIN via hint /*+ USE_HASH */."
                ),
                operator="NESTED LOOPS",
            ))

        # OR04 — Partition Full Scan (pode ser intencional mas merece atenção)
        if "PARTITION" in operation and "FULL" in options:
            _add(PlanFinding(
                finding_id="OR04",
                severity="info",
                description=(
                    f"Partition Full Scan em '{obj or 'objeto'}' — "
                    "todas as partições estão sendo lidas."
                ),
                suggestion=(
                    "Verifique se o filtro inclui a chave de particionamento "
                    "para habilitar Partition Pruning e reduzir o scan."
                ),
                operator="PARTITION FULL",
                object_name=obj,
            ))

    _order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: _order[f.severity])
    return findings


# ── Dispatcher ───────────────────────────────────────────────────────────────

def analyze_plan_by_format(plan_text: str, plan_format: str) -> list[PlanFinding]:
    """Despacha para o analisador correto conforme o formato do plano."""
    if plan_format == "postgresql_json":
        return analyze_plan_postgresql_json(plan_text)
    if plan_format == "mysql_json":
        return analyze_plan_mysql_json(plan_text)
    if plan_format == "oracle_text":
        return analyze_plan_oracle_text(plan_text)
    # Default: SQL Server XML
    return analyze_plan(plan_text)
