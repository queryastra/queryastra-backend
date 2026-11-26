"""`sql_hash` candidatos de um `<process>` do grafo de deadlock (SQL Server).

O deadlock graph não traz o `sql_hash` que o QueryAstra usa como identidade de query
(`sha256(mask_sql_literals(statement))`, ver `collectors/sqlserver/adapter.py::_hash_query_text`)
— só o `<inputbuf>` (texto do BATCH inteiro, não do statement) e a pilha `<executionStack>/<frame>`
(cada frame com `stmtstart`/`stmtend` em bytes e, a partir do SQL Server 2016, o texto do statement
como conteúdo do elemento). Para ligar um deadlock à tela de detalhe de uma query (seção
"Envolvimento em eventos"), calcula-se um conjunto de hashes CANDIDATOS por processo:

1. texto de cada `<frame>` (exceto o placeholder `unknown`), como veio e sem espaços nas pontas;
2. fatia do `<inputbuf>` por `stmtstart`/`stmtend` de cada frame `adhoc` (mesma conta do
   `SUBSTRING(... statement_start_offset / 2 ...)` da coleta normal);
3. o `<inputbuf>` inteiro (batch de um statement só — caso comum de query ad-hoc da aplicação).

Melhor esforço: um hash que bate é a mesma query (mesma função de hash da coleta); nenhum hash bater
NÃO prova que a query não participou do deadlock (texto truncado no XML, espaços diferentes etc.).

Usado em dois momentos:
- na COLETA (`_parse_deadlock_process`), com o texto original → grava `sql_hashes` no `detail`;
- na LEITURA de deadlocks antigos (coletados antes deste campo existir), a partir de
  `raw_xml_masked` — o texto ali já passou por `mask_sql_literals`, que é idempotente
  (`?`/`'?'` não são literais de novo), então `mask_sql_literals(texto_mascarado)` devolve o mesmo
  valor e o hash bate. Por isso o mesmo helper serve aos dois casos.
"""

from __future__ import annotations

import hashlib
from typing import Any
from xml.etree import ElementTree

from app.masking.sql_masking import mask_sql_literals

# Placeholder que o SQL Server grava como texto de frame quando não tem o statement disponível.
_UNKNOWN_FRAME_TEXT = "unknown"

# Teto de candidatos por processo — pilha de execução profunda (procedure chamando procedure) não
# pode inflar o JSONB indefinidamente.
_MAX_HASHES_PER_PROCESS = 20


def _hash(text: str) -> str:
    return hashlib.sha256(mask_sql_literals(text).encode("utf-8")).hexdigest()


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _slice_statement(batch_text: str, stmtstart: int | None, stmtend: int | None) -> str | None:
    """Mesma fatia do `SUBSTRING(text, start/2 + 1, (end - start)/2 + 1)` da coleta normal —
    offsets em bytes (texto nvarchar, 2 bytes por caractere). `stmtend` ausente/-1 = até o fim."""
    if stmtstart is None or stmtstart < 0:
        return None
    start = stmtstart // 2
    if start >= len(batch_text):
        return None
    if stmtend is None or stmtend < 0:
        return batch_text[start:]
    end = stmtend // 2 + 1
    if end <= start:
        return None
    return batch_text[start:end]


def deadlock_process_sql_hashes(process: ElementTree.Element) -> list[str]:
    """Hashes candidatos (sem duplicatas, ordem estável) de um `<process>` do deadlock graph."""
    texts: list[str] = []
    inputbuf_el = process.find("./inputbuf")
    inputbuf = inputbuf_el.text if inputbuf_el is not None and inputbuf_el.text else None

    for frame in process.findall("./executionStack/frame"):
        frame_text = frame.text
        if frame_text and frame_text.strip() and frame_text.strip() != _UNKNOWN_FRAME_TEXT:
            texts.append(frame_text)
            texts.append(frame_text.strip())
        if inputbuf and (frame.get("procname") or "").lower() in ("adhoc", ""):
            sliced = _slice_statement(
                inputbuf, _to_int(frame.get("stmtstart")), _to_int(frame.get("stmtend"))
            )
            if sliced:
                texts.append(sliced)
                texts.append(sliced.strip())

    if inputbuf:
        texts.append(inputbuf)
        texts.append(inputbuf.strip())

    hashes: list[str] = []
    for text in texts:
        if not text:
            continue
        h = _hash(text)
        if h not in hashes:
            hashes.append(h)
        if len(hashes) >= _MAX_HASHES_PER_PROCESS:
            break
    return hashes


def deadlock_sql_hashes_by_process_from_xml(raw_xml: str | None) -> dict[str, list[str]]:
    """`id` do processo (`"process2a1b3c8"`) → hashes candidatos, a partir do `<deadlock>`
    serializado (`raw_xml_masked`). Usado só para deadlocks antigos sem `sql_hashes` no `detail`.

    Chaveado pelo `id` do processo, não pelo `spid`: `raw_xml_masked` passou inteiro por
    `mask_sql_literals`, que troca atributos numéricos (`spid`, `stmtstart`, `stmtend`) por `?` —
    o `id` (alfanumérico) sobrevive e casa com `detail.processes[].id`. Pelo mesmo motivo a fatia
    por `stmtstart`/`stmtend` não é possível aqui (offsets mascarados); sobram o texto do frame e o
    `inputbuf`. XML inválido → `{}` (nunca levanta — leitura de histórico, melhor esforço)."""
    if not raw_xml:
        return {}
    try:
        root = ElementTree.fromstring(raw_xml)
    except ElementTree.ParseError:
        return {}
    deadlock_el = root if root.tag == "deadlock" else root.find(".//deadlock")
    if deadlock_el is None:
        return {}
    result: dict[str, list[str]] = {}
    for process in deadlock_el.findall("./process-list/process"):
        process_id = process.get("id")
        if process_id:
            result[process_id] = deadlock_process_sql_hashes(process)
    return result
