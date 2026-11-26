"""Testes de `app.normalizers.deadlock_sql_hashes` e do casamento deadlock ↔ `sql_hash` usado pela
seção "Envolvimento em eventos" do detalhe de query."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4
from xml.etree import ElementTree

from app.collectors.sqlserver.adapter import _build_deadlock_events
from app.masking.sql_masking import mask_sql_literals
from app.normalizers.deadlock_sql_hashes import (
    deadlock_process_sql_hashes,
    deadlock_sql_hashes_by_process_from_xml,
)
from app.services.query_involvement_service import match_deadlock


def _collector_hash(statement: str) -> str:
    """Mesma identidade de `collectors/sqlserver/adapter.py::_hash_query_text`."""
    return hashlib.sha256(mask_sql_literals(statement).encode("utf-8")).hexdigest()


_STATEMENT = "UPDATE saldo_conta SET valor = valor - 100 WHERE conta_id = 42"
_PREFIX = "(@p1 int)"
# stmtstart em BYTES (nvarchar): o statement começa depois do prefixo de parâmetros.
_STMTSTART = len(_PREFIX) * 2

_EVENT_XML = f"""
<event name="xml_deadlock_report" package="sqlserver" timestamp="2026-11-20T10:00:00.000Z">
  <data name="xml_report"><value>
    <deadlock>
      <victim-list><victimProcess id="process1" /></victim-list>
      <process-list>
        <process id="process1" spid="61" currentdbname="AB_COBRANCA">
          <executionStack>
            <frame procname="adhoc" line="1" stmtstart="{_STMTSTART}"
                   sqlhandle="0x02">unknown</frame>
          </executionStack>
          <inputbuf>{_PREFIX}{_STATEMENT}</inputbuf>
        </process>
        <process id="process2" spid="62" currentdbname="AB_COBRANCA">
          <executionStack>
            <frame procname="AB_COBRANCA.dbo.sp_x"
                   line="3">SELECT 1 FROM outra_tabela WHERE id = 7</frame>
          </executionStack>
          <inputbuf>EXEC dbo.sp_x</inputbuf>
        </process>
      </process-list>
      <resource-list />
    </deadlock>
  </value></data>
</event>
"""


def _process(xml: str) -> ElementTree.Element:
    return ElementTree.fromstring(xml)


def test_adhoc_frame_slice_matches_collector_statement_hash() -> None:
    process = _process(
        f'<process id="p1"><executionStack><frame procname="adhoc" stmtstart="{_STMTSTART}"'
        f" /></executionStack><inputbuf>{_PREFIX}{_STATEMENT}</inputbuf></process>"
    )
    assert _collector_hash(_STATEMENT) in deadlock_process_sql_hashes(process)


def test_frame_text_matches_procedure_statement_hash() -> None:
    process = _process(
        '<process id="p2"><executionStack><frame procname="db.dbo.sp_x">'
        "SELECT 1 FROM outra_tabela WHERE id = 7</frame></executionStack>"
        "<inputbuf>EXEC dbo.sp_x</inputbuf></process>"
    )
    hashes = deadlock_process_sql_hashes(process)
    assert _collector_hash("SELECT 1 FROM outra_tabela WHERE id = 7") in hashes
    assert _collector_hash("EXEC dbo.sp_x") in hashes


def test_unknown_frame_text_is_ignored() -> None:
    process = _process(
        '<process id="p"><executionStack><frame procname="x">unknown</frame>'
        "</executionStack></process>"
    )
    assert deadlock_process_sql_hashes(process) == []


def test_collector_stores_sql_hashes_and_match_is_exact() -> None:
    events = _build_deadlock_events(
        [{"event_data": _EVENT_XML, "timestamp_utc": datetime(2026, 11, 20, 10, tzinfo=UTC)}],
        uuid4(),
    )
    assert len(events) == 1
    normalized = events[0]
    victim = next(p for p in normalized.detail["processes"] if p["is_victim"])
    assert _collector_hash(_STATEMENT) in victim["sql_hashes"]

    stored = SimpleNamespace(
        id=uuid4(),
        time=normalized.time,
        database_name=normalized.database_name,
        resource_description=normalized.resource_description,
        detail=normalized.detail,
        raw_xml_masked=normalized.raw_xml_masked,
    )
    items = match_deadlock(stored, _collector_hash(_STATEMENT))  # type: ignore[arg-type]
    assert len(items) == 1
    assert items[0].session_id == "61"
    assert items[0].is_victim is True
    assert items[0].match == "exact"

    assert match_deadlock(stored, _collector_hash("SELECT 999")) == []  # type: ignore[arg-type]


def test_historical_deadlock_without_sql_hashes_matches_from_masked_xml() -> None:
    """Deadlock coletado antes de `sql_hashes` existir: hashes recalculados de `raw_xml_masked`
    (mascaramento idempotente) — vínculo `probable`, sessão resolvida pelo id do processo."""
    events = _build_deadlock_events(
        [{"event_data": _EVENT_XML, "timestamp_utc": datetime(2026, 11, 20, 10, tzinfo=UTC)}],
        uuid4(),
    )
    normalized = events[0]
    old_detail = {
        "processes": [
            {k: v for k, v in p.items() if k != "sql_hashes"}
            for p in normalized.detail["processes"]
        ],
        "resources": normalized.detail["resources"],
    }
    by_process = deadlock_sql_hashes_by_process_from_xml(normalized.raw_xml_masked)
    assert set(by_process) == {"process1", "process2"}

    stored = SimpleNamespace(
        id=uuid4(),
        time=normalized.time,
        database_name=normalized.database_name,
        resource_description=None,
        detail=old_detail,
        raw_xml_masked=normalized.raw_xml_masked,
    )
    # Statement de procedure: texto do frame sobrevive ao mascaramento → bate.
    items = match_deadlock(
        stored,  # type: ignore[arg-type]
        _collector_hash("SELECT 1 FROM outra_tabela WHERE id = 7"),
    )
    assert [(i.session_id, i.match, i.is_victim) for i in items] == [("62", "probable", False)]


def test_invalid_xml_returns_empty() -> None:
    assert deadlock_sql_hashes_by_process_from_xml("<not-xml") == {}
    assert deadlock_sql_hashes_by_process_from_xml(None) == {}
