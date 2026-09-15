"""Testes do parser de deadlock (`system_health` ring buffer) — Etapa 10, item 7.

XML de amostra modelado no formato documentado oficialmente pela Microsoft
(`xml_deadlock_report`/`sys.dm_xe_session_targets`) — 2 processos (1 vitima, 1 sobrevivente) e 1
recurso (keylock) com owner/waiter.
"""

from __future__ import annotations

from uuid import uuid4

from app.collectors.sqlserver.adapter import _build_deadlock_events

_SAMPLE_RING_BUFFER_XML = """
<RingBufferTarget>
  <event name="xml_deadlock_report" timestamp="2026-09-15T18:30:00.123Z">
    <data name="xml_report">
      <value>
        <deadlock>
          <victim-list>
            <victimProcess id="process1" />
          </victim-list>
          <process-list>
            <process id="process1" spid="61" currentdbname="AB_COBRANCA"
                     xactid="5001" lasttranstarted="2026-09-15T18:29:55.000"
                     loginname="app_user" hostname="app-server-1" clientapp="sqlcmd"
                     isolationlevel="read committed (2)">
              <executionStack>
                <frame procname="AB_COBRANCA.dbo.sp_atualiza_saldo" line="12" />
              </executionStack>
              <inputbuf>UPDATE saldo_conta SET valor = valor - 100 WHERE conta_id = 42</inputbuf>
            </process>
            <process id="process2" spid="62" currentdbname="AB_COBRANCA"
                     xactid="5002" lasttranstarted="2026-09-15T18:29:56.000"
                     loginname="app_user" hostname="app-server-2" clientapp="sqlcmd"
                     isolationlevel="read committed (2)">
              <executionStack>
                <frame procname="AB_COBRANCA.dbo.sp_atualiza_saldo" line="12" />
              </executionStack>
              <inputbuf>UPDATE saldo_conta SET valor = valor + 100 WHERE conta_id = 43</inputbuf>
            </process>
          </process-list>
          <resource-list>
            <keylock objectname="AB_COBRANCA.dbo.saldo_conta" indexname="PK_saldo_conta" mode="X">
              <owner-list>
                <owner id="process2" mode="X" />
              </owner-list>
              <waiter-list>
                <waiter id="process1" mode="X" />
              </waiter-list>
            </keylock>
          </resource-list>
        </deadlock>
      </value>
    </data>
  </event>
</RingBufferTarget>
"""


def test_build_deadlock_events_extracts_full_graph() -> None:
    instance_id = uuid4()
    rows = [{"target_data": _SAMPLE_RING_BUFFER_XML}]

    events = _build_deadlock_events(rows, instance_id)

    assert len(events) == 1
    event = events[0]
    assert event.instance_id == instance_id
    assert event.database_name == "AB_COBRANCA"
    assert event.victim_session_id == "61"
    assert (
        event.victim_sql_text_masked
        == "UPDATE saldo_conta SET valor = valor - ? WHERE conta_id = ?"
    )
    assert event.survivor_session_id == "62"
    assert "PK_saldo_conta" not in (event.resource_description or "")  # so objectname, nao index
    assert event.resource_description == "keylock (AB_COBRANCA.dbo.saldo_conta)"

    processes = event.detail["processes"]
    assert len(processes) == 2
    victim = next(p for p in processes if p["is_victim"])
    survivor = next(p for p in processes if not p["is_victim"])
    assert victim["session_id"] == "61"
    assert victim["transaction_id"] == "5001"
    assert victim["transaction_begin_time"] == "2026-09-15T18:29:55.000"
    assert victim["login_name"] == "app_user"
    assert victim["host_name"] == "app-server-1"
    assert victim["program_name"] == "sqlcmd"
    assert victim["isolation_level"] == "read committed (2)"
    assert victim["procedures"] == ["AB_COBRANCA.dbo.sp_atualiza_saldo"]
    assert survivor["session_id"] == "62"

    resources = event.detail["resources"]
    assert len(resources) == 1
    resource = resources[0]
    assert resource["type"] == "keylock"
    assert resource["object_name"] == "AB_COBRANCA.dbo.saldo_conta"
    assert resource["index_name"] == "PK_saldo_conta"
    assert resource["owner_session_ids"] == ["62"]
    assert resource["waiter_session_ids"] == ["61"]

    # Texto de query e XML bruto nunca em claro - mascarados antes de persistir.
    assert "100" not in event.victim_sql_text_masked
    assert event.raw_xml_masked is not None
    assert "100" not in event.raw_xml_masked


def test_build_deadlock_events_returns_empty_when_no_deadlock_report_event() -> None:
    rows = [{"target_data": "<RingBufferTarget></RingBufferTarget>"}]
    assert _build_deadlock_events(rows, uuid4()) == []


def test_build_deadlock_events_ignores_invalid_xml() -> None:
    rows = [{"target_data": "<not-valid-xml"}]
    assert _build_deadlock_events(rows, uuid4()) == []


def test_build_deadlock_events_handles_missing_target_data() -> None:
    assert _build_deadlock_events([{"target_data": None}], uuid4()) == []
