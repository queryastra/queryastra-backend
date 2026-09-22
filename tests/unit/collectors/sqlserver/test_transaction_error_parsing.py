"""Testes do parser de erro de transacao (sessao Extended Events dedicada `sqlserver.
error_reported`) — Etapa 10, item 3.

XML de amostra no formato documentado do Extended Events (`<data>`/`<action>`, cada um com um
`<value>` filho) - mesma estrutura de `test_deadlock_parsing.py`, evento diferente.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from app.collectors.sqlserver.adapter import _build_confirmed_transaction_errors

_SAMPLE_ERROR_EVENT_XML = """
<event name="error_reported" package="sqlserver" timestamp="2026-09-22T12:00:00.000Z">
  <data name="error_number"><value>2627</value></data>
  <data name="severity"><value>14</value></data>
  <data name="state"><value>1</value></data>
  <data name="message">
    <value>Violation of UNIQUE KEY constraint 'UQ_Contas_Numero'. Cannot insert duplicate key
    in object 'dbo.Contas'. The duplicate key value is (12345).</value>
  </data>
  <action name="session_id"><value>62</value></action>
  <action name="database_name"><value>AB_COBRANCA</value></action>
</event>
"""

_SAMPLE_TIMESTAMP = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)


def test_build_confirmed_transaction_errors_extracts_fields() -> None:
    instance_id = uuid4()
    rows = [{"event_data": _SAMPLE_ERROR_EVENT_XML, "timestamp_utc": _SAMPLE_TIMESTAMP}]

    errors = _build_confirmed_transaction_errors(rows, instance_id)

    assert len(errors) == 1
    error = errors[0]
    assert error.instance_id == instance_id
    assert error.session_id == "62"
    assert error.error_number == 2627
    assert error.severity == 14
    assert error.time == _SAMPLE_TIMESTAMP
    # Mensagem mascarada por completo (blanket) - nenhum valor/identificador original sobra.
    assert "UQ_Contas_Numero" not in error.message_masked
    assert "dbo.Contas" not in error.message_masked
    assert "12345" not in error.message_masked
    assert "Violation of UNIQUE KEY constraint" in error.message_masked  # estrutura preservada


def test_build_confirmed_transaction_errors_falls_back_to_event_timestamp_attribute() -> None:
    """Sem `timestamp_utc` na linha (ou nao-datetime) - cai pro atributo `timestamp` do proprio
    `<event>` (mesmo fallback ja usado pelo parser de deadlock)."""
    instance_id = uuid4()
    rows = [{"event_data": _SAMPLE_ERROR_EVENT_XML, "timestamp_utc": None}]

    errors = _build_confirmed_transaction_errors(rows, instance_id)

    assert len(errors) == 1
    assert errors[0].time == _SAMPLE_TIMESTAMP


def test_build_confirmed_transaction_errors_ignores_invalid_xml() -> None:
    rows = [{"event_data": "<not-valid-xml", "timestamp_utc": _SAMPLE_TIMESTAMP}]
    assert _build_confirmed_transaction_errors(rows, uuid4()) == []


def test_build_confirmed_transaction_errors_handles_missing_event_data() -> None:
    assert _build_confirmed_transaction_errors([{"event_data": None}], uuid4()) == []


def test_build_confirmed_transaction_errors_skips_row_missing_required_field() -> None:
    """Evento sem `session_id` (action ausente) - nao ha como correlacionar com uma transacao,
    linha ignorada em vez de gerar um erro "orfao" sem identidade."""
    xml_without_session_id = """
    <event name="error_reported" package="sqlserver" timestamp="2026-09-22T12:00:00.000Z">
      <data name="error_number"><value>2627</value></data>
      <data name="severity"><value>14</value></data>
      <data name="state"><value>1</value></data>
      <data name="message"><value>algum erro</value></data>
      <action name="database_name"><value>AB_COBRANCA</value></action>
    </event>
    """
    rows = [{"event_data": xml_without_session_id, "timestamp_utc": _SAMPLE_TIMESTAMP}]
    assert _build_confirmed_transaction_errors(rows, uuid4()) == []
