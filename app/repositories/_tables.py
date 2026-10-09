"""Definicoes SQLAlchemy Core (nao-ORM) das hypertables/tabelas de coleta continua.

`metric_samples`, `query_samples` e `blocking_events` sao hypertables TimescaleDB — nao tem
model ORM dedicado (`app/models`) porque `create_hypertable`/continuous aggregates fogem do
autogenerate padrao do Alembic (ver `migrations/versions/66ae4664300c_hypertables.py`). Estas
`Table` do SQLAlchemy Core espelham exatamente as colunas criadas naquela migration e sao usadas
pelos repositorios (`app/repositories/*_repository.py`) para INSERT via `sa.insert(...)`.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = sa.MetaData()

metric_samples_table = sa.Table(
    "metric_samples",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("target_type", sa.Text(), nullable=False),
    sa.Column("target_id", UUID(as_uuid=True), nullable=False),
    sa.Column("metric_name", sa.Text(), nullable=False),
    sa.Column("labels", JSONB(astext_type=sa.Text()), nullable=False, server_default="{}"),
    sa.Column("value", sa.Float(), nullable=False),
)

query_samples_table = sa.Table(
    "query_samples",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("instance_id", UUID(as_uuid=True), nullable=False),
    sa.Column("sql_hash", sa.Text(), nullable=False),
    sa.Column("sql_name", sa.Text(), nullable=True),
    sa.Column("sql_text_masked", sa.Text(), nullable=False),
    sa.Column("sql_text_full_masked", sa.Text(), nullable=True),
    sa.Column("sql_text_encrypted", sa.LargeBinary(), nullable=True),
    sa.Column("wait_type", sa.Text(), nullable=True),
    sa.Column("wait_event_native", sa.Text(), nullable=True),
    sa.Column("wait_time_ms", sa.BigInteger(), nullable=True),
    sa.Column("cpu_time_ms", sa.BigInteger(), nullable=True),
    sa.Column("duration_ms", sa.BigInteger(), nullable=True),
    sa.Column("session_id", sa.Text(), nullable=True),
    sa.Column("db_user", sa.Text(), nullable=True),
    sa.Column("program", sa.Text(), nullable=True),
    sa.Column("database_name", sa.Text(), nullable=True),
    sa.Column("machine", sa.Text(), nullable=True),
)

blocking_events_table = sa.Table(
    "blocking_events",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("instance_id", UUID(as_uuid=True), nullable=False),
    sa.Column("blocker_session_id", sa.Text(), nullable=False),
    sa.Column("blocked_session_id", sa.Text(), nullable=False),
    sa.Column("blocker_sql_hash", sa.Text(), nullable=True),
    sa.Column("blocked_sql_hash", sa.Text(), nullable=True),
    sa.Column("wait_time_ms", sa.BigInteger(), nullable=False),
    # Enriquecimento (Etapa 9 — arvore completa de bloqueio): capturado no momento da coleta,
    # nao reconstruido depois via join contra query_samples — unico jeito de identificar um
    # bloqueador OCIOSO (sem request ativa, portanto sem linha em query_samples). Todas aditivas
    # e nullable; blocked_wait_resource/blocked_object sao melhor esforco (ver _tables docstring
    # e app/normalizers/models.py::NormalizedBlockingEvent).
    sa.Column("blocker_db_user", sa.Text(), nullable=True),
    sa.Column("blocker_program", sa.Text(), nullable=True),
    sa.Column("blocker_machine", sa.Text(), nullable=True),
    sa.Column("blocker_wait_event_native", sa.Text(), nullable=True),
    sa.Column("blocked_db_user", sa.Text(), nullable=True),
    sa.Column("blocked_program", sa.Text(), nullable=True),
    sa.Column("blocked_machine", sa.Text(), nullable=True),
    sa.Column("blocked_wait_event_native", sa.Text(), nullable=True),
    sa.Column("blocked_wait_resource", sa.Text(), nullable=True),
    sa.Column("blocked_object", sa.Text(), nullable=True),
    # Etapa 10 (rastreamento de transacoes) — liga cada lado da aresta de bloqueio a sua
    # transacao (coluna "Transacao" na tela de Bloqueios). SQL Server:
    # `sys.dm_tran_session_transactions.transaction_id`; PostgreSQL: o proprio `pid` do backend
    # (sem conceito de transacao nomeada separado da sessao).
    sa.Column("blocker_transaction_id", sa.Text(), nullable=True),
    sa.Column("blocked_transaction_id", sa.Text(), nullable=True),
    # Banco de dados de cada lado (a pedido explicito do usuario) — ver docstring de
    # NormalizedBlockingEvent para a logica de fallback do lado bloqueador ocioso.
    sa.Column("blocker_database_name", sa.Text(), nullable=True),
    sa.Column("blocked_database_name", sa.Text(), nullable=True),
    # Inicio da transacao de cada lado — denormalizado dos mesmos dados de transacao ja lidos no
    # ciclo (SQL Server: _TRANSACTION_QUERY; PostgreSQL: xact_start), sem query extra. Persistido
    # aqui tambem (alem de em `blocking_incidents`) so para manter o log bruto por ciclo completo;
    # o ciclo de vida do INCIDENTE em si (started_at/ended_at) vive em `blocking_incidents` — esta
    # tabela e uma hypertable (particionada por `time`) e NAO suporta upsert-in-place por chave de
    # negocio sem incluir `time` na constraint unica, o que descaracterizaria "started_at" (ver
    # `app/models/blocking_incident.py`).
    sa.Column("blocker_transaction_begin_time", sa.DateTime(timezone=True), nullable=True),
    sa.Column("blocked_transaction_begin_time", sa.DateTime(timezone=True), nullable=True),
)

query_stats_snapshot_table = sa.Table(
    "query_stats_snapshot",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("instance_id", UUID(as_uuid=True), nullable=False),
    sa.Column("queryid", sa.BigInteger(), nullable=False),
    sa.Column("query_text", sa.Text(), nullable=True),
    sa.Column("calls", sa.BigInteger(), nullable=False),
    sa.Column("total_exec_time", sa.Float(), nullable=False),
    sa.Column("total_rows", sa.BigInteger(), nullable=False),
)

sql_exclusions_table = sa.Table(
    "sql_exclusions",
    metadata,
    sa.Column("id", UUID(as_uuid=True), nullable=False, primary_key=True),
    sa.Column("instance_id", UUID(as_uuid=True), nullable=True),
    sa.Column("match_type", sa.Text(), nullable=False),
    sa.Column("match_value", sa.Text(), nullable=False),
    sa.Column("reason", sa.Text(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
)
