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
