"""Nomes canonicos de capacidade e mandatoriedade por engine.

Fonte: `Docs/collector-queries.md` §7 (permissoes minimas confirmadas),
`Docs/rules/04-engine-capabilities.md`.
Uma capacidade mandatoria ausente reprova o cadastro (`InstanceRegistrationRejected` em
`app/services/instance_service.py`); uma capacidade opcional ausente so degrada
(`CapabilityStatus.DEGRADED`), sem bloquear.
"""

from __future__ import annotations

from app.collectors.base import CapabilityResult, CapabilityStatus

# PostgreSQL — Docs/collector-queries.md §7.
POSTGRESQL_SESSION_ACTIVITY = "session_activity"  # pg_read_all_stats
POSTGRESQL_CONFIGURATION_VISIBILITY = "configuration_visibility"  # pg_read_all_settings
POSTGRESQL_SESSION_TERMINATION = "session_termination"  # pg_signal_backend
POSTGRESQL_TOP_SQL = "top_sql"  # pg_stat_statements (opcional)
POSTGRESQL_QUALSTATS = "qualstats"  # pg_qualstats (opcional)

# SQL Server — Docs/collector-queries.md §7.
SQLSERVER_SERVER_STATE = (
    "server_state_visibility"  # VIEW SERVER STATE / VIEW SERVER PERFORMANCE STATE
)
SQLSERVER_DATABASE_VISIBILITY = "database_visibility"  # VIEW ANY DATABASE (opcional)
SQLSERVER_DEFINITION_VISIBILITY = "definition_visibility"  # VIEW ANY DEFINITION (opcional)
SQLSERVER_SESSION_TERMINATION = "session_termination"  # processadmin (opcional)
SQLSERVER_TABLE_READ_ACCESS = "table_read_access"  # db_datareader (opcional)

# MySQL / MariaDB / Aurora MySQL
MYSQL_PERFORMANCE_SCHEMA = "mysql_performance_schema"
MYSQL_SESSION_ACTIVITY = "mysql_session_activity"
MYSQL_WAIT_EVENTS = "mysql_wait_events"
MYSQL_PROCESS_VISIBILITY = "mysql_process_visibility"

# CockroachDB
COCKROACHDB_ENGINE = "cockroachdb_engine"

# Aurora
AURORA_POSTGRESQL_ENGINE = "aurora_postgresql_engine"
AURORA_MYSQL_ENGINE = "aurora_mysql_engine"

# Oracle
ORACLE_SESSION_ACTIVITY = "oracle_session_activity"  # SELECT on V$SESSION (CREATE SESSION)
ORACLE_QUERY_STATS = "oracle_query_stats"           # SELECT on V$SQL, V$SQLAREA (SELECT_CATALOG_ROLE ou SELECT ANY DICTIONARY)

# IBM DB2
DB2_SESSION_ACTIVITY = "db2_session_activity"       # EXECUTE on MON_GET_ACTIVITY (DBADM ou DATAACCESS)
DB2_QUERY_STATS = "db2_query_stats"                # EXECUTE on MON_GET_STATEMENT

# Amazon Redshift (PostgreSQL-compatible, via asyncpg)
REDSHIFT_SESSION_ACTIVITY = "redshift_session_activity"  # SELECT on SVV_SESSIONS / STV_WLM_QUERY_STATE

# Usado por todos os adaptadores quando a conexao nem abre (Docs/rules/02-safe-collection.md).
CONNECTIVITY = "connectivity"

MANDATORY_CAPABILITIES: dict[str, frozenset[str]] = {
    "postgresql": frozenset(
        {
            POSTGRESQL_SESSION_ACTIVITY,
            POSTGRESQL_CONFIGURATION_VISIBILITY,
            POSTGRESQL_SESSION_TERMINATION,
        }
    ),
    "sqlserver": frozenset({SQLSERVER_SERVER_STATE}),
    "mysql": frozenset({MYSQL_PERFORMANCE_SCHEMA}),
    "aurora_mysql": frozenset({MYSQL_PERFORMANCE_SCHEMA}),
    "cockroachdb": frozenset(
        {
            POSTGRESQL_SESSION_ACTIVITY,
            POSTGRESQL_CONFIGURATION_VISIBILITY,
            POSTGRESQL_SESSION_TERMINATION,
        }
    ),
    "aurora_postgresql": frozenset(
        {
            POSTGRESQL_SESSION_ACTIVITY,
            POSTGRESQL_CONFIGURATION_VISIBILITY,
            POSTGRESQL_SESSION_TERMINATION,
        }
    ),
    "oracle": frozenset({ORACLE_SESSION_ACTIVITY}),
    "db2": frozenset({DB2_SESSION_ACTIVITY}),
    "redshift": frozenset({REDSHIFT_SESSION_ACTIVITY}),
}


def has_missing_mandatory_capability(db_type: str, capabilities: list[CapabilityResult]) -> bool:
    """True se a conexao falhou ou se alguma capacidade mandatoria da engine esta UNAVAILABLE."""
    if any(
        c.capability == CONNECTIVITY and c.status is CapabilityStatus.UNAVAILABLE
        for c in capabilities
    ):
        return True
    mandatory = MANDATORY_CAPABILITIES.get(db_type, frozenset())
    return any(
        c.capability in mandatory and c.status is CapabilityStatus.UNAVAILABLE for c in capabilities
    )
