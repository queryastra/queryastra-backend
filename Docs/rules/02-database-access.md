# Rule: Database Access

## TimescaleDB (internal store)
All reads/writes to the QueryAstra TimescaleDB use `AsyncSession` from SQLAlchemy async.
Never use sync sessions. Never call `session.execute()` with raw strings — always use
`text()` with bound parameters or ORM queries.

## Monitored instance connections
Connections to monitored databases (SQL Server, PostgreSQL, etc.) are opened on-demand by
collectors and maintenance scan endpoints. They are **not** persisted across requests.

Each collector opens a connection, runs its queries, and closes the connection in a
`finally` block. Leaking a connection ties up a SQL Server worker thread.

## Per-instance thread pools
SQL Server calls go through `run_in_instance_executor(instance_id, func, *args)`.
**Never** run a pymssql or pyodbc call directly on the asyncio event loop thread.

## Query timeouts
Every query on a monitored instance must have a timeout:
- Polling queries: `COLLECTION_QUERY_TIMEOUT_SECONDS` (default 5s)
- Maintenance scans: per-database `MAINTENANCE_PER_DATABASE_TIMEOUT_SECONDS` (default 60s),
  total `MAINTENANCE_SCAN_TIMEOUT_SECONDS` (default 300s)
- Deadlock queries: `DEADLOCK_QUERY_TIMEOUT_SECONDS` (default 30s)
- Transaction error queries: `TRANSACTION_ERROR_QUERY_TIMEOUT_SECONDS` (default 30s)

## Row limits
API endpoints cap returned rows at `API_READ_MAX_ROWS` (default 2000). Collectors cap at
`COLLECTION_MAX_ROWS` (default 5000). Maintenance scans cap at `MAINTENANCE_MAX_ROWS`
(default 1000). Never return unbounded result sets.

## Soft deletes
`Instance` supports soft-delete (`soft_deleted_at`). Soft-deleted instances are excluded
from all queries by default. Restoring requires explicitly clearing `soft_deleted_at`.

## Migration integrity
Never modify or reuse an Alembic revision ID. Each migration must have a globally unique
`revision` value. Chain migrations correctly (`down_revision` must point to the actual
previous head).
