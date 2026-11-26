# QueryAstra Backend — Claude Guidelines

## Overview
FastAPI backend for a database performance monitoring platform. See `Docs/specs/00-overview.md`
for the full architecture overview.

## Critical rules

### Never change ports
8124 is the QueryAstra backend. 8123 is AB-XQL-PM. 3001 is the QA frontend. 3000 is XQL-PM frontend.

### Database credentials are encrypted
All instance passwords use Fernet encryption via `CredentialResolver`. Never log credentials.
See `Docs/rules/01-credential-security.md`.

### SQL Server calls go through instance executors
`run_in_instance_executor(instance_id, func, *args)` — not `asyncio.to_thread()`.
Reason: pymssql is not cancellable; a stuck thread must be isolated per-instance.
See `app/core/instance_executors.py`.

### pymssql >= 2.4.6 — never lower
2.4.1 deadlocks the whole process when a SQL Server connection dies (every SQL Server instance
goes ERR until restart). Do not wrap `pymssql.connect` in `socket.setdefaulttimeout()`: it
doesn't reach FreeTDS sockets and is process-global. See `Docs/specs/00-overview.md`.

### One operation at a time per asyncpg connection
Never `asyncio.gather` several queries on the same asyncpg connection ("another operation is
in progress"). Run them sequentially or open one connection per query.

### Never run unbounded queries
All query results have row limits. See `Docs/rules/02-database-access.md`.

## Running the backend
```bash
uv run uvicorn app.main:app --reload --port 8124
```

## Running tests
```bash
uv run pytest tests/unit -x
```
Repository and integration tests hit the REAL dev database from `.env` — never write a test with
a global delete/purge that can reach collected data. See `Docs/rules/04-tests-shared-database.md`.

## Migrations
```bash
uv run alembic upgrade head
uv run alembic revision --autogenerate -m "description"
```
Never reuse a revision ID. Never modify an existing migration after it has been applied.

## Worker flags
All workers are disabled by default. See `Docs/rules/03-worker-configuration.md` for which
flags are currently enabled.

## Linting
```bash
uv run ruff check .
uv run ruff format .
```

## Documentation
- Architecture: `Docs/specs/00-overview.md`
- Data models: `Docs/specs/01-data-models.md`
- API contract: `Docs/specs/02-backend-contract.md`
- Screen specs: `Docs/specs/screens/`
- Rules: `Docs/rules/` (01 credentials, 02 database access, 03 workers, 04 tests on the shared dev DB)
