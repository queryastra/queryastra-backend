# QueryAstra — Architecture Overview

## What it is

QueryAstra is a database performance monitoring and observability platform. It continuously
collects metrics from monitored database instances, stores them in TimescaleDB, and surfaces
the data through a web UI and alerting system.

## Stack

| Layer | Technology |
|-------|-----------|
| Backend API | FastAPI + SQLAlchemy async |
| Persistent store | TimescaleDB (PostgreSQL extension) |
| Migrations | Alembic |
| Encryption | Fernet symmetric encryption for credentials |
| Auth | JWT (HS256), 8h default expiry |
| Background workers | asyncio tasks (supervisors) + per-instance ThreadPoolExecutors |
| Frontend | Next.js 16, React Query v5, next-intl (11 locales) |
| Email | SMTP (configurable) or Brevo |
| Payments/billing | Stripe (Customer Portal proxy) |
| License verification | Supabase |
| Deployment | Cloudflare Pages (frontend), GitHub Releases (backend) |

## Ports (never change)

| Port | Service |
|------|---------|
| 3001 | QueryAstra frontend (Next.js) |
| 8124 | QueryAstra backend (uvicorn) |
| 3000 | AB-XQL-PM frontend |
| 8123 | AB-XQL-PM backend |

## Directory structure

```
queryastra-backend/
  app/
    api/v1/          # FastAPI routers
    collectors/      # Per-engine metric collectors (postgresql/, sqlserver/, mysql/, etc.)
    core/            # Config, logging, instance executors, credential handling
    models/          # SQLAlchemy ORM models
    normalizers/     # Pydantic output schemas (NormalizedXxx)
    repositories/    # Data access layer for TimescaleDB queries
    security/        # Auth, permission checks, credential resolver
    workers/         # Background supervisors and workers
    main.py          # Lifespan, router mounting, middleware
  migrations/        # Alembic versions
  tests/             # Unit and integration tests
  Docs/              # This documentation

queryastra-frontend/
  src/app/           # Next.js App Router pages
  src/components/    # Shared UI components
  src/hooks/         # React Query hooks
  src/lib/           # API client, utilities
  src/types/         # TypeScript type definitions
```

## Key architectural decisions

### Per-instance thread pool isolation

`pymssql` and `pyodbc` are synchronous and cannot be cancelled. When a SQL Server connection
hangs, `asyncio.wait_for` only drops the coroutine — the underlying thread stays stuck.
Using the default shared executor would eventually exhaust the event loop's thread pool.

Solution: each monitored instance has its own `ThreadPoolExecutor` with a small cap
(`INSTANCE_EXECUTOR_MAX_WORKERS=6`). A stuck thread only affects that one instance.

**Saturation watchdog:** if *all* of an instance's threads have been running for more than
`INSTANCE_EXECUTOR_STUCK_THRESHOLD_SECONDS` (default 90), the pool is discarded and recreated
before the next call, so the instance recovers without a backend restart. The orphaned threads
are tracked for the shutdown check below.

On shutdown, `exit_if_threads_stuck(grace_seconds=5)` starts a daemon timer that calls
`os._exit(0)` if any pool threads are still alive after 5 seconds, preventing uvicorn from
hanging indefinitely on reload or Ctrl+C.

### SQL Server driver: `pymssql>=2.4.6` (never lower)

pymssql 2.4.1 has a process-wide deadlock: in `_mssql.pyx`, `connection_list_lock` is a
non-reentrant `threading.Lock`; when a connection dies (timeout, network drop) `err_handler`
holds it and calls `mark_disconnected()`, which tries to take it again. That thread waits on
itself and every later `connect`/`close` in the process waits on the same lock: **all** SQL
Server instances go ERR at once until restart (PostgreSQL keeps working; the worker's thread
count grows without bound). 2.4.6 ("FIX:1018") calls `mark_disconnected()` outside the lock.

Rejected workaround: wrapping `pymssql.connect` in `socket.setdefaulttimeout()`. FreeTDS opens
its sockets in C, so Python's default timeout never reaches them (measured: no effect), and the
setting is process-global and racy between threads.

### Maintenance scans vs polling

The polling supervisor runs every few seconds and collects lightweight metrics using the
shared event loop. Maintenance scans (index health, statistics, compression) are on-demand,
run in the same per-instance thread pool, and have a 300-second global timeout
(`MAINTENANCE_SCAN_TIMEOUT_SECONDS`) with a 60-second per-database sub-timeout
(`MAINTENANCE_PER_DATABASE_TIMEOUT_SECONDS`).

### Plan enforcement

A `PlanService` checks the current license plan and enforces instance limits. Exceeding the
plan returns HTTP 402. Plan data is synced from Supabase.

### Worker enable flags

All background workers are opt-in via `.env` flags (e.g. `POLLING_SUPERVISOR_ENABLED=true`).
Default is `false` for most workers to allow safe initial setup.

## Supported database engines

- SQL Server (pymssql)
- PostgreSQL (asyncpg)
- MySQL / MariaDB (aiomysql)
- CockroachDB (asyncpg, same adapter as PostgreSQL)
- Oracle (cx_Oracle, limited)

## Migration chain

Alembic versions are chained. The head revision as of this writing is `f7a8b9c0d1e2`.
To upgrade: `alembic upgrade head`.
