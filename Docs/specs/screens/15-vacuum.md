# Screen: Vacuum / Idle-in-Transaction

**Route:** `/instances/[instanceId]/vacuum`  
**Component:** `src/app/instances/[instanceId]/vacuum/page.tsx`

## Purpose
PostgreSQL-specific. Shows auto-vacuum progress, tables that need manual vacuum, and sessions
stuck in idle-in-transaction state (which prevent autovacuum from running).

## API calls
- `GET /api/v1/instances/{instance_id}/vacuum` — vacuum statistics
- `GET /api/v1/instances/{instance_id}/idle-in-transaction` — stuck sessions

## UI
- **Tables needing vacuum** — name, dead rows, last vacuum, last autovacuum, bloat estimate
- **Autovacuum activity** — currently running vacuum workers
- **Idle-in-transaction sessions** — session ID, query, duration, blocking status

## Engine support
PostgreSQL only. The screen is hidden / shows "not supported" for SQL Server and MySQL.
