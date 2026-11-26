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
PostgreSQL concepts. The menu entry (Saúde → Vacuum / Saúde) is shown for every engine; on
engines without vacuum the endpoints return empty lists and each section shows its empty state
("Nenhum dado encontrado"), never an error.

## Implementation notes
- `collect_vacuum_stats` runs its two queries (table stats, autovacuum workers) **one after the
  other**: an asyncpg connection runs one operation at a time, and running them with
  `asyncio.gather` on the same connection failed every time ("another operation is in
  progress" → 502).
- The screen also shows index bloat (`GET /api/v1/instances/{instance_id}/index-bloat`).
