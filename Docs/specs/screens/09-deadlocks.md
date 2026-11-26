# Screen: Deadlocks

**Route:** `/instances/[instanceId]/deadlocks`  
**Component:** `src/app/instances/[instanceId]/deadlocks/page.tsx`

## Purpose
Lists deadlock events captured from the monitored instance. For SQL Server, reads from
Extended Events / ring buffer. Shows a deadlock heatmap and links to individual events.

## API calls
- `GET /api/v1/deadlocks/{instance_id}` — paginated deadlock list
- `GET /api/v1/instances/{instance_id}/deadlock-history` — rate-over-time for heatmap

## UI
- **DeadlockHistoryPanel** — heatmap of deadlock frequency by hour/day
- **Deadlock table** — timestamp, victim query text, participating tables, resolution
- Filters: database, time range. The database options are built from the deadlocks loaded for
  the selected period (only databases with deadlocks in that window — intentional).
- Click a row → deadlock detail page

## Deadlock detail (`/deadlocks/[deadlockId]`)

**API:** `GET /api/v1/deadlocks/{instance_id}/{deadlock_id}`

Shows:
- Full deadlock XML from SQL Server
- Graph visualization: nodes (processes), edges (resource locks)
- Victim process highlighted
- Query text for each participant
- Wait resource details (table, page, row)

## Collection
`DeadlockSupervisor` polls SQL Server's ring buffer and Extended Events session at
`DEADLOCK_CHECK_INTERVAL_SECONDS` (default 300s = 5 min). Must be enabled with
`DEADLOCK_SUPERVISOR_ENABLED=true`.

## Engine support
- SQL Server: ✓ (ring buffer + Extended Events)
- PostgreSQL: ✓ (pg_locks + deadlock log)
- MySQL: limited
