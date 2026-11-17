# Screen: Index Health (Saúde dos Índices)

**Route:** `/instances/[instanceId]/index-health`  
**Component:** `src/app/instances/[instanceId]/index-health/page.tsx`

## Purpose
On-demand scan of index fragmentation, unused indexes, duplicate indexes, and overlapping
indexes. Helps DBAs prioritize REBUILD/REORGANIZE operations.

## API calls
- `GET /api/v1/instances/{instance_id}/databases` — populate database selector
- `GET /api/v1/instances/{instance_id}/index-health?database=X` — run scan (on-demand)

## UI
- **Database selector** — "All databases" or a specific database
- **"Scan all databases" button** — always visible
- **"Scan this database" button** — visible when a specific database is selected
- **Last scan** — timestamp updated after each scan
- **Results table** — schema, table, index name, type, fragmentation %, page count, action recommendation

## State behavior
Scan results persist in the page state until a new scan is triggered. Navigating away and
back shows "No scan yet" until the user triggers one (results are not persisted in the DB).

## Important: scan duration
The scan calls `sys.dm_db_index_physical_stats` for SQL Server, which is a heavyweight DMV.
Large databases (thousands of tables) can take 1–5 minutes per database.

Timeouts:
- `MAINTENANCE_PER_DATABASE_TIMEOUT_SECONDS` = 60s per database
- `MAINTENANCE_SCAN_TIMEOUT_SECONDS` = 300s total

If the per-database timeout fires, the backend returns HTTP 504 with the message:
"Varredura de saude de indices excedeu o tempo limite na instancia. Selecione um banco
especifico ou tente novamente."

## Database selector population
`/databases` uses `maintenance_per_database_timeout_seconds` (not the polling timeout)
and returns all accessible user databases without a cap (the previous 50-database cap was
removed to support instances with hundreds of databases).

## Engine support
- SQL Server: ✓ (full fragmentation data via `sys.dm_db_index_physical_stats`)
- PostgreSQL: ✓ (`pg_stat_user_indexes`, bloat estimation)
- MySQL: ✓ (`information_schema.STATISTICS`)
