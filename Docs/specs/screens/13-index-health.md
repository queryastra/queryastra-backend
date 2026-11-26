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
Scans run database by database ("Scan all" loops sequentially and fills the table as each
database finishes). Results are kept in `sessionStorage` per instance (`ih-map-{id}`,
`ih-ts-{id}`) so navigating away and back restores them; they are never persisted in the DB.

## Database selector contents
The selector is the **union** of the `/databases` list and the databases already scanned in
this session (`mergeDatabaseLists`, `src/lib/utils/databaseList.ts`). The scan history must
never *replace* the list: it is a subset, and replacing it made the selector (and "Scan all")
shrink to the one database scanned with "Scan this database". "Scan all" is disabled while
`/databases` is still loading, so it never runs on a partial list.

## Important: scan duration
The scan calls `sys.dm_db_index_physical_stats` for SQL Server, which is a heavyweight DMV.
Large databases (thousands of tables) can take 1–5 minutes per database.

Timeouts:
- `MAINTENANCE_PER_DATABASE_TIMEOUT_SECONDS` = 60s per database
- `MAINTENANCE_SCAN_TIMEOUT_SECONDS` = 300s total

If the per-database timeout fires, the backend returns HTTP 504 with the message:
"Varredura de saude de indices excedeu o tempo limite na instancia. Selecione um banco
especifico ou tente novamente."

## Database selector population (`GET /databases`)
1. First tries the cache: `SELECT DISTINCT database_name FROM query_samples` for the instance
   (all history, no time filter).
2. Only when there is no history it queries the monitored server live (15 s timeout → 504).

Known limitations of the cache path: it lists only databases where a query was ever captured
(quiet databases are missing) and, on instances with a long history, the `DISTINCT` can take
8–24 s.

## Engine support
- SQL Server: ✓ (full fragmentation data via `sys.dm_db_index_physical_stats`)
- PostgreSQL: ✓ (`pg_stat_user_indexes`, bloat estimation)
- MySQL: ✓ (`information_schema.STATISTICS`)
