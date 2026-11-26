# Screen: Missing Indexes

**Route:** `/instances/[instanceId]/missing-index`  
**Component:** `src/app/instances/[instanceId]/missing-index/page.tsx`

## Purpose
Shows index suggestions from the database engine's DMVs / optimizer hints. Helps identify
missing indexes that would have the most impact on query performance.

## API calls
- `GET /api/v1/missing-index-suggestions/{instance_id}/collection-info` — last scan metadata
- `POST /api/v1/missing-index-suggestions/{instance_id}` — trigger on-demand scan
- `GET /api/v1/missing-index-suggestions/{instance_id}` — list suggestions

## UI
- **Last scan info** — timestamp, database scanned, suggestion count
- **Database selector** — "All databases" or a specific database
- **"Scan all databases" button** — always visible; triggers full scan regardless of selector
- **"Scan this database" button** — visible only when a specific database is selected
- **Suggestions table** — table, columns, estimated improvement %, CREATE INDEX script
- **Copy script button** — copies the CREATE INDEX statement to clipboard

## State management
`scanTarget` state tracks whether the pending scan covers all databases or a specific one.
Both buttons call `collectMutation`; the target is passed as a parameter.
`isBusy = isScanningAll || isScanningSelected` controls button disabled state.

## Engine support
- SQL Server: ✓ (`sys.dm_db_missing_index_details` — suggests specific columns)
- PostgreSQL (and Aurora PG / CockroachDB / Redshift, which delegate to it): heuristic per
  database (`pg_stat_user_tables`, seq_scan ≫ idx_scan); with the `pg_qualstats` extension,
  adds column-level suggestions — best-effort, a failure there never drops the heuristic ones
- MySQL / MariaDB / Aurora MySQL: `performance_schema.table_io_waits_summary_by_index_usage`
- Oracle, Db2: tables with no index and many rows

Only SQL Server (and PostgreSQL with `pg_qualstats`) name columns; the others point to the
candidate table. The list only contains databases that have suggestions — it is expected that
a database present in other screens does not appear here.
