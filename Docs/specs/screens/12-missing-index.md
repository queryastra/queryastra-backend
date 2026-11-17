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
- SQL Server: ✓ (`sys.dm_db_missing_index_details`)
- PostgreSQL: partial (pg_stat_user_tables scan patterns)
- MySQL: partial
