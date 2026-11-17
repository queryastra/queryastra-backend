# Screen: Storage

**Route:** `/instances/[instanceId]/storage`  
**Component:** `src/app/instances/[instanceId]/storage/page.tsx`

## Purpose
Shows current storage usage breakdown by database, file group, and file type (data vs log).

## API calls
- `GET /api/v1/instances/{instance_id}/db-sizes` — per-database size breakdown

## UI
- **Storage treemap or bar chart** — visual breakdown of database sizes
- **Table** — database name, data size, log size, total, % of server
- Sortable by size

## Engine support
- SQL Server: ✓ (`sys.master_files`, `sys.databases`)
- PostgreSQL: ✓ (`pg_database_size`)
- MySQL: ✓ (`information_schema.TABLE_SCHEMA`)
