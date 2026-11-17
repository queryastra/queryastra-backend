# Screen: Memory Grants

**Route:** `/instances/[instanceId]/memory-grants`  
**Component:** `src/app/instances/[instanceId]/memory-grants/page.tsx`

## Purpose
On-demand scan of memory grant requests. Identifies queries that request excessive memory
(causing others to wait) or spill to TempDB (causing I/O).

## API calls
- `GET /api/v1/instances/{instance_id}/memory-grants` — active/pending grants + top consumers

## UI
- **Active grants table** — query, database, requested KB, granted KB, used KB, wait time
- **Pending grants** — queries waiting for memory
- **Top memory consumers** — queries that historically request the most memory
- Spill indicator: queries that spilled to TempDB highlighted in red

## Engine support
- SQL Server: ✓ (`sys.dm_exec_query_memory_grants`)
- PostgreSQL: ✓ (`pg_stat_activity`, work_mem tracking)
- MySQL: limited

## Notes
This is an on-demand scan (no historical data). The result reflects the current state of
the server at the moment the button is clicked.
