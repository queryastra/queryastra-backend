# Screen: Memory Grants (Memória)

**Route:** `/instances/[instanceId]/memory-grants`  
**Component:** `src/app/instances/[instanceId]/memory-grants/page.tsx` + `src/components/maintenance/MemoryGrantsPanel.tsx`  
**Menu:** Performance → Memória

## Purpose
On-demand view of memory grants. Identifies queries that request excessive memory (causing
others to wait on RESOURCE_SEMAPHORE), leave most of the grant unused, or spill to TempDB.

## API calls
- `GET /api/v1/instances/{instance_id}/memory-grants` — summary + active/pending grants + top
  queries by memory

## UI
- **Summary cards** — granted requests, waiting for memory (red when > 0), total granted, total
  used, available / target, cumulative memory wait
- **Running now** — sessions holding or waiting for work memory (waiting rows highlighted):
  session, state, query, requested, granted, used (+ max), ideal, wait, DOP, spill. Sortable.
- **Top queries by memory** — historical: severity, query (+ database), executions, avg grant,
  max grant, avg used (+ max), unused grant %, spills, spilled volume, last execution. Sortable.
  **"Copiar resultado"** copies it as TSV in the current sort order (full query on one line,
  database in its own column, severity translated).

## Engine support
- SQL Server: ✓ (`sys.dm_exec_query_memory_grants`, `sys.dm_exec_query_stats`)
- PostgreSQL: ✓ (`pg_stat_activity`, work_mem tracking)
- MySQL: limited

## Notes
No history is stored for the "Running now" table: it reflects the server at request time.
