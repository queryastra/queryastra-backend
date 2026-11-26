# Screen: Statistics Health (Estatísticas)

**Route:** `/instances/[instanceId]/statistics`  
**Component:** `src/app/instances/[instanceId]/statistics/page.tsx`

## Purpose
On-demand scan of statistics staleness. Stale statistics cause the query optimizer to
generate poor execution plans. This screen shows which statistics need UPDATE STATISTICS.

## API calls
- `GET /api/v1/instances/{instance_id}/databases` — populate database selector
- `GET /api/v1/instances/{instance_id}/statistics-health?database=X` — run scan

## UI
- **Database selector** — same as Index Health
- **"Scan all databases" / "Scan this database" buttons** — same pattern as Index Health
- **Last scan** — timestamp
- **Results table** — schema, table, statistic name, last updated, rows, rows modified since
  last update, modification %, staleness severity

## Staleness classification
| Rows Modified % | Severity |
|----------------|----------|
| < 20% | OK |
| 20–50% | Warning |
| > 50% | Critical |
| Never updated | Critical |

## Notes
Same scan timeout behavior as Index Health (`MAINTENANCE_SCAN_TIMEOUT_SECONDS=300s`).
Both screens share the `MaintenanceShared` base component (same two-button pattern), the same
`sessionStorage` persistence (`stat-map-{id}`, `stat-ts-{id}`) and the same database selector
rule: union of `/databases` with the already-scanned databases, "Scan all" disabled while the
list loads (see `13-index-health.md`).

## Engine support
- SQL Server: ✓ (`sys.stats`, `sys.dm_db_stats_properties`)
- PostgreSQL: ✓ (`pg_stat_user_tables`, `pg_statistic`)
- MySQL: ✓ (`information_schema.TABLE_STATISTICS`)
