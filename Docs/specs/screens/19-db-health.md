# Screen: DB Health (Saúde do BD)

**Route:** `/instances/[instanceId]/db-health`  
**Component:** `src/app/instances/[instanceId]/db-health/page.tsx` (panels in `src/components/health/`)  
**Menu:** Saúde → Saúde do BD

## Purpose
On-demand database-level health panels that go beyond CPU/connection metrics. Each panel calls
its own endpoint and fails independently: one failing endpoint shows "Não foi possível conectar
a esta instância" only in that panel.

## Panels (in screen order)
| Panel | Endpoint (`/api/v1/instances/{id}/…`) | Shown as | Copy |
|---|---|---|---|
| Configuration compliance | `config-compliance` | table | ✓ |
| Replication slots (PostgreSQL) | `replication-slots` | table | ✓ |
| Bgwriter / checkpoints (PostgreSQL) | `bgwriter` | card | — |
| User functions (PL/pgSQL) | `user-functions` | table | ✓ |
| Connection consumers | `connection-consumers` | table | ✓ |
| TempDB usage (SQL Server) | `tempdb` | card | — |
| Wait events heatmap (24 h) | `wait-heatmap?window=24` | heatmap | — |
| Query performance trends (7 days) | `query-trends?days=7` | query table + chart | ✓ (table) |
| Table I/O statistics | `table-io-stats` | table | ✓ |
| Unused indexes | `unused-indexes` | table | ✓ |
| Schema health | `schema-health` | table | ✓ |
| Buffer cache and I/O | `buffer-cache` | 3 tables (tables in cache, temp files, I/O per file) | ✓ each |
| Contention hot spots | `hot-objects?window=24` | table | ✓ |
| Session concurrency | `concurrency-trend?window=24` | chart | — |
| Workload classification | `workload-classification?window=24` | pie chart | — |
| Database sizes | `db-sizes` | table with bars | ✓ |

Engine-specific panels return an empty result (or `null`) on engines that don't have the
concept — the panel shows its empty state, not an error.

## Database column
Every table that lists per-database data starts with **Banco de dados** (`database_name` in the API):
connection consumers (grouped by database too), query trends, table I/O, unused indexes, schema
health, buffer cache (tables in cache, temp files, I/O per file), contention hot spots, user functions
and database sizes. Configuration compliance and replication slots are instance-level (slots already
had their database).

Where the value comes from:
- PostgreSQL catalog/statistics views (`pg_stat_user_*`, `pg_statio_*`, `pg_class`): `current_database()`
  — they only cover the database the collector connects to.
- `pg_stat_activity`: `datname`. `pg_stat_statements` (query trends): `pg_database.datname` of `dbid`,
  stored in `query_stats_snapshot.database_name` (migration `d2e3f4a5b6c7`; older snapshots have none).
- SQL Server: `DB_NAME(database_id)` for sessions; `DB_NAME()` (login's default database) for schema
  health.
- Hot spots: `blocking_events.blocked_database_name` (grouped by database + object).

## "Copiar resultado"
Every table panel is wrapped in `CopyableTable` (`src/components/common/CopyableTable.tsx`),
which copies the **rendered** table as TSV (header + rows; bar columns become the value). The
button only appears when the panel actually renders a table — empty, unsupported and error
states have none. These tables neither truncate text nor reorder rows, so the rendered table is
the data.

Exception: the query-trends table cuts the query text at 80 characters, so its button builds
the TSV from the data (`toTsv`) with the full query text.

## Notes
- `query-trends` comes from `query_stats_snapshot` (pg_stat_statements snapshots) — empty on
  engines without that collection.
- SQL Server I/O per file uses `sys.dm_io_virtual_file_stats` columns
  `io_stall_read_ms` / `io_stall_write_ms`.
