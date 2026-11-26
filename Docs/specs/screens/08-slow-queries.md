# Screen: Slow Queries (Queries lentas)

**Route:** `/instances/[instanceId]/queries/slow`  
**Component:** `src/app/instances/[instanceId]/queries/slow/page.tsx`  
**Menu:** Atividade → Queries lentas

## Purpose
Top queries by duration per execution over the last days, with the daily average trend, and
regressions — queries whose average got worse after a cut-off point (e.g. a deploy).

## API calls
- `GET /api/v1/instances/{instance_id}/slow-queries?days=D&limit=N&order_by=avg|p95|max|total`
- `GET /api/v1/instances/{instance_id}/query-regressions?split=…&before_hours=H&after_hours=H&min_executions=N&min_change_pct=0`
- `GET /api/v1/annotations…` — last 30 days of annotations, offered as quick cut-off points

## UI
### Queries mais lentas
- Filters: period (1/7/14/30 days), Top (10/25/50), sort by (avg, p95, max, total); the
  effective window is shown next to them
- Table: #, query (name + masked SQL, truncated; full text in the tooltip), database,
  executions, avg, p95, max, total, daily-average sparkline
- Click a query → query detail (`07-query-detail.md`)
- **"Copiar resultado"** — copies the table as TSV: full SQL on one line (not the truncated
  text), durations formatted as on screen. Pastes into columns in Excel.

### Regressões
- Cut-off point (date/time or one of the recent annotations), window before/after (6/24/72/168 h),
  minimum executions
- Table: query, database, avg before, avg after, change % (badge), executions before/after

## Notes
The CTE in `slow_queries_repository.py` only counts runs that *started* inside the
selected time window (`min(time - duration_ms * interval '1 millisecond') >= :since`).
