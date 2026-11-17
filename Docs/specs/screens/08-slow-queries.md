# Screen: Slow Query History

**Route:** `/instances/[instanceId]/queries/slow`  
**Component:** `src/app/instances/[instanceId]/queries/slow/page.tsx`

## Purpose
Historical log of queries that exceeded the slow-query threshold. Shows regressions —
queries that became significantly slower over time.

## API calls
- `GET /api/v1/instances/{instance_id}/slow-queries` — paginated history
- `GET /api/v1/instances/{instance_id}/query-regressions` — regression analysis

## UI
- **Slow queries table** — query text, database, duration, executions, first/last seen
- **Regression tab** — queries whose P95 duration increased significantly vs. baseline period
- Filters: database, duration threshold, time range
- Click a row → navigates to query detail page

## Regression detection
The backend compares rolling P95 duration of recent executions vs. a baseline window
(configurable). A query appears in the regressions tab when its recent P95 is more than
2× the baseline P95.

## Slow query threshold
Configurable per instance in settings. Default: queries exceeding 1000ms.
The CTE in `slow_queries_repository.py` only counts runs that *started* inside the
selected time window (`min(time - duration_ms * interval '1 millisecond') >= :since`).
