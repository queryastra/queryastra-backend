# Screen: Instance Queries

**Route:** `/instances/[instanceId]/queries`  
**Component:** `src/app/instances/[instanceId]/queries/page.tsx`

## Purpose
Top SQL workload analysis. Shows which queries consume the most resources, drillable by
database, wait type, user, program, or machine.

## API calls
- `GET /api/v1/queries/top-sql/{instance_id}` — ranked queries by CPU/reads/duration
- `GET /api/v1/queries/databases/{instance_id}` — workload by database
- `GET /api/v1/queries/timeslice/{instance_id}` — workload heatmap by time bucket
- `GET /api/v1/queries/wait-types/{instance_id}` — wait type breakdown
- `GET /api/v1/queries/users/{instance_id}` — workload by DB user
- `GET /api/v1/queries/programs/{instance_id}` — workload by application
- `GET /api/v1/queries/machines/{instance_id}` — workload by client machine

## UI
- **Top section**: time-range bar chart of total workload
- **SQL list**: sortable table of top queries (SQL text truncated, copy button)
- **Database pills**: click to filter to a specific database
- **Tab bar**: switch between CPU, Reads, Duration, Executions sort
- **Drill tabs**: Waits, Users, Programs, Machines pivot views

## Sub-routes
- `/queries/[databaseName]` — queries filtered to that database
- `/queries/[databaseName]/[sqlHash]` — single query deep-dive
- `/queries/slow` — slow query history
- `/queries/trends` — trend analysis across time
- `/queries/waits/[waitEventNative]` — queries waiting on a specific event
- `/queries/users/[dbUser]` — queries by a specific DB user
- `/queries/programs/[program]` — queries from a specific application
- `/queries/machines/[machine]` — queries from a specific machine

## Time range
`TimeRangePicker`. Changing the range re-fetches all data.
