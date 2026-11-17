# Screen: Waits

**Route:** `/instances/[instanceId]/waits`  
**Component:** `src/app/instances/[instanceId]/waits/page.tsx`

## Purpose
Wait type analysis. Shows which wait events are consuming the most time, helping diagnose
I/O bottlenecks, lock contention, CPU pressure, and network latency.

## API calls
- `GET /api/v1/waits-blocking/waits/{instance_id}` — wait type summary
- `GET /api/v1/queries/wait-types/{instance_id}` — wait breakdown by time bucket
- `GET /api/v1/queries/wait-types-by-day/{instance_id}` — daily wait breakdown
- `GET /api/v1/queries/wait-types-by-hour/{instance_id}` — hourly wait breakdown
- `GET /api/v1/instances/{instance_id}/wait-heatmap` — wait event heatmap

## UI
- **Wait type pie/bar chart** — top wait types by total wait time
- **Trend chart** — wait time evolution over selected period
- **Heatmap** — wait intensity by hour of day vs. day of week
- **Wait type table** — name, normalized category, total wait ms, avg wait ms, waiting tasks
- Click a wait type → drills into `/queries/waits/[waitEventNative]`

## Wait categories (normalized)
Wait events are grouped into categories: CPU, I/O, Lock, Memory, Network, Log, Parallelism,
Service Broker, CLR, etc. Categories are consistent across SQL Server, PostgreSQL, and MySQL.
