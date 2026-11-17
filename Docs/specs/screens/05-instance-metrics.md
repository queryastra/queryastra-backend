# Screen: Instance Metrics (Activity)

**Route:** `/instances/[instanceId]/metrics`  
**Component:** `src/app/instances/[instanceId]/metrics/page.tsx`

## Purpose
Detailed time-series charts for all collected metrics on an instance.

## API calls
- `GET /api/v1/metrics/available-metrics/{instance_id}` — metric catalog (what is collected)
- `GET /api/v1/metrics/{target_type}/{target_id}` — raw metric series
- `GET /api/v1/metrics/trends/{instance_id}` — trend indicators

## UI
- Grouped metric charts (CPU, Memory, I/O, Network, Connections, etc.)
- Each chart: line chart with min/max/avg, zoom into any range
- Metric selector: show/hide individual series
- Anomaly markers (from `AnomalyDetectionWorker`)
- Annotation overlay (from `/api/v1/annotations`)

## Supported metrics vary by engine

| Metric | SQL Server | PostgreSQL | MySQL |
|--------|-----------|-----------|-------|
| CPU % | ✓ | ✓ | ✓ |
| Connections | ✓ | ✓ | ✓ |
| Buffer hit ratio | ✓ | ✓ | ✓ |
| Deadlocks/s | ✓ | ✓ | — |
| Replication lag | ✓ | ✓ | ✓ |
| TempDB usage | ✓ | — | — |
