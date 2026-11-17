# Screen: Instance Overview / Dashboard

**Route:** `/instances/[instanceId]`  
**Component:** `src/app/instances/[instanceId]/page.tsx`

## Purpose
Top-level health dashboard for a single monitored instance. Shows current state and
real-time metrics.

## API calls
- `GET /api/v1/instances/{instance_id}` — instance metadata + latest status
- `GET /api/v1/metrics/{target_type}/{target_id}` — sparkline metrics (CPU, connections, waits)
- WebSocket `/api/v1/ws/live` — real-time updates

## Sections
- **Header** — instance name, engine, host, status (green/yellow/red), last collection time
- **Key metrics tiles** — CPU %, connections, active queries, top wait type
- **Activity chart** — query executions and wait time over the selected time window
- **Top databases** — CPU consumption by database (SQL Server)
- **Health panels** — schema health, buffer cache, hot objects, concurrency trend, workload
  classification (expanded from the DB Health sub-tabs)
- **Annotation layer** — markers for deploys and incidents overlaid on charts

## Navigation bar tabs
Every instance page shares the same top navigation bar:
- Visão geral (this page)
- Atividade (metrics)
- Saúde do índices ↓ (submenu: Saúde dos índices, Estatísticas, Índices ausentes)
- Problemas ↓ (submenu: Deadlocks, Transações, Bloqueios, Waits)
- Saúde ↓ (submenu: DB Health, Grants de memória, Backup, Vacuum, Storage, Capacidade)

## Time range
`TimeRangePicker` in the top-right. Presets: 1h, 6h, 24h, 7d, 30d. Custom range.
State stored in URL (`?since=`/`?until=`).
