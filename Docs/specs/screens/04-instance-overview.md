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
Every instance page shares the same navigation bar (`src/components/instances/InstanceTabs.tsx`):

| Entry | Screens (route) |
|---|---|
| Visão geral | this page (`/instances/[id]`) |
| Atividade ↓ | Atividade (`queries`), Tendências (`queries/trends`), Queries lentas (`queries/slow`), Anotações (`annotations`) |
| Performance ↓ | Capacidade (`capacity`), Métricas de recurso (`metrics`), Missing Indexes (`missing-index`), Saúde dos índices (`index-health`), Estatísticas (`statistics`), Particionamento e compressão (`storage`), Memória (`memory-grants`) |
| Problemas ↓ | Bloqueios (`waits`), Transações (`transactions`), Deadlocks (`deadlocks`), Anomalias (`anomalies`) |
| Saúde ↓ | Backup (`backup-status`), Vacuum / Saúde (`vacuum`), Saúde do BD (`db-health`) |

A group button shows the active screen's name when one of its screens is open.

### Screen title rule
Every instance screen (except this overview) shows, right below the tabs, an `h2` with the
**active tab's label** — rendered by `InstanceTabs` itself, so the visible title can never
diverge from the menu. Pages must not add their own page-level heading; screens with several
sections (Vacuum / Saúde, Saúde do BD, Queries lentas, Particionamento e compressão) use
section headings below it. The top-bar title (`pageTitle` i18n key) uses the same text as the
menu label, followed by the instance name.

## Time range
`TimeRangePicker` in the top-right. Presets: 1h, 6h, 24h, 7d, 30d. Custom range.
State stored in URL (`?since=`/`?until=`).
