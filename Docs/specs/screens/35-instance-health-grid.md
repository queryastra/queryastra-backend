# Screen: Instance Health Grid

**Component:** `src/components/instances/InstanceHealthGrid.tsx`  
**Hook:** `src/hooks/useInstancesOverview.ts`  
**Used in:** Instances list (`/instances`) as the grid view toggle

## Purpose
Card-based overview of all accessible instances, refreshed every
`OVERVIEW_REFRESH_MS` (30 s). Provides at-a-glance health status,
key metrics and open alert counts without navigating into each instance.

## API calls
- `GET /api/v1/instances/overview` — returns `InstanceOverviewCard[]`

## Grid layout
Responsive CSS grid (auto-fill, min card width ~280 px). Each card links to
`/instances/{id}`.

## HealthCard
Left border and status dot colour driven by `OverviewStatus`:

| Status   | Border / dot colour |
|----------|-------------------|
| up       | emerald-500 |
| warning  | amber-500 |
| critical | red-500 |
| down     | red-700 |
| unknown  | slate-400 |
| paused   | slate-300 |

**Card header:**
- Status dot (coloured circle)
- Instance name (truncated)
- Engine label (e.g. "PostgreSQL 16")
- Last collection time as relative time ("há 2 min")
- Alert triangle icon if `openAlerts.critical > 0`

**Metrics row (4 tiles):**
| Tile | Source field |
|------|-------------|
| CPU % | `card.cpuPercent` |
| Conexões | `card.connections` |
| Queries ativas | `card.activeQueries` |
| Wait type | `card.topWaitType` (or "—") |

CPU % tile text turns red when value is high (threshold defined by component).

**Open alerts row** (shown when `openAlerts.critical + openAlerts.warning > 0`):
- "N crítico(s)" badge (red) + "N aviso(s)" badge (amber)

## Header controls
- Refresh button (spins while fetching) — calls `refetch()`
- Last-updated timestamp

## Empty state
"Nenhuma instância encontrada" with "Adicionar instância" link to `/instances/new`.
