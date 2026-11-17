# Screen: Global View

**Route:** `/global`  
**Component:** `src/app/global/page.tsx`

## Purpose
Cross-instance overview. Shows health, blocking, events, and top queries across all
accessible instances in a single view.

## API calls
- `GET /api/v1/overview/instances` — health summary for all instances
- `GET /api/v1/waits-blocking/blocking/{instance_id}` — per-instance blocking (aggregated)

## Sub-routes

### Global Blocking (`/global/blocking`)
Shows active blocking chains across all instances. Useful when multiple instances are
affected by a widespread problem.

### Global Events (`/global/events`)
Aggregated event feed: alerts, anomalies, deadlocks, and annotations from all instances
in a single timeline.

### Global Top Queries (`/global/top-queries`)
Top SQL across all instances ranked by total CPU/reads. Helps identify hot queries that
appear across multiple servers.

## UI
- Instance health grid (same as `/instances?view=overview`)
- Cross-instance alert count badges
- Quick-jump links to individual instance screens
