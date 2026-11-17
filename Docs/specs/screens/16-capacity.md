# Screen: Capacity Forecast

**Route:** `/instances/[instanceId]/capacity`  
**Component:** `src/app/instances/[instanceId]/capacity/page.tsx`

## Purpose
Projects database storage growth based on historical trends. Helps plan disk capacity and
identify databases growing unexpectedly fast.

## API calls
- `GET /api/v1/capacity/forecast/{instance_id}` — per-database growth forecast

## UI
- **Growth forecast table** — database, current size, 30/90/180-day projections, monthly growth rate
- **Storage chart** — historical size + projected line
- Color coding: green (stable), yellow (growing), red (filling up within 90 days)
- Sortable by projected fill date

## Forecast method
Linear regression over the last 30 days of `db_sizes` metric data. Extrapolated to
configurable forecast horizon (default 180 days).
