# Screen: Trends

**Route:** `/instances/[instanceId]/trends`  
**Component:** `src/app/instances/[instanceId]/trends/page.tsx`

## Purpose
Long-range trend analysis across all collected metrics. Identifies recurring patterns,
seasonal variation, and gradual drift over weeks or months.

## API calls
- `GET /api/v1/metrics/trends/{instance_id}` — trend indicators per metric
- `GET /api/v1/metrics/{target_type}/{target_id}` — raw series for selected metric

## UI
- **Metric picker** — select which metric to analyze
- **Trend chart** — weekly/monthly view with rolling average overlay
- **Day-of-week pattern** — average load by hour of week
- **Trend indicators** — up/flat/down arrows for each metric category
- **Anomaly events** — marked on the trend chart

## Notes
Long retention periods require TimescaleDB compression/aggregation to be configured.
Trending over 30+ days uses the hourly aggregate tables rather than raw metric rows.
