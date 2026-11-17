# Screen: Alerts

**Route:** `/alerts`  
**Component:** `src/app/alerts/page.tsx`

## Purpose
Central alert inbox. Shows active and recent alerts from all alert rules across all accessible
instances.

## API calls
- `GET /api/v1/alerts` — list alerts (filterable by status, severity, instance)
- `POST /api/v1/alerts/{alert_id}/acknowledge` — acknowledge
- `POST /api/v1/alerts/{alert_id}/close` — close
- `POST /api/v1/alerts/silences` — create silence

## UI
- **Alert list** — severity badge, rule name, instance, metric value, triggered at, status
- **Status filter** — Active / Acknowledged / Closed / All
- **Severity filter** — Critical / Warning / Info
- **Acknowledge button** — marks alert as acknowledged (still active, just seen)
- **Close button** — closes the alert (manual resolution)
- **Silence button** — suppress future alerts from the same rule for a configurable period

## Alert lifecycle
```
Triggered → Active → Acknowledged → Closed
                   ↘ Closed (direct)
```
