# Screen: System Status

**Route:** `/status`  
**Component:** `src/app/status/page.tsx`

## Purpose
Shows the operational status of the QueryAstra system itself: worker health, database
connectivity, license state, and software version.

## API calls
- `GET /api/v1/status/overview` — system status summary
- `GET /api/v1/update/check` — latest available version

## UI
- **Backend workers** — each worker's enabled/running/last-run status
- **Database connectivity** — TimescaleDB connection status
- **License** — plan, expiry date, instance count vs limit
- **Version** — current version, available version (if update available)

## Notes
This page is accessible only to admin users. Regular users see a simplified version.
