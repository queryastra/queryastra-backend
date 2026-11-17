# Screen: New Instance

**Route:** `/instances/new`  
**Component:** `src/app/instances/new/page.tsx`

## Purpose
Multi-step wizard for registering a new monitored database instance.

## API calls
- `POST /api/v1/instances/test-connection` — validate credentials before saving
- `POST /api/v1/instances` — persist the new instance

## Steps
1. **Engine selection** — SQL Server, PostgreSQL, MySQL, CockroachDB, Oracle
2. **Connection details** — host, port, database, username, password
3. **Test connection** — live check against the target server
4. **Settings** — collection interval, alert thresholds
5. **Confirm** — name, tags, finish

## Behavior
- "Test connection" button calls the test endpoint and shows result inline
- Save is disabled until connection test passes (or user bypasses)
- On success: redirects to the new instance's dashboard
- Duplicate host/port/database shows a warning

## Notes
- Password is never returned from the API after save (write-only field)
- Credentials are encrypted server-side before storage
