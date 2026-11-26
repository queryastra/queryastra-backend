# Screen: Blocking (Bloqueios)

**Route:** `/instances/[instanceId]/waits` (route name kept for link compatibility)  
**Component:** `src/app/instances/[instanceId]/waits/page.tsx`  
**Menu:** Problemas → Bloqueios

## Purpose
Lock contention: who blocks whom. Two views of the same data — the blocking tree happening
now, and the history of blocking incidents (including closed ones, kept for later analysis).
Wait-type analysis lives in the Atividade drill-down (wait dimension) and in the wait heatmap of
Saúde do BD (`19-db-health.md`).

## API calls
- `GET /api/v1/waits-blocking/blocking/{instance_id}/incidents?open_only=false&from=…&to=…&database_name=…`
  — incident history (default view)
- `GET /api/v1/waits-blocking/blocking/{instance_id}?from=…&to=…` — open blocking tree
  (blocker → blocked), when "Somente em aberto" is checked
- `GET /api/v1/waits-blocking/blocking/{instance_id}/session/{session_id}?incident=` — session detail
  (`/blocking/[sessionId]?incident=`). Identity and **command** come from ONE incident (the
  anchor): `?incident=` when given (links from the query detail's "Event involvement"), otherwise
  the session's most recent incident. The command is the anchor side's `sql_hash` (sample of that
  session inside the incident window, else any sample with that hash) — never "the session's latest
  sample", because session ids are reused and connection pools run different commands on the same
  session seconds apart. With `?incident=`, only overlapping incidents are listed and the page shows
  a note with a "show all blocking for this session" link.
- WebSocket live updates (`useInstanceLiveUpdates`)

## UI
- "Somente em aberto" checkbox (`?openOnly=true`) — switches history ↔ live tree
- `TimeRangePicker` (URL `from`/`to`)
- **Database filter** (`?db=`) — options are the databases present in the rows loaded for the
  selected period/view (blocked and blocking sides), plus "Todos os bancos". Intentional: it
  lists only databases with blocking history in that window, not every database of the server.
- SPID search
- `BlockingIncidentsTable` (history) or `BlockingTreeTable` (open tree); click a session → detail

## Notes
Transactions (`10-transactions.md`) and Deadlocks (`09-deadlocks.md`) use the same rule for
their database filter: built from the rows of the selected period.
