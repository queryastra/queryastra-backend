# Screen: Query Detail

**Route:** `/instances/[instanceId]/queries/[databaseName]/[sqlHash]`  
**Component:** `src/app/instances/[instanceId]/queries/[databaseName]/[sqlHash]/page.tsx`

## Purpose
Deep-dive into a single query identified by its SQL hash. Shows execution history, plan,
resource usage, and AI-powered recommendations.

## API calls
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}` — query metadata and stats
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}/stats` — execution statistics history
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}/execution-plan` — cached plan XML (live, on-demand)
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}/plan-history` — distinct plans captured by `PlanHistorySupervisor`
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}/involvement?from&to` — sessions where this query took part in blocking, recorded transactions and deadlocks, plus linked-server origins (see below)
- `POST /api/v1/instances/{instance_id}/ai/deep-analysis` — AI analysis of query

## UI sections
- **SQL text** — full query text with syntax highlighting, copy button
- **Event involvement** — blocking / transactions / deadlocks / linked server for this query (see below)
- **Stats panel** — avg/max CPU, reads, duration; execution count; last seen
- **History chart** — execution time over the selected period
- **Execution plan** — visual plan tree with cost annotations (on-demand, requires plan still in cache)
- **Plan history panel** — list of past distinct plans with compare controls (see below)
- **Wait breakdown** — wait types and durations for this specific query
- **AI recommendations** — contextual optimization suggestions

## Event Involvement Panel (`QueryInvolvementPanel`)

**Component:** `src/components/queries/QueryInvolvementPanel.tsx`  
**Backend:** `app/services/query_involvement_service.py`

The detail screen is per QUERY (`sql_hash`); blocking, transactions and deadlocks are recorded per
SESSION. This panel lists the sessions in which this query took part in each kind of event, in the
same `from`/`to` period as the rest of the screen, with links to the existing detail screens
(each link carries `?return=` so "back" returns here):

| Section | How it is linked | `match` | Link target |
|---|---|---|---|
| Blocking | `blocking_incidents.blocker_sql_hash` / `blocked_sql_hash` = this hash (one row per role) | always exact | `/blocking/{sessionId}` (both sessions) |
| Recorded transactions | transaction referenced by one of those incidents (`*_transaction_id`) | `exact` | `/transactions/{transactionIdentifier}` |
| | same `session_id` ran this hash inside `[begin_time, closed_at or now]` | `probable` (session ids are reused) | same |
| Deadlocks | `detail.processes[].sql_hashes` contains this hash (collected since this feature) | `exact` | `/deadlocks/{deadlockId}` |
| | older deadlocks: hashes recomputed from `raw_xml_masked` (frame text + `inputbuf` only) | `probable` | same |
| Linked server | `machine` of this query's samples resolves to another registered instance's host | — | `/instances/{id}` |

Deadlock hashes: `app/normalizers/deadlock_sql_hashes.py` — candidate hashes per process (frame
text, `inputbuf` sliced by `stmtstart`/`stmtend`, whole `inputbuf`), same function as
`query_samples.sql_hash`. Best effort: a match is the same query; no match does not prove absence.

Each section is capped at 200 rows (`*_truncated` → "narrow the period" note). With nothing found
the panel says nothing was OBSERVED in the collected samples/events — not that the query was never
involved (a transaction whose query was never sampled cannot be linked).

## Plan History Panel (`PlanHistoryPanel`)

**Component:** `src/components/queries/PlanHistoryPanel.tsx`

Answers "this query was fast and now it's slow — did the plan change?" by showing every
distinct plan the `PlanHistorySupervisor` captured for this SQL hash over time.

The panel renders nothing (graceful degradation) when the history list is empty —
the supervisor is off by default; no empty-state card is shown.

### Plan list
Each row:
- Checkbox — selects the entry for comparison (max 2 checked; oldest checked entry is
  dropped when a third is added)
- Captured-at timestamp
- First 10 chars of `planHash` (monospace, truncated)
- "Ver gráfico" button → opens `ExecutionPlanDialog` for that snapshot inline

### Compare controls (shown below the list)
| Control | Enabled when | Action |
|---------|-------------|--------|
| "Comparar selecionados" | exactly 2 checked | Opens `PlanCompareDialog` with the two checked snapshots (older = before) |
| "Comparar com plano atual" | exactly 1 checked, not loading | Fetches live plan via `GET …/execution-plan`, then opens `PlanCompareDialog` with checked snapshot as before and live plan as after |

Error message shown if live plan fetch fails or plan is not available in cache.

### Compare dialog integration
`PlanCompareDialog` receives:
```ts
before: { label: "Capturado em <datetime>", text: planXml, format }
after:  { label: "Capturado em <datetime>" | "Plano atual", text: planXml, format }
```
See `33-plan-compare.md` for the full dialog spec.

### Backend worker
`PlanHistorySupervisor` runs periodically and stores a new `QueryPlanSnapshot` row only
when the plan hash differs from the most recently stored one for that SQL hash.
Worker is **disabled by default** — enabled per-instance via worker flags.
