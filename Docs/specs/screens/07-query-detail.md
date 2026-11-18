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
- `POST /api/v1/instances/{instance_id}/ai/deep-analysis` — AI analysis of query

## UI sections
- **SQL text** — full query text with syntax highlighting, copy button
- **Stats panel** — avg/max CPU, reads, duration; execution count; last seen
- **History chart** — execution time over the selected period
- **Execution plan** — visual plan tree with cost annotations (on-demand, requires plan still in cache)
- **Plan history panel** — list of past distinct plans with compare controls (see below)
- **Wait breakdown** — wait types and durations for this specific query
- **AI recommendations** — contextual optimization suggestions

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
