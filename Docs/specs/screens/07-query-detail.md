# Screen: Query Detail

**Route:** `/instances/[instanceId]/queries/[databaseName]/[sqlHash]`  
**Component:** `src/app/instances/[instanceId]/queries/[databaseName]/[sqlHash]/page.tsx`

## Purpose
Deep-dive into a single query identified by its SQL hash. Shows execution history, plan,
resource usage, and AI-powered recommendations.

## API calls
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}` — query metadata and stats
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}/stats` — execution statistics history
- `GET /api/v1/queries/detail/{instance_id}/{sql_hash}/execution-plan` — cached plan XML
- `POST /api/v1/instances/{instance_id}/ai/deep-analysis` — AI analysis of query

## UI
- **SQL text** — full query text with syntax highlighting, copy button
- **Stats panel** — avg/max CPU, reads, duration; execution count; last seen
- **History chart** — execution time over the selected period
- **Execution plan** — visual plan tree with cost annotations
- **Plan history** — list of past plans with timestamps; diff view on click
- **Plan compare dialog** — side-by-side diff of two plan versions (`PlanCompareDialog`)
- **Wait breakdown** — wait types and durations for this specific query
- **AI recommendations** — contextual optimization suggestions

## Plan compare
`PlanCompareDialog` shows a diff of two `QueryPlanSnapshot` rows. Uses the `planDiff` util
to highlight added/removed/changed operators.
