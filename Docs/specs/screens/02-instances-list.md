# Screen: Instances List / Overview

**Route:** `/instances` and `/instances?view=overview`  
**Component:** `src/app/instances/page.tsx` + `_content.tsx`

## Purpose
Central hub showing all monitored database instances. Two views: grid list and health overview.

## API calls
- `GET /api/v1/instances` — list of instances with latest status
- `GET /api/v1/overview/instances` — health summary grid (overview view only)

## Views

### List view (default)
- Card grid of instances with name, engine badge, host, status indicator
- Quick-link to each instance's dashboard
- "New instance" button (plan-gated)
- Soft-deleted instances hidden by default

### Overview view (`?view=overview`)
- `InstanceHealthGrid` component
- Shows CPU, connections, queries/s, waits for every instance at a glance
- Color-coded health indicators (green/yellow/red)

## URL state
`useSearchParams().get("view") === "overview"` toggles the view. The page is wrapped in
`<Suspense fallback={null}>` for the `useSearchParams` requirement.

## Navigation
- Instances button in sidebar and QueryAstra logo always navigate here and reset any stale
  `?view=` param.
