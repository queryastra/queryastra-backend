# Screen: Topology

**Route:** `/topology`  
**Component:** `src/app/topology/page.tsx`

## Purpose
Visual graph of the relationships between monitored instances and their underlying host
servers. Useful for understanding which physical servers host multiple database instances.

## API calls
- `GET /api/v1/topology` — graph data: nodes (instances + hosts) and edges

## UI
- **Force-directed graph** — instances and servers as nodes, connections as edges
- Instance nodes colored by health status
- Server nodes grouped by rack/datacenter (if configured)
- Click an instance node → navigates to that instance's dashboard
- Hover → shows instance details tooltip

## Notes
Servers are registered separately via `POST /api/v1/hosts`. The topology graph only shows
instances whose server has been registered. Instances without a registered server appear
as isolated nodes.
