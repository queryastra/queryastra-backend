# Screen: Annotations

**Route:** `/instances/[instanceId]/annotations`  
**Component:** `src/app/instances/[instanceId]/annotations/page.tsx`

## Purpose
Manage event annotations that appear as vertical markers on all instance charts. Used to
mark deploys, configuration changes, incidents, or any contextual event.

## API calls
- `GET /api/v1/annotations?instance_id=X` — list annotations for instance
- `POST /api/v1/annotations` — create annotation
- `PATCH /api/v1/annotations/{annotation_id}` — edit
- `DELETE /api/v1/annotations/{annotation_id}` — delete

## UI
- **Annotation table** — time, type (deploy/config/incident/other), title, description
- **New annotation button** — opens inline form
- Form fields: timestamp, type, title, description (optional), linked instance (optional)
- Delete with confirmation

## AnnotationLayer component
`AnnotationLayer` is a shared component that overlays annotation markers on any chart.
It fetches annotations for the current instance and time range, then renders a vertical
line + tooltip at each annotation's timestamp.

## Annotation types
- `deploy` — green flag
- `config` — blue flag
- `incident` — red flag
- `maintenance` — yellow flag
- `other` — grey flag
