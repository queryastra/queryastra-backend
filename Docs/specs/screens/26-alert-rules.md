# Screen: Alert Rules

**Route:** `/alerts/rules`  
**Component:** `src/app/alerts/rules/page.tsx`

## Purpose
Manage the conditions that trigger alerts. Each rule monitors one metric on a scope
(global, specific instance, or specific database) and fires when a threshold is crossed.

## API calls
- `GET /api/v1/alerts/rules` — list rules
- `GET /api/v1/alerts/metric-catalog` — available metrics and operators
- `POST /api/v1/alerts/rules` — create rule (from `/alerts/rules/new`)
- `PATCH /api/v1/alerts/rules/{rule_id}` — edit rule
- `DELETE /api/v1/alerts/rules/{rule_id}` — delete rule

## Rule form (`/alerts/rules/new`, `/alerts/rules/[ruleId]/edit`)

**Component:** `AlertRuleForm.tsx`

Fields:
- **Name** — descriptive rule name
- **Metric** — from metric catalog (e.g. `cpu_percent`, `active_connections`)
- **Operator** — `>`, `<`, `>=`, `<=`, `=`, `!=`
- **Threshold** — numeric value
- **Scope** — Global / Instance / Database (each scope type shows the relevant selector)
- **Severity** — Critical / Warning / Info
- **Notification channels** — Email, Webhook, In-app, SMS
- **Cooldown** — minimum time between consecutive alerts for the same rule
- **Evaluation window** — time window for metric aggregation before comparison

## Scope types
- **Global** — fires when the metric exceeds the threshold on ANY instance
- **Instance** — fires for a specific instance only
- **Database** — fires for a specific database on a specific instance

## Notes
Every `Select.Value` in `AlertRuleForm` renders with a children function mapping the
raw value to its display label (not a bare `<SelectValue />`). This ensures the selected
value displays correctly after the initial form load.
