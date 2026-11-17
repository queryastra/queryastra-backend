# Screen: Settings

**Route:** `/settings` and sub-routes  
**Component:** `src/app/settings/` hierarchy

## Purpose
System and account configuration. Split into several sub-pages accessed via the settings
flyout in the sidebar (the gear icon at the bottom-left).

## Sub-pages

### General (`/settings/geral`)
- Application name, logo, timezone, language defaults

### Management (`/settings/gerenciar`)
- Instance limits, collection intervals, retention periods

### AI (`/settings/ia`)
- AI provider (OpenAI / Anthropic), API key, model selection
- SQL Insight integration URL

### License (`/settings/licenca`)
- Current plan, usage vs limits
- Upgrade / billing portal link (proxied through queryastra.com)
- License key activation

### Collection Interval (`/settings/intervalo-coleta`)
- Global and per-instance collection frequency
- Per-database lock/sleep thresholds

### Criticality (`/settings/criticidade`)
- Customize severity thresholds for metrics

### Audit Log (`/settings/audit-log`)
- View all admin actions (user changes, instance changes, settings changes)
- Paginated, filterable by user, action type, date

### Refund (`/settings/reembolso`)
- Billing support / refund request form

### Users (`/settings/administracao/usuarios`)
- Create, edit, disable users
- Assign instance access
- Generate password-reset tokens

### Teams (`/settings/administracao/times`)
- Create and manage teams
- Add/remove members
- Assign instance access to entire teams

## Settings flyout
The gear button in the sidebar opens a flyout with links to all settings sub-pages.
The flyout uses `absolute bottom-0 left-full` positioning and `max-h-[calc(100vh-1rem)]
overflow-y-auto` to avoid being cut off by the viewport edge.

## API calls vary by sub-page
- `GET/PATCH /api/v1/settings` — general settings
- `GET /api/v1/auth/users` — user management
- `GET/POST/PATCH/DELETE /api/v1/auth/teams` — team management
- `GET /api/v1/audit-log` — audit log
- `GET /api/v1/license/` — license status
- `GET /api/v1/license/billing-portal` — Stripe portal URL
- `POST /api/v1/settings/test-smtp` — SMTP test
