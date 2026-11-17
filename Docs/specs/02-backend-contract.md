# Backend API Contract

Base URL: `http://localhost:8124/api/v1`

All routes except `/auth/login`, `/auth/reset-password`, and `/api/v1/health` require a
valid JWT in the `Authorization: Bearer <token>` header.

## Authentication (`/auth`)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/auth/login` | No | Returns `{access_token, token_type}` |
| GET | `/auth/me` | Yes | Current user profile |
| PUT | `/auth/me/profile` | Yes | Update own profile |
| PUT | `/auth/me/password` | Yes | Change own password |
| GET | `/auth/plan` | Yes | Current plan info |
| GET | `/auth/users` | Admin | List all users |
| POST | `/auth/users` | Admin | Create user |
| PATCH | `/auth/users/{user_id}` | Admin | Edit user |
| DELETE | `/auth/users/{user_id}` | Admin | Soft-delete user |
| GET | `/auth/users/{user_id}/instances` | Admin | Instances accessible by user |
| POST | `/auth/users/{user_id}/instances` | Admin | Grant instance access |
| DELETE | `/auth/users/{user_id}/instances/{instance_id}` | Admin | Revoke instance access |
| POST | `/auth/users/{user_id}/reset-token` | Admin | Generate password-reset token |
| PUT | `/auth/users/{user_id}/password` | Admin | Force-set password |
| POST | `/auth/reset-password` | No | Reset password via token |

## Teams (`/auth/teams`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/auth/teams` | List teams |
| POST | `/auth/teams` | Create team |
| GET | `/auth/teams/{team_id}` | Team detail |
| PATCH | `/auth/teams/{team_id}` | Edit team |
| DELETE | `/auth/teams/{team_id}` | Delete team |
| POST | `/auth/teams/{team_id}/members/{user_id}` | Add member |
| DELETE | `/auth/teams/{team_id}/members/{user_id}` | Remove member |
| POST | `/auth/teams/{team_id}/instances/{instance_id}` | Add instance to team |
| DELETE | `/auth/teams/{team_id}/instances/{instance_id}` | Remove instance from team |

## Instances (`/instances`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/instances` | List accessible instances |
| POST | `/instances` | Register new instance |
| GET | `/instances/{instance_id}` | Instance detail + latest metrics |
| DELETE | `/instances/{instance_id}` | Soft-delete |
| PATCH | `/instances/{instance_id}/active` | Enable/disable monitoring |
| PATCH | `/instances/{instance_id}/settings` | Update collection settings |
| GET | `/instances/{instance_id}/health-check` | Live connectivity test |
| POST | `/instances/test-connection` | Test connection (pre-registration) |

## Metrics (`/metrics`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics/{target_type}/{target_id}` | Time-series metric data |
| GET | `/metrics/trends/{instance_id}` | Rolling trends |
| GET | `/metrics/available-metrics/{instance_id}` | Metric catalog for instance |

## Queries (`/queries`)

All query endpoints accept `instance_id` and optional `since`/`until` time range.

Key endpoints:
- `GET /queries/active/{instance_id}` — currently running queries
- `GET /queries/top-sql/{instance_id}` — top SQL by CPU/reads/duration
- `GET /queries/timeslice/{instance_id}` — workload by time bucket
- `GET /queries/detail/{instance_id}/{sql_hash}` — single query deep-dive
- `GET /queries/detail/{instance_id}/{sql_hash}/execution-plan` — cached plan XML
- `GET /instances/{instance_id}/slow-queries` — slow query history
- `GET /instances/{instance_id}/query-regressions` — queries that regressed

## Waits / Blocking (`/waits-blocking`)

- `GET /waits-blocking/blocking/{instance_id}` — blocking tree (live)
- `GET /waits-blocking/blocking/{instance_id}/incidents` — historical blocking incidents
- `GET /waits-blocking/waits/{instance_id}` — wait type summary

## Deadlocks (`/deadlocks`)

- `GET /deadlocks/{instance_id}` — list deadlocks (paginated)
- `GET /deadlocks/{instance_id}/{deadlock_id}` — full deadlock XML + parsed graph
- `GET /instances/{instance_id}/deadlock-history` — deadlock rate over time

## Transactions (`/transactions`)

- `GET /transactions/{instance_id}` — open/long-running transactions
- `GET /transactions/{instance_id}/{transaction_identifier}` — transaction detail

## Alerts (`/alerts`)

- `GET /alerts/metric-catalog` — available alert metrics
- `POST /alerts/rules` — create alert rule
- `GET /alerts/rules` — list rules
- `PATCH /alerts/rules/{rule_id}` — edit rule
- `DELETE /alerts/rules/{rule_id}` — delete rule
- `GET /alerts` — active alerts
- `POST /alerts/{alert_id}/acknowledge` — acknowledge
- `POST /alerts/{alert_id}/close` — close
- `POST /alerts/silences` — silence alerts

## Maintenance scans (`/instances/{instance_id}/...`)

All maintenance endpoints are on-demand; they connect to the monitored server each call.

| Endpoint | Description |
|----------|-------------|
| `GET .../databases` | List user databases (populates selector) |
| `GET .../index-health?database=X` | Index fragmentation, unused, duplicate indexes |
| `GET .../statistics-health?database=X` | Stale statistics |
| `GET .../memory-grants` | Memory grant requests and spills |
| `GET .../compression-estimate?database=X` | Compression savings estimate |
| `GET .../partitions?database=X` | Partitioned tables/indexes |

## Notifications (`/notifications`)

- `GET /notifications` — list (paginable, filterable by read/unread)
- `GET /notifications/unread-count` — badge count
- `POST /notifications/read-all` — mark all read
- `POST /notifications/{notification_id}/read` — mark one read

## Annotations (`/annotations`)

- `GET /annotations` — list (filterable by instance, time range)
- `POST /annotations` — create
- `PATCH /annotations/{annotation_id}` — edit
- `DELETE /annotations/{annotation_id}` — delete

## Exports (`/exports`)

- `POST /exports` — export table data as CSV or PDF

## Overview (`/overview`)

- `GET /overview/instances` — health summary for all accessible instances

## WebSocket (`/api/v1/ws/live`)

Streams live events (new alerts, metric spikes, notifications) to connected clients.
The handler runs two concurrent tasks (`forward_events` + `wait_for_disconnect`) and
unsubscribes cleanly when the client disconnects.

## Error responses

| Status | Meaning |
|--------|---------|
| 400 | Validation error |
| 401 | Missing or invalid JWT |
| 402 | Plan limit exceeded |
| 403 | Insufficient permissions |
| 404 | Resource not found |
| 502 | Error querying the monitored database |
| 504 | Timeout querying the monitored database |
