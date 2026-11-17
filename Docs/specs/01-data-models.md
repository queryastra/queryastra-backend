# Data Models

All models live in `app/models/`. SQLAlchemy async ORM backed by TimescaleDB.

## Core entities

### User (`user.py`)
Represents a system user. Fields: `id`, `username`, `email`, `hashed_password`, `role`
(`admin`/`user`), `is_active`, `full_name`, `created_at`, `last_login_at`.

Relationships: many-to-many with `Instance` through `instance_access` join table;
many-to-many with `Team` through `TeamMember`.

### Instance (`instance.py`)
A monitored database server endpoint. Fields: `id` (UUID), `name`, `db_type` (`sqlserver`,
`postgresql`, `mysql`, `cockroachdb`, `oracle`), `host`, `port`, `database`, `username`,
`encrypted_password`, `is_active`, `created_at`, `soft_deleted_at`, plus polling/collection
settings.

Credentials are Fernet-encrypted at rest. The `CredentialResolver` in
`app/security/credential_reference.py` decrypts them (or fetches from an external vault).

### Team (`team.py`)
Groups users for access control. `Team` → `TeamMember` (users) → `TeamInstanceAccess`
(instances the team can see). Team membership grants instance visibility to all team members.

### AlertRule / Alert (`alert.py`)
`AlertRule` defines a threshold condition on a metric for a scope (instance, database, global).
`Alert` is a fired instance of a rule. `AlertNotification` tracks per-channel delivery.
`AlertSilence` suppresses alerts for a time window.

### Annotation (`annotation.py`)
Free-text markers on the timeline (deploys, config changes, incidents). Associated with an
instance (optional) and a time range. Rendered as overlays on all charts.

### Anomaly models (`anomaly.py`)
`AnomalyMetricHourly` stores aggregated metric values used as the baseline for anomaly
detection. `AnomalyMetricStatus` tracks per-metric detection state. `AnomalyEvent` records
detected anomalies.

### QueryPlanSnapshot (`query_plan_snapshot.py`)
Snapshots of execution plans captured by `PlanHistorySupervisor`. Used by the plan comparison
UI to show when a query plan changed.

### DeadlockEvent (`deadlock_event.py`)
Single deadlock captured from SQL Server Extended Events or ring buffer. Contains the raw XML
from SQL Server plus parsed victim/node info.

### Notification (`notification.py`)
In-app notification (bell icon in TopNav). Created by the alerting worker or other events.
Has `is_read`, `severity`, `title`, `body`, `link`.

### MissingIndexSuggestion (`missing_index_suggestion.py`)
An index that SQL Server's DMVs recommend. Contains the CREATE INDEX script, estimated
improvement, and the databases/queries that would benefit.

### BackupStatus (`backup_status.py`)
Last backup timestamp per database per instance. Collected by `BackupStatusSupervisor`.

### ScheduledReport (`scheduled_report.py`)
Configuration for a report that runs on a cron schedule and delivers results by email.

### SystemSettings / SystemLicense
Singleton rows that hold global configuration and the current license state.

## TimescaleDB hypertables

Metric time-series are stored in hypertables (auto-chunked by time). Key tables:
- `metric_values` — raw numeric metrics per instance per timestamp
- `query_stats` — per-query execution statistics snapshots
- `active_queries` — point-in-time snapshots of running queries

The `repositories/` layer wraps these with async SQL queries rather than ORM queries for
performance.
