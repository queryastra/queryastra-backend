# Rule: Worker Configuration

## All workers are opt-in
Every background worker has a boolean flag in `.env`. All default to `false` except
`ANOMALY_DETECTION_ENABLED`, `UPDATE_CHECK_ENABLED`, `TRIAL_EXPIRY_WORKER_ENABLED`,
and `PROBLEM_EVENTS_RETENTION_WORKER_ENABLED` (which default to `true`).

Flags currently enabled in the production `.env`:
```
POLLING_SUPERVISOR_ENABLED=true
ALERTING_WORKER_ENABLED=true
PLAN_HISTORY_SUPERVISOR_ENABLED=true
DEADLOCK_SUPERVISOR_ENABLED=true
```

## Workers must not crash the process
Each supervisor wraps its main loop in a broad exception handler that logs the error and
sleeps before retrying. An unhandled exception in a supervisor kills that worker, not the
process. Workers are restarted by the lifespan on the next poll cycle.

## Plan history
`PlanHistorySupervisor` captures plan changes for SQL Server and EXPLAIN-based engines
(PostgreSQL, MySQL). It is excluded for engines where plans are not meaningful.
Check: `if instance.db_type not in {"sqlserver"} | _EXPLAIN_BASED_TYPES: continue`

## Deadlock supervisor
`DeadlockSupervisor` reads SQL Server's ring buffer and Extended Events. It requires the
`VIEW SERVER STATE` permission on the monitored instance's login. Without it, the query
returns no rows silently — no error, no data.

## Alerting worker
`AlertingWorker` evaluates alert rules at `ALERTING_EVALUATION_INTERVAL_SECONDS` (60s).
It fires `AlertNotification` records for each channel (email, webhook, in-app). Email
delivery uses `EmailService` which requires SMTP config or Brevo API key.

## Shutdown behavior
On shutdown, `instance_executors.shutdown_all()` is called, then
`exit_if_threads_stuck(grace_seconds=5.0)`. If any driver thread is still alive after 5
seconds, the process exits forcibly via `os._exit(0)`. This prevents uvicorn reload and
Ctrl+C from hanging indefinitely on stuck pymssql/pyodbc threads.
