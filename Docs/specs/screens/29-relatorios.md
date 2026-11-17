# Screen: Scheduled Reports

**Route:** `/relatorios`  
**Component:** `src/app/relatorios/page.tsx`

## Purpose
Configure and manage automated reports that run on a cron schedule and deliver results
by email.

## API calls
- `GET /api/v1/reports` — list reports
- `GET /api/v1/reports/executions` — execution history log
- `POST /api/v1/reports` — create report
- `PUT /api/v1/reports/{report_id}` — update report
- `DELETE /api/v1/reports/{report_id}` — delete report
- `PATCH /api/v1/reports/{report_id}/toggle` — enable/disable
- `POST /api/v1/reports/{report_id}/download` — download generated report

## Report configuration
- **Name** — descriptive label
- **Type** — Index Health, Statistics Health, Missing Indexes, Slow Queries, etc.
- **Schedule** — cron expression (e.g. `0 8 * * 1` = every Monday at 8am)
- **Target instance** — which instance to report on
- **Target database** — specific database or "all"
- **Email recipients** — comma-separated list
- **Format** — CSV or PDF

## Execution history
Shows each run with status (success/failure/running), generated report download link,
and any error message.

## Engine
`ReportWorker` checks for due reports at `REPORT_WORKER_CHECK_INTERVAL_SECONDS` (default 60s).
Enable with `REPORT_WORKER_ENABLED=true`. PDF generation uses `TablePDFService`.
