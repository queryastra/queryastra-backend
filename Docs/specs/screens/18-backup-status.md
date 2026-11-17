# Screen: Backup Status

**Route:** `/instances/[instanceId]/backup-status`  
**Component:** `src/app/instances/[instanceId]/backup-status/page.tsx`

## Purpose
Shows the last backup time for each database on the instance. Quickly identifies databases
that have not been backed up recently.

## API calls
- `GET /api/v1/backup-status/{instance_id}` — per-database backup times

## UI
- **Backup table** — database, last full backup, last differential, last log backup, age
- Color coding by age: green (< 24h), yellow (24h–7d), red (> 7d or never)
- Filter by backup type or status

## Collection
`BackupStatusSupervisor` collects backup history periodically. Enable with
`BACKUP_STATUS_SUPERVISOR_ENABLED=true`.

## Engine support
- SQL Server: ✓ (`msdb.dbo.backupset`)
- PostgreSQL: ✓ (pg_basebackup history / WAL archiving)
- MySQL: limited (no native backup history table)
