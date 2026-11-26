# Screen: Transactions

**Route:** `/instances/[instanceId]/transactions`  
**Component:** `src/app/instances/[instanceId]/transactions/page.tsx`

## Purpose
Shows open long-running transactions and transaction errors. Useful for diagnosing lock
contention and rollback storms.

## API calls
- `GET /api/v1/transactions/{instance_id}` — open/long-running transactions
- `GET /api/v1/transactions/{instance_id}/{transaction_identifier}` — transaction detail

## UI
- **Transactions table** — session ID, database, duration, status, blocking, SQL text
- Filters: database, min duration, status (open/blocked/sleeping). The database options are
  built from the transactions loaded for the selected period (only databases with transactions
  in that window — intentional).
- Color coding: green → normal, yellow → long-running, red → blocking others
- Click → transaction detail page

## Transaction detail
- Full SQL text of the open transaction
- Wait chain visualization if blocking others
- Session metadata (login, program, machine, host process ID)
- Lock details (what objects are locked, lock type)

## Engine support
- SQL Server: ✓
- PostgreSQL: ✓ (idle-in-transaction sessions)
- MySQL: ✓ (INFORMATION_SCHEMA.INNODB_TRX)
