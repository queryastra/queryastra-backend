# Rule 04 — Tests run against the real dev database

`tests/unit/repositories` and `tests/integration` use the database in `.env` (`DATABASE_URL`) —
the SAME database the running app collects into. There is no separate test database.

## Never
- Call a global operation (purge/retention, `DELETE`/`UPDATE` without an instance filter,
  `TRUNCATE`) with a cutoff or filter that can reach real rows. Example of what went wrong
  (2026-11-26): purge tests cut at "now + 1s" over the whole table and deleted every ended
  `blocking_incidents` row and every closed `transaction_events` row of the dev database.

## Do
- Scope every write/delete to the test instance created by the `test_instance` fixture.
- When the method under test is global by design (e.g. `purge_ended_before`,
  `purge_closed_before`), give the test rows an ancient timestamp (year 2000) and cut just after
  it — see `_ANCIENT` in `tests/unit/repositories/test_blocking_incidents_repository.py`.
- If collected data disappears from the dev database, suspect a test run first.
