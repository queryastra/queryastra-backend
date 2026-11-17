# Screen: DB Health

**Route:** `/instances/[instanceId]/db-health`  
**Component:** `src/app/instances/[instanceId]/db-health/page.tsx`

## Purpose
Aggregated database-level health panels. Shows five dimensions of database health that go
beyond simple CPU/connection metrics.

## API calls
- `GET /api/v1/instances/{instance_id}/schema-health` — table counts, missing PKs, wide tables
- `GET /api/v1/instances/{instance_id}/buffer-cache` — buffer hit ratios, memory pressure
- `GET /api/v1/instances/{instance_id}/hot-objects` — most accessed tables and indexes
- `GET /api/v1/instances/{instance_id}/concurrency-trend` — locks, waits, deadlocks trend
- `GET /api/v1/instances/{instance_id}/workload-classification` — query pattern analysis

## Panels

### Schema Health
- Table count per database
- Tables without primary keys
- Very wide tables (>50 columns)
- Tables with no indexes

### Buffer Cache
- Buffer hit ratio trend
- Pages in cache by database
- Memory pressure indicators

### Hot Objects
- Top tables by logical reads
- Top indexes by range scan / seek count
- Heatmap of access patterns

### Concurrency Trend
- Lock waits per second
- Deadlock rate
- Blocking chain frequency

### Workload Classification
- Query pattern distribution (point lookups vs. range scans vs. aggregations)
- Application-level breakdown

## Engine support
Panels vary by engine. SQL Server provides the richest data. PostgreSQL and MySQL have
equivalent but different DMVs/catalog views.
