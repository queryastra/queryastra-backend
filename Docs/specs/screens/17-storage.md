# Screen: Partitioning & Compression (Particionamento e compressão)

**Route:** `/instances/[instanceId]/storage` (route name kept for link compatibility)  
**Component:** `src/app/instances/[instanceId]/storage/page.tsx`  
**Menu:** Performance → Particionamento e compressão (moved from the "Saúde" group in 0.2.2;
formerly named "Armazenamento" / "Storage")

## Purpose
DBA analysis of table/index partitioning and of data compression savings. Database sizes live
in **Saúde do BD** (`db-health`), not here.

## API calls
- `GET /api/v1/instances/{instance_id}/databases` — database selector
- `GET /api/v1/instances/{instance_id}/partitions?database=X` — loaded when the screen opens and
  on "Atualizar"
- `GET /api/v1/instances/{instance_id}/compression-estimate?database=X&compression=PAGE|ROW&max_objects=N`
  — **only on click of "Estimar"**: it samples real data on the monitored server
  (`sp_estimate_data_compression_savings` on SQL Server)

## UI
- **Screen title** "Particionamento e compressão" (from the menu, see `04-instance-overview.md`)
- **Database selector** + "Atualizar" (`MaintenanceToolbar`)
- **Particionamento** section — `PartitionsPanel`: severity, object (`db.schema.table` + index),
  method/scheme/function, partition key, partition count, last boundary, rows beyond the last
  boundary (highlighted — sliding window behind), total rows, compression. Rows expand to the
  per-partition detail and the recommended command (with copy button). Sortable.
- **Compressão** section — `CompressionPanel`: compression type (PAGE/ROW) and max objects;
  after "Estimar": totals (current, estimated, savings), minimum-savings filter, table (object,
  index, current → requested compression, current size, estimated size, savings %, command).
- **"Copiar resultado"** on both tables — copies the table as TSV in the current sort order
  (pastes into columns in Excel). Compression also keeps **"Copiar todos os comandos"**
  (only the `ALTER ... REBUILD` DDL above the minimum savings).

## Database column
Every result table has a **Banco de dados** column of its own (sortable, also in the copy and in the
PDF/CSV export), before the object column. The object column shows only `schema.table` — the
database is no longer a prefix there.

## Engine support
Both endpoints exist for every collector; each report's `meta.supported` tells the screen
whether the engine supports it (otherwise `UnsupportedEngineState`). Richest data on SQL Server
(`sys.partitions`, `sys.partition_schemes`, `sys.partition_functions`).
