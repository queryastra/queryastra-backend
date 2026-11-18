# Screen: Wait Heatmap

**Component:** `src/components/health/WaitHeatmap.tsx`  
**Used in:** Deadlocks page (`09-deadlocks.md`), DB Health (`19-db-health.md`)

## Purpose
Time × wait-type grid showing wait pressure across the selected time window.
Rows = wait categories; columns = time buckets. Cell intensity reflects total wait
time for that category in that interval — allows spotting spikes and shifts in wait
profile at a glance.

## Data
```ts
interface WaitHeatmapBucket {
  bucket: string;         // ISO timestamp (time bucket start)
  wait_type: string;      // e.g. "CPU", "IO", "LOCK"
  total_wait_ms: number;
}
```
API: `GET /api/v1/instances/{id}/waits/heatmap?since=&until=`

## Layout
Grid: rows indexed by wait category, columns indexed by sorted unique time buckets.

**X axis:** time labels derived from `bucket` ISO string formatted as `HH:MM`.

**Y axis:** fixed category order:
`CPU · IO · LOCK · LATCH · MEMORY · NETWORK · IPC · IDLE · OTHER`

**Cell rendering:**
- Background opacity proportional to `total_wait_ms / maxWait` (linear scale)
- Base colour per category (see colour map below)
- Hover tooltip: category, bucket time, `total_wait_ms` formatted (ms / s / min)

**Empty state:** "Sem dados de waits no período" text.

## Category colour map
| Category | Colour token |
|----------|-------------|
| CPU      | blue-500 |
| IO       | orange-500 |
| LOCK     | red-500 |
| LATCH    | red-400 |
| MEMORY   | purple-500 |
| NETWORK  | cyan-500 |
| IPC      | teal-500 |
| IDLE     | gray-400 |
| OTHER    | slate-500 |

## Notes
- Categories not present in `buckets` still render as an empty row for visual consistency.
- `maxWait` is computed across all buckets; a dataset with only one non-zero category
  still uses the global max so relative intensity is comparable across categories.
