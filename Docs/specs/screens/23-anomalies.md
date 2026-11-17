# Screen: Anomalies

**Route:** `/instances/[instanceId]/anomalies`  
**Component:** `src/app/instances/[instanceId]/anomalies/page.tsx`

## Purpose
Shows automatically detected metric anomalies. The anomaly detection worker compares
current metric values against a seasonal baseline to flag unusual spikes or drops.

## API calls
- `GET /api/v1/instances/{instance_id}/anomalies` — list of anomaly events

## UI
- **Anomaly list** — metric, timestamp, observed value, expected range, z-score, severity
- Filters: metric category, severity, time range
- Click an anomaly → navigates to the metric chart with the anomaly timestamp highlighted

## Detection algorithm
`AnomalyDetectionWorker` runs at `ANOMALY_DETECTION_INTERVAL_SECONDS` (default 300s).
For each metric, it:
1. Calculates the same-hour-of-week mean and standard deviation from the last
   `ANOMALY_BASELINE_WEEKS` weeks (default 4)
2. Computes z-score: `(current - mean) / std`
3. Flags as warning if z-score > `ANOMALY_WARNING_Z` (default 3.0)
4. Flags as critical if z-score > `ANOMALY_CRITICAL_Z` (default 4.0)

Enable with `ANOMALY_DETECTION_ENABLED=true` (enabled by default).
