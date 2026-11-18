# Screen: Plan Compare Dialog

**Component:** `src/components/queries/PlanCompareDialog.tsx`  
**Opened from:** Query Detail (`07-query-detail.md`) — `PlanHistoryPanel`

## Purpose
Side-by-side visual and structural comparison of two execution plans captured at
different points in time. Helps identify regressions caused by statistics changes,
index drops or query rewrites.

## Props
```ts
interface ComparablePlan {
  label: string;   // "Capturado em 2026-11-01 14:32" or "Plano atual"
  text: string;    // raw plan text / XML / JSON
  format: PlanFormat; // "sqlserver_xml" | "postgresql_json" | "text"
}
```
Dialog receives `before` and `after` as `ComparablePlan | null`.

## Dialog structure
Full-width dialog, max 1400 px, max height 92 vh, scrollable.

**Header:** "Comparar planos" title + "before.label → after.label" description.

**Summary row (4 tiles):**
| Tile | Content |
|------|---------|
| Custo total | before → after, delta + % (red if +, green if −) |
| Adicionados | count of new operators (green) |
| Removidos | count of dropped operators (red) |
| Alterados | count of changed operators + unchanged count |

**Removed / Added operators section** (2-column, shown when count > 0):
- Each operator shown as a chip: operation name, optional object name (monospace), cost + rows

**Changed operators table** (shown when count > 0):
- Columns: Operador, Custo (before → after), Δ Custo, Linhas (before → after), Δ Linhas
- Deltas colour-coded: red if positive, green if negative

**Line diff section** (shown for text/JSON formats):
- Toggle "Apenas mudanças" (default on) — shows 2-line context around changes
- Side-by-side line numbers (before / after), prefix (+/−/space), colour-coded rows
- "⋯" separator between non-contiguous change groups
- "Diff muito grande" message if diff exceeds size limit

**Side-by-side plan panels** (2-column):
- Each panel: label header + scrollable plan viewer
- SQL Server XML → `ExecutionPlanDiagram`
- PostgreSQL JSON → `ExplainPlanTree`
- Other formats → `<pre>` with `prettyPlanText()`

## Plan diff algorithm
`comparePlans()` from `src/lib/utils/planDiff.ts`:
- Parses both plans into operator trees
- Matches operators by key (operation + object)
- Produces added / removed / changed / unchanged sets
- Computes total cost delta and line-level diff
