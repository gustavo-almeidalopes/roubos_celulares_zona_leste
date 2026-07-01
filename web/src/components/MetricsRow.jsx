import MetricCard from "./MetricCard";
import { formatDeltaPct, formatInt, formatPct } from "../lib/format";

/** Porta de metrics.py::render_metric_cards para React. */
export default function MetricsRow({ metrics }) {
  if (!metrics) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="card p-4 h-[84px] animate-pulse" />
        ))}
      </div>
    );
  }

  const outros = Math.max(metrics.total - metrics.furtos - metrics.roubos, 0);

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
      <MetricCard
        label="Total de ocorrências"
        value={formatInt(metrics.total)}
        delta={formatDeltaPct(metrics.yoyDeltaPct)}
        help={`Variação compara ${metrics.lastYear ?? "—"} vs ${metrics.prevYear ?? "—"}.`}
      />
      <MetricCard label="Furtos" value={formatInt(metrics.furtos)} help="Rubricas que começam com 'FURTO'." />
      <MetricCard label="Roubos" value={formatInt(metrics.roubos)} help="Rubricas que começam com 'ROUBO'." />
      <MetricCard
        label="Outros crimes"
        value={formatInt(outros)}
        delta={metrics.total ? `${((outros / metrics.total) * 100).toFixed(1)}% do total` : null}
      />
      <MetricCard
        label="Delegacias envolvidas"
        value={formatInt(metrics.delegacias)}
        help="Distintas DPs com pelo menos um BO no recorte."
      />
      <MetricCard
        label="% Geocodificado"
        value={formatPct(metrics.geocodedPct)}
        help="Percentual de BOs com latitude/longitude válidas."
      />
    </div>
  );
}
