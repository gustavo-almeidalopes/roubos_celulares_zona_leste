import MetricCard from "./MetricCard";
import { formatDeltaPct, formatInt } from "../lib/format";

/** Cards de KPI (escopados à Zona Leste). */
export default function MetricsRow({ metrics }) {
  if (!metrics) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="brutal-card-sm h-[92px] animate-pulse" />
        ))}
      </div>
    );
  }

  const outros = Math.max(metrics.total - metrics.furtos - metrics.roubos, 0);

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
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
        label="Distritos"
        value={formatInt(metrics.distritos)}
        help="Distritos da Zona Leste com pelo menos um BO no recorte."
      />
    </div>
  );
}
