import { deltaParts, formatInt } from "../lib/format";

function KpiTile({ label, value, sub, mono = true, children }) {
  return (
    <div className="card px-4 py-3 min-w-0" data-animate="kpi">
      <p className="filter-label truncate">{label}</p>
      <div className="flex items-baseline gap-2 mt-1.5 min-w-0">
        <span
          className={
            (mono ? "num text-[1.45rem] " : "font-extrabold text-[1.15rem] ") +
            "leading-none text-ink truncate"
          }
        >
          {value}
        </span>
        {children}
      </div>
      {sub ? <p className="text-[0.64rem] font-semibold text-faint mt-1.5 truncate">{sub}</p> : null}
    </div>
  );
}

/**
 * Faixa de KPIs sempre visível (não fica atrás de toggle): total + variação
 * com baseline explícito, roubos, furtos e o distrito campeão do recorte.
 */
export default function KpiStrip({ metrics, distritoStats }) {
  const leader = distritoStats?.[0];
  const yoy = deltaParts(metrics?.yoyDeltaPct);
  const baseline =
    metrics?.lastYear != null && metrics?.prevYear != null
      ? `${metrics.lastYear} vs. ${metrics.prevYear}`
      : "sem base de comparação";

  return (
    <section className="grid grid-cols-2 lg:grid-cols-4 gap-3" aria-label="Indicadores do recorte">
      <KpiTile
        label="Ocorrências no recorte"
        value={metrics ? formatInt(metrics.total) : "—"}
        sub={`variação: ${baseline}`}
      >
        <span className="delta num text-[0.78rem] flex-none" data-dir={yoy.dir}>
          {yoy.arrow ? `${yoy.arrow} ` : ""}
          {yoy.text}
        </span>
      </KpiTile>
      <KpiTile
        label="Roubos"
        value={metrics ? formatInt(metrics.roubos) : "—"}
        sub={
          metrics?.total
            ? `${((metrics.roubos / metrics.total) * 100).toFixed(0)}% do total`
            : null
        }
      />
      <KpiTile
        label="Furtos"
        value={metrics ? formatInt(metrics.furtos) : "—"}
        sub={
          metrics?.total
            ? `${((metrics.furtos / metrics.total) * 100).toFixed(0)}% do total`
            : null
        }
      />
      <KpiTile
        label="Distrito líder"
        mono={false}
        value={leader && leader.total > 0 ? leader.name : "—"}
        sub={leader && leader.total > 0 ? `${formatInt(leader.total)} ocorrências` : null}
      />
    </section>
  );
}
