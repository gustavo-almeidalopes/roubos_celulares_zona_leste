import { deltaParts, formatInt } from "./format";

/**
 * HTML do card de detalhe de um distrito — usado tanto pelo tooltip de hover
 * do ECharts quanto pelo card fixado (tap/teclado), garantindo estrutura
 * idêntica e escaneável nos dois: ordem FIXA das métricas
 * (Roubos → Furtos → Total), seta indicando direção e baseline explícito.
 *
 * Seguro contra HTML arbitrário: nomes/valores vêm de listas fixas do app
 * (ZONA_LESTE_LABELS) e de agregações numéricas.
 */
export function districtCardHtml(row, { lastYear, prevYear, hint } = {}) {
  if (!row) return "";

  const baseline =
    lastYear != null && prevYear != null
      ? `variação: ${lastYear} vs. ${prevYear}`
      : "sem base de comparação";

  const metricRow = (label, value, pct) => {
    const d = deltaParts(pct);
    const arrow = d.arrow ? `${d.arrow}&nbsp;` : "";
    return (
      `<div class="dc-row">` +
      `<span class="dc-metric">${label}</span>` +
      `<span class="dc-value">${formatInt(value)}</span>` +
      `<span class="dc-delta" data-dir="${d.dir}">${arrow}${d.text}</span>` +
      `</div>`
    );
  };

  return (
    `<div class="district-card">` +
    `<div class="dc-title">${row.name}</div>` +
    `<div class="dc-baseline">${baseline}</div>` +
    metricRow("Roubos", row.roubos, row.deltas?.roubos) +
    metricRow("Furtos", row.furtos, row.deltas?.furtos) +
    metricRow("Total", row.total, row.deltas?.total) +
    (hint ? `<div class="dc-hint">${hint}</div>` : "") +
    `</div>`
  );
}
