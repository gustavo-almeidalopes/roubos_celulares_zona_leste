/** Formato BR: 1.234.567 (porta de metrics.py::_fmt). */
export function formatInt(n) {
  return new Intl.NumberFormat("pt-BR").format(Math.round(n ?? 0));
}

export function formatPct(pct) {
  return `${(pct ?? 0).toFixed(1)}%`;
}

/**
 * Variação percentual como { dir, arrow, text } — a DIREÇÃO vai na seta
 * (funciona em P&B e para daltônicos); a cor fica só no número, via
 * `.delta[data-dir]` / `.dc-delta[data-dir]` no CSS.
 *   dir: "up" (piora), "down" (melhora), "flat" (0% ou sem base).
 */
export function deltaParts(pct) {
  if (pct == null || Number.isNaN(pct)) return { dir: "flat", arrow: "", text: "—" };
  const rounded = Math.abs(pct) < 0.05 ? 0 : pct;
  if (rounded === 0) return { dir: "flat", arrow: "", text: "0%" };
  return {
    dir: rounded > 0 ? "up" : "down",
    arrow: rounded > 0 ? "↑" : "↓",
    text: `${Math.abs(rounded).toFixed(0)}%`,
  };
}
