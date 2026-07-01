/** Formato BR: 1.234.567 (porta de metrics.py::_fmt). */
export function formatInt(n) {
  return new Intl.NumberFormat("pt-BR").format(Math.round(n ?? 0));
}

/** Delta percentual formatado com sinal (porta de metrics.py::_delta). */
export function formatDeltaPct(pct) {
  if (pct == null) return null;
  const sign = pct >= 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}% YoY`;
}

export function formatPct(pct) {
  return `${(pct ?? 0).toFixed(1)}%`;
}
