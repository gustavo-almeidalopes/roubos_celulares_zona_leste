/** Paleta e helpers de estilo ECharts — porta de components/charts.py (MONO/MONO_SEQ). */
export const MONO = {
  ink: "#0a0a0a",
  paper: "#ffffff",
  muted: "#6b7280",
  soft: "#9ca3af",
  faint: "#d4d4d4",
  line: "#e5e7eb",
};

export const MONO_SEQ = ["#0a0a0a", "#737373", "#a3a3a3", "#404040", "#c4c4c4", "#525252"];

export const baseAxis = {
  axisLine: { lineStyle: { color: MONO.line } },
  axisTick: { lineStyle: { color: MONO.line } },
  axisLabel: { color: MONO.muted, fontFamily: "Public Sans, sans-serif", fontSize: 11 },
  splitLine: { lineStyle: { color: MONO.line } },
};

export const baseGrid = { left: 8, right: 16, top: 36, bottom: 8, containLabel: true };

export const baseTextStyle = { fontFamily: "Public Sans, sans-serif", color: MONO.ink };

export const baseTooltip = {
  backgroundColor: "#ffffff",
  borderColor: MONO.line,
  borderWidth: 1,
  textStyle: { color: MONO.ink, fontFamily: "Public Sans, sans-serif", fontSize: 12 },
};
