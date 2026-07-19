/** Paleta e helpers de estilo ECharts — tema monocromático escuro. */
export const MONO = {
  ink: "#f0f0f0", // primeiro plano (barras/linhas principais)
  paper: "#1e1e1e", // superfície dos cards
  muted: "#b3b3b3",
  soft: "#8c8c8c",
  faint: "#5c5c5c",
  line: "#f0f0f0",
};

/* Séries em escala de cinza — vizinhos com bom afastamento de luminância. */
export const MONO_SEQ = ["#f0f0f0", "#9e9e9e", "#616161", "#cfcfcf", "#7d7d7d", "#4a4a4a"];

const MONO_FONT = "Space Mono, monospace";
const SANS_FONT = "Public Sans, sans-serif";

export const baseAxis = {
  axisLine: { lineStyle: { color: "#454545", width: 1 } },
  axisTick: { lineStyle: { color: "#454545" } },
  axisLabel: { color: "#c9c9c9", fontFamily: SANS_FONT, fontSize: 11, fontWeight: 500 },
  splitLine: { lineStyle: { color: "#2a2a2a" } },
};

export const baseGrid = { left: 8, right: 16, top: 28, bottom: 8, containLabel: true };

export const baseTextStyle = { fontFamily: SANS_FONT, color: "#e5e5e5" };

export const baseLegend = {
  top: 0,
  right: 0,
  textStyle: { color: "#c9c9c9", fontSize: 11 },
  inactiveColor: "#5c5c5c",
};

export const baseTooltip = {
  backgroundColor: "#262626",
  borderColor: "#454545",
  borderWidth: 1,
  padding: [8, 12],
  textStyle: { color: "#f0f0f0", fontFamily: MONO_FONT, fontSize: 12 },
  extraCssText: "border-radius: 10px; box-shadow: 0 12px 32px rgba(0,0,0,0.55);",
};
