/** Paleta e helpers de estilo ECharts — tema brutalista em escala de cinza. */
export const MONO = {
  ink: "#0a0a0a",
  paper: "#ffffff",
  muted: "#404040",
  soft: "#737373",
  faint: "#a3a3a3",
  line: "#0a0a0a",
};

export const MONO_SEQ = ["#0a0a0a", "#525252", "#a3a3a3", "#262626", "#737373", "#d4d4d4"];

const MONO_FONT = "Space Mono, monospace";
const SANS_FONT = "Public Sans, sans-serif";

export const baseAxis = {
  axisLine: { lineStyle: { color: MONO.ink, width: 2 } },
  axisTick: { lineStyle: { color: MONO.ink } },
  axisLabel: { color: MONO.ink, fontFamily: SANS_FONT, fontSize: 11, fontWeight: 600 },
  splitLine: { lineStyle: { color: "#e5e5e5" } },
};

export const baseGrid = { left: 8, right: 16, top: 24, bottom: 8, containLabel: true };

export const baseTextStyle = { fontFamily: SANS_FONT, color: MONO.ink };

export const baseTooltip = {
  backgroundColor: "#0a0a0a",
  borderColor: "#0a0a0a",
  borderWidth: 0,
  padding: [8, 12],
  textStyle: { color: "#ffffff", fontFamily: MONO_FONT, fontSize: 12 },
};
