import ReactECharts from "echarts-for-react";
import ChartCard from "./ChartCard";
import { MONO, baseAxis, baseGrid, baseTextStyle, baseTooltip } from "../../lib/echartsTheme";

/** Porta de charts.py::render_yoy_comparison (barras agrupadas). Roubo usa "decal" (hachura) em vez de cor para diferenciar em P&B. */
export default function YoyChart({ data }) {
  const isEmpty = !data || data.length === 0;
  const years = (data ?? []).map((r) => String(r.ano));

  const option = {
    textStyle: baseTextStyle,
    tooltip: { ...baseTooltip, trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { top: 0, right: 0, textStyle: { color: "#1a1a1a", fontSize: 11 } },
    grid: baseGrid,
    xAxis: { type: "category", data: years, ...baseAxis },
    yAxis: { type: "value", ...baseAxis },
    series: [
      {
        name: "Furto",
        type: "bar",
        data: (data ?? []).map((r) => Number(r.furtos)),
        itemStyle: { color: MONO.ink },
      },
      {
        name: "Roubo",
        type: "bar",
        data: (data ?? []).map((r) => Number(r.roubos)),
        itemStyle: {
          color: MONO.soft,
          decal: { symbol: "line", dashArrayX: [1, 0], dashArrayY: [4, 4], rotation: Math.PI / 4, color: MONO.ink },
        },
      },
    ],
  };

  return (
    <ChartCard title="Furtos vs Roubos por Ano" isEmpty={isEmpty} emptyMessage="Sem dados anuais para os filtros selecionados.">
      <ReactECharts option={option} style={{ height: 380 }} notMerge lazyUpdate />
    </ChartCard>
  );
}
