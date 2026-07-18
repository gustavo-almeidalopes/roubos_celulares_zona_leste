import ReactECharts from "echarts-for-react";
import ChartCard from "./ChartCard";
import { MONO, baseAxis, baseGrid, baseTextStyle, baseTooltip } from "../../lib/echartsTheme";

/** Porta de charts.py::render_top_rubricas (barra horizontal). */
export default function TopRubricasChart({ data, topN = 15 }) {
  const isEmpty = !data || data.length === 0;
  const rows = [...(data ?? [])].reverse(); // ECharts categoria: primeiro item embaixo

  const option = {
    textStyle: baseTextStyle,
    tooltip: { ...baseTooltip, trigger: "axis", axisPointer: { type: "shadow" } },
    grid: baseGrid,
    xAxis: { type: "value", ...baseAxis },
    yAxis: {
      type: "category",
      data: rows.map((r) => r.RUBRICA),
      ...baseAxis,
      axisLabel: { ...baseAxis.axisLabel, width: 160, overflow: "truncate" },
    },
    series: [
      {
        type: "bar",
        data: rows.map((r) => Number(r.occurrences)),
        itemStyle: { color: MONO.ink },
        label: { show: true, position: "right", color: MONO.ink, fontSize: 11 },
      },
    ],
  };

  return (
    <ChartCard title={`Top ${topN} Tipos de Crime`} isEmpty={isEmpty} emptyMessage="Sem dados de rubrica para os filtros selecionados.">
      <ReactECharts option={option} style={{ height: 420 }} notMerge lazyUpdate />
    </ChartCard>
  );
}
