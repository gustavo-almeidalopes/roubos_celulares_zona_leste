import ReactECharts from "echarts-for-react";
import ChartCard from "./ChartCard";
import { MONO_SEQ, baseAxis, baseGrid, baseLegend, baseTextStyle, baseTooltip } from "../../lib/echartsTheme";
import { MONTH_LABELS } from "../../lib/data";

const DASHES = ["solid", "dashed", "dotted"];

/** Porta de charts.py::render_monthly_trend (linhas multi-ano). */
export default function MonthlyTrendChart({ data }) {
  const isEmpty = !data || data.length === 0;

  const years = [...new Set((data ?? []).map((r) => Number(r.ano)))].sort((a, b) => a - b);
  const byYear = new Map(years.map((y) => [y, Array(12).fill(null)]));
  for (const row of data ?? []) {
    const y = Number(row.ano);
    const m = Number(row.mes);
    if (byYear.has(y) && m >= 1 && m <= 12) {
      byYear.get(y)[m - 1] = Number(row.occurrences);
    }
  }

  const option = {
    textStyle: baseTextStyle,
    tooltip: { ...baseTooltip, trigger: "axis" },
    legend: baseLegend,
    grid: baseGrid,
    xAxis: { type: "category", data: MONTH_LABELS, ...baseAxis },
    yAxis: { type: "value", ...baseAxis },
    series: years.map((year, i) => ({
      name: String(year),
      type: "line",
      data: byYear.get(year),
      symbol: "circle",
      symbolSize: 7,
      lineStyle: { width: 2.5, type: DASHES[i % DASHES.length], color: MONO_SEQ[i % MONO_SEQ.length] },
      itemStyle: { color: MONO_SEQ[i % MONO_SEQ.length] },
    })),
  };

  return (
    <ChartCard title="Tendência mensal por ano" isEmpty={isEmpty} emptyMessage="Sem dados temporais para os filtros selecionados.">
      <ReactECharts option={option} style={{ height: 380 }} notMerge lazyUpdate />
    </ChartCard>
  );
}
