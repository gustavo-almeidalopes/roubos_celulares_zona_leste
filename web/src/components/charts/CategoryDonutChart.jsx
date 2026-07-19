import ReactECharts from "echarts-for-react";
import ChartCard from "./ChartCard";
import { MONO, baseTextStyle, baseTooltip } from "../../lib/echartsTheme";

const CATEGORY_COLOR = { Furto: MONO.ink, Roubo: MONO.soft, Outros: MONO.faint };

/** Porta de charts.py::render_crime_category_pie (donut). */
export default function CategoryDonutChart({ data }) {
  const isEmpty = !data || data.length === 0;

  const option = {
    textStyle: baseTextStyle,
    tooltip: { ...baseTooltip, trigger: "item", formatter: "{b}: {c} ({d}%)" },
    series: [
      {
        type: "pie",
        radius: ["58%", "82%"],
        avoidLabelOverlap: true,
        itemStyle: { borderColor: "#1e1e1e", borderWidth: 2 },
        label: { formatter: "{b}\n{d}%", color: MONO.ink, fontSize: 12, fontWeight: 600 },
        data: (data ?? []).map((r) => ({
          name: r.category,
          value: Number(r.occurrences),
          itemStyle: { color: CATEGORY_COLOR[r.category] ?? MONO.faint },
        })),
      },
    ],
  };

  return (
    <ChartCard title="Categoria de Crime" isEmpty={isEmpty} emptyMessage="Sem dados para os filtros selecionados.">
      <ReactECharts option={option} style={{ height: 380 }} notMerge lazyUpdate />
    </ChartCard>
  );
}
