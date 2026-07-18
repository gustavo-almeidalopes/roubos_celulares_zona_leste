import { useMemo } from "react";
import * as echarts from "echarts";
import ReactECharts from "echarts-for-react";
import zonaLesteGeo from "../assets/zonaLesteDistritos.json";
import { formatInt } from "../lib/format";

// Registra o GeoJSON dos 33 distritos da Zona Leste como um mapa ECharts (uma vez).
echarts.registerMap("zonaLeste", zonaLesteGeo);

const GRAY_RAMP = ["#f5f5f5", "#d4d4d4", "#a3a3a3", "#737373", "#404040", "#0a0a0a"];

/**
 * Mapa coroplético brutalista (escala de cinza) dos distritos da Zona Leste.
 * Clique num distrito seleciona/filtra por ele; `selected` o realça.
 */
export default function ZonaLesteMap({ data, selected, onSelect }) {
  const rows = data ?? [];
  const maxTotal = useMemo(() => Math.max(1, ...rows.map((r) => r.total)), [rows]);
  const byName = useMemo(() => new Map(rows.map((r) => [r.name, r])), [rows]);

  const option = useMemo(
    () => ({
      backgroundColor: "transparent",
      tooltip: {
        trigger: "item",
        backgroundColor: "#0a0a0a",
        borderColor: "#0a0a0a",
        borderWidth: 0,
        padding: [8, 12],
        textStyle: { color: "#fff", fontFamily: "Public Sans, sans-serif", fontSize: 12 },
        formatter: (p) => {
          const r = byName.get(p.name);
          if (!r) return `<b>${p.name}</b>`;
          return [
            `<b style="font-size:13px">${p.name.toUpperCase()}</b>`,
            `TOTAL &nbsp;<b>${formatInt(r.total)}</b>`,
            `FURTOS &nbsp;${formatInt(r.furtos)}`,
            `ROUBOS &nbsp;${formatInt(r.roubos)}`,
          ].join("<br/>");
        },
      },
      visualMap: {
        type: "continuous",
        min: 0,
        max: maxTotal,
        calculable: false,
        orient: "horizontal",
        left: "left",
        bottom: 6,
        itemWidth: 14,
        itemHeight: 90,
        text: ["+ ROUBOS", "0"],
        textStyle: { color: "#0a0a0a", fontWeight: 700, fontSize: 10, fontFamily: "Space Mono, monospace" },
        inRange: { color: GRAY_RAMP },
      },
      series: [
        {
          type: "map",
          map: "zonaLeste",
          nameProperty: "name",
          roam: true,
          scaleLimit: { min: 1, max: 8 },
          selectedMode: false,
          label: {
            show: true,
            color: "#0a0a0a",
            fontSize: 8,
            fontWeight: 700,
            fontFamily: "Public Sans, sans-serif",
          },
          itemStyle: {
            areaColor: "#e5e5e5",
            borderColor: "#0a0a0a",
            borderWidth: 1.4,
          },
          emphasis: {
            disabled: false,
            label: { show: true, color: "#fff", fontSize: 10, fontWeight: 800 },
            itemStyle: { areaColor: "#0a0a0a", borderColor: "#0a0a0a", borderWidth: 2.5 },
          },
          data: rows.map((r) => ({
            name: r.name,
            value: r.total,
            ...(r.name === selected
              ? {
                  itemStyle: { areaColor: "#0a0a0a", borderColor: "#0a0a0a", borderWidth: 3 },
                  label: { show: true, color: "#fff", fontSize: 10, fontWeight: 900 },
                }
              : {}),
          })),
        },
      ],
    }),
    [rows, byName, maxTotal, selected]
  );

  const onEvents = useMemo(
    () => ({
      click: (params) => {
        if (params?.name && onSelect) {
          onSelect(params.name === selected ? null : params.name);
        }
      },
    }),
    [onSelect, selected]
  );

  return (
    <div className="brutal-card p-0" data-animate="map">
      <div className="flex items-center justify-between border-b-[3px] border-ink px-4 py-2 bg-ink">
        <p className="text-paper font-extrabold uppercase tracking-wider text-sm">
          Mapa · Zona Leste
        </p>
        <span className="brutal-num text-paper text-xs">
          {selected ? selected.toUpperCase() : "33 DISTRITOS"}
        </span>
      </div>
      <ReactECharts
        echarts={echarts}
        option={option}
        onEvents={onEvents}
        notMerge
        lazyUpdate
        style={{ height: 560, width: "100%" }}
      />
      <div className="border-t-[3px] border-ink px-4 py-2 text-[0.68rem] font-semibold text-g600 uppercase tracking-wide">
        Clique num distrito para filtrar · tom mais escuro = mais ocorrências · Fonte SSP-SP
      </div>
    </div>
  );
}
