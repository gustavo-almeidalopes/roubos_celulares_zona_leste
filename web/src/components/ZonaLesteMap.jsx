import { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts";
import ReactECharts from "echarts-for-react";
import zonaLesteGeo from "../assets/zonaLesteDistritos.json";
import { districtCardHtml } from "../lib/districtCard";
import { CloseIcon } from "./icons";

// Registra o GeoJSON dos 33 distritos da Zona Leste como um mapa ECharts (uma vez).
echarts.registerMap("zonaLeste", zonaLesteGeo);

/**
 * Preenchimento monocromático SUTIL: mais claro = mais ocorrências.
 * Mantém a leitura espacial dos hotspots sem interação (o superpoder do
 * choropleth) sem poluir o estado de repouso — que mostra só perímetro + nome.
 */
const FILL_RAMP = ["#1f1f1f", "#292929", "#343434", "#414141", "#4f4f4f", "#5e5e5e"];

/**
 * Mapa da Zona Leste com progressive disclosure:
 *
 * - Repouso: perímetro + nome (+ preenchimento sutil por volume).
 * - Hover: card de detalhe com ~150ms de delay (evita "piscar" ao cruzar o
 *   mapa), estrutura fixa (Roubos → Furtos → Total), seta + número colorido
 *   discreto para a direção e baseline temporal explícito.
 *   O card abre do lado OPOSTO ao quadrante do cursor, para não cobrir o
 *   distrito que o originou.
 * - Clique/tap: seleciona o distrito (filtra o dashboard) e FIXA o card no
 *   canto do mapa — é o caminho para touch (hover não existe) e para teclado
 *   (via seleção de distrito em "Mais filtros").
 */
export default function ZonaLesteMap({ data, selected, onSelect, lastYear, prevYear }) {
  const rows = data ?? [];
  const maxTotal = useMemo(() => Math.max(1, ...rows.map((r) => r.total)), [rows]);
  const byName = useMemo(() => new Map(rows.map((r) => [r.name, r])), [rows]);
  const selectedRow = selected ? byName.get(selected) : null;

  const option = useMemo(
    () => ({
      backgroundColor: "transparent",
      aria: { enabled: true },
      tooltip: {
        trigger: "item",
        // O delay de ~150ms é controlado FORA do ECharts (ver handlers abaixo):
        // o showDelay interno agenda um timer que sobrevive ao re-render
        // notMerge e estoura num DOM de tooltip já descartado.
        triggerOn: "none",
        transitionDuration: 0.25,
        confine: true,
        backgroundColor: "#262626",
        borderColor: "#454545",
        borderWidth: 1,
        padding: [12, 14],
        extraCssText: "border-radius: 12px; box-shadow: 0 12px 32px rgba(0,0,0,0.55);",
        // Desloca o card para o quadrante oposto ao da âncora do distrito,
        // para não cobrir o próprio distrito que o originou.
        position: (point, _params, _dom, _rect, size) => {
          const [x, y] = point;
          const [vw, vh] = size.viewSize;
          const [cw, ch] = size.contentSize;
          const pad = 34;
          const left = x < vw / 2 ? x + pad : x - cw - pad;
          const top = y < vh / 2 ? y + pad : y - ch - pad;
          return [
            Math.max(8, Math.min(left, vw - cw - 8)),
            Math.max(8, Math.min(top, vh - ch - 8)),
          ];
        },
        formatter: (p) => {
          if (p.name === selected) return ""; // já está no card fixado
          const r = byName.get(p.name);
          if (!r) return `<div class="district-card"><div class="dc-title">${p.name}</div></div>`;
          return districtCardHtml(r, { lastYear, prevYear, hint: "clique para fixar e filtrar" });
        },
      },
      visualMap: {
        // invisível: só faz o mapeamento valor → tom; a legenda visual é a
        // barra CSS no rodapé (renderização determinística).
        show: false,
        type: "continuous",
        min: 0,
        max: maxTotal,
        inRange: { color: FILL_RAMP },
      },
      series: [
        {
          type: "map",
          map: "zonaLeste",
          nameProperty: "name",
          roam: true,
          scaleLimit: { min: 1, max: 8 },
          selectedMode: false,
          layoutCenter: ["50%", "50%"],
          layoutSize: "112%",
          labelLayout: { hideOverlap: true },
          label: {
            show: true,
            color: "#e5e5e5",
            fontSize: 9,
            fontWeight: 600,
            fontFamily: "Public Sans, sans-serif",
            textBorderColor: "rgba(0,0,0,0.55)",
            textBorderWidth: 2,
          },
          itemStyle: {
            areaColor: "#242424",
            borderColor: "#5c5c5c",
            borderWidth: 1,
          },
          emphasis: {
            disabled: false,
            label: { show: true, color: "#ffffff", fontSize: 10, fontWeight: 700 },
            itemStyle: { areaColor: "#181818", borderColor: "#f5f5f5", borderWidth: 2 },
          },
          data: rows.map((r) => ({
            name: r.name,
            value: r.total,
            ...(r.name === selected
              ? {
                  itemStyle: { areaColor: "#141414", borderColor: "#ffffff", borderWidth: 2.5 },
                  label: { show: true, color: "#ffffff", fontSize: 10, fontWeight: 800 },
                }
              : {}),
          })),
        },
      ],
    }),
    [rows, byName, maxTotal, selected, lastYear, prevYear]
  );

  const chartRef = useRef(null);
  const hoverTimer = useRef(null);

  const cancelHover = () => {
    if (hoverTimer.current) {
      clearTimeout(hoverTimer.current);
      hoverTimer.current = null;
    }
  };
  const hideTip = () => {
    cancelHover();
    chartRef.current?.getEchartsInstance()?.dispatchAction({ type: "hideTip" });
  };

  // Nenhum timer de hover pode sobreviver a um re-render/unmount do chart.
  useEffect(() => cancelHover, [option]);

  const onEvents = useMemo(
    () => ({
      // Progressive disclosure com delay próprio (~150ms): mostra o card só
      // se o cursor permanecer no distrito — cruzar o mapa não pisca cards.
      mouseover: (params) => {
        if (!params?.name || params.name === selected) return; // selecionado já tem card fixado
        cancelHover();
        hoverTimer.current = setTimeout(() => {
          hoverTimer.current = null;
          chartRef.current
            ?.getEchartsInstance()
            ?.dispatchAction({ type: "showTip", seriesIndex: 0, name: params.name });
        }, 150);
      },
      mouseout: hideTip,
      globalout: hideTip,
      click: (params) => {
        if (params?.name && onSelect) {
          hideTip();
          onSelect(params.name === selected ? null : params.name);
        }
      },
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [onSelect, selected]
  );

  return (
    <div className="card overflow-hidden" data-animate="map">
      <div className="flex items-center justify-between gap-3 px-4 py-2.5 border-b border-border">
        <p className="text-[0.72rem] font-bold uppercase tracking-wider text-muted">
          Ocorrências por distrito
        </p>
        <span className="num text-[0.7rem] text-faint">
          {selected ? selected : "33 distritos"}
        </span>
      </div>

      <div className="relative">
        <ReactECharts
          ref={chartRef}
          echarts={echarts}
          option={option}
          onEvents={onEvents}
          notMerge
          lazyUpdate
          style={{ height: "100%", width: "100%" }}
          className="!h-[420px] sm:!h-[500px] lg:!h-[560px]"
        />

        {/* Card fixado do distrito selecionado (tap no touch / teclado via
            "Mais filtros → Distrito"). Fica no canto, não sobre o distrito. */}
        {selectedRow && (
          <div className="absolute top-3 right-3 z-10 max-w-[248px] rounded-xl border border-border-strong bg-raised shadow-[var(--shadow-float)] p-3 pr-9">
            <button
              type="button"
              onClick={() => onSelect?.(null)}
              className="btn-ghost btn absolute top-1.5 right-1.5 !p-1"
              aria-label={`Limpar seleção de ${selectedRow.name}`}
            >
              <CloseIcon size={12} />
            </button>
            <div
              // Mesmo HTML do tooltip (estrutura idêntica e escaneável); o
              // conteúdo vem de listas fixas do app + números agregados.
              dangerouslySetInnerHTML={{
                __html: districtCardHtml(selectedRow, {
                  lastYear,
                  prevYear,
                  hint: "filtrando o dashboard por este distrito",
                }),
              }}
            />
          </div>
        )}
      </div>

      <div className="border-t border-border px-4 py-2 flex items-center gap-3 flex-wrap">
        <span className="inline-flex items-center gap-1.5 text-[0.64rem] font-semibold text-faint flex-none">
          menos
          <span
            aria-hidden="true"
            className="inline-block w-20 h-1.5 rounded-full"
            style={{ background: `linear-gradient(90deg, ${FILL_RAMP[0]}, ${FILL_RAMP[FILL_RAMP.length - 1]})` }}
          />
          mais ocorrências
        </span>
        <span className="text-[0.64rem] font-semibold text-faint">
          · passe o mouse ou toque num distrito para detalhes · clique para fixar e filtrar ·
          Fonte: SSP-SP
        </span>
      </div>
    </div>
  );
}
