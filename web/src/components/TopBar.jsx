import { ChartIcon, FunnelIcon, MapIcon } from "./icons";

/**
 * Barra superior do conteúdo: contexto do recorte à esquerda, alternador
 * Mapa/Gráficos à direita. Os KPIs NÃO ficam atrás de botão — vivem na
 * faixa sempre visível logo abaixo (KpiStrip); o toggle só troca a visão
 * principal.
 */
export default function TopBar({ view, onViewChange, onOpenFilters, activeCount }) {
  return (
    <header className="flex items-center gap-3 flex-wrap" data-animate="topbar">
      <button
        type="button"
        onClick={onOpenFilters}
        className="btn relative lg:hidden !px-2.5"
        aria-label="Abrir filtros"
      >
        <FunnelIcon />
        {activeCount > 0 && (
          <span className="num absolute -top-1.5 -right-1.5 inline-flex items-center justify-center min-w-[17px] h-[17px] px-0.5 rounded-full bg-ink text-bg text-[0.6rem]">
            {activeCount}
          </span>
        )}
      </button>

      <h1 className="text-[0.95rem] font-bold text-ink">
        Zona Leste <span className="text-faint font-semibold">· 33 distritos</span>
      </h1>

      <div
        className="ml-auto inline-flex rounded-xl border border-border-strong p-0.5 bg-panel"
        role="tablist"
        aria-label="Visão principal"
      >
        <button
          type="button"
          role="tab"
          aria-selected={view === "map"}
          data-active={view === "map"}
          onClick={() => onViewChange("map")}
          className="btn !border-0 !py-1.5 !px-3.5 !text-[0.74rem]"
        >
          <MapIcon size={13} /> Mapa
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={view === "charts"}
          data-active={view === "charts"}
          onClick={() => onViewChange("charts")}
          className="btn !border-0 !py-1.5 !px-3.5 !text-[0.74rem]"
        >
          <ChartIcon size={13} /> Gráficos
        </button>
      </div>
    </header>
  );
}
