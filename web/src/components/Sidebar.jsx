import { useEffect, useState } from "react";
import { DAY_PERIODS, MONTH_LABELS } from "../lib/data";
import { ZONA_LESTE_BAIRROS } from "../lib/zonaLeste";
import {
  ChevronDownIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  CloseIcon,
  FunnelIcon,
  ResetIcon,
} from "./icons";

const CATEGORY_OPTIONS = ["Furto", "Roubo", "Outros"];

function toggleValue(list, value) {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

function Chip({ active, onClick, children, title }) {
  return (
    <button
      type="button"
      data-active={active}
      aria-pressed={active}
      onClick={onClick}
      className="chip"
      title={title}
    >
      {children}
    </button>
  );
}

function FilterGroup({ label, children }) {
  return (
    <div>
      <p className="filter-label mb-2">{label}</p>
      {children}
    </div>
  );
}

/** Total de filtros ativos (para os badges do rail/botão mobile). */
export function countActiveFilters(filters) {
  return (
    (filters.years?.length ?? 0) +
    (filters.months?.length ?? 0) +
    (filters.dayPeriods?.length ?? 0) +
    (filters.categories?.length ?? 0) +
    (filters.bairro ? 1 : 0) +
    (filters.rubrica?.trim() ? 1 : 0) +
    (filters.dp ? 1 : 0)
  );
}

/** Lista de chips "filtro ativo" com respectivo remover. */
function activeFilterChips(filters, set) {
  const chips = [];
  for (const y of filters.years ?? []) {
    chips.push({ key: `y-${y}`, label: String(y), remove: () => set({ years: filters.years.filter((v) => v !== y) }) });
  }
  for (const m of filters.months ?? []) {
    chips.push({
      key: `m-${m}`,
      label: MONTH_LABELS[m - 1],
      remove: () => set({ months: filters.months.filter((v) => v !== m) }),
    });
  }
  for (const p of filters.dayPeriods ?? []) {
    const period = DAY_PERIODS.find((d) => d.id === p);
    chips.push({
      key: `p-${p}`,
      label: period?.label ?? p,
      remove: () => set({ dayPeriods: filters.dayPeriods.filter((v) => v !== p) }),
    });
  }
  for (const c of filters.categories ?? []) {
    chips.push({ key: `c-${c}`, label: c, remove: () => set({ categories: filters.categories.filter((v) => v !== c) }) });
  }
  if (filters.bairro) chips.push({ key: "bairro", label: filters.bairro, remove: () => set({ bairro: null }) });
  if (filters.rubrica?.trim()) {
    chips.push({ key: "rubrica", label: `"${filters.rubrica.trim()}"`, remove: () => set({ rubrica: "" }) });
  }
  if (filters.dp) chips.push({ key: "dp", label: filters.dp, remove: () => set({ dp: null }) });
  return chips;
}

/**
 * Painel de filtros à esquerda.
 *
 * Progressive disclosure em dois níveis: os filtros frequentes ficam sempre
 * à vista ("Filtros básicos"); os avançados (distrito, rubrica, DP) só
 * aparecem ao abrir "Mais filtros". Chips de "Filtros ativos" + Limpar
 * evitam o clássico "por que meus números estão estranhos?".
 *
 * Desktop: coluna fixa recolhível (mais espaço pro mapa). Mobile: drawer
 * acionado pelo botão de filtros na barra superior.
 */
export default function Sidebar({
  availableYears,
  delegacias,
  filters,
  setFilters,
  onReset,
  mobileOpen,
  onMobileClose,
  collapsedDesktop = false,
  onCollapse,
}) {
  const set = (patch) => setFilters((prev) => ({ ...prev, ...patch }));
  const [moreOpen, setMoreOpen] = useState(false);

  // Fecha o drawer mobile com Escape.
  useEffect(() => {
    if (!mobileOpen) return;
    const onKey = (e) => {
      if (e.key === "Escape") onMobileClose?.();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [mobileOpen, onMobileClose]);

  const chips = activeFilterChips(filters, set);
  const extraCount = (filters.bairro ? 1 : 0) + (filters.rubrica?.trim() ? 1 : 0) + (filters.dp ? 1 : 0);

  return (
    <>
      {mobileOpen && (
        <div
          className="fixed inset-0 z-[60] bg-black/60 lg:hidden"
          onClick={onMobileClose}
          aria-hidden="true"
        />
      )}

      <aside
        aria-label="Filtros"
        data-animate="sidebar"
        className={
          "fixed inset-y-0 left-0 z-[70] w-[282px] flex flex-col bg-panel border-r border-border " +
          "transition-transform duration-200 lg:transition-none " +
          (mobileOpen ? "translate-x-0 " : "-translate-x-full ") +
          (collapsedDesktop
            ? "lg:hidden" // recolhido no desktop (o rail assume); drawer mobile segue funcionando
            : "lg:translate-x-0 lg:sticky lg:top-0 lg:z-30 lg:h-screen lg:flex-none")
        }
      >
        {/* Marca */}
        <div className="flex items-center gap-2 px-3.5 h-16 border-b border-border flex-none">
          <span className="w-7 h-7 rounded-lg bg-ink flex items-center justify-center flex-none" aria-hidden="true">
            <span className="w-2.5 h-2.5 rounded-[3px] bg-bg" />
          </span>
          <div className="min-w-0">
            <p className="text-[0.85rem] font-extrabold text-ink leading-tight truncate">
              Radar Segurança SP
            </p>
            <p className="text-[0.62rem] font-semibold text-faint uppercase tracking-widest">
              Zona Leste · SSP-SP
            </p>
          </div>
          <button
            type="button"
            onClick={onCollapse}
            className="btn-ghost btn ml-auto !p-1.5 hidden lg:inline-flex"
            title="Recolher painel de filtros"
            aria-label="Recolher painel de filtros"
          >
            <ChevronLeftIcon />
          </button>
          <button
            type="button"
            onClick={onMobileClose}
            className="btn-ghost btn ml-auto !p-1.5 lg:hidden"
            aria-label="Fechar filtros"
          >
            <CloseIcon />
          </button>
        </div>

        {/* Filtros básicos (sempre à vista) */}
        <div className="flex-1 overflow-y-auto px-4 py-4 space-y-5">
          <p className="text-[0.78rem] font-bold text-ink">Filtros básicos</p>

          <FilterGroup label="Ano">
            <div className="flex flex-wrap gap-1.5">
              {availableYears.map((year) => (
                <Chip
                  key={year}
                  active={filters.years.includes(year)}
                  onClick={() => set({ years: toggleValue(filters.years, year) })}
                >
                  {year}
                </Chip>
              ))}
            </div>
          </FilterGroup>

          <FilterGroup label="Mês">
            <div className="grid grid-cols-4 gap-1.5">
              {MONTH_LABELS.map((label, idx) => {
                const monthNum = idx + 1;
                return (
                  <Chip
                    key={monthNum}
                    active={filters.months.includes(monthNum)}
                    onClick={() => set({ months: toggleValue(filters.months, monthNum) })}
                  >
                    {label}
                  </Chip>
                );
              })}
            </div>
          </FilterGroup>

          <FilterGroup label="Tipo de ocorrência">
            <div className="flex flex-wrap gap-1.5">
              {CATEGORY_OPTIONS.map((cat) => (
                <Chip
                  key={cat}
                  active={filters.categories.includes(cat)}
                  onClick={() => set({ categories: toggleValue(filters.categories, cat) })}
                >
                  {cat}
                </Chip>
              ))}
            </div>
          </FilterGroup>

          <FilterGroup label="Período do dia">
            <div className="flex flex-wrap gap-1.5">
              {DAY_PERIODS.map((p) => (
                <Chip
                  key={p.id}
                  active={filters.dayPeriods.includes(p.id)}
                  onClick={() => set({ dayPeriods: toggleValue(filters.dayPeriods, p.id) })}
                  title={`${String(p.hours[0]).padStart(2, "0")}h–${String(p.hours[1]).padStart(2, "0")}h59`}
                >
                  {p.label}
                </Chip>
              ))}
            </div>
          </FilterGroup>

          {/* Filtros ativos: o que está aplicado, sempre visível e removível */}
          {chips.length > 0 && (
            <div className="border-t border-border pt-4">
              <div className="flex items-center justify-between mb-2">
                <p className="filter-label">Filtros ativos</p>
                <button
                  type="button"
                  onClick={() => {
                    setMoreOpen(false);
                    onReset();
                  }}
                  className="inline-flex items-center gap-1 text-[0.66rem] font-bold text-muted hover:text-ink transition-colors"
                >
                  <ResetIcon size={11} /> Limpar tudo
                </button>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {chips.map((chip) => (
                  <span key={chip.key} className="active-chip">
                    {chip.label}
                    <button type="button" onClick={chip.remove} aria-label={`Remover filtro ${chip.label}`}>
                      <CloseIcon size={10} />
                    </button>
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Mais filtros (avançados, sob demanda) */}
        <div className="flex-none border-t border-border">
          {moreOpen && (
            <div className="px-4 py-4 space-y-4 max-h-[45vh] overflow-y-auto border-b border-border">
              <FilterGroup label="Distrito">
                <select
                  className="input"
                  value={filters.bairro ?? ""}
                  onChange={(e) => set({ bairro: e.target.value || null })}
                >
                  <option value="">Todos os distritos</option>
                  {ZONA_LESTE_BAIRROS.map((b) => (
                    <option key={b} value={b}>
                      {b}
                    </option>
                  ))}
                </select>
              </FilterGroup>

              <FilterGroup label="Tipo de roubo / rubrica">
                <input
                  type="text"
                  value={filters.rubrica}
                  onChange={(e) => set({ rubrica: e.target.value })}
                  placeholder="ex: 157, roubo, estelionato…"
                  className="input"
                />
              </FilterGroup>

              <FilterGroup label="Delegacia (DP)">
                <select
                  className="input"
                  value={filters.dp ?? ""}
                  onChange={(e) => set({ dp: e.target.value || null })}
                >
                  <option value="">Todas as delegacias</option>
                  {(delegacias ?? []).map((dp) => (
                    <option key={dp} value={dp}>
                      {dp}
                    </option>
                  ))}
                </select>
              </FilterGroup>
            </div>
          )}
          <div className="p-3">
            <button
              type="button"
              onClick={() => setMoreOpen((v) => !v)}
              aria-expanded={moreOpen}
              className="btn w-full justify-between"
            >
              <span className="inline-flex items-center gap-2">
                <FunnelIcon size={13} />
                Mais filtros
                {extraCount > 0 && (
                  <span className="num inline-flex items-center justify-center min-w-[18px] h-[18px] px-1 rounded-full bg-ink text-bg text-[0.62rem]">
                    {extraCount}
                  </span>
                )}
              </span>
              <ChevronDownIcon size={13} style={{ transform: moreOpen ? "none" : "rotate(180deg)" }} />
            </button>
          </div>
        </div>
      </aside>
    </>
  );
}

/** Trilho fino exibido no desktop quando o painel está recolhido. */
export function SidebarRail({ onExpand, activeCount }) {
  return (
    <div className="hidden lg:flex sticky top-0 z-30 h-screen w-14 flex-none flex-col items-center gap-3 bg-panel border-r border-border py-4">
      <button
        type="button"
        onClick={onExpand}
        className="btn-ghost btn !p-2"
        title="Expandir painel de filtros"
        aria-label="Expandir painel de filtros"
      >
        <ChevronRightIcon />
      </button>
      <button
        type="button"
        onClick={onExpand}
        className="btn-ghost btn relative !p-2"
        title="Filtros"
        aria-label="Abrir filtros"
      >
        <FunnelIcon />
        {activeCount > 0 && (
          <span className="num absolute -top-1 -right-1 inline-flex items-center justify-center min-w-[17px] h-[17px] px-0.5 rounded-full bg-ink text-bg text-[0.6rem]">
            {activeCount}
          </span>
        )}
      </button>
    </div>
  );
}
