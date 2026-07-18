import { MONTH_LABELS } from "../lib/data";
import { ZONA_LESTE_BAIRROS } from "../lib/zonaLeste";

function toggleValue(list, value) {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

function MiniPill({ active, onClick, children }) {
  return (
    <button type="button" data-active={active} onClick={onClick} className="brutal-btn text-[0.66rem] px-2 py-1">
      {children}
    </button>
  );
}

/**
 * Filtros principais — data (ano/mês) + distrito. Fica flutuando no canto
 * superior esquerdo, sobre o mapa (estático empilhado no mobile).
 */
export default function MainFilters({ availableYears, filters, setFilters, onReset }) {
  const set = (patch) => setFilters((prev) => ({ ...prev, ...patch }));

  return (
    <aside className="brutal-card bg-paper w-full lg:w-[264px] lg:absolute lg:top-4 lg:left-4 lg:z-[500]">
      <div className="flex items-center justify-between border-b-[3px] border-ink bg-ink px-3 py-2">
        <span className="text-paper font-extrabold uppercase tracking-wider text-xs">Filtros</span>
        <button
          type="button"
          onClick={onReset}
          className="text-g300 hover:text-paper text-[0.62rem] font-bold uppercase tracking-wide"
        >
          ↻ Limpar
        </button>
      </div>

      <div className="p-3 space-y-4">
        <div>
          <p className="brutal-label mb-1.5">Ano</p>
          <div className="flex flex-wrap gap-1.5">
            {availableYears.map((year) => (
              <MiniPill
                key={year}
                active={filters.years.includes(year)}
                onClick={() => set({ years: toggleValue(filters.years, year) })}
              >
                {year}
              </MiniPill>
            ))}
          </div>
        </div>

        <div>
          <p className="brutal-label mb-1.5">Mês</p>
          <div className="grid grid-cols-6 gap-1.5">
            {MONTH_LABELS.map((label, idx) => {
              const monthNum = idx + 1;
              return (
                <MiniPill
                  key={monthNum}
                  active={filters.months.includes(monthNum)}
                  onClick={() => set({ months: toggleValue(filters.months, monthNum) })}
                >
                  {label}
                </MiniPill>
              );
            })}
          </div>
        </div>

        <div>
          <p className="brutal-label mb-1.5">Bairro / Distrito</p>
          <select
            className="brutal-input text-sm"
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
        </div>
      </div>
    </aside>
  );
}
