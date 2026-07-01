import { MONTH_LABELS } from "../lib/data";

const CATEGORY_OPTIONS = ["Furto", "Roubo", "Outros"];

function toggleValue(list, value) {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

function Pill({ active, onClick, children }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={
        "px-3 py-1.5 rounded-full text-xs font-semibold border transition-colors " +
        (active
          ? "bg-ink text-white border-ink"
          : "bg-surface text-text border-border-strong hover:bg-hover")
      }
    >
      {children}
    </button>
  );
}

/** Porta simplificada (MVP) de filters.py::render_sidebar_filters para React. */
export default function FilterPanel({ availableYears, filters, setFilters, onReset }) {
  const set = (patch) => setFilters((prev) => ({ ...prev, ...patch }));

  return (
    <aside className="card p-5 space-y-6 lg:sticky lg:top-6 h-fit">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-bold text-ink uppercase tracking-wide">Filtros</h2>
        <button
          type="button"
          onClick={onReset}
          className="text-xs font-semibold text-muted hover:text-ink transition-colors"
        >
          ↻ Limpar
        </button>
      </div>

      <div>
        <p className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">Ano</p>
        <div className="flex flex-wrap gap-2">
          {availableYears.map((year) => (
            <Pill
              key={year}
              active={filters.years.includes(year)}
              onClick={() => set({ years: toggleValue(filters.years, year) })}
            >
              {year}
            </Pill>
          ))}
        </div>
      </div>

      <div>
        <p className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">Mês</p>
        <div className="flex flex-wrap gap-2">
          {MONTH_LABELS.map((label, idx) => {
            const monthNum = idx + 1;
            return (
              <Pill
                key={monthNum}
                active={filters.months.includes(monthNum)}
                onClick={() => set({ months: toggleValue(filters.months, monthNum) })}
              >
                {label}
              </Pill>
            );
          })}
        </div>
      </div>

      <div>
        <p className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">Categoria</p>
        <div className="flex flex-wrap gap-2">
          {CATEGORY_OPTIONS.map((cat) => (
            <Pill
              key={cat}
              active={filters.categories.includes(cat)}
              onClick={() => set({ categories: toggleValue(filters.categories, cat) })}
            >
              {cat}
            </Pill>
          ))}
        </div>
      </div>

      <div>
        <p className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">Busca livre</p>
        <input
          type="text"
          value={filters.search}
          onChange={(e) => set({ search: e.target.value })}
          placeholder="ex: veiculo, celular…"
          className="w-full rounded-lg border border-border-strong bg-surface px-3 py-2 text-sm text-text focus:outline-none focus:ring-2 focus:ring-ink"
        />
      </div>
    </aside>
  );
}
