const CATEGORY_OPTIONS = ["Furto", "Roubo", "Outros"];

function toggleValue(list, value) {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

function Pill({ active, onClick, children }) {
  return (
    <button type="button" data-active={active} onClick={onClick} className="brutal-btn text-xs px-3 py-1.5">
      {children}
    </button>
  );
}

/** Conta filtros extras ativos (categoria/tipo/dp), para o badge no botão. */
export function countExtraFilters(filters) {
  let n = 0;
  if (filters.categories?.length) n += filters.categories.length;
  if (filters.rubrica?.trim()) n += 1;
  if (filters.dp) n += 1;
  return n;
}

/**
 * Barra fixa na base da página com o botão "+ FILTROS"; abre um painel para
 * cima com filtros extras: categoria, tipo de roubo (rubrica) e delegacia (DP).
 */
export default function MoreFilters({ open, onToggle, filters, setFilters, delegacias }) {
  const set = (patch) => setFilters((prev) => ({ ...prev, ...patch }));
  const count = countExtraFilters(filters);

  return (
    <div className="fixed inset-x-0 bottom-0 z-[600]">
      {/* Painel que sobe */}
      <div
        className={
          "mx-auto max-w-[1440px] overflow-hidden transition-[max-height,opacity] duration-200 ease-out " +
          (open ? "max-h-[520px] opacity-100" : "max-h-0 opacity-0")
        }
      >
        <div className="brutal-card bg-paper mx-2 mb-2">
          <div className="border-b-[3px] border-ink bg-ink px-4 py-2 flex items-center justify-between">
            <span className="text-paper font-extrabold uppercase tracking-wider text-xs">Mais filtros</span>
            <button
              type="button"
              onClick={() => set({ categories: [], rubrica: "", dp: null })}
              className="text-g300 hover:text-paper text-[0.62rem] font-bold uppercase tracking-wide"
            >
              ↻ Limpar extras
            </button>
          </div>

          <div className="p-4 grid grid-cols-1 md:grid-cols-3 gap-5">
            <div>
              <p className="brutal-label mb-2">Categoria</p>
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
              <p className="brutal-label mb-2">Tipo de roubo / rubrica</p>
              <input
                type="text"
                value={filters.rubrica}
                onChange={(e) => set({ rubrica: e.target.value })}
                placeholder="ex: celular, veículo, transeunte…"
                className="brutal-input text-sm"
              />
            </div>

            <div>
              <p className="brutal-label mb-2">Delegacia (DP)</p>
              <select
                className="brutal-input text-sm"
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
            </div>
          </div>
        </div>
      </div>

      {/* Barra fixa */}
      <div className="bg-ink border-t-[3px] border-ink px-3 py-2 flex items-center justify-center gap-3">
        <button type="button" onClick={onToggle} className="brutal-btn bg-paper text-sm px-4 py-2">
          {open ? "− Fechar filtros" : "+ Filtros"}
          {count > 0 && (
            <span className="ml-1 inline-flex items-center justify-center min-w-[20px] h-[20px] px-1 bg-ink text-paper text-[0.65rem] font-black border-2 border-paper">
              {count}
            </span>
          )}
        </button>
      </div>
    </div>
  );
}
