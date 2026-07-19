/**
 * Skeleton do layout completo para a PRIMEIRA carga (DuckDB-WASM + parquet
 * não são instantâneos). Espelha a estrutura real — sidebar, barra superior,
 * faixa de KPIs e mapa — para a primeira impressão ser "está chegando",
 * não "quebrou".
 */
export default function AppSkeleton() {
  return (
    <div className="flex min-h-screen" aria-hidden="true">
      {/* Sidebar */}
      <div className="hidden lg:flex w-[282px] flex-none flex-col bg-panel border-r border-border">
        <div className="flex items-center gap-2.5 px-4 h-16 border-b border-border">
          <div className="skeleton w-8 h-8 !rounded-lg" />
          <div className="space-y-1.5">
            <div className="skeleton h-3.5 w-36" />
            <div className="skeleton h-2 w-24" />
          </div>
        </div>
        <div className="px-4 py-4 space-y-6">
          <div className="skeleton h-3 w-24" />
          {[3, 4, 3].map((n, g) => (
            <div key={g} className="space-y-2">
              <div className="skeleton h-2.5 w-16" />
              <div className="flex gap-1.5">
                {Array.from({ length: n }).map((_, i) => (
                  <div key={i} className="skeleton h-7 w-14 !rounded-full" />
                ))}
              </div>
            </div>
          ))}
        </div>
        <div className="mt-auto p-3">
          <div className="skeleton h-9 w-full" />
        </div>
      </div>

      {/* Conteúdo */}
      <div className="flex-1 min-w-0 px-3 sm:px-5 py-4 space-y-4 max-w-[1200px] mx-auto">
        <div className="flex items-center gap-3">
          <div className="skeleton h-8 w-8 lg:hidden" />
          <div className="skeleton h-5 w-48" />
          <div className="skeleton h-9 w-44 ml-auto !rounded-xl" />
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="card px-4 py-3 space-y-2.5">
              <div className="skeleton h-2.5 w-24" />
              <div className="skeleton h-6 w-20" />
              <div className="skeleton h-2 w-28" />
            </div>
          ))}
        </div>

        <div className="card overflow-hidden">
          <div className="skeleton h-[520px] w-full !rounded-none" />
        </div>

        <p className="text-center text-[0.7rem] font-semibold text-faint" role="status" aria-hidden="false">
          Preparando o motor de dados no navegador — a primeira carga pode levar alguns segundos…
        </p>
      </div>
    </div>
  );
}
