import { formatInt } from "../lib/format";

export default function Header({ total }) {
  return (
    <header id="app-header" className="border-b border-border pb-5 mb-6">
      <h1 className="flex items-center gap-2 flex-wrap text-[1.9rem] font-bold tracking-tight text-ink m-0">
        Radar Segurança SP
        <span className="text-[0.62rem] tracking-[0.08em] uppercase bg-ink text-white px-[9px] py-[3px] rounded-full font-bold">
          Capital
        </span>
      </h1>
      <p className="mt-2 text-muted text-[0.85rem]">
        Fonte: SSP-SP ·{" "}
        {total != null ? `${formatInt(total)} ocorrências no município de São Paulo` : "carregando…"}{" "}
        · DuckDB-WASM + React
      </p>
    </header>
  );
}
