import { formatInt } from "../lib/format";

export default function Header({ total }) {
  return (
    <header id="app-header" className="brutal-card mb-5 bg-ink text-paper">
      <div className="flex flex-wrap items-end justify-between gap-3 px-5 py-4">
        <div>
          <h1 className="text-[1.7rem] sm:text-[2.1rem] font-black uppercase tracking-tight leading-none m-0">
            Radar <span className="bg-paper text-ink px-2">Zona Leste</span>
          </h1>
          <p className="mt-2 text-g300 text-[0.72rem] font-bold uppercase tracking-[0.12em]">
            Roubos e furtos de celulares · SSP-SP
          </p>
        </div>
        <div className="text-right">
          <div className="brutal-num text-2xl sm:text-3xl text-paper leading-none">
            {total != null ? formatInt(total) : "—"}
          </div>
          <div className="text-g400 text-[0.6rem] font-bold uppercase tracking-widest mt-1">
            ocorrências no recorte
          </div>
        </div>
      </div>
    </header>
  );
}
