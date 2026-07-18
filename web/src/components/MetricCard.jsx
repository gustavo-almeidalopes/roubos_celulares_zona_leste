export default function MetricCard({ label, value, delta, help }) {
  return (
    <div className="brutal-card-sm p-3" data-animate="metric-card" title={help}>
      <div className="brutal-label">{label}</div>
      <div className="brutal-num text-[1.8rem] leading-none text-ink mt-2">{value}</div>
      {delta ? (
        <div className="mt-2 inline-block border-2 border-ink bg-ink text-paper text-[0.62rem] font-bold uppercase tracking-wide px-1.5 py-0.5">
          {delta}
        </div>
      ) : null}
    </div>
  );
}
