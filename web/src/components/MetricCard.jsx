export default function MetricCard({ label, value, delta, help }) {
  return (
    <div
      className="card p-4"
      data-animate="metric-card"
      title={help}
    >
      <div className="text-[0.68rem] uppercase tracking-[0.05em] font-semibold text-muted">
        {label}
      </div>
      <div className="text-[1.7rem] font-bold text-ink mt-1">{value}</div>
      {delta ? <div className="text-xs text-muted mt-1">{delta}</div> : null}
    </div>
  );
}
