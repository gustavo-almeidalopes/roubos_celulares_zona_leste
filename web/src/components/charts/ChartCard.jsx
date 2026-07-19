export default function ChartCard({ title, isEmpty, emptyMessage, children }) {
  return (
    <div className="card overflow-hidden" data-animate="chart-card">
      <p className="px-4 py-2.5 border-b border-border text-[0.72rem] font-bold uppercase tracking-wider text-muted">
        {title}
      </p>
      {isEmpty ? (
        <div className="text-sm font-medium text-faint px-4 py-10">{emptyMessage}</div>
      ) : (
        <div className="p-2">{children}</div>
      )}
    </div>
  );
}
