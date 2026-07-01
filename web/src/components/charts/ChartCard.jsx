export default function ChartCard({ title, isEmpty, emptyMessage, children }) {
  return (
    <div className="card p-3 pt-3" data-animate="chart-card">
      <p className="text-sm font-bold text-ink px-2 pt-1 pb-2">{title}</p>
      {isEmpty ? (
        <div className="text-sm text-muted px-2 pb-6">{emptyMessage}</div>
      ) : (
        children
      )}
    </div>
  );
}
