export default function ChartCard({ title, isEmpty, emptyMessage, children }) {
  return (
    <div className="brutal-card" data-animate="chart-card">
      <p className="border-b-[3px] border-ink bg-ink text-paper text-xs font-extrabold uppercase tracking-wider px-3 py-2">
        {title}
      </p>
      {isEmpty ? (
        <div className="text-sm font-semibold text-g600 px-3 py-8">{emptyMessage}</div>
      ) : (
        <div className="p-2">{children}</div>
      )}
    </div>
  );
}
