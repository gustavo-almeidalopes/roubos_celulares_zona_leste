import Header from "./components/Header";
import FilterPanel from "./components/FilterPanel";
import MetricsRow from "./components/MetricsRow";
import TopRubricasChart from "./components/charts/TopRubricasChart";
import MonthlyTrendChart from "./components/charts/MonthlyTrendChart";
import CategoryDonutChart from "./components/charts/CategoryDonutChart";
import YoyChart from "./components/charts/YoyChart";
import { useDashboardData } from "./hooks/useDashboardData";
import { useEntranceAnimation } from "./hooks/useEntranceAnimation";

export default function App() {
  const {
    availableYears,
    filters,
    setFilters,
    resetFilters,
    status,
    error,
    metrics,
    topRubricas,
    monthlyTrend,
    categoryBreakdown,
    yoyComparison,
  } = useDashboardData();

  useEntranceAnimation(status === "ready");

  if (status === "error") {
    return (
      <div className="max-w-2xl mx-auto mt-24 px-6">
        <div className="card p-6 border-border-strong">
          <h1 className="text-lg font-bold text-ink mb-2">Não foi possível carregar os dados</h1>
          <p className="text-sm text-muted mb-3">{String(error?.message ?? error)}</p>
          <p className="text-xs text-muted">
            Verifique se <code>web/public/data/cleaned_data.parquet</code> existe (gerado pelo pipeline
            Python: <code>python -m src.data_pipeline run</code>).
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-[1440px] mx-auto px-5 pt-8 pb-12">
      <Header total={metrics?.total} />

      <div className="grid grid-cols-1 lg:grid-cols-[260px_1fr] gap-6">
        <FilterPanel
          availableYears={availableYears}
          filters={filters}
          setFilters={setFilters}
          onReset={resetFilters}
        />

        <div className="space-y-6 min-w-0">
          <MetricsRow metrics={metrics} />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <TopRubricasChart data={topRubricas} />
            <CategoryDonutChart data={categoryBreakdown} />
          </div>

          <MonthlyTrendChart data={monthlyTrend} />

          <YoyChart data={yoyComparison} />
        </div>
      </div>
    </div>
  );
}
