import { useState } from "react";
import Header from "./components/Header";
import MainFilters from "./components/MainFilters";
import MoreFilters from "./components/MoreFilters";
import MetricsRow from "./components/MetricsRow";
import ZonaLesteMap from "./components/ZonaLesteMap";
import TopRubricasChart from "./components/charts/TopRubricasChart";
import MonthlyTrendChart from "./components/charts/MonthlyTrendChart";
import CategoryDonutChart from "./components/charts/CategoryDonutChart";
import YoyChart from "./components/charts/YoyChart";
import { useDashboardData } from "./hooks/useDashboardData";
import { useEntranceAnimation } from "./hooks/useEntranceAnimation";

export default function App() {
  const {
    availableYears,
    delegacias,
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
    distritoTotals,
  } = useDashboardData();

  const [moreOpen, setMoreOpen] = useState(false);

  useEntranceAnimation(status === "ready");

  if (status === "error") {
    return (
      <div className="max-w-2xl mx-auto mt-24 px-4">
        <div className="brutal-card p-6">
          <h1 className="text-lg font-black uppercase text-ink mb-2">Não foi possível carregar os dados</h1>
          <p className="text-sm font-semibold text-g700 mb-3">{String(error?.message ?? error)}</p>
          <p className="text-xs font-semibold text-g600">
            Verifique se <code className="bg-g200 px-1">web/public/data/cleaned_data.parquet</code> existe
            (gerado pelo pipeline Python: <code className="bg-g200 px-1">python -m src.data_pipeline run</code>).
          </p>
        </div>
      </div>
    );
  }

  const setBairro = (name) => setFilters((prev) => ({ ...prev, bairro: name }));

  return (
    <div className="max-w-[1440px] mx-auto px-3 pt-5 pb-24">
      <Header total={metrics?.total} />

      <section className="relative mb-5">
        <MainFilters
          availableYears={availableYears}
          filters={filters}
          setFilters={setFilters}
          onReset={resetFilters}
        />
        <ZonaLesteMap data={distritoTotals} selected={filters.bairro} onSelect={setBairro} />
      </section>

      <div className="mb-5">
        <MetricsRow metrics={metrics} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        <TopRubricasChart data={topRubricas} />
        <CategoryDonutChart data={categoryBreakdown} />
      </div>

      <div className="mb-4">
        <MonthlyTrendChart data={monthlyTrend} />
      </div>

      <YoyChart data={yoyComparison} />

      <MoreFilters
        open={moreOpen}
        onToggle={() => setMoreOpen((v) => !v)}
        filters={filters}
        setFilters={setFilters}
        delegacias={delegacias}
      />
    </div>
  );
}
