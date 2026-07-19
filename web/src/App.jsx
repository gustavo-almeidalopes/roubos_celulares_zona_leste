import { useState } from "react";
import AppSkeleton from "./components/AppSkeleton";
import KpiStrip from "./components/KpiStrip";
import Sidebar, { SidebarRail, countActiveFilters } from "./components/Sidebar";
import TopBar from "./components/TopBar";
import ZonaLesteMap from "./components/ZonaLesteMap";
import CategoryDonutChart from "./components/charts/CategoryDonutChart";
import MonthlyTrendChart from "./components/charts/MonthlyTrendChart";
import TopRubricasChart from "./components/charts/TopRubricasChart";
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
    distritoStats,
    lastYear,
    prevYear,
  } = useDashboardData();

  const [view, setView] = useState("map"); // map | charts
  const [drawerOpen, setDrawerOpen] = useState(false); // filtros no mobile
  const [collapsed, setCollapsed] = useState(false); // painel recolhido no desktop

  useEntranceAnimation(status === "ready");

  if (status === "error") {
    return (
      <div className="max-w-2xl mx-auto mt-24 px-4">
        <div className="card p-6">
          <h1 className="text-lg font-extrabold text-ink mb-2">Não foi possível carregar os dados</h1>
          <p className="text-sm font-medium text-muted mb-3">{String(error?.message ?? error)}</p>
          <p className="text-xs font-medium text-faint">
            Verifique se <code className="bg-raised px-1 rounded">web/public/data/cleaned_data.parquet</code> existe
            (gerado pelo pipeline Python: <code className="bg-raised px-1 rounded">python -m src.data_pipeline run</code>).
          </p>
        </div>
      </div>
    );
  }

  // Primeira carga (DuckDB-WASM + parquet): skeleton do layout, não spinner.
  if (metrics == null) {
    return <AppSkeleton />;
  }

  const setBairro = (name) => setFilters((prev) => ({ ...prev, bairro: name }));
  const activeCount = countActiveFilters(filters);

  return (
    <div className="flex min-h-screen">
      <Sidebar
        availableYears={availableYears}
        delegacias={delegacias}
        filters={filters}
        setFilters={setFilters}
        onReset={resetFilters}
        mobileOpen={drawerOpen}
        onMobileClose={() => setDrawerOpen(false)}
        collapsedDesktop={collapsed}
        onCollapse={() => setCollapsed(true)}
      />
      {collapsed && <SidebarRail onExpand={() => setCollapsed(false)} activeCount={activeCount} />}

      <main className="flex-1 min-w-0">
        <div className="max-w-[1240px] mx-auto px-3 sm:px-5 py-4 space-y-4">
          <TopBar
            view={view}
            onViewChange={setView}
            onOpenFilters={() => setDrawerOpen(true)}
            activeCount={activeCount}
          />

          <KpiStrip metrics={metrics} distritoStats={distritoStats} />

          {view === "map" ? (
            <ZonaLesteMap
              data={distritoStats}
              selected={filters.bairro}
              onSelect={setBairro}
              lastYear={lastYear}
              prevYear={prevYear}
            />
          ) : (
            <div className="space-y-4">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <TopRubricasChart data={topRubricas} />
                <CategoryDonutChart data={categoryBreakdown} />
              </div>
              <MonthlyTrendChart data={monthlyTrend} />
              <YoyChart data={yoyComparison} />
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
