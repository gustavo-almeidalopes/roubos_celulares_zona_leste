import { useEffect, useMemo, useState } from "react";
import {
  buildWhereClause,
  fetchCategoryBreakdown,
  fetchDistinctDelegacias,
  fetchDistinctYears,
  fetchDistritoTotals,
  fetchMetrics,
  fetchMonthlyTrend,
  fetchTopRubricas,
  fetchYoyComparison,
} from "../lib/data";

const DEFAULT_FILTERS = {
  years: [],
  months: [],
  bairro: null,
  categories: [],
  rubrica: "",
  dp: null,
};

/**
 * Estado central do dashboard: filtros + dados derivados via DuckDB-WASM
 * (tudo escopado à Zona Leste pela VIEW `crimes_zl`).
 *
 * O mapa ignora o filtro de bairro (mostra sempre os 33 distritos) e usa a
 * seleção só para realçar; KPIs e gráficos respeitam todos os filtros.
 */
export function useDashboardData() {
  const [availableYears, setAvailableYears] = useState([]);
  const [delegacias, setDelegacias] = useState([]);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [status, setStatus] = useState("loading"); // loading | ready | error
  const [error, setError] = useState(null);

  const [metrics, setMetrics] = useState(null);
  const [topRubricas, setTopRubricas] = useState([]);
  const [monthlyTrend, setMonthlyTrend] = useState([]);
  const [categoryBreakdown, setCategoryBreakdown] = useState([]);
  const [yoyComparison, setYoyComparison] = useState([]);
  const [distritoTotals, setDistritoTotals] = useState([]);

  const whereAll = useMemo(() => buildWhereClause(filters), [filters]);
  // O mapa mostra todos os distritos: ignora o filtro de bairro.
  const whereMap = useMemo(() => buildWhereClause({ ...filters, bairro: null }), [filters]);

  // Bootstrap: anos + DPs disponíveis; default = 3 anos mais recentes.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const [years, dps] = await Promise.all([fetchDistinctYears(), fetchDistinctDelegacias()]);
        if (cancelled) return;
        setAvailableYears(years);
        setDelegacias(dps);
        setFilters((prev) => ({ ...prev, years: years.slice(0, 3) }));
      } catch (err) {
        if (!cancelled) {
          setError(err);
          setStatus("error");
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Re-roda as queries quando os filtros mudam.
  useEffect(() => {
    if (availableYears.length === 0) return; // aguarda o bootstrap
    let cancelled = false;
    setStatus((s) => (s === "error" ? s : "loading"));
    (async () => {
      try {
        const [m, top, trend, cat, yoy, distritos] = await Promise.all([
          fetchMetrics(whereAll),
          fetchTopRubricas(whereAll),
          fetchMonthlyTrend(whereAll),
          fetchCategoryBreakdown(whereAll),
          fetchYoyComparison(whereAll),
          fetchDistritoTotals(whereMap),
        ]);
        if (cancelled) return;
        setMetrics(m);
        setTopRubricas(top);
        setMonthlyTrend(trend);
        setCategoryBreakdown(cat);
        setYoyComparison(yoy);
        setDistritoTotals(distritos);
        setStatus("ready");
      } catch (err) {
        if (!cancelled) {
          setError(err);
          setStatus("error");
        }
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [whereAll, whereMap, availableYears.length]);

  return {
    availableYears,
    delegacias,
    filters,
    setFilters,
    resetFilters: () => setFilters({ ...DEFAULT_FILTERS, years: availableYears.slice(0, 3) }),
    status,
    error,
    metrics,
    topRubricas,
    monthlyTrend,
    categoryBreakdown,
    yoyComparison,
    distritoTotals,
  };
}
