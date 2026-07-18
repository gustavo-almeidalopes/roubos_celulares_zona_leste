import { useEffect, useMemo, useState } from "react";
import {
  buildWhereClause,
  fetchCategoryBreakdown,
  fetchDistinctYears,
  fetchMetrics,
  fetchMonthlyTrend,
  fetchTopRubricas,
  fetchYoyComparison,
  fetchZonaLesteBairros,
} from "../lib/data";

const DEFAULT_FILTERS = {
  years: [],
  months: [],
  categories: [],
  search: "",
};

/**
 * Estado central do dashboard: filtros + dados derivados via DuckDB-WASM.
 * Equivalente ao ciclo "mudou o filtro → Streamlit re-executa as queries cacheadas".
 */
export function useDashboardData() {
  const [availableYears, setAvailableYears] = useState([]);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [status, setStatus] = useState("loading"); // loading | ready | error
  const [error, setError] = useState(null);

  const [metrics, setMetrics] = useState(null);
  const [topRubricas, setTopRubricas] = useState([]);
  const [monthlyTrend, setMonthlyTrend] = useState([]);
  const [categoryBreakdown, setCategoryBreakdown] = useState([]);
  const [yoyComparison, setYoyComparison] = useState([]);
  const [zonaLesteBairros, setZonaLesteBairros] = useState([]);

  const where = useMemo(() => buildWhereClause(filters), [filters]);

  // Carrega os anos disponíveis uma vez e define o default (3 mais recentes).
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const years = await fetchDistinctYears();
        if (cancelled) return;
        setAvailableYears(years);
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

  // Re-roda todas as queries sempre que o WHERE muda.
  useEffect(() => {
    if (availableYears.length === 0) return; // aguarda o bootstrap de anos
    let cancelled = false;
    setStatus((s) => (s === "error" ? s : "loading"));
    (async () => {
      try {
        const [m, top, trend, cat, yoy, bairros] = await Promise.all([
          fetchMetrics(where),
          fetchTopRubricas(where),
          fetchMonthlyTrend(where),
          fetchCategoryBreakdown(where),
          fetchYoyComparison(where),
          fetchZonaLesteBairros(where),
        ]);
        if (cancelled) return;
        setMetrics(m);
        setTopRubricas(top);
        setMonthlyTrend(trend);
        setCategoryBreakdown(cat);
        setYoyComparison(yoy);
        setZonaLesteBairros(bairros);
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
  }, [where, availableYears.length]);

  return {
    availableYears,
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
    zonaLesteBairros,
  };
}
