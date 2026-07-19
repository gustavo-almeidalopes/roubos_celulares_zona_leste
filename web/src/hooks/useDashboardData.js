import { useEffect, useMemo, useState } from "react";
import {
  buildWhereClause,
  fetchCategoryBreakdown,
  fetchDistinctDelegacias,
  fetchDistinctYears,
  fetchDistritoStats,
  fetchMetrics,
  fetchMonthlyTrend,
  fetchTopRubricas,
  fetchYoyComparison,
} from "../lib/data";

const DEFAULT_FILTERS = {
  years: [],
  months: [],
  dayPeriods: [],
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
 *
 * Baseline temporal explícito: toda variação YoY compara `lastYear` (ano mais
 * recente do recorte) contra `prevYear` (ano anterior), calculada sem o filtro
 * de anos — assim o ano-base sempre existe e o card pode dizer "X vs Y".
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
  const [distritoStats, setDistritoStats] = useState([]);

  // Ano de referência da variação: o mais recente do recorte (ou dos dados).
  const lastYear = useMemo(() => {
    if (filters.years.length) return Math.max(...filters.years.map(Number));
    return availableYears.length ? availableYears[0] : null;
  }, [filters.years, availableYears]);
  const prevYear = lastYear != null ? lastYear - 1 : null;

  const whereAll = useMemo(() => buildWhereClause(filters), [filters]);
  // O mapa mostra todos os distritos: ignora o filtro de bairro.
  const whereMap = useMemo(() => buildWhereClause({ ...filters, bairro: null }), [filters]);
  // Comparações YoY: sem o recorte de anos (o ano-base precisa existir).
  const whereCompareAll = useMemo(() => buildWhereClause({ ...filters, years: [] }), [filters]);
  const whereCompareMap = useMemo(
    () => buildWhereClause({ ...filters, years: [], bairro: null }),
    [filters]
  );

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
          fetchMetrics(whereAll, whereCompareAll, lastYear, prevYear),
          fetchTopRubricas(whereAll),
          fetchMonthlyTrend(whereAll),
          fetchCategoryBreakdown(whereAll),
          fetchYoyComparison(whereAll),
          fetchDistritoStats(whereMap, whereCompareMap, lastYear, prevYear),
        ]);
        if (cancelled) return;
        setMetrics(m);
        setTopRubricas(top);
        setMonthlyTrend(trend);
        setCategoryBreakdown(cat);
        setYoyComparison(yoy);
        setDistritoStats(distritos);
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
  }, [whereAll, whereMap, whereCompareAll, whereCompareMap, lastYear, availableYears.length]);

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
    distritoStats,
    lastYear,
    prevYear,
  };
}
