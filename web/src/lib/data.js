/**
 * WHERE-clause builder + funções de consulta — porta de filters.py / metrics.py
 * / charts.py para JS. Todas as funções rodam sobre a VIEW `crimes` (DuckDB-WASM).
 */
import { runQuery } from "./duckdb";
import { aggregateZonaLeste } from "./zonaLeste";

export const MONTH_LABELS = [
  "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
  "Jul", "Ago", "Set", "Out", "Nov", "Dez",
];

function categoryClause(categories) {
  const parts = [];
  if (categories.includes("Furto")) parts.push("RUBRICA LIKE 'FURTO%'");
  if (categories.includes("Roubo")) parts.push("RUBRICA LIKE 'ROUBO%'");
  if (categories.includes("Outros")) {
    parts.push("(RUBRICA NOT LIKE 'FURTO%' AND RUBRICA NOT LIKE 'ROUBO%')");
  }
  return parts.length ? `(${parts.join(" OR ")})` : null;
}

/** Converte o estado de filtros do React em uma cláusula SQL `WHERE ...`. */
export function buildWhereClause(filters) {
  const clauses = [];

  if (filters.years?.length) {
    clauses.push(`CAST(ANO AS INTEGER) IN (${filters.years.map((y) => parseInt(y, 10)).join(", ")})`);
  }
  if (filters.months?.length) {
    clauses.push(`CAST(MES AS INTEGER) IN (${filters.months.map((m) => parseInt(m, 10)).join(", ")})`);
  }
  if (filters.categories?.length) {
    const clause = categoryClause(filters.categories);
    if (clause) clauses.push(clause);
  }
  if (filters.search?.trim()) {
    const raw = filters.search.trim();
    const safe = raw
      .replace(/\\/g, "\\\\")
      .replace(/'/g, "''")
      .replace(/%/g, "\\%")
      .replace(/_/g, "\\_");
    clauses.push(`UPPER(RUBRICA) LIKE UPPER('%${safe}%') ESCAPE '\\'`);
  }

  return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
}

/** Anos distintos disponíveis (para popular o filtro), ordenados desc. */
export async function fetchDistinctYears() {
  const rows = await runQuery(
    "SELECT DISTINCT CAST(ANO AS INTEGER) AS ano FROM crimes WHERE ANO IS NOT NULL ORDER BY ano DESC"
  );
  return rows.map((r) => Number(r.ano));
}

/** KPIs agregados — total, furtos, roubos, delegacias, bairros, geocodificado, YoY. */
export async function fetchMetrics(where) {
  const lastYearRow = await runQuery(`SELECT MAX(CAST(ANO AS INTEGER)) AS y FROM crimes ${where}`);
  const lastYear = lastYearRow[0]?.y != null ? Number(lastYearRow[0].y) : null;
  const prevYear = lastYear != null ? lastYear - 1 : null;

  const rows = await runQuery(`
    SELECT
      COUNT(*) AS total,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 END), 0) AS furtos,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 END), 0) AS roubos,
      COUNT(DISTINCT NOME_DELEGACIA) AS delegacias,
      COUNT(DISTINCT BAIRRO) AS bairros,
      COALESCE(SUM(CASE WHEN LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL THEN 1 END), 0) AS geocoded,
      COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = ${lastYear ?? -1} THEN 1 END), 0) AS total_curr,
      COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = ${prevYear ?? -1} THEN 1 END), 0) AS total_prev
    FROM crimes ${where}
  `);
  const m = rows[0] ?? {};
  const total = Number(m.total ?? 0);
  const totalCurr = Number(m.total_curr ?? 0);
  const totalPrev = Number(m.total_prev ?? 0);

  return {
    total,
    furtos: Number(m.furtos ?? 0),
    roubos: Number(m.roubos ?? 0),
    delegacias: Number(m.delegacias ?? 0),
    bairros: Number(m.bairros ?? 0),
    geocodedPct: total ? (Number(m.geocoded ?? 0) / total) * 100 : 0,
    yoyDeltaPct: totalPrev > 0 ? ((totalCurr - totalPrev) / totalPrev) * 100 : null,
    lastYear,
    prevYear,
  };
}

/** Top N rubricas (tipos de crime) por volume. */
export async function fetchTopRubricas(where, topN = 15) {
  const extra = where ? "AND" : "WHERE";
  return runQuery(`
    SELECT RUBRICA, COUNT(*) AS occurrences
    FROM crimes ${where}
    ${extra} RUBRICA IS NOT NULL
    GROUP BY RUBRICA
    ORDER BY occurrences DESC
    LIMIT ${topN}
  `);
}

/** Série mensal de ocorrências, agrupada por ano (para o gráfico de tendência). */
export async function fetchMonthlyTrend(where) {
  const extra = where ? "AND" : "WHERE";
  return runQuery(`
    SELECT CAST(ANO AS INTEGER) AS ano, CAST(MES AS INTEGER) AS mes, COUNT(*) AS occurrences
    FROM crimes ${where}
    ${extra} ANO IS NOT NULL AND MES IS NOT NULL
    GROUP BY ano, mes
    ORDER BY ano, mes
  `);
}

/** Quebra por categoria (Furto / Roubo / Outros) — para o donut. */
export async function fetchCategoryBreakdown(where) {
  return runQuery(`
    SELECT
      CASE
        WHEN RUBRICA LIKE 'FURTO%' THEN 'Furto'
        WHEN RUBRICA LIKE 'ROUBO%' THEN 'Roubo'
        ELSE 'Outros'
      END AS category,
      COUNT(*) AS occurrences
    FROM crimes ${where}
    GROUP BY category
    ORDER BY occurrences DESC
  `);
}

/** Ocorrências por bairro da Zona Leste (centróide geocodificado + furtos/roubos) — para o mapa. */
export async function fetchZonaLesteBairros(where) {
  const extra = where ? "AND" : "WHERE";
  const rows = await runQuery(`
    SELECT
      BAIRRO,
      COUNT(*) AS total,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 END), 0) AS furtos,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 END), 0) AS roubos,
      COALESCE(SUM(LATITUDE), 0) AS lat_sum,
      COALESCE(SUM(LONGITUDE), 0) AS lon_sum,
      COUNT(LATITUDE) AS geocoded
    FROM crimes ${where}
    ${extra} BAIRRO IS NOT NULL
    GROUP BY BAIRRO
  `);
  return aggregateZonaLeste(rows);
}

/** Furtos vs Roubos por ano — para o gráfico YoY. */
export async function fetchYoyComparison(where) {
  const extra = where ? "AND" : "WHERE";
  return runQuery(`
    SELECT
      CAST(ANO AS INTEGER) AS ano,
      SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 ELSE 0 END) AS furtos,
      SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 ELSE 0 END) AS roubos
    FROM crimes ${where}
    ${extra} ANO IS NOT NULL
    GROUP BY ano
    ORDER BY ano
  `);
}
