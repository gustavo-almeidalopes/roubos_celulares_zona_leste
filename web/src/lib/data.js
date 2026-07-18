/**
 * WHERE-clause builder + funções de consulta. Todas rodam sobre a VIEW
 * `crimes_zl` (DuckDB-WASM) — já escopada à Zona Leste, com a coluna limpa
 * `DISTRITO`.
 */
import { runQuery } from "./duckdb";
import { ZONA_LESTE_LABELS } from "./zonaLeste";

export const MONTH_LABELS = [
  "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
  "Jul", "Ago", "Set", "Out", "Nov", "Dez",
];

const sqlStr = (s) => `'${String(s).replace(/'/g, "''")}'`;

function likeEscape(raw) {
  return raw
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "''")
    .replace(/%/g, "\\%")
    .replace(/_/g, "\\_");
}

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
  if (filters.bairro) {
    clauses.push(`DISTRITO = ${sqlStr(filters.bairro)}`);
  }
  if (filters.dp) {
    clauses.push(`NOME_DELEGACIA = ${sqlStr(filters.dp)}`);
  }
  if (filters.categories?.length) {
    const clause = categoryClause(filters.categories);
    if (clause) clauses.push(clause);
  }
  if (filters.rubrica?.trim()) {
    clauses.push(`UPPER(RUBRICA) LIKE UPPER('%${likeEscape(filters.rubrica.trim())}%') ESCAPE '\\'`);
  }

  return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
}

/** Anos distintos disponíveis (para popular o filtro), ordenados desc. */
export async function fetchDistinctYears() {
  const rows = await runQuery(
    "SELECT DISTINCT CAST(ANO AS INTEGER) AS ano FROM crimes_zl WHERE ANO IS NOT NULL ORDER BY ano DESC"
  );
  return rows.map((r) => Number(r.ano));
}

/** Delegacias (DPs) distintas na Zona Leste, ordenadas por volume desc. */
export async function fetchDistinctDelegacias() {
  const rows = await runQuery(`
    SELECT NOME_DELEGACIA AS dp, COUNT(*) AS n
    FROM crimes_zl
    WHERE NOME_DELEGACIA IS NOT NULL
    GROUP BY NOME_DELEGACIA
    ORDER BY n DESC
  `);
  return rows.map((r) => r.dp);
}

/** KPIs agregados — total, furtos, roubos, delegacias, distritos, YoY. */
export async function fetchMetrics(where) {
  const lastYearRow = await runQuery(`SELECT MAX(CAST(ANO AS INTEGER)) AS y FROM crimes_zl ${where}`);
  const lastYear = lastYearRow[0]?.y != null ? Number(lastYearRow[0].y) : null;
  const prevYear = lastYear != null ? lastYear - 1 : null;

  const rows = await runQuery(`
    SELECT
      COUNT(*) AS total,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 END), 0) AS furtos,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 END), 0) AS roubos,
      COUNT(DISTINCT NOME_DELEGACIA) AS delegacias,
      COUNT(DISTINCT DISTRITO) AS distritos,
      COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = ${lastYear ?? -1} THEN 1 END), 0) AS total_curr,
      COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = ${prevYear ?? -1} THEN 1 END), 0) AS total_prev
    FROM crimes_zl ${where}
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
    distritos: Number(m.distritos ?? 0),
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
    FROM crimes_zl ${where}
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
    FROM crimes_zl ${where}
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
    FROM crimes_zl ${where}
    GROUP BY category
    ORDER BY occurrences DESC
  `);
}

/**
 * Ocorrências por distrito da Zona Leste — para o mapa coroplético.
 * Retorna sempre os 33 distritos (preenchendo com zero os sem ocorrências
 * no recorte), com `name` = rótulo idêntico ao GeoJSON.
 */
export async function fetchDistritoTotals(where) {
  const rows = await runQuery(`
    SELECT
      DISTRITO AS name,
      COUNT(*) AS total,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 END), 0) AS furtos,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 END), 0) AS roubos
    FROM crimes_zl ${where}
    GROUP BY DISTRITO
  `);
  const byName = new Map(rows.map((r) => [r.name, r]));
  return Object.values(ZONA_LESTE_LABELS)
    .map((name) => {
      const r = byName.get(name);
      return {
        name,
        total: Number(r?.total ?? 0),
        furtos: Number(r?.furtos ?? 0),
        roubos: Number(r?.roubos ?? 0),
      };
    })
    .sort((a, b) => b.total - a.total);
}

/** Furtos vs Roubos por ano — para o gráfico YoY. */
export async function fetchYoyComparison(where) {
  const extra = where ? "AND" : "WHERE";
  return runQuery(`
    SELECT
      CAST(ANO AS INTEGER) AS ano,
      SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 ELSE 0 END) AS furtos,
      SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 ELSE 0 END) AS roubos
    FROM crimes_zl ${where}
    ${extra} ANO IS NOT NULL
    GROUP BY ano
    ORDER BY ano
  `);
}
