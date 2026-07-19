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

/**
 * Períodos do dia, derivados de HORA_OCORRENCIA ("HH:MM:SS").
 * BOs sem hora ficam de fora quando o filtro está ativo.
 */
export const DAY_PERIODS = [
  { id: "manha", label: "Manhã", hours: [6, 11] },
  { id: "tarde", label: "Tarde", hours: [12, 17] },
  { id: "noite", label: "Noite", hours: [18, 23] },
  { id: "madrugada", label: "Madrugada", hours: [0, 5] },
];

const DAY_PERIOD_BY_ID = new Map(DAY_PERIODS.map((p) => [p.id, p]));

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

function dayPeriodClause(ids) {
  const hour = "TRY_CAST(SUBSTRING(HORA_OCORRENCIA, 1, 2) AS INTEGER)";
  const parts = ids
    .map((id) => DAY_PERIOD_BY_ID.get(id))
    .filter(Boolean)
    .map(({ hours: [a, b] }) => `${hour} BETWEEN ${a} AND ${b}`);
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
  if (filters.dayPeriods?.length) {
    const clause = dayPeriodClause(filters.dayPeriods);
    if (clause) clauses.push(clause);
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

const deltaPct = (curr, prev) => (prev > 0 ? ((curr - prev) / prev) * 100 : null);

/**
 * KPIs agregados — total, furtos, roubos, delegacias, distritos + variação
 * YoY com baseline explícito: `lastYear` vs `prevYear`, calculada em
 * `whereCompare` (mesmos filtros SEM o recorte de anos, senão o ano-base
 * quase nunca estaria no recorte e a variação viraria ruído).
 */
export async function fetchMetrics(where, whereCompare, lastYear, prevYear) {
  const rows = await runQuery(`
    SELECT
      COUNT(*) AS total,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 END), 0) AS furtos,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 END), 0) AS roubos,
      COUNT(DISTINCT NOME_DELEGACIA) AS delegacias,
      COUNT(DISTINCT DISTRITO) AS distritos
    FROM crimes_zl ${where}
  `);

  let yoyDeltaPct = null;
  if (lastYear != null && prevYear != null) {
    const glue = whereCompare ? `${whereCompare} AND` : "WHERE";
    const cmp = await runQuery(`
      SELECT
        COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = ${lastYear} THEN 1 END), 0) AS total_curr,
        COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = ${prevYear} THEN 1 END), 0) AS total_prev
      FROM crimes_zl ${glue} CAST(ANO AS INTEGER) IN (${lastYear}, ${prevYear})
    `);
    yoyDeltaPct = deltaPct(Number(cmp[0]?.total_curr ?? 0), Number(cmp[0]?.total_prev ?? 0));
  }

  const m = rows[0] ?? {};
  return {
    total: Number(m.total ?? 0),
    furtos: Number(m.furtos ?? 0),
    roubos: Number(m.roubos ?? 0),
    delegacias: Number(m.delegacias ?? 0),
    distritos: Number(m.distritos ?? 0),
    yoyDeltaPct,
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
 * Estatísticas por distrito para o mapa — totais do recorte atual + variação
 * YoY (`lastYear` vs `prevYear`) por métrica, para o card de detalhe.
 *
 * `where` = filtros do mapa (sem bairro); `whereCompare` = idem SEM o recorte
 * de anos, para o ano-base existir mesmo quando só um ano está selecionado.
 * Retorna sempre os 33 distritos (zerando os sem ocorrências no recorte).
 */
export async function fetchDistritoStats(where, whereCompare, lastYear, prevYear) {
  const totalsPromise = runQuery(`
    SELECT
      DISTRITO AS name,
      COUNT(*) AS total,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 END), 0) AS furtos,
      COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 END), 0) AS roubos
    FROM crimes_zl ${where}
    GROUP BY DISTRITO
  `);

  let comparePromise = Promise.resolve([]);
  if (lastYear != null && prevYear != null) {
    const glue = whereCompare ? `${whereCompare} AND` : "WHERE";
    const ano = "CAST(ANO AS INTEGER)";
    comparePromise = runQuery(`
      SELECT
        DISTRITO AS name,
        SUM(CASE WHEN ${ano} = ${lastYear} THEN 1 ELSE 0 END) AS total_curr,
        SUM(CASE WHEN ${ano} = ${prevYear} THEN 1 ELSE 0 END) AS total_prev,
        SUM(CASE WHEN ${ano} = ${lastYear} AND RUBRICA LIKE 'ROUBO%' THEN 1 ELSE 0 END) AS roubos_curr,
        SUM(CASE WHEN ${ano} = ${prevYear} AND RUBRICA LIKE 'ROUBO%' THEN 1 ELSE 0 END) AS roubos_prev,
        SUM(CASE WHEN ${ano} = ${lastYear} AND RUBRICA LIKE 'FURTO%' THEN 1 ELSE 0 END) AS furtos_curr,
        SUM(CASE WHEN ${ano} = ${prevYear} AND RUBRICA LIKE 'FURTO%' THEN 1 ELSE 0 END) AS furtos_prev
      FROM crimes_zl ${glue} ${ano} IN (${lastYear}, ${prevYear})
      GROUP BY DISTRITO
    `);
  }

  const [totals, compare] = await Promise.all([totalsPromise, comparePromise]);
  const totalsByName = new Map(totals.map((r) => [r.name, r]));
  const compareByName = new Map(compare.map((r) => [r.name, r]));

  return Object.values(ZONA_LESTE_LABELS)
    .map((name) => {
      const t = totalsByName.get(name);
      const c = compareByName.get(name);
      return {
        name,
        total: Number(t?.total ?? 0),
        furtos: Number(t?.furtos ?? 0),
        roubos: Number(t?.roubos ?? 0),
        deltas: {
          total: c ? deltaPct(Number(c.total_curr), Number(c.total_prev)) : null,
          roubos: c ? deltaPct(Number(c.roubos_curr), Number(c.roubos_prev)) : null,
          furtos: c ? deltaPct(Number(c.furtos_curr), Number(c.furtos_prev)) : null,
        },
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
