/**
 * Camada de dados client-side — DuckDB-WASM.
 *
 * Motor SQL roda inteiramente no navegador, sem servidor. Carrega o
 * `cleaned_data.parquet` gerado pelo pipeline Python e cria:
 *
 *  1. VIEW `crimes`      — todos os BOs do município de São Paulo (capital).
 *  2. TABLE `bairro_zl`  — mapa (BAIRRO bruto → distrito canônico da Zona Leste),
 *                          construído em JS a partir dos valores DISTINCT de BAIRRO
 *                          (o campo é texto livre com 7k+ variações).
 *  3. VIEW `crimes_zl`   — `crimes` ⨝ `bairro_zl`, adicionando a coluna limpa
 *                          `DISTRITO`. Como é INNER JOIN, escopa TODO o dashboard
 *                          à Zona Leste automaticamente.
 *
 * Todas as queries do app rodam sobre `crimes_zl`.
 */
import * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm_mvp from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_eh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import { matchZonaLesteBairro, ZONA_LESTE_LABELS } from "./zonaLeste";

const MANUAL_BUNDLES = {
  mvp: { mainModule: duckdb_wasm_mvp, mainWorker: mvp_worker },
  eh: { mainModule: duckdb_wasm_eh, mainWorker: eh_worker },
};

const PARQUET_URL = "/data/cleaned_data.parquet";
const PARQUET_HANDLE = "cleaned_data.parquet";

// Mesmo filtro de SP capital do app.py (SP_CAPITAL_FILTER).
const SP_CAPITAL_FILTER = `
  UPPER(TRIM(NOME_MUNICIPIO)) IN ('S.PAULO', 'SAO PAULO', 'SÃO PAULO', 'S. PAULO', 'SP')
`;

let connectionPromise = null;

async function createDatabase() {
  const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

const sqlStr = (s) => `'${String(s).replace(/'/g, "''")}'`;

/** Constrói a tabela `bairro_zl` (BAIRRO bruto → distrito canônico) via mapeamento em JS. */
async function buildZonaLesteMap(conn) {
  const rows = await conn.query(`
    SELECT DISTINCT BAIRRO FROM crimes WHERE BAIRRO IS NOT NULL
  `);
  const pairs = [];
  for (const row of rows.toArray()) {
    const raw = row.toJSON().BAIRRO;
    const key = matchZonaLesteBairro(raw);
    if (key) pairs.push([raw, ZONA_LESTE_LABELS[key]]);
  }

  await conn.query(`CREATE TABLE bairro_zl (raw VARCHAR, distrito VARCHAR)`);
  // Insere em lotes para não estourar o tamanho de um único statement.
  const BATCH = 400;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const values = pairs
      .slice(i, i + BATCH)
      .map(([raw, dist]) => `(${sqlStr(raw)}, ${sqlStr(dist)})`)
      .join(",");
    if (values) await conn.query(`INSERT INTO bairro_zl VALUES ${values}`);
  }
  return pairs.length;
}

async function loadViews(db, conn) {
  const response = await fetch(PARQUET_URL);
  if (!response.ok) {
    throw new Error(
      `Não foi possível carregar ${PARQUET_URL} (HTTP ${response.status}). ` +
        "Verifique se o pipeline gerou o Parquet em web/public/data/."
    );
  }
  const buffer = new Uint8Array(await response.arrayBuffer());
  await db.registerFileBuffer(PARQUET_HANDLE, buffer);

  await conn.query(`
    CREATE VIEW crimes AS
    SELECT * FROM read_parquet('${PARQUET_HANDLE}')
    WHERE ${SP_CAPITAL_FILTER}
  `);

  await buildZonaLesteMap(conn);

  // INNER JOIN → escopa tudo à Zona Leste e expõe a coluna limpa DISTRITO.
  await conn.query(`
    CREATE VIEW crimes_zl AS
    SELECT c.*, m.distrito AS DISTRITO
    FROM crimes c
    JOIN bairro_zl m ON c.BAIRRO = m.raw
  `);
}

/** Garante uma única conexão DuckDB-WASM (com as views prontas) para o app inteiro. */
export function getConnection() {
  if (!connectionPromise) {
    connectionPromise = (async () => {
      const db = await createDatabase();
      const conn = await db.connect();
      await loadViews(db, conn);
      return conn;
    })().catch((err) => {
      connectionPromise = null; // permite nova tentativa em caso de falha
      throw err;
    });
  }
  return connectionPromise;
}

/** Executa uma query SQL e devolve as linhas como array de objetos JS simples. */
export async function runQuery(sql) {
  const conn = await getConnection();
  const table = await conn.query(sql);
  return table.toArray().map((row) => row.toJSON());
}
