/**
 * Camada de dados client-side — DuckDB-WASM.
 *
 * Substitui o DuckDB in-process do app Streamlit: aqui o motor roda
 * inteiramente no navegador, sem servidor. Carrega o mesmo
 * `cleaned_data.parquet` gerado pelo pipeline Python e cria a VIEW `crimes`
 * já filtrada para o município de São Paulo (capital) — igual ao app.py.
 */
import * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm_mvp from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_eh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

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

async function loadCrimesView(db, conn) {
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
}

/** Garante uma única conexão DuckDB-WASM (com a VIEW `crimes` pronta) para o app inteiro. */
export function getConnection() {
  if (!connectionPromise) {
    connectionPromise = (async () => {
      const db = await createDatabase();
      const conn = await db.connect();
      await loadCrimesView(db, conn);
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
