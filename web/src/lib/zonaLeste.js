/**
 * Distritos da Zona Leste de São Paulo — chave canônica normalizada
 * (maiúsculas, sem acento) → rótulo de exibição em português.
 *
 * O conjunto corresponde às 12 subprefeituras da Zona Leste (33 distritos
 * oficiais) e às features de `assets/zonaLesteDistritos.json`: os rótulos aqui
 * são idênticos ao `properties.name` do GeoJSON, para casar os dados de
 * ocorrências com as regiões do mapa coroplético (ECharts).
 *
 * O campo BAIRRO do BO é texto livre digitado pelo policial (7k+ variações,
 * incluindo erros de encoding e abreviações). `normalizeBairro` resolve
 * abreviações comuns (JD/VL/PQ/CID) e mojibake antes de comparar contra esta
 * lista — variações não mapeadas ficam de fora do mapa.
 */
export const ZONA_LESTE_LABELS = {
  "AGUA RASA": "Água Rasa",
  ARICANDUVA: "Aricanduva",
  "ARTUR ALVIM": "Artur Alvim",
  BELEM: "Belém",
  BRAS: "Brás",
  CANGAIBA: "Cangaíba",
  CARRAO: "Carrão",
  "CIDADE LIDER": "Cidade Líder",
  "CIDADE TIRADENTES": "Cidade Tiradentes",
  "ERMELINO MATARAZZO": "Ermelino Matarazzo",
  GUAIANASES: "Guaianases",
  IGUATEMI: "Iguatemi",
  "ITAIM PAULISTA": "Itaim Paulista",
  ITAQUERA: "Itaquera",
  "JARDIM HELENA": "Jardim Helena",
  "JOSE BONIFACIO": "José Bonifácio",
  LAJEADO: "Lajeado",
  MOOCA: "Mooca",
  PARI: "Pari",
  "PARQUE DO CARMO": "Parque do Carmo",
  PENHA: "Penha",
  "PONTE RASA": "Ponte Rasa",
  "SAO LUCAS": "São Lucas",
  "SAO MATEUS": "São Mateus",
  "SAO MIGUEL": "São Miguel Paulista",
  "SAO RAFAEL": "São Rafael",
  SAPOPEMBA: "Sapopemba",
  TATUAPE: "Tatuapé",
  "VILA CURUCA": "Vila Curuçá",
  "VILA FORMOSA": "Vila Formosa",
  "VILA JACUI": "Vila Jacuí",
  "VILA MATILDE": "Vila Matilde",
  "VILA PRUDENTE": "Vila Prudente",
};

const ZONA_LESTE_SET = new Set(Object.keys(ZONA_LESTE_LABELS));

/** Rótulos de exibição (com acento) de todos os distritos, para popular o filtro de bairro. */
export const ZONA_LESTE_BAIRROS = Object.values(ZONA_LESTE_LABELS).sort((a, b) =>
  a.localeCompare(b, "pt-BR")
);

// Variações frequentes no dado bruto que sobrevivem à normalização de prefixo
// (erro de digitação ou palavra extra) e que identificamos manualmente nos dados.
const ALIASES = {
  "SAO MIGUEL PAULISTA": "SAO MIGUEL",
  "SAO MIGUEL PTA": "SAO MIGUEL",
  "ERMELINO MATARAZO": "ERMELINO MATARAZZO",
  VILAPRUDENTE: "VILA PRUDENTE",
};

// Sequências de mojibake (UTF-8 lido como Latin-1) observadas no BAIRRO bruto.
const MOJIBAKE_FIXES = [
  ["Ã", "A"], // "Ã" (A-til maiúsculo)
  ["Ã", "A"], // "Á"
  ["Ã", "C"], // "Ç"
  ["Ã", "E"], // "É"
  ["Ã£", "a"], // "ã"
  ["Ã¡", "a"], // "á"
  ["Ã§", "c"], // "ç"
  ["Ã©", "e"], // "é"
  ["Ãª", "e"], // "ê"
  ["Ã³", "o"], // "ó"
  ["Ã´", "o"], // "ô"
  ["Ã­", "i"], // "í"
  ["Ãº", "u"], // "ú"
  ["Ã¼", "u"], // "ü"
];

/** Normaliza um BAIRRO bruto para comparação: maiúsculas, sem acento/mojibake, prefixos expandidos. */
export function normalizeBairro(raw) {
  if (!raw) return "";
  let s = String(raw);
  for (const [bad, good] of MOJIBAKE_FIXES) s = s.split(bad).join(good);
  s = s.normalize("NFD").replace(/[̀-ͯ]/g, ""); // remove acentos combinantes
  s = s
    .toUpperCase()
    .replace(/[.\-/]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  s = s
    .replace(/^(JD|JARDIM)\b/, "JARDIM")
    .replace(/^(VL|V)\b/, "VILA")
    .replace(/^(PQ|PARQUE)\b/, "PARQUE")
    .replace(/^(CID|CD)\b/, "CIDADE");
  return s;
}

/** Resolve um BAIRRO bruto para a chave canônica da Zona Leste, ou null se não pertence/reconhecido. */
export function matchZonaLesteBairro(raw) {
  const normalized = normalizeBairro(raw);
  const key = ALIASES[normalized] ?? normalized;
  return ZONA_LESTE_SET.has(key) ? key : null;
}

/**
 * Agrupa linhas `{BAIRRO, total, furtos, roubos}` (uma por valor bruto de
 * BAIRRO) nos distritos canônicos da Zona Leste. Retorna uma linha por
 * distrito, com `name` = rótulo de exibição (idêntico ao GeoJSON, usado pelo
 * ECharts para casar dado ↔ região).
 */
export function aggregateZonaLeste(rows) {
  const buckets = new Map();

  for (const row of rows ?? []) {
    const key = matchZonaLesteBairro(row.BAIRRO);
    if (!key) continue;
    const acc = buckets.get(key) ?? { total: 0, furtos: 0, roubos: 0 };
    acc.total += Number(row.total ?? 0);
    acc.furtos += Number(row.furtos ?? 0);
    acc.roubos += Number(row.roubos ?? 0);
    buckets.set(key, acc);
  }

  // Garante uma entrada para cada distrito (mesmo com zero ocorrências no filtro),
  // para o mapa pintar todos os polígonos.
  return Object.entries(ZONA_LESTE_LABELS)
    .map(([key, name]) => {
      const acc = buckets.get(key) ?? { total: 0, furtos: 0, roubos: 0 };
      return { key, name, total: acc.total, furtos: acc.furtos, roubos: acc.roubos };
    })
    .sort((a, b) => b.total - a.total);
}
