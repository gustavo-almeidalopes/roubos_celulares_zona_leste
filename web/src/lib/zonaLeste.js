/**
 * Bairros (distritos) da Zona Leste de São Paulo — chave canônica normalizada
 * (maiúsculas, sem acento) → rótulo de exibição em português.
 *
 * O campo BAIRRO do BO é texto livre digitado pelo policial (7k+ variações
 * distintas, incluindo erros de encoding e abreviações). `normalizeBairro`
 * resolve abreviações comuns (JD/VL/PQ/CID) e problemas de encoding antes de
 * comparar contra esta lista — variações não mapeadas ficam de fora do mapa.
 */
export const ZONA_LESTE_LABELS = {
  "AGUA RASA": "Água Rasa",
  ARICANDUVA: "Aricanduva",
  "ARTUR ALVIM": "Artur Alvim",
  BELEM: "Belém",
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
  "VILA MATILDE": "Vila Matilde",
  "VILA PRUDENTE": "Vila Prudente",
};

const ZONA_LESTE_SET = new Set(Object.keys(ZONA_LESTE_LABELS));

// Variações frequentes no dado bruto que sobrevivem à normalização de prefixo
// (erro de digitação ou palavra extra) e que identificamos manualmente nos dados.
const ALIASES = {
  "SAO MIGUEL PAULISTA": "SAO MIGUEL",
  "ERMELINO MATARAZO": "ERMELINO MATARAZZO",
  VILAPRUDENTE: "VILA PRUDENTE",
};

// Sequências de mojibake (UTF-8 lido como Latin-1) observadas no BAIRRO bruto,
// escritas via \uXXXX para evitar caracteres de controle invisíveis no fonte.
const MOJIBAKE_FIXES = [
  ["Ã", "A"], // "Ã" (capital A-tilde) corrompido
  ["Ã", "A"], // "Á" corrompido
  ["Ã", "C"], // "Ç" corrompido
  ["Ã", "E"], // "É" corrompido
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
  s = s.normalize("NFD").replace(/[̀-ͯ]/g, ""); // remove acentos combinantes (NFD)
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
 * Agrupa linhas `{BAIRRO, total, furtos, roubos, lat_sum, lon_sum, geocoded}`
 * (uma por valor bruto de BAIRRO) nos bairros canônicos da Zona Leste,
 * somando contagens e calculando o centróide geocodificado ponderado.
 */
export function aggregateZonaLeste(rows) {
  const buckets = new Map();

  for (const row of rows ?? []) {
    const key = matchZonaLesteBairro(row.BAIRRO);
    if (!key) continue;

    const acc = buckets.get(key) ?? {
      total: 0,
      furtos: 0,
      roubos: 0,
      latSum: 0,
      lonSum: 0,
      geocoded: 0,
    };
    acc.total += Number(row.total ?? 0);
    acc.furtos += Number(row.furtos ?? 0);
    acc.roubos += Number(row.roubos ?? 0);
    acc.latSum += Number(row.lat_sum ?? 0);
    acc.lonSum += Number(row.lon_sum ?? 0);
    acc.geocoded += Number(row.geocoded ?? 0);
    buckets.set(key, acc);
  }

  return [...buckets.entries()]
    .map(([key, acc]) => ({
      key,
      label: ZONA_LESTE_LABELS[key],
      total: acc.total,
      furtos: acc.furtos,
      roubos: acc.roubos,
      lat: acc.geocoded ? acc.latSum / acc.geocoded : null,
      lon: acc.geocoded ? acc.lonSum / acc.geocoded : null,
    }))
    .filter((r) => r.lat != null && r.lon != null)
    .sort((a, b) => b.total - a.total);
}
