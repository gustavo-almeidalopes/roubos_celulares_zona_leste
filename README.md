<div align="center">

# 🔍 SP Public Safety Dashboard

**Dashboard analítico interativo para ocorrências de subtrações de celulares no Estado de São Paulo**  
Dados oficiais da SSP-SP · Pipeline de ingestão em chunks · DuckDB + Parquet · Streamlit + Plotly

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://rouboscelulareszonaleste-cpqy3z3r56vvkkpbhjfvhn.streamlit.app/)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-in--process-yellow?logo=duckdb)
![License](https://img.shields.io/badge/license-MIT-green)

</div>

---

## 📌 Sobre o Projeto

Este projeto transforma os CSVs brutos de ocorrências da **Secretaria de Segurança Pública do Estado de São Paulo (SSP-SP)** em um dashboard analítico interativo. O foco atual são os **roubos e furtos de celulares na Zona Leste de São Paulo**, com planos de expansão para todas as regiões do estado.

A motivação central é **democratizar o acesso aos dados de segurança pública**: transformar planilhas brutas, densas e difíceis de interpretar em visualizações claras que qualquer cidadão, pesquisador ou gestor público consiga consumir sem fricção.

A stack foi projetada para escalar **sem servidor e sem banco de dados externo**: o pipeline de ingestão limpa e persiste os dados em **DuckDB + Parquet**, e o app lê o arquivo diretamente na memória via VIEW — zero dependência de nuvem, zero custo de infraestrutura.

---

## 🌐 Acesso ao Projeto

| Recurso | Link |
|---|---|
| 🚀 **App ao vivo (Streamlit Cloud)** | https://rouboscelulareszonaleste-cpqy3z3r56vvkkpbhjfvhn.streamlit.app/ |
| 🐙 **Repositório GitHub** | https://github.com/gustavo-almeidalopes/roubos_celulares_zona_leste |

---

## 📊 Fonte dos Dados

Os dados utilizados são provenientes exclusivamente de fontes oficiais do Governo do Estado de São Paulo:

| Fonte | Descrição | Link |
|---|---|---|
| **SSP-SP** | Consulta de Boletins de Ocorrência de celulares subtraídos (furtos e roubos) | [ssp.sp.gov.br/estatistica/dados-mensais](https://www.ssp.sp.gov.br/estatistica/consultas) |
| **SSP-SP — Transparência** | Portal de dados abertos de segurança pública | [transparencia.ssp.sp.gov.br](https://www.ssp.sp.gov.br/) |

> Os dados são de domínio público e utilizados exclusivamente para fins analíticos e educacionais, sem qualquer fins comerciais.

---

## ⚙️ Configuração de Fontes (Pipeline)

O bloco abaixo é a **fonte única de configuração** do pipeline de ingestão. O módulo
[`src/data_pipeline/config.py`](src/data_pipeline/config.py) lê o **primeiro bloco `json`** deste README
que contenha a chave `sources`. Para registrar uma nova fonte, basta adicionar um objeto ao array
`sources` — nenhum código precisa mudar.

```json
{
  "sources": [
    {
      "name": "celulares_subtraidos",
      "url": "<COLE A URL DO XLSX/CSV DA SSP-SP AQUI>",
      "format": "xlsx",
      "enabled": true,
      "sheet": 0,
      "sep": ";",
      "encoding": "utf-8"
    }
  ],
  "schema": {
    "bbox": { "lat_min": -25.5, "lat_max": -19.5, "lon_min": -53.0, "lon_max": -44.0 }
  }
}
```

**Campos de cada fonte**

| Campo | Obrigatório | Descrição |
|---|---|---|
| `name` | sim | Identificador da fonte. Também é o nome do arquivo em `data/raw/` (ex.: `celulares_subtraidos.xlsx`). |
| `format` | sim | `xlsx`, `xls` ou `csv`. |
| `url` | não | URL http(s) de download. Se ausente, inválida ou placeholder, o pipeline usa o arquivo local (**modo drop manual**). |
| `enabled` | não | `true`/`false` para ligar/desligar a fonte (padrão `true`). |
| `sheet` | não | Índice/nome da aba do Excel (padrão `0`). |
| `sep` / `encoding` | não | Apenas para fontes `csv` (padrão `;` / `utf-8`). |

**Dois modos de operação**

1. **Automático** — preencha `url` com o link real do XLSX/CSV da SSP-SP. O pipeline baixa, converte e processa sozinho (use o GitHub Actions abaixo para rodar de forma agendada).
2. **Drop manual** — deixe `url` como placeholder e coloque o arquivo em `src/data/raw/<name>.<formato>`. Útil enquanto a URL direta da SSP-SP não está disponível.

> O `bbox` define a caixa delimitadora do Estado de SP: coordenadas fora dela são anuladas na limpeza.

---

## ✨ Funcionalidades

### KPIs (Cards de Métricas)
Exibidos no topo do dashboard, recalculados dinamicamente a cada mudança de filtro:

- **Total de Ocorrências** — contagem absoluta de registros no período filtrado
- **Furtos (Furtos)** — ocorrências cujo `RUBRICA` começa com `FURTO%`
- **Roubos (Roubos)** — ocorrências cujo `RUBRICA` começa com `ROUBO%`
- **Outros Crimes** — demais categorias não classificadas acima
- **Delegacias / Municípios** — contagem distinta de precincts e municípios presentes na seleção

### Gráficos e Visualizações

| # | Visualização | Tipo | Descrição |
|---|---|---|---|
| 1 | **Top 15 Rubricas** | Barras horizontais | Categorias criminais mais frequentes, coloridas por volume (escala Reds) |
| 2 | **Breakdown por Categoria** | Donut (rosca) | Proporção entre Furtos, Roubos e Outros — com percentual e rótulo |
| 3 | **Tendência Mensal por Ano** | Linha multi-série | Evolução mês a mês com uma linha por ano; eixo X rotulado em inglês abreviado |
| 4 | **Top 15 Municípios** | Barras verticais | Municípios com maior volume de ocorrências, coloridos em escala Blues |
| 5 | **YoY: Furtos vs Roubos** | Barras agrupadas | Comparativo ano a ano entre as duas principais categorias criminais |
| 6 | **Heatmap Dia × Período** | Heatmap (YlOrRd) | Concentração de ocorrências por dia da semana cruzado com período do dia (`DESCR_PERIODO`) |
| 7 | **Mapa de Incidentes** | Scatter Mapbox | Amostra georreferenciada de até 5.000 registros; cor por tipo de crime; basemap Carto |

### Seções Expansíveis

- **Preview dos dados brutos** — tabela com as primeiras 200 linhas do resultado filtrado
- **Download CSV filtrado** — exportação completa dos dados com os filtros ativos, delimitada por `;`

### Filtros da Sidebar

| Filtro | Tipo | Comportamento padrão |
|---|---|---|
| **Ano** | Multiselect | Seleciona os 3 anos mais recentes |
| **Mês** | Multiselect | Todos os 12 meses selecionados |
| **Delegacia (Precinct)** | Multiselect | Nenhuma (= todas as delegacias) |
| **Município** | Multiselect | Nenhum (= todos os municípios) |

Todos os valores dos filtros são carregados com `DISTINCT` direto do DuckDB e cacheados por 1 hora (`@st.cache_data(ttl=3600)`).

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│                   CSV BRUTO  (SSP-SP)                   │
│         CelularesSubtraidos.csv  —  delim: ";"          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
          ┌──────────────────────────────┐
          │      clean_ingest.py         │   ← executa UMA vez via CLI
          │                              │
          │  1. Probe do header CSV      │
          │  2. Leitura em chunks        │
          │     de 50.000 linhas         │
          │  3. Limpeza por chunk:       │
          │     · texto → upper + strip  │
          │     · coords → float, bbox   │
          │     · datas → datetime       │
          │     · ints → Int64 nullable  │
          │     · dedup intra-chunk      │
          │       (NUM_BO + ANO_BO)      │
          │  4. Append → DuckDB table    │
          │  5. Dedup cross-chunk        │
          │     (QUALIFY ROW_NUMBER)     │
          │  6. Índices por coluna       │
          │  7. COPY → Parquet (zstd)    │
          └──────────┬───────────────────┘
                     │
           ┌─────────┴──────────┐
           │                    │
           ▼                    ▼
    crimes.duckdb      cleaned_data.parquet
    (store analítico)  (snapshot zstd — commitado no git)
                                │
                                ▼
              ┌─────────────────────────────────┐
              │            app.py               │
              │                                 │
              │  DuckDB in-memory               │
              │  CREATE VIEW crimes AS          │
              │  SELECT * FROM read_parquet()   │
              │                                 │
              │  @st.cache_resource → 1 conexão │
              │  por sessão Streamlit            │
              └──────────┬──────────────────────┘
                         │
           ┌─────────────┼──────────────────┐
           ▼             ▼                  ▼
      filters.py      metrics.py        charts.py
   (WHERE clause)   (KPI cards)      (7 gráficos Plotly)
   @cache_data      @cache_data       @cache_data
   por where_clause por where_clause  por where_clause
```

**Princípio central de performance:** o app nunca carrega o dataset inteiro em memória. O DuckDB opera sobre o Parquet via VIEW e materializa apenas os pequenos DataFrames resultantes de cada `GROUP BY`. Todos os resultados de query são cacheados por `where_clause`, então alterar um filtro dispara exatamente **uma** re-query por componente — re-renders subsequentes servem do cache.

---

## 🗂️ Estrutura do Projeto

```
sp-public-safety-dashboard/
│
├── src/
│   ├── app.py                          # Entrada principal do Streamlit
│   │
│   ├── components/
│   │   ├── __init__.py
│   │   ├── charts.py                   # 7 builders de gráficos Plotly
│   │   ├── filters.py                  # Widgets da sidebar + WHERE builder
│   │   └── metrics.py                  # KPI cards (5 métricas)
│   │
│   ├── data_pipeline/                  # Pipeline modular de ingestão
│   │   ├── __init__.py
│   │   ├── __main__.py                 # `python -m src.data_pipeline`
│   │   ├── cli.py                      # CLI (subcomando `run`)
│   │   ├── config.py                   # lê a config de fontes do README
│   │   ├── download.py                 # baixa as fontes (+ fallback manual)
│   │   ├── convert.py                  # Excel (xls/xlsx) → CSV
│   │   ├── clean.py                    # limpeza/normalização em chunks
│   │   ├── validate.py                 # validação + relatório de qualidade
│   │   └── pipeline.py                 # orquestrador
│   │
│   └── data/
│       ├── raw/
│       │   └── CelularesSubtraidos.csv # ← CSV bruto da SSP-SP (não commitado)
│       └── processed/
│           ├── crimes.duckdb           # store analítico (gerado pelo pipeline)
│           └── cleaned_data.parquet    # snapshot zstd (commitado no git)
│
├── requirements.txt
└── README.md
```

---

## 🚀 Como Executar Localmente

### Pré-requisitos

- Python 3.9 ou superior
- pip

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/sp-public-safety-dashboard.git
cd sp-public-safety-dashboard
```

### 2. Instale as dependências

```bash
pip install -r requirements.txt
```

### 3. Forneça os dados da SSP-SP

Há dois caminhos (veja [Configuração de Fontes](#️-configuração-de-fontes-pipeline)):

- **Automático:** preencha a `url` da fonte no bloco de config do README. O pipeline baixa sozinho.
- **Drop manual:** baixe o arquivo de celulares subtraídos em [ssp.sp.gov.br/estatistica/dados-mensais](https://www.ssp.sp.gov.br/estatistica/dados-mensais) e salve como `src/data/raw/celulares_subtraidos.xlsx` (também aceita `.xls`/`.csv`).

> Excel é convertido para CSV automaticamente. Fontes CSV usam `;` por padrão (configurável).

### 4. Execute o pipeline de ingestão

```bash
python -m src.data_pipeline run
```

O pipeline baixa (se houver URL), converte Excel→CSV, limpa em chunks de 50.000 linhas, valida e exporta o Parquet — exibindo o progresso em tempo real. Um arquivo de 1 milhão de linhas leva tipicamente entre 30 e 90 segundos.

**Exemplo de saída esperada:**
```
2024-01-01 10:00:00 [INFO] CSV columns: 46 total | loading 44 selected
2024-01-01 10:00:02 [INFO] Chunk    1 | raw:  50000 → clean:  49812 | cumulative:     49812 | 2s
2024-01-01 10:00:04 [INFO] Chunk    2 | raw:  50000 → clean:  49934 | cumulative:     99746 | 4s
...
2024-01-01 10:01:15 [INFO] Deduplication complete: 3241 rows removed, 847623 unique records remain.
2024-01-01 10:01:18 [INFO] ============================================================
2024-01-01 10:01:18 [INFO] INGESTION COMPLETE in 78.3 seconds
2024-01-01 10:01:18 [INFO]   Input rows  : 850864
2024-01-01 10:01:18 [INFO]   Clean rows  : 847623 (99.6% retained)
2024-01-01 10:01:18 [INFO]   DuckDB      : 312.4 MB  → src/data/processed/crimes.duckdb
2024-01-01 10:01:18 [INFO]   Parquet     :  87.1 MB  → src/data/processed/cleaned_data.parquet
2024-01-01 10:01:18 [INFO] ============================================================
```

**Flags completas do CLI:**

```
python -m src.data_pipeline run --help

  --readme        Caminho do README com a config de fontes   (padrão: ./README.md)
  --raw-dir       Diretório dos arquivos brutos              (padrão: src/data/raw)
  --out-dir       Diretório de saída (Parquet + relatório)   (padrão: src/data/processed)
  --source        Processa apenas a fonte com este 'name'    (padrão: todas)
  --chunk-size    Linhas por chunk na limpeza                (padrão: 50000)
  --keep-duckdb   Mantém o crimes.duckdb após a execução     (padrão: descarta)
  --log-level     DEBUG | INFO | WARNING | ERROR             (padrão: INFO)
```

Saídas geradas em `src/data/processed/`: `cleaned_data.parquet` (consumido pelo app) e
`quality_report.md` (relatório de qualidade: linhas, % de nulos por coluna, % geocodificado).

### 5. Inicie o dashboard

```bash
streamlit run src/app.py
```

Acesse em `http://localhost:8501`.

---

## ☁️ Deploy no Streamlit Cloud

1. Execute o pipeline localmente para gerar o `cleaned_data.parquet`

2. Commit o Parquet no repositório:
```bash
# Se o arquivo ultrapassar 100 MB, use Git LFS:
git lfs track "*.parquet"
git add src/data/processed/cleaned_data.parquet
git commit -m "chore: add processed parquet"
git push
```

3. Acesse [share.streamlit.io](https://share.streamlit.io) → **New app** → aponte para `src/app.py`

4. O app lê o Parquet diretamente — nenhum servidor de banco de dados necessário

---

## ▲ Landing page no Vercel

O Vercel **não roda Streamlit** (precisa de servidor persistente). A estratégia é:
o **app interativo** roda no Streamlit Cloud e uma **landing page estática** (em
[`web/index.html`](web/index.html)) é publicada no Vercel apontando para ele.

**Deploy (dashboard do Vercel):**
1. [vercel.com/new](https://vercel.com/new) → importe o repositório `roubos_celulares_zona_leste`.
2. Em **Framework Preset** escolha **Other**. O [`vercel.json`](vercel.json) já define a saída estática (`web/`) e desliga build/install — o Vercel só serve a página.
3. **Deploy**. A landing page sobe e o botão "Abrir dashboard" leva ao app no Streamlit.

**Deploy (CLI):**
```bash
npm i -g vercel
vercel --prod   # na raiz do repositório; o vercel.json cuida do resto
```

> Para trocar o link do dashboard, edite a URL do Streamlit em `web/index.html`.

---

## 🧰 Stack Técnica

| Tecnologia | Versão recomendada | Papel no projeto |
|---|---|---|
| [Python](https://python.org/) | 3.9+ | Linguagem base |
| [Streamlit](https://streamlit.io/) | latest | Framework do dashboard e deploy |
| [DuckDB](https://duckdb.org/) | latest | Engine analítica in-process; VIEW sobre Parquet; queries SQL |
| [Pandas](https://pandas.pydata.org/) | latest | Leitura do CSV em chunks e limpeza de dados |
| [Plotly](https://plotly.com/python/) | latest | Todos os gráficos interativos (bar, line, pie, heatmap, scatter_mapbox) |
| [Parquet + zstd](https://parquet.apache.org/) | — | Armazenamento colunar comprimido; gerado via DuckDB Arrow (sem round-trip Pandas) |

---

## 🔧 Pipeline de Limpeza — Detalhes

O `clean_ingest.py` aplica as seguintes transformações em cada chunk antes de persistir:

| Etapa | O que faz |
|---|---|
| **Seleção de colunas** | Carrega apenas as 44 colunas relevantes dos 46+ do CSV original, economizando ~30–50% de memória |
| **Padronização de texto** | `upper()` + `strip()` em 12 colunas; substitui `"NAN"`, `"NONE"` e `""` por `None` |
| **Coordenadas geográficas** | Converte vírgula decimal pt-BR para ponto; descarta coords fora do bounding box do Estado de SP (`lat: -25.5 a -19.5`, `lon: -53.0 a -44.0`) |
| **Datas de ocorrência** | `pd.to_datetime` com `dayfirst=True`; erros silenciados como `NaT` |
| **Datetimes de registro** | `DATAHORA_REGISTRO_BO` e `DATA_COMUNICACAO_BO` parseados como datetime |
| **Inteiros nullable** | `ANO`, `MES`, `ANO_BO`, `ID_DELEGACIA` → `Int64` (nullable, compatível com DuckDB) |
| **Deduplicação intra-chunk** | `drop_duplicates(subset=["NUM_BO", "ANO_BO"])` por chunk |
| **Deduplicação cross-chunk** | `QUALIFY ROW_NUMBER() OVER (PARTITION BY NUM_BO, ANO_BO)` via DuckDB após todos os chunks |
| **Índices analíticos** | `CREATE INDEX` em `ANO`, `MES`, `RUBRICA`, `NOME_DELEGACIA`, `NOME_MUNICIPIO`, `ANO_BO` |

---

## 🗺️ Roadmap — Próximas Melhorias

### ⚡ Performance
- [ ] Migrar queries de `@st.cache_data` para cache persistente entre sessões (Redis ou DuckDB materializado)
- [ ] Implementar paginação no preview de dados brutos para evitar serialização de DataFrames grandes
- [ ] Avaliar particionamento do Parquet por `ANO` para queries temporais mais rápidas
- [ ] Benchmarking de chunk sizes maiores (100k, 200k) para máquinas com +16 GB de RAM

### 🗺️ Expansão Geográfica
- [ ] Ampliar a cobertura para **todas as regiões do Estado de São Paulo** (demais zonas da capital + interior)
- [ ] Adicionar filtro por Seccional e Departamento policial
- [ ] Implementar drill-down geográfico: Estado → Município → Bairro
- [ ] Adicionar camada de dados populacionais (IBGE) para calcular taxa de ocorrências per capita por município

### 🤖 Automação de Dados
- [x] Pipeline modular (download → Excel→CSV → limpeza → validação → Parquet) configurável pelo README
- [x] Execução agendada via **GitHub Actions** (cron + disparo manual) com commit automático do Parquet
- [ ] Descoberta automática da URL do XLSX no portal JS da SSP-SP (scraping) — hoje a URL é declarada no README
- [ ] Pipeline de atualização incremental: processar apenas os novos registros em vez de reingerir tudo
- [ ] Notificação automática (e-mail / webhook) quando novos dados forem disponibilizados pela SSP-SP

### 🧹 Pré-limpeza Automatizada
- [ ] Detecção e tratamento automático de encoding (UTF-8 / Latin-1 / CP1252) sem flag manual
- [ ] Normalização automática de nomes de municípios (ex: `SAO PAULO` vs `SÃO PAULO` vs `S. PAULO`)
- [ ] Validação de schema no momento da ingestão com relatório de qualidade de dados (% nulos por coluna, outliers de coordenadas, etc.)
- [ ] Remoção automática de colunas com mais de X% de valores nulos (configurável)

### 🎨 Aparência e Acessibilidade
- [x] Redesign minimalista monocromático (preto & branco) com tema customizado no Streamlit
- [x] Melhor contraste e tamanho de fonte para acessibilidade + foco visível (rumo a WCAG 2.1 AA)
- [x] Layout responsivo (desktop + mobile)
- [ ] Implementar modo escuro (dark mode)
- [ ] Adicionar tooltips explicativos em cada gráfico (KPI cards já têm `help`)
- [ ] Adicionar suporte a português para todos os rótulos do dashboard (atualmente em inglês)
- [ ] Incluir seção "Como ler este dashboard" para usuários não-técnicos

---

## 👤 Autor

Desenvolvido por **Gustavo** como parte do portfólio de Data Analytics.

🐙 [GitHub](https://github.com/gustavo-almeidalopes/) · 💼 [LinkedIn]([https://linkedin.com/in/seu-perfil](https://www.linkedin.com/in/gustavorobertodealmeidalopes/))

---

## 📄 Licença

Distribuído sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para mais detalhes.

---

<div align="center">
  <sub>Dados públicos utilizados exclusivamente para fins analíticos e educacionais · SSP-SP</sub>
</div>
