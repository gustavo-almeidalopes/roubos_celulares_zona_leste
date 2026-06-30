"""
SP Public Safety Dashboard — São Paulo (Capital)
================================================

Filtra automaticamente APENAS ocorrências do município de São Paulo (capital)
e aplica um visual "brutalista" inspirado no protótipo Radar Celular SP
(fundo cinza claro, bordas pretas grossas, sombras duras, tipografia pesada).

Como rodar
----------
    streamlit run src/app.py

Pipeline (rodar uma vez antes):
    python -m src.data_pipeline.clean_ingest --input src/data/raw/SEU_ARQUIVO.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import duckdb
import streamlit as st

from components.charts import (
    render_crime_category_pie,
    render_crime_map,
    render_heatmap_period,
    render_monthly_trend,
    render_top_municipios,  # reaproveitado p/ Top Bairros
    render_top_rubricas,
    render_yoy_comparison,
)
from components.filters import build_where_clause, render_sidebar_filters
from components.metrics import render_metric_cards

# ── Paths ─────────────────────────────────────────────────────────────────────
PARQUET_PATH = _SRC / "data" / "processed" / "cleaned_data.parquet"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Radar Segurança SP — Capital",
    page_icon="◼",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Tema minimalista monocromático (preto & branco, analytics moderno) ────────
st.markdown(
    """
<link href="https://fonts.googleapis.com/css2?family=Public+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
    :root {
        --bg:            #FAFAFA;
        --surface:       #FFFFFF;
        --ink:           #0A0A0A;
        --text:          #1A1A1A;
        --muted:         #6B7280;
        --border:        #E5E7EB;
        --border-strong: #D1D5DB;
        --hover:         #F3F4F6;
    }

    /* Base */
    html, body, [class*="css"], .stApp {
        font-family: 'Public Sans', system-ui, -apple-system, sans-serif !important;
        background-color: var(--bg) !important;
        color: var(--text) !important;
        font-size: 15px;
    }
    .block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1440px; }

    /* Headings */
    h1, h2, h3, h4 {
        font-family: 'Public Sans', sans-serif !important;
        color: var(--ink) !important;
        font-weight: 700;
        letter-spacing: -0.01em;
    }
    h1 { font-size: 1.9rem !important; line-height: 1.15 !important; }

    /* Header limpo */
    .app-header {
        padding: 0 0 1.2rem 0;
        border-bottom: 1px solid var(--border);
        margin-bottom: 1.6rem;
    }
    .app-header h1 {
        margin: 0 !important;
        display: flex; align-items: center; gap: .6rem; flex-wrap: wrap;
    }
    .app-header .badge {
        font-size: .62rem; letter-spacing: .08em; text-transform: uppercase;
        background: var(--ink); color: #fff; padding: 3px 9px;
        border-radius: 999px; font-weight: 700;
    }
    .app-header p { margin: .45rem 0 0 0; color: var(--muted); font-size: .85rem; }

    /* Metric cards — limpos, hairline */
    [data-testid="stMetric"] {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        padding: 1rem 1.1rem !important;
        box-shadow: 0 1px 2px rgba(0,0,0,.04) !important;
    }
    [data-testid="stMetricLabel"] {
        text-transform: uppercase; letter-spacing: .05em;
        font-size: .68rem !important; font-weight: 600;
        color: var(--muted) !important;
    }
    [data-testid="stMetricValue"] {
        color: var(--ink) !important;
        font-size: 1.7rem !important; font-weight: 700;
    }
    /* Deltas em escala de cinza (sem verde/vermelho) — identidade B&W */
    [data-testid="stMetricDelta"] { color: var(--muted) !important; }
    [data-testid="stMetricDelta"] svg {
        stroke: var(--muted) !important; fill: var(--muted) !important;
    }

    /* Plotly chart wrapper */
    [data-testid="stPlotlyChart"] {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: .6rem .6rem .2rem;
        box-shadow: 0 1px 2px rgba(0,0,0,.04);
        margin-bottom: .4rem;
    }

    /* Dividers — hairline */
    hr, [data-testid="stDivider"] {
        border: none !important;
        border-top: 1px solid var(--border) !important;
        margin: 1.4rem 0 !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: var(--surface) !important;
        border-right: 1px solid var(--border) !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        border: 1px solid var(--border-strong) !important;
        border-radius: 8px !important;
        background: var(--surface) !important;
    }

    /* Botões */
    .stButton > button, .stDownloadButton > button {
        background: var(--ink) !important;
        color: #fff !important;
        border: 1px solid var(--ink) !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        box-shadow: none !important;
        transition: background .15s ease, opacity .15s ease;
    }
    .stButton > button:hover, .stDownloadButton > button:hover {
        background: #262626 !important;
        border-color: #262626 !important;
    }

    /* Expanders */
    [data-testid="stExpander"] {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        box-shadow: 0 1px 2px rgba(0,0,0,.04);
        margin-bottom: .8rem;
    }
    [data-testid="stExpander"] summary { font-weight: 600 !important; }

    /* Alerts */
    [data-testid="stAlert"] {
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        box-shadow: none !important;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--border);
        border-radius: 10px;
    }

    /* Caption */
    .stCaption, [data-testid="stCaptionContainer"] {
        color: var(--muted) !important;
        font-weight: 500;
    }

    /* Acessibilidade — foco visível */
    :focus-visible { outline: 2px solid var(--ink); outline-offset: 2px; }

    /* Responsivo (mobile) */
    @media (max-width: 768px) {
        .block-container { padding-left: .6rem; padding-right: .6rem; padding-top: 1rem; }
        h1 { font-size: 1.5rem !important; }
        [data-testid="stMetricValue"] { font-size: 1.35rem !important; }
        [data-testid="stMetricLabel"] { font-size: .62rem !important; }
    }
</style>
    """,
    unsafe_allow_html=True,
)


# ── DuckDB connection — JÁ FILTRA SÃO PAULO CAPITAL ───────────────────────────
# A view `crimes` expõe APENAS o município de São Paulo (capital). Como todas
# as queries dos componentes consultam `crimes`, o filtro vale para o app
# inteiro automaticamente — não há como "escapar" dele acidentalmente.
SP_CAPITAL_FILTER = """
    -- TODO: avaliar incluir todos os bairros da Zona Leste de SP
    UPPER(TRIM(NOME_MUNICIPIO)) IN (
        'S.PAULO', 'SAO PAULO', 'SÃO PAULO', 'S. PAULO', 'SP'
    )
"""

@st.cache_resource(show_spinner=False)
def _get_connection(parquet_path: str) -> duckdb.DuckDBPyConnection | None:
    p = Path(parquet_path)
    if not p.exists():
        return None
    con = duckdb.connect()
    con.execute(f"""
        CREATE VIEW crimes AS
        SELECT *
        FROM read_parquet('{p.as_posix()}')
        WHERE {SP_CAPITAL_FILTER}
    """)
    return con


# ── Setup screen ──────────────────────────────────────────────────────────────
def _show_setup_screen() -> None:
    st.markdown(
        '<div class="app-header"><h1>Radar Segurança SP'
        '<span class="badge">Capital</span></h1>'
        '<p>Dados não encontrados — siga os passos abaixo</p></div>',
        unsafe_allow_html=True,
    )
    st.error("**Nenhum dado processado encontrado.**")
    st.markdown("### 1 — Coloque o arquivo bruto (XLS/XLSX/CSV)")
    st.code("src/data/raw/celulares_subtraidos.xlsx", language="text")
    st.markdown("### 2 — Rode o pipeline")
    st.code("python -m src.data_pipeline run", language="bash")
    st.markdown("### 3 — Inicie o app")
    st.code("streamlit run src/app.py", language="bash")


# ── Main ──────────────────────────────────────────────────────────────────────
con = _get_connection(str(PARQUET_PATH))

if con is None:
    _show_setup_screen()
    st.stop()

# Sanidade: quantas linhas sobraram após filtro de SP capital?
total_sp = con.execute("SELECT COUNT(*) FROM crimes").fetchone()[0]

# ── Header brutalista ─────────────────────────────────────────────────────────
st.markdown(
    f'''
    <div class="app-header">
        <h1>Radar Segurança SP <span class="badge">Capital</span></h1>
        <p>Fonte: SSP-SP · {total_sp:,} ocorrências no município de São Paulo · DuckDB + Streamlit</p>
    </div>
    '''.replace(",", "."),
    unsafe_allow_html=True,
)

if total_sp == 0:
    st.error(
        "O filtro de São Paulo capital retornou **0 linhas**. "
        "Verifique se o campo `NOME_MUNICIPIO` no seu Parquet usa um dos formatos: "
        "`S.PAULO`, `SAO PAULO`, `SÃO PAULO`."
    )
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.markdown("## ◼ FILTROS")
filters = render_sidebar_filters(con)
# Trava o filtro de município — só SP capital existe nessa view
filters["municipios"] = []
where = build_where_clause(filters)

if filters.get("years") or filters.get("delegacias"):
    parts = []
    if filters["years"]:
        parts.append(f"Anos: {', '.join(str(y) for y in sorted(filters['years']))}")
    if filters["delegacias"]:
        parts.append(f"{len(filters['delegacias'])} delegacia(s)")
    st.sidebar.caption("Ativo: " + " · ".join(parts))

st.sidebar.markdown("---")
st.sidebar.caption("📍 Escopo fixo: **São Paulo / Capital**")

# ── KPI row ───────────────────────────────────────────────────────────────────
render_metric_cards(con, where)

st.divider()

# ── Row 1 ─────────────────────────────────────────────────────────────────────
col1, col2 = st.columns([2, 1])
with col1:
    render_top_rubricas(con, where)
with col2:
    render_crime_category_pie(con, where)

st.divider()

# ── Row 2 ─────────────────────────────────────────────────────────────────────
render_monthly_trend(con, where)

st.divider()

# ── Row 3 ─────────────────────────────────────────────────────────────────────
col3, col4 = st.columns(2)
with col3:
    # Como só temos SP capital, "top municípios" vira efetivamente "top bairros"
    # se o componente agrupar por NOME_MUNICIPIO ele mostrará apenas SP — então
    # mantemos o YoY ao lado pra balancear a linha.
    render_top_municipios(con, where)
with col4:
    render_yoy_comparison(con, where)

st.divider()

# ── Row 4 ─────────────────────────────────────────────────────────────────────
render_heatmap_period(con, where)

st.divider()

# ── Expanders ─────────────────────────────────────────────────────────────────
with st.expander("🗺 MAPA DE INCIDENTES (registros geocodificados)", expanded=False):
    st.caption("Amostra de até 5.000 registros. Use os filtros pra recortar áreas.")
    render_crime_map(con, where)

with st.expander("📋 PRÉVIA DOS DADOS (200 linhas)", expanded=False):
    preview = con.execute(f"SELECT * FROM crimes {where} LIMIT 200").df()
    st.dataframe(preview, use_container_width=True, hide_index=True)

with st.expander("⬇ EXPORTAR CSV", expanded=False):
    st.warning("Filtre antes de exportar pra reduzir o tamanho do download.")
    if st.button("PREPARAR DOWNLOAD"):
        with st.spinner("Consultando…"):
            dl_df = con.execute(f"SELECT * FROM crimes {where}").df()
        st.download_button(
            label=f"BAIXAR {len(dl_df):,} LINHAS (.CSV)".replace(",", "."),
            data=dl_df.to_csv(index=False, sep=";").encode("utf-8"),
            file_name="ocorrencias_sp_capital.csv",
            mime="text/csv",
        )
