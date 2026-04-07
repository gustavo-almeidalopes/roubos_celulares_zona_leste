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

# ── Brutalist theme (inspirado no index.html / Radar Celular SP) ──────────────
st.markdown(
    """
<link href="https://fonts.googleapis.com/css2?family=Archivo+Black&family=Public+Sans:wght@400;700;900&display=swap" rel="stylesheet">
<style>
    :root {
        --brutal-bg:     #E5E5E5;
        --brutal-card:   #FFFFFF;
        --brutal-ink:    #000000;
        --brutal-muted:  #404040;
        --brutal-soft:   #A3A3A3;
        --brutal-accent: #DC2626;
    }

    /* Base */
    html, body, [class*="css"], .stApp {
        font-family: 'Public Sans', sans-serif !important;
        background-color: var(--brutal-bg) !important;
        color: var(--brutal-ink) !important;
    }
    .block-container { padding-top: 1rem; padding-bottom: 2rem; max-width: 1500px; }

    /* Headings — Archivo Black */
    h1, h2, h3, h4, .font-brutal {
        font-family: 'Archivo Black', sans-serif !important;
        color: var(--brutal-ink) !important;
        letter-spacing: -0.5px;
    }
    h1 { font-size: 2.6rem !important; line-height: 1 !important; }

    /* Header bar */
    .brutal-header {
        background: var(--brutal-ink);
        color: #fff;
        padding: 1.2rem 1.4rem;
        border: 3px solid #000;
        box-shadow: 6px 6px 0 0 #000;
        margin-bottom: 1.4rem;
    }
    .brutal-header h1 { color: #fff !important; margin: 0 !important; }
    .brutal-header .tag {
        display: inline-block;
        background: var(--brutal-accent);
        color: #fff;
        padding: 2px 10px;
        font-family: 'Archivo Black', sans-serif;
        font-size: 0.75rem;
        margin-left: 8px;
        border: 2px solid #fff;
        vertical-align: middle;
    }
    .brutal-header p { margin: 6px 0 0 0; color: #A3A3A3; font-size: 0.85rem; }

    /* Metric cards — caixinhas brutalistas */
    [data-testid="stMetric"] {
        background: var(--brutal-card) !important;
        border: 3px solid #000 !important;
        box-shadow: 5px 5px 0 0 #000 !important;
        padding: 14px 16px !important;
        border-radius: 0 !important;
    }
    [data-testid="stMetricLabel"] {
        font-family: 'Archivo Black', sans-serif !important;
        text-transform: uppercase;
        font-size: 0.7rem !important;
        color: var(--brutal-muted) !important;
    }
    [data-testid="stMetricValue"] {
        font-family: 'Archivo Black', sans-serif !important;
        color: #000 !important;
        font-size: 1.8rem !important;
    }

    /* Plotly chart wrapper → caixa brutalista */
    [data-testid="stPlotlyChart"] {
        background: #fff;
        border: 3px solid #000;
        box-shadow: 5px 5px 0 0 #000;
        padding: 8px;
        margin-bottom: 8px;
    }

    /* Dividers — linha preta sólida */
    hr, [data-testid="stDivider"] {
        border: none !important;
        border-top: 3px solid #000 !important;
        margin: 1.6rem 0 !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: var(--brutal-card) !important;
        border-right: 3px solid #000 !important;
    }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] .stMarkdown {
        font-family: 'Archivo Black', sans-serif !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        border: 2px solid #000 !important;
        border-radius: 0 !important;
        box-shadow: 3px 3px 0 0 #000;
        background: #fff !important;
    }

    /* Botões */
    .stButton > button, .stDownloadButton > button {
        background: #000 !important;
        color: #fff !important;
        border: 3px solid #000 !important;
        border-radius: 0 !important;
        font-family: 'Archivo Black', sans-serif !important;
        text-transform: uppercase;
        box-shadow: 4px 4px 0 0 #000;
        transition: transform .05s ease;
    }
    .stButton > button:hover, .stDownloadButton > button:hover {
        background: var(--brutal-accent) !important;
        border-color: #000 !important;
        transform: translate(-1px, -1px);
        box-shadow: 5px 5px 0 0 #000;
    }

    /* Expanders */
    [data-testid="stExpander"] {
        background: #fff !important;
        border: 3px solid #000 !important;
        border-radius: 0 !important;
        box-shadow: 5px 5px 0 0 #000;
        margin-bottom: 1rem;
    }
    [data-testid="stExpander"] summary {
        font-family: 'Archivo Black', sans-serif !important;
        text-transform: uppercase;
    }

    /* Alerts */
    [data-testid="stAlert"] {
        border: 3px solid #000 !important;
        border-radius: 0 !important;
        box-shadow: 4px 4px 0 0 #000;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] {
        border: 3px solid #000;
        box-shadow: 5px 5px 0 0 #000;
    }

    /* Caption */
    .stCaption, [data-testid="stCaptionContainer"] {
        color: var(--brutal-muted) !important;
        font-weight: 700;
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
        '<div class="brutal-header"><h1>◼ RADAR SEGURANÇA SP'
        '<span class="tag">CAPITAL</span></h1>'
        '<p>Dados não encontrados — siga os passos abaixo</p></div>',
        unsafe_allow_html=True,
    )
    st.error("**Nenhum dado processado encontrado.**")
    st.markdown("### 1 — Coloque o CSV bruto")
    st.code("src/data/raw/ocorrencias_full.csv", language="text")
    st.markdown("### 2 — Rode o pipeline")
    st.code(
        "python -m src.data_pipeline.clean_ingest \\\n"
        "    --input src/data/raw/ocorrencias_full.csv",
        language="bash",
    )
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
    <div class="brutal-header">
        <h1>◼ RADAR SEGURANÇA SP <span class="tag">CAPITAL</span></h1>
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
