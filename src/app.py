"""
SP Public Safety Dashboard — main Streamlit application.

How to run (local)
------------------
    streamlit run src/app.py

How to deploy on streamlit.io
------------------------------
1. Run the pipeline once locally to generate the Parquet file:

       python -m src.data_pipeline.clean_ingest \\
           --input  src/data/raw/YOUR_FILE.csv

2. Commit  src/data/processed/cleaned_data.parquet  to your git repo.
   (If the file exceeds 100 MB, use Git LFS:  git lfs track "*.parquet")

3. Push to GitHub and connect the repo in https://share.streamlit.io

Architecture
------------
                  raw CSV (put in src/data/raw/)
                       │
              clean_ingest.py          ← run ONCE from terminal
                       │
              cleaned_data.parquet     ← commit this to git
                       │
       DuckDB in-memory VIEW (read_parquet)
                       │  SQL GROUP BY → tiny DataFrames only
              components/              ← charts, filters, metrics
                       │
              Streamlit widgets
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
    render_top_municipios,
    render_top_rubricas,
    render_yoy_comparison,
)
from components.filters import build_where_clause, render_sidebar_filters
from components.metrics import render_metric_cards

# ── Paths ─────────────────────────────────────────────────────────────────────
PARQUET_PATH = _SRC / "data" / "processed" / "cleaned_data.parquet"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SP Public Safety Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.2rem; }
    [data-testid="stMetric"] {
        border-left: 4px solid #636EFA;
        padding-left: 0.6rem;
        background: #f8f9fb;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)


# ── DuckDB connection (one per session, cached) ───────────────────────────────
@st.cache_resource(show_spinner=False)
def _get_connection(parquet_path: str) -> duckdb.DuckDBPyConnection | None:
    """Open an in-memory DuckDB and expose the Parquet as a view named `crimes`.

    Using a VIEW instead of COPY INTO TABLE means DuckDB streams columns
    from the Parquet file on demand — query results are materialised but the
    full 1M+ row dataset never sits in RAM all at once.
    """
    p = Path(parquet_path)
    if not p.exists():
        return None
    con = duckdb.connect()                      # pure in-memory, no .duckdb file
    con.execute(
        f"CREATE VIEW crimes AS SELECT * FROM read_parquet('{p.as_posix()}')"
    )
    return con


# ── Setup / error screen ──────────────────────────────────────────────────────
def _show_setup_screen() -> None:
    st.title("🔍 SP Public Safety Dashboard")
    st.error("**No processed data found.** Follow the steps below to get started.")

    st.markdown("### Step 1 — Place the raw CSV")
    st.code(
        "src/\n"
        "└── data/\n"
        "    └── raw/\n"
        "        └── ocorrencias_full.csv   ← put your SSP-SP CSV here",
        language="text",
    )
    st.caption(
        "The file must be semicolon-delimited (`;`). "
        "Any SSP-SP export downloaded from https://www.ssp.sp.gov.br works."
    )

    st.markdown("### Step 2 — Install dependencies (once)")
    st.code("pip install -r requirements.txt", language="bash")

    st.markdown("### Step 3 — Run the ingestion pipeline (once)")
    st.code(
        "# From the project root:\n"
        "python -m src.data_pipeline.clean_ingest \\\n"
        "    --input  src/data/raw/ocorrencias_full.csv\n\n"
        "# Output goes to  src/data/processed/cleaned_data.parquet\n"
        "# Progress is printed per chunk (50 000 rows each)",
        language="bash",
    )
    st.info(
        "The pipeline reads the CSV in 50 000-row chunks so it never loads the "
        "full file into RAM. A 1M-row file typically takes 30–90 seconds."
    )

    st.markdown("### Step 4 — Launch the dashboard")
    st.code("streamlit run src/app.py", language="bash")

    st.markdown("### Deploying on streamlit.io")
    st.markdown(
        "1. Commit `src/data/processed/cleaned_data.parquet` to your GitHub repo.  \n"
        "   *(If the file is > 100 MB use Git LFS: `git lfs track '*.parquet'`)*  \n"
        "2. Go to https://share.streamlit.io → **New app** → point to `src/app.py`.  \n"
        "3. The app reads the Parquet directly — no database server required."
    )

    st.markdown("### Optional flags for `clean_ingest.py`")
    st.code(
        "python -m src.data_pipeline.clean_ingest --help\n\n"
        "  --input        Path to raw CSV           (required)\n"
        "  --output-dir   Where to write output     (default: src/data/processed)\n"
        "  --chunk-size   Rows per chunk            (default: 50000)\n"
        "  --sep          CSV delimiter             (default: ;)\n"
        "  --encoding     File encoding             (default: utf-8, try latin-1)",
        language="text",
    )


# ── Main ──────────────────────────────────────────────────────────────────────
con = _get_connection(str(PARQUET_PATH))

if con is None:
    _show_setup_screen()
    st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔍 SP Public Safety Dashboard")
st.caption(
    "Source: SSP-SP — Secretaria da Segurança Pública do Estado de São Paulo  "
    "· Powered by DuckDB + Streamlit"
)

# ── Sidebar filters ───────────────────────────────────────────────────────────
filters = render_sidebar_filters(con)
where   = build_where_clause(filters)

if where:
    parts = []
    if filters["years"]:
        parts.append(f"Years: {', '.join(str(y) for y in sorted(filters['years']))}")
    if filters["delegacias"]:
        parts.append(f"{len(filters['delegacias'])} precinct(s)")
    if filters["municipios"]:
        parts.append(f"{len(filters['municipios'])} municipality(ies)")
    st.sidebar.caption("Active: " + " · ".join(parts))

# ── KPI row ───────────────────────────────────────────────────────────────────
render_metric_cards(con, where)

st.divider()

# ── Row 1: Top crime types + category donut ───────────────────────────────────
col1, col2 = st.columns([2, 1])
with col1:
    render_top_rubricas(con, where)
with col2:
    render_crime_category_pie(con, where)

st.divider()

# ── Row 2: Monthly trend ──────────────────────────────────────────────────────
render_monthly_trend(con, where)

st.divider()

# ── Row 3: Top municipalities + YoY bars ─────────────────────────────────────
col3, col4 = st.columns(2)
with col3:
    render_top_municipios(con, where)
with col4:
    render_yoy_comparison(con, where)

st.divider()

# ── Row 4: Day × period heatmap ───────────────────────────────────────────────
render_heatmap_period(con, where)

st.divider()

# ── Expandable sections ───────────────────────────────────────────────────────
with st.expander("🗺 Incident Map (geo-coded records only)", expanded=False):
    st.caption(
        "Random sample of up to 5 000 geo-coded records. "
        "Apply filters above to zoom in on an area."
    )
    render_crime_map(con, where)

with st.expander("📋 Raw Data Preview (200 rows)", expanded=False):
    preview = con.execute(f"SELECT * FROM crimes {where} LIMIT 200").df()
    st.dataframe(preview, use_container_width=True, hide_index=True)

with st.expander("⬇ Download Filtered Data as CSV", expanded=False):
    st.warning(
        "Exporting a large filtered result set may take a moment. "
        "Narrow the filters first to reduce the download size."
    )
    if st.button("Prepare download"):
        with st.spinner("Querying …"):
            dl_df = con.execute(f"SELECT * FROM crimes {where}").df()
        st.download_button(
            label=f"Download {len(dl_df):,} rows (.csv)",
            data=dl_df.to_csv(index=False, sep=";").encode("utf-8"),
            file_name="filtered_crimes.csv",
            mime="text/csv",
        )
