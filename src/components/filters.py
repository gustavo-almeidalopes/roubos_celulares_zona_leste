"""
Sidebar filter widgets and SQL WHERE-clause builder.

Design notes
------------
* Distinct-value queries use @st.cache_data so the Parquet file is scanned
  only once per session — subsequent renders hit the in-memory cache.
* The `_con` parameter (leading underscore) tells Streamlit's cache NOT to
  hash the DuckDB connection object, which is not hashable.
* build_where_clause() produces safe SQL literals because all values come
  from DuckDB's own DISTINCT queries, never from free-form user input.
"""

from __future__ import annotations

import duckdb
import streamlit as st


# ── Cached distinct-value loaders ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_years(_con: duckdb.DuckDBPyConnection) -> list[int]:
    return (
        _con.execute(
            "SELECT DISTINCT CAST(ANO AS INTEGER) AS ANO "
            "FROM crimes WHERE ANO IS NOT NULL ORDER BY ANO DESC"
        )
        .df()["ANO"]
        .tolist()
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_delegacias(_con: duckdb.DuckDBPyConnection) -> list[str]:
    return (
        _con.execute(
            "SELECT DISTINCT NOME_DELEGACIA FROM crimes "
            "WHERE NOME_DELEGACIA IS NOT NULL ORDER BY 1"
        )
        .df()["NOME_DELEGACIA"]
        .tolist()
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_municipios(_con: duckdb.DuckDBPyConnection) -> list[str]:
    return (
        _con.execute(
            "SELECT DISTINCT NOME_MUNICIPIO FROM crimes "
            "WHERE NOME_MUNICIPIO IS NOT NULL ORDER BY 1"
        )
        .df()["NOME_MUNICIPIO"]
        .tolist()
    )


# ── Sidebar render ─────────────────────────────────────────────────────────────

_MONTH_LABELS = {
    1: "Jan", 2: "Feb", 3: "Mar",  4: "Apr",
    5: "May", 6: "Jun", 7: "Jul",  8: "Aug",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


def render_sidebar_filters(con: duckdb.DuckDBPyConnection) -> dict:
    """Render all sidebar filters and return the selected values as a dict."""
    st.sidebar.title("Filters")

    years = _distinct_years(con)
    selected_years: list[int] = st.sidebar.multiselect(
        "Year",
        options=years,
        default=years[:3] if len(years) >= 3 else years,
    )

    selected_months: list[int] = st.sidebar.multiselect(
        "Month",
        options=list(_MONTH_LABELS.keys()),
        default=list(_MONTH_LABELS.keys()),
        format_func=lambda m: _MONTH_LABELS[m],
    )

    delegacias = _distinct_delegacias(con)
    selected_delegacias: list[str] = st.sidebar.multiselect(
        "Precinct (Delegacia)",
        options=delegacias,
        default=[],
        placeholder="All precincts",
    )

    municipios = _distinct_municipios(con)
    selected_municipios: list[str] = st.sidebar.multiselect(
        "Municipality",
        options=municipios,
        default=[],
        placeholder="All municipalities",
    )

    return {
        "years":      selected_years,
        "months":     selected_months,
        "delegacias": selected_delegacias,
        "municipios": selected_municipios,
    }


# ── WHERE clause builder ───────────────────────────────────────────────────────

def build_where_clause(filters: dict) -> str:
    """Convert the filter dict into a SQL WHERE clause string.

    Returns ``""`` when nothing is filtered (selects all rows).
    """
    clauses: list[str] = []

    if filters.get("years"):
        vals = ", ".join(str(int(y)) for y in filters["years"])
        clauses.append(f"CAST(ANO AS INTEGER) IN ({vals})")

    if filters.get("months"):
        vals = ", ".join(str(int(m)) for m in filters["months"])
        clauses.append(f"CAST(MES AS INTEGER) IN ({vals})")

    if filters.get("delegacias"):
        escaped = ", ".join(
            "'" + d.replace("'", "''") + "'" for d in filters["delegacias"]
        )
        clauses.append(f"NOME_DELEGACIA IN ({escaped})")

    if filters.get("municipios"):
        escaped = ", ".join(
            "'" + m.replace("'", "''") + "'" for m in filters["municipios"]
        )
        clauses.append(f"NOME_MUNICIPIO IN ({escaped})")

    return ("WHERE " + " AND ".join(clauses)) if clauses else ""
