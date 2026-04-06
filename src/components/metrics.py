"""
KPI metric card row.

The aggregation query is cached by (where_clause) so changing filters
re-runs it exactly once, then subsequent renders hit the cache.
"""

from __future__ import annotations

import duckdb
import streamlit as st


@st.cache_data(ttl=3600, show_spinner=False)
def _query_metrics(_con: duckdb.DuckDBPyConnection, where: str) -> tuple:
    return _con.execute(f"""
        SELECT
            COUNT(*)                                                        AS total,
            COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%'  THEN 1 END), 0)  AS furtos,
            COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%'  THEN 1 END), 0)  AS roubos,
            COUNT(DISTINCT NOME_MUNICIPIO)                                  AS municipios,
            COUNT(DISTINCT NOME_DELEGACIA)                                  AS delegacias
        FROM crimes
        {where}
    """).fetchone()


def render_metric_cards(con: duckdb.DuckDBPyConnection, where: str) -> None:
    total, furtos, roubos, n_mun, n_del = (int(v) for v in _query_metrics(con, where))
    outros = total - furtos - roubos

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Occurrences",    f"{total:,}")
    c2.metric("Thefts (Furtos)",      f"{furtos:,}")
    c3.metric("Robberies (Roubos)",   f"{roubos:,}")
    c4.metric("Other Crimes",         f"{outros:,}")
    c5.metric("Precincts / Municipalities", f"{n_del:,} / {n_mun:,}")
