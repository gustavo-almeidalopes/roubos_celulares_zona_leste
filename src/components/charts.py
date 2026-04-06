"""
Plotly chart builders — each function runs one DuckDB aggregation query
and renders a Plotly figure.

All heavy queries are wrapped in @st.cache_data keyed on (where_clause),
so changing a filter re-runs the query once and subsequent re-renders are
served from cache.  The `_con` parameter uses the leading-underscore
convention to opt out of Streamlit's hashing for the connection object.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ── 1. Top crime types (RUBRICA) — horizontal bar ─────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_top_rubricas(_con, where: str, top_n: int) -> pd.DataFrame:
    return _con.execute(f"""
        SELECT RUBRICA, COUNT(*) AS occurrences
        FROM crimes {where}
        {"AND" if where else "WHERE"} RUBRICA IS NOT NULL
        GROUP BY RUBRICA
        ORDER BY occurrences DESC
        LIMIT {top_n}
    """).df()


def render_top_rubricas(
    con: duckdb.DuckDBPyConnection, where: str, top_n: int = 15
) -> None:
    df = _q_top_rubricas(con, where, top_n)
    if df.empty:
        st.info("No data for the selected filters.")
        return
    fig = px.bar(
        df, x="occurrences", y="RUBRICA", orientation="h",
        title=f"Top {top_n} Crime Types (RUBRICA)",
        labels={"occurrences": "Occurrences", "RUBRICA": ""},
        color="occurrences", color_continuous_scale="Reds",
        text="occurrences",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        height=480,
        yaxis={"categoryorder": "total ascending"},
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── 2. Monthly trend — multi-year line chart ──────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_monthly_trend(_con, where: str) -> pd.DataFrame:
    extra = "AND" if where else "WHERE"
    return _con.execute(f"""
        SELECT
            CAST(ANO AS INTEGER) AS ANO,
            CAST(MES AS INTEGER) AS MES,
            COUNT(*) AS occurrences
        FROM crimes {where}
        {extra} ANO IS NOT NULL AND MES IS NOT NULL
        GROUP BY ANO, MES
        ORDER BY ANO, MES
    """).df()


def render_monthly_trend(con: duckdb.DuckDBPyConnection, where: str) -> None:
    df = _q_monthly_trend(con, where)
    if df.empty:
        st.info("No temporal data for the selected filters.")
        return
    df["period"] = df["MES"].astype(str).str.zfill(2)
    fig = px.line(
        df, x="period", y="occurrences", color="ANO",
        title="Monthly Occurrence Trend by Year",
        labels={"period": "Month", "occurrences": "Occurrences", "ANO": "Year"},
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )
    fig.update_xaxes(
        tickmode="array",
        tickvals=[str(m).zfill(2) for m in range(1, 13)],
        ticktext=["Jan","Feb","Mar","Apr","May","Jun",
                  "Jul","Aug","Sep","Oct","Nov","Dec"],
    )
    st.plotly_chart(fig, use_container_width=True)


# ── 3. Crime category — donut chart ───────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_category_pie(_con, where: str) -> pd.DataFrame:
    return _con.execute(f"""
        SELECT
            CASE
                WHEN RUBRICA LIKE 'FURTO%' THEN 'Furto (Theft)'
                WHEN RUBRICA LIKE 'ROUBO%' THEN 'Roubo (Robbery)'
                ELSE 'Other'
            END AS category,
            COUNT(*) AS occurrences
        FROM crimes {where}
        GROUP BY category
        ORDER BY occurrences DESC
    """).df()


def render_crime_category_pie(con: duckdb.DuckDBPyConnection, where: str) -> None:
    df = _q_category_pie(con, where)
    if df.empty:
        st.info("No data for the selected filters.")
        return
    fig = px.pie(
        df, names="category", values="occurrences",
        title="Crime Category Breakdown",
        color_discrete_map={
            "Furto (Theft)":   "#EF553B",
            "Roubo (Robbery)": "#636EFA",
            "Other":           "#00CC96",
        },
        hole=0.45,
    )
    fig.update_traces(textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)


# ── 4. Top municipalities — vertical bar chart ────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_top_municipios(_con, where: str, top_n: int) -> pd.DataFrame:
    return _con.execute(f"""
        SELECT NOME_MUNICIPIO AS municipio, COUNT(*) AS occurrences
        FROM crimes {where}
        {"AND" if where else "WHERE"} NOME_MUNICIPIO IS NOT NULL
        GROUP BY NOME_MUNICIPIO
        ORDER BY occurrences DESC
        LIMIT {top_n}
    """).df()


def render_top_municipios(
    con: duckdb.DuckDBPyConnection, where: str, top_n: int = 15
) -> None:
    df = _q_top_municipios(con, where, top_n)
    if df.empty:
        st.info("No municipality data for the selected filters.")
        return
    fig = px.bar(
        df, x="municipio", y="occurrences",
        title=f"Top {top_n} Municipalities by Occurrences",
        labels={"municipio": "Municipality", "occurrences": "Occurrences"},
        color="occurrences", color_continuous_scale="Blues",
        text="occurrences",
    )
    fig.update_traces(textposition="outside")
    fig.update_xaxes(tickangle=40)
    fig.update_layout(height=420, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)


# ── 5. Day-of-week × period heatmap ───────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_heatmap(_con, where: str) -> pd.DataFrame:
    extra = "AND" if where else "WHERE"
    return _con.execute(f"""
        SELECT
            DAYOFWEEK(TRY_CAST(DATA_OCORRENCIA_BO AS TIMESTAMP)) AS dow,
            DESCR_PERIODO,
            COUNT(*) AS occurrences
        FROM crimes {where}
        {extra} DATA_OCORRENCIA_BO IS NOT NULL AND DESCR_PERIODO IS NOT NULL
        GROUP BY dow, DESCR_PERIODO
    """).df()


def render_heatmap_period(con: duckdb.DuckDBPyConnection, where: str) -> None:
    df = _q_heatmap(con, where)
    if df.empty:
        st.info("No temporal data for the selected filters.")
        return
    _dow = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
    df["day"] = df["dow"].map(_dow)
    pivot = df.pivot_table(
        index="DESCR_PERIODO", columns="day",
        values="occurrences", fill_value=0, aggfunc="sum",
    )
    day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    pivot = pivot.reindex(columns=[d for d in day_order if d in pivot.columns])
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values.tolist(),
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale="YlOrRd",
        hoverongaps=False,
    ))
    fig.update_layout(
        title="Occurrences by Day of Week × Period of Day",
        height=320,
        xaxis_title="Day",
        yaxis_title="Period",
    )
    st.plotly_chart(fig, use_container_width=True)


# ── 6. Geo scatter map (sampled) ──────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_map(_con, where: str, max_points: int) -> pd.DataFrame:
    extra = "AND" if where else "WHERE"
    return _con.execute(f"""
        SELECT LATITUDE, LONGITUDE, RUBRICA, NOME_MUNICIPIO, BAIRRO
        FROM crimes {where}
        {extra} LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        USING SAMPLE {max_points} ROWS
    """).df()


def render_crime_map(
    con: duckdb.DuckDBPyConnection, where: str, max_points: int = 5_000
) -> None:
    df = _q_map(con, where, max_points)
    if df.empty:
        st.info("No geo-coded records for the selected filters.")
        return
    fig = px.scatter_mapbox(
        df,
        lat="LATITUDE", lon="LONGITUDE",
        color="RUBRICA",
        hover_data=["NOME_MUNICIPIO", "BAIRRO", "RUBRICA"],
        title=f"Incident Map — {len(df):,} sampled geo-coded records",
        zoom=9, height=520, opacity=0.6,
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        legend={"title": "Crime Type", "orientation": "v"},
    )
    st.plotly_chart(fig, use_container_width=True)


# ── 7. Year-over-year Furto vs Roubo — grouped bar ───────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_yoy(_con, where: str) -> pd.DataFrame:
    extra = "AND" if where else "WHERE"
    return _con.execute(f"""
        SELECT
            CAST(ANO AS INTEGER) AS ANO,
            SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 ELSE 0 END) AS furtos,
            SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 ELSE 0 END) AS roubos
        FROM crimes {where}
        {extra} ANO IS NOT NULL
        GROUP BY ANO
        ORDER BY ANO
    """).df()


def render_yoy_comparison(con: duckdb.DuckDBPyConnection, where: str) -> None:
    df = _q_yoy(con, where)
    if df.empty:
        st.info("No year data for the selected filters.")
        return
    df["ANO"] = df["ANO"].astype(str)
    df_m = df.melt(
        id_vars="ANO", value_vars=["furtos", "roubos"],
        var_name="type", value_name="count",
    )
    df_m["type"] = df_m["type"].map(
        {"furtos": "Furto (Theft)", "roubos": "Roubo (Robbery)"}
    )
    fig = px.bar(
        df_m, x="ANO", y="count", color="type", barmode="group",
        title="Year-over-Year: Furtos vs Roubos",
        labels={"ANO": "Year", "count": "Occurrences", "type": ""},
        color_discrete_map={
            "Furto (Theft)":   "#EF553B",
            "Roubo (Robbery)": "#636EFA",
        },
        text_auto=True,
    )
    fig.update_layout(height=380)
    st.plotly_chart(fig, use_container_width=True)
