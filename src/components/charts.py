"""
Plotly chart builders — versão "brutalista" para o Radar Segurança SP / Capital.

Cada função roda UMA query DuckDB e devolve uma figura Plotly. Todas as queries
pesadas são cacheadas por (where_clause), então mudar um filtro re-executa
a query uma única vez. O parâmetro `_con` (underscore) opta por não-hashear
a conexão DuckDB no cache do Streamlit.

Mudanças nesta versão
---------------------
* Tema visual brutalista: paleta preto/branco/vermelho, fontes pesadas,
  bordas sólidas, sem grids suaves nem sombras coloridas.
* `render_top_municipios` foi reaproveitado como **Top Bairros** — como o
  app já filtra apenas São Paulo capital, agrupar por município retornaria
  uma única barra; agrupamos por BAIRRO.
* Helper `_apply_brutal_layout` centraliza o estilo, evitando repetição.
* Tooltips, eixos e títulos padronizados.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ── Paleta & helpers de estilo ────────────────────────────────────────────────

BRUTAL = {
    "ink":     "#000000",
    "paper":   "#FFFFFF",
    "bg":      "#E5E5E5",
    "muted":   "#404040",
    "soft":    "#A3A3A3",
    "accent":  "#DC2626",   # vermelho
    "accent2": "#1F1F1F",
    "accent3": "#737373",
}

# Sequência usada quando precisamos de várias cores discretas
BRUTAL_SEQ = ["#000000", "#DC2626", "#404040", "#A3A3A3", "#737373", "#1F1F1F"]

# Escala contínua mono-vermelho (do cinza claro ao vermelho forte)
BRUTAL_SCALE_RED = [
    [0.0, "#F5F5F5"],
    [0.5, "#F87171"],
    [1.0, "#7F1D1D"],
]

# Escala contínua mono-preto
BRUTAL_SCALE_INK = [
    [0.0, "#F5F5F5"],
    [0.5, "#737373"],
    [1.0, "#000000"],
]


def _apply_brutal_layout(fig: go.Figure, *, height: int = 420, title: str | None = None) -> go.Figure:
    """Aplica o tema brutalista a uma figura Plotly já construída."""
    fig.update_layout(
        height=height,
        paper_bgcolor=BRUTAL["paper"],
        plot_bgcolor=BRUTAL["paper"],
        font=dict(family="Public Sans, sans-serif", color=BRUTAL["ink"], size=13),
        title=dict(
            text=f"<b>{title.upper()}</b>" if title else None,
            font=dict(family="Archivo Black, sans-serif", size=16, color=BRUTAL["ink"]),
            x=0.01, xanchor="left", y=0.97,
        ),
        margin=dict(l=14, r=14, t=56 if title else 14, b=40),
        legend=dict(
            font=dict(family="Public Sans", size=11, color=BRUTAL["ink"]),
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor=BRUTAL["ink"],
            borderwidth=2,
        ),
        hoverlabel=dict(
            bgcolor="#FFFFFF",
            bordercolor="#000000",
            font=dict(family="Public Sans", size=12, color="#000"),
        ),
    )
    fig.update_xaxes(
        showline=True, linewidth=2, linecolor=BRUTAL["ink"], mirror=False,
        gridcolor="#E5E5E5", zerolinecolor=BRUTAL["ink"], zerolinewidth=2,
        ticks="outside", tickcolor=BRUTAL["ink"], ticklen=5,
        tickfont=dict(family="Public Sans", size=11, color=BRUTAL["muted"]),
    )
    fig.update_yaxes(
        showline=True, linewidth=2, linecolor=BRUTAL["ink"], mirror=False,
        gridcolor="#E5E5E5", zerolinecolor=BRUTAL["ink"], zerolinewidth=2,
        ticks="outside", tickcolor=BRUTAL["ink"], ticklen=5,
        tickfont=dict(family="Public Sans", size=11, color=BRUTAL["muted"]),
    )
    return fig


def _empty(msg: str) -> None:
    st.info(msg)


# ── 1. Top crime types (RUBRICA) — barra horizontal ──────────────────────────

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


def render_top_rubricas(con: duckdb.DuckDBPyConnection, where: str, top_n: int = 15) -> None:
    df = _q_top_rubricas(con, where, top_n)
    if df.empty:
        _empty("Sem dados de rubrica para os filtros selecionados.")
        return
    fig = px.bar(
        df, x="occurrences", y="RUBRICA", orientation="h",
        labels={"occurrences": "Ocorrências", "RUBRICA": ""},
        color="occurrences",
        color_continuous_scale=BRUTAL_SCALE_RED,
        text="occurrences",
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(family="Archivo Black", size=11, color=BRUTAL["ink"]),
        marker_line_color=BRUTAL["ink"],
        marker_line_width=2,
        cliponaxis=False,
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        coloraxis_showscale=False,
    )
    _apply_brutal_layout(fig, height=500, title=f"Top {top_n} Tipos de Crime")
    st.plotly_chart(fig, use_container_width=True)


# ── 2. Tendência mensal — linhas ──────────────────────────────────────────────

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
        _empty("Sem dados temporais para os filtros selecionados.")
        return
    df["period"] = df["MES"].astype(str).str.zfill(2)
    fig = px.line(
        df, x="period", y="occurrences", color="ANO",
        labels={"period": "Mês", "occurrences": "Ocorrências", "ANO": "Ano"},
        markers=True,
        color_discrete_sequence=BRUTAL_SEQ,
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=9, line=dict(color="#000", width=2)))
    fig.update_xaxes(
        tickmode="array",
        tickvals=[str(m).zfill(2) for m in range(1, 13)],
        ticktext=["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                  "Jul", "Ago", "Set", "Out", "Nov", "Dez"],
    )
    _apply_brutal_layout(fig, height=420, title="Tendência mensal por ano")
    st.plotly_chart(fig, use_container_width=True)


# ── 3. Categoria de crime — donut ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_category_pie(_con, where: str) -> pd.DataFrame:
    return _con.execute(f"""
        SELECT
            CASE
                WHEN RUBRICA LIKE 'FURTO%' THEN 'Furto'
                WHEN RUBRICA LIKE 'ROUBO%' THEN 'Roubo'
                ELSE 'Outros'
            END AS category,
            COUNT(*) AS occurrences
        FROM crimes {where}
        GROUP BY category
        ORDER BY occurrences DESC
    """).df()


def render_crime_category_pie(con: duckdb.DuckDBPyConnection, where: str) -> None:
    df = _q_category_pie(con, where)
    if df.empty:
        _empty("Sem dados para os filtros selecionados.")
        return
    fig = px.pie(
        df, names="category", values="occurrences",
        color="category",
        color_discrete_map={
            "Furto":  BRUTAL["ink"],
            "Roubo":  BRUTAL["accent"],
            "Outros": BRUTAL["soft"],
        },
        hole=0.55,
    )
    fig.update_traces(
        textinfo="percent+label",
        textfont=dict(family="Archivo Black", size=13, color="#fff"),
        marker=dict(line=dict(color=BRUTAL["ink"], width=3)),
        pull=[0.02, 0.02, 0.02],
    )
    _apply_brutal_layout(fig, height=500, title="Categoria de Crime")
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


# ── 4. Top BAIRROS (renomeado de "municipios" — só SP capital) ───────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_top_bairros(_con, where: str, top_n: int) -> pd.DataFrame:
    return _con.execute(f"""
        SELECT BAIRRO AS bairro, COUNT(*) AS occurrences
        FROM crimes {where}
        {"AND" if where else "WHERE"} BAIRRO IS NOT NULL AND TRIM(BAIRRO) <> ''
        GROUP BY BAIRRO
        ORDER BY occurrences DESC
        LIMIT {top_n}
    """).df()


def render_top_municipios(con: duckdb.DuckDBPyConnection, where: str, top_n: int = 15) -> None:
    """Mantém o nome antigo p/ compatibilidade com o app.py, mas agora mostra BAIRROS."""
    df = _q_top_bairros(con, where, top_n)
    if df.empty:
        _empty("Sem dados de bairro para os filtros selecionados.")
        return
    fig = px.bar(
        df, x="bairro", y="occurrences",
        labels={"bairro": "Bairro", "occurrences": "Ocorrências"},
        color="occurrences", color_continuous_scale=BRUTAL_SCALE_INK,
        text="occurrences",
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(family="Archivo Black", size=10, color=BRUTAL["ink"]),
        marker_line_color=BRUTAL["ink"],
        marker_line_width=2,
        cliponaxis=False,
    )
    fig.update_xaxes(tickangle=40)
    fig.update_layout(coloraxis_showscale=False)
    _apply_brutal_layout(fig, height=460, title=f"Top {top_n} Bairros — SP Capital")
    st.plotly_chart(fig, use_container_width=True)


# ── 5. Heatmap dia-da-semana × período ────────────────────────────────────────

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
        _empty("Sem dados temporais para os filtros selecionados.")
        return
    _dow = {0: "Dom", 1: "Seg", 2: "Ter", 3: "Qua", 4: "Qui", 5: "Sex", 6: "Sáb"}
    df["day"] = df["dow"].map(_dow)
    pivot = df.pivot_table(
        index="DESCR_PERIODO", columns="day",
        values="occurrences", fill_value=0, aggfunc="sum",
    )
    day_order = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    pivot = pivot.reindex(columns=[d for d in day_order if d in pivot.columns])

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values.tolist(),
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=BRUTAL_SCALE_RED,
            hoverongaps=False,
            xgap=3, ygap=3,                       # "grid" brutalista entre células
            colorbar=dict(
                outlinecolor=BRUTAL["ink"],
                outlinewidth=2,
                tickfont=dict(family="Public Sans", color=BRUTAL["ink"]),
                title=dict(text="Ocorr.", font=dict(family="Archivo Black", size=11)),
            ),
            text=pivot.values,
            texttemplate="%{text:,}",
            textfont=dict(family="Archivo Black", size=11, color=BRUTAL["ink"]),
        )
    )
    _apply_brutal_layout(fig, height=360, title="Dia da semana × Período do dia")
    fig.update_xaxes(title_text="Dia")
    fig.update_yaxes(title_text="Período")
    st.plotly_chart(fig, use_container_width=True)


# ── 6. Mapa geo (amostra) ─────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _q_map(_con, where: str, max_points: int) -> pd.DataFrame:
    extra = "AND" if where else "WHERE"
    return _con.execute(f"""
        SELECT LATITUDE, LONGITUDE, RUBRICA, NOME_MUNICIPIO, BAIRRO
        FROM crimes {where}
        {extra} LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        USING SAMPLE {max_points} ROWS
    """).df()


def render_crime_map(con: duckdb.DuckDBPyConnection, where: str, max_points: int = 5_000) -> None:
    df = _q_map(con, where, max_points)
    if df.empty:
        _empty("Sem registros geocodificados para os filtros selecionados.")
        return

    # Agrupa rubricas raras em "Outros" pra manter a legenda enxuta
    top_rub = df["RUBRICA"].value_counts().head(8).index
    df["RUBRICA_PLOT"] = df["RUBRICA"].where(df["RUBRICA"].isin(top_rub), "Outros")

    fig = px.scatter_mapbox(
        df,
        lat="LATITUDE", lon="LONGITUDE",
        color="RUBRICA_PLOT",
        hover_data={"NOME_MUNICIPIO": True, "BAIRRO": True,
                    "RUBRICA": True, "RUBRICA_PLOT": False,
                    "LATITUDE": False, "LONGITUDE": False},
        zoom=10, height=560, opacity=0.75,
        color_discrete_sequence=BRUTAL_SEQ + ["#991B1B", "#525252", "#262626"],
    )
    fig.update_traces(marker=dict(size=7))
    fig.update_layout(
        mapbox_style="carto-positron",
        margin=dict(l=0, r=0, t=44, b=0),
        paper_bgcolor=BRUTAL["paper"],
        title=dict(
            text=f"<b>MAPA DE INCIDENTES — {len(df):,} REGISTROS AMOSTRADOS</b>".replace(",", "."),
            font=dict(family="Archivo Black", size=14, color=BRUTAL["ink"]),
            x=0.01, xanchor="left",
        ),
        legend=dict(
            title=dict(text="<b>RUBRICA</b>", font=dict(family="Archivo Black", size=11)),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor=BRUTAL["ink"], borderwidth=2,
            font=dict(family="Public Sans", size=10),
        ),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── 7. YoY Furto vs Roubo ─────────────────────────────────────────────────────

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
        _empty("Sem dados anuais para os filtros selecionados.")
        return
    df["ANO"] = df["ANO"].astype(str)
    df_m = df.melt(
        id_vars="ANO", value_vars=["furtos", "roubos"],
        var_name="type", value_name="count",
    )
    df_m["type"] = df_m["type"].map({"furtos": "Furto", "roubos": "Roubo"})

    fig = px.bar(
        df_m, x="ANO", y="count", color="type", barmode="group",
        labels={"ANO": "Ano", "count": "Ocorrências", "type": ""},
        color_discrete_map={"Furto": BRUTAL["ink"], "Roubo": BRUTAL["accent"]},
        text_auto=True,
    )
    fig.update_traces(
        marker_line_color=BRUTAL["ink"], marker_line_width=2,
        textfont=dict(family="Archivo Black", size=10, color=BRUTAL["ink"]),
        textposition="outside",
        cliponaxis=False,
    )
    _apply_brutal_layout(fig, height=460, title="Furtos vs Roubos por Ano")
    st.plotly_chart(fig, use_container_width=True)
