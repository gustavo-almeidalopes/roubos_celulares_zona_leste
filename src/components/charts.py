"""
Plotly chart builders — tema minimalista monocromático (Radar Segurança SP).

Cada função roda UMA query DuckDB e devolve uma figura Plotly. Todas as queries
pesadas são cacheadas por (where_clause), então mudar um filtro re-executa a query
uma única vez. O parâmetro `_con` (underscore) opta por não-hashear a conexão
DuckDB no cache do Streamlit.

Identidade visual
-----------------
* Escala **preto & branco** pura — sem cores de destaque.
* Diferenciação de séries por **tom de cinza + traço/hachura** (não por cor),
  garantindo legibilidade mesmo em impressão P&B ou para daltônicos.
* Bordas hairline, gridlines sutis, tipografia Public Sans, sem sombras.
* Helper `_apply_minimal_layout` centraliza o estilo, evitando repetição.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ── Paleta monocromática & helpers de estilo ──────────────────────────────────

MONO = {
    "ink":   "#0A0A0A",   # quase preto — texto, eixos, série principal
    "paper": "#FFFFFF",
    "bg":    "#FAFAFA",
    "muted": "#6B7280",   # rótulos secundários
    "soft":  "#9CA3AF",   # série secundária
    "faint": "#D4D4D4",   # série terciária
    "line":  "#E5E7EB",   # gridlines / bordas
}

# Sequência de cinzas para múltiplas séries discretas (alto → baixo contraste).
MONO_SEQ = ["#0A0A0A", "#737373", "#A3A3A3", "#404040", "#C4C4C4", "#525252", "#8A8A8A"]

# Escala contínua claro → preto (para barras coloridas por volume e heatmap).
MONO_SCALE = [
    [0.0, "#F3F4F6"],
    [0.5, "#9CA3AF"],
    [1.0, "#0A0A0A"],
]

# Traços para diferenciar linhas em P&B.
_DASHES = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]

# Config padrão do Plotly: responsivo e sem barra de ferramentas (limpo).
_PLOTLY_CONFIG = {"displayModeBar": False, "responsive": True}


def _apply_minimal_layout(fig: go.Figure, *, height: int = 420, title: str | None = None) -> go.Figure:
    """Aplica o tema minimalista monocromático a uma figura Plotly."""
    fig.update_layout(
        height=height,
        paper_bgcolor=MONO["paper"],
        plot_bgcolor=MONO["paper"],
        font=dict(family="Public Sans, sans-serif", color=MONO["ink"], size=13),
        title=dict(
            text=f"<b>{title}</b>" if title else None,
            font=dict(family="Public Sans, sans-serif", size=15, color=MONO["ink"]),
            x=0.01, xanchor="left", y=0.98,
        ),
        margin=dict(l=14, r=14, t=52 if title else 14, b=40),
        legend=dict(
            font=dict(family="Public Sans", size=11, color=MONO["ink"]),
            bgcolor="rgba(255,255,255,0)",
            borderwidth=0,
        ),
        hoverlabel=dict(
            bgcolor="#FFFFFF",
            bordercolor=MONO["line"],
            font=dict(family="Public Sans", size=12, color=MONO["ink"]),
        ),
    )
    axis_common = dict(
        showline=True, linewidth=1, linecolor=MONO["line"], mirror=False,
        gridcolor=MONO["line"], zerolinecolor=MONO["line"], zerolinewidth=1,
        ticks="outside", tickcolor=MONO["line"], ticklen=4,
        tickfont=dict(family="Public Sans", size=11, color=MONO["muted"]),
    )
    fig.update_xaxes(**axis_common)
    fig.update_yaxes(**axis_common)
    return fig


def _empty(msg: str) -> None:
    st.info(msg)


def _show(fig: go.Figure) -> None:
    st.plotly_chart(fig, use_container_width=True, config=_PLOTLY_CONFIG)


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
        color_continuous_scale=MONO_SCALE,
        text="occurrences",
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(family="Public Sans", size=11, color=MONO["ink"]),
        marker_line_color=MONO["ink"],
        marker_line_width=0.5,
        cliponaxis=False,
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        coloraxis_showscale=False,
    )
    _apply_minimal_layout(fig, height=500, title=f"Top {top_n} Tipos de Crime")
    _show(fig)


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
        color_discrete_sequence=MONO_SEQ,
    )
    # Diferencia cada ano também pelo traço (legível em P&B).
    for i, trace in enumerate(fig.data):
        trace.line.width = 2.5
        trace.line.dash = _DASHES[i % len(_DASHES)]
        trace.marker.size = 7
        trace.marker.line = dict(color=MONO["paper"], width=1)
    fig.update_xaxes(
        tickmode="array",
        tickvals=[str(m).zfill(2) for m in range(1, 13)],
        ticktext=["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                  "Jul", "Ago", "Set", "Out", "Nov", "Dez"],
    )
    _apply_minimal_layout(fig, height=420, title="Tendência mensal por ano")
    _show(fig)


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
            "Furto":  MONO["ink"],
            "Roubo":  MONO["soft"],
            "Outros": MONO["faint"],
        },
        hole=0.58,
    )
    fig.update_traces(
        textinfo="percent+label",
        textfont=dict(family="Public Sans", size=13, color="#FFFFFF"),
        marker=dict(line=dict(color=MONO["paper"], width=2)),
        sort=False,
    )
    _apply_minimal_layout(fig, height=500, title="Categoria de Crime")
    fig.update_layout(showlegend=False)
    _show(fig)


# ── 4. Top BAIRROS (só SP capital) ───────────────────────────────────────────

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
    """Mantém o nome antigo p/ compatibilidade com o app.py, mas mostra BAIRROS."""
    df = _q_top_bairros(con, where, top_n)
    if df.empty:
        _empty("Sem dados de bairro para os filtros selecionados.")
        return
    fig = px.bar(
        df, x="bairro", y="occurrences",
        labels={"bairro": "Bairro", "occurrences": "Ocorrências"},
        color="occurrences", color_continuous_scale=MONO_SCALE,
        text="occurrences",
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(family="Public Sans", size=10, color=MONO["ink"]),
        marker_line_color=MONO["ink"],
        marker_line_width=0.5,
        cliponaxis=False,
    )
    fig.update_xaxes(tickangle=40)
    fig.update_layout(coloraxis_showscale=False)
    _apply_minimal_layout(fig, height=460, title=f"Top {top_n} Bairros — SP Capital")
    _show(fig)


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
            colorscale=MONO_SCALE,
            hoverongaps=False,
            xgap=2, ygap=2,
            colorbar=dict(
                outlinewidth=0,
                tickfont=dict(family="Public Sans", color=MONO["muted"]),
                title=dict(text="Ocorr.", font=dict(family="Public Sans", size=11)),
            ),
            text=pivot.values,
            texttemplate="%{text:,}",
            textfont=dict(family="Public Sans", size=11, color=MONO["ink"]),
        )
    )
    _apply_minimal_layout(fig, height=360, title="Dia da semana × Período do dia")
    fig.update_xaxes(title_text="Dia")
    fig.update_yaxes(title_text="Período")
    _show(fig)


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

    # Agrupa rubricas raras em "Outros" pra manter a legenda enxuta.
    top_rub = df["RUBRICA"].value_counts().head(8).index
    df["RUBRICA_PLOT"] = df["RUBRICA"].where(df["RUBRICA"].isin(top_rub), "Outros")

    fig = px.scatter_mapbox(
        df,
        lat="LATITUDE", lon="LONGITUDE",
        color="RUBRICA_PLOT",
        hover_data={"NOME_MUNICIPIO": True, "BAIRRO": True,
                    "RUBRICA": True, "RUBRICA_PLOT": False,
                    "LATITUDE": False, "LONGITUDE": False},
        zoom=10, height=560, opacity=0.7,
        color_discrete_sequence=MONO_SEQ + ["#1F1F1F", "#616161", "#2E2E2E"],
    )
    fig.update_traces(marker=dict(size=7))
    fig.update_layout(
        mapbox_style="carto-positron",
        margin=dict(l=0, r=0, t=40, b=0),
        paper_bgcolor=MONO["paper"],
        title=dict(
            text=f"<b>Mapa de incidentes — {len(df):,} registros amostrados</b>".replace(",", "."),
            font=dict(family="Public Sans", size=14, color=MONO["ink"]),
            x=0.01, xanchor="left",
        ),
        legend=dict(
            title=dict(text="<b>Rubrica</b>", font=dict(family="Public Sans", size=11)),
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor=MONO["line"], borderwidth=1,
            font=dict(family="Public Sans", size=10),
        ),
    )
    _show(fig)


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
        color_discrete_map={"Furto": MONO["ink"], "Roubo": MONO["soft"]},
        text_auto=True,
    )
    fig.update_traces(
        marker_line_color=MONO["ink"], marker_line_width=0.5,
        textfont=dict(family="Public Sans", size=10, color=MONO["ink"]),
        textposition="outside",
        cliponaxis=False,
    )
    # Hachura na série "Roubo" → distinção sem cor.
    fig.update_traces(marker_pattern_shape="/", selector=dict(name="Roubo"))
    _apply_minimal_layout(fig, height=460, title="Furtos vs Roubos por Ano")
    _show(fig)
