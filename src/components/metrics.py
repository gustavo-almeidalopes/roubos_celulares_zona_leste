"""
KPI metric cards — Radar Segurança SP / Capital.

Melhorias desta versão
----------------------
* Uma única query agregada calcula TODOS os KPIs (totais + ano corrente +
  ano anterior) numa só passada no DuckDB — antes precisaríamos de várias
  consultas.
* Cada card mostra um **delta YoY** (variação % vs ano anterior), com a cor
  invertida (`delta_color="inverse"`) porque pra criminalidade subir é ruim.
* Dois cards novos: "Pico mensal" (mês com mais ocorrências no recorte) e
  "% Geocodificado" (qualidade do dado).
* Layout em duas linhas de cards mantendo o tema brutalista do app.
* Assinatura `render_metric_cards(con, where)` preservada — o app.py não
  precisa mudar.
"""

from __future__ import annotations

from typing import Any

import duckdb
import streamlit as st


# ── Query agregada (uma só passada) ───────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _query_metrics(_con: duckdb.DuckDBPyConnection, where: str) -> dict[str, Any]:
    """Roda uma query única que devolve totais + recorte do ano corrente/anterior."""
    # Descobre o ano mais recente DENTRO do recorte filtrado
    last_year_row = _con.execute(f"""
        SELECT MAX(CAST(ANO AS INTEGER)) AS last_year
        FROM crimes {where}
    """).fetchone()
    last_year = last_year_row[0] if last_year_row and last_year_row[0] is not None else None
    prev_year = (last_year - 1) if last_year is not None else None

    # Agregação principal — uma única varredura
    extra = "AND" if where else "WHERE"
    row = _con.execute(f"""
        SELECT
            COUNT(*)                                                       AS total,
            COALESCE(SUM(CASE WHEN RUBRICA LIKE 'FURTO%' THEN 1 END), 0)  AS furtos,
            COALESCE(SUM(CASE WHEN RUBRICA LIKE 'ROUBO%' THEN 1 END), 0)  AS roubos,
            COUNT(DISTINCT NOME_DELEGACIA)                                 AS delegacias,
            COUNT(DISTINCT BAIRRO)                                         AS bairros,
            COALESCE(SUM(CASE WHEN LATITUDE IS NOT NULL
                              AND LONGITUDE IS NOT NULL THEN 1 END), 0)   AS geocoded,
            COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = {last_year or -1}
                              THEN 1 END), 0)                              AS total_curr,
            COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = {last_year or -1}
                              AND RUBRICA LIKE 'FURTO%' THEN 1 END), 0)   AS furtos_curr,
            COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = {last_year or -1}
                              AND RUBRICA LIKE 'ROUBO%' THEN 1 END), 0)   AS roubos_curr,
            COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = {prev_year or -1}
                              THEN 1 END), 0)                              AS total_prev,
            COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = {prev_year or -1}
                              AND RUBRICA LIKE 'FURTO%' THEN 1 END), 0)   AS furtos_prev,
            COALESCE(SUM(CASE WHEN CAST(ANO AS INTEGER) = {prev_year or -1}
                              AND RUBRICA LIKE 'ROUBO%' THEN 1 END), 0)   AS roubos_prev
        FROM crimes
        {where}
    """).fetchone()

    keys = [
        "total", "furtos", "roubos", "delegacias", "bairros", "geocoded",
        "total_curr", "furtos_curr", "roubos_curr",
        "total_prev", "furtos_prev", "roubos_prev",
    ]
    out: dict[str, Any] = {k: int(v or 0) for k, v in zip(keys, row)}
    out["last_year"] = last_year
    out["prev_year"] = prev_year

    # Pico mensal (mês mais violento dentro do recorte)
    peak = _con.execute(f"""
        SELECT
            CAST(ANO AS INTEGER) AS ano,
            CAST(MES AS INTEGER) AS mes,
            COUNT(*) AS n
        FROM crimes {where}
        {extra} ANO IS NOT NULL AND MES IS NOT NULL
        GROUP BY ano, mes
        ORDER BY n DESC
        LIMIT 1
    """).fetchone()
    out["peak"] = (
        {"ano": int(peak[0]), "mes": int(peak[1]), "n": int(peak[2])}
        if peak else None
    )
    return out


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(n: int) -> str:
    """Formato BR: 1.234.567"""
    return f"{n:,}".replace(",", ".")


def _delta(curr: int, prev: int) -> str | None:
    """Delta percentual formatado (ou None se não há base de comparação)."""
    if prev <= 0:
        return None
    pct = (curr - prev) / prev * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}% YoY"


_MES_BR = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}


# ── Render principal ──────────────────────────────────────────────────────────

def render_metric_cards(con: duckdb.DuckDBPyConnection, where: str) -> None:
    m = _query_metrics(con, where)

    total      = m["total"]
    furtos     = m["furtos"]
    roubos     = m["roubos"]
    outros     = max(total - furtos - roubos, 0)
    delegacias = m["delegacias"]
    bairros    = m["bairros"]
    geocoded   = m["geocoded"]

    pct_geo = (geocoded / total * 100) if total else 0.0
    last_y  = m["last_year"]
    peak    = m["peak"]

    # ── Linha 1 — totais com delta YoY ────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "TOTAL DE OCORRÊNCIAS",
        _fmt(total),
        delta=_delta(m["total_curr"], m["total_prev"]),
        delta_color="inverse",   # subir é ruim
        help=(
            f"Variação compara {last_y} vs {m['prev_year']}, "
            "considerando o recorte atual de filtros."
            if last_y else "Sem comparação YoY disponível."
        ),
    )
    c2.metric(
        "FURTOS",
        _fmt(furtos),
        delta=_delta(m["furtos_curr"], m["furtos_prev"]),
        delta_color="inverse",
        help="Rubricas que começam com 'FURTO'.",
    )
    c3.metric(
        "ROUBOS",
        _fmt(roubos),
        delta=_delta(m["roubos_curr"], m["roubos_prev"]),
        delta_color="inverse",
        help="Rubricas que começam com 'ROUBO'.",
    )
    c4.metric(
        "OUTROS CRIMES",
        _fmt(outros),
        delta=f"{(outros / total * 100):.1f}% do total" if total else None,
        delta_color="off",
    )

    # ── Linha 2 — contexto operacional ────────────────────────────────────────
    c5, c6, c7, c8 = st.columns(4)

    c5.metric(
        "DELEGACIAS ENVOLVIDAS",
        _fmt(delegacias),
        help="Distintas DPs com pelo menos um BO no recorte.",
    )
    c6.metric(
        "BAIRROS COBERTOS",
        _fmt(bairros),
        help="Distintos bairros com ao menos uma ocorrência no recorte.",
    )

    if peak:
        c7.metric(
            "PICO MENSAL",
            f"{_MES_BR[peak['mes']]}/{str(peak['ano'])[-2:]}",
            delta=f"{_fmt(peak['n'])} BOs",
            delta_color="off",
            help="Mês com o maior número de ocorrências dentro do recorte.",
        )
    else:
        c7.metric("PICO MENSAL", "—")

    c8.metric(
        "% GEOCODIFICADO",
        f"{pct_geo:.1f}%",
        delta=f"{_fmt(geocoded)} pontos",
        delta_color="off",
        help="Percentual de BOs com latitude/longitude válidas — qualidade do dado.",
    )
