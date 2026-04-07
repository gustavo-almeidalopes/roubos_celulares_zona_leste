"""
Sidebar filters + SQL WHERE-clause builder — Radar Segurança SP / Capital.

Melhorias desta versão
----------------------
* **Mais dimensões filtráveis**: ano, mês, categoria (Furto/Roubo/Outros),
  rubrica específica, delegacia, bairro, período do dia, e busca textual livre.
* **Organização em seções colapsáveis** (`st.expander`) — sidebar não fica
  poluído mesmo com 8 filtros.
* **Botão "Limpar filtros"** que reseta tudo via `st.session_state` + rerun.
* **Filtro de município removido** (o app já fixa SP capital), mas a chave
  `municipios` continua existindo no dict de retorno pra não quebrar contratos.
* **Cache mais inteligente**: queries de DISTINCT compartilham TTL longo
  (1h) já que os valores não mudam durante a sessão.
* **Sanitização SQL**: todas as listas vêm de DISTINCT do próprio DuckDB,
  e strings são escapadas com `''` duplo. Texto livre passa por escape de
  `'` e `%` antes de ir pro `LIKE`.
* **Compatibilidade total**: as funções `render_sidebar_filters(con)` e
  `build_where_clause(filters)` mantêm assinatura idêntica.
"""

from __future__ import annotations

import duckdb
import streamlit as st


# ── Constantes ────────────────────────────────────────────────────────────────

_MONTH_LABELS = {
    1: "Jan", 2: "Fev", 3: "Mar",  4: "Abr",
    5: "Mai", 6: "Jun", 7: "Jul",  8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

_CATEGORY_OPTIONS = ["Furto", "Roubo", "Outros"]

# Chaves do session_state usadas pelos widgets — centralizadas pro reset
_FILTER_KEYS = (
    "flt_years", "flt_months", "flt_categories",
    "flt_rubricas", "flt_delegacias", "flt_bairros",
    "flt_periodos", "flt_search",
)


# ── Cached distinct loaders ───────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_years(_con: duckdb.DuckDBPyConnection) -> list[int]:
    return (
        _con.execute(
            "SELECT DISTINCT CAST(ANO AS INTEGER) AS ANO "
            "FROM crimes WHERE ANO IS NOT NULL ORDER BY ANO DESC"
        ).df()["ANO"].tolist()
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_delegacias(_con: duckdb.DuckDBPyConnection) -> list[str]:
    return (
        _con.execute(
            "SELECT DISTINCT NOME_DELEGACIA FROM crimes "
            "WHERE NOME_DELEGACIA IS NOT NULL ORDER BY 1"
        ).df()["NOME_DELEGACIA"].tolist()
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_bairros(_con: duckdb.DuckDBPyConnection) -> list[str]:
    return (
        _con.execute(
            "SELECT DISTINCT BAIRRO FROM crimes "
            "WHERE BAIRRO IS NOT NULL AND TRIM(BAIRRO) <> '' "
            "ORDER BY 1"
        ).df()["BAIRRO"].tolist()
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_rubricas(_con: duckdb.DuckDBPyConnection) -> list[str]:
    return (
        _con.execute(
            "SELECT DISTINCT RUBRICA FROM crimes "
            "WHERE RUBRICA IS NOT NULL ORDER BY 1"
        ).df()["RUBRICA"].tolist()
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _distinct_periodos(_con: duckdb.DuckDBPyConnection) -> list[str]:
    return (
        _con.execute(
            "SELECT DISTINCT DESCR_PERIODO FROM crimes "
            "WHERE DESCR_PERIODO IS NOT NULL ORDER BY 1"
        ).df()["DESCR_PERIODO"].tolist()
    )


# ── Reset helper ──────────────────────────────────────────────────────────────

def _reset_filters() -> None:
    """Apaga todas as chaves de filtro do session_state — força os widgets ao default."""
    for k in _FILTER_KEYS:
        if k in st.session_state:
            del st.session_state[k]


# ── Sidebar render ────────────────────────────────────────────────────────────

def render_sidebar_filters(con: duckdb.DuckDBPyConnection) -> dict:
    """Renderiza todos os filtros e devolve um dict pronto pra `build_where_clause`."""

    # Botão de reset no topo
    if st.sidebar.button("↻ LIMPAR FILTROS", use_container_width=True):
        _reset_filters()
        st.rerun()

    years = _distinct_years(con)

    # ── Seção: Tempo ──────────────────────────────────────────────────────────
    with st.sidebar.expander("📅 TEMPO", expanded=True):
        selected_years = st.multiselect(
            "Ano",
            options=years,
            default=years[:3] if len(years) >= 3 else years,
            key="flt_years",
        )
        selected_months = st.multiselect(
            "Mês",
            options=list(_MONTH_LABELS.keys()),
            default=list(_MONTH_LABELS.keys()),
            format_func=lambda m: _MONTH_LABELS[m],
            key="flt_months",
        )
        periodos = _distinct_periodos(con)
        selected_periodos = st.multiselect(
            "Período do dia",
            options=periodos,
            default=[],
            placeholder="Todos os períodos",
            key="flt_periodos",
        )

    # ── Seção: Tipo de crime ──────────────────────────────────────────────────
    with st.sidebar.expander("🔫 TIPO DE CRIME", expanded=True):
        selected_categories = st.multiselect(
            "Categoria",
            options=_CATEGORY_OPTIONS,
            default=[],
            placeholder="Todas as categorias",
            key="flt_categories",
            help="Furto = rubricas 'FURTO%' · Roubo = 'ROUBO%' · Outros = o resto",
        )
        rubricas = _distinct_rubricas(con)
        selected_rubricas = st.multiselect(
            "Rubrica específica",
            options=rubricas,
            default=[],
            placeholder=f"Todas ({len(rubricas)} rubricas)",
            key="flt_rubricas",
        )
        search_text = st.text_input(
            "Busca livre na rubrica",
            value="",
            placeholder="ex: veiculo, celular, residencia…",
            key="flt_search",
            help="Busca case-insensitive em RUBRICA (operador LIKE).",
        )

    # ── Seção: Localização ────────────────────────────────────────────────────
    with st.sidebar.expander("📍 LOCALIZAÇÃO", expanded=False):
        delegacias = _distinct_delegacias(con)
        selected_delegacias = st.multiselect(
            "Delegacia",
            options=delegacias,
            default=[],
            placeholder=f"Todas ({len(delegacias)} DPs)",
            key="flt_delegacias",
        )
        bairros = _distinct_bairros(con)
        selected_bairros = st.multiselect(
            "Bairro",
            options=bairros,
            default=[],
            placeholder=f"Todos ({len(bairros)} bairros)",
            key="flt_bairros",
        )

    return {
        "years":      selected_years,
        "months":     selected_months,
        "categories": selected_categories,
        "rubricas":   selected_rubricas,
        "search":     search_text.strip(),
        "delegacias": selected_delegacias,
        "bairros":    selected_bairros,
        "periodos":   selected_periodos,
        # mantida pra compatibilidade — o app trava em [] já que só temos SP capital
        "municipios": [],
    }


# ── WHERE clause builder ──────────────────────────────────────────────────────

def _quote_list(values) -> str:
    """Lista de strings → literal SQL com aspas simples escapadas."""
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


def _category_clause(cats: list[str]) -> str | None:
    """Converte categorias amigáveis em condição SQL sobre RUBRICA."""
    parts = []
    if "Furto" in cats:
        parts.append("RUBRICA LIKE 'FURTO%'")
    if "Roubo" in cats:
        parts.append("RUBRICA LIKE 'ROUBO%'")
    if "Outros" in cats:
        parts.append("(RUBRICA NOT LIKE 'FURTO%' AND RUBRICA NOT LIKE 'ROUBO%')")
    if not parts:
        return None
    return "(" + " OR ".join(parts) + ")"


def build_where_clause(filters: dict) -> str:
    """Converte o dict de filtros em string SQL `WHERE …`. Devolve `""` se vazio."""
    clauses: list[str] = []

    if filters.get("years"):
        vals = ", ".join(str(int(y)) for y in filters["years"])
        clauses.append(f"CAST(ANO AS INTEGER) IN ({vals})")

    if filters.get("months"):
        vals = ", ".join(str(int(m)) for m in filters["months"])
        clauses.append(f"CAST(MES AS INTEGER) IN ({vals})")

    if filters.get("categories"):
        cat_clause = _category_clause(filters["categories"])
        if cat_clause:
            clauses.append(cat_clause)

    if filters.get("rubricas"):
        clauses.append(f"RUBRICA IN ({_quote_list(filters['rubricas'])})")

    if filters.get("search"):
        # escape ' " % e _ pra evitar wildcards acidentais e injeção
        raw = filters["search"]
        safe = (
            raw.replace("'", "''")
               .replace("\\", "\\\\")
               .replace("%", r"\%")
               .replace("_", r"\_")
        )
        clauses.append(f"UPPER(RUBRICA) LIKE UPPER('%{safe}%') ESCAPE '\\'")

    if filters.get("delegacias"):
        clauses.append(f"NOME_DELEGACIA IN ({_quote_list(filters['delegacias'])})")

    if filters.get("bairros"):
        clauses.append(f"BAIRRO IN ({_quote_list(filters['bairros'])})")

    if filters.get("periodos"):
        clauses.append(f"DESCR_PERIODO IN ({_quote_list(filters['periodos'])})")

    if filters.get("municipios"):
        clauses.append(f"NOME_MUNICIPIO IN ({_quote_list(filters['municipios'])})")

    return ("WHERE " + " AND ".join(clauses)) if clauses else ""
