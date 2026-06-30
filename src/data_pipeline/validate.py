"""
Validação de schema e qualidade dos dados.
===========================================

Roda **depois** da limpeza e da deduplicação, **antes** de exportar o Parquet —
assim, dados ruins nunca sobrescrevem o snapshot bom usado pelo dashboard.

Checagens:

* **Fatais** (lançam ``ValidationError`` e abortam o pipeline):
    - tabela vazia (0 linhas);
    - ausência de colunas obrigatórias (``ANO``, ``MES``, ``RUBRICA``).
* **Avisos** (logados e registrados no relatório, não abortam):
    - % de nulos por coluna;
    - % de linhas geocodificadas (lat/lon válidas);
    - duplicatas remanescentes em (``NUM_BO``, ``ANO_BO``).

Gera um relatório legível em ``data/processed/quality_report.md``.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ("ANO", "MES", "RUBRICA")
REPORT_NAME = "quality_report.md"


class ValidationError(RuntimeError):
    """Falha de validação que deve abortar o pipeline."""


def _table_columns(con: "duckdb.DuckDBPyConnection", table: str) -> list[str]:
    rows = con.execute(f'PRAGMA table_info("{table}")').fetchall()
    # PRAGMA table_info → (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]


def _null_percentages(con, table: str, columns: list[str], total: int) -> dict[str, float]:
    if total == 0:
        return {c: 100.0 for c in columns}
    # Uma única query: COUNT(col) conta apenas não-nulos.
    selects = ", ".join(f'COUNT("{c}") AS "{c}"' for c in columns)
    row = con.execute(f'SELECT {selects} FROM "{table}"').fetchone()
    pct = {}
    for col, non_null in zip(columns, row):
        pct[col] = round((1 - (non_null or 0) / total) * 100, 2)
    return pct


def run_checks(
    con: "duckdb.DuckDBPyConnection",
    table: str,
    processed_dir: Path,
) -> dict[str, Any]:
    """Valida a tabela e grava o relatório. Lança ``ValidationError`` se fatal."""
    columns = _table_columns(con, table)

    total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    if total == 0:
        raise ValidationError(f"Tabela '{table}' está vazia após a limpeza.")

    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise ValidationError(
            f"Colunas obrigatórias ausentes: {', '.join(missing)}. "
            f"Colunas presentes: {', '.join(columns)}"
        )

    nulls = _null_percentages(con, table, columns, total)

    # Geocodificação.
    geocoded_pct = None
    if {"LATITUDE", "LONGITUDE"}.issubset(columns):
        geocoded = con.execute(
            f'SELECT COUNT(*) FROM "{table}" '
            'WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL'
        ).fetchone()[0]
        geocoded_pct = round(geocoded / total * 100, 2)

    # Duplicatas remanescentes.
    dup_count = None
    if {"NUM_BO", "ANO_BO"}.issubset(columns):
        dup_count = con.execute(
            f'SELECT COUNT(*) - COUNT(DISTINCT (NUM_BO, ANO_BO)) FROM "{table}"'
        ).fetchone()[0]
        if dup_count and dup_count > 0:
            logger.warning("%d duplicatas remanescentes em (NUM_BO, ANO_BO).", dup_count)

    report = {
        "rows": total,
        "columns": len(columns),
        "null_pct": nulls,
        "geocoded_pct": geocoded_pct,
        "duplicates": dup_count,
    }

    _write_report(processed_dir / REPORT_NAME, table, report)
    logger.info(
        "Validação OK: %d linhas, %d colunas, %s%% geocodificado.",
        total, len(columns),
        geocoded_pct if geocoded_pct is not None else "n/a",
    )
    return report


def _write_report(path: Path, table: str, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# Relatório de Qualidade de Dados",
        "",
        f"- **Gerado em:** {ts}",
        f"- **Tabela:** `{table}`",
        f"- **Linhas:** {report['rows']:,}".replace(",", "."),
        f"- **Colunas:** {report['columns']}",
    ]
    if report["geocoded_pct"] is not None:
        lines.append(f"- **Geocodificado:** {report['geocoded_pct']}%")
    if report["duplicates"] is not None:
        lines.append(f"- **Duplicatas (NUM_BO, ANO_BO):** {report['duplicates']}")

    lines += ["", "## Nulos por coluna", "", "| Coluna | % nulos |", "|---|---|"]
    for col, pct in sorted(report["null_pct"].items(), key=lambda kv: kv[1], reverse=True):
        lines.append(f"| {col} | {pct}% |")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Relatório de qualidade gravado em %s", path)
