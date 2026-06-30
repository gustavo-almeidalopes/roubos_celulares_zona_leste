"""
Orquestrador do pipeline de ingestão.
======================================

Encadeia todas as etapas e produz o ``cleaned_data.parquet`` consumido pelo
dashboard::

    config → download → convert (Excel→CSV) → clean (chunks) → validate → Parquet

Decisões de robustez (para a automação rodar sem babá no GitHub Actions):

* **Sem entradas → no-op, não erro.** Se nenhuma fonte tem URL utilizável nem
  arquivo local, o pipeline loga e devolve ``status="no_input"`` (a CLI sai 0).
* **URL presente que falha → erro real.** Se uma fonte declara uma URL e o
  download falha sem fallback local, o pipeline aborta (a CLI sai != 0).
* **Dados ruins nunca sobrescrevem o Parquet bom**: a validação roda antes do
  ``COPY``.

``duckdb`` é importado preguiçosamente (lazy).
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import clean, convert, download
from .config import PipelineConfig, Source, load_config

logger = logging.getLogger(__name__)

# Raiz do repositório: src/data_pipeline/pipeline.py → parents[2].
_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_README = _REPO_ROOT / "README.md"
DEFAULT_RAW_DIR = _REPO_ROOT / "src" / "data" / "raw"
DEFAULT_OUT_DIR = _REPO_ROOT / "src" / "data" / "processed"

TABLE = "crimes"
PARQUET_NAME = "cleaned_data.parquet"
DUCKDB_NAME = "crimes.duckdb"

_LOCAL_GLOBS = ("*.xlsx", "*.xls", "*.csv")


class PipelineError(RuntimeError):
    """Falha geral do pipeline."""


@dataclass
class RunResult:
    status: str                      # "ok" | "no_input"
    sources_processed: int = 0
    rows_in: int = 0
    rows_out: int = 0
    parquet_path: Path | None = None
    report: dict[str, Any] = field(default_factory=dict)


# ── Logging ───────────────────────────────────────────────────────────────────

def configure_logging(level: str = "INFO", logfile: str | Path | None = None) -> None:
    """Configura logging para stdout (+ arquivo opcional)."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


# ── Descoberta de entradas ────────────────────────────────────────────────────

def _local_only_sources(raw_dir: Path) -> list[Source]:
    """Cria Sources sintéticas para arquivos achados em raw/ (modo drop manual)."""
    found: list[Source] = []
    if not raw_dir.exists():
        return found
    seen: set[str] = set()
    for pattern in _LOCAL_GLOBS:
        for path in sorted(raw_dir.glob(pattern)):
            if path.name in seen:
                continue
            seen.add(path.name)
            found.append(Source(name=path.stem, fmt=path.suffix.lower().lstrip("."), url=None))
    return found


def _resolve_sources(cfg: PipelineConfig, raw_dir: Path, only: str | None) -> list[Source]:
    sources = cfg.enabled_sources()
    if not sources:
        logger.info("Sem fontes no README — procurando arquivos em %s …", raw_dir)
        sources = _local_only_sources(raw_dir)
    if only:
        sources = [s for s in sources if s.name == only]
    return sources


# ── Execução ──────────────────────────────────────────────────────────────────

def run(
    *,
    readme_path: str | Path = DEFAULT_README,
    raw_dir: str | Path = DEFAULT_RAW_DIR,
    out_dir: str | Path = DEFAULT_OUT_DIR,
    only_source: str | None = None,
    chunk_size: int = clean.DEFAULT_CHUNK_SIZE,
    keep_duckdb: bool = False,
) -> RunResult:
    """Executa o pipeline completo e devolve um ``RunResult``."""
    import duckdb  # lazy import

    # validate é importado aqui para manter a árvore de imports enxuta no topo.
    from . import validate

    raw_dir = Path(raw_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    cfg = load_config(readme_path)
    sources = _resolve_sources(cfg, raw_dir, only_source)

    if not sources:
        logger.warning(
            "Nenhuma fonte habilitada e nenhum arquivo em %s. Nada a fazer.", raw_dir
        )
        return RunResult(status="no_input")

    duckdb_path = out_dir / DUCKDB_NAME
    if duckdb_path.exists():
        duckdb_path.unlink()  # rebuild limpo a cada execução
    con = duckdb.connect(str(duckdb_path))

    processed = 0
    rows_in = rows_out = 0
    try:
        con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')

        for source in sources:
            logger.info("── Fonte: %s (%s) ─────────────────────────", source.name, source.fmt)
            try:
                raw_file = download.fetch(source, raw_dir)
            except download.DownloadError as exc:
                if source.url:
                    raise PipelineError(f"Fonte '{source.name}': {exc}") from exc
                logger.warning("Sem entrada para '%s' — pulando.", source.name)
                continue

            csv_path, sep, encoding = convert.to_csv(source, raw_file, raw_dir)
            n_in, n_out = clean.clean_csv_into_table(
                csv_path, con, TABLE, cfg.schema,
                sep=sep, encoding=encoding, chunk_size=chunk_size,
            )
            rows_in += n_in
            rows_out += n_out
            processed += 1

        if processed == 0:
            logger.warning("Nenhuma fonte produziu dados. Nada a fazer.")
            return RunResult(status="no_input")

        # Deduplicação cross-chunk (entre chunks/fontes).
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{TABLE}")').fetchall()]
        if {"NUM_BO", "ANO_BO"}.issubset(cols):
            logger.info("Deduplicando em (NUM_BO, ANO_BO) …")
            con.execute(
                f'CREATE OR REPLACE TABLE "{TABLE}" AS '
                f'SELECT * FROM "{TABLE}" '
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY NUM_BO, ANO_BO) = 1"
            )

        # Validação (aborta se fatal) ANTES de exportar o Parquet.
        report = validate.run_checks(con, TABLE, out_dir)

        # Exporta o snapshot Parquet (zstd) consumido pelo dashboard.
        parquet_path = out_dir / PARQUET_NAME
        con.execute(
            f"COPY \"{TABLE}\" TO '{parquet_path.as_posix()}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        logger.info("Parquet exportado: %s", parquet_path)

        final_rows = report["rows"]
        logger.info(
            "PIPELINE OK | fontes: %d | lidas: %d → finais: %d",
            processed, rows_in, final_rows,
        )
        return RunResult(
            status="ok",
            sources_processed=processed,
            rows_in=rows_in,
            rows_out=final_rows,
            parquet_path=parquet_path,
            report=report,
        )
    finally:
        con.close()
        if not keep_duckdb and duckdb_path.exists():
            duckdb_path.unlink()
