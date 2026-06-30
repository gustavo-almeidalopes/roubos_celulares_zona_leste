"""
Interface de linha de comando do pipeline.
===========================================

Exemplos::

    # Roda tudo, lendo as fontes do README (ou arquivos de data/raw/):
    python -m src.data_pipeline run

    # Roda só uma fonte específica e mantém o crimes.duckdb:
    python -m src.data_pipeline run --source celulares_subtraidos --keep-duckdb

    # Aponta para caminhos customizados:
    python -m src.data_pipeline run --readme README.md --raw-dir data/raw --out-dir data/processed

Códigos de saída:
    0 → sucesso, OU "nada a fazer" (sem fontes/arquivos) — não quebra a CI.
    1 → erro real (download de URL declarada falhou, validação fatal, etc.).
"""

from __future__ import annotations

import argparse
import logging
import sys

from . import pipeline

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.data_pipeline",
        description="Pipeline de ingestão de dados da SSP-SP → Parquet.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Executa o pipeline completo.")
    run.add_argument("--readme", default=str(pipeline.DEFAULT_README),
                     help="Caminho do README com a configuração de fontes.")
    run.add_argument("--raw-dir", default=str(pipeline.DEFAULT_RAW_DIR),
                     help="Diretório dos arquivos brutos baixados/manuais.")
    run.add_argument("--out-dir", default=str(pipeline.DEFAULT_OUT_DIR),
                     help="Diretório de saída (Parquet + relatório).")
    run.add_argument("--source", default=None,
                     help="Processa apenas a fonte com este 'name'.")
    run.add_argument("--chunk-size", type=int, default=50_000,
                     help="Linhas por chunk na limpeza (padrão: 50000).")
    run.add_argument("--keep-duckdb", action="store_true",
                     help="Mantém o arquivo crimes.duckdb após a execução.")
    run.add_argument("--log-level", default="INFO",
                     choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                     help="Nível de log (padrão: INFO).")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.command == "run":
        pipeline.configure_logging(level=args.log_level)
        try:
            result = pipeline.run(
                readme_path=args.readme,
                raw_dir=args.raw_dir,
                out_dir=args.out_dir,
                only_source=args.source,
                chunk_size=args.chunk_size,
                keep_duckdb=args.keep_duckdb,
            )
        except Exception as exc:  # noqa: BLE001 — converte em exit code claro
            logger.error("Pipeline falhou: %s", exc)
            return 1

        if result.status == "no_input":
            logger.warning("Nada a atualizar (sem fontes/arquivos disponíveis).")
            return 0

        logger.info("Concluído: %s linhas no Parquet.", result.rows_out)
        return 0

    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
