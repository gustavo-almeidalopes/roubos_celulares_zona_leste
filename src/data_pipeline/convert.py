"""
Conversão Excel (xls/xlsx) → CSV.
==================================

Requisito explícito do projeto: os arquivos de origem chegam em XLS/XLSX e
precisam virar **CSV** para o consumo do dashboard. Este módulo:

* Lê planilhas Excel com pandas (engine ``openpyxl`` p/ xlsx, ``xlrd`` p/ xls).
* Exporta um CSV padronizado (``sep=';'``, ``encoding='utf-8'``).
* Para fontes que já são CSV, faz passagem direta — devolve o caminho original
  com o ``sep``/``encoding`` declarados (evita reescrever arquivos grandes).

Devolve sempre uma tupla ``(csv_path, sep, encoding)`` para que a etapa de
limpeza saiba como ler o arquivo.

``pandas`` é importado preguiçosamente (lazy) — permite importar o módulo para
checagem de sintaxe sem ter a dependência instalada.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .config import Source

logger = logging.getLogger(__name__)

# Saída padronizada para arquivos convertidos de Excel.
_STD_SEP = ";"
_STD_ENCODING = "utf-8"


class ConversionError(RuntimeError):
    """Falha ao converter um arquivo de origem para CSV."""


def _excel_engine(fmt: str) -> str:
    return "openpyxl" if fmt == "xlsx" else "xlrd"


def to_csv(source: Source, src_path: Path, raw_dir: Path) -> tuple[Path, str, str]:
    """Garante um CSV legível para ``source`` e devolve ``(csv, sep, encoding)``.

    * ``csv``  → fonte CSV: passa direto, devolvendo ``source.sep``/encoding.
    * ``xls/xlsx`` → lê a planilha e grava ``<name>.csv`` padronizado em raw_dir.
    """
    fmt = source.fmt
    if fmt == "csv":
        logger.info("Fonte '%s' já é CSV — sem conversão.", source.name)
        return src_path, source.sep, source.encoding

    if fmt not in ("xls", "xlsx"):
        raise ConversionError(f"Formato não suportado para conversão: {fmt!r}")

    import pandas as pd  # lazy import

    out_csv = raw_dir / f"{source.name}.csv"
    try:
        logger.info("Convertendo %s (%s) → %s …", src_path.name, fmt, out_csv.name)
        # dtype=str preserva os dados brutos; a limpeza/tipagem acontece depois.
        df = pd.read_excel(
            src_path,
            sheet_name=source.sheet,
            engine=_excel_engine(fmt),
            dtype=str,
        )
    except Exception as exc:  # noqa: BLE001
        raise ConversionError(f"Erro lendo Excel '{src_path.name}': {exc}") from exc

    if df.empty:
        raise ConversionError(f"Planilha '{src_path.name}' não tem linhas.")

    df.to_csv(out_csv, index=False, sep=_STD_SEP, encoding=_STD_ENCODING)
    logger.info("CSV gerado: %s (%d linhas, %d colunas).", out_csv.name, len(df), df.shape[1])
    return out_csv, _STD_SEP, _STD_ENCODING
