"""
Limpeza e normalização dos dados (em chunks).
==============================================

Implementa fielmente as transformações documentadas no README ("Pipeline de
Limpeza — Detalhes"), aplicadas chunk a chunk para manter o uso de memória baixo
mesmo com arquivos de +1M de linhas:

* **Texto** → ``strip()`` + ``upper()``; ``""``/``"NAN"``/``"NONE"``/``"NULL"`` viram nulo.
* **Coordenadas** → vírgula decimal pt-BR → ponto; valores fora da *bounding box*
  do Estado de SP são anulados (a linha é mantida, mas sem geocodificação).
* **Datas** → ``to_datetime(dayfirst=True)``; valores inválidos viram ``NaT``.
* **Inteiros** → ``ANO/MES/ANO_BO/ID_DELEGACIA`` como ``Int64`` (nullable).
* **Deduplicação intra-chunk** → ``drop_duplicates(["NUM_BO", "ANO_BO"])``.

Cada chunk limpo é inserido em uma tabela DuckDB (``INSERT ... BY NAME``), o que
torna o append robusto a pequenas variações de ordem/colunas entre fontes. A
deduplicação *cross-chunk* acontece no ``pipeline`` via ``QUALIFY``.

``pandas`` é importado preguiçosamente (lazy).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from .config import SchemaConfig

if TYPE_CHECKING:  # pragma: no cover - apenas para type hints
    import duckdb
    import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 50_000

# Colunas tratadas de forma especial (só aplicadas se presentes no arquivo).
COORD_COLS = ("LATITUDE", "LONGITUDE")
DATE_COLS = (
    "DATA_OCORRENCIA_BO",
    "DATAHORA_REGISTRO_BO",
    "DATA_COMUNICACAO_BO",
    "DATAHORA_COMUNICACAO_BO",
)
INT_COLS = ("ANO", "MES", "ANO_BO", "ID_DELEGACIA")
DEDUP_KEYS = ("NUM_BO", "ANO_BO")

_NULL_TOKENS = {"", "NAN", "NONE", "NULL", "<NA>"}


class CleaningError(RuntimeError):
    """Falha durante a limpeza dos dados."""


def _to_coord(series: "pd.Series") -> "pd.Series":
    """Converte coordenada pt-BR (vírgula) para float."""
    import pandas as pd

    cleaned = series.astype("string").str.replace(",", ".", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def clean_dataframe(df: "pd.DataFrame", schema: SchemaConfig) -> "pd.DataFrame":
    """Aplica todas as transformações de limpeza a um DataFrame de strings."""
    import pandas as pd

    out = df.copy()

    # 1. Coordenadas (antes do upper genérico, pois viram numéricas).
    present_coords = [c for c in COORD_COLS if c in out.columns]
    for col in present_coords:
        out[col] = _to_coord(out[col])
    if {"LATITUDE", "LONGITUDE"}.issubset(out.columns):
        b = schema.bbox
        inside = (
            out["LATITUDE"].between(b.lat_min, b.lat_max)
            & out["LONGITUDE"].between(b.lon_min, b.lon_max)
        )
        # Anula coordenadas fora da bbox (mantém a linha).
        out.loc[~inside, ["LATITUDE", "LONGITUDE"]] = pd.NA

    # 2. Datas.
    for col in DATE_COLS:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], dayfirst=True, errors="coerce")

    # 3. Inteiros nullable.
    for col in INT_COLS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    # 4. Texto: padroniza qualquer coluna ainda do tipo objeto/string.
    for col in out.columns:
        if out[col].dtype == object or str(out[col].dtype) == "string":
            s = out[col].astype("string").str.strip().str.upper()
            s = s.mask(s.isin(_NULL_TOKENS), other=pd.NA)
            out[col] = s

    # 5. Deduplicação intra-chunk.
    keys = [k for k in DEDUP_KEYS if k in out.columns]
    if keys:
        out = out.drop_duplicates(subset=keys)

    return out


def clean_csv_into_table(
    csv_path: Path,
    con: "duckdb.DuckDBPyConnection",
    table: str,
    schema: SchemaConfig,
    *,
    sep: str = ";",
    encoding: str = "utf-8",
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> tuple[int, int]:
    """Lê o CSV em chunks, limpa e insere na tabela DuckDB ``table``.

    A tabela é criada (se ainda não existir) a partir do schema do primeiro
    chunk. Devolve ``(linhas_lidas, linhas_limpas)``.
    """
    import pandas as pd

    reader = pd.read_csv(
        csv_path,
        sep=sep,
        encoding=encoding,
        dtype=str,
        chunksize=chunk_size,
        on_bad_lines="skip",
        low_memory=False,
    )

    total_in = total_out = 0
    for i, chunk in enumerate(reader, start=1):
        total_in += len(chunk)
        cleaned = clean_dataframe(chunk, schema)
        total_out += len(cleaned)

        con.register("incoming_chunk", cleaned)
        # Cria a tabela com o schema do primeiro chunk (vazia), depois insere.
        con.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" AS '
            "SELECT * FROM incoming_chunk LIMIT 0"
        )
        con.execute(f'INSERT INTO "{table}" BY NAME SELECT * FROM incoming_chunk')
        con.unregister("incoming_chunk")

        logger.info(
            "Chunk %4d | bruto: %6d → limpo: %6d | acumulado: %8d",
            i, len(chunk), len(cleaned), total_out,
        )

    if total_in == 0:
        raise CleaningError(f"Nenhuma linha lida de {csv_path.name}.")

    return total_in, total_out
