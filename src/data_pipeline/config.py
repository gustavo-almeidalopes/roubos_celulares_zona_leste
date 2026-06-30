"""
Leitura da configuração de fontes a partir do README.md.
=========================================================

O README é a **fonte única de configuração** (requisito do projeto): ele contém
um bloco ``json`` que descreve as fontes de dados e os parâmetros de schema. Este
módulo localiza esse bloco, faz o parsing e devolve dataclasses tipadas.

Formato esperado no README (primeiro bloco ```json``` que contenha a chave
``sources``)::

    {
      "sources": [
        {"name": "celulares_subtraidos",
         "url": "https://.../CelularesSubtraidos.xlsx",
         "format": "xlsx", "enabled": true,
         "sheet": 0, "sep": ";", "encoding": "utf-8"}
      ],
      "schema": {
        "bbox": {"lat_min": -25.5, "lat_max": -19.5,
                 "lon_min": -53.0, "lon_max": -44.0}
      }
    }

Se o bloco não existir, devolvemos uma configuração padrão (sem fontes remotas) —
o pipeline ainda funciona em modo "drop manual", processando arquivos colocados
em ``data/raw/``.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Formatos de arquivo aceitos como entrada do pipeline.
SUPPORTED_FORMATS = ("xlsx", "xls", "csv")

# Bounding box padrão do Estado de São Paulo (descarta coordenadas fora dele).
_DEFAULT_BBOX = {
    "lat_min": -25.5,
    "lat_max": -19.5,
    "lon_min": -53.0,
    "lon_max": -44.0,
}


class ConfigError(ValueError):
    """Erro de configuração inválida no README."""


@dataclass(frozen=True)
class BBox:
    """Caixa delimitadora geográfica para validar coordenadas."""

    lat_min: float = _DEFAULT_BBOX["lat_min"]
    lat_max: float = _DEFAULT_BBOX["lat_max"]
    lon_min: float = _DEFAULT_BBOX["lon_min"]
    lon_max: float = _DEFAULT_BBOX["lon_max"]


@dataclass(frozen=True)
class Source:
    """Uma fonte de dados declarada no README."""

    name: str
    fmt: str
    url: str | None = None
    enabled: bool = True
    sheet: int | str = 0
    sep: str = ";"
    encoding: str = "utf-8"


@dataclass
class SchemaConfig:
    """Parâmetros de limpeza/validação derivados do README."""

    bbox: BBox = field(default_factory=BBox)


@dataclass
class PipelineConfig:
    """Configuração completa do pipeline."""

    sources: list[Source] = field(default_factory=list)
    schema: SchemaConfig = field(default_factory=SchemaConfig)

    def enabled_sources(self) -> list[Source]:
        return [s for s in self.sources if s.enabled]


# ── Parsing do README ─────────────────────────────────────────────────────────

_JSON_BLOCK_RE = re.compile(r"```json\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def _extract_config_block(markdown: str) -> dict | None:
    """Devolve o primeiro bloco ```json``` que contenha a chave ``sources``."""
    for raw in _JSON_BLOCK_RE.findall(markdown):
        snippet = raw.strip()
        try:
            data = json.loads(snippet)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and "sources" in data:
            return data
    return None


def _parse_source(item: dict) -> Source:
    if "name" not in item:
        raise ConfigError("Cada fonte precisa de um campo 'name'.")
    fmt = str(item.get("format", "")).lower().strip()
    if fmt not in SUPPORTED_FORMATS:
        raise ConfigError(
            f"Fonte '{item['name']}': formato '{fmt}' inválido. "
            f"Use um de: {', '.join(SUPPORTED_FORMATS)}."
        )
    # Só aceitamos URLs http(s) reais. Placeholders (ex.: "<COLE A URL...>") ou
    # strings vazias viram None → o pipeline cai no modo "drop manual".
    raw_url = str(item.get("url", "")).strip()
    url = raw_url if raw_url.lower().startswith(("http://", "https://")) else None

    return Source(
        name=str(item["name"]).strip(),
        fmt=fmt,
        url=url,
        enabled=bool(item.get("enabled", True)),
        sheet=item.get("sheet", 0),
        sep=str(item.get("sep", ";")),
        encoding=str(item.get("encoding", "utf-8")),
    )


def _parse_schema(data: dict) -> SchemaConfig:
    schema_raw = data.get("schema", {}) or {}
    bbox_raw = schema_raw.get("bbox", {}) or {}
    bbox = BBox(
        lat_min=float(bbox_raw.get("lat_min", _DEFAULT_BBOX["lat_min"])),
        lat_max=float(bbox_raw.get("lat_max", _DEFAULT_BBOX["lat_max"])),
        lon_min=float(bbox_raw.get("lon_min", _DEFAULT_BBOX["lon_min"])),
        lon_max=float(bbox_raw.get("lon_max", _DEFAULT_BBOX["lon_max"])),
    )
    return SchemaConfig(bbox=bbox)


def load_config(readme_path: str | Path) -> PipelineConfig:
    """Lê o README e devolve a ``PipelineConfig``.

    Nunca lança por ausência do bloco — apenas loga e devolve config padrão,
    permitindo o modo "drop manual".
    """
    path = Path(readme_path)
    if not path.exists():
        logger.warning("README não encontrado em %s — usando configuração padrão.", path)
        return PipelineConfig()

    markdown = path.read_text(encoding="utf-8")
    data = _extract_config_block(markdown)
    if data is None:
        logger.warning(
            "Nenhum bloco de configuração ```json``` com 'sources' no README. "
            "Pipeline rodará apenas com arquivos locais de data/raw/."
        )
        return PipelineConfig()

    sources = [_parse_source(item) for item in data.get("sources", [])]
    schema = _parse_schema(data)
    logger.info(
        "Config carregada do README: %d fonte(s) (%d habilitada(s)).",
        len(sources),
        sum(1 for s in sources if s.enabled),
    )
    return PipelineConfig(sources=sources, schema=schema)
