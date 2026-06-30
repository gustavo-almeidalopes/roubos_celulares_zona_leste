"""
Download de arquivos remotos das fontes públicas.
==================================================

Baixa o arquivo apontado por ``Source.url`` para ``data/raw/`` com:

* **Retry com backoff exponencial** (resiliência a instabilidade do portal).
* **Streaming** em blocos (não carrega o arquivo inteiro na memória).
* **Fallback manual**: se a fonte não tem URL, ou o download falha mas já existe
  um arquivo local correspondente em ``data/raw/``, o pipeline segue com ele.

``requests`` é importado preguiçosamente (lazy) para que este módulo possa ser
importado em ambientes onde a dependência ainda não está instalada (ex.: apenas
checagem de sintaxe).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from .config import Source

logger = logging.getLogger(__name__)

_CHUNK_BYTES = 1 << 16  # 64 KiB por bloco
_DEFAULT_RETRIES = 3
_DEFAULT_TIMEOUT = 60  # segundos


class DownloadError(RuntimeError):
    """Falha irrecuperável ao baixar uma fonte."""


def _raw_filename(source: Source) -> str:
    """Nome canônico do arquivo bruto: ``<name>.<fmt>``."""
    return f"{source.name}.{source.fmt}"


def _existing_local_file(source: Source, raw_dir: Path) -> Path | None:
    """Procura um arquivo previamente baixado/colocado manualmente."""
    candidate = raw_dir / _raw_filename(source)
    if candidate.exists() and candidate.stat().st_size > 0:
        return candidate
    # Aceita também qualquer arquivo com o mesmo "name" e extensão compatível.
    for path in sorted(raw_dir.glob(f"{source.name}.*")):
        if path.suffix.lower().lstrip(".") in {"xlsx", "xls", "csv"} and path.stat().st_size > 0:
            return path
    return None


def _http_download(url: str, dest: Path, *, retries: int, timeout: int) -> None:
    """Baixa ``url`` para ``dest`` com retry/backoff. Lança ``DownloadError``."""
    import requests  # lazy import — só necessário em runtime

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            logger.info("Baixando %s (tentativa %d/%d)…", url, attempt, retries)
            with requests.get(url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                tmp = dest.with_suffix(dest.suffix + ".part")
                size = 0
                with open(tmp, "wb") as fh:
                    for block in resp.iter_content(chunk_size=_CHUNK_BYTES):
                        if block:
                            fh.write(block)
                            size += len(block)
                if size == 0:
                    raise DownloadError("Resposta vazia (0 bytes).")
                tmp.replace(dest)
                logger.info("Download concluído: %s (%.1f KB).", dest.name, size / 1024)
                return
        except Exception as exc:  # noqa: BLE001 — re-tentamos qualquer erro de rede
            last_err = exc
            logger.warning("Falha no download (tentativa %d): %s", attempt, exc)
            if attempt < retries:
                backoff = 2 ** attempt
                time.sleep(backoff)
    raise DownloadError(f"Não foi possível baixar {url}: {last_err}")


def fetch(source: Source, raw_dir: Path, *, retries: int = _DEFAULT_RETRIES,
          timeout: int = _DEFAULT_TIMEOUT) -> Path:
    """Garante um arquivo bruto local para ``source`` e devolve seu caminho.

    Estratégia:
      1. Se há ``url``: tenta baixar. Em sucesso, devolve o arquivo baixado.
      2. Se não há URL **ou** o download falha: usa um arquivo local existente
         (modo "drop manual"), se houver.
      3. Caso contrário, lança ``DownloadError``.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / _raw_filename(source)

    if source.url:
        try:
            _http_download(source.url, dest, retries=retries, timeout=timeout)
            return dest
        except DownloadError as exc:
            logger.warning(
                "Download de '%s' falhou (%s). Tentando arquivo local…", source.name, exc
            )

    local = _existing_local_file(source, raw_dir)
    if local is not None:
        logger.info("Usando arquivo local para '%s': %s", source.name, local.name)
        return local

    raise DownloadError(
        f"Fonte '{source.name}': sem URL utilizável e nenhum arquivo em "
        f"{raw_dir} (coloque '{_raw_filename(source)}' manualmente ou defina a URL no README)."
    )
