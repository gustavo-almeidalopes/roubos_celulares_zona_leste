"""Permite executar o pacote como módulo: ``python -m src.data_pipeline run``."""

from __future__ import annotations

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
