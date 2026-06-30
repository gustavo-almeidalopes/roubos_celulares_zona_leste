"""
Pipeline de ingestão de dados — Radar Segurança SP.
====================================================

Pacote modular que transforma planilhas públicas da SSP-SP (XLS/XLSX/CSV) no
``cleaned_data.parquet`` consumido pelo dashboard. Cada etapa vive em um módulo
com responsabilidade única (arquitetura limpa):

    config    → lê a configuração de fontes a partir do README.md
    download  → baixa os arquivos remotos (com fallback para arquivos locais)
    convert   → converte Excel (xls/xlsx) em CSV
    clean     → limpa e normaliza os dados em chunks
    validate  → valida schema e qualidade, gerando um relatório
    pipeline  → orquestra todas as etapas
    cli       → interface de linha de comando (``python -m src.data_pipeline``)

Uso programático::

    from data_pipeline.pipeline import run
    run()

Uso por CLI::

    python -m src.data_pipeline run
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "1.0.0"
