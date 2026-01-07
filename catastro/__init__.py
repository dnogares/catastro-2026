#!/usr/bin/env python3
"""
catastro/__init__.py
Módulo de descarga y procesamiento de datos catastrales
"""

from .catastro_downloader import CatastroDownloader
from .lote_manager import LoteManager

__all__ = ['CatastroDownloader', 'LoteManager']