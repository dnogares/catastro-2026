#!/usr/bin/env python3
"""
afecciones/__init__.py
Módulo de análisis de afecciones vectoriales y generación de PDFs
"""

from .vector_analyzer import VectorAnalyzer
from .pdf_generator import AfeccionesPDF

__all__ = ['VectorAnalyzer', 'AfeccionesPDF']