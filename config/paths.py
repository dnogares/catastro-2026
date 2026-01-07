#!/usr/bin/env python3
"""
config/paths.py
Configuración centralizada de rutas del proyecto
Compatible con Docker / Easypanel
"""

from pathlib import Path

# Raíz de datos persistentes (montar volumen aquí)
DATA_ROOT = Path("/data")

# Directorios principales
OUTPUTS_DIR = DATA_ROOT / "outputs"
CAPAS_DIR = DATA_ROOT / "capas"
STATIC_DIR = DATA_ROOT / "static"
TEMP_DIR = DATA_ROOT / "temp"

# Subdirectorios de capas
CAPAS_AMBIENTAL_DIR = CAPAS_DIR / "ambiental"
CAPAS_RIESGOS_DIR = CAPAS_DIR / "riesgos"
CAPAS_INFRAESTRUCTURAS_DIR = CAPAS_DIR / "infraestructuras"

def inicializar_directorios():
    """Crea todos los directorios necesarios"""
    directorios = [
        OUTPUTS_DIR,
        CAPAS_DIR,
        CAPAS_AMBIENTAL_DIR,
        CAPAS_RIESGOS_DIR,
        CAPAS_INFRAESTRUCTURAS_DIR,
        STATIC_DIR,
        TEMP_DIR,
    ]

    for directorio in directorios:
        directorio.mkdir(parents=True, exist_ok=True)

    print(f"✅ Directorios inicializados en {DATA_ROOT}")
