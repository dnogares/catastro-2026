#!/usr/bin/env python3
"""
config/paths.py
Configuración centralizada de rutas del proyecto
"""

from pathlib import Path

# Directorio raíz del proyecto
PROJECT_ROOT = Path(__file__).parent.parent

# Directorios principales
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
CAPAS_DIR = PROJECT_ROOT / "capas"
STATIC_DIR = PROJECT_ROOT / "static"
TEMP_DIR = PROJECT_ROOT / "temp"

# Subdirectorios de capas (archivos vectoriales base)
CAPAS_AMBIENTAL_DIR = CAPAS_DIR / "ambiental"
CAPAS_RIESGOS_DIR = CAPAS_DIR / "riesgos"
CAPAS_INFRAESTRUCTURAS_DIR = CAPAS_DIR / "infraestructuras"

# Crear directorios si no existen
def inicializar_directorios():
    """Crea todos los directorios necesarios"""
    directorios = [
        OUTPUTS_DIR,
        CAPAS_DIR,
        CAPAS_AMBIENTAL_DIR,
        CAPAS_RIESGOS_DIR,
        CAPAS_INFRAESTRUCTURAS_DIR,
        STATIC_DIR,
        TEMP_DIR
    ]
    
    for directorio in directorios:
        directorio.mkdir(parents=True, exist_ok=True)
        
    print(f"✅ Directorios inicializados en: {PROJECT_ROOT}")

# Rutas de archivos de capas comunes (si existen)
GPKG_AFECCIONES = CAPAS_DIR / "afecciones_totales.gpkg"
GPKG_ESPACIOS_NATURALES = CAPAS_AMBIENTAL_DIR / "espacios_naturales.gpkg"
GPKG_ZONAS_INUNDABLES = CAPAS_RIESGOS_DIR / "zonas_inundables.gpkg"

# Ejecutar al importar
if __name__ == "__main__":
    inicializar_directorios()
else:
    # Auto-inicializar al importar
    try:
        inicializar_directorios()
    except Exception as e:
        print(f"⚠️ Advertencia: No se pudieron crear todos los directorios: {e}")