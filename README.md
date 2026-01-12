# Suite Tasación Catastral 2026

Herramienta avanzada para la descarga de datos catastrales, visualización GIS y análisis de afecciones urbanísticas.

## 🚀 Características

- **Descarga Completa**: Obtención automática de GML, Ficha Catastral, Ortofotos y planos.
- **Visor GIS**: Mapa interactivo basado en Leaflet con capas oficiales (Catastro, PNOA, Hidrografía).
- **Conversión KML**: Generación automática de archivos KML para Google Earth.
- **Análisis de Afecciones**:
  - Cruce espacial contra capas vectoriales (GPKG).
  - Herramienta de carga de archivos externos (KML/GeoJSON).
- **Generación de Informes**: Creación de PDFs profesionales con mapas y resultados del análisis.
- **Refactorización 2026**: Código modularizado y optimizado para mejor mantenimiento.

## 🛠️ Instalación

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/dnogares/catastro-2026.git
   cd catastro-2026
   ```

2. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```

3. Ejecutar la aplicación:
   ```bash
   python main.py
   ```
  O usando uvicorn:
  ```bash
  uvicorn main:app --reload --port 81
  ```

## 🐳 Docker (Recomendado)

```bash
docker build -t catastro-tool .
docker run -p 81:81 -v $(pwd)/outputs:/app/outputs -v $(pwd)/capas:/app/capas catastro-tool
```

## 📂 Estructura del Proyecto

- `main.py`: Punto de entrada de la API FastAPI.
- `catastro/`: Módulos de descarga y gestión catastral.
- `afecciones/`: Analizador espacial y generador de informes.
- `static/`: Frontend (HTML/JS/CSS).
- `config/`: Configuraciones de rutas y arquitectura.

---
Desarrollado para el proyecto Tasación 2026.
