# 🏢 Suite Tasación dnogares

Sistema completo de descarga, análisis y generación de informes de datos catastrales.

## 🎯 Características

- ✅ **Descarga completa de datos catastrales** (GML, PDF, mapas)
- ✅ **Generación de ortofotos multi-escala** (4 niveles de zoom)
- ✅ **Análisis de afecciones vectoriales** (con GeoPandas)
- ✅ **Generación de PDFs profesionales** (con ReportLab)
- ✅ **Procesamiento por lotes** (múltiples referencias)
- ✅ **API REST con FastAPI**
- ✅ **Interfaz web moderna** (HTML/JS)

---

## 📁 Estructura del Proyecto

```
proyecto/
├── main.py                          # API FastAPI
├── test_sistema.py                  # Script de pruebas
├── README.md                        # Este archivo
│
├── config/
│   ├── __init__.py
│   └── paths.py                     # Configuración de rutas
│
├── catastro/
│   ├── __init__.py
│   ├── catastro_downloader.py      # Descargador completo
│   └── lote_manager.py             # Gestor de lotes
│
├── afecciones/
│   ├── __init__.py
│   ├── vector_analyzer.py          # Analizador vectorial
│   └── pdf_generator.py            # Generador de PDFs
│
├── static/
│   └── index.html                  # Interfaz web
│
├── outputs/                         # Generado automáticamente
│   ├── {referencia}/
│   │   ├── json/
│   │   ├── html/
│   │   ├── gml/
│   │   ├── images/
│   │   └── pdf/
│   └── _lotes/
│
└── capas/                          # Capas vectoriales base
    └── gpkg/
        └── afecciones_totales.gpkg
```

---

## 🚀 Instalación

### 1. Requisitos Previos

- Python 3.8 o superior
- pip

### 2. Instalar Dependencias

```bash
# Dependencias básicas
pip install fastapi uvicorn requests

# Procesamiento geoespacial
pip install geopandas shapely

# Generación de PDFs
pip install reportlab pillow

# Opcional (para tests)
pip install httpx pandas
```

O instalar todo de una vez:

```bash
pip install fastapi uvicorn requests geopandas shapely reportlab pillow httpx pandas
```

### 3. Crear Estructura de Archivos

```bash
# Crear directorios
mkdir -p config catastro afecciones static outputs capas/gpkg

# Crear archivos __init__.py
touch config/__init__.py
touch catastro/__init__.py
touch afecciones/__init__.py
```

### 4. Copiar Archivos

Copia los siguientes archivos de los artifacts generados:

1. `main.py` → raíz del proyecto
2. `config/paths.py`
3. `catastro/catastro_downloader.py`
4. `catastro/lote_manager.py`
5. `afecciones/vector_analyzer.py`
6. `afecciones/pdf_generator.py`
7. `static/index.html`
8. `test_sistema.py` (opcional, para pruebas)

---

## 🧪 Verificar Instalación

```bash
python test_sistema.py
```

Este script verificará:
- ✅ Imports de módulos
- ✅ Dependencias instaladas
- ✅ Estructura de directorios
- ✅ Configuración de API
- ✅ (Opcional) Descarga de referencia real

---

## 🎮 Uso

### Opción 1: Interfaz Web

```bash
# Iniciar servidor
python main.py

# Abrir navegador
http://localhost:8090
```

La interfaz web permite:
- 📋 Procesar referencias únicas
- 📦 Subir archivos con lotes
- 📄 Generar PDFs personalizados
- 📊 Consultar estado de lotes

### Opción 2: API REST

#### Analizar una referencia

```bash
curl -X POST "http://localhost:8090/api/v1/analizar-parcela" \
     -F "referencia=1234567VK1234S0001WX"
```

#### Procesar lote (archivo .txt)

```bash
curl -X POST "http://localhost:8090/api/v1/lote" \
     -F "file=@referencias.txt"
```

Formato del archivo:
```
1234567VK1234S0001WX
9876543AB9876N0001YZ
5555555CD5555M0001AB
```

#### Generar PDF

```bash
curl -X POST "http://localhost:8090/api/v1/generar-pdf" \
     -H "Content-Type: application/json" \
     -d '{
       "referencia": "1234567VK1234S0001WX",
       "incluir_mapa": true,
       "incluir_afecciones": true
     }'
```

#### Consultar estado de lote

```bash
curl "http://localhost:8090/api/v1/lote/lote_20250107_143022/status"
```

### Opción 3: Uso Programático

```python
from catastro.catastro_downloader import CatastroDownloader

# Crear downloader
downloader = CatastroDownloader(output_dir="outputs")

# Descargar datos
exito, zip_path = downloader.descargar_todo_completo("1234567VK1234S0001WX")

if exito:
    print(f"✅ Datos descargados: {zip_path}")
```

---

## 📊 API Endpoints

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/` | GET | Interfaz web |
| `/api/health` | GET | Estado del servicio |
| `/api/v1/analizar-parcela` | POST | Procesar referencia única |
| `/api/v1/referencia-simple` | POST | Descarga rápida sin análisis |
| `/api/v1/generar-pdf` | POST | Generar PDF de informe |
| `/api/v1/lote` | POST | Procesar lote de referencias |
| `/api/v1/lote/{id}/status` | GET | Estado de procesamiento |
| `/api/v1/lote/{id}/resumen` | GET | Resumen HTML del lote |
| `/api/v1/referencia/{ref}` | GET | Info de referencia procesada |

---

## 📦 Archivos Generados

Para cada referencia catastral se genera:

```
outputs/
└── {REFERENCIA}/
    ├── json/
    │   ├── {REF}_info.json
    │   └── {REF}_consulta_descriptiva.json
    ├── html/
    │   └── {REF}_info.html
    ├── gml/
    │   ├── {REF}_parcela.gml          # ← Geometría para análisis
    │   └── {REF}_edificio.gml
    ├── images/
    │   ├── {REF}_Ortofoto_zoom1_Nacional.png
    │   ├── {REF}_Ortofoto_zoom2_Regional.png
    │   ├── {REF}_Ortofoto_zoom3_Local.png
    │   ├── {REF}_Ortofoto_zoom4_Parcela.png
    │   ├── {REF}_Catastro_zoom4_Parcela.png
    │   ├── {REF}_Callejero_zoom4_Parcela.png
    │   └── {REF}_Silueta_zoom4_Parcela.png
    └── pdf/
        └── {REF}_ficha_catastral.pdf

└── {REFERENCIA}_completo.zip         # ← Todo comprimido
```

---

## 🔧 Configuración Avanzada

### Cambiar puertos

Editar `main.py`:

```python
uvicorn.run(app, host="0.0.0.0", port=8080)  # Cambiar puerto
```

### Cambiar directorios

Editar `config/paths.py`:

```python
OUTPUTS_DIR = PROJECT_ROOT / "mis_outputs"
CAPAS_DIR = PROJECT_ROOT / "mis_capas"
```

### Añadir capas de afecciones

1. Coloca tu archivo GPKG en: `capas/gpkg/`
2. Usa `VectorAnalyzer` para analizarlo:

```python
from afecciones.vector_analyzer import VectorAnalyzer

analyzer = VectorAnalyzer(capas_dir="capas")
resultado = analyzer.analizar(
    parcela_path="outputs/{REF}/gml/{REF}_parcela.gml",
    gpkg_name="mi_capa.gpkg",
    campo_clasificacion="tipo"
)
```

---

## 🐛 Solución de Problemas

### Error: "ModuleNotFoundError"

```bash
# Verificar que estás en el directorio correcto
pwd

# Verificar estructura de archivos
ls -la catastro/
ls -la afecciones/

# Reinstalar dependencias
pip install -r requirements.txt
```

### Error: "Permission denied"

```bash
# En Linux/Mac, dar permisos de ejecución
chmod +x main.py
chmod +x test_sistema.py
```

### Error: "GeoPandas no disponible"

El sistema funcionará sin GeoPandas pero con funcionalidad limitada:
- ❌ No habrá análisis de afecciones
- ❌ No se generarán siluetas vectoriales
- ✅ Seguirá descargando mapas y datos básicos

Para instalar GeoPandas:

```bash
# En Linux/Mac
pip install geopandas

# En Windows (puede requerir conda)
conda install geopandas
```

---

## 📝 Notas Importantes

1. **Uso responsable**: No abuses de los servicios del Catastro
2. **Rate limiting**: Se incluyen pausas entre peticiones
3. **Referencias válidas**: Usa referencias catastrales reales de 14-20 caracteres
4. **Espacio en disco**: Cada referencia genera ~10-50 MB de datos
5. **Tiempos**: Una referencia tarda ~30-60 segundos en procesarse

---

## 📄 Licencia

Este proyecto es para uso educativo y profesional.  
Los datos catastrales pertenecen a la Dirección General del Catastro.

---

## 🤝 Contribuir

Para reportar problemas o sugerir mejoras, contacta al desarrollador.

---

## 📞 Soporte

- **Email**: soporte@dnogares.com
- **Documentación**: Ver artifacts generados
- **Tests**: `python test_sistema.py`

---

**Desarrollado con ❤️ para dnogares**
