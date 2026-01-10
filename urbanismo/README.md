# Módulo de Análisis Urbanístico

Módulo especializado para análisis urbanístico de parcelas catastrales, integrado con el sistema SuiteTasacion.

## 🏗️ Características

- **Análisis urbanístico completo**: Clasificación de suelos y porcentajes de afectación
- **Integración WFS/WMS**: Descarga automática de datos del CARM (Región de Murcia)
- **Generación de mapas**: Ortofotos IGN + planificación urbanística
- **Caché optimizado**: Evita descargas repetidas
- **Formatos múltiples**: Exportación en TXT, CSV, PNG
- **Integración total**: Compatible con LoteManager y PDFGenerator

## 📁 Estructura del módulo

```
urbanismo/
├── __init__.py                 # Exportaciones principales
├── analisisurbano_mejorado.py  # Motor de análisis urbanístico
├── urbanismo_service.py         # Servicio de integración
├── ejemplo_integracion.py      # Ejemplos de uso
└── README.md                   # Este archivo
```

## 🚀 Uso básico

### Análisis individual

```python
from urbanismo import UrbanismoService

# Crear servicio
servicio = UrbanismoService(output_dir="resultados")

# Analizar parcela
resultados = servicio.analizar_parcela("parcela.geojson", "1234567VK1234S0001LL")

print(f"Área total: {resultados['area_parcela_m2']:.2f} m²")
print(f"Clasificaciones: {resultados['detalle']}")
```

### Procesamiento por lote

```python
from urbanismo import AnalisisUrbano

# Crear analizador
analizador = AnalisisUrbano(output_dir="resultados_urbanismo")

# Procesar directorio completo
resultados = analizador.procesar_lote("GEOJSONs")

for resultado in resultados:
    print(f"{resultado.referencia}: {resultado.area_total_m2:.2f} m²")
```

### Integración con el sistema principal

```python
from urbanismo import crear_servicio_urbanismo
from afecciones.pdf_generator import AfeccionesPDF

# En LoteManager
urbanismo_service = crear_servicio_urbanismo("resultados")
resultados_urbano = urbanismo_service.analizar_parcela(gml_path, referencia)

# En PDFGenerator
pdf_service = AfeccionesPDF(output_dir="pdfs")
pdf_path = pdf_service.generar(
    referencia=referencia,
    resultados=resultados_urbano,
    mapas=urbanismo_service.obtener_mapas(referencia)
)
```

## 📊 Formato de resultados

```python
{
    "total": 15.5,                    # Porcentaje total afectado
    "detalle": {                       # Detalle por clasificación
        "Suelo Urbano": 10.2,
        "Suelo Urbanizable - Sector": 5.3
    },
    "area_parcela_m2": 1000.0,        # Área total parcela
    "area_afectada_m2": 155.0,        # Área afectada
    "urbanismo": True,                 # Flag de análisis urbanístico
    "mapa_urbano": "ruta/al/mapa.png", # Mapa generado
    "referencia": "1234567VK1234S0001LL",
    "timestamp": "2024-01-10_15-30-00"
}
```

## 🗺️ Servicios utilizados

### WFS - CARM (Región de Murcia)
- **URL**: `https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wfs?`
- **Capa**: `SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun`
- **Formato**: GeoJSON

### WMS - IGN (Ortofotos)
- **URL**: `https://www.ign.es/wms-inspire/pnoa-ma`
- **Capa**: `OI.OrthoimageCoverage`
- **Formato**: JPEG

### WMS - CARM (Urbanismo)
- **URL**: `https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wms?`
- **Capa**: `SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun`
- **Formato**: PNG

## 📋 Requisitos

```bash
pip install owslib geopandas matplotlib requests pandas
```

Las dependencias ya están incluidas en `requirements.txt` del proyecto principal.

## 🔧 Configuración

### Personalizar URLs de servicios

```python
analizador = AnalisisUrbano(
    output_dir="resultados",
    encuadre_factor=4.0  # Factor de zoom para mapas
)

# Personalizar URLs
analizador.wfs_carm_url = "https://otro-servicio.com/wfs?"
analizador.wms_ign_url = "https://otro-ign.com/wms?"
```

### Factor de encuadre

```python
# Más cerca (menor zoom)
analizador.encuadre_factor = 2.0

# Más lejos (mayor zoom)
analizador.encuadre_factor = 6.0
```

## 📁 Directorios de trabajo

```
proyecto/
├── urbanismo/
│   ├── GEOJSONs/              # Archivos GeoJSON de entrada
│   └── RESULTADOS-MAPAS/      # Salidas del análisis
├── resultados/
│   ├── urbanismo/            # Resultados del servicio
│   │   ├── ref1_timestamp/
│   │   │   ├── ref1_mapa.png
│   │   │   ├── ref1_porcentajes.txt
│   │   │   └── ref1_porcentajes.csv
│   │   └── ref2_timestamp/
│   └── pdfs/                 # PDFs generados
└── catastro/                 # Datos catastrales
```

## 🎯 Ejemplos de uso

### 1. Análisis rápido

```bash
python urbanismo/analisisurbano_mejorado.py
```

### 2. Integración completa

```bash
python urbanismo/ejemplo_integracion.py 1234567VK1234S0001LL
```

### 3. Procesamiento por lote

```bash
python urbanismo/ejemplo_integracion.py ref1 ref2 ref3
```

## 🔍 Campos de datos

### Campos requeridos en capa WFS
- `clasificacion`: Tipo de suelo
- `ambito`: Ámbito de protección (opcional)
- `geometry`: Geometría

### Tipos de suelo detectados
- Suelo Urbano
- Suelo Urbanizable
- Suelo Urbanizable - Sector
- Suelo Urbanizable - Sistema General
- Suelo No Urbanizable - Protegido
- Suelo No Urbanizable - Común
- Suelo Rústico

## 🚨 Manejo de errores

El sistema incluye manejo robusto de errores:

- **Red**: Reintentos automáticos y timeouts
- **Datos**: Validación de campos requeridos
- **Archivos**: Verificación de existencia y formato
- **Servicios**: Detección de caídas de servicios WFS/WMS

## 📈 Optimizaciones

- **Caché inteligente**: Evita descargas repetidas
- **Archivos temporales**: Limpieza automática
- **Procesamiento paralelo**: Soporte para múltiples parcelas
- **Memoria eficiente**: Liberación de recursos

## 🔗 Integración con el sistema

### Con LoteManager

```python
# En lote_manager.py
from urbanismo import UrbanismoService

urbanismo_service = UrbanismoService(output_dir=f"{ref_dir}/urbanismo")
resultados_urbano = urbanismo_service.analizar_parcela(gml_path, referencia)
```

### Con PDFGenerator

```python
# En pdf_generator.py
mapas_urbanismo = urbanismo_service.obtener_mapas(referencia)
pdf_service.generar(referencia, resultados_urbano, mapas_urbanismo)
```

## 📞 Soporte

Para problemas o preguntas:

1. Revisa los logs del sistema
2. Verifica conexión a servicios WFS/WMS
3. Comprueba formato de archivos de entrada
4. Revisa dependencias en `requirements.txt`

## 📝 Notas

- El módulo está optimizado para la Región de Murcia (CARM)
- Se puede adaptar a otras comunidades cambiando las URLs
- Los resultados son compatibles con el formato del sistema principal
- Incluye manejo de coordenadas UTM y Web Mercator
