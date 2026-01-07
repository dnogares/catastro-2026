import json
import os
from pathlib import Path
from typing import List, Optional
from fastapi import FastAPI, Form, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- IMPORTS CORREGIDOS ---
from config.paths import CAPAS_DIR, OUTPUTS_DIR
from catastro.catastro_downloader import CatastroDownloader
from catastro.lote_manager import LoteManager
from afecciones.vector_analyzer import VectorAnalyzer
from afecciones.pdf_generator import AfeccionesPDF
from analisisurbano import AnalizadorUrbanistico

from config.paths import CAPAS_DIR, OUTPUTS_DIR

app = FastAPI(title="Suite Tasación ", version="3.1")

# Crear directorios base SIEMPRE
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
CAPAS_DIR.mkdir(parents=True, exist_ok=True)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/outputs", StaticFiles(directory=str(OUTPUTS_DIR)), name="outputs")
# Inicialización de Clases
downloader = CatastroDownloader(output_dir=str(OUTPUTS_DIR))
analyzer = VectorAnalyzer(capas_dir=str(CAPAS_DIR))
pdf_gen = AfeccionesPDF(output_dir=str(OUTPUTS_DIR))
lote_manager = LoteManager(output_dir=str(OUTPUTS_DIR))
urban_analyzer = AnalizadorUrbanistico(output_base_dir=OUTPUTS_DIR)

# --- MODELOS DE DATOS ---
class PdfRequest(BaseModel):
    referencia: str
    incluir_mapa: bool = True
    incluir_afecciones: bool = True

@app.on_event("startup")
async def startup_event():
    """Ejecuta logs y validaciones al iniciar el servidor"""
    print("\n" + "="*50)
    print("🚀 Iniciando servidor Suite Tasación...")
    print(f"📁 Root Dir: {Path('.').absolute()}")
    print(f"📁 Outputs: {OUTPUTS_DIR.absolute()} (Existe: {OUTPUTS_DIR.exists()})")
    print(f"📁 Capas: {CAPAS_DIR.absolute()} (Existe: {CAPAS_DIR.exists()})")
    
    # Listar contenido de capas para depuración
    if CAPAS_DIR.exists():
        capas_encontradas = list(CAPAS_DIR.rglob("*.gpkg"))
        print(f"📂 Capas detectadas: {len(capas_encontradas)}")
        for c in capas_encontradas[:5]:
            print(f"  - {c.relative_to(CAPAS_DIR)}")
    else:
        print("⚠️ ADVERTENCIA: La carpeta de capas no existe o no es accesible")
    
    print("="*50 + "\n")
    print(f"🌐 Accede a: http://localhost:8090")

# --- RUTA PRINCIPAL ---
@app.get("/")
async def read_index():
    """Sirve la página principal"""
    static_index = Path("static/index.html")
    if static_index.exists():
        return FileResponse(static_index)
    else:
        return {"message": "API Suite Tasación activa", "version": "3.1"}

# --- ENDPOINTS ---

@app.get("/api/health")
async def health_check():
    """Endpoint de verificación de salud del servicio"""
    return {
        "status": "healthy",
        "version": "3.1",
        "outputs_dir": str(OUTPUTS_DIR.exists()),
        "capas_dir": str(CAPAS_DIR.exists())
    }

@app.post("/api/v1/analizar-parcela")
async def paso1_analizar(referencia: str = Form(...)):
    """
    Paso 1: Descarga datos catastrales y analiza afecciones
    """
    try:
        # Limpiar referencia
        ref_limpia = referencia.replace(' ', '').strip().upper()
        
        if len(ref_limpia) < 14:
            raise HTTPException(
                status_code=400, 
                detail="Referencia catastral inválida (mínimo 14 caracteres)"
            )

        # 1. Ejecutar análisis completo a través del módulo unificado
        result_urban = urban_analyzer.obtener_datos_catastrales(ref_limpia)
        
        if result_urban.get("status") == "error":
            raise HTTPException(
                status_code=404, 
                detail=result_urban.get("message", "Error procesando referencia")
            )

        # 2. Análisis de afecciones específico (Capa base)
        # Nota: Podríamos mover esto a AnalizadorUrbanistico más adelante
        gml_path = OUTPUTS_DIR / ref_limpia / "gml" / f"{ref_limpia}_parcela.gml"
        res_afecciones = analyzer.analizar(
            parcela_path=gml_path,
            gpkg_name="afecciones_totales.gpkg", 
            campo_clasificacion="tipo" 
        )

        # 3. Localizar mapa para el frontend
        mapa_disponible = None
        images_dir = OUTPUTS_DIR / ref_limpia / "images"
        posibles_mapas = [
            images_dir / f"{ref_limpia}_Catastro_zoom4_Parcela.png",
            images_dir / f"{ref_limpia}_composicion.png",
        ]
        
        for mapa in posibles_mapas:
            if mapa.exists():
                mapa_disponible = f"/outputs/{ref_limpia}/images/{mapa.name}"
                break

        return {
            "status": "success",
            "referencia": ref_limpia,
            "url_mapa_web": mapa_disponible,
            "afecciones": res_afecciones,
            "archivos_generados": {
                "gml": str(gml_path.exists()),
                "kml": result_urban.get("kml"),
                "zip": result_urban.get("zip")
            },
            "urban_data": result_urban.get("resumen")
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en analizar-parcela: {e}")
        return JSONResponse(
            status_code=500, 
            content={
                "status": "error",
                "error": str(e),
                "detail": "Error procesando la referencia catastral"
            }
        )

@app.post("/api/v1/generar-pdf")
async def paso2_generar_pdf(req: PdfRequest):
    """
    Paso 2: Genera PDF con mapas y afecciones
    """
    try:
        ref_limpia = req.referencia.replace(' ', '').strip().upper()
        ref_dir = OUTPUTS_DIR / ref_limpia
        
        if not ref_dir.exists():
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron datos para la referencia {ref_limpia}"
            )

        # Recopilar mapas disponibles
        mapas_a_incluir = []
        if req.incluir_mapa:
            images_dir = ref_dir / "images"
            if images_dir.exists():
                # Buscar mapas de parcela (zoom 4)
                for mapa_file in images_dir.glob(f"{ref_limpia}*zoom4*.png"):
                    mapas_a_incluir.append(str(mapa_file))
                
                # Si no hay zoom4, buscar cualquier composición
                if not mapas_a_incluir:
                    for mapa_file in images_dir.glob(f"{ref_limpia}*.png"):
                        mapas_a_incluir.append(str(mapa_file))
                        break  # Solo el primero

        # Análisis de afecciones
        resultados_afecciones = {}
        if req.incluir_afecciones:
            gml_path = ref_dir / "gml" / f"{ref_limpia}_parcela.gml"
            if gml_path.exists():
                try:
                    resultados_afecciones = analyzer.analizar(
                        gml_path, 
                        "afecciones_totales.gpkg", 
                        "tipo"
                    )
                except Exception as e:
                    print(f"⚠️ Error analizando afecciones para PDF: {e}")
                    resultados_afecciones = {}

        # Generar PDF
        print(f"📄 Generando PDF para: {ref_limpia}")
        pdf_path = pdf_gen.generar(
            referencia=ref_limpia,
            resultados=resultados_afecciones,
            mapas=mapas_a_incluir,
            incluir_tabla=req.incluir_afecciones
        )

        if not pdf_path or not Path(pdf_path).exists():
            raise HTTPException(
                status_code=500,
                detail="Error al generar el PDF"
            )

        pdf_filename = Path(pdf_path).name
        return {
            "status": "created",
            "pdf_url": f"/outputs/{ref_limpia}/{pdf_filename}",
            "mapas_incluidos": len(mapas_a_incluir),
            "afecciones_incluidas": bool(resultados_afecciones)
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en generar-pdf: {e}")
        return JSONResponse(
            status_code=500, 
            content={
                "status": "error",
                "error": str(e),
                "detail": "Error generando el PDF"
            }
        )

@app.post("/api/v1/lote")
async def procesar_lote_endpoint(
    file: UploadFile = File(...), 
    background_tasks: BackgroundTasks = None
):
    """
    Procesa un archivo con múltiples referencias catastrales
    Formato: una referencia por línea
    """
    try:
        # Leer archivo
        content = await file.read()
        decoded = content.decode("utf-8", errors="ignore")
        
        # Extraer referencias (una por línea)
        referencias = [
            line.strip().replace(' ', '').upper() 
            for line in decoded.splitlines() 
            if line.strip() and len(line.strip()) >= 14
        ]
        
        if not referencias:
            raise HTTPException(
                status_code=400, 
                detail="Archivo vacío o sin referencias válidas"
            )

        print(f"📦 Lote recibido: {len(referencias)} referencias")

        # Procesar en segundo plano
        if background_tasks:
            background_tasks.add_task(
                lote_manager.procesar_lista, 
                referencias, 
                downloader, 
                analyzer, 
                pdf_gen
            )
        else:
            # Si no hay background tasks, procesar directamente
            # (solo para desarrollo/testing)
            lote_manager.procesar_lista(referencias, downloader, analyzer, pdf_gen)
        
        return {
            "status": "processing",
            "mensaje": f"Procesando {len(referencias)} referencias en segundo plano",
            "lote_id": getattr(lote_manager, 'lote_id', 'N/A'),
            "referencias": referencias[:10]  # Solo primeras 10 para no saturar respuesta
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en lote: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": str(e),
                "detail": "Error procesando el lote"
            }
        )

@app.get("/api/v1/lote/{lote_id}/status")
async def obtener_estado_lote(lote_id: str):
    """
    Obtiene el estado de procesamiento de un lote
    """
    try:
        estado = lote_manager.obtener_estado(lote_id)
        
        if not estado:
            raise HTTPException(
                status_code=404,
                detail=f"Lote {lote_id} no encontrado"
            )
        
        return estado
        
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.get("/api/v1/lote/{lote_id}/resumen")
async def obtener_resumen_lote(lote_id: str):
    """
    Obtiene el resumen HTML de un lote procesado
    """
    try:
        lotes_dir = OUTPUTS_DIR / "_lotes"
        resumen_path = lotes_dir / f"{lote_id}_resumen.html"
        
        if not resumen_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Resumen del lote {lote_id} no encontrado"
            )
        
        return FileResponse(resumen_path)
        
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.post("/api/v1/referencia-simple")
async def procesar_referencia_simple(referencia: str = Form(...)):
    """
    Endpoint simplificado: solo descarga datos sin análisis
    Más rápido para obtener solo información catastral
    """
    try:
        ref_limpia = referencia.replace(' ', '').strip().upper()
        
        if len(ref_limpia) < 14:
            raise HTTPException(
                status_code=400,
                detail="Referencia catastral inválida"
            )

        print(f"📥 Descarga simple: {ref_limpia}")
        exito, zip_path = downloader.descargar_todo_completo(ref_limpia)
        
        if not exito:
            raise HTTPException(
                status_code=404,
                detail=f"No se pudieron descargar datos para {ref_limpia}"
            )

        ref_dir = OUTPUTS_DIR / ref_limpia
        
        # Recopilar archivos disponibles
        archivos_disponibles = {
            "gml": [],
            "pdf": [],
            "images": [],
            "json": []
        }
        
        for tipo in archivos_disponibles.keys():
            tipo_dir = ref_dir / tipo
            if tipo_dir.exists():
                archivos_disponibles[tipo] = [
                    f"/outputs/{ref_limpia}/{tipo}/{f.name}"
                    for f in tipo_dir.glob("*")
                    if f.is_file()
                ]
        
        return {
            "status": "success",
            "referencia": ref_limpia,
            "zip": f"/outputs/{ref_limpia}_completo.zip" if zip_path else None,
            "archivos": archivos_disponibles
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.get("/api/v1/referencia/{referencia}")
async def obtener_info_referencia(referencia: str):
    """
    Obtiene información de una referencia ya procesada
    """
    try:
        ref_limpia = referencia.replace(' ', '').strip().upper()
        ref_dir = OUTPUTS_DIR / ref_limpia
        
        if not ref_dir.exists():
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron datos para {ref_limpia}"
            )

        # Recopilar archivos disponibles
        info = {
            "referencia": ref_limpia,
            "archivos": {
                "gml_parcela": None,
                "gml_edificio": None,
                "ficha_catastral": None,
                "imagenes": [],
                "pdfs": [],
                "json": []
            }
        }

        # GML
        gml_dir = ref_dir / "gml"
        if gml_dir.exists():
            for gml_file in gml_dir.glob("*.gml"):
                if "parcela" in gml_file.name:
                    info["archivos"]["gml_parcela"] = f"/outputs/{ref_limpia}/gml/{gml_file.name}"
                elif "edificio" in gml_file.name:
                    info["archivos"]["gml_edificio"] = f"/outputs/{ref_limpia}/gml/{gml_file.name}"

        # PDFs
        pdf_dir = ref_dir / "pdf"
        if pdf_dir.exists():
            for pdf_file in pdf_dir.glob("*.pdf"):
                info["archivos"]["pdfs"].append(f"/outputs/{ref_limpia}/pdf/{pdf_file.name}")
                if "ficha_catastral" in pdf_file.name:
                    info["archivos"]["ficha_catastral"] = f"/outputs/{ref_limpia}/pdf/{pdf_file.name}"

        # Imágenes y Metadata
        images_dir = ref_dir / "images"
        if images_dir.exists():
            for img_file in images_dir.glob("*.png"):
                info["archivos"]["imagenes"].append(f"/outputs/{ref_limpia}/images/{img_file.name}")
            
            # Cargar metadata.json si existe
            metadata_path = images_dir / "metadata.json"
            if metadata_path.exists():
                try:
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        info["metadata_imagenes"] = json.load(f)
                except Exception as e:
                    print(f"⚠️ Error cargando metadata: {e}")
                    info["metadata_imagenes"] = {}

        # JSON
        json_dir = ref_dir / "json"
        if json_dir.exists():
            for json_file in json_dir.glob("*.json"):
                info["archivos"]["json"].append(f"/outputs/{ref_limpia}/json/{json_file.name}")

        return info

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.get("/api/v1/capas/geojson")
async def obtener_capa_vectorial_geojson(ruta: str):
    """
    Convierte una capa GPKG del volumen a GeoJSON para el visor
    """
    try:
        import json
        import geopandas as gpd
        
        # Seguridad: evitar path traversal
        if ".." in ruta or ruta.startswith("/"):
            raise HTTPException(status_code=400, detail="Ruta de capa inválida")
            
        capa_path = CAPAS_DIR / ruta
        
        if not capa_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Capa no encontrada en {ruta}"
            )
        
        # Leer GPKG y limitar a 5000 entidades por rendimiento
        gdf = gpd.read_file(capa_path, rows=5000)
        
        # Reproyectar a WGS84
        if gdf.crs and gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
        
        return json.loads(gdf.to_json())
        
    except Exception as e:
        print(f"❌ Error convirtiendo capa {ruta} a GeoJSON: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.get("/api/v1/referencia/{referencia}/geojson")
async def obtener_geojson(referencia: str):
    """
    Convierte GML de parcela a GeoJSON para visualización en el visor GIS
    """
    try:
        import json
        import geopandas as gpd
        
        ref_limpia = referencia.replace(' ', '').strip().upper()
        gml_path = OUTPUTS_DIR / ref_limpia / "gml" / f"{ref_limpia}_parcela.gml"
        
        if not gml_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"GML no encontrado para la referencia {ref_limpia}"
            )
        
        # Leer GML con GeoPandas y convertir a GeoJSON
        gdf = gpd.read_file(gml_path)
        
        # Reproyectar a WGS84 (EPSG:4326) para Leaflet
        if gdf.crs and gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
        
        # Convertir a GeoJSON
        geojson = json.loads(gdf.to_json())
        
        return geojson
        
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="GeoPandas no está disponible. Instala con: pip install geopandas"
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error convirtiendo GML a GeoJSON: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.get("/api/v1/referencia/{referencia}/kml")
async def obtener_kml(referencia: str, tipo: str = "parcela"):
    """
    Sirve el archivo KML generado para la referencia (parcela o edificio)
    """
    try:
        ref_limpia = referencia.replace(' ', '').strip().upper()
        kml_path = OUTPUTS_DIR / ref_limpia / "gml" / f"{ref_limpia}_{tipo}.kml"
        
        if not kml_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"KML de {tipo} no encontrado para la referencia {ref_limpia}"
            )
        
        return FileResponse(
            kml_path, 
            media_type="application/vnd.google-earth.kml+xml",
            filename=f"{ref_limpia}_{tipo}.kml"
        )
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

@app.post("/api/v1/analizar-afecciones")
async def analizar_afecciones_manual(
    archivos: List[UploadFile] = File(...),
    capas: str = Form("[\"afecciones_totales.gpkg\"]")
):
    """
    Endpoint para análisis manual de afecciones subiendo varios KML/GeoJSON
    """
    import tempfile
    import json
    
    try:
        # Parsear capas solicitadas
        capas_list = json.loads(capas)
        resultados_por_archivo = {}

        for file in archivos:
            # Guardar archivo temporal
            suffix = Path(file.filename).suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                content = await file.read()
                tmp.write(content)
                tmp_path = Path(tmp.name)
            
            resultados_capas = {}
            # Analizar contra cada capa
            for capa_name in capas_list:
                try:
                    res = analyzer.analizar(
                        parcela_path=tmp_path,
                        gpkg_name=capa_name,
                        campo_clasificacion="tipo"
                    )
                    resultados_capas[capa_name] = res
                except Exception as e:
                    resultados_capas[capa_name] = {"error": str(e)}
            
            resultados_por_archivo[file.filename] = resultados_capas
            # Limpiar temporal
            tmp_path.unlink()
        
        return {
            "status": "success",
            "archivos_procesados": len(archivos),
            "capas_analizadas": len(capas_list),
            "resultados": resultados_por_archivo
        }
        
    except Exception as e:
        print(f"❌ Error analizando afecciones manuales: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/capas-disponibles")
async def obtener_capas_disponibles():
    """
    Obtiene la lista de capas vectoriales disponibles en el volumen
    """
    try:
        capas_info = {
            "capas_vectoriales": [],
            "capas_wms": {
                "catastro": {
                    "nombre": "Catastro",
                    "descripcion": "Cartografía catastral",
                    "disponible": True
                },
                "pnoa": {
                    "nombre": "Ortofoto PNOA", 
                    "descripcion": "Ortofotografía de alta resolución",
                    "disponible": True
                }
            }
        }
        
        # Buscar capas vectoriales en el volumen
        if CAPAS_DIR.exists():
            for capa_file in CAPAS_DIR.rglob("*.gpkg"):
                if capa_file.is_file():
                    capas_info["capas_vectoriales"].append({
                        "nombre": capa_file.stem,
                        "archivo": capa_file.name,
                        "ruta": str(capa_file.relative_to(CAPAS_DIR)),
                        "tamano": capa_file.stat().st_size,
                        "tipo": "vectorial"
                    })
            
            # Buscar en subdirectorios
            for subdir in ["ambiental", "riesgos", "infraestructuras"]:
                subdir_path = CAPAS_DIR / subdir
                if subdir_path.exists():
                    for capa_file in subdir_path.rglob("*.gpkg"):
                        if capa_file.is_file():
                            capas_info["capas_vectoriales"].append({
                                "nombre": capa_file.stem,
                                "archivo": str(subdir_path / capa_file.name),
                                "ruta": str(subdir_path / capa_file.relative_to(subdir_path)),
                                "tamano": capa_file.stat().st_size,
                                "tipo": "vectorial",
                                "categoria": subdir
                            })
        
        return {
            "status": "success",
            "total_capas": len(capas_info["capas_vectoriales"]),
            "capas": capas_info
        }
        
    except Exception as e:
        print(f"❌ Error obteniendo capas disponibles: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )

# --- SERVIDOR ---
if __name__ == "__main__":
    import uvicorn
    
    # Asegurar carpetas base
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    CAPAS_DIR.mkdir(parents=True, exist_ok=True)
    
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8090, 
        reload=True
    )