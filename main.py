import afecciones.vector_analyzer
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
from urbanismo import UrbanismoService

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
print(f"✅ Analyzer inicializado. Capas en: {CAPAS_DIR}")
pdf_gen = AfeccionesPDF(output_dir=str(OUTPUTS_DIR))
lote_manager = LoteManager(output_dir=str(OUTPUTS_DIR))
urbanismo_service = UrbanismoService(output_base_dir=str(OUTPUTS_DIR))

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


def get_all_vector_layers(base_dir: Path) -> List[Path]:
    """Busca recursivamente capas vectoriales en el directorio."""
    layers = []
    extensions = {".gpkg", ".geojson", ".shp", ".gml", ".kml", ".json"}
    
    if not base_dir.exists():
        return layers
        
    for item in base_dir.rglob("*"):
        if item.is_file() and item.suffix.lower() in extensions:
            # Excluir archivos de configuración o auxiliares
            if "leyenda" in item.name.lower() or "titulo" in item.name.lower() or "cpg" in item.suffix.lower():
                continue
            layers.append(item)
    return layers

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

def generar_csv_tecnico(referencia, urban_data, aff_data, output_dir):
    """Genera un CSV con todos los datos técnicos del análisis."""
    import csv
    from datetime import datetime
    
    filepath = output_dir / f"{referencia}_datos_tecnicos.csv"
    
    # Estructura base
    data = {}
    
    # 1. Datos Identificativos
    data["Referencia"] = referencia
    data["Fecha_Analisis"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 2. Datos Catastrales (Area)
    # Intentar obtener área de urbanismo o afecciones
    area = 0.0
    if urban_data and not urban_data.get("error"):
        area = urban_data.get("area_parcela_m2", 0)
    if area == 0 and aff_data:
        area = aff_data.get("area_parcela_m2", aff_data.get("area_total_m2", 0))
    
    data["Area_Parcela_m2"] = round(area, 2)
    
    # 3. Datos Urbanísticos
    if urban_data and not urban_data.get("error") and urban_data.get("urbanismo"):
        # Detalles (suelen ser porcentajes)
        for k, v in urban_data.get("detalle", {}).items():
            key_name = f"URB_{k.replace(' ', '_')}_pct"
            data[key_name] = v
            # Calcular área aprox
            data[f"URB_{k.replace(' ', '_')}_m2"] = round((v / 100) * area, 2)
    
    # 4. Afecciones Vectoriales
    if aff_data:
        data["Afecciones_Total_Max_pct"] = aff_data.get("total", 0)
        
        # Detalles (en paso1_analizar guardamos áreas en 'detalle')
        for k, v in aff_data.get("detalle", {}).items():
            # k es "Capa - Clase"
            clean_key = f"AF_{k}".replace(" ", "_").replace("-", "_").replace("__", "_")
            data[f"{clean_key}_m2"] = v
            # Calcular porcentaje
            if area > 0:
                data[f"{clean_key}_pct"] = round((v / area) * 100, 2)
            else:
                data[f"{clean_key}_pct"] = 0.0

    # Escribir CSV (Vertical key-value para legibilidad técnica, o horizontal?)
    # El usuario pide "un csv con todos los datos". Formato tabla horizontal es estándar.
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Cabecera
            writer.writerow(data.keys())
            # Valores
            writer.writerow(data.values())
        return str(filepath)
    except Exception as e:
        print(f"⚠️ Error generando CSV técnico: {e}")
        return None

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

        # 1. Descargar datos catastrales
        exito, zip_path = downloader.descargar_todo_completo(ref_limpia)
        
        if not exito:
            raise HTTPException(
                status_code=404, 
                detail=f"No se pudieron descargar datos catastrales para {ref_limpia}"
            )
        
        # 2. Análisis urbanístico si hay GML disponible
        result_urban = {}
        ref_dir = OUTPUTS_DIR / ref_limpia
        gml_path = ref_dir / "gml" / f"{ref_limpia}_parcela.gml"
        
        if gml_path.exists():
            try:
                result_urban = urbanismo_service.analizar_parcela(str(gml_path), ref_limpia)
            except Exception as e:
                print(f"⚠️ Error en análisis urbanístico: {e}")
                result_urban = {"error": str(e), "urbanismo": False}
        else:
            result_urban = {"error": "GML no disponible", "urbanismo": False}

        # 2. Análisis de afecciones MULTI-CAPA
        images_dir = ref_dir / "images"
        
        # Buscar todas las capas disponibles
        todas_capas = get_all_vector_layers(CAPAS_DIR)
        print(f"🔍 Analizando parcelas contra {len(todas_capas)} capas encontradas.")

        res_afecciones = {
            "detalle": {},
            "total": 0.0,
            "area_total_m2": 0.0,
            "afecciones_detectadas": False
        }
        
        max_afeccion = 0.0
        
        for capa_path in todas_capas:
            try:
                # Analizar capa individual
                res_capa = analyzer.analizar(
                    parcela_path=gml_path,
                    capa_input=capa_path,
                    campo_clasificacion="tipo" 
                )
                
                # Si hay error o no hay intersección, continuar
                if "error" in res_capa or not res_capa.get("afecciones_detectadas"):
                    continue
                
                # Actualizar área total (debería ser la misma siempre, tomamos la primera válida)
                if res_afecciones["area_total_m2"] == 0:
                    res_afecciones["area_total_m2"] = res_capa.get("area_parcela_m2", 0)
                
                # Agregar detalles
                nombre_capa = capa_path.stem
                if res_capa.get("afecciones"):
                    res_afecciones["afecciones_detectadas"] = True
                    for af in res_capa["afecciones"]:
                        clave = f"{nombre_capa} - {af.get('clase', 'General')}"
                        res_afecciones["detalle"][clave] = af.get("area_m2", 0)
                    
                    # Trackear máxima afectación encontrada
                    total_capa = res_capa.get("total_afectado_percent", 0)
                    if total_capa > max_afeccion:
                        max_afeccion = total_capa
                        
            except Exception as e:
                print(f"⚠️ Error analizando capa {capa_path.name}: {e}")
                
        # Asignar la máxima afectación como 'total' (proxy seguro sin hacer union geométrica compleja en runtime)
        res_afecciones["total"] = max_afeccion
        if not res_afecciones["detalle"]:
            res_afecciones["mensaje"] = "No se detectaron intersecciones con las capas disponibles."

        # 3. Generar "Plano Perfecto" para el informe
        plano_path = images_dir / f"{ref_limpia}_plano_perfecto.jpg"
        if gml_path.exists():
            downloader.generar_plano_perfecto(
                gml_path=gml_path,
                output_path=plano_path,
                ref=ref_limpia,
                info_afecciones=res_afecciones
            )
        
        # 4. Generar CSV Técnico Consolidado
        csv_path = generar_csv_tecnico(ref_limpia, result_urban, res_afecciones, ref_dir)

        # 5. Localizar mapa para el frontend
        mapa_disponible = None
        posibles_mapas = [
            plano_path,
            images_dir / f"{ref_limpia}_Catastro_zoom4_Parcela.png",
            images_dir / f"{ref_limpia}_composicion.png",
        ]
        
        for mapa in posibles_mapas:
            if mapa.exists():
                mapa_disponible = f"/outputs/{ref_limpia}/images/{mapa.name}"
                break
        
        response_data = {
            "referencia": ref_limpia,
            "status": "success",
            "datos_urbanos": result_urban,
            "afecciones": res_afecciones,
            "pixel_afecciones": result_urban.get("pixel_afecciones", {}), # Inyectar datos matriciales
            "url_mapa_web": mapa_disponible,
            "archivos_generados": {
                "zip": f"/outputs/{ref_limpia}_completo.zip",
                "kml": f"/outputs/{ref_limpia}/gml/{ref_limpia}_parcela.kml",
                "pdf_ficha": f"/outputs/{ref_limpia}/pdf/{ref_limpia}_ficha_catastral.pdf"
            }
        }
        
        if csv_path:
            response_data["archivos_generados"]["csv_tecnico"] = f"/outputs/{ref_limpia}/{Path(csv_path).name}"
            
        return response_data

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

        # Análisis de afecciones MULTI-CAPA
        resultados_afecciones = {}
        if req.incluir_afecciones:
            gml_path = ref_dir / "gml" / f"{ref_limpia}_parcela.gml"
            if gml_path.exists():
                try:
                    todas_capas = get_all_vector_layers(CAPAS_DIR)
                    print(f"📄 PDF Afecciones: analizando contra {len(todas_capas)} capas")
                    
                    resultados_afecciones = {
                        "detalle": {},
                        "total": 0.0, 
                        "area_total_m2": 0.0,
                        "area_afectada_m2": 0.0
                    }
                    max_afeccion_pct = 0.0
                    max_afeccion_area = 0.0

                    for capa_path in todas_capas:
                        try:
                            # Analizar capa
                            res_capa = analyzer.analizar(
                                gml_path, 
                                capa_path, 
                                "tipo"
                            )
                            
                            if "error" in res_capa or not res_capa.get("afecciones_detectadas"):
                                continue

                            # Setear área total de parcela una sola vez
                            if resultados_afecciones["area_total_m2"] == 0:
                                resultados_afecciones["area_total_m2"] = res_capa.get("area_parcela_m2", 0)

                            # Agregar detalles
                            nombre_capa = capa_path.stem
                            for af in res_capa.get("afecciones", []):
                                clave = f"{nombre_capa} - {af.get('clase', 'General')}"
                                # PDF Generator espera porcentajes en 'detalle'
                                resultados_afecciones["detalle"][clave] = af.get("porcentaje", 0)

                            # Calcular máximos para resumen
                            total_capa_pct = res_capa.get("total_afectado_percent", 0)
                            total_capa_area = res_capa.get("total_afectado_m2", 0)
                            
                            if total_capa_pct > max_afeccion_pct:
                                max_afeccion_pct = total_capa_pct
                                max_afeccion_area = total_capa_area

                        except Exception as e:
                            print(f"⚠️ Error capa PDF {capa_path.name}: {e}")
                    
                    # Asignar máximos (Peor caso)
                    resultados_afecciones["total"] = max_afeccion_pct
                    resultados_afecciones["area_afectada_m2"] = max_afeccion_area

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
            # Analizar contra cada capa
            for capa_name in capas_list:
                try:
                    # Si piden "afecciones_totales", analizamos TODO lo que haya en el sistema
                    if capa_name == "afecciones_totales.gpkg":
                        todas = get_all_vector_layers(CAPAS_DIR)
                        res_total = {
                            "afecciones": [],
                            "total_afectado_percent": 0.0,
                            "afecciones_detectadas": False,
                            "mensaje": f"Análisis completo contra {len(todas)} capas del sistema"
                        }
                        max_pct = 0.0
                        
                        for c_path in todas:
                            try:
                                r = analyzer.analizar(tmp_path, c_path, "tipo")
                                if r.get("afecciones_detectadas"):
                                    res_total["afecciones_detectadas"] = True
                                    # Extender lista de afecciones con el nombre de la capa
                                    nombre_capa = c_path.stem
                                    for af in r.get("afecciones", []):
                                        af["clase"] = f"{nombre_capa} - {af.get('clase', 'General')}"
                                        res_total["afecciones"].append(af)
                                    
                                    # Maximizar porcentaje
                                    pct = r.get("total_afectado_percent", 0)
                                    if pct > max_pct:
                                        max_pct = pct
                                        res_total["area_afectada_m2"] = r.get("total_afectado_m2") # Aproximado
                            except Exception:
                                continue
                        
                        res_total["total_afectado_percent"] = max_pct
                        resultados_capas["Afecciones Totales (System)"] = res_total
                        
                    else:
                        # Análisis de capa específica solicitada explícitamente
                        res = analyzer.analizar(
                            parcela_path=tmp_path,
                            capa_input=capa_name,
                            campo_clasificacion="tipo"
                        )
                        resultados_capas[capa_name] = res

                except Exception as e:
                    resultados_capas[capa_name] = {"error": str(e)}
            
            resultados_por_archivo[file.filename] = resultados_capas
            # Limpiar temporal
            if tmp_path.exists():
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