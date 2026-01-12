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

# Importaciones necesarias para ZIP
import zipfile
import json
from datetime import datetime

def generar_csv_tecnico(referencia, urban_data, aff_data, output_dir):
    """Genera un CSV con todos los datos técnicos del análisis."""
    import csv
    from datetime import datetime
    import os
    
    filepath = output_dir / f"{referencia}_datos_tecnicos.csv"
    
    # Estructura base con más información
    data = {}
    
    # 1. Datos Identificativos
    data["Referencia"] = referencia
    data["Fecha_Analisis"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 2. Datos Catastrales básicos
    data["Procesado"] = "Sí"
    data["Error_Procesamiento"] = ""
    
    # 3. Datos Urbanísticos
    if urban_data and not urban_data.get("error"):
        data["Analisis_Urbanistico"] = "Sí"
        data["Area_Parcela_m2"] = round(urban_data.get("area_parcela_m2", 0), 2)
        data["Urbanismo_Detectado"] = urban_data.get("urbanismo", False)
        
        # Análisis avanzado (nuevas funcionalidades)
        if urban_data.get("analisis_avanzado"):
            data["Analisis_Avanzado"] = "Sí"
            
            # Parámetros urbanísticos
            params = urban_data.get("parametros_urbanisticos", {})
            if params:
                for param, valor in params.items():
                    if param != "superficie_parcela" and isinstance(valor, dict):
                        key_name = f"URB_{param.replace(' ', '_')}"
                        if "valor" in valor:
                            data[key_name] = valor["valor"]
                
                # Campos específicos importantes
                if "coeficiente_ocupacion" in params:
                    data["URB_Coeficiente_Ocupacion"] = params["coeficiente_ocupacion"].get("valor", 0)
                    data["URB_Superficie_Ocupada_m2"] = params["coeficiente_ocupacion"].get("superficie_ocupada_m2", 0)
                
                if "edificabilidad" in params:
                    data["URB_Edificabilidad_m2m2"] = params["edificabilidad"].get("valor", 0)
                
                if "altura_maxima" in params:
                    data["URB_Altura_Maxima_m"] = params["altura_maxima"].get("valor", 0)
                    data["URB_Altura_Maxima_Plantas"] = params["altura_maxima"].get("plantas", 0)
                
                if "separacion_linderos" in params:
                    data["URB_Separacion_Linderos_m"] = params["separacion_linderos"].get("valor", 0)
            
            # Zonas afectadas
            zonas = urban_data.get("zonas_afectadas", [])
            if zonas:
                data["URB_Zonas_Afectadas_Count"] = len([z for z in zonas if "capa" in z])
                zonas_nombres = [z.get("capa", "") for z in zonas if "capa" in z]
                data["URB_Zonas_Afectadas"] = "; ".join(zonas_nombres)
            
            # Afecciones específicas
            afecciones = urban_data.get("afecciones_detectadas", [])
            if afecciones:
                data["URB_Afecciones_Especificas_Count"] = len([a for a in afecciones if "capa" in a])
                afecciones_tipos = [a.get("tipo", "") for a in afecciones if "tipo" in a]
                data["URB_Afecciones_Tipos"] = "; ".join(set(afecciones_tipos))
            
            # Recomendaciones
            recomendaciones = urban_data.get("recomendaciones", [])
            if recomendaciones:
                data["URB_Recomendaciones_Count"] = len(recomendaciones)
                data["URB_Recomendaciones"] = " | ".join(recomendaciones[:3])  # Primeras 3
        
        # Detalles urbanísticos (compatibilidad con sistema anterior)
        if urban_data.get("detalle"):
            for k, v in urban_data.get("detalle", {}).items():
                key_name = f"URB_{k.replace(' ', '_')}_pct"
                data[key_name] = v
                # Calcular área aprox
                area = data["Area_Parcela_m2"]
                if area > 0:
                    data[f"URB_{k.replace(' ', '_')}_m2"] = round((v / 100) * area, 2)
    else:
        data["Analisis_Urbanistico"] = "No"
        data["Area_Parcela_m2"] = 0.0
        data["Urbanismo_Detectado"] = False
        data["Analisis_Avanzado"] = "No"
        if urban_data and urban_data.get("error"):
            data["Error_Urbanistico"] = urban_data.get("error")
    
    # 4. Afecciones Vectoriales
    if aff_data and not aff_data.get("mensaje"):
        data["Analisis_Afecciones"] = "Sí"
        data["Afecciones_Detectadas"] = aff_data.get("afecciones_detectadas", False)
        data["Afecciones_Total_pct"] = aff_data.get("total", 0.0)
        data["Area_Total_Parcela_m2"] = aff_data.get("area_total_m2", 0.0)
        
        # Detalles de afecciones
        if aff_data.get("detalle"):
            for k, v in aff_data.get("detalle", {}).items():
                # k es "Capa - Clase"
                clean_key = f"AF_{k}".replace(" ", "_").replace("-", "_").replace("__", "_")
                data[f"{clean_key}_m2"] = v
                # Calcular porcentaje
                area = data["Area_Total_Parcela_m2"]
                if area > 0:
                    data[f"{clean_key}_pct"] = round((v / area) * 100, 2)
                else:
                    data[f"{clean_key}_pct"] = 0.0
    else:
        data["Analisis_Afecciones"] = "No"
        data["Afecciones_Detectadas"] = False
        data["Afecciones_Total_pct"] = 0.0
        if aff_data and aff_data.get("mensaje"):
            data["Estado_Afecciones"] = aff_data.get("mensaje")
    
    # 5. Archivos generados (verificar existencia)
    ref_dir = output_dir
    data["PDF_Ficha"] = "Sí" if (ref_dir / "pdf" / f"{referencia}_ficha_catastral.pdf").exists() else "No"
    data["PDF_Urbanistico"] = "Sí" if (ref_dir / f"Informe_{referencia}.pdf").exists() else "No"  # Cambiado: misma carpeta
    data["GML_Parcela"] = "Sí" if (ref_dir / f"{referencia}_parcela.gml").exists() or (ref_dir / "gml" / f"{referencia}_parcela.gml").exists() else "No"
    data["KML_Parcela"] = "Sí" if (ref_dir / f"{referencia}_parcela.kml").exists() or (ref_dir / "gml" / f"{referencia}_parcela.kml").exists() else "No"
    data["Certificado_Urb"] = "Sí" if (ref_dir / f"certificado_{referencia}.txt").exists() else "No"  # Nuevo: certificado
    
    # 6. Metadatos del sistema
    data["Servidor"] = "Suite Tasación v3.1"
    data["Directorio_Salida"] = str(output_dir)
    data["Estado_Afecciones"] = aff_data.get("mensaje", "")
    
    # Escribir CSV
    try:
        # Ordenar columnas lógicamente
        column_order = [
            "Referencia", "Fecha_Analisis", "Procesado", "Error_Procesamiento",
            "Analisis_Urbanistico", "Analisis_Avanzado", "Analisis_Afecciones", "Area_Parcela_m2",
            "Urbanismo_Detectado", "Afecciones_Detectadas", "Afecciones_Total_pct",
            "URB_Coeficiente_Ocupacion", "URB_Superficie_Ocupada_m2", "URB_Edificabilidad_m2m2",
            "URB_Altura_Maxima_m", "URB_Altura_Maxima_Plantas", "URB_Separacion_Linderos_m",
            "URB_Zonas_Afectadas_Count", "URB_Afecciones_Especificas_Count", "URB_Recomendaciones_Count",
            "PDF_Ficha", "PDF_Urbanistico", "Certificado_Urb", "GML_Parcela", "KML_Parcela",
            "Servidor", "Directorio_Salida", "Estado_Afecciones"
        ]
        
        # Agregar columnas dinámicas (urbanísticas y afecciones)
        all_keys = list(data.keys())
        dynamic_keys = [k for k in all_keys if k.startswith(("URB_", "AF_"))]
        column_order.extend(sorted(dynamic_keys))
        
        # Filtrar solo las columnas que existen
        final_columns = [col for col in column_order if col in data]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=final_columns)
            writer.writeheader()
            writer.writerow(data)
            
        print(f"✅ CSV técnico generado: {filepath}")
        print(f"   📊 Columnas: {len(final_columns)}")
        print(f"   📏 Área parcela: {data['Area_Parcela_m2']} m²")
        print(f"   🏙️ Análisis urbanístico: {data['Analisis_Urbanistico']}")
        print(f"   ⚠️ Análisis afecciones: {data['Analisis_Afecciones']}")
        
        return str(filepath)
    except Exception as e:
        print(f"⚠️ Error generando CSV técnico: {e}")
        return None


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

        # 1. Descargar datos catastrales (sin ZIP)
        exito, _ = downloader.descargar_todo_completo(ref_limpia)
        
        if not exito:
            raise HTTPException(
                status_code=404, 
                detail=f"No se pudieron descargar datos catastrales para {ref_limpia}"
            )
        
        # 2. Análisis urbanístico si hay GML disponible
        result_urban = {}
        ref_dir = OUTPUTS_DIR / ref_limpia
        
        # Buscar GML en la raíz (descargas individuales) o en subcarpeta gml/ (lotes)
        gml_path = None
        posibles_gml = [
            ref_dir / f"{ref_limpia}_parcela.gml",  # Descarga individual
            ref_dir / "gml" / f"{ref_limpia}_parcela.gml"  # Procesamiento de lote
        ]
        
        for gml_candidate in posibles_gml:
            if gml_candidate.exists():
                gml_path = gml_candidate
                break
        
        if gml_path:
            try:
                result_urban = urbanismo_service.analizar_parcela(str(gml_path), ref_limpia)
            except Exception as e:
                print(f"⚠️ Error en análisis urbanístico: {e}")
                result_urban = {"error": str(e), "urbanismo": False}
        else:
            result_urban = {"error": "GML no disponible", "urbanismo": False}

        # 2. Análisis de afecciones (OPCIONAL - desactivado por defecto)
        images_dir = ref_dir / "images"
        
        # Usar solo el archivo de capas consolidadas específico
        capas_consolidadas_path = CAPAS_DIR / "capas_consolidadas_20260112_173239.gpkg"
        todas_capas = [capas_consolidadas_path] if capas_consolidadas_path.exists() else []
        print(f"🔍 Analizando parcelas contra archivo consolidado: {capas_consolidadas_path.name if capas_consolidadas_path.exists() else 'NO ENCONTRADO'}")

        res_afecciones = {
            "detalle": {},
            "total": 0.0,
            "area_total_m2": 0.0,
            "afecciones_detectadas": False,
            "mensaje": "Análisis de afecciones desactivado. Use el panel 'Análisis Afecciones' para análisis manual."
        }
        
        # Comentar/descomentar esta línea para activar/desactivar análisis automático
        # ANALISIS_AFECCIONES_ACTIVO = False  # Desactivado por defecto
        ANALISIS_AFECCIONES_ACTIVO = False
        
        if ANALISIS_AFECCIONES_ACTIVO and todas_capas:
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
        else:
            print(f"📋 Análisis de afecciones desactivado para {ref_limpia}")

        # 3. Generar "Plano Perfecto" para el informe
        plano_path = images_dir / f"{ref_limpia}_plano_perfecto.jpg"
        if gml_path.exists():
            # Asegurar que el directorio images exista
            images_dir.mkdir(parents=True, exist_ok=True)
            downloader.generar_plano_perfecto(
                gml_path=gml_path,
                output_path=plano_path,
                ref=ref_limpia,
                info_afecciones=res_afecciones
            )
        
        # 4. Generar PDF Urbanístico si hay análisis urbanístico
        urbanismo_pdf_path = None
        if result_urban and not result_urban.get("error") and result_urban.get("urbanismo"):
            try:
                print("📄 Generando PDF urbanístico...")
                
                # Obtener mapas del servicio urbanístico
                mapas_urbanismo = urbanismo_service.obtener_mapas(ref_limpia)
                
                if mapas_urbanismo:
                    # Encontrar el directorio con timestamp más reciente
                    urbanismo_base_dir = OUTPUTS_DIR / "urbanismo"
                    timestamp_dirs = list(urbanismo_base_dir.glob(f"{ref_limpia}_*"))
                    
                    if timestamp_dirs:
                        # Usar el directorio más reciente
                        latest_dir = max(timestamp_dirs, key=lambda x: x.stat().st_mtime)
                        print(f"📂 Usando directorio urbanismo: {latest_dir.name}")
                        
                        # Generar PDF usando el generador de afecciones en directorio de la referencia
                        from afecciones.pdf_generator import AfeccionesPDF
                        ref_dir = OUTPUTS_DIR / ref_limpia
                        pdf_gen_temp = AfeccionesPDF(output_dir=str(ref_dir))
                        urbanismo_pdf_path = pdf_gen_temp.generar(
                            referencia=ref_limpia,
                            resultados=result_urban,
                            mapas=mapas_urbanismo,
                            incluir_tabla=bool(result_urban.get("urbanismo", False))
                        )
                        
                        if urbanismo_pdf_path:
                            print(f"✅ PDF urbanístico generado: {urbanismo_pdf_path}")
                        else:
                            print("⚠️ No se pudo generar el PDF urbanístico")
                    else:
                        print("⚠️ No se encontraron directorios de urbanismo con timestamp")
                else:
                    print("⚠️ No se encontraron mapas para generar el PDF urbanístico")
                    
            except Exception as e:
                print(f"❌ Error generando PDF urbanístico: {e}")

        # 5. Generar CSV Técnico Consolidado
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
                "kml": f"/outputs/{ref_limpia}/gml/{ref_limpia}_parcela.gml",  # KML en subcarpeta gml/
                "pdf_ficha": f"/outputs/{ref_limpia}/pdf/{ref_limpia}_ficha_catastral.pdf"
            }
        }
        
        # Agregar PDF urbanístico si se generó
        if urbanismo_pdf_path:
            # Convertir la ruta absoluta a relativa para el servidor
            urbanismo_pdf_path = Path(urbanismo_pdf_path)
            if urbanismo_pdf_path.exists():
                # La ruta debe ser relativa al OUTPUTS_DIR
                try:
                    relative_path = urbanismo_pdf_path.relative_to(OUTPUTS_DIR)
                    response_data["archivos_generados"]["pdf_urbanistico"] = f"/outputs/{relative_path}"
                    print(f"✅ URL PDF urbanístico: /outputs/{relative_path}")
                except ValueError:
                    # Si no puede ser relativa, construir la ruta correcta con timestamp
                    # Buscar el directorio con timestamp más reciente
                    urbanismo_base = OUTPUTS_DIR / "urbanismo"
                    timestamp_dirs = list(urbanismo_base.glob(f"{ref_limpia}_*"))
                    if timestamp_dirs:
                        latest_dir = max(timestamp_dirs, key=lambda x: x.stat().st_mtime)
                        pdf_relative = latest_dir.relative_to(OUTPUTS_DIR) / f"Informe_{ref_limpia}.pdf"
                        response_data["archivos_generados"]["pdf_urbanistico"] = f"/outputs/{pdf_relative}"
                        print(f"✅ URL PDF urbanístico (fallback): /outputs/{pdf_relative}")
                    else:
                        response_data["archivos_generados"]["pdf_urbanistico"] = None
            else:
                print(f"⚠️ El PDF urbanístico no existe en: {urbanismo_pdf_path}")
                response_data["archivos_generados"]["pdf_urbanistico"] = None
        
        if csv_path:
            response_data["archivos_generados"]["csv_tecnico"] = f"/outputs/{ref_limpia}/{Path(csv_path).name}"
        
        # 6. Crear ZIP completo con TODOS los archivos generados
        zip_path = None
        try:
            zip_path = OUTPUTS_DIR / f"{ref_limpia}_completo.zip"
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Añadir todos los archivos del directorio de la referencia
                for file_path in ref_dir.rglob('*'):
                    if file_path.is_file():
                        # Ruta relativa dentro del ZIP
                        arcname = file_path.relative_to(ref_dir)
                        zipf.write(file_path, arcname)
                        print(f"   📦 Añadido al ZIP: {arcname}")
                
                # Crear manifiesto
                manifest = {
                    "referencia": ref_limpia,
                    "fecha_generacion": datetime.now().isoformat(),
                    "archivos": []
                }
                
                # Contar archivos en el ZIP
                for file_path in ref_dir.rglob('*'):
                    if file_path.is_file():
                        arcname = str(file_path.relative_to(ref_dir))
                        manifest["archivos"].append({
                            "ruta": arcname,
                            "tamano": file_path.stat().st_size,
                            "fecha": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                        })
                
                # Añadir manifiesto al ZIP
                manifest_json = json.dumps(manifest, indent=2, ensure_ascii=False)
                zipf.writestr("manifesto.json", manifest_json)
                
            print(f"✅ ZIP completo creado: {zip_path}")
            print(f"   📁 Archivos incluidos: {len(manifest['archivos'])}")
            
        except Exception as e:
            print(f"❌ Error creando ZIP completo: {e}")
            zip_path = None
            
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
                    # Usar solo el archivo de capas consolidadas específico
                    capas_consolidadas_path = CAPAS_DIR / "capas_consolidadas_20260112_173239.gpkg"
                    todas_capas = [capas_consolidadas_path] if capas_consolidadas_path.exists() else []
                    print(f"📄 PDF Afecciones: analizando contra archivo consolidado: {capas_consolidadas_path.name if capas_consolidadas_path.exists() else 'NO ENCONTRADO'}")
                    
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
    Obtiene la lista de capas vectoriales disponibles
    Ahora solo muestra el archivo consolidado específico
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
        
        # Buscar solo el archivo consolidado específico
        capas_consolidadas = CAPAS_DIR / "capas_consolidadas_20260112_173239.gpkg"
        if capas_consolidadas.exists():
            capas_info["capas_vectoriales"].append({
                "nombre": "Capas Consolidadas",
                "archivo": capas_consolidadas.name,
                "ruta": str(capas_consolidadas.relative_to(CAPAS_DIR)),
                "tamano": capas_consolidadas.stat().st_size,
                "tipo": "vectorial",
                "descripcion": "Archivo consolidado con todas las capas de análisis"
            })
        
        return {
            "status": "success",
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
        port=81, 
        reload=True
    )