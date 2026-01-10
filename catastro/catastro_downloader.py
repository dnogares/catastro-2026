from dns.message import _maybe_import_update
import os
import time
import geopandas
import PILLOW_AVAILABLE
import matplotlib
import contextily

import json
import zipfile
import requests
import logging
from pathlib import Path
from io import BytesIO
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
from shapely.geometry import shape, Polygon, MultiPolygon, Point
from shapely.ops import transform
from pyproj import Transformer

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Dependencias opcionales
try:
    import geopandas as gpd
    import matplotlib.pyplot as plt
    import contextily as cx
    from shapely.geometry import mapping, Point
    from PIL import Image, ImageDraw, ImageFont
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    GEOTOOLS_AVAILABLE = True
    PILLOW_AVAILABLE = True
except ImportError:
    logger.warning("Faltan dependencias (geopandas, matplotlib, pillow, contextily). Funcionalidad limitada.")
    GEOTOOLS_AVAILABLE = False
    PILLOW_AVAILABLE = False

def safe_get(url, params=None, headers=None, timeout=30, max_retries=2, method='get', json_body=None):
    """Wrapper con reintentos para requests"""
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            if method.lower() == 'get':
                r = requests.get(url, params=params, headers=headers, timeout=timeout)
            else:
                r = requests.post(url, params=params, headers=headers, json=json_body, timeout=timeout)
            return r
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(1 + attempt)
    raise last_exc

class CatastroDownloader:
    """
    Descarga documentación del Catastro español a partir de referencias catastrales.
    Incluye generación de mapas con ortofoto usando servicios WMS y superposición de contorno.
    """

    BASE_URL = "https://www.sedecatastro.gob.es"
    OVC_URL = "https://ovc.catastro.meh.es"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "es-ES,es;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.sedecatastro.gob.es/",
    }

    def __init__(self, output_dir="descargas_catastro", retries=3, timeout=20):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.retries = retries
        self.timeout = timeout
        self.base_url = "https://ovc.catastro.meh.es"
        
        # Diccionario auxiliar para los códigos de municipio/delegación. 
        # Es necesario para descargar la consulta oficial
        self._municipio_cache = {} 
        
        # Capas WMS del Catastro (identificadores / nombres de ejemplo)
        self.capas_wms = {
            'catastro': 'Catastro',
            'ortofoto': 'PNOA',
            'callejero': 'Callejero',
            'hidrografia': 'Hidrografia',
        }

        # Fuentes alternativas para ortofoto (IGN PNOA)
        self.enable_ign_ortofoto = True
        self.capas_wms_extra = {
            'ortofoto': {
                'wms_url': 'https://www.ign.es/wms-inspire/pnoa-ma',
                'layers': 'OI.OrthoimageCoverage',
                'version': '1.3.0',
                'crs': 'EPSG:4326',
                'use_wms13_bbox': True
            }
        }

        # Servicios WFS para afectaciones
        self.servicios_wfs = {
            'espacios_naturales': {
                'url': 'https://www.miteco.gob.es/wfs/espacios_protegidos',
                'layer': 'espacios_protegidos',
                'descripcion': 'Espacios Naturales Protegidos',
            },
            'zonas_inundables': {
                'url': 'https://www.miteco.gob.es/wfs/snczi',
                'layer': 'zonas_inundables',
                'descripcion': 'Zonas de Riesgo de Inundación SNCZI',
            },
        }

        self.base_catastro = "https://ovc.catastro.meh.es"
        
        # Capas WMS del Catastro y Afecciones (Legacy compatibility / Extended)
        self.wms_urls = {
            # ✅ Cartografía base
            "catastro": "http://ovc.catastro.meh.es/cartografia/INSPIRE/spadgcwms.aspx",
            "catastro_https": "https://ovc.catastro.meh.es/Cartografia/WMS/ServidorWMS.aspx",
            "pnoa": "https://www.ign.es/wms-inspire/pnoa-ma",
            
            # ✅ Riesgos hídricos
            "inundabilidad_10años": "https://wms.mapama.gob.es/sig/agua/ZI_LaminasQ10/wms.aspx",
            "inundabilidad_100años": "https://wms.mapama.gob.es/sig/agua/ZI_LaminasQ100/wms.aspx",
            "inundabilidad_500años": "https://wms.mapama.gob.es/sig/agua/ZI_LaminasQ500/wms.aspx",
            
            # ✅ Biodiversidad y protección
            "red_natura": "https://wms.mapama.gob.es/sig/Biodiversidad/RedNatura/wms.aspx",
            "espacios_protegidos": "https://wms.mapama.gob.es/sig/Biodiversidad/ENP/wms.aspx",
            "vias_pecuarias": "https://wms.mapama.gob.es/sig/Biodiversidad/ViasPecuarias/wms.aspx",
            
            # ✅ Montes y forestal
            "montes_utilidad_publica": "https://wms.mapama.gob.es/sig/Biodiversidad/PropiedadMontes_UP/wms.aspx",
            "titularidad_montes": "https://wms.mapama.gob.es/sig/Biodiversidad/PropiedadMontes/wms.aspx",
            "mapa_forestal": "https://wms.mapama.gob.es/sig/Biodiversidad/MFE/wms.aspx",
            
            # ✅ Otros servicios útiles
            "erosion_laminar": "https://wms.mapama.gob.es/sig/Biodiversidad/INESErosionLaminarRaster/wms.aspx",
            "incendios_forestales": "https://wms.mapama.gob.es/sig/Biodiversidad/Incendios/2006_2015/wms.aspx",
            
            # ✅ Nuevos servicios sugeridos
            "planeamiento": "https://www.idee.es/wms/IDEE-Planeamiento/IDEE-Planeamiento",
            "dominio_maritimo": "https://ideihm.covam.es/wms-c/mapas/Demarcaciones",
            "zonas_valor": "http://ovc.catastro.meh.es/Cartografia/WMS/ServidorWMS.aspx"
        }

    def limpiar_referencia(self, ref):
        """Limpia la referencia catastral eliminando espacios."""
        return ref.replace(" ", "").strip()

    def extraer_del_mun(self, ref):
        """Extrae el código de delegación (2 dígitos) y municipio (3 dígitos) de la referencia."""
        ref = self.limpiar_referencia(ref)
        if len(ref) >= 5:
            # El Catastro usa los 5 primeros dígitos para delegación/municipio
            return ref[:2], ref[2:5] # C=provincia (2), M=municipio (3)
        return "", ""

    def convertir_coordenadas_a_etrs89(self, lon, lat):
        """Convierte coordenadas WGS84 a ETRS89/UTM (aproximación)."""
        # Esto es una aproximación para determinar la zona UTM correcta
        if lon < -6:
            zona = 29
            epsg = 25829
        elif lon < 0:
            zona = 30
            epsg = 25830
        else:
            zona = 31
            epsg = 25831

        return {"epsg": epsg, "zona": zona}

    def _coords_to_shapely_polygon(self, coords_utm: List[Tuple[float, float]]) -> Optional[Polygon]:
        """Convierte coordenadas UTM de GML a un polígono de Shapely"""
        try:
            if not coords_utm or len(coords_utm) < 3:
                return None
            return Polygon(coords_utm)
        except Exception as e:
            logger.error(f"Error creando polígono shapely: {e}")
            return None

    def calcular_porcentaje_pixeles(self, parcela_geom: Polygon, capa_img: Any, bbox_wgs84: str, umbral: int = 250) -> float:
        """Calcula el porcentaje de píxeles de la parcela intersectados por la capa WMS (análisis matricial)"""
        if not parcela_geom:
            return 0.0
            
        try:
            width, height = capa_img.size
            lon_min, lat_min, lon_max, lat_max = [float(x) for x in bbox_wgs84.split(",")]
            
            # Crear malla de coordenadas WGS84 para cada píxel
            xs = np.linspace(lon_min, lon_max, width)
            ys = np.linspace(lat_max, lat_min, height) # Invertido para imagen
            X, Y = np.meshgrid(xs, ys)
            
            # Transformar parcela_geom a WGS84 si no lo está (asumimos que viene en UTM 25830 si es del GML original)
            # PERO para el cálculo de píxeles en BBOX WGS84, necesitamos la geometría en WGS84
            # Nota: calcular_bbox_dinamico ya maneja la proyección si es necesario
            
            # Crear máscara de pertenencia a la parcela
            # Optimizamos: Comprobar primero si el punto está dentro del BBOX de la parcela
            p_minx, p_miny, p_maxx, p_maxy = parcela_geom.bounds
            
            mask = np.zeros((height, width), dtype=bool)
            for i in range(height):
                for j in range(width):
                    px, py = X[i, j], Y[i, j]
                    # Filtro rápido de BBOX
                    if p_minx <= px <= p_maxx and p_miny <= py <= p_maxy:
                        if parcela_geom.contains(Point(px, py)):
                            mask[i, j] = True
            
            # Analizar imagen
            arr = np.array(capa_img.convert("L")) # Escala de grises
            arr_masked = arr[mask]
            
            if arr_masked.size == 0:
                return 0.0
                
            # Píxeles "coloreados" (con información de afección) suelen tener valores bajos (< 255)
            # Un umbral de 250 filtra el fondo casi blanco
            afectados = np.sum(arr_masked < umbral)
            return (afectados / arr_masked.size) * 100
            
        except Exception as e:
            logger.error(f"Error en calcular_porcentaje_pixeles: {e}")
            return 0.0

    def calcular_bbox_dinamico(self, coords_wgs84: List[Tuple[float, float]], zoom_factor: float = 1.2) -> str:
        """Calcula un BBOX WGS84 dinámico que envuelve la parcela con un margen visual"""
        if not coords_wgs84:
            return ""
            
        lons = [c[0] for c in coords_wgs84]
        lats = [c[1] for c in coords_wgs84]
        
        lon_min, lon_max = min(lons), max(lons)
        lat_min, lat_max = min(lats), max(lats)
        
        lon_center = (lon_max + lon_min) / 2
        lat_center = (lat_max + lat_min) / 2
        
        lon_size = (lon_max - lon_min) * zoom_factor
        lat_size = (lat_max - lat_min) * zoom_factor
        
        # Buffer mínimo si es una parcela muy pequeña o punto
        lon_size = max(lon_size, 0.002)
        lat_size = max(lat_size, 0.0015)
        
        return f"{lon_center - lon_size/2},{lat_center - lat_size/2},{lon_center + lon_size/2},{lat_center + lat_size/2}"

    def obtener_datos_basicos(self, referencia: str):
        """Obtiene datos básicos de la referencia catastral"""
        ref = self.limpiar_referencia(referencia)
        try:
            url_json = f"{self.OVC_URL}/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/json/Geo_RCToWGS84/{ref}"
            r = safe_get(url_json, timeout=20)
            if r.status_code == 200:
                return r.json()
            else:
                logger.warning(f"Error obteniendo datos básicos: {r.status_code}")
                return {}
        except Exception as e:
            logger.error(f"Error en obtener_datos_basicos: {e}")
            return {}

    def extraer_coordenadas_desde_gml(self, gml_path):
        """Extrae coordenadas del GML de parcela con nuevo formato"""
        try:
            tree = ET.parse(gml_path)
            root = tree.getroot()

            ns = {
                'gml': 'http://www.opengis.net/gml/3.2',
                'cp': 'http://inspire.ec.europa.eu/schemas/cp/4.0'
            }

            ref_point = root.find('.//cp:referencePoint/gml:Point/gml:pos', ns)
            if ref_point is not None and ref_point.text:
                coords = ref_point.text.strip().split()
                if len(coords) >= 2:
                    return {
                        'x_utm': float(coords[0]),
                        'y_utm': float(coords[1]),
                        'epsg': '25830',
                        'source': 'referencePoint'
                    }

            poslist = root.find('.//gml:posList', ns)
            if poslist is not None and poslist.text:
                coords = [float(x) for x in poslist.text.strip().split()]
                x_coords = coords[0::2]
                y_coords = coords[1::2]
                if x_coords and y_coords:
                    return {
                        'x_utm': sum(x_coords) / len(x_coords),
                        'y_utm': sum(y_coords) / len(y_coords),
                        'epsg': '25830',
                        'source': 'centroid'
                    }

            return None
        except Exception:
            return None

    def utm_a_wgs84(self, x_utm, y_utm, epsg='25830'):
        """Convierte coordenadas UTM a WGS84 usando GeoPandas"""
        if not GEOTOOLS_AVAILABLE:
            return None
        try:
            from shapely.geometry import Point
            gdf = gpd.GeoDataFrame(geometry=[Point(x_utm, y_utm)], crs=f'EPSG:{epsg}')
            gdf_wgs84 = gdf.to_crs('EPSG:4326')
            point_wgs84 = gdf_wgs84.geometry.iloc[0]
            return {'lon': point_wgs84.x, 'lat': point_wgs84.y, 'srs': 'EPSG:4326'}
        except Exception:
            return None

    def convertir_gml_a_kml(self, gml_path: Path, kml_path: Path) -> bool:
        """Convierte archivo GML a KML usando GeoPandas"""
        if not GEOTOOLS_AVAILABLE: return False
        try:
            import fiona
            gdf = gpd.read_file(str(gml_path))
            if gdf.empty:
                logger.warning(f"    ⚠️ GML vacío, no se puede generar KML: {gml_path.name}")
                return False
            if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            
            try:
                # Activar driver KML en fiona si está disponible
                if 'KML' not in fiona.drvsupport.supported_drivers:
                    fiona.drvsupport.supported_drivers['KML'] = 'rw'
                
                gdf.to_file(str(kml_path), driver='KML')
                logger.info(f"    ✅ KML generado: {kml_path.name}")
                return True
            except Exception as e:
                logger.error(f"Error convirtiendo GML a KML: {e}")
                # Fallback GeoJSON
                try:
                    geojson_path = kml_path.with_suffix('.geojson')
                    gdf.to_file(str(geojson_path), driver='GeoJSON')
                    logger.info(f"    ⚠️ KML falló, generado GeoJSON: {geojson_path.name}")
                except Exception as geojson_e:
                    logger.error(f"    ❌ Fallo al generar GeoJSON como fallback: {geojson_e}")
                return False
        except Exception as e:
            logger.error(f"Error leyendo GML para conversión a KML: {e}")
            return False

    def obtener_coordenadas(self, referencia):
        """Obtiene las coordenadas de la parcela desde el servicio del Catastro."""
        ref = self.limpiar_referencia(referencia)

        # Método 1: Servicio REST JSON
        try:
            url_json = (
                "http://ovc.catastro.meh.es/OVCServWeb/OVCWcfCallejero/"
                f"COVCCallejero.svc/json/Geo_RCToWGS84/{ref}"
            )
            response = requests.get(url_json, timeout=30)

            if response.status_code == 200:
                data = response.json()
                if (
                    "geo" in data
                    and "xcen" in data["geo"]
                    and "ycen" in data["geo"]
                ):
                    lon = float(data["geo"]["xcen"])
                    lat = float(data["geo"]["ycen"])
                    print(f"  Coordenadas obtenidas (JSON): Lon={lon}, Lat={lat}")
                    return {"lon": lon, "lat": lat, "srs": "EPSG:4326"}
        except Exception as e:
            # print(f"  ⚠ Método JSON falló: {e}")
            pass

        # Método 2: Extraer del GML de parcela
        try:
            # print("  Intentando extraer coordenadas del GML de parcela...")
            url_gml = "http://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx"
            params = {
                "service": "wfs",
                "version": "2.0.0",
                "request": "GetFeature",
                "STOREDQUERY_ID": "GetParcel", # Corregido: 'STOREDQUERY_ID'
                "refcat": ref,
                "srsname": "EPSG:4326",
            }

            response = requests.get(url_gml, params=params, timeout=30)
            if response.status_code == 200:
                root = ET.fromstring(response.content)

                namespaces = {
                    "gml": "http://www.opengis.net/gml/3.2",
                    "cp": "http://inspire.ec.europa.eu/schemas/cp/4.0",
                    "gmd": "http://www.isotc211.org/2005/gmd",
                }

                # Buscar coordenadas en pos o posList
                for ns_uri in namespaces.values():
                    # Buscar pos (coordenada de centro o un punto)
                    pos_list = root.findall(f".//{{{ns_uri}}}pos")
                    if pos_list:
                        coords_text = pos_list[0].text.strip().split()
                        if len(coords_text) >= 2:
                            # En el GML de INSPIRE, a menudo es Lat, Lon (orden de eje)
                            v1 = float(coords_text[0])
                            v2 = float(coords_text[1])
                            # Heurística para Lat/Lon en España
                            if 36 <= v1 <= 44 and -10 <= v2 <= 5: 
                                lat, lon = v1, v2
                            elif 36 <= v2 <= 44 and -10 <= v1 <= 5:
                                lat, lon = v2, v1
                            else: # Por defecto (Lat, Lon)
                                lat, lon = v1, v2
                                
                            print(f"  Coordenadas extraídas del GML: Lon={lon}, Lat={lat}")
                            return {"lon": lon, "lat": lat, "srs": "EPSG:4326"}

                    # Buscar posList (coordenadas de polígono)
                    pos_list = root.findall(f".//{{{ns_uri}}}posList")
                    if pos_list:
                        coords_text = pos_list[0].text.strip().split()
                        if len(coords_text) >= 2:
                            # Tomamos el primer par como aproximación
                            v1 = float(coords_text[0])
                            v2 = float(coords_text[1])
                            # Heurística
                            if 36 <= v1 <= 44 and -10 <= v2 <= 5: 
                                lat, lon = v1, v2
                            elif 36 <= v2 <= 44 and -10 <= v1 <= 5:
                                lat, lon = v2, v1
                            else:
                                lat, lon = v1, v2
                                
                            print(f"  Coordenadas extraídas del GML (PosList): Lon={lon}, Lat={lat}")
                            return {"lon": lon, "lat": lat, "srs": "EPSG:4326"}
        except Exception as e:
            # print(f"  ⚠ Extracción de GML falló: {e}")
            pass

        # Método 3: Servicio XML original
        try:
            url = (
                "http://ovc.catastro.meh.es/ovcservweb/ovcswlocalizacionrc/"
                "ovccoordenadas.asmx/Consulta_RCCOOR"
            )
            params = {"SRS": "EPSG:4326", "RC": ref}

            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                coords_element = root.find(
                    ".//{http://www.catastro.meh.es/}coord"
                )
                if coords_element is not None:
                    geo = coords_element.find(
                        "{http://www.catastro.meh.es/}geo"
                    )
                    if geo is not None:
                        xcen = geo.find(
                            "{http://www.catastro.meh.es/}xcen"
                        )
                        ycen = geo.find(
                            "{http://www.catastro.meh.es/}ycen"
                        )

                        if xcen is not None and ycen is not None:
                            lon = float(xcen.text)
                            lat = float(ycen.text)
                            print(f"  Coordenadas obtenidas (XML): Lon={lon}, Lat={lat}")
                            return {"lon": lon, "lat": lat, "srs": "EPSG:4326"}
        except Exception as e:
            # print(f"  ⚠ Método XML falló: {e}")
            pass

        print("  ✗ No se pudieron obtener coordenadas por ningún método")
        return None

    def descargar_parcela_gml(self, referencia, output_dir):
        """Descarga la geometría de la parcela en formato GML"""
        ref = self.limpiar_referencia(referencia)
        url = "http://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx"
        
        # Corregir: es STOREDQUERY_ID (sin la E)
        params = {
            'service': 'wfs',
            'version': '2.0.0',
            'request': 'GetFeature',
            'STOREDQUERY_ID': 'GetParcel',
            'refcat': ref,
            'srsname': 'EPSG:4326' # Pide el GML en EPSG:4326 para que coincida con el WMS/BBOX
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                target_dir = Path(output_dir) / "gml"
                target_dir.mkdir(exist_ok=True, parents=True)
                filename = target_dir / f"{ref}_parcela.gml"
                
                # Verificar si es un error XML (ExceptionReport)
                if b'ExceptionReport' in response.content or b'Exception' in response.content:
                    print(f"  ⚠ Parcela GML no disponible para {ref} (Exception Report en la respuesta)")
                    return None

                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f"  ✓ Parcela GML descargada: {filename}")
                return filename
            else:
                print(f"  ✗ Error descargando parcela GML para {ref}: Status {response.status_code}")
                return None
        except Exception as e:
            print(f"  ✗ Error descargando parcela GML para {ref}: {e}")
            return None

    def _descargar_gml_robust(self, referencia: str, ref_dir: Path) -> bool:
        """
        Descarga el GML de parcela usando la ruta SEC (para robustez en servidores cloud)
        """
        gml_dir = ref_dir / "gml"
        gml_dir.mkdir(parents=True, exist_ok=True)
        gml_path = gml_dir / f"{referencia}_parcela.gml"

        if gml_path.exists():
            logger.info(f"ℹ️ GML ya existe: {gml_path}")
            return True

        url = f"{self.BASE_URL}/Accesos/SECAccesos.aspx?RC={referencia}&tipo=parcelas"
        
        for intento in range(1, self.retries + 1):
            try:
                logger.info(f"⬇️ Descargando GML Robusto ({intento}/{self.retries})")
                r = requests.get(url, headers=self.HEADERS, timeout=self.timeout)
                if r.status_code == 200 and r.content and b'gml:' in r.content:
                    gml_path.write_bytes(r.content)
                    logger.info(f"✅ GML Robusto guardado en {gml_path}")
                    return True
                raise ConnectionError(f"Respuesta inválida o sin GML ({r.status_code})")
            except Exception as e:
                logger.warning(f"⚠️ Intento {intento} fallido: {e}")
                time.sleep(2 + intento * 2)
        return False

    def descargar_edificio_gml(self, referencia, output_dir):
        """Descarga la geometría del edificio en formato GML"""
        ref = self.limpiar_referencia(referencia)
        url = "http://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx"
        
        # Corregir: es STOREDQUERY_ID (sin la E)
        params = {
            'service': 'wfs',
            'version': '2.0.0',
            'request': 'GetFeature',
            'STOREDQUERY_ID': 'GetBuilding',
            'refcat': ref,
            'srsname': 'EPSG:4326' # Pide el GML en EPSG:4326
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                # Verificar que no sea un error XML
                content = response.content
                if b'ExceptionReport' in content or b'Exception' in content:
                    print(f"  ⚠ Edificio GML no disponible para {ref} (puede ser solo parcela)")
                    return None
                
                target_dir = Path(output_dir) / "gml"
                target_dir.mkdir(exist_ok=True, parents=True)
                filename = target_dir / f"{ref}_edificio.gml"
                with open(filename, 'wb') as f:
                    f.write(content)
                print(f"  ✓ Edificio GML descargado: {filename}")
                return filename
            else:
                print(f"  ✗ Error descargando edificio GML para {ref}: Status {response.status_code}")
                return None
        except Exception as e:
            print(f"  ✗ Error descargando edificio GML para {ref}: {e}")
            return None

    def descargar_consulta_descriptiva_pdf(self, referencia, output_dir=None):
        """Descarga el PDF oficial de consulta descriptiva"""
        ref = self.limpiar_referencia(referencia)
        # El endpoint requiere los 5 primeros dígitos (código provincial + municipal)
        del_code, mun_code = self.extraer_del_mun(ref)
        
        url = f"https://www1.sedecatastro.gob.es/CYCBienInmueble/SECImprimirCroquisYDatos.aspx?del={del_code}&mun={mun_code}&refcat={ref}"
        
        # Usar output_dir si se proporciona, sino el de la clase
        target_dir = Path(output_dir) if output_dir else self.output_dir
        filename = target_dir / "pdf" / f"{ref}_consulta_oficial.pdf"
        filename.parent.mkdir(exist_ok=True, parents=True)
        
        if filename.exists():
            print(f"  ↩ PDF oficial ya existe")
            return True
        
        try:
            response = requests.get(url, timeout=30)
                
            if response.status_code == 200 and response.headers.get("Content-Type", "").startswith("application/pdf"):
                with open(filename, "wb") as f:
                    f.write(response.content)
                print(f"  ✓ PDF oficial descargado: {filename}")
                return True
            else:
                print(f"  ✗ PDF oficial falló (Status {response.status_code})")
                return False
                    
        except Exception as e:
            print(f"  ✗ Error descargando PDF: {e}")
            return False

    def extraer_coordenadas_gml(self, gml_file):
        """Extrae las coordenadas del polígono desde el archivo GML."""
        try:
            tree = ET.parse(gml_file)
            root = tree.getroot()

            coords = []

            # posList GML 3.2 (Lat Lon)
            for pos_list in root.findall(
                ".//{http://www.opengis.net/gml/3.2}posList"
            ):
                parts = pos_list.text.strip().split()
                
                for i in range(0, len(parts), 2):
                    if i + 1 < len(parts):
                         # Almacenamos el par como está. Asumimos que es Lat/Lon o Lon/Lat.
                         coords.append((float(parts[i]), float(parts[i + 1])))

            # pos individuales si no hay posList
            if not coords:
                for pos in root.findall(
                    ".//{http://www.opengis.net/gml/3.2}pos"
                ):
                    parts = pos.text.strip().split()
                    if len(parts) >= 2:
                        coords.append((float(parts[0]), float(parts[1])))

            if coords:
                print(f"  ✓ Extraídas {len(coords)} coordenadas del GML")
                return coords

            print("  ⚠ No se encontraron coordenadas en el GML")
            return None

        except Exception as e:
            print(f"  ⚠ Error extrayendo coordenadas del GML: {e}")
            return None

    def convertir_coordenadas_a_pixel(self, coords, bbox, width, height):
        """
        Convierte coordenadas (Lat/Lon o Lon/Lat) a píxeles de la imagen según BBOX WGS84.
        
        Incluye heurística para el orden Lat/Lon vs Lon/Lat.
        """
        try:
            # bbox es 'minx,miny,maxx,maxy' (Lon, Lat)
            minx, miny, maxx, maxy = [float(x) for x in bbox.split(",")] 
            pixels = []

            # Rangos aproximados para España peninsular
            LAT_RANGE = (36, 44) 
            LON_RANGE = (-10, 5)

            for v1, v2 in coords:
                
                # Heurística para decidir el orden
                lat, lon = v1, v2 # Asumimos Lat, Lon (orden de eje del GML/EPSG:4326)
                
                # Caso 1: Lat es v1, Lon es v2 (Orden Lat/Lon)
                if LAT_RANGE[0] <= v1 <= LAT_RANGE[1] and LON_RANGE[0] <= v2 <= LON_RANGE[1]: 
                     lat, lon = v1, v2
                
                # Caso 2: Lon es v1, Lat es v2 (Orden Lon/Lat)
                elif LON_RANGE[0] <= v1 <= LON_RANGE[1] and LAT_RANGE[0] <= v2 <= LAT_RANGE[1]: 
                     lon, lat = v1, v2
                
                # Si no está claro, mantenemos la asunción por defecto Lat=v1, Lon=v2
                else: 
                     lat, lon = v1, v2
                
                # Normalización en X (Longitud)
                x_norm = (lon - minx) / (maxx - minx) if maxx != minx else 0.5
                # Normalización en Y (Latitud) (Y se invierte en la imagen: MaxY es el píxel 0)
                y_norm = (maxy - lat) / (maxy - miny) if maxy != miny else 0.5 

                x = max(0, min(width - 1, int(x_norm * width)))
                y = max(0, min(height - 1, int(y_norm * height)))
                pixels.append((x, y))

            return pixels

        except Exception as e:
            print(f"  ⚠ Error convirtiendo coordenadas a píxeles: {e}")
            return None

    def dibujar_contorno_en_imagen(
        self, imagen_path, pixels, output_path, color=(255, 0, 0), width=4
    ):
        """Dibuja el contorno de la parcela sobre una imagen existente."""
        if not PILLOW_AVAILABLE:
            print("  ⚠ Pillow no disponible, no se puede dibujar contorno")
            return False

        try:
            img = Image.open(imagen_path).convert("RGBA")
            overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
            draw = ImageDraw.Draw(overlay)

            if len(pixels) > 2:
                # Cerrar el polígono
                if pixels[0] != pixels[-1]:
                    pixels = pixels + [pixels[0]]
                draw.line(pixels, fill=color + (255,), width=width)

            # Combina la imagen original con la capa de contorno
            result = Image.alpha_composite(img, overlay).convert("RGB")
            result.save(output_path)
            print(f"  ✓ Contorno dibujado en {output_path}")
            return True

        except Exception as e:
            print(f"  ⚠ Error dibujando contorno: {e}")
            return False

    def superponer_contorno_parcela(self, ref, bbox_wgs84, output_dir):
        """Superpone el contorno de la parcela sobre plano, ortofoto y composición."""
        ref = self.limpiar_referencia(ref)
        target_dir = Path(output_dir)
        gml_file = target_dir / "gml" / f"{ref}_parcela.gml"
        
        if not gml_file.exists():
            print("  ⚠ No existe GML de parcela, no se puede dibujar contorno")
            return False

        coords = self.extraer_coordenadas_gml(gml_file)
        if not coords:
            return False

        exito = False
        img_dir = target_dir / "images"

        for in_path in img_dir.glob(f"{ref}_*.png"):
            # Evitar procesar lo que ya procesamos o duplicar infinitamente
            if "_contorno" in in_path.name: continue
            
            out_path = in_path.with_name(in_path.stem + "_contorno.png")
            try:
                with Image.open(in_path) as img:
                    w, h = img.size
                pixels = self.convertir_coordenadas_a_pixel(
                    coords, bbox_wgs84, w, h
                )
                if pixels and self.dibujar_contorno_en_imagen(
                    in_path, pixels, out_path
                ):
                    exito = True
            except Exception as e:
                print(f"  ⚠ Error procesando imagen {in_path}: {e}")

        # También para JPGs si los hay (PNOA pura a veces es JPG en el código del usuario)
        for in_path in img_dir.glob(f"{ref}_*.jpg"):
            if "_contorno" in in_path.name: continue
            out_path = in_path.with_name(in_path.stem + "_contorno.jpg")
            try:
                with Image.open(in_path) as img:
                    w, h = img.size
                pixels = self.convertir_coordenadas_a_pixel(
                    coords, bbox_wgs84, w, h
                )
                if pixels and self.dibujar_contorno_en_imagen(
                    in_path, pixels, out_path
                ):
                    exito = True
            except Exception as e:
                print(f"  ⚠ Error procesando imagen {in_path}: {e}")

        return exito

    def calcular_bbox(self, lon, lat, nivel=None, buffer_metros=None):
        """Calcula BBOX para WMS. Soporta nivel de zoom o buffer en metros."""
        if buffer_metros:
            # Aproximación simple suficiente para pequeñas áreas
            buffer_lon = buffer_metros / 85000
            buffer_lat = buffer_metros / 111000
            return f"{lon-buffer_lon},{lat-buffer_lat},{lon+buffer_lon},{lat+buffer_lat}"
        
        # Lógica multiescala original
        radios = {1: 1.5, 2: 0.15, 3: 0.015, 4: 0.0025}
        r = radios.get(nivel, 0.002)
        return f"{lon-r},{lat-r},{lon+r},{lat+r}"

    def _get_wms_layer(self, url, bbox, layer, transparent=True) -> Optional[Image.Image]:
        """Descarga una capa WMS y la convierte en RGBA"""
        if not GEOTOOLS_AVAILABLE: return None
        params = {
            'SERVICE': 'WMS', 'VERSION': '1.1.1', 'REQUEST': 'GetMap',
            'LAYERS': layer, 'STYLES': '', 'SRS': 'EPSG:4326',
            'BBOX': bbox, 'WIDTH': '1200', 'HEIGHT': '1200',
            'FORMAT': 'image/png', 'TRANSPARENT': 'TRUE' if transparent else 'FALSE'
        }
        try:
            logger.info(f"    🛰️ Descargando WMS: {layer}...")
            r = safe_get(url, params=params, timeout=25)
            if r.status_code == 200 and b'PNG' in r.content[:10]:
                return Image.open(BytesIO(r.content)).convert("RGBA")
            else:
                logger.warning(f"    ⚠️ Error WMS {layer}: Status {r.status_code}")
        except Exception as e:
            logger.error(f"    ❌ Error WMS {layer}: {e}")
        return None

    def _generar_silueta_roja(self, gml_path, bbox) -> Optional[Image.Image]:
        """Genera silueta roja gruesa desde el GML"""
        if not GEOTOOLS_AVAILABLE: return None
        try:
            gdf = gpd.read_file(gml_path)
            if gdf.crs is None: gdf.set_crs("EPSG:25830", inplace=True)
            gdf = gdf.to_crs("EPSG:4326")
            
            b = [float(x) for x in bbox.split(',')]
            fig = plt.figure(figsize=(12, 12), dpi=100)
            ax = fig.add_axes([0, 0, 1, 1])
            ax.set_axis_off()
            ax.set_xlim(b[0], b[2]); ax.set_ylim(b[1], b[3])
            
            gdf.plot(ax=ax, facecolor='none', edgecolor='#FF0000', linewidth=6)
            
            buf = BytesIO()
            fig.savefig(buf, transparent=True, format='png', dpi=100)
            plt.close(fig)
            return Image.open(buf).convert("RGBA")
        except: return None

    def generar_plano_perfecto(self, gml_path: Path, output_path: Path, ref: str, info_afecciones: Dict[str, Any] = None):
        """Genera un mapa de alta calidad tipo 'Plano Perfecto' usando Matplotlib e IGN."""
        if not GEOTOOLS_AVAILABLE:
            logger.error("No se puede generar plano perfecto: Geotools no disponible")
            return False
            
        try:
            # 1. Cargar parcela y proyectar
            parcela = gpd.read_file(gml_path).to_crs(epsg=3857)
            
            # 2. Configurar figura
            fig, ax = plt.subplots(1, 1, figsize=(10, 8), dpi=150)
            
            # Límites con margen del 50%
            minx, miny, maxx, maxy = parcela.total_bounds
            margin = (maxx - minx) * 0.50
            ax.set_xlim(minx - margin, maxx + margin)
            ax.set_ylim(miny - margin, maxy + margin)
            
            # 3. Añadir mapa base del IGN
            url_ign_base = "https://www.ign.es/wmts/ign-base?layer=IGNBaseTodo&style=default&tilematrixset=GoogleMapsCompatible&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image/jpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"
            
            try:
                cx.add_basemap(ax, source=url_ign_base, crs=3857, attribution="IGN - Base")
            except Exception as e:
                logger.warning(f"Error añadiendo basemap: {e}. Usando fondo blanco.")
                
            # 4. Dibujar parcela (borde rojo grueso)
            parcela.plot(ax=ax, color="none", edgecolor="red", linewidth=3, zorder=10)
            
            # 5. Título y Detalles
            titulo = f"MAPA: Intersección Parcela {ref}"
            plt.title(titulo, loc='left', pad=20, fontsize=12, fontweight='bold')
            
            if info_afecciones:
                total = info_afecciones.get('total', 0)
                detalle = info_afecciones.get('detalle', {})
                
                txt_secundario = f"Afección Total: {total:.2f}%"
                if detalle:
                    # Mostrar primeros 3 detalles
                    det_str = ", ".join([f"{k}: {v:.2f}%" for k, v in list(detalle.items())[:3]])
                    if det_str: txt_secundario += f" | Detalle: {det_str}"
                
                fig.text(0.02, 0.93, txt_secundario, fontsize=9, color='darkred')

            # 6. Limpieza final y guardado
            ax.axis("off")
            plt.tight_layout(pad=2)
            
            # Crear directorio si no existe
            output_path.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(str(output_path), dpi=150, bbox_inches='tight', pad_inches=0.3)
            plt.close(fig)
            
            logger.info(f"✅ Plano perfecto generado: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error generando plano perfecto: {e}")
            return False

    def descargar_set_capas_completo(self, referencia, coords, output_dir: Path):
        """Descarga set de mapas multiescala con afecciones integradas y porcentajes matriciales"""
        if not GEOTOOLS_AVAILABLE: return []
        
        ref = self.limpiar_referencia(referencia)
        img_dir = output_dir / "images"
        img_dir.mkdir(exist_ok=True, parents=True)
        
        gml_path = output_dir / "gml" / f"{ref}_parcela.gml"
        lon, lat = coords['lon'], coords['lat']
        
        # Preparar geometría para análisis matricial y dibujo
        parcela_wgs84 = None
        gml_coords = None
        if gml_path.exists():
            try:
                gdf = gpd.read_file(gml_path)
                if gdf.crs is None: gdf.set_crs("EPSG:25830", inplace=True)
                gdf_wgs = gdf.to_crs("EPSG:4326")
                parcela_wgs84 = gdf_wgs.geometry.iloc[0]
                gml_coords = self.extraer_coordenadas_gml(gml_path)
            except Exception as e:
                logger.warning(f"No se pudo cargar geometría para análisis matricial/dibujo: {e}")

        niveles = {1: "España", 2: "Regional", 3: "Local", 4: "Parcela"}
        
        # Capas a procesar con su config WMS
        capas_config = [
            (self.wms_urls["inundabilidad_100años"], "NZ.RiskZone", 100, "ZONA INUNDABLE (T100)"),
            (self.wms_urls["red_natura"], "PS.ProtectedSite", 90, "RED NATURA 2000"),
            (self.wms_urls["vias_pecuarias"], "Vias_Pecuarias", 130, "VÍA PECUARIA"),
            (self.wms_urls["montes_utilidad_publica"], "MUP", 110, "MONTE PÚBLICO"),
            (self.wms_urls["espacios_protegidos"], "PS.ProtectedSite", 100, "ESPACIO PROTEGIDO"),
            (self.wms_urls["planeamiento"], "PlaneamientoGeneral", 120, "PLANEAMIENTO"),
            (self.wms_urls["dominio_maritimo"], "Demarcaciones", 100, "DOMINIO MARÍTIMO"),
            (self.wms_urls["catastro_https"], "Catastro", 140, None)
        ]
        
        resumen = []
        metadata_images = {}
        info_porcentajes = {} # Almacenar porcentajes detectados

        for n, nombre in niveles.items():
            # Usar BBOX dinámico para el nivel de parcela (Zoom 4)
            if n == 4 and parcela_wgs84:
                bbox_str = self.calcular_bbox_dinamico(list(parcela_wgs84.exterior.coords), zoom_factor=1.3)
            else:
                bbox_str = self.calcular_bbox(lon, lat, n)
                
            img_final = self._get_wms_layer(self.wms_urls["pnoa"], bbox_str, "OI.OrthoimageCoverage", False)
            if not img_final: continue

            avisos_detectados = []
            for url, layer, alpha, alerta in capas_config:
                overlay = self._get_wms_layer(url, bbox_str, layer)
                if overlay:
                    # Análisis matricial en nivel Local/Parcela
                    if n >= 3 and alerta and parcela_wgs84:
                        pct = self.calcular_porcentaje_pixeles(parcela_wgs84, overlay, bbox_str)
                        if pct > 0:
                            info_porcentajes[alerta] = max(info_porcentajes.get(alerta, 0), pct)
                            avisos_detectados.append(f"{alerta} ({pct:.1f}%)")
                    elif alerta and overlay.getextrema()[3][1] > 0:
                        avisos_detectados.append(alerta)
                        
                    overlay.putalpha(alpha)
                    img_final.alpha_composite(overlay)

            # SUPERPONER SILUETA ROJA (Siempre, en todos los niveles)
            if gml_coords and PILLOW_AVAILABLE:
                width, height = img_final.size
                pixels = self.convertir_coordenadas_a_pixel(gml_coords, bbox_str, width, height)
                if pixels and len(pixels) > 2:
                    # Dibujamos directamente sobre img_final usando overlay para transparencia si se quiere, 
                    # pero aquí lo hacemos sólido según petición "que destaque"
                    draw_overlay = Image.new("RGBA", img_final.size, (0, 0, 0, 0))
                    draw = ImageDraw.Draw(draw_overlay)
                    if pixels[0] != pixels[-1]: pixels.append(pixels[0])
                    draw.line(pixels, fill=(255, 0, 0, 255), width=5) # Más grueso para destacar
                    img_final.alpha_composite(draw_overlay)

            draw = ImageDraw.Draw(img_final)
            draw.rectangle([0, 0, 1200, 60], fill=(0, 0, 0, 200))
            draw.text((20, 20), f"REF: {ref} | VISTA: {nombre}", fill="white")
            
            if avisos_detectados:
                txt_alerta = " | ".join(list(set(avisos_detectados)))
                draw.rectangle([0, 1140, 1200, 1200], fill=(200, 0, 0, 230))
                draw.text((20, 1160), f"ALERTA: {txt_alerta}", fill="white")

            filename = f"{ref}_Catastro_zoom{n}_{nombre}.png"
            path_img = img_dir / filename
            img_final.save(path_img)
            
            b = [float(x) for x in bbox_str.split(',')]
            leaflet_bbox = [[b[1], b[0]], [b[3], b[2]]]
            metadata_images[filename] = {
                "bbox": leaflet_bbox,
                "zoom": n,
                "nombre": nombre,
                "avisos": avisos_detectados,
                "porcentajes": info_porcentajes if n == 4 else None
            }

            if n == 4:
                comp_filename = f"{ref}_composicion.png"
                comp_path = img_dir / comp_filename
                img_final.save(comp_path)
                metadata_images[comp_filename] = metadata_images[filename]
                resumen.append({"nivel": "composicion", "path": str(comp_path), "avisos": avisos_detectados, "porcentajes": info_porcentajes})

            resumen.append({"nivel": nombre, "path": str(path_img), "avisos": avisos_detectados})
            logger.info(f"    📷 Generado Zoom {n}: {nombre} (con silueta destacada)")

        with open(img_dir / "metadata.json", "w", encoding='utf-8') as f:
            json.dump(metadata_images, f, indent=2, ensure_ascii=False)

        return resumen

    def _crear_zip(self, carpeta: Path, zip_path: Path):
        """Crea ZIP con todo el contenido de la carpeta"""
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in carpeta.rglob("*"):
                if file.is_file():
                    zipf.write(file, arcname=file.relative_to(carpeta))
        logger.info(f"📦 ZIP creado: {zip_path}")
        return zip_path

    def descargar_todo_completo(self, referencia: str) -> Tuple[bool, Optional[Path], Dict[str, float]]:
        """Descarga completa con pipeline analítico y porcentajes matriciales"""
        ref = self.limpiar_referencia(referencia)
        ref_dir = self.output_dir / ref
        zip_path = self.output_dir / f"{ref}_completo.zip"
        pixel_data = {}

        # Cache check (opcional, desactivamos para asegurar nuevos cambios)
        # if zip_path.exists(): ...

        ref_dir.mkdir(parents=True, exist_ok=True)
        for d in ["json", "html", "gml", "images", "pdf"]: (ref_dir / d).mkdir(exist_ok=True)

        # 1. GML (Fallback strategy)
        gml_path = self.descargar_parcela_gml(ref, ref_dir)
        if not gml_path:
            logger.info("  ⚠️ Fallback a descarga robusta...")
            if self._descargar_gml_robust(ref, ref_dir):
                gml_path = ref_dir / "gml" / f"{ref}_parcela.gml"

        if not gml_path:
            logger.error(f"❌ No se pudo obtener GML para {ref}")
            return False, None, {}

        # 2. Coordenadas y Datos
        coords = self.obtener_coordenadas(ref)
        data = self.obtener_datos_basicos(ref)
        if data:
            with open(ref_dir / "json" / f"{ref}_info.json", "w", encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        # 3. Mapas con análisis matricial y siluetas en todos los niveles
        if coords:
            resumen_mapas = self.descargar_set_capas_completo(ref, coords, ref_dir)
            # Extraer porcentajes del resumen de mapas (nivel composicion)
            for item in resumen_mapas:
                if item.get("nivel") == "composicion" and item.get("porcentajes"):
                    pixel_data.update(item["porcentajes"])
            
            # Superponer contornos extra sobre cualquier imagen que haya quedado
            # (Aunque descargar_set_capas_completo ya lo hace, lo mantenemos como robustez)
            bbox_str = self.calcular_bbox(coords['lon'], coords['lat'], 4)
            self.superponer_contorno_parcela(ref, bbox_str, ref_dir)

        # 4. KML conversion
        self.convertir_gml_a_kml(gml_path, ref_dir / "gml" / f"{ref}_parcela.kml")
        
        # 5. Edificios y Fichas (Oficial y Técnica)
        self.descargar_edificio_gml(ref, ref_dir)
        # self.descargar_ficha_catastral(ref, ref_dir) # Opcional si la oficial es suficiente
        self.descargar_consulta_descriptiva_pdf(ref, ref_dir)

        # 6. Finalizar ZIP
        self._crear_zip(ref_dir, zip_path)
        return True, zip_path, pixel_data

    def obtener_datos_catastrales(self, referencia: str):
        """Método de compatibilidad para API"""
        ref = self.limpiar_referencia(referencia)
        try:
            exito, zip_path, pixel_data = self.descargar_todo_completo(ref)
            if not exito: return {"status": "error", "message": "Error en descarga"}
            
            ref_dir = self.output_dir / ref
            gml_path = ref_dir / "gml" / f"{ref}_parcela.gml"
            img_path = None
            for img in (ref_dir / "images").glob(f"*zoom4*.png"): img_path = img; break
            
            return {
                "status": "success", "referencia": ref,
                "vector": str(gml_path) if gml_path.exists() else None,
                "wms_layers": {"composicion": str(img_path) if img_path else None},
                "zip_path": f"/outputs/{ref}_completo.zip" if zip_path else None,
                "kml": f"/outputs/{ref}/gml/{ref}_parcela.kml",
                "directorio": str(ref_dir)
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "referencia": ref}

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python catastro_downloader.py <referencia>")
        sys.exit(1)
    downloader = CatastroDownloader(output_dir="outputs")
    exito, zip_p = downloader.descargar_todo_completo(sys.argv[1])
    print(f"Resultado: {'Éxito' if exito else 'Fallo'}. ZIP: {zip_p}")
