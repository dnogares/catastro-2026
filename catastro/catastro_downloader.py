#!/usr/bin/env python3
"""
catastro/catastro_downloader.py
Downloader completo integrado con todos los métodos necesarios
Combina funcionalidad de CatastroCompleteDownloader con API simplificada
"""

import os
import time
import json
import requests
import zipfile
from pathlib import Path
from io import BytesIO
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dependencias opcionales
try:
    import geopandas as gpd
    from shapely.geometry import mapping, Point
    GEOPANDAS_AVAILABLE = True
except Exception:
    GEOPANDAS_AVAILABLE = False
    logger.warning("GeoPandas no disponible - funcionalidad limitada")

try:
    from PIL import Image, ImageDraw, ImageFont
    PILLOW_AVAILABLE = True
except Exception:
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
    Downloader completo de datos catastrales
    Integra descarga de datos, geometrías, mapas y análisis
    """
    
    def __init__(self, output_dir="outputs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_catastro = "https://ovc.catastro.meh.es"

        # Capas WMS del Catastro
        self.capas_wms = {
            'catastro': 'Catastro',
            'ortofoto': 'PNOA',
            'callejero': 'Callejero',
            'hidrografia': 'Hidrografia',
        }

        # Servicios WFS para afectaciones
        self.servicios_wfs = {
            'espacios_naturales': {
                'url': 'https://www.miteco.gob.es/wfs/espacios_protegidos',
                'layer': 'espacios_protegidos',
                'descripcion': 'Espacios Naturales Protegidos',
                'categoria': 'ambiental',
                'impacto_valor': 'MEDIO-ALTO'
            },
            'zonas_inundables': {
                'url': 'https://www.miteco.gob.es/wfs/snczi',
                'layer': 'zonas_inundables',
                'descripcion': 'Zonas de Riesgo de Inundación SNCZI',
                'categoria': 'riesgos',
                'impacto_valor': 'ALTO'
            },
        }

    def limpiar_referencia(self, ref: str) -> str:
        """Limpia y normaliza referencia catastral"""
        return ref.replace(' ', '').strip().upper()

    def obtener_datos_basicos(self, referencia: str):
        """Obtiene datos básicos de la referencia catastral"""
        ref = self.limpiar_referencia(referencia)
        
        try:
            url_json = f"{self.base_catastro}/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/json/Geo_RCToWGS84/{ref}"
            r = safe_get(url_json, timeout=20)
            
            if r.status_code == 200:
                data = r.json()
                return data
            else:
                logger.warning(f"Error obteniendo datos básicos: {r.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error en obtener_datos_basicos: {e}")
            return {}

    def extraer_coordenadas_desde_gml(self, gml_path):
        """Extrae coordenadas del GML de parcela"""
        try:
            tree = ET.parse(gml_path)
            root = tree.getroot()
            
            ns = {
                'gml': 'http://www.opengis.net/gml/3.2',
                'cp': 'http://inspire.ec.europa.eu/schemas/cp/4.0'
            }
            
            # Método 1: Punto de referencia
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
            
            # Método 2: Centroide del polígono
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
            
        except Exception as e:
            logger.error(f"Error extrayendo coordenadas: {e}")
            return None

    def utm_a_wgs84(self, x_utm, y_utm, epsg='25830'):
        """Convierte coordenadas UTM a WGS84"""
        if not GEOPANDAS_AVAILABLE:
            return None
        
        try:
            gdf = gpd.GeoDataFrame(
                geometry=[Point(x_utm, y_utm)], 
                crs=f'EPSG:{epsg}'
            )
            gdf_wgs84 = gdf.to_crs('EPSG:4326')
            point_wgs84 = gdf_wgs84.geometry.iloc[0]
            
            return {
                'lon': point_wgs84.x,
                'lat': point_wgs84.y,
                'srs': 'EPSG:4326'
            }
        except Exception as e:
            logger.error(f"Error convirtiendo coordenadas: {e}")
            return None

    def convertir_gml_a_kml(self, gml_path: Path, kml_path: Path) -> bool:
        """Convierte archivo GML a KML usando GeoPandas"""
        if not GEOPANDAS_AVAILABLE:
            logger.warning("GeoPandas no disponible para conversión KML")
            return False
            
        try:
            import fiona
            # Habilitar driver KML si no está habilitado
            if 'KML' not in fiona.drvsupport.supported_drivers:
                fiona.drvsupport.supported_drivers['KML'] = 'rw'
            
            gdf = gpd.read_file(str(gml_path))
            if gdf.empty:
                return False
                
            # Reproyectar a WGS84 para KML
            if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            
            gdf.to_file(str(kml_path), driver='KML')
            logger.info(f"    ✅ KML generado: {kml_path.name}")
            return True
        except Exception as e:
            logger.error(f"Error convirtiendo GML a KML: {e}")
            return False

    def obtener_coordenadas(self, referencia: str, gml_parcela_path=None):
        """Obtiene coordenadas con estrategia de fallback"""
        ref = self.limpiar_referencia(referencia)
        
        # Método 1: Desde GML de parcela
        if gml_parcela_path and os.path.exists(gml_parcela_path):
            coords_utm = self.extraer_coordenadas_desde_gml(gml_parcela_path)
            if coords_utm:
                coords_wgs84 = self.utm_a_wgs84(coords_utm['x_utm'], coords_utm['y_utm'])
                if coords_wgs84:
                    return coords_wgs84
        
        # Método 2: Servicio JSON rápido
        try:
            url_json = f"{self.base_catastro}/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/json/Geo_RCToWGS84/{ref}"
            r = safe_get(url_json, timeout=20)
            if r.status_code == 200:
                data = r.json()
                if 'geo' in data and 'xcen' in data['geo'] and 'ycen' in data['geo']:
                    return {
                        'lon': float(data['geo']['xcen']),
                        'lat': float(data['geo']['ycen']),
                        'srs': 'EPSG:4326'
                    }
        except Exception as e:
            logger.warning(f"Error método JSON: {e}")

        # Método 3: Servicio XML
        try:
            url_xml = f"{self.base_catastro}/ovcservweb/ovcswlocalizacionrc/ovccoordenadas.asmx/Consulta_RCCOOR"
            params = {'SRS': 'EPSG:4326', 'RC': ref}
            r = safe_get(url_xml, params=params, timeout=20)
            if r.status_code == 200:
                root = ET.fromstring(r.content)
                ns = {'cat': 'http://www.catastro.meh.es/'}
                coord = root.find('.//cat:coord', ns)
                if coord is not None:
                    geo = coord.find('cat:geo', ns)
                    if geo is not None:
                        xcen = geo.find('cat:xcen', ns)
                        ycen = geo.find('cat:ycen', ns)
                        if xcen is not None and ycen is not None:
                            return {
                                'lon': float(xcen.text),
                                'lat': float(ycen.text),
                                'srs': 'EPSG:4326'
                            }
        except Exception as e:
            logger.warning(f"Error método XML: {e}")

        return None

    def descargar_parcela_gml(self, referencia: str, output_dir: Path):
        """Descarga geometría GML de la parcela"""
        ref = self.limpiar_referencia(referencia)
        gml_dir = output_dir / "gml"
        gml_dir.mkdir(exist_ok=True)
        
        try:
            logger.info("  📐 Descargando geometría parcela (GML)...")
            
            url_gml = f"{self.base_catastro}/INSPIRE/wfsCP.aspx"
            params = {
                'service': 'wfs',
                'version': '2.0.0',
                'request': 'GetFeature',
                'STOREDQUERIE_ID': 'GetParcel',
                'refcat': ref,
                'srsname': 'EPSG:25830'
            }
            
            r = safe_get(url_gml, params=params, timeout=60)
            
            if r.status_code == 200 and len(r.content) > 500:
                if b'ExceptionReport' not in r.content and b'gml:' in r.content:
                    gml_path = gml_dir / f"{ref}_parcela.gml"
                    with open(gml_path, 'wb') as f:
                        f.write(r.content)
                    logger.info(f"    ✅ Parcela GML guardada")
                    return gml_path
            
            logger.warning("    ⚠️ No se pudo descargar GML de parcela")
            return None
            
        except Exception as e:
            logger.error(f"    ❌ Error descargando parcela GML: {e}")
            return None

    def descargar_edificio_gml(self, referencia: str, output_dir: Path):
        """Descarga geometría GML del edificio"""
        ref = self.limpiar_referencia(referencia)
        gml_dir = output_dir / "gml"
        gml_dir.mkdir(exist_ok=True)
        
        try:
            logger.info("  🏢 Descargando geometría edificio (GML)...")
            
            url_gml = f"{self.base_catastro}/INSPIRE/wfsCP.aspx"
            params = {
                'service': 'wfs',
                'version': '2.0.0',
                'request': 'GetFeature',
                'STOREDQUERIE_ID': 'GetBuilding',
                'refcat': ref,
                'srsname': 'EPSG:25830'
            }
            
            r = safe_get(url_gml, params=params, timeout=60)
            
            if r.status_code == 200 and len(r.content) > 500:
                if b'ExceptionReport' not in r.content and b'gml:' in r.content:
                    gml_path = gml_dir / f"{ref}_edificio.gml"
                    with open(gml_path, 'wb') as f:
                        f.write(r.content)
                    logger.info(f"    ✅ Edificio GML guardado")
                    return gml_path
            
            logger.warning("    ⚠️ No hay edificio en esta parcela")
            return None
            
        except Exception as e:
            logger.error(f"    ❌ Error descargando edificio GML: {e}")
            return None

    def descargar_ficha_catastral(self, referencia: str, output_dir: Path):
        """Descarga la ficha catastral oficial en PDF"""
        ref = self.limpiar_referencia(referencia)
        pdf_dir = output_dir / "pdf"
        pdf_dir.mkdir(exist_ok=True)
        
        try:
            logger.info("  📄 Descargando ficha catastral...")
            
            # URL directa del PDF
            url_pdf = f"{self.base_catastro}/OVCServWeb/OVCWcfCallejero/COVCCoordenadas.svc/Consulta_DNPRC_PDF"
            params_pdf = {
                'RefCat': ref,
                'RCCompleta': 'SI'
            }
            
            r_pdf = safe_get(url_pdf, params=params_pdf, timeout=60)
            
            if r_pdf.status_code == 200 and len(r_pdf.content) > 1000:
                pdf_path = pdf_dir / f"{ref}_ficha_catastral.pdf"
                with open(pdf_path, 'wb') as f:
                    f.write(r_pdf.content)
                logger.info("    ✅ Ficha catastral guardada")
                return pdf_path
            
            logger.warning("    ⚠️ No se pudo descargar la ficha catastral")
            return None
            
        except Exception as e:
            logger.error(f"    ❌ Error descargando ficha: {e}")
            return None

    def calcular_bbox_escala(self, lon_centro, lat_centro, nivel):
        """Calcula BBOX para 4 niveles de zoom"""
        radios = {
            1: 600000,  # España entera
            2: 25000,   # Región
            3: 1000,    # Municipio
            4: 150      # Parcela
        }
        
        radio = radios.get(nivel, 150)
        delta_lat = radio / 111000.0
        delta_lon = radio / 85000.0
        
        return f"{lon_centro - delta_lon},{lat_centro - delta_lat},{lon_centro + delta_lon},{lat_centro + delta_lat}"

    def _descargar_wms_generico(self, bbox, layers, output_path: Path, transparent=False):
        """Helper para descargas WMS estandarizadas"""
        wms_url = f"{self.base_catastro}/Cartografia/WMS/ServidorWMS.aspx"
        params = {
            'SERVICE': 'WMS', 'VERSION': '1.1.1', 'REQUEST': 'GetMap',
            'LAYERS': layers, 'STYLES': '', 'SRS': 'EPSG:4326', 
            'BBOX': bbox,
            'WIDTH': '1600', 'HEIGHT': '1600',
            'FORMAT': 'image/png', 
            'TRANSPARENT': 'TRUE' if transparent else 'FALSE'
        }
        
        if layers == "PNOA":
            params['TRANSPARENT'] = 'FALSE'
            
        try:
            r = safe_get(wms_url, params=params, timeout=60)
            if r.status_code == 200 and len(r.content) > 1000:
                with open(output_path, 'wb') as f:
                    f.write(r.content)
                return output_path
        except Exception:
            pass
        return None

    def generar_silueta_png(self, gml_path: Path, bbox_str: str, output_path: Path):
        """Genera PNG transparente con silueta roja"""
        if not GEOPANDAS_AVAILABLE or not gml_path or not gml_path.exists():
            return None
        
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt

            gdf = gpd.read_file(gml_path)
            if gdf.empty:
                return None

            if gdf.crs is None:
                gdf.set_crs("EPSG:25830", inplace=True)
            
            if gdf.crs.to_string() != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")

            coords = [float(x) for x in bbox_str.split(',')]
            minx, miny, maxx, maxy = coords

            fig = plt.figure(figsize=(16, 16), dpi=100)
            ax = plt.Axes(fig, [0., 0., 1., 1.])
            ax.set_axis_off()
            fig.add_axes(ax)

            ax.set_xlim(minx, maxx)
            ax.set_ylim(miny, maxy)

            gdf.plot(ax=ax, facecolor=(1, 0, 0, 0.2), edgecolor='red', linewidth=3)
            
            fig.savefig(str(output_path), transparent=True, dpi=100)
            plt.close(fig)
            return output_path

        except Exception as e:
            logger.error(f"Error generando silueta: {e}")
            return None

    def descargar_set_capas_completo(self, referencia, coords, output_dir: Path):
        """Descarga set completo de ortofotos + capas + silueta"""
        ref = self.limpiar_referencia(referencia)
        images_dir = output_dir / "images"
        images_dir.mkdir(exist_ok=True, parents=True)
        
        gml_path = output_dir / "gml" / f"{ref}_parcela.gml"
        if not gml_path.exists():
            try:
                gml_path = self.descargar_parcela_gml(ref, output_dir)
            except:
                gml_path = None
        
        lon, lat = coords['lon'], coords['lat']
        
        niveles = [
            (1, "Nacional"),
            (2, "Regional"),
            (3, "Local"),
            (4, "Parcela")
        ]
        
        resumen_descargas = []

        for nivel, nombre_nivel in niveles:
            bbox = self.calcular_bbox_escala(lon, lat, nivel)
            suffix = f"zoom{nivel}_{nombre_nivel}"
            
            path_orto = self._descargar_wms_generico(
                bbox, "PNOA", images_dir / f"{ref}_Ortofoto_{suffix}.png"
            )
            
            path_cat = self._descargar_wms_generico(
                bbox, "Catastro", images_dir / f"{ref}_Catastro_{suffix}.png", transparent=True
            )
            
            path_call = self._descargar_wms_generico(
                bbox, "Callejero", images_dir / f"{ref}_Callejero_{suffix}.png", transparent=True
            )
            
            path_hidro = self._descargar_wms_generico(
                bbox, "Hidrografia", images_dir / f"{ref}_Hidrografia_{suffix}.png", transparent=True
            )
            
            path_silueta = None
            if gml_path and gml_path.exists():
                try:
                    path_silueta = self.generar_silueta_png(
                        gml_path, bbox, images_dir / f"{ref}_Silueta_{suffix}.png"
                    )
                except Exception as e:
                    logger.warning(f"No se pudo generar silueta zoom {nivel}: {e}")
            
            resumen_descargas.append({
                "nivel": nombre_nivel,
                "ortofoto": str(path_orto) if path_orto else None,
                "catastro": str(path_cat) if path_cat else None,
                "callejero": str(path_call) if path_call else None,
                "hidrografia": str(path_hidro) if path_hidro else None,
                "silueta": str(path_silueta) if path_silueta else None
            })
            
            logger.info(f"    📷 Generado Zoom {nivel}: {nombre_nivel}")

        return resumen_descargas

    def generar_html_descriptivo(self, data, ref):
        """Genera HTML descriptivo básico"""
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Consulta {ref}</title>
<style>body {{ font-family: sans-serif; margin: 20px; }} 
.seccion {{ margin-bottom: 20px; padding: 10px; border: 1px solid #ccc; }}</style>
</head><body><h1>Consulta {ref}</h1>
"""
        if data and 'ldt' in data:
            html += f"<div class='seccion'><h3>Dirección</h3><p>{data['ldt']}</p></div>"
        html += "</body></html>"
        return html

    def descargar_consulta_descriptiva(self, referencia: str, output_dir: Path):
        """Descarga consulta descriptiva y guarda JSON/HTML"""
        ref = self.limpiar_referencia(referencia)
        json_dir = output_dir / "json"
        json_dir.mkdir(exist_ok=True)
        
        try:
            url_json = f"{self.base_catastro}/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/json/Geo_RCToWGS84/{ref}"
            r = safe_get(url_json, timeout=20)
            if r.status_code == 200:
                data = r.json()
                out_json = json_dir / f"{ref}_consulta_descriptiva.json"
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                html_dir = output_dir / "html"
                html_dir.mkdir(exist_ok=True)
                html = self.generar_html_descriptivo(data, ref)
                out_html = html_dir / f"{ref}_consulta_descriptiva.html"
                with open(out_html, 'w', encoding='utf-8') as f:
                    f.write(html)
                
                return data
        except Exception as e:
            logger.error(f"Error consulta descriptiva: {e}")
        return None

    def crear_zip_comprimido(self, referencia: str, ref_dir: Path) -> Optional[Path]:
        """Crea ZIP comprimido con todos los archivos"""
        try:
            zip_path = self.output_dir / f"{referencia}_completo.zip"
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in ref_dir.rglob('*'):
                    if file_path.is_file():
                        arcname = file_path.relative_to(ref_dir.parent)
                        zipf.write(file_path, arcname)
            
            logger.info(f"  ✅ ZIP creado: {zip_path}")
            return zip_path
        except Exception as e:
            logger.error(f"  ❌ Error creando ZIP: {e}")
            return None

    def descargar_todo_completo(self, referencia: str) -> Tuple[bool, Optional[Path]]:
        """Descarga completa de todos los datos de una referencia"""
        logger.info(f"\n{'='*70}\n🚀 Procesando: {referencia}\n{'='*70}")
        ref = self.limpiar_referencia(referencia)
        
        ref_dir = self.output_dir / ref
        ref_dir.mkdir(parents=True, exist_ok=True)
        
        carpetas = {
            'json': ref_dir / "json",
            'html': ref_dir / "html", 
            'gml': ref_dir / "gml",
            'images': ref_dir / "images",
            'pdf': ref_dir / "pdf"
        }
        
        for carpeta in carpetas.values():
            carpeta.mkdir(exist_ok=True)

        resultados = {}
        
        # 1. Datos básicos
        logger.info("\n📋 DATOS BÁSICOS")
        try:
            data = self.obtener_datos_basicos(ref)
            if data:
                with open(carpetas['json'] / f"{ref}_info.json", "w", encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                html_content = self.generar_html_descriptivo(data, ref)
                with open(carpetas['html'] / f"{ref}_info.html", "w", encoding='utf-8') as f:
                    f.write(html_content)
                
                resultados['datos_basicos'] = True
            else:
                resultados['datos_basicos'] = False
        except Exception as e:
            logger.error(f"Error datos básicos: {e}")
            resultados['datos_basicos'] = False

        # 2. Consulta descriptiva
        logger.info("\n📋 CONSULTA DESCRIPTIVA")
        resultados['consulta_descriptiva'] = self.descargar_consulta_descriptiva(ref, ref_dir) is not None
        
        # 3. Parcela GML
        logger.info("\n🗺️ GEOMETRÍA PARCELA")
        archivo_parcela_gml = self.descargar_parcela_gml(ref, ref_dir)
        resultados['parcela_gml'] = archivo_parcela_gml is not None
        
        # 4. Coordenadas
        logger.info("\n📍 COORDENADAS")
        coords = self.obtener_coordenadas(ref, archivo_parcela_gml)
        
        if not coords and data and 'geo' in data:
            try:
                coords = {
                    'lon': float(data['geo']['xcen']), 
                    'lat': float(data['geo']['ycen']),
                    'srs': 'EPSG:4326'
                }
                logger.info("  ✅ Coordenadas desde datos básicos")
            except:
                pass
        
        if coords:
            logger.info(f"  ✅ Coordenadas: {coords['lat']:.6f}, {coords['lon']:.6f}")
        else:
            logger.warning("  ⚠️ No se pudieron obtener coordenadas")
        
        # 5. Capas WMS
        if coords:
            logger.info("\n🖼️ ORTOFOTOS Y CAPAS")
            try:
                resultados['capas_wms'] = self.descargar_set_capas_completo(ref, coords, ref_dir)
            except Exception as e:
                logger.error(f"Error capas WMS: {e}")
                resultados['capas_wms'] = []
        else:
            resultados['capas_wms'] = []
        
        # 6. Edificio GML
        logger.info("\n🏢 GEOMETRÍA EDIFICIO")
        archivo_edificio_gml = self.descargar_edificio_gml(ref, ref_dir)
        resultados['edificio_gml'] = archivo_edificio_gml is not None

        # 6.b Conversión KML
        logger.info("\n🔄 CONVERSIÓN KML")
        if archivo_parcela_gml and archivo_parcela_gml.exists():
            kml_parcela_path = carpetas['gml'] / f"{ref}_parcela.kml"
            self.convertir_gml_a_kml(archivo_parcela_gml, kml_parcela_path)
        
        if archivo_edificio_gml and archivo_edificio_gml.exists():
            kml_edificio_path = carpetas['gml'] / f"{ref}_edificio.kml"
            self.convertir_gml_a_kml(archivo_edificio_gml, kml_edificio_path)

        # 7. Ficha catastral
        logger.info("\n📄 FICHA CATASTRAL")
        resultados['ficha_catastral'] = self.descargar_ficha_catastral(ref, ref_dir) is not None

        # 8. ZIP
        logger.info("\n📦 CREANDO ZIP")
        zip_path = self.crear_zip_comprimido(ref, ref_dir)
        resultados['zip_creado'] = zip_path is not None
        
        # Resumen
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ COMPLETADO: {ref}")
        logger.info(f"{'='*70}\n📊 RESUMEN:")
        for k, v in resultados.items():
            emoji = '✅' if v else '❌'
            logger.info(f"  {emoji} {k}: {v}")
        print()
        
        exito_minimo = resultados.get('parcela_gml', False) and coords is not None
        return exito_minimo, zip_path

    def obtener_datos_catastrales(self, referencia: str):
        """Método de compatibilidad para API - Descarga y retorna paths"""
        ref = self.limpiar_referencia(referencia)
        
        try:
            exito, zip_path = self.descargar_todo_completo(ref)
            
            if not exito:
                return {
                    "status": "error",
                    "message": "Error descargando datos catastrales"
                }
            
            ref_dir = self.output_dir / ref
            gml_path = ref_dir / "gml" / f"{ref}_parcela.gml"
            
            composicion_path = None
            images_dir = ref_dir / "images"
            if images_dir.exists():
                for img in images_dir.glob(f"{ref}*zoom4*.png"):
                    composicion_path = img
                    break
                if not composicion_path:
                    for img in images_dir.glob(f"{ref}*.png"):
                        composicion_path = img
                        break
            
            return {
                "status": "success",
                "referencia": ref,
                "vector": str(gml_path) if gml_path.exists() else None,
                "wms_layers": {
                    "composicion": str(composicion_path) if composicion_path and composicion_path.exists() else None
                },
                "zip_path": f"/outputs/{ref}_completo.zip" if zip_path else None,
                "directorio": str(ref_dir)
            }
            
        except Exception as e:
            logger.error(f"Error en obtener_datos_catastrales: {e}")
            return {
                "status": "error",
                "message": str(e),
                "referencia": ref
            }

    def procesar_lote(self, referencias: List[str]) -> Dict[str, Tuple[bool, Optional[Path]]]:
        """Procesa lote de referencias"""
        resultados = {}
        total = len(referencias)
        
        logger.info(f"📦 Procesando lote de {total} referencias...")
        
        for idx, ref in enumerate(referencias, 1):
            try:
                logger.info(f"\n[{idx}/{total}] Procesando {ref}...")
                exito, zip_path = self.descargar_todo_completo(ref)
                resultados[ref] = (exito, zip_path)
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error en lote {ref}: {e}")
                resultados[ref] = (False, None)
        
        return resultados


# Testing directo
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python catastro_downloader.py <referencia_catastral>")
        print("Ejemplo: python catastro_downloader.py 1234567VK1234S0001WX")
        sys.exit(1)
    
    referencia = sys.argv[1]
    
    downloader = CatastroDownloader()
    print(f"🚀 Descargando: {referencia}")
    
    exito, zip_path = downloader.descargar_todo_completo(referencia)
    
    if exito:
        print(f"\n✅ ¡Completado exitosamente!")
        print(f"📦 ZIP generado: {zip_path}")
    else:
        print("\n❌ Error en la descarga")
        sys.exit(1)