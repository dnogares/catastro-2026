#!/usr/bin/env python3
"""
urbanismo/analisisurbano.py
Análisis urbanístico mejorado con clase, caché y manejo robusto de errores
"""

import os
import tempfile
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from dataclasses import dataclass

import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from io import BytesIO
from owslib.wms import WebMapService

# Configuración de logging
logger = logging.getLogger(__name__)

@dataclass
class ResultadosUrbanismo:
    """Estructura de datos para resultados del análisis urbanístico"""
    referencia: str
    area_total_m2: float
    porcentajes: Dict[str, float]
    areas_m2: Dict[str, float]
    mapa_path: Optional[str] = None
    txt_path: Optional[str] = None
    csv_path: Optional[str] = None
    timestamp: Optional[str] = None

class AnalisisUrbano:
    """
    Clase principal para análisis urbanístico de parcelas
    Integra descarga WFS/WMS, cálculo de porcentajes y generación de mapas
    """
    
    def __init__(self, output_dir: str = "resultados_urbanismo", encuadre_factor: float = 4.0):
        """
        Inicializa el analizador urbanístico
        
        Args:
            output_dir: Directorio base para resultados
            encuadre_factor: Factor de zoom para mapas (menor = más cerca)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.encuadre_factor = encuadre_factor
        
        # Caché para evitar descargas repetidas
        self._wfs_cache = {}
        self._wms_cache = {}
        
        # URLs de servicios (configurables)
        self.wfs_carm_url = "https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wfs?"
        self.wms_carm_url = "https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wms?"
        self.wms_ign_url = "https://www.ign.es/wms-inspire/pnoa-ma"
        
        # Nombres de capas
        self.wfs_layer = "SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun"
        self.wms_layer = "SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun"
        
        logger.info(f"AnalisisUrbano inicializado. Output: {self.output_dir}")

    def cargar_parcela(self, path_geojson: str) -> gpd.GeoDataFrame:
        """
        Carga parcela desde GeoJSON y reprojecta a Web Mercator
        
        Args:
            path_geojson: Ruta al archivo GeoJSON
            
        Returns:
            GeoDataFrame de la parcela en EPSG:3857
            
        Raises:
            FileNotFoundError: Si no existe el archivo
            ValueError: Si el archivo está vacío o no es válido
        """
        try:
            if not Path(path_geojson).exists():
                raise FileNotFoundError(f"No existe el archivo: {path_geojson}")
            
            gdf = gpd.read_file(path_geojson)
            
            if gdf.empty:
                raise ValueError(f"El archivo GeoJSON está vacío: {path_geojson}")
            
            logger.info(f"Parcela cargada: {path_geojson} ({len(gdf)} geometrías)")
            return gdf.to_crs(epsg=3857)  # Web Mercator para visualización
            
        except Exception as e:
            logger.error(f"Error cargando parcela {path_geojson}: {e}")
            raise

    def descargar_capa_wfs(self, base_url: str, typename: str, use_cache: bool = True) -> gpd.GeoDataFrame:
        """
        Descarga capa WFS como GeoDataFrame con caché optimizado
        
        Args:
            base_url: URL base del servicio WFS
            typename: Nombre de la capa a descargar
            use_cache: Si usar caché para evitar descargas repetidas
            
        Returns:
            GeoDataFrame en EPSG:25830 para cálculos de área
            
        Raises:
            requests.RequestException: Si falla la descarga
            ValueError: Si la respuesta no es válida
        """
        cache_key = f"{base_url}_{typename}"
        
        # Usar caché si está disponible
        if use_cache and cache_key in self._wfs_cache:
            logger.info(f"Usando capa WFS desde caché: {typename}")
            return self._wfs_cache[cache_key]
        
        try:
            params = {
                "service": "WFS",
                "version": "1.0.0",
                "request": "GetFeature",
                "typename": typename,
                "outputFormat": "json",
                "srsName": "EPSG:4326"
            }
            
            logger.info(f"Descargando capa WFS: {typename}")
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            
            if not response.content:
                raise ValueError("Respuesta vacía del servicio WFS")
            
            gdf = gpd.read_file(BytesIO(response.content))
            
            if gdf.empty:
                raise ValueError(f"La capa WFS está vacía: {typename}")
            
            # Estandarizar nombres de columnas a minúsculas
            gdf.columns = [c.lower() for c in gdf.columns]
            
            # Reproyectar a EPSG:25830 para cálculos de área precisos
            gdf = gdf.to_crs(epsg=25830)
            
            # Validar campos requeridos
            campos_requeridos = ['clasificacion', 'geometry']
            campos_faltantes = [c for c in campos_requeridos if c not in gdf.columns]
            if campos_faltantes:
                raise ValueError(f"Faltan campos requeridos en la capa: {campos_faltantes}")
            
            # Guardar en caché
            if use_cache:
                self._wfs_cache[cache_key] = gdf
            
            logger.info(f"Capa WFS descargada: {typename} ({len(gdf)} elementos)")
            return gdf
            
        except requests.RequestException as e:
            logger.error(f"Error de red descargando WFS {typename}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error procesando capa WFS {typename}: {e}")
            raise

    def calcular_porcentajes(self, gdf_parcela: gpd.GeoDataFrame, 
                           gdf_planeamiento: gpd.GeoDataFrame) -> Tuple[Dict[str, float], Dict[str, float]]:
        """
        Calcula porcentajes reales con subtipos de protección
        
        Args:
            gdf_parcela: GeoDataFrame de la parcela
            gdf_planeamiento: GeoDataFrame del planeamiento urbanístico
            
        Returns:
            Tuple: (areas_m2, porcentajes) con resultados por tipo de suelo
        """
        try:
            # Asegurar CRS para cálculos de área
            gdf_parcela_calc = gdf_parcela.to_crs(epsg=25830)
            
            # Calcular intersección
            interseccion = gpd.overlay(gdf_planeamiento, gdf_parcela_calc, how="intersection")
            
            if interseccion.empty:
                logger.warning("No hay intersección entre parcela y planeamiento")
                return {}, {}
            
            # Calcular áreas en m²
            interseccion["area_m2"] = interseccion.geometry.area
            
            # Validar campos necesarios
            if 'clasificacion' not in interseccion.columns:
                raise ValueError("Falta campo 'clasificacion' en la capa de planeamiento")
            
            # Crear campo combinado para diferenciar subtipos
            interseccion["tipo_suelo"] = interseccion["clasificacion"].copy()
            
            # Para suelos no urbanizables, añadir ámbito si existe
            if 'ambito' in interseccion.columns:
                mask_no_urb = interseccion["clasificacion"].str.contains("No Urbanizable", case=False, na=False)
                interseccion.loc[mask_no_urb, "tipo_suelo"] = (
                    interseccion["clasificacion"] + " - " + interseccion["ambito"].fillna("")
                )
            
            # Agrupar por tipo de suelo y sumar áreas
            resumen = interseccion.groupby("tipo_suelo")["area_m2"].sum()
            total_area = resumen.sum()
            
            if total_area == 0:
                logger.warning("El área total de intersección es 0")
                return {}, {}
            
            # Calcular porcentajes
            porcentajes = (resumen / total_area) * 100
            
            logger.info(f"Calculados {len(resumen)} tipos de suelo. Total: {total_area:.2f} m²")
            return resumen.to_dict(), porcentajes.to_dict()
            
        except Exception as e:
            logger.error(f"Error calculando porcentajes: {e}")
            return {}, {}

    def descargar_ortofoto(self, extent: Tuple[float, float, float, float], 
                         wms_url: Optional[str] = None) -> str:
        """
        Descarga ortofoto WMS (IGN PNOA) usando archivo temporal
        
        Args:
            extent: Tupla (minx, maxx, miny, maxy) en EPSG:3857
            wms_url: URL del servicio WMS (opcional, usa IGN por defecto)
            
        Returns:
            Ruta al archivo temporal de la ortofoto
            
        Raises:
            requests.RequestException: Si falla la descarga
        """
        wms_url = wms_url or self.wms_ign_url
        minx, maxx, miny, maxy = extent
        
        try:
            wms = WebMapService(wms_url, version="1.3.0")
            
            img = wms.getmap(
                layers=["OI.OrthoimageCoverage"],
                srs="EPSG:3857",
                bbox=(minx, miny, maxx, maxy),
                size=(1000, 1000),
                format="image/jpeg",
                transparent=True
            )
            
            # Crear archivo temporal
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
                f.write(img.read())
                ortofoto_path = f.name
            
            logger.debug(f"Ortofoto descargada: {ortofoto_path}")
            return ortofoto_path
            
        except Exception as e:
            logger.error(f"Error descargando ortofoto: {e}")
            raise

    def descargar_urbanismo(self, extent: Tuple[float, float, float, float],
                          wms_url: Optional[str] = None) -> str:
        """
        Descarga capa de urbanismo WMS (colores oficiales CARM)
        
        Args:
            extent: Tupla (minx, maxx, miny, maxy) en EPSG:3857
            wms_url: URL del servicio WMS (opcional, usa CARM por defecto)
            
        Returns:
            Ruta al archivo temporal de la capa de urbanismo
        """
        wms_url = wms_url or self.wms_carm_url
        minx, maxx, miny, maxy = extent
        
        try:
            wms = WebMapService(wms_url, version="1.3.0")
            
            img = wms.getmap(
                layers=[self.wms_layer],
                srs="EPSG:3857",
                bbox=(minx, miny, maxx, maxy),
                size=(1000, 1000),
                format="image/png",
                transparent=True
            )
            
            # Crear archivo temporal
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
                f.write(img.read())
                urbanismo_path = f.name
            
            logger.debug(f"Capa urbanismo descargada: {urbanismo_path}")
            return urbanismo_path
            
        except Exception as e:
            logger.error(f"Error descargando capa urbanismo: {e}")
            raise

    def descargar_leyenda(self, wms_url: Optional[str] = None) -> Optional[str]:
        """
        Descarga leyenda oficial WMS
        
        Args:
            wms_url: URL del servicio WMS (opcional, usa CARM por defecto)
            
        Returns:
            Ruta al archivo temporal de la leyenda o None si falla
        """
        wms_url = wms_url or self.wms_carm_url
        
        try:
            url = f"{wms_url}service=WMS&version=1.1.0&request=GetLegendGraphic&layer={self.wms_layer}&format=image/png"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Crear archivo temporal
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
                f.write(response.content)
                leyenda_path = f.name
            
            logger.debug(f"Leyenda descargada: {leyenda_path}")
            return leyenda_path
            
        except Exception as e:
            logger.warning(f"No se pudo descargar la leyenda oficial: {e}")
            return None

    def generar_mapa(self, parcela: gpd.GeoDataFrame, 
                    ortofoto_path: str, urbanismo_path: str, 
                    leyenda_path: Optional[str], extent: Tuple[float, float, float, float],
                    salida: str) -> str:
        """
        Genera mapa final con ortofoto + urbanismo + leyenda
        
        Args:
            parcela: GeoDataFrame de la parcela
            ortofoto_path: Ruta a la ortofoto
            urbanismo_path: Ruta a la capa de urbanismo
            leyenda_path: Ruta a la leyenda (opcional)
            extent: Extent del mapa en EPSG:3857
            salida: Ruta de salida para el mapa
            
        Returns:
            Ruta al mapa generado
        """
        try:
            fig, ax = plt.subplots(figsize=(10, 10))
            
            # Cargar y mostrar ortofoto
            ortofoto = plt.imread(ortofoto_path)
            ax.imshow(ortofoto, extent=extent, origin="upper")
            
            # Superponer capa de urbanismo con transparencia
            urbanismo_img = plt.imread(urbanismo_path)
            ax.imshow(urbanismo_img, extent=extent, origin="upper", alpha=0.5)
            
            # Dibujar límite de parcela en rojo
            parcela.boundary.plot(ax=ax, color="red", linewidth=2)
            
            # Configuración del mapa
            plt.title("Parcela sobre ortofoto + urbanismo (colores oficiales)", fontsize=14, pad=20)
            plt.axis("off")
            
            # Añadir leyenda si está disponible
            if leyenda_path and Path(leyenda_path).exists():
                leyenda_img = plt.imread(leyenda_path)
                ax_leyenda = fig.add_axes([0.75, 0.05, 0.2, 0.2])
                ax_leyenda.imshow(leyenda_img)
                ax_leyenda.axis("off")
            
            # Guardar mapa con alta calidad
            plt.savefig(salida, dpi=200, bbox_inches='tight', pad_inches=0.1)
            plt.close()
            
            logger.info(f"Mapa generado: {salida}")
            return salida
            
        except Exception as e:
            logger.error(f"Error generando mapa: {e}")
            raise

    def calcular_extent(self, parcela: gpd.GeoDataFrame) -> Tuple[float, float, float, float]:
        """
        Calcula extent con factor de encuadre
        
        Args:
            parcela: GeoDataFrame de la parcela
            
        Returns:
            Tupla (minx, maxx, miny, maxy) en EPSG:3857
        """
        minx, miny, maxx, maxy = parcela.total_bounds
        ancho = maxx - minx
        alto = maxy - miny
        
        # Aplicar factor de encuadre
        minx -= (self.encuadre_factor - 1) * ancho / 2
        maxx += (self.encuadre_factor - 1) * ancho / 2
        miny -= (self.encuadre_factor - 1) * alto / 2
        maxy += (self.encuadre_factor - 1) * alto / 2
        
        return (minx, maxx, miny, maxy)

    def procesar_parcela(self, geojson_path: str, referencia: Optional[str] = None) -> ResultadosUrbanismo:
        """
        Procesa una parcela completa: análisis urbanístico + mapa
        
        Args:
            geojson_path: Ruta al archivo GeoJSON de la parcela
            referencia: Referencia catastral (opcional, se extrae del nombre)
            
        Returns:
            Objeto ResultadosUrbanismo con todos los resultados
        """
        # Extraer referencia del nombre del archivo si no se proporciona
        if not referencia:
            referencia = Path(geojson_path).stem
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # Crear directorio de salida
        carpeta_salida = self.output_dir / f"{referencia}_{timestamp}"
        carpeta_salida.mkdir(exist_ok=True)
        
        logger.info(f"Procesando parcela: {referencia}")
        
        try:
            # 1. Cargar parcela
            parcela = self.cargar_parcela(geojson_path)
            
            # 2. Calcular extent para mapas
            extent = self.calcular_extent(parcela)
            
            # 3. Descargar capa de planeamiento (con caché)
            gdf_planeamiento = self.descargar_capa_wfs(self.wfs_carm_url, self.wfs_layer)
            
            # 4. Calcular porcentajes
            areas_m2, porcentajes = self.calcular_porcentajes(parcela, gdf_planeamiento)
            
            # 5. Generar archivos de salida
            salida_mapa = carpeta_salida / f"{referencia}_mapa.png"
            salida_txt = carpeta_salida / f"{referencia}_porcentajes.txt"
            salida_csv = carpeta_salida / f"{referencia}_porcentajes.csv"
            
            # 6. Guardar resultados textuales
            self._guardar_resultados_textuales(salida_txt, salida_csv, referencia, 
                                              timestamp, areas_m2, porcentajes)
            
            # 7. Generar mapa visual
            ortofoto_path = self.descargar_ortofoto(extent)
            urbanismo_path = self.descargar_urbanismo(extent)
            leyenda_path = self.descargar_leyenda()
            
            try:
                self.generar_mapa(parcela, ortofoto_path, urbanismo_path, 
                                leyenda_path, extent, str(salida_mapa))
            finally:
                # Limpiar archivos temporales
                self._limpiar_temporales([ortofoto_path, urbanismo_path, leyenda_path])
            
            # 8. Crear objeto de resultados
            resultados = ResultadosUrbanismo(
                referencia=referencia,
                area_total_m2=sum(areas_m2.values()),
                porcentajes=porcentajes,
                areas_m2=areas_m2,
                mapa_path=str(salida_mapa),
                txt_path=str(salida_txt),
                csv_path=str(salida_csv),
                timestamp=timestamp
            )
            
            logger.info(f"Análisis completado para {referencia}. Resultados en: {carpeta_salida}")
            return resultados
            
        except Exception as e:
            logger.error(f"Error procesando parcela {referencia}: {e}")
            raise

    def _guardar_resultados_textuales(self, txt_path: Path, csv_path: Path, 
                                   referencia: str, timestamp: str,
                                   areas_m2: Dict[str, float], porcentajes: Dict[str, float]):
        """Guarda resultados en formatos TXT y CSV"""
        
        # Guardar TXT
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(f"Resultados para {referencia} ({timestamp}):\n")
            f.write(f"Área total afectada: {sum(areas_m2.values()):.2f} m²\n")
            f.write("-" * 50 + "\n")
            for tipo, pct in porcentajes.items():
                area = areas_m2.get(tipo, 0)
                f.write(f"{tipo}: {area:.2f} m² ({pct:.2f}%)\n")
        
        # Guardar CSV
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("Clase,Area_m2,Porcentaje\n")
            for tipo in areas_m2.keys():
                f.write(f'"{tipo}",{areas_m2[tipo]:.2f},{porcentajes[tipo]:.2f}\n')

    def _limpiar_temporales(self, temp_files: List[Optional[str]]):
        """Limpia archivos temporales"""
        for temp_file in temp_files:
            if temp_file and Path(temp_file).exists():
                try:
                    os.unlink(temp_file)
                except Exception as e:
                    logger.warning(f"No se pudo eliminar temporal {temp_file}: {e}")

    def procesar_lote(self, geojson_dir: str) -> List[ResultadosUrbanismo]:
        """
        Procesa todos los GeoJSON de un directorio
        
        Args:
            geojson_dir: Directorio con archivos GeoJSON
            
        Returns:
            Lista de resultados para todas las parcelas
        """
        geojson_dir = Path(geojson_dir)
        
        if not geojson_dir.exists():
            raise FileNotFoundError(f"No existe el directorio: {geojson_dir}")
        
        geojson_files = list(geojson_dir.glob("*.geojson"))
        
        if not geojson_files:
            logger.warning(f"No se encontraron archivos GeoJSON en: {geojson_dir}")
            return []
        
        logger.info(f"Procesando {len(geojson_files)} parcelas...")
        
        resultados = []
        for geojson_path in geojson_files:
            try:
                resultado = self.procesar_parcela(str(geojson_path))
                resultados.append(resultado)
            except Exception as e:
                logger.error(f"Error procesando {geojson_path.name}: {e}")
                continue
        
        logger.info(f"Completado. {len(resultados)} parcelas procesadas exitosamente")
        return resultados

    def limpiar_cache(self):
        """Limpia caché de descargas"""
        self._wfs_cache.clear()
        self._wms_cache.clear()
        logger.info("Caché limpiado")


# Función de compatibilidad con el código original
def procesar_parcelas_legacy(geojson_dir: str, resultados_dir: str, encuadre_factor: float = 4.0):
    """
    Función legacy para compatibilidad con el código original
    
    Args:
        geojson_dir: Directorio con GeoJSONs
        resultados_dir: Directorio de resultados (legacy, no se usa)
        encuadre_factor: Factor de encuadre
    """
    analizador = AnalisisUrbano(encuadre_factor=encuadre_factor)
    return analizador.procesar_lote(geojson_dir)


# Ejecución principal (mejorada)
if __name__ == "__main__":
    import sys
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Directorios configurables
    script_dir = Path(__file__).parent
    geojson_dir = script_dir / "GEOJSONs"
    resultados_dir = script_dir / "RESULTADOS-MAPAS"
    
    # Crear directorios si no existen
    geojson_dir.mkdir(exist_ok=True)
    resultados_dir.mkdir(exist_ok=True)
    
    # Verificar archivos de entrada
    geojson_files = list(geojson_dir.glob("*.geojson"))
    
    if not geojson_files:
        print(f"❌ No se encontraron archivos GeoJSON en: {geojson_dir}")
        print("💡 Coloca tus archivos GeoJSON en la carpeta 'GEOJSONs'")
        sys.exit(1)
    
    print(f"📁 Encontrados {len(geojson_files)} archivos GeoJSON")
    print(f"📂 Directorio de salida: {resultados_dir}")
    
    try:
        # Crear analizador y procesar
        analizador = AnalisisUrbano(output_dir=str(resultados_dir))
        
        # Procesar todas las parcelas
        resultados = analizador.procesar_lote(str(geojson_dir))
        
        if resultados:
            print(f"\n✅ Proceso completado exitosamente")
            print(f"📊 {len(resultados)} parcelas procesadas")
            print(f"📁 Resultados guardados en: {resultados_dir}")
            
            # Resumen de resultados
            print("\n📋 Resumen de resultados:")
            for resultado in resultados:
                print(f"  • {resultado.referencia}: {resultado.area_total_m2:.2f} m² afectados")
        else:
            print("⚠️  No se procesó ninguna parcela exitosamente")
            
    except KeyboardInterrupt:
        print("\n⏹️  Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error durante el procesamiento: {e}")
        logger.exception("Error detallado:")
        sys.exit(1)
