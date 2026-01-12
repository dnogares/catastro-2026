#!/usr/bin/env python3
"""
urbanismo/urbanismo_service.py
Servicio de urbanismo integrado con el sistema SuiteTasacion
Incluye análisis avanzado con AnalizadorUrbanistico
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import asdict

from .analisisurbano_mejorado import AnalisisUrbano, ResultadosUrbanismo
from .analizador_urbanistico import AnalizadorUrbanistico

logger = logging.getLogger(__name__)

class UrbanismoService:
    """
    Servicio de urbanismo para integración con el sistema principal
    Proporciona interfaz compatible con LoteManager y PDFGenerator
    Incluye análisis avanzado de parámetros urbanísticos y afecciones
    """
    
    def __init__(self, output_base_dir: str = "resultados"):
        """
        Inicializa el servicio de urbanismo
        
        Args:
            output_base_dir: Directorio base para resultados
        """
        self.output_base_dir = Path(output_base_dir)
        
        # Analizador básico (para compatibilidad) - ahora usa directorio base directamente
        self.analizador = AnalisisUrbano(
            output_dir=str(self.output_base_dir),  # Cambiado: ya no usa subcarpeta urbanismo
            encuadre_factor=4.0
        )
        
        # Analizador avanzado (nuevas funcionalidades)
        self.analizador_avanzado = AnalizadorUrbanistico(
            normativa_dir=str(self.output_base_dir / "normativa"),
            capas_service=self  # Pasar el mismo servicio para usar GPKG local
        )
        
        logger.info(f"UrbanismoService inicializado. Output: {self.output_base_dir}")
    
    # --- Métodos de CapasService para usar GPKG local ---
    def listar_capas(self) -> List[Dict]:
        """
        Lista las capas disponibles en el GPKG consolidado
        """
        try:
            from config.paths import CAPAS_DIR
            capas_consolidadas = CAPAS_DIR / "capas_consolidadas_20260112_173239.gpkg"
            
            if not capas_consolidadas.exists():
                logger.warning(f"No se encuentra el GPKG consolidado: {capas_consolidadas}")
                return []
            
            # Listar capas del GPKG
            import fiona
            capas_disponibles = []
            
            for layer_name in fiona.listlayers(capas_consolidadas):
                capas_disponibles.append({
                    "nombre": layer_name,
                    "tipo": "vectorial",
                    "ruta": str(capas_consolidadas)
                })
            
            logger.info(f"Capas encontradas en GPKG: {[c['nombre'] for c in capas_disponibles]}")
            return capas_disponibles
            
        except Exception as e:
            logger.error(f"Error listando capas del GPKG: {e}")
            return []
    
    def cargar_capa(self, nombre_capa: str):
        """
        Carga una capa específica del GPKG consolidado
        """
        try:
            from config.paths import CAPAS_DIR
            capas_consolidadas = CAPAS_DIR / "capas_consolidadas_20260112_173239.gpkg"
            
            if not capas_consolidadas.exists():
                logger.error(f"No se encuentra el GPKG consolidado: {capas_consolidadas}")
                return None
            
            import geopandas as gpd
            capa_gdf = gpd.read_file(capas_consolidadas, layer=nombre_capa)
            
            # Asegurar CRS WGS84
            if capa_gdf.crs and capa_gdf.crs != "EPSG:4326":
                capa_gdf = capa_gdf.to_crs("EPSG:4326")
            
            logger.info(f"Capa '{nombre_capa}' cargada: {len(capa_gdf)} geometrías")
            return capa_gdf
            
        except Exception as e:
            logger.error(f"Error cargando capa '{nombre_capa}': {e}")
            return None

    def analizar_parcela(self, parcela_path: str, referencia: str) -> Dict[str, any]:
        """
        Analiza una parcela y devuelve resultados completos
        
        Args:
            parcela_path: Ruta al archivo de la parcela (GML/GeoJSON)
            referencia: Referencia catastral
            
        Returns:
            Diccionario con resultados completos (básicos + avanzados)
        """
        try:
            # 1. Análisis básico (compatibilidad con sistema existente)
            geojson_path = self._asegurar_geojson(parcela_path)
            resultados_basicos = self.analizador.procesar_parcela(geojson_path, referencia)
            
            # 2. Análisis avanzado (nuevas funcionalidades)
            resultados_avanzados = self.analizador_avanzado.analizar_referencia(
                referencia=referencia,
                geometria_path=geojson_path
            )
            
            # 3. Combinar resultados
            resultado_final = self._combinar_resultados(resultados_basicos, resultados_avanzados)
            
            # 4. Generar certificado si hay análisis avanzado
            if resultados_avanzados and not resultados_avanzados.get("error"):
                self._generar_certificado_avanzado(resultados_avanzados, referencia)
            
            return resultado_final
            
        except Exception as e:
            logger.error(f"Error en análisis urbanístico para {referencia}: {e}")
            return self._resultados_vacios(referencia, str(e))

    def _combinar_resultados(self, basicos: ResultadosUrbanismo, avanzados: Dict) -> Dict[str, any]:
        """
        Combina resultados básicos y avanzados en un solo diccionario
        
        Args:
            basicos: Resultados del análisis básico
            avanzados: Resultados del análisis avanzado
            
        Returns:
            Diccionario combinado compatible con el sistema
        """
        # Base: resultados básicos (para compatibilidad)
        resultado = {
            "total": sum(basicos.porcentajes.values()),
            "detalle": basicos.porcentajes,
            "area_parcela_m2": basicos.area_total_m2,
            "area_afectada_m2": basicos.area_total_m2,
            "urbanismo": True,
            "mapa_urbano": basicos.mapa_path,
            "referencia": basicos.referencia,
            "timestamp": basicos.timestamp,
            "csv_path": basicos.csv_path,
            "txt_path": basicos.txt_path
        }
        
        # Agregar datos avanzados
        if avanzados and not avanzados.get("error"):
            resultado.update({
                "analisis_avanzado": True,
                "superficie": avanzados.get("superficie"),
                "zonas_afectadas": avanzados.get("zonas_afectadas", []),
                "parametros_urbanisticos": avanzados.get("parametros_urbanisticos", {}),
                "afecciones_detectadas": avanzados.get("afecciones", []),
                "recomendaciones": avanzados.get("recomendaciones", [])
            })
        
        return resultado

    def _generar_certificado_avanzado(self, analisis: Dict, referencia: str):
        """
        Genera certificado de análisis avanzado
        
        Args:
            analisis: Resultados del análisis avanzado
            referencia: Referencia catastral
        """
        try:
            # Directorio de la referencia (misma carpeta que todo lo demás)
            ref_dir = self.output_base_dir / referencia
            ref_dir.mkdir(parents=True, exist_ok=True)
            
            # Ruta del certificado en la misma carpeta
            cert_path = ref_dir / f"certificado_{referencia}.txt"
            
            # Generar certificado
            self.analizador_avanzado.generar_certificado(analisis, str(cert_path))
            
            logger.info(f"Certificado avanzado generado: {cert_path}")
            
        except Exception as e:
            logger.error(f"Error generando certificado avanzado: {e}")

    def _asegurar_geojson(self, parcela_path: str) -> str:
        """
        Convierte GML a GeoJSON si es necesario
        
        Args:
            parcela_path: Ruta al archivo original
            
        Returns:
            Ruta al archivo GeoJSON
        """
        parcela_path = Path(parcela_path)
        
        # Si ya es GeoJSON, retornar directamente
        if parcela_path.suffix.lower() == '.geojson':
            return str(parcela_path)
        
        # Si es GML, convertir a GeoJSON
        if parcela_path.suffix.lower() == '.gml':
            import geopandas as gpd
            import tempfile
            
            try:
                # Leer GML
                gdf = gpd.read_file(parcela_path)
                
                # Crear archivo temporal GeoJSON
                with tempfile.NamedTemporaryFile(suffix='.geojson', delete=False) as f:
                    temp_path = f.name
                
                # Guardar como GeoJSON
                gdf.to_file(temp_path, driver='GeoJSON')
                
                logger.debug(f"GML convertido a GeoJSON: {parcela_path} -> {temp_path}")
                return temp_path
                
            except Exception as e:
                logger.error(f"Error convirtiendo GML a GeoJSON: {e}")
                raise
        
        raise ValueError(f"Formato de archivo no soportado: {parcela_path.suffix}")

    def _convertir_resultados_sistema(self, resultados: ResultadosUrbanismo) -> Dict[str, any]:
        """
        Convierte ResultadosUrbanismo a formato compatible con el sistema
        
        Args:
            resultados: Objeto ResultadosUrbanismo del analizador
            
        Returns:
            Diccionario en formato compatible con LoteManager/PDFGenerator
        """
        return {
            "total": sum(resultados.porcentajes.values()),
            "detalle": resultados.porcentajes,
            "area_parcela_m2": resultados.area_total_m2,
            "area_afectada_m2": resultados.area_total_m2,  # En urbanismo, todo el área es "afectada"
            "urbanismo": True,  # Flag para identificar análisis urbanístico
            "mapa_urbano": resultados.mapa_path,
            "referencia": resultados.referencia,
            "timestamp": resultados.timestamp,
            "csv_path": resultados.csv_path,
            "txt_path": resultados.txt_path
        }

    def _resultados_vacios(self, referencia: str, error: str) -> Dict[str, any]:
        """
        Genera resultados vacíos en caso de error
        
        Args:
            referencia: Referencia catastral
            error: Mensaje de error
            
        Returns:
            Diccionario con resultados vacíos
        """
        return {
            "total": 0.0,
            "detalle": {},
            "area_parcela_m2": 0.0,
            "area_afectada_m2": 0.0,
            "urbanismo": True,
            "error": error,
            "referencia": referencia
        }

    def obtener_mapas(self, referencia: str) -> List[str]:
        """
        Obtiene lista de mapas generados para una referencia
        
        Args:
            referencia: Referencia catastral
            
        Returns:
            Lista de rutas a archivos de mapa
        """
        mapas = []
        
        # Buscar en directorio de urbanismo
        urbanismo_dir = self.output_base_dir / "urbanismo"
        
        for carpeta in urbanismo_dir.glob(f"{referencia}_*"):
            mapa_files = list(carpeta.glob("*_mapa.png"))
            mapas.extend([str(m) for m in mapa_files])
        
        return sorted(mapas)

    def limpiar_cache(self):
        """Limpia caché del analizador"""
        self.analizador.limpiar_cache()

    def get_estadisticas_globales(self) -> Dict[str, any]:
        """
        Obtiene estadísticas globales de todos los análisis realizados
        
        Returns:
            Diccionario con estadísticas agregadas
        """
        urbanismo_dir = self.output_base_dir / "urbanismo"
        
        if not urbanismo_dir.exists():
            return {"total_analisis": 0}
        
        # Contar análisis por tipo de suelo
        tipos_suelo = {}
        total_analisis = 0
        
        for carpeta in urbanismo_dir.iterdir():
            if not carpeta.is_dir():
                continue
            
            # Buscar archivos CSV de resultados
            csv_files = list(carpeta.glob("*_porcentajes.csv"))
            
            for csv_file in csv_files:
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_file)
                    
                    for _, row in df.iterrows():
                        clase = row.get('Clase', 'Desconocido')
                        area = row.get('Area_m2', 0)
                        
                        if clase not in tipos_suelo:
                            tipos_suelo[clase] = 0
                        tipos_suelo[clase] += area
                    
                    total_analisis += 1
                    
                except Exception as e:
                    logger.warning(f"Error leyendo CSV {csv_file}: {e}")
        
        return {
            "total_analisis": total_analisis,
            "tipos_suelo": tipos_suelo,
            "area_total_analizada": sum(tipos_suelo.values())
        }


# Función de compatibilidad para integración con LoteManager
def crear_servicio_urbanismo(output_dir: str = "resultados") -> UrbanismoService:
    """
    Crea instancia del servicio de urbanismo para integración
    
    Args:
        output_dir: Directorio base de resultados
        
    Returns:
        Instancia de UrbanismoService
    """
    return UrbanismoService(output_base_dir=output_dir)


# Ejemplo de integración con LoteManager
if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    
    # Ejemplo de uso
    if len(sys.argv) < 2:
        print("Uso: python urbanismo_service.py <archivo_parcela>")
        sys.exit(1)
    
    parcela_path = sys.argv[1]
    referencia = Path(parcela_path).stem
    
    # Crear servicio
    servicio = crear_servicio_urbanismo("test_urbanismo")
    
    # Analizar parcela
    resultados = servicio.analizar_parcela(parcela_path, referencia)
    
    print("Resultados del análisis urbanístico:")
    for key, value in resultados.items():
        print(f"  {key}: {value}")
    
    # Obtener mapas generados
    mapas = servicio.obtener_mapas(referencia)
    print(f"\nMapas generados: {mapas}")
