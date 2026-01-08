import geopandas as gpd
import pandas as pd
import sqlite3
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class VectorAnalyzer:
    """
    Analiza afecciones vectoriales sobre una parcela catastral.
    Calcula intersecciones y porcentajes de afectación.
    """

    def __init__(self, capas_dir: str, crs_calculo: str = "EPSG:25830"):
        """
        Inicializa el analizador
        
        Args:
            capas_dir: Directorio donde están las capas vectoriales (GPKG)
            crs_calculo: Sistema de coordenadas para cálculos (por defecto UTM 30N)
        """
        self.capas_dir = Path(capas_dir)
        self.crs_calculo = crs_calculo
        
        # Asegurar que existe el directorio de capas y config
        self.capas_dir.mkdir(parents=True, exist_ok=True)
        (self.capas_dir / "gpkg").mkdir(exist_ok=True)
        (self.capas_dir / "config").mkdir(exist_ok=True)

    def _get_styling(self, gpkg_name: str) -> Dict[str, Any]:
        """Obtiene el estilo (campo de clasificación y etiquetas) para la capa."""
        capa_nombre = Path(gpkg_name).stem
        # Buscar en capas/config/leyenda_[nombre].csv
        csv_path = self.capas_dir / "config" / f"leyenda_{capa_nombre.lower()}.csv"
        
        styling = {'field': None, 'labels': {}, 'colors': {}}
        
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, encoding="utf-8")
                
                # Buscar el campo de clasificación: CAMPO_GPKG
                if 'CAMPO_GPKG' in df.columns:
                    styling['field'] = df['CAMPO_GPKG'].iloc[0]
                    
                    # Mapeo de etiquetas
                    clasif_cols = [col for col in df.columns if col.lower() in ['clasificacion', 'clase', 'clave', 'id']]
                    if clasif_cols and 'etiqueta' in df.columns:
                        campo_clasif = clasif_cols[0]
                        styling['labels'] = dict(zip(df[campo_clasif].astype(str), df['etiqueta']))
                    
                    # Mapeo de colores si existen
                    if clasif_cols and 'color' in df.columns:
                        campo_clasif = clasif_cols[0]
                        styling['colors'] = dict(zip(df[campo_clasif].astype(str), df['color']))
                        
                return styling
            except Exception as e:
                logger.warning(f"Error procesando CSV de leyenda para {capa_nombre}: {e}")
        
        return styling

    def _nombre_bonito_gpkg(self, ruta: Path) -> str:
        """Extrae el identificador del GPKG usando metadata de SQLite."""
        try:
            con = sqlite3.connect(ruta)
            cur = con.cursor()
            cur.execute("SELECT identifier FROM gpkg_contents LIMIT 1")
            row = cur.fetchone()
            con.close()
            if row and row[0]: return row[0]
        except Exception:
            pass
        return ruta.stem

    def analizar(
        self,
        parcela_path: Path,
        gpkg_name: str,
        campo_clasificacion: Optional[str] = None,
        etiquetas: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Analiza afecciones de una capa GPKG sobre una parcela
        
        Args:
            parcela_path: Ruta al archivo GML/GeoJSON de la parcela
            gpkg_name: Nombre del archivo GPKG con las afecciones
            campo_clasificacion: Campo para clasificar afecciones (ej: 'tipo')
            etiquetas: Diccionario para renombrar valores del campo
            
        Returns:
            dict: Resultados del análisis con porcentajes
        """
        try:
            # Verificar que existe la parcela
            if not parcela_path.exists():
                logger.error(f"No existe la parcela: {parcela_path}")
                return {
                    "total": 0.0, 
                    "detalle": {}, 
                    "area_parcela_m2": 0.0,
                    "area_afectada_m2": 0.0,
                    "error": "Parcela no encontrada"
                }

            # Verificar que existe el GPKG
            # Intentar primero en subcarpeta gpkg y luego en raíz
            posibles_rutas = [
                self.capas_dir / "gpkg" / gpkg_name,
                self.capas_dir / gpkg_name
            ]
            
            gpkg_path = None
            for ruta in posibles_rutas:
                if ruta.exists():
                    gpkg_path = ruta
                    break
                    
            if not gpkg_path:
                logger.warning(f"No existe el GPKG: {gpkg_name} en {self.capas_dir}")
                return {
                    "total": 0.0, 
                    "detalle": {},
                    "area_parcela_m2": 0.0,
                    "area_afectada_m2": 0.0,
                    "mensaje": f"No se encontró la capa {gpkg_name}"
                }

            # Leer parcela
            logger.info(f"Leyendo parcela: {parcela_path}")
            parcela = gpd.read_file(parcela_path)
            
            # Asegurar CRS
            if parcela.crs is None:
                logger.warning("Parcela sin CRS, asumiendo EPSG:25830")
                parcela = parcela.set_crs("EPSG:25830")
            
            parcela = parcela.to_crs(self.crs_calculo)

            # Leer capa de afecciones
            logger.info(f"Leyendo capa de afecciones: {gpkg_path}")
            capa = gpd.read_file(gpkg_path)
            
            if capa.crs is None:
                logger.warning("Capa sin CRS, asumiendo EPSG:25830")
                capa = capa.set_crs("EPSG:25830")
                
            capa = capa.to_crs(self.crs_calculo)

            # Calcular geometría y área de la parcela
            geom_parcela = parcela.union_all()
            area_total = parcela.geometry.area.sum()

            if area_total == 0:
                logger.error("Área de parcela es 0")
                return {
                    "total": 0.0, 
                    "detalle": {}, 
                    "area_parcela_m2": 0.0,
                    "area_afectada_m2": 0.0,
                    "error": "Área de parcela inválida"
                }

            # Filtrar elementos que intersectan
            capa = capa[capa.intersects(geom_parcela)]

            if capa.empty:
                logger.info("No hay afecciones que intersecten con la parcela")
                return {
                    "total": 0.0, 
                    "detalle": {},
                    "area_parcela_m2": round(area_total, 2),
                    "area_afectada_m2": 0.0,
                    "mensaje": "Sin afecciones detectadas"
                }

            # Calcular intersección
            logger.info("Calculando intersecciones...")
            inter = gpd.overlay(parcela, capa, how="intersection", keep_geom_type=False)
            inter["area"] = inter.geometry.area

            # Lógica de clasificación detallada (Plano Perfecto)
            porcentaje_detalle = {}
            styling = self._get_styling(gpkg_name)
            
            # Priorizar campo del CSV sobre el argumento
            field = styling['field'] or campo_clasificacion
            labels = styling['labels'] or (etiquetas or {})
            
            if field and field in inter.columns:
                # Agrupar por la clasificación y sumar áreas
                detalle_area = inter.groupby(field)['area'].sum()
                for clasif, area_sum in detalle_area.items():
                    porcentaje_clasif = (area_sum / area_total) * 100
                    # Solo incluir si es significativo (> 0.01%)
                    if porcentaje_clasif > 0.01:
                        etiqueta = labels.get(str(clasif), str(clasif))
                        porcentaje_detalle[etiqueta] = round(porcentaje_clasif, 2)

            resultado = {
                "total": round(porcentaje_total, 2),
                "detalle": porcentaje_detalle if porcentaje_detalle else detalle,
                "area_parcela_m2": round(area_total, 2),
                "area_afectada_m2": round(area_afectada, 2),
                "elementos_afectantes": len(inter),
                "capa_nombre": self._nombre_bonito_gpkg(gpkg_path)
            }

            logger.info(f"Análisis completado: {porcentaje_total:.2f}% afectado")
            return resultado

        except Exception as e:
            logger.error(f"Error en análisis de afecciones: {e}")
            return {
                "total": 0.0,
                "detalle": {},
                "area_parcela_m2": 0.0,
                "area_afectada_m2": 0.0,
                "error": str(e)
            }

    def analizar_multiple(
        self,
        parcela_path: Path,
        capas_gpkg: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Analiza múltiples capas de afecciones sobre una parcela
        
        Args:
            parcela_path: Ruta al GML/GeoJSON de la parcela
            capas_gpkg: Dict con nombre_capa: archivo_gpkg
            
        Returns:
            dict: Resultados agregados de todas las capas
        """
        resultados = {}
        
        for nombre_capa, gpkg_name in capas_gpkg.items():
            logger.info(f"Analizando capa: {nombre_capa}")
            resultado = self.analizar(parcela_path, gpkg_name)
            resultados[nombre_capa] = resultado
        
        # Calcular resumen
        total_capas = len(capas_gpkg)
        capas_con_afeccion = sum(1 for r in resultados.values() if r.get("total", 0) > 0)
        
        return {
            "capas_analizadas": total_capas,
            "capas_con_afeccion": capas_con_afeccion,
            "resultados": resultados
        }


# Testing
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Uso: python vector_analyzer.py <parcela.gml> <afecciones.gpkg>")
        sys.exit(1)
    
    parcela = Path(sys.argv[1])
    gpkg = sys.argv[2]
    
    analyzer = VectorAnalyzer(capas_dir="capas")
    resultado = analyzer.analizar(parcela, gpkg, campo_clasificacion="tipo")
    
    print(f"\n📊 Resultados:")
    print(f"  Total afectado: {resultado['total']}%")
    print(f"  Detalle: {resultado['detalle']}")