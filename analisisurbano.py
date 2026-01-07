import logging
import json
from pathlib import Path
from typing import List, Dict, Optional

# Integración con configuración del proyecto
from config.paths import OUTPUTS_DIR
from catastro.catastro_downloader import CatastroDownloader

# Configuración de logs
logger = logging.getLogger(__name__)

class AnalizadorUrbanistico:
    def __init__(self, output_base_dir: Optional[Path] = None):
        self.output_base_dir = output_base_dir or OUTPUTS_DIR
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        # Usamos el downloader del proyecto para evitar duplicidad de lógica
        self.downloader = CatastroDownloader(output_dir=str(self.output_base_dir))

    def procesar_lote_referencias(self, path_archivo: str) -> List[Dict]:
        """
        Lee un archivo y procesa cada referencia catastral incluida.
        """
        resultados = []
        try:
            with open(path_archivo, 'r') as f:
                contenido = f.read().replace(',', '\n').replace(' ', '\n')
                referencias = [line.strip() for line in contenido.split('\n') if len(line.strip()) >= 14]
            
            for ref in referencias:
                logger.info(f"Procesando referencia de lote: {ref}")
                res = self.obtener_datos_catastrales(ref)
                resultados.append(res)
            
            return resultados
        except Exception as e:
            logger.error(f"Error en procesar_lote_referencias: {e}")
            return [{"error": str(e)}]

    def obtener_datos_catastrales(self, referencia: str) -> Dict:
        """
        Módulo 1: Obtiene geometría y datos de la parcela usando el downloader base
        y añade la lógica de KML y capas específicas.
        """
        try:
            ref = self.downloader.limpiar_referencia(referencia)
            
            # Ejecutar descarga completa
            exito, zip_path = self.downloader.descargar_todo_completo(ref)
            
            if not exito:
                raise Exception(f"Fallo en la descarga catastral para {ref}")

            ref_dir = self.output_base_dir / ref
            gml_dir = ref_dir / "gml"
            kml_path = gml_dir / f"{ref}_parcela.kml"

            # Fallback si no hay KML (por si GeoPandas falló en el downloader)
            if not kml_path.exists():
                logger.info(f"Generando GeoJSON fallback para {ref}")
                coords = self.downloader.obtener_coordenadas(ref)
                kml_path = Path(self._generar_geojson_basico(ref, kml_path, coords))

            # Recopilar capas WMS generadas
            wms_layers = {}
            images_dir = ref_dir / "images"
            if images_dir.exists():
                for img_file in images_dir.glob(f"{ref}_*.png"):
                    name = img_file.name.split('_')[1].lower()
                    wms_layers[name] = str(img_file)

            return {
                "referencia": ref,
                "status": "success",
                "folder": str(ref_dir),
                "kml": str(kml_path),
                "zip": str(zip_path) if zip_path else None,
                "wms_layers": wms_layers,
                "resumen": {
                    "total_capas": len(wms_layers),
                    "capas_afectan": 0,
                    "superficie_total_afectada": "0.00 m²",
                    "archivos_generados": len(list(ref_dir.rglob('*')))
                }
            }
        except Exception as e:
            logger.error(f"Error al obtener datos de {referencia}: {e}")
            return {"referencia": referencia, "status": "error", "message": str(e)}

    def _generar_geojson_basico(self, referencia: str, output_path: Path, coords: Optional[dict] = None) -> str:
        """Crea un archivo GeoJSON básico cuando fallan otros métodos"""
        lat = coords.get("lat") if coords else 40.416775
        lon = coords.get("lon") if coords else -3.70379
        delta = 0.0005 

        geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"referencia": referencia},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[lon-delta, lat-delta], [lon+delta, lat-delta], 
                                   [lon+delta, lat+delta], [lon-delta, lat+delta], [lon-delta, lat-delta]]]
                }
            }]
        }
        
        geojson_path = output_path.with_suffix('.geojson')
        with open(geojson_path, 'w', encoding='utf-8') as f:
            json.dump(geojson, f)
        return str(geojson_path)

    def exportar_informe_csv(self, resultados: List[Dict], filename: str = "resumen_catastro.csv"):
        import pandas as pd
        df = pd.DataFrame(resultados)
        output_path = self.output_base_dir / filename
        df.to_csv(output_path, index=False)
        return str(output_path)

def integrar_analisis_urbanistico(referencia: str):
    """Función puente para main.py"""
    analizador = AnalizadorUrbanistico()
    return analizador.obtener_datos_catastrales(referencia)