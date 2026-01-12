#!/usr/bin/env python3
"""
urbanismo/analizador_urbanistico.py
Analizador urbanístico avanzado con cálculo de parámetros y afecciones
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging
import geopandas as gpd
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

class AnalizadorUrbanistico:
    """
    Analizador urbanístico avanzado que calcula parámetros, afecciones y genera certificados
    """
    
    def __init__(self, normativa_dir: str = None, capas_service=None):
        """
        Inicializa el analizador urbanístico
        
        Args:
            normativa_dir: Directorio con normativa urbanística
            capas_service: Servicio de capas para análisis espacial
        """
        self.normativa_dir = Path(normativa_dir) if normativa_dir else None
        self.capas_service = capas_service
        logger.info("[AnalizadorUrbanistico] Inicializado")
    
    def analizar_referencia(self, referencia: str, geometria_path: str = None) -> Dict:
        """
        Análisis completo de una referencia catastral
        
        Args:
            referencia: Referencia catastral
            geometria_path: Ruta al archivo de geometría (GML/GeoJSON)
            
        Returns:
            Diccionario con análisis completo
        """
        logger.info(f"[AnalizadorUrbanistico] Analizando: {referencia}")
        
        resultado = {
            "referencia": referencia,
            "timestamp": datetime.now().isoformat(),
            "superficie": None,
            "zonas_afectadas": [],
            "parametros_urbanisticos": {},
            "afecciones": [],
            "recomendaciones": []
        }
        
        # 1. Calcular superficie
        if geometria_path and Path(geometria_path).exists():
            try:
                gdf = gpd.read_file(geometria_path)
                if gdf.crs:
                    gdf_meters = gdf.to_crs(epsg=25830)
                    area_total = gdf_meters.geometry.area.sum()
                    resultado["superficie"] = {
                        "valor": round(area_total, 2),
                        "unidad": "m²",
                        "valor_ha": round(area_total / 10000, 4)
                    }
            except Exception as e:
                resultado["error"] = str(e)
        
        # 2. Analizar zonas afectadas
        resultado["zonas_afectadas"] = self._analizar_zonas(geometria_path)
        
        # 3. Calcular parámetros urbanísticos
        resultado["parametros_urbanisticos"] = self._calcular_parametros(resultado)
        
        # 4. Analizar afecciones específicas
        resultado["afecciones"] = self._analizar_afecciones(geometria_path)
        
        # 5. Generar recomendaciones
        resultado["recomendaciones"] = self._generar_recomendaciones(resultado)
        
        return resultado
    
    def _analizar_zonas(self, geometria_path: str = None) -> List[Dict]:
        """
        Analiza las zonas urbanísticas que afectan a la parcela
        
        Args:
            geometria_path: Ruta al archivo de geometría
            
        Returns:
            Lista de zonas afectadas
        """
        zonas = []
        
        if not geometria_path:
            return [{"nota": "Sin geometria para analisis"}]
        
        try:
            entrada_gdf = gpd.read_file(geometria_path).to_crs("EPSG:4326")
            
            if self.capas_service:
                for capa in self.capas_service.listar_capas():
                    try:
                        capa_gdf = self.capas_service.cargar_capa(capa["nombre"])
                        if capa_gdf is not None:
                            intersectado = gpd.sjoin(
                                entrada_gdf, 
                                capa_gdf.to_crs("EPSG:4326"), 
                                how='inner', 
                                predicate='intersects'
                            )
                            if len(intersectado) > 0:
                                zonas.append({
                                    "capa": capa["nombre"],
                                    "elementos": len(intersectado),
                                    "tipo": capa.get("tipo", "desconocido")
                                })
                    except Exception:
                        pass
        
        except Exception as e:
            logger.error(f"[AnalizadorUrbanistico] Error en análisis de zonas: {e}")
        
        return zonas if zonas else [{"nota": "No se encontraron capas de zonificacion"}]
    
    def _calcular_parametros(self, analisis: Dict) -> Dict:
        """
        Calcula parámetros urbanísticos basados en el análisis
        
        Args:
            analisis: Diccionario con análisis previo
            
        Returns:
            Diccionario con parámetros urbanísticos
        """
        params = {
            "superficie_parcela": analisis.get("superficie", {})
        }
        
        superficie = analisis.get("superficie", {}).get("valor_ha", 0)
        
        if superficie > 0:
            # Parámetros básicos (valores genéricos que podrían venir de normativa)
            params["coeficiente_ocupacion"] = {
                "valor": 0.5,
                "nota": "50% (valor generico)",
                "superficie_ocupada_m2": round(superficie * 10000 * 0.5, 2)
            }
            
            params["edificabilidad"] = {
                "valor": round(superficie * 1.5, 2),
                "nota": "1.5 m²/m² (valor generico)",
                "tipo": "bruta"
            }
            
            params["altura_maxima"] = {
                "valor": 12,
                "nota": "12 metros (valor generico)",
                "plantas": 4
            }
            
            params["separacion_linderos"] = {
                "valor": 3,
                "nota": "3 metros (valor generico)",
                "unidad": "metros"
            }
        
        return params
    
    def _analizar_afecciones(self, geometria_path: str = None) -> List[Dict]:
        """
        Analiza afecciones específicas sobre la parcela
        
        Args:
            geometria_path: Ruta al archivo de geometría
            
        Returns:
            Lista de afecciones detectadas
        """
        afecciones = []
        
        if not geometria_path:
            return [{"nota": "Sin geometria para analisis"}]
        
        try:
            entrada_gdf = gpd.read_file(geometria_path).to_crs("EPSG:4326")
            
            if self.capas_service:
                for capa in self.capas_service.listar_capas():
                    nombre = capa["nombre"].lower()
                    
                    # Buscar capas de afecciones específicas
                    if any(p in nombre for p in ["afeccion", "riesgo", "proteccion", "dominio", "servidumbre"]):
                        try:
                            capa_gdf = self.capas_service.cargar_capa(capa["nombre"])
                            if capa_gdf is not None:
                                intersectado = gpd.sjoin(
                                    entrada_gdf, 
                                    capa_gdf.to_crs("EPSG:4326"), 
                                    how='inner', 
                                    predicate='intersects'
                                )
                                if len(intersectado) > 0:
                                    afecciones.append({
                                        "tipo": self._clasificar_afeccion(capa["nombre"]),
                                        "capa": capa["nombre"],
                                        "elementos": len(intersectado),
                                        "descripcion": capa.get("descripcion", "Afección detectada")
                                    })
                        except Exception:
                            pass
        
        except Exception as e:
            logger.error(f"[AnalizadorUrbanistico] Error en análisis de afecciones: {e}")
        
        return afecciones if afecciones else [{"nota": "No se detectaron afecciones"}]
    
    def _clasificar_afeccion(self, nombre_capa: str) -> str:
        """
        Clasifica el tipo de afección según el nombre de la capa
        
        Args:
            nombre_capa: Nombre de la capa
            
        Returns:
            Tipo de afección
        """
        nombre_lower = nombre_capa.lower()
        
        if "dominio" in nombre_lower:
            return "dominio_publico"
        elif "servidumbre" in nombre_lower:
            return "servidumbre"
        elif "riesgo" in nombre_lower:
            return "riesgo"
        elif "proteccion" in nombre_lower:
            return "proteccion"
        elif "afeccion" in nombre_lower:
            return "afeccion"
        else:
            return "otra"
    
    def _generar_recomendaciones(self, analisis: Dict) -> List[str]:
        """
        Genera recomendaciones basadas en el análisis
        
        Args:
            analisis: Diccionario con análisis completo
            
        Returns:
            Lista de recomendaciones
        """
        recomendaciones = [
            "Consultar el Plan General de Ordenacion Urbana vigente.",
            "Verificar correspondencia con el registro de la propiedad.",
            "Confirmar parametros con el ayuntamiento.",
            "Este analisis tiene caracter informativo."
        ]
        
        # Recomendaciones específicas según afecciones
        afecciones = analisis.get("afecciones", [])
        if any(af.get("tipo") == "dominio_publico" for af in afecciones):
            recomendaciones.insert(0, "⚠️ Existen posibles afectaciones de dominio público.")
        
        if any(af.get("tipo") == "riesgo" for af in afecciones):
            recomendaciones.insert(0, "⚠️ La parcela se encuentra en zona de riesgo.")
        
        superficie = analisis.get("superficie", {}).get("valor_ha", 0)
        if superficie > 1.0:
            recomendaciones.insert(0, "📐 Parcela de gran superficie. Verificar regulación específica.")
        
        return recomendaciones
    
    def generar_certificado(self, analisis: Dict, output_path: str):
        """
        Genera un certificado de análisis urbanístico
        
        Args:
            analisis: Diccionario con análisis completo
            output_path: Ruta donde guardar el certificado
        """
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("CERTIFICADO DE ANALISIS URBANISTICO\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Referencia Catastral: {analisis.get('referencia', 'N/A')}\n")
            f.write(f"Fecha: {analisis.get('timestamp', 'N/A')}\n\n")
            
            # Superficie
            superf = analisis.get('superficie', {})
            f.write(f"Superficie: {superf.get('valor', 'N/A')} {superf.get('unidad', 'm²')}")
            if superf.get('valor_ha'):
                f.write(f" ({superf.get('valor_ha')} ha)")
            f.write("\n\n")
            
            # Parámetros urbanísticos
            params = analisis.get('parametros_urbanisticos', {})
            if params:
                f.write("PARAMETROS URBANISTICOS:\n")
                for param, valor in params.items():
                    if param != "superficie_parcela" and isinstance(valor, dict):
                        f.write(f"  - {param.replace('_', ' ').title()}: {valor.get('valor', 'N/A')} {valor.get('nota', '')}\n")
                f.write("\n")
            
            # Zonas afectadas
            f.write("ZONAS AFECTADAS:\n")
            for zona in analisis.get('zonas_afectadas', []):
                if "capa" in zona:
                    f.write(f"  - {zona.get('capa', 'N/A')}: {zona.get('elementos', 0)} elementos\n")
                else:
                    f.write(f"  - {zona.get('nota', 'N/A')}\n")
            f.write("\n")
            
            # Afecciones
            f.write("AFECCIONES DETECTADAS:\n")
            for afeccion in analisis.get('afecciones', []):
                if "capa" in afeccion:
                    f.write(f"  - {afeccion.get('tipo', 'N/A')}: {afeccion.get('capa', 'N/A')}\n")
                else:
                    f.write(f"  - {afeccion.get('nota', 'N/A')}\n")
            f.write("\n")
            
            # Recomendaciones
            f.write("RECOMENDACIONES:\n")
            for rec in analisis.get('recomendaciones', []):
                f.write(f"  - {rec}\n")
            f.write("\n")
            
            f.write("=" * 80 + "\n")
            f.write("GENERADO POR Suite Tasación dnogares\n")
            f.write("=" * 80 + "\n")
        
        logger.info(f"Certificado generado: {output_path}")
