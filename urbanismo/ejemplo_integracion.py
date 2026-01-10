#!/usr/bin/env python3
"""
urbanismo/ejemplo_integracion.py
Ejemplo de integración del módulo urbanismo con el sistema principal
"""

import logging
from pathlib import Path

# Importar servicios del sistema
from urbanismo import UrbanismoService, crear_servicio_urbanismo
from afecciones.pdf_generator import AfeccionesPDF
from catastro.catastro_downloader import CatastroDownloader

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def ejemplo_procesamiento_completo(referencia: str, output_dir: str = "ejemplo_integracion"):
    """
    Ejemplo completo de procesamiento integrado:
    1. Descarga catastral
    2. Análisis de afecciones
    3. Análisis urbanístico
    4. Generación de PDF
    
    Args:
        referencia: Referencia catastral
        output_dir: Directorio de salida
    """
    
    print(f"🚀 Iniciando procesamiento completo para: {referencia}")
    
    # 1. Crear directorio de salida
    output_path = Path(output_dir) / referencia
    output_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # 2. Descargar datos catastrales
        print("📥 Descargando datos catastrales...")
        downloader = CatastroDownloader(output_dir=str(output_path / "catastro"))
        exito, zip_path = downloader.descargar_todo_completo(referencia)
        
        if not exito:
            print(f"❌ Error descargando datos catastrales para {referencia}")
            return
        
        print(f"✅ Datos catastrales descargados: {zip_path}")
        
        # 3. Buscar archivo de parcela (GML)
        parcela_gml = None
        for gml_file in (output_path / "catastro" / referencia).glob("*.gml"):
            parcela_gml = str(gml_file)
            break
        
        if not parcela_gml:
            print(f"❌ No se encontró archivo GML para {referencia}")
            return
        
        print(f"📄 Parcela encontrada: {parcela_gml}")
        
        # 4. Análisis urbanístico
        print("🏗️  Realizando análisis urbanístico...")
        urbanismo_service = crear_servicio_urbanismo(str(output_path))
        resultados_urbanismo = urbanismo_service.analizar_parcela(parcela_gml, referencia)
        
        if resultados_urbanismo.get("error"):
            print(f"⚠️  Error en análisis urbanístico: {resultados_urbanismo['error']}")
        else:
            print(f"✅ Análisis urbanístico completado:")
            print(f"   Área total: {resultados_urbanismo['area_parcela_m2']:.2f} m²")
            print(f"   Clasificaciones: {len(resultados_urbanismo['detalle'])}")
        
        # 5. Generar PDF integrado
        print("📄 Generando PDF integrado...")
        pdf_service = AfeccionesPDF(output_dir=str(output_path / "pdfs"))
        
        # Recopilar mapas
        mapas_urbanismo = urbanismo_service.obtener_mapas(referencia)
        
        # Combinar resultados (simulados para este ejemplo)
        resultados_completos = {
            "total": resultados_urbanismo.get("total", 0),
            "detalle": resultados_urbanismo.get("detalle", {}),
            "area_parcela_m2": resultados_urbanismo.get("area_parcela_m2", 0),
            "area_afectada_m2": resultados_urbanismo.get("area_afectada_m2", 0),
            "urbanismo": True
        }
        
        pdf_path = pdf_service.generar(
            referencia=referencia,
            resultados=resultados_completos,
            mapas=mapas_urbanismo,
            incluir_tabla=True
        )
        
        if pdf_path:
            print(f"✅ PDF generado: {pdf_path}")
        else:
            print("❌ Error generando PDF")
        
        # 6. Resumen final
        print("\n📋 Resumen del procesamiento:")
        print(f"   📁 Directorio: {output_path}")
        print(f"   🏗️  Urbanismo: {len(resultados_urbanismo.get('detalle', {}))} clasificaciones")
        print(f"   📊 PDF: {'✅' if pdf_path else '❌'}")
        print(f"   🗺️  Mapas: {len(mapas_urbanismo)} generados")
        
        return {
            "referencia": referencia,
            "output_dir": str(output_path),
            "urbanismo": resultados_urbanismo,
            "pdf": str(pdf_path) if pdf_path else None,
            "mapas": mapas_urbanismo
        }
        
    except Exception as e:
        logger.error(f"Error en procesamiento completo: {e}")
        print(f"❌ Error: {e}")
        return None

def ejemplo_lote_parcelas(referencias: list, output_dir: str = "ejemplo_lote"):
    """
    Ejemplo de procesamiento por lote de múltiples parcelas
    
    Args:
        referencias: Lista de referencias catastrales
        output_dir: Directorio de salida
    """
    
    print(f"🚀 Procesando lote de {len(referencias)} parcelas...")
    
    # Crear servicio urbanístico compartido
    urbanismo_service = crear_servicio_urbanismo(output_dir)
    
    resultados_lote = []
    
    for i, referencia in enumerate(referencias, 1):
        print(f"\n📍 [{i}/{len(referencias)}] Procesando: {referencia}")
        
        try:
            # Descargar datos catastrales
            downloader = CatastroDownloader(output_dir=f"{output_dir}/catastro/{referencia}")
            exito, zip_path = downloader.descargar_todo_completo(referencia)
            
            if not exito:
                print(f"⚠️  Omitiendo {referencia} (error en descarga)")
                continue
            
            # Buscar GML
            parcela_gml = None
            for gml_file in Path(f"{output_dir}/catastro/{referencia}").glob("*.gml"):
                parcela_gml = str(gml_file)
                break
            
            if not parcela_gml:
                print(f"⚠️  Omitiendo {referencia} (sin GML)")
                continue
            
            # Análisis urbanístico
            resultados = urbanismo_service.analizar_parcela(parcela_gml, referencia)
            resultados_lote.append(resultados)
            
            print(f"✅ Completado: {referencia}")
            
        except Exception as e:
            print(f"❌ Error procesando {referencia}: {e}")
            continue
    
    # Estadísticas del lote
    print(f"\n📊 Estadísticas del lote:")
    print(f"   ✅ Exitosos: {len(resultados_lote)}/{len(referencias)}")
    print(f"   🏗️  Urbanismo: {sum(1 for r in resultados_lote if r.get('urbanismo'))}")
    
    # Estadísticas globales
    stats = urbanismo_service.get_estadisticas_globales()
    print(f"   📈 Análisis totales: {stats['total_analisis']}")
    print(f"   🗺️  Tipos de suelo: {len(stats['tipos_suelo'])}")
    
    return resultados_lote

if __name__ == "__main__":
    import sys
    
    # Ejemplo de uso individual
    if len(sys.argv) == 2:
        referencia = sys.argv[1]
        resultado = ejemplo_procesamiento_completo(referencia)
        
        if resultado:
            print(f"\n🎉 Proceso completado exitosamente para {referencia}")
        else:
            print(f"\n💥 Falló el procesamiento para {referencia}")
    
    # Ejemplo de lote
    elif len(sys.argv) > 2:
        referencias = sys.argv[1:]
        resultados = ejemplo_lote_parcelas(referencias)
    
    # Ejemplo con datos de prueba
    else:
        print("📝 Ejemplo con datos de prueba...")
        
        # Referencias de ejemplo (descomentar para usar)
        referencias_ejemplo = [
            # "1234567VK1234S0001LL",
            # "9876543AB9876N0001YZ"
        ]
        
        if referencias_ejemplo:
            ejemplo_lote_parcelas(referencias_ejemplo)
        else:
            print("💡 Agrega referencias catastrales reales para probar:")
            print("   python ejemplo_integracion.py 1234567VK1234S0001LL")
            print("   python ejemplo_integracion.py ref1 ref2 ref3")
