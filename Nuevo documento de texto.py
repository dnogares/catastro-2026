from qgis.core import QgsVectorLayer, QgsProject

# Ejemplo: Descargar de Murcia
wfs_url = "https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wfs"
layer_name = "SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun"

# Construir URL WFS
uri = f"{wfs_url}?service=WFS&version=2.0.0&request=GetFeature&typename={layer_name}&srsname=EPSG:4326&outputFormat=application/json"

# Cargar capa
layer = QgsVectorLayer(uri, layer_name, "WFS")

if layer.isValid():
    # Guardar como GPKG
    QgsVectorFileWriter.writeAsVectorFormat(
        layer, 
        "capas_urbanisticas/murcia/clasificacion_suelo.gpkg",
        "UTF-8", 
        layer.crs(), 
        "GPKG"
    )
    print("✅ Capa descargada")
else:
    print("❌ Error cargando capa")