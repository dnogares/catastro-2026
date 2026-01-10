import os
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from io import BytesIO
from owslib.wms import WebMapService
from datetime import datetime

# -----------------------------
# Cargar parcela desde GeoJSON
# -----------------------------
def cargar_parcela(path_geojson):
    gdf = gpd.read_file(path_geojson)
    return gdf.to_crs(epsg=3857)  # Web Mercator

# -----------------------------
# Descargar capa WFS como GeoDataFrame
# -----------------------------
def descargar_capa_wfs(base_url, typename):
    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typename": typename,
        "outputFormat": "json",
        "srsName": "EPSG:4326"
    }
    r = requests.get(base_url, params=params, timeout=60)
    if r.status_code == 200:
        gdf = gpd.read_file(BytesIO(r.content))
        gdf.columns = [c.lower() for c in gdf.columns]
        return gdf.to_crs(epsg=25830)  # reproyectar para cálculo de área
    else:
        raise Exception(f"Error {r.status_code} al descargar WFS\nURL: {r.url}")

# -----------------------------
# Calcular porcentajes reales con subtipos de protección
# -----------------------------
def calcular_porcentajes(gdf_parcela, gdf_planeamiento):
    gdf_parcela = gdf_parcela.to_crs(epsg=25830)
    interseccion = gpd.overlay(gdf_planeamiento, gdf_parcela, how="intersection")

    if interseccion.empty:
        return {}, {}

    interseccion["area_m2"] = interseccion.geometry.area

    # Crear campo combinado para diferenciar subtipos
    interseccion["tipo_suelo"] = interseccion["clasificacion"]
    mask_no_urb = interseccion["clasificacion"].str.contains("No Urbanizable", case=False, na=False)
    interseccion.loc[mask_no_urb, "tipo_suelo"] = (
        interseccion["clasificacion"] + " - " + interseccion["ambito"].fillna("")
    )

    resumen = interseccion.groupby("tipo_suelo")["area_m2"].sum()
    total_area = resumen.sum()
    porcentajes = (resumen / total_area) * 100
    return resumen.to_dict(), porcentajes.to_dict()

# -----------------------------
# Descargar ortofoto WMS (IGN PNOA)
# -----------------------------
def descargar_ortofoto(extent, wms_url="https://www.ign.es/wms-inspire/pnoa-ma"):
    wms = WebMapService(wms_url, version="1.3.0")
    minx, maxx, miny, maxy = extent
    img = wms.getmap(
        layers=["OI.OrthoimageCoverage"],
        srs="EPSG:3857",
        bbox=(minx, miny, maxx, maxy),
        size=(1000, 1000),
        format="image/jpeg",
        transparent=True
    )
    ortofoto_path = "ortofoto.jpg"
    with open(ortofoto_path, "wb") as f:
        f.write(img.read())
    return ortofoto_path

# -----------------------------
# Descargar urbanismo WMS (colores oficiales)
# -----------------------------
def descargar_urbanismo(extent, wms_url="https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wms?"):
    wms = WebMapService(wms_url, version="1.3.0")
    minx, maxx, miny, maxy = extent
    img = wms.getmap(
        layers=["SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun"],
        srs="EPSG:3857",
        bbox=(minx, miny, maxx, maxy),
        size=(1000, 1000),
        format="image/png",
        transparent=True
    )
    urbanismo_path = "urbanismo.png"
    with open(urbanismo_path, "wb") as f:
        f.write(img.read())
    return urbanismo_path

# -----------------------------
# Descargar leyenda oficial WMS
# -----------------------------
def descargar_leyenda(wms_url="https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wms?"):
    url = f"{wms_url}service=WMS&version=1.1.0&request=GetLegendGraphic&layer=SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun&format=image/png"
    r = requests.get(url)
    if r.status_code == 200:
        leyenda_path = "leyenda.png"
        with open(leyenda_path, "wb") as f:
            f.write(r.content)
        return leyenda_path
    else:
        print("No se pudo descargar la leyenda oficial.")
        return None

# -----------------------------
# Generar mapa final
# -----------------------------
def generar_mapa(parcela, ortofoto_path, urbanismo_path, leyenda_path, extent, salida="mapa_final.png"):
    fig, ax = plt.subplots(figsize=(10,10))

    ortofoto = plt.imread(ortofoto_path)
    ax.imshow(ortofoto, extent=extent, origin="upper")

    urbanismo_img = plt.imread(urbanismo_path)
    ax.imshow(urbanismo_img, extent=extent, origin="upper", alpha=0.5)

    parcela.boundary.plot(ax=ax, color="red", linewidth=2)

    plt.title("Parcela sobre ortofoto + urbanismo (colores oficiales)")
    plt.axis("off")

    if leyenda_path:
        leyenda_img = plt.imread(leyenda_path)
        ax_leyenda = fig.add_axes([0.75, 0.05, 0.2, 0.2])
        ax_leyenda.imshow(leyenda_img)
        ax_leyenda.axis("off")

    plt.savefig(salida, dpi=200)
    plt.close()
    print(f"Mapa guardado en: {salida}")

# -----------------------------
# BLOQUE DE ENCUADRE/PERSPECTIVA, menor se acerca, mayor, se aleja el satélite.
# -----------------------------
ENCUADRE_FACTOR = 4
# -----------------------------


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    geojson_dir = os.path.join(script_dir, "GEOJSONs")
    resultados_dir = os.path.join(script_dir, "RESULTADOS-MAPAS")

    os.makedirs(geojson_dir, exist_ok=True)
    os.makedirs(resultados_dir, exist_ok=True)

    geojson_files = [f for f in os.listdir(geojson_dir) if f.lower().endswith(".geojson")]

    if not geojson_files:
        print("No se encontraron archivos GeoJSON en la carpeta 'GEOJSONs'.")
        exit()

    base_url_wfs = "https://mapas-gis-inter.carm.es/geoserver/SIT_USU_PLA_URB_CARM/wfs?"
    typename = "SIT_USU_PLA_URB_CARM:clases_plu_ze_37mun"

    for gj_name in geojson_files:
        gj_path = os.path.join(geojson_dir, gj_name)
        nombre_base = os.path.splitext(gj_name)[0]
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        carpeta_salida = os.path.join(resultados_dir, f"{nombre_base}_{timestamp}")
        os.makedirs(carpeta_salida, exist_ok=True)

        salida_mapa = os.path.join(carpeta_salida, f"{nombre_base}_mapa.png")
        salida_txt = os.path.join(carpeta_salida, f"{nombre_base}_porcentajes.txt")
        salida_csv = os.path.join(carpeta_salida, f"{nombre_base}_porcentajes.csv")

        print(f"\nProcesando: {gj_name}")
        try:
            parcela = cargar_parcela(gj_path)

            # ENCUADRE
            minx, miny, maxx, maxy = parcela.total_bounds
            ancho = maxx - minx
            alto = maxy - miny
            minx -= (ENCUADRE_FACTOR-1) * ancho/2
            maxx += (ENCUADRE_FACTOR-1) * ancho/2
            miny -= (ENCUADRE_FACTOR-1) * alto/2
            maxy += (ENCUADRE_FACTOR-1) * alto/2
            extent = (minx, maxx, miny, maxy)

            # Cálculo real de porcentajes con subtipos
            gdf_planeamiento = descargar_capa_wfs(base_url_wfs, typename)
            resumen, porcentajes = calcular_porcentajes(parcela, gdf_planeamiento)

            # Guardar TXT con porcentajes reales
            with open(salida_txt, "w", encoding="utf-8") as f:
                f.write(f"Resultados para {gj_name} ({timestamp}):\n")
                for tipo, pct in porcentajes.items():
                    f.write(f"{tipo}: {pct:.2f}%\n")

            # Guardar CSV con áreas y porcentajes reales
            with open(salida_csv, "w", encoding="utf-8") as f:
                f.write("Clase,Area_m2,Porcentaje\n")
                for tipo in resumen.keys():
                    f.write(f"{tipo},{resumen[tipo]:.2f},{porcentajes[tipo]:.2f}\n")

            # Generar mapa con ortofoto + urbanismo + leyenda
            ortofoto_path = descargar_ortofoto(extent)
            urbanismo_path = descargar_urbanismo(extent)
            leyenda_path = descargar_leyenda()
            generar_mapa(parcela, ortofoto_path, urbanismo_path, leyenda_path, extent, salida=salida_mapa)

            print(f"Resultados guardados en: {carpeta_salida}")

        except Exception as e:
            print(f"Error al generar resultados: {e}")
