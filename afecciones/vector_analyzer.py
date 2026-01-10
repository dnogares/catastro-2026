import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from datetime import datetime
from PIL import Image
import requests
from io import BytesIO
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import contextily as cx 

# Librerías necesarias para la gestión de títulos y GPKG
import sqlite3
import xml.etree.ElementTree as ET 
import csv 

# ------------------------------------------------------------
# Funcion añadir escala dinámica
# ------------------------------------------------------------

def añadir_escala(ax, dist_m=100): #<--- poner una # delante de def, y QUITAREMOS LA ESCALA
    """Añade una barra de escala que no da error y mantiene el mapa centrado"""

    return # <-- Al añadir esto aquí, la función se detiene y no dibuja nada
    
    from mpl_toolkits.axes_grid1.anchored_artists import AnchoredSizeBar
    
    # Creamos la barra de escala
    bar = AnchoredSizeBar(ax.transData,
                          dist_m, 
                          f'{dist_m} m', 
                          loc='lower left',   # La dejamos a la izquierda
                          pad=0.1,            # Espaciado interno mínimo
                          borderpad=2.0,      # Separación del borde del cuadro
                          color='black',      # Color de la barra y texto
                          frameon=False,      # Sin recuadro para evitar desplazamientos
                          size_vertical=1)    # Grosor de la línea de la escala
    
    # LA CLAVE DEL CENTRADO:
    # Esta línea le dice a Matplotlib: "Dibuja la escala, pero no la tengas 
    # en cuenta para calcular los márgenes del papel". 
    # Así el mapa se queda centrado como si la escala no existiera.
    bar.set_in_layout(False) 
    
    # Añadimos la escala al eje del mapa
    ax.add_artist(bar)

# ------------------------------------------------------------
# Funciones auxiliares de listado de capas
# ------------------------------------------------------------
def listar_capas_locales():
    capas = []
    # (Mantenido el listado simple por si usas SHP/GML/GEOJSON para afecciones)
    for carpeta, extensiones in [
        ("capas/shp", [".shp"]),
        ("capas/gml", [".gml"]),
        ("capas/geojson", [".geojson", ".json"]),
        ("capas/gpkg", [".gpkg"])
    ]:
        if os.path.exists(carpeta):
            for archivo in os.listdir(carpeta):
                if any(archivo.lower().endswith(ext) for ext in extensiones):
                    ruta = os.path.join(carpeta, archivo)
                    capas.append((archivo, ruta)) 
    return capas

def listar_capas_wfs(csv_path):
    capas = []
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            nombre = row.get("nombre", "sin_nombre")
            ruta_online = row.get("ruta_online", None)
            if ruta_online and str(ruta_online).startswith("http"):
                capas.append((nombre, ruta_online))
    return capas

def listar_capas_wms(csv_path):
    capas = []
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            # Usamos 'gpkg' como referencia principal al GPKG local
            capas.append({
                "nombre": row["nombre"],
                "url": row["ruta_wms"],
                "layers": row["layers"],
                "leyenda_url": row.get("leyenda_url", None),
                "modo_leyenda": row.get("modo_leyenda", "auto").strip().lower(),
                "gpkg": row.get("gpkg", None) 
            })
    return capas

# ------------------------------------------------------------
# Funciones auxiliares de Leyenda y Estilado (CORREGIDAS)
# ------------------------------------------------------------

def get_legend_styling(capa_nombre):
    """Obtiene el estilo (colores, campo de clasificación y etiquetas) para la capa local."""
    leyenda_csv_path = os.path.join("capas", "wms", f"leyenda_{capa_nombre.lower()}.csv")
    styling = {'unique': True, 'color': "blue", 'field': None, 'labels': {}} 
    
    if os.path.exists(leyenda_csv_path):
        try:
            df = pd.read_csv(leyenda_csv_path, encoding="utf-8")
            
            # Buscar el campo de clasificación: CAMPO_GPKG
            if 'CAMPO_GPKG' in df.columns:
                styling['field'] = df['CAMPO_GPKG'].iloc[0]
                
                # Mapeo de colores y etiquetas
                clasificacion_columns = [col for col in df.columns if col.lower() in ['clasificacion', 'clase', 'clave']]
                
                if clasificacion_columns and 'color' in df.columns:
                    campo_clasificacion = clasificacion_columns[0]
                    styling['colors'] = dict(zip(df[campo_clasificacion].astype(str), df['color']))
                    styling['unique'] = False
                    
                    if 'etiqueta' in df.columns:
                        styling['labels'] = dict(zip(df[campo_clasificacion].astype(str), df['etiqueta']))
                    
                return styling

            if not df.empty and 'color' in df.columns:
                styling['color'] = df['color'].iloc[0]
                styling['unique'] = True
                return styling
                
        except Exception as e:
            print(f"Error procesando CSV de leyenda para {capa_nombre}. Usando color azul por defecto. Error: {e}")
    
    return styling

def cargar_leyenda_csv(csv_path):
    df = pd.read_csv(csv_path, encoding="utf-8")
    leyenda = []
    for _, row in df.iterrows():
        leyenda.append({
            "capa": row["capa"],
            "tipo": row["tipo"],
            "color": row["color"],
            "etiqueta": row["etiqueta"]
        })
    return leyenda

def pintar_leyenda_desde_csv(ax, leyenda):
    # (Sin cambios, usa la leyenda cargada por cargar_leyenda_csv)
    handles = []
    for item in leyenda:
        tipo = item["tipo"].strip().lower()
        if tipo == "línea":
            patch = Line2D([], [], color=item["color"], linewidth=6, alpha=0.8, label=item["etiqueta"])
        elif tipo == "punto":
            patch = Line2D([], [], marker='o', color=item["color"], linestyle='None',
                           markersize=8, alpha=0.8, label=item["etiqueta"])
        elif tipo == "polígono":
            patch = Patch(facecolor=item["color"], edgecolor='black', alpha=0.6, label=item["etiqueta"])
        else:
            continue
        handles.append(patch)

    ax.legend(handles=handles, loc='lower right', fontsize=8, ncol=2,
              handlelength=1.5, columnspacing=0.8, borderpad=0.5, labelspacing=0.4)

def aplicar_leyenda(ax, capa):
    modo = capa.get("modo_leyenda", "auto")
    # (Simplificamos la leyenda para solo usar CSV si existe, ignorando WMS por ahora)
    leyenda_csv_path = os.path.join("capas", "wms", f"leyenda_{capa['nombre'].lower()}.csv")
    
    if os.path.exists(leyenda_csv_path):
        try:
            leyenda = cargar_leyenda_csv(leyenda_csv_path)
            pintar_leyenda_desde_csv(ax, leyenda)
            return True
        except Exception as e:
             print(f"Advertencia: Error al pintar leyenda CSV para {capa['nombre']}: {e}")
             return False
    return False

# ------------------------------------------------------------
# Funciones auxiliares de Títulos (CORREGIDAS)
# ------------------------------------------------------------
def cargar_config_titulos(csv_path="capas/wms/titulos.csv"):
    config = {}
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            config[row["capa"].lower()] = {
                "texto_previo": row.get("texto_previo", ""),
                "texto_posterior": row.get("texto_posterior", ""),
                "font": row.get("font", "Arial"),
                "color": row.get("color", "black"),
                "size": int(row.get("size", 14))
            }
    return config

def nombre_bonito_gpkg(ruta):
    # (Sin cambios)
    try:
        con = sqlite3.connect(ruta)
        cur = con.cursor()
        cur.execute("SELECT identifier, description FROM gpkg_contents LIMIT 1")
        row = cur.fetchone()
        if row:
            if row[0]: return row[0]
            if row[1]: return row[1]
    except Exception:
        pass
    return os.path.basename(ruta)

def aplicar_titulo(ax, capa, archivo_parcela, config_titulos, porcentaje_total=None, porcentaje_detalle=None):
    # (ACTUALIZADA para mostrar detalle de afección)
    conf = config_titulos.get("default", {
        "texto_previo": "MAPA: intersección de la parcela consultada con ",
        "texto_posterior": "",
        "font": "Arial",
        "color": "black",
        "size": 14
    })

    if capa["nombre"].lower() in config_titulos:
        conf = config_titulos[capa["nombre"].lower()]


    if "gpkg" in capa:
        nombre_gpkg_base = os.path.basename(capa["gpkg"])
        ruta_gpkg = os.path.join("capas", "gpkg", nombre_gpkg_base)
        nombre_bonito = nombre_bonito_gpkg(ruta_gpkg)
    else:
        nombre_bonito = capa.get("nombre", "Capa desconocida")

    texto_titulo = f"{conf['texto_previo']}{nombre_bonito}{conf['texto_posterior']}"
    
    texto_secundario = []
    
    if porcentaje_total is not None:
        texto_secundario.append(f"Afección Total: {porcentaje_total:.2f}%")
        
    if porcentaje_detalle:
        # Solo incluir detalles con porcentaje > 0.01%
        detalle_str = ", ".join([f"{etiqueta}: {perc:.2f}%" for etiqueta, perc in porcentaje_detalle.items() if perc > 0.01])
        if detalle_str:
            texto_secundario.append(f"Detalle: {detalle_str}")
    
    
    # Dibujar título principal
    fig = ax.figure
    fig.text(0.01, 0.97, texto_titulo,
             ha="left", va="top",
             fontname=conf["font"], color=conf["color"], fontsize=conf["size"])
             
    # Dibujar detalle de porcentajes
    if texto_secundario:
        texto_secundario_str = " | ".join(texto_secundario)
        fig.text(0.01, 0.94, texto_secundario_str,
                 ha="left", va="top",
                 fontname=conf["font"], color=conf["color"], fontsize=conf["size"]-2)

# ------------------------------------------------------------
# Procesar parcelas (Función principal) (CON CÁLCULO DETALLADO)
# ------------------------------------------------------------
def procesar_parcelas(capas_locales, capas_wfs, capas_wms, crs_objetivo, config_titulos):
    
    for archivo_parcela in os.listdir("datos_origen"):
        if archivo_parcela.lower().endswith((".shp", ".gml", ".geojson", ".json", ".kml")):
            ruta_parcela = os.path.join("datos_origen", archivo_parcela)
            try:
                parcela = gpd.read_file(ruta_parcela).to_crs(epsg=4326)
                print(f"Procesando parcela: {archivo_parcela}")


                # --- BLOQUE MODIFICADO PARA NOMBRE CON FECHA Y HORA ---
                nombre_base = os.path.splitext(archivo_parcela)[0]
                ahora = datetime.now().strftime("%Y%m%d_%H%M")
                
                # Ejemplo de resultado: "NombreParcela_20240522_1430"
                nombre_subcarpeta = f"{nombre_base}_{ahora}"
                carpeta_resultados = os.path.join("resultados", nombre_subcarpeta)


                os.makedirs(carpeta_resultados, exist_ok=True)
                resultados = []
                parcela_wgs84 = parcela
                parcela_proj = parcela_wgs84.to_crs(crs_objetivo) 
                geom_parcela_proj = parcela_proj.union_all()
                area_total_parcela_proj = parcela_proj.area.sum()
                minx, miny, maxx, maxy = parcela_wgs84.total_bounds
                bbox_4326 = (miny, minx, maxy, maxx)

                # --- CÁLCULOS VECTORIALES (TOTAL y DETALLADO) ---
                for capa_wms_config in capas_wms:
                    nombre_base_capa = capa_wms_config["gpkg"]
                    
                    if not nombre_base_capa: continue

                    ruta_gpkg = os.path.join("capas", "gpkg", nombre_base_capa)
                    
                    if not os.path.exists(ruta_gpkg):
                         print(f"Advertencia: GPKG no encontrado en {ruta_gpkg}. Saltando cálculo vectorial.")
                         continue
                    
                    try:
                        capa = gpd.read_file(ruta_gpkg).to_crs(crs_objetivo)
                        capa_filtrada = capa[capa.intersects(geom_parcela_proj)]
                        
                        interseccion = gpd.overlay(parcela_proj, capa_filtrada, how="intersection", keep_geom_type=False)
                        
                        # Cálculo total
                        porcentaje_total = 0
                        if not interseccion.empty:
                            porcentaje_total = (interseccion.area.sum() / area_total_parcela_proj) * 100
                        
                        resultados.append({"parcela": archivo_parcela, "capa": nombre_base_capa, "porcentaje": porcentaje_total})
                        
                        # Cálculo detallado por clasificación
                        porcentaje_detalle = {}
                        styling = get_legend_styling(capa_wms_config['nombre'])
                        field = styling['field'] # Campo de clasificación del GPKG (ej: id_tipo_af)
                        labels = styling.get('labels', {}) # Etiquetas de la leyenda
                        
                        if field and field in interseccion.columns:
                            interseccion['area_afectada'] = interseccion.area
                            
                            detalle_area = interseccion.groupby(field)['area_afectada'].sum()
                            
                            for clasificacion, area_sum in detalle_area.items():
                                porcentaje_clasif = (area_sum / area_total_parcela_proj) * 100
                                # Usar la etiqueta del CSV si existe, si no, el código de clasificación
                                etiqueta = labels.get(str(clasificacion), str(clasificacion)) 
                                porcentaje_detalle[etiqueta] = porcentaje_clasif
                                
                        capa_wms_config['porcentaje_detalle'] = porcentaje_detalle
                        
                    except Exception as e:
                        print(f"Error cruzando {archivo_parcela} con {nombre_base_capa}: {e}")
                        
                # --- Mapas bonitos con servicios del IGN ---
                crs_mapa_visual = "EPSG:3857"
                
                # OPCIÓN A: IGN PNOA (Ortofoto / Satélite)
                url_pnoa = "https://www.ign.es/wmts/pnoa-ma?layer=OI.OrthoimageCoverage&style=default&tilematrixset=GoogleMapsCompatible&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image/jpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"

                # OPCIÓN B: IGN Base (Mapa topográfico y callejero)
                url_ign_base = "https://www.ign.es/wmts/ign-base?layer=IGNBaseTodo&style=default&tilematrixset=GoogleMapsCompatible&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image/jpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"

                for capa in capas_wms:
                    try:
                        parcela_para_dibujar = parcela_wgs84.to_crs(crs_mapa_visual)
                        fig, ax = plt.subplots(1, 1, figsize=(10, 8)) 

                        # 1. Establecemos primero los límites para que contextily sepa qué descargar
                        minx_p, miny_p, maxx_p, maxy_p = parcela_para_dibujar.total_bounds # <-- Rectángulo invisible que encierra tu parcela perfectamente.
                        margin_proj = (maxx_p - minx_p) * 0.50 # Esta línea calcula un 50% de espacio extra alrededor de la parcela
                        ax.set_xlim(minx_p - margin_proj, maxx_p + margin_proj)
                        ax.set_ylim(miny_p - margin_proj, maxy_p + margin_proj)

                        # 2. Añadimos el mapa base del IGN (PNOA)
                        cx.add_basemap(ax, 
                                       crs=crs_mapa_visual, 
                                     # source=url_pnoa, 
                                     # attribution="IGN - PNOA",
                                       source=url_ign_base, # <-- ahora la Opción B
                                       attribution="IGN - Base",  # <--ahora la Opción B
                                       interpolation='bilinear')
                        
                        # 3. Dibujar la Capa Temática LOCAL (GPKG)
                        ruta_gpkg_base = os.path.basename(capa["gpkg"])
                        ruta_gpkg = os.path.join("capas", "gpkg", ruta_gpkg_base)
                        
                        if os.path.exists(ruta_gpkg):
                            capa_tematica = gpd.read_file(ruta_gpkg)
                            capa_tematica_proj = capa_tematica.to_crs(crs_mapa_visual)
                            
                            styling = get_legend_styling(capa['nombre'])
                            
                            plot_kwargs = {
                                "ax": ax,
                                "edgecolor": "black", 
                                "alpha": 0.4, # <--(0.3 o 0.4) para que el mapa de fondo se vea mejor a través del color. 0.6 Apenas se vería - TRANSPARENCIA AFECCIÓN.
                                "zorder": 5 
                            }
                            
                            if styling['unique']:
                                plot_kwargs['color'] = styling['color']
                                capa_tematica_proj.plot(**plot_kwargs)
                            else:
                                field = styling['field']
                                colors_map = styling['colors']
                                
                                if field in capa_tematica_proj.columns:
                                    # Mapeo y relleno de colores (importante para el contraste)
                                    color_list = capa_tematica_proj[field].astype(str).map(colors_map).fillna('gray').tolist()
                                    
                                    capa_tematica_proj.plot(color=color_list,
                                                            legend=False,
                                                            **plot_kwargs)
                                else:
                                    plot_kwargs['color'] = "gray"
                                    capa_tematica_proj.plot(**plot_kwargs)


                        # --- 1) Dibujar la parcela (borde rojo grueso) ---
                        parcela_para_dibujar.plot(ax=ax, color="none", edgecolor="red", linewidth=2, zorder=10) 
                        
                        # --- 2) Quitar los ejes (coordenadas) para que quede limpio ---
                        ax.axis("off")

                        # --- 3) AÑADIR LA ESCALA (Aquí es donde va tu nuevo código) ---
                        # Si la parcela mide menos de 1km, ponemos escala de 100m. Si es más grande, de 500m.
                        distancia_escala = 100 if (maxx_p - minx_p) < 1000 else 500
                        añadir_escala(ax, dist_m=distancia_escala)


                        # --- 4) Gestión del Título y Leyenda ---
                        # (Aquí el script pone el título con el % de afección que calculamos)

                      # 1. ESCALA OCULTA (Comentamos la llamada para que no se ejecute)
                        # distancia_escala = 100 if (maxx_p - minx_p) < 1000 else 500
                        # añadir_escala(ax, dist_m=distancia_escala)

                        # LEYENDA (Sigue activa, pero ahora flotará)
                        aplicar_leyenda(ax, capa)

                        # Porcentaje y Título (ACTUALIZADO PARA MOSTRAR DETALLE)
                        porcentaje_total = None
                        porcentaje_detalle = capa.get('porcentaje_detalle', {})

                        for r in resultados:
                            if r["capa"].strip() == capa["gpkg"]:
                                porcentaje_total = r["porcentaje"]
                                break
                        
                        aplicar_titulo(ax, capa, archivo_parcela, config_titulos, porcentaje_total, porcentaje_detalle)

                        # Ajuste de márgenes internos
                        plt.tight_layout(pad=0)

                        # Forzar al mapa a ocupar todo el espacio (0 a 1)
                        # Esto hace que la leyenda flote encima y no empuje el mapa
                        # Definimos un margen manual: [izquierda, abajo, ancho, alto]
                        # Dejamos 0.1 (10%) de margen para que el título y bordes respiren
                        ax.set_position([0.05, 0.05, 0.9, 0.85])                        


                        # Guardar mapa
                        salida_jpg = os.path.join(carpeta_resultados, f"mapa_{capa['nombre']}.jpg")

                        # bbox_inches='tight' elimina los bordes blancos excesivos alrededor del mapa


                        # Al usar bbox_inches='tight' con un pad pequeño, creamos el marco blanco
                        # Usamos pad_inches=0 para que no añada aire extra a los lados
                        plt.savefig(salida_jpg, dpi=150, bbox_inches='tight', pad_inches=0.3)

                        plt.close() # Cierra la figura para liberar memoria
                        print(f"Mapa guardado en {salida_jpg}")

                    except Exception as e:
                        print(f"Error generando mapa con {capa['nombre']} (Capas Locales): {e}")

                # 4) Guardar resultados numéricos (sin cambios)
                df = pd.DataFrame(resultados)
                df.to_csv(os.path.join(carpeta_resultados, "resultados.csv"), index=False)
                df.to_excel(os.path.join(carpeta_resultados, "resultados.xlsx"), index=False)

            except Exception as e:
                print(f"No se pudo procesar {archivo_parcela}: {e}")

# ------------------------------------------------------------
# Ejecución principal (Sin cambios relevantes)
# ------------------------------------------------------------
if __name__ == "__main__":
    # Crear directorios
    os.makedirs("datos_origen", exist_ok=True)
    os.makedirs("capas/shp", exist_ok=True)
    os.makedirs("capas/gml", exist_ok=True)
    os.makedirs("capas/geojson", exist_ok=True)
    os.makedirs("capas/gpkg", exist_ok=True)
    os.makedirs("capas/wfs", exist_ok=True)
    os.makedirs("capas/wms", exist_ok=True)
    os.makedirs("resultados", exist_ok=True)

    # Cargar configuraciones
    capas_locales = listar_capas_locales()
    capas_wfs = listar_capas_wfs("capas_wfs.csv")
    capas_wms = listar_capas_wms("capas/wms/capas_wms.csv")
    config_titulos = cargar_config_titulos()

    # CRSes para el cálculo y visualización
    crs_objetivo = "EPSG:25830"
    
    # Iniciar el procesamiento
    procesar_parcelas(capas_locales, capas_wfs, capas_wms, crs_objetivo, config_titulos)