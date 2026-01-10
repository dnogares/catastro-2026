import os
import sqlite3
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx
from datetime import datetime
from PIL import Image
from io import BytesIO
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from mpl_toolkits.axes_grid1.anchored_artists import AnchoredSizeBar

class VectorAnalyzer:
    def __init__(self, crs_objetivo="EPSG:25830"):
        self.crs_objetivo = crs_objetivo
        self.config_titulos = self.cargar_config_titulos()

    # ------------------------------------------------------------
    # Configuración y Utilidades
    # ------------------------------------------------------------
    def cargar_config_titulos(self, csv_path="capas/wms/titulos.csv"):
        config = {}
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                for _, row in df.iterrows():
                    config[row["capa"].lower()] = {
                        "texto_previo": row.get("texto_previo", ""),
                        "texto_posterior": row.get("texto_posterior", ""),
                        "font": row.get("font", "Arial"),
                        "color": row.get("color", "black"),
                        "size": int(row.get("size", 14))
                    }
            except Exception as e:
                print(f"Error cargando titulos.csv: {e}")
        return config

    def añadir_escala(self, ax, dist_m=100):
        """Añade una barra de escala dinámica"""
        # Según tu código, esto está desactivado por el return inicial
        return 
        
        bar = AnchoredSizeBar(ax.transData, dist_m, f'{dist_m} m', 
                             loc='lower left', pad=0.1, borderpad=2.0, 
                             color='black', frameon=False, size_vertical=1)
        bar.set_in_layout(False) 
        ax.add_artist(bar)

    def nombre_bonito_gpkg(self, ruta):
        try:
            con = sqlite3.connect(ruta)
            cur = con.cursor()
            cur.execute("SELECT identifier, description FROM gpkg_contents LIMIT 1")
            row = cur.fetchone()
            con.close()
            if row:
                return row[0] if row[0] else row[1]
        except Exception:
            pass
        return os.path.basename(ruta)

    # ------------------------------------------------------------
    # Gestión de Leyendas y Estilos
    # ------------------------------------------------------------
    def get_legend_styling(self, capa_nombre):
        leyenda_csv_path = os.path.join("capas", "wms", f"leyenda_{capa_nombre.lower()}.csv")
        styling = {'unique': True, 'color': "blue", 'field': None, 'labels': {}, 'colors': {}} 
        
        if os.path.exists(leyenda_csv_path):
            try:
                df = pd.read_csv(leyenda_csv_path, encoding="utf-8")
                if 'CAMPO_GPKG' in df.columns:
                    styling['field'] = df['CAMPO_GPKG'].iloc[0]
                    clasif_cols = [col for col in df.columns if col.lower() in ['clasificacion', 'clase', 'clave']]
                    
                    if clasif_cols and 'color' in df.columns:
                        campo_clasif = clasif_cols[0]
                        styling['colors'] = dict(zip(df[campo_clasif].astype(str), df['color']))
                        styling['unique'] = False
                        if 'etiqueta' in df.columns:
                            styling['labels'] = dict(zip(df[campo_clasif].astype(str), df['etiqueta']))
                    return styling

                if not df.empty and 'color' in df.columns:
                    styling['color'] = df['color'].iloc[0]
                    styling['unique'] = True
            except Exception as e:
                print(f"Error en leyenda para {capa_nombre}: {e}")
        return styling

    def aplicar_leyenda(self, ax, capa):
        leyenda_csv_path = os.path.join("capas", "wms", f"leyenda_{capa['nombre'].lower()}.csv")
        if os.path.exists(leyenda_csv_path):
            try:
                df = pd.read_csv(leyenda_csv_path, encoding="utf-8")
                handles = []
                for _, item in df.iterrows():
                    tipo = str(item["tipo"]).strip().lower()
                    color = item["color"]
                    etiq = item["etiqueta"]
                    
                    if tipo == "línea":
                        patch = Line2D([], [], color=color, linewidth=6, alpha=0.8, label=etiq)
                    elif tipo == "punto":
                        patch = Line2D([], [], marker='o', color=color, linestyle='None', markersize=8, alpha=0.8, label=etiq)
                    elif tipo == "polígono":
                        patch = Patch(facecolor=color, edgecolor='black', alpha=0.6, label=etiq)
                    else: continue
                    handles.append(patch)
                
                if handles:
                    ax.legend(handles=handles, loc='lower right', fontsize=8, ncol=2)
                    return True
            except Exception as e:
                print(f"Error al pintar leyenda: {e}")
        return False

    # ------------------------------------------------------------
    # Títulos y Mapas
    # ------------------------------------------------------------
    def aplicar_titulo(self, ax, capa, porcentaje_total=None, porcentaje_detalle=None):
        conf = self.config_titulos.get(capa["nombre"].lower(), {
            "texto_previo": "MAPA: intersección de la parcela con ",
            "texto_posterior": "", "font": "Arial", "color": "black", "size": 14
        })

        if "gpkg" in capa and capa["gpkg"]:
            ruta_gpkg = os.path.join("capas", "gpkg", os.path.basename(capa["gpkg"]))
            nombre_bonito = self.nombre_bonito_gpkg(ruta_gpkg)
        else:
            nombre_bonito = capa.get("nombre", "Capa desconocida")

        texto_titulo = f"{conf['texto_previo']}{nombre_bonito}{conf['texto_posterior']}"
        
        fig = ax.figure
        fig.text(0.01, 0.97, texto_titulo, ha="left", va="top",
                 fontname=conf["font"], color=conf["color"], fontsize=conf["size"])
             
        texto_secundario = []
        if porcentaje_total is not None:
            texto_secundario.append(f"Afección Total: {porcentaje_total:.2f}%")
        if porcentaje_detalle:
            detalle_str = ", ".join([f"{k}: {v:.2f}%" for k, v in porcentaje_detalle.items() if v > 0.01])
            if detalle_str: texto_secundario.append(f"Detalle: {detalle_str}")
        
        if texto_secundario:
            fig.text(0.01, 0.94, " | ".join(texto_secundario), ha="left", va="top",
                     fontname=conf["font"], color=conf["color"], fontsize=conf["size"]-2)

    # ------------------------------------------------------------
    # Procesamiento Principal
    # ------------------------------------------------------------
    def procesar_parcelas(self, capas_wms):
        """Procesa los archivos en datos_origen contra las capas configuradas"""
        if not os.path.exists("datos_origen"): return

        for archivo_parcela in os.listdir("datos_origen"):
            if not archivo_parcela.lower().endswith((".shp", ".gml", ".geojson", ".json", ".kml")):
                continue
                
            ruta_parcela = os.path.join("datos_origen", archivo_parcela)
            try:
                parcela_wgs84 = gpd.read_file(ruta_parcela).to_crs(epsg=4326)
                parcela_proj = parcela_wgs84.to_crs(self.crs_objetivo)
                geom_parcela_proj = parcela_proj.union_all()
                area_total_parcela = parcela_proj.area.sum()
                
                # Crear carpeta de resultados
                nombre_subcarpeta = f"{os.path.splitext(archivo_parcela)[0]}_{datetime.now().strftime('%Y%m%d_%H%M')}"
                carpeta_res = os.path.join("resultados", nombre_subcarpeta)
                os.makedirs(carpeta_res, exist_ok=True)
                
                resultados_csv = []

                for capa_cfg in capas_wms:
                    if not capa_cfg.get("gpkg"): continue
                    ruta_gpkg = os.path.join("capas", "gpkg", os.path.basename(capa_cfg["gpkg"]))
                    
                    if not os.path.exists(ruta_gpkg): continue
                    
                    try:
                        # 1. Cálculo Vectorial
                        capa_vec = gpd.read_file(ruta_gpkg).to_crs(self.crs_objetivo)
                        interseccion = gpd.overlay(parcela_proj, capa_vec[capa_vec.intersects(geom_parcela_proj)], how="intersection")
                        
                        perc_total = (interseccion.area.sum() / area_total_parcela) * 100 if not interseccion.empty else 0
                        resultados_csv.append({"parcela": archivo_parcela, "capa": capa_cfg["nombre"], "porcentaje": perc_total})
                        
                        # Detalle por clasificación
                        perc_detalle = {}
                        styling = self.get_legend_styling(capa_cfg['nombre'])
                        field = styling['field']
                        if field and field in interseccion.columns:
                            interseccion['tmp_area'] = interseccion.area
                            detalle_area = interseccion.groupby(field)['tmp_area'].sum()
                            for cl, a in detalle_area.items():
                                etiq = styling['labels'].get(str(cl), str(cl))
                                perc_detalle[etiq] = (a / area_total_parcela) * 100

                        # 2. Generación de Mapa
                        fig, ax = plt.subplots(figsize=(10, 8))
                        parcela_viz = parcela_wgs84.to_crs(epsg=3857)
                        minx, miny, maxx, maxy = parcela_viz.total_bounds
                        margin = (maxx - minx) * 0.5
                        ax.set_xlim(minx - margin, maxx + margin)
                        ax.set_ylim(miny - margin, maxy + margin)

                        # Mapa base
                        cx.add_basemap(ax, crs="EPSG:3857", source="https://www.ign.es/wmts/ign-base?layer=IGNBaseTodo&style=default&tilematrixset=GoogleMapsCompatible&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image/jpeg&TileMatrix={z}&TileCol={x}&TileRow={y}", attribution="IGN")
                        
                        # Dibujar capa temática
                        capa_viz = capa_vec.to_crs(epsg=3857)
                        plot_args = {"ax": ax, "edgecolor": "black", "alpha": 0.4, "zorder": 5}
                        if styling['unique']:
                            capa_viz.plot(color=styling['color'], **plot_args)
                        else:
                            color_list = capa_viz[field].astype(str).map(styling['colors']).fillna('gray').tolist()
                            capa_viz.plot(color=color_list, **plot_args)

                        parcela_viz.plot(ax=ax, color="none", edgecolor="red", linewidth=2, zorder=10)
                        ax.axis("off")
                        
                        self.aplicar_leyenda(ax, capa_cfg)
                        self.aplicar_titulo(ax, capa_cfg, perc_total, perc_detalle)
                        
                        ax.set_position([0.05, 0.05, 0.9, 0.85])
                        plt.savefig(os.path.join(carpeta_res, f"mapa_{capa_cfg['nombre']}.jpg"), dpi=150, bbox_inches='tight', pad_inches=0.3)
                        plt.close()

                    except Exception as e:
                        print(f"Error procesando capa {capa_cfg['nombre']}: {e}")

                # Guardar Excels
                pd.DataFrame(resultados_csv).to_excel(os.path.join(carpeta_res, "resultados.xlsx"), index=False)

            except Exception as e:
                print(f"Error general procesando {archivo_parcela}: {e}")