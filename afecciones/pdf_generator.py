#!/usr/bin/env python3
"""
afecciones/pdf_generator.py
Generador de informes PDF con ReportLab
"""

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
from reportlab.lib.utils import ImageReader
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class AfeccionesPDF:
    """
    Genera informes PDF profesionales con análisis de afecciones
    """
    
    def __init__(self, output_dir: str):
        """
        Inicializa el generador de PDFs
        
        Args:
            output_dir: Directorio base donde guardar los PDFs
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generar(
        self, 
        referencia: str, 
        resultados: Dict, 
        mapas: List[str], 
        incluir_tabla: bool = True
    ) -> Optional[Path]:
        """
        Genera un PDF completo con análisis de afecciones
        
        Args:
            referencia: Referencia catastral
            resultados: Diccionario con resultados del análisis
            mapas: Lista de rutas a imágenes de mapas
            incluir_tabla: Si incluir tabla de afecciones
            
        Returns:
            Path al PDF generado o None si falla
        """
        try:
            # Crear directorio para esta referencia
            target_dir = self.output_dir / referencia
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Ruta del PDF
            pdf_path = target_dir / f"Informe_{referencia}.pdf"
            
            logger.info(f"Generando PDF: {pdf_path}")
            
            # Crear canvas
            c = canvas.Canvas(str(pdf_path), pagesize=A4)
            width, height = A4

            # PÁGINA 1: PORTADA Y DATOS
            self._dibujar_cabecera(c, "INFORME TÉCNICO DE TASACIÓN", width, height)
            
            # Datos identificativos
            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, height - 120, "DATOS IDENTIFICATIVOS")
            
            c.setFont("Helvetica", 11)
            y_pos = height - 140
            c.drawString(50, y_pos, f"Referencia Catastral: {referencia}")
            y_pos -= 20
            c.drawString(50, y_pos, f"Fecha de Informe: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
            y_pos -= 20
            
            # Información adicional si está en resultados
            if resultados:
                if "area_parcela_m2" in resultados:
                    c.drawString(50, y_pos, f"Superficie Parcela: {resultados['area_parcela_m2']:.2f} m²")
                    y_pos -= 20
                
                if "area_afectada_m2" in resultados:
                    c.drawString(50, y_pos, f"Superficie Afectada: {resultados['area_afectada_m2']:.2f} m²")
                    y_pos -= 20

            # TABLA DE AFECCIONES
            if incluir_tabla and resultados:
                y_table = height - 250
                c.setFont("Helvetica-Bold", 12)
                c.drawString(50, y_table + 20, "ANÁLISIS DE AFECCIONES VECTORIALES")
                self._dibujar_tabla_afecciones(c, resultados, 50, y_table)

            # Pie de página
            self._dibujar_pie(c, width, height)
            
            # PÁGINAS DE MAPAS
            if mapas:
                for idx, mapa_path in enumerate(mapas, 1):
                    mapa_path = Path(mapa_path)
                    
                    if not mapa_path.exists():
                        logger.warning(f"Mapa no encontrado: {mapa_path}")
                        continue
                    
                    # Nueva página
                    c.showPage()
                    
                    # Cabecera
                    self._dibujar_cabecera(
                        c, 
                        f"CARTOGRAFÍA Y POSICIONAMIENTO ({idx}/{len(mapas)})", 
                        width, 
                        height
                    )
                    
                    # Insertar imagen
                    try:
                        img = ImageReader(str(mapa_path))
                        img_width, img_height = img.getSize()
                        
                        # Calcular dimensiones manteniendo aspecto
                        max_width = width - 100
                        max_height = height - 200
                        
                        aspect = img_width / img_height
                        
                        if aspect > 1:  # Horizontal
                            draw_width = min(max_width, img_width)
                            draw_height = draw_width / aspect
                        else:  # Vertical
                            draw_height = min(max_height, img_height)
                            draw_width = draw_height * aspect
                        
                        # Centrar imagen
                        x_pos = (width - draw_width) / 2
                        y_pos = 150
                        
                        c.drawImage(
                            str(mapa_path), 
                            x_pos, 
                            y_pos, 
                            width=draw_width,
                            height=draw_height,
                            preserveAspectRatio=True
                        )
                        
                        # Etiqueta del mapa
                        c.setFont("Helvetica", 9)
                        c.setFillColor(colors.grey)
                        c.drawCentredString(
                            width / 2, 
                            y_pos - 20, 
                            f"Mapa {idx}: {mapa_path.name}"
                        )
                        c.setFillColor(colors.black)
                        
                    except Exception as e:
                        logger.error(f"Error insertando imagen {mapa_path}: {e}")
                        c.setFont("Helvetica", 11)
                        c.drawString(50, height / 2, f"Error cargando mapa: {mapa_path.name}")
                    
                    # Pie de página
                    self._dibujar_pie(c, width, height)

            # Guardar PDF
            c.save()
            
            logger.info(f"✅ PDF generado: {pdf_path}")
            return pdf_path
            
        except Exception as e:
            logger.error(f"Error generando PDF: {e}")
            return None

    def _dibujar_cabecera(self, c, titulo: str, width: float, height: float):
        """Dibuja la cabecera del PDF"""
        # Fondo azul oscuro
        c.setFillColor(colors.HexColor("#1e293b"))
        c.rect(0, height - 80, width, 80, fill=1, stroke=0)
        
        # Título en blanco
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 16)
        c.drawCentredString(width / 2, height - 50, titulo)
        
        # Subtítulo
        c.setFont("Helvetica", 10)
        c.drawCentredString(width / 2, height - 65, "Suite Tasación dnogares")
        
        # Restaurar color negro
        c.setFillColor(colors.black)

    def _dibujar_tabla_afecciones(self, c, resultados: Dict, x: float, y: float):
        """Dibuja tabla con resultados de afecciones"""
        try:
            # Preparar datos
            data = [["Normativa / Capa", "Impacto (%)", "Área (m²)"]]
            
            detalles = resultados.get("detalle", {})
            area_total = resultados.get("area_parcela_m2", 0)
            
            if detalles:
                for nombre, porcentaje in detalles.items():
                    area = (porcentaje / 100) * area_total if area_total else 0
                    data.append([
                        nombre, 
                        f"{porcentaje:.2f}%",
                        f"{area:.2f}"
                    ])
            else:
                data.append(["Sin afecciones detectadas", "0.0%", "0.0"])
            
            # Fila de total
            total_porcentaje = resultados.get("total", 0)
            total_area = resultados.get("area_afectada_m2", 0)
            data.append([
                "TOTAL AFECTADO", 
                f"{total_porcentaje:.2f}%",
                f"{total_area:.2f}"
            ])

            # Crear tabla
            table = Table(data, colWidths=[280, 100, 100])
            
            # Estilo
            style = TableStyle([
                # Cabecera
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#334155")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                
                # Celdas normales
                ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -2), 9),
                ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                
                # Fila total
                ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#f1f5f9")),
                
                # Bordes
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ])
            table.setStyle(style)
            
            # Dibujar tabla
            w, h = table.wrap(0, 0)
            table.drawOn(c, x, y - h)
            
        except Exception as e:
            logger.error(f"Error dibujando tabla: {e}")
            c.setFont("Helvetica", 10)
            c.drawString(x, y, "Error generando tabla de afecciones")

    def _dibujar_pie(self, c, width: float, height: float):
        """Dibuja pie de página con información legal"""
        c.setFont("Helvetica-Oblique", 8)
        c.setFillColor(colors.grey)
        
        # Línea 1
        c.drawCentredString(
            width / 2, 
            40, 
            "Datos obtenidos de la Sede Electrónica del Catastro y fuentes oficiales."
        )
        
        # Línea 2
        c.drawCentredString(
            width / 2, 
            30, 
            f"Documento generado automáticamente - Página {c.getPageNumber()}"
        )
        
        c.setFillColor(colors.black)


# Testing
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python pdf_generator.py <referencia>")
        sys.exit(1)
    
    referencia = sys.argv[1]
    
    # Datos de ejemplo
    resultados = {
        "total": 15.5,
        "detalle": {
            "Zona Inundable": 10.2,
            "Espacio Natural": 5.3
        },
        "area_parcela_m2": 1000.0,
        "area_afectada_m2": 155.0
    }
    
    pdf_gen = AfeccionesPDF(output_dir="outputs")
    pdf_path = pdf_gen.generar(
        referencia=referencia,
        resultados=resultados,
        mapas=[],
        incluir_tabla=True
    )
    
    if pdf_path:
        print(f"✅ PDF generado: {pdf_path}")
    else:
        print("❌ Error generando PDF")