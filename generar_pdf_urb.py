#!/usr/bin/env python3
from pathlib import Path
from afecciones.pdf_generator import AfeccionesPDF

# Directorio con los archivos urbanísticos
urbanismo_dir = Path('i:/Tasacion2026/data/outputs/urbanismo/8884601WF4788S0020LL_2026-01-12_19-26-29')
ref = '8884601WF4788S0020LL'

# Resultados urbanísticos de ejemplo
resultados = {
    'urbanismo': True,
    'area_parcela_m2': 1250.5,
    'detalle': {
        'Suelo Urbano': 85.0,
        'Suelo Rustico': 15.0
    }
}

# Mapas disponibles
mapas = [str(urbanismo_dir / '8884601WF4788S0020LL_mapa.png')]

print('Generando PDF urbanístico...')
print(f'Directorio: {urbanismo_dir}')
print(f'Mapas: {mapas}')

# Generar PDF
pdf_gen = AfeccionesPDF(output_dir=str(urbanismo_dir))
pdf_path = pdf_gen.generar(
    referencia=ref,
    resultados=resultados,
    mapas=mapas,
    incluir_tabla=True
)

print(f'✅ PDF generado: {pdf_path}')
print(f'URL: http://localhost:81/outputs/urbanismo/8884601WF4788S0020LL_2026-01-12_19-26-29/{Path(pdf_path).name}')
