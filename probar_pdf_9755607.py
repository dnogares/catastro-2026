#!/usr/bin/env python3
from pathlib import Path
from afecciones.pdf_generator import AfeccionesPDF
import traceback

# Directorio con los archivos urbanísticos
urbanismo_dir = Path('i:/Tasacion2026/data/outputs/urbanismo/9755607VJ1195N_2026-01-12_19-04-49')
ref = '9755607VJ1195N'

print('=== Probando PDF Urbanístico con referencia 9755607VJ1195N ===')
print(f'Directorio: {urbanismo_dir}')
print(f'Existe: {urbanismo_dir.exists()}')

if urbanismo_dir.exists():
    print('Archivos en el directorio:')
    for f in urbanismo_dir.iterdir():
        print(f'  - {f.name}')

# Resultados urbanísticos de ejemplo
resultados = {
    'urbanismo': True,
    'area_parcela_m2': 890.2,
    'detalle': {
        'Suelo Urbano': 75.0,
        'Suelo Rustico': 25.0
    }
}

# Mapas disponibles
mapas = [str(urbanismo_dir / '9755607VJ1195N_mapa.png')]
print(f'Mapas disponibles: {len(mapas)}')

try:
    print('Creando instancia AfeccionesPDF...')
    pdf_gen = AfeccionesPDF(output_dir=str(urbanismo_dir))
    print('Instancia creada correctamente')
    
    print('Generando PDF...')
    pdf_path = pdf_gen.generar(
        referencia=ref,
        resultados=resultados,
        mapas=mapas,
        incluir_tabla=True
    )
    
    print(f'✅ PDF generado: {pdf_path}')
    print(f'URL: http://localhost:81/outputs/urbanismo/9755607VJ1195N_2026-01-12_19-04-49/{Path(pdf_path).name}')
    
    # Verificar que el PDF existe
    if Path(pdf_path).exists():
        print(f'✅ PDF confirmado en: {pdf_path}')
        print(f'Tamaño: {Path(pdf_path).stat().st_size} bytes')
    else:
        print(f'❌ PDF no encontrado en: {pdf_path}')
    
except Exception as e:
    print(f'❌ Error: {e}')
    print(f'❌ Traceback: {traceback.format_exc()}')
