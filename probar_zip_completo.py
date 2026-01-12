#!/usr/bin/env python3
from pathlib import Path
from catastro.catastro_downloader import CatastroDownloader
import zipfile
import json

# Configuración
ref = '8884601WF4788S0020LL'
output_dir = Path('i:/Tasacion2026/data/outputs')

print('=== Probando ZIP Completo ===')
print(f'Referencia: {ref}')
print(f'Directorio: {output_dir}')

# Crear instancia del downloader
downloader = CatastroDownloader(output_dir=str(output_dir))

# Generar ZIP completo
print('Generando ZIP completo...')
exito, zip_path = downloader.descargar_todo_completo(ref)

if exito and zip_path:
    print(f'✅ ZIP generado: {zip_path}')
    print(f'Tamaño: {Path(zip_path).stat().st_size / 1024 / 1024:.2f} MB')
    
    # Verificar contenido del ZIP
    print('\n📦 Contenido del ZIP:')
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        for file_info in zipf.filelist:
            size_kb = file_info.file_size / 1024
            print(f'  📄 {file_info.filename} ({size_kb:.1f} KB)')
        
        # Verificar manifiesto
        if 'manifesto.json' in [f.filename for f in zipf.filelist]:
            manifest_data = zipf.read('manifesto.json')
            manifest = json.loads(manifest_data)
            print(f'\n📋 Manifiesto:')
            print(f'  📅 Fecha: {manifest["fecha_generacion"]}')
            print(f'  📊 Archivos: {len(manifest["archivos_incluidos"])}')
            
            print('\n📋 Archivos principales:')
            for archivo in manifest["archivos_incluidos"]:
                if any(ext in archivo["ruta"].lower() for ext in ['.pdf', '.csv', '.kml', '.gml']):
                    print(f'  📄 {archivo["ruta"]} ({archivo["tamaño"]} bytes)')
else:
    print('❌ No se pudo generar el ZIP')
