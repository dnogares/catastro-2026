#!/usr/bin/env python3
import requests
import sys

# Procesar la referencia para que se genere el PDF
ref = '8884601WF4788S0020LL'
url = 'http://localhost:81/api/v1/analizar-parcela'

try:
    response = requests.post(url, data={'referencia': ref})
    if response.status_code == 200:
        data = response.json()
        print(f'✅ Referencia {ref} procesada')
        print(f'Status: {data.get("status")}')
        if 'archivos_generados' in data:
            print('Archivos generados:')
            for tipo, ruta in data['archivos_generados'].items():
                print(f'  {tipo}: {ruta}')
    else:
        print(f'❌ Error: {response.status_code} - {response.text}')
except Exception as e:
    print(f'❌ Error: {e}')
    sys.exit(1)
