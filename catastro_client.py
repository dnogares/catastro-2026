# En tu main.py o archivo principal
from fastapi import FastAPI
from catastro_client import CatastroClient  # ← Importar aquí

app = FastAPI()

# Crear una instancia del cliente
catastro = CatastroClient()

# Ejemplo 1: Endpoint simple
@app.get("/descargar-catastro/{referencia}")
async def descargar_catastro(referencia: str):
    """Descarga catastro desde el worker de Windows"""
    
    resultado = catastro.descargar_catastro(referencia)
    
    if resultado["success"]:
        return {
            "success": True,
            "mensaje": "Descarga completada",
            "archivo": resultado["archivo_local"]
        }
    else:
        return {
            "success": False,
            "error": resultado["error"]
        }

# Ejemplo 2: Endpoint con procesamiento
@app.post("/api/tasacion/{referencia}")
async def crear_tasacion(referencia: str):
    """Descarga catastro y procesa la tasación"""
    
    # 1. Descargar desde Windows
    resultado = catastro.descargar_catastro(referencia)
    
    if not resultado["success"]:
        return {"error": resultado["error"]}
    
    # 2. Aquí procesas el archivo descargado
    archivo_zip = resultado["archivo_local"]
    
    # Por ejemplo:
    # - Descomprimir el ZIP
    # - Leer los archivos GML, PDF, mapas
    # - Calcular la tasación
    # - Guardar en base de datos
    # - Subir a S3
    
    return {
        "success": True,
        "referencia": referencia,
        "archivo_procesado": archivo_zip
    }