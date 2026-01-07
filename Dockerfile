FROM python:3.10-slim

# Instalar dependencias del sistema para GeoPandas y ReportLab
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar requerimientos e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install matplotlib
RUN apt-get update && apt-get install -y libpng-dev libfreetype6-dev

# Copiar el resto de la aplicación
COPY . .

# Crear directorios necesarios
RUN mkdir -p outputs/gml outputs/pdfs outputs/imagenes outputs/html outputs/zips capas/gpkg

# Exponer el puerto
EXPOSE 8080

# Comando para ejecutar la aplicación
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]
