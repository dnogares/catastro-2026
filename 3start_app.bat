@echo off
title Suite Tasacion Launcher

echo ==========================================
echo    DESPLEGANDO SUITE TASACION LOCAL
echo ==========================================

REM Activar conda
call conda activate tasacion || (
    echo [INFO] Creando entorno conda...
    conda create -n tasacion python=3.11 -y
    call conda activate tasacion
)

REM Instalar dependencias geoespaciales con conda-forge
echo [INFO] Instalando dependencias geoespaciales...
conda install -c conda-forge geopandas fiona gdal shapely pyogrio pandas numpy -y

REM Instalar resto con pip
echo [INFO] Instalando dependencias web...
pip install fastapi==0.104.1 uvicorn==0.24.0 pydantic==2.5.2 requests==2.31.0 pillow==10.1.0 reportlab==4.0.7 python-multipart==0.0.6 pytz jinja2 matplotlib contextily owslib

REM Crear carpetas necesarias
if not exist "capas" mkdir capas
if not exist "outputs" mkdir outputs

echo.
echo ==========================================
echo    INICIANDO SERVIDOR
echo    Accede a: http://localhost:8090
echo ==========================================
echo.

python main.py
pause
