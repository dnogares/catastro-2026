@echo off
title Suite Tasacion Launcher

echo ==========================================
echo    DESPLEGANDO SUITE TASACION LOCAL
echo ==========================================

REM Verificar Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python no esta instalado o no esta en el PATH.
    echo Por favor instala Python 3.10+ desde python.org
    pause
    exit /b
)

REM Crear entorno virtual si no existe
if not exist "venv" (
    echo [INFO] Creando entorno virtual 'venv'...
    python -m venv venv
)

REM Activar entorno
call venv\Scripts\activate

REM Actualizar pip
python -m pip install --upgrade pip

REM Instalar pipwin para dependencias geoespaciales
echo [INFO] Instalando pipwin para manejar dependencias de Windows...
pip install pipwin

REM Instalar dependencias geoespaciales con wheels precompilados
echo [INFO] Instalando GDAL, Fiona y Shapely precompilados...
pipwin install gdal
pipwin install fiona
pipwin install shapely

REM Instalar resto de dependencias
echo [INFO] Instalando dependencias restantes...
pip install -r requirements.txt

REM Crear carpetas necesarias
if not exist "capas" mkdir capas
if not exist "outputs" mkdir outputs

echo.
echo ==========================================
echo    INICIANDO SERVIDOR
echo    Accede a: http://localhost:8090
echo ==========================================
echo.

REM Ejecutar aplicacion
python main.py

pause

