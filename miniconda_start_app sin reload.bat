@echo off

title Suite Tasacion Launcher

echo ==========================================
echo DESPLEGANDO SUITE TASACION LOCAL
echo ==========================================

REM Ir al directorio del proyecto
cd /d I:\Tasacion2026

REM Verificar Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
echo [ERROR] Python no esta instalado
pause
exit /b
)

REM Crear entorno virtual si no existe
if not exist "venv" (
echo [INFO] Creando entorno virtual...
python -m venv venv
)

REM Activar entorno
call venv\Scripts\activate

REM Actualizar pip y wheel
echo [INFO] Actualizando pip...
python -m pip install --upgrade pip wheel setuptools

REM Limpiar cache
echo [INFO] Limpiando cache...
pip cache purge

REM Instalar SOLO wheels precompilados (no compilar nada)
echo [INFO] Instalando dependencias con wheels precompilados...
pip install --only-binary :all: --upgrade numpy pandas pillow shapely matplotlib

REM Instalar el resto permitiendo compilacion solo si es necesario
echo [INFO] Instalando FastAPI y dependencias...
pip install --upgrade fastapi uvicorn[standard] pydantic requests

REM Instalar GIS
echo [INFO] Instalando geopandas...
pip install --upgrade geopandas pyogrio

REM Instalar resto
echo [INFO] Instalando dependencias adicionales...
pip install --upgrade reportlab python-multipart pytz jinja2 lxml contextily owslib rtree

REM Crear carpetas necesarias
if not exist "capas" mkdir capas
if not exist "outputs" mkdir outputs

echo.
echo ==========================================
echo INICIANDO SERVIDOR (SIN RECARGA AUTOMATICA)
echo Accede a: http://localhost:81
echo ==========================================
echo.

REM Ejecutar aplicacion sin reload
uvicorn main:app --host 0.0.0.0 --port 81

pause
