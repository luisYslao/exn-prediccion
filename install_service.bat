@echo off
setlocal

:: ============================================================
::  INSTALADOR DEL SERVICIO - PrediccionService
::  Requiere: nssm.exe en la misma carpeta o en PATH
::  Ejecutar como Administrador
:: ============================================================

set SERVICE_NAME=PrediccionService
set PROJECT_DIR=C:\Users\Luis\Desktop\Prediccion3
set PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe
set SCRIPT=src\scheduler_service.py

:: -- Verificar que se ejecuta como Administrador --
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Este script debe ejecutarse como Administrador.
    echo Click derecho ^> "Ejecutar como administrador"
    pause
    exit /b 1
)

:: -- Buscar nssm.exe --
where nssm >nul 2>&1
if %errorlevel% neq 0 (
    if exist "%PROJECT_DIR%\libs\nssm.exe" (
        set NSSM=%PROJECT_DIR%\libs\nssm.exe
    ) else (
        echo [ERROR] No se encontro nssm.exe
        echo.
        echo Descargalo desde: https://nssm.cc/download
        echo Coloca nssm.exe en: %PROJECT_DIR%\libs\
        echo.
        pause
        exit /b 1
    )
) else (
    set NSSM=nssm
)

:: -- Verificar que existe el .venv --
if not exist "%PYTHON_EXE%" (
    echo [ERROR] No se encontro el entorno virtual en: %PYTHON_EXE%
    echo Verifica que el .venv este creado en: %PROJECT_DIR%\.venv\
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Configuracion del servicio:
echo  Nombre:    %SERVICE_NAME%
echo  Directorio: %PROJECT_DIR%
echo  Python:    %PYTHON_EXE%
echo  Script:    %SCRIPT%
echo ============================================================
echo.

:: -- Eliminar servicio anterior si existe --
%NSSM% status %SERVICE_NAME% >nul 2>&1
if %errorlevel% equ 0 (
    echo [INFO] Servicio anterior encontrado, eliminando...
    %NSSM% stop %SERVICE_NAME% >nul 2>&1
    %NSSM% remove %SERVICE_NAME% confirm >nul 2>&1
    echo [OK] Servicio anterior eliminado.
)

:: -- Instalar el servicio --
echo [INFO] Instalando servicio...
%NSSM% install %SERVICE_NAME% "%PYTHON_EXE%" "%PROJECT_DIR%\%SCRIPT%"
if %errorlevel% neq 0 (
    echo [ERROR] Fallo la instalacion del servicio.
    pause
    exit /b 1
)

:: -- Configurar directorio de trabajo --
%NSSM% set %SERVICE_NAME% AppDirectory "%PROJECT_DIR%"

:: -- Inicio automatico al arrancar Windows --
%NSSM% set %SERVICE_NAME% Start SERVICE_AUTO_START

:: -- Reiniciar automaticamente si falla (espera 5 seg) --
%NSSM% set %SERVICE_NAME% AppRestartDelay 5000
%NSSM% set %SERVICE_NAME% AppExit Default Restart

:: -- Guardar logs del servicio --
if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"
%NSSM% set %SERVICE_NAME% AppStdout "%PROJECT_DIR%\logs\service_stdout.log"
%NSSM% set %SERVICE_NAME% AppStderr "%PROJECT_DIR%\logs\service_stderr.log"
%NSSM% set %SERVICE_NAME% AppRotateFiles 1
%NSSM% set %SERVICE_NAME% AppRotateBytes 5242880

:: -- Iniciar el servicio ahora --
echo [INFO] Iniciando servicio...
%NSSM% start %SERVICE_NAME%
if %errorlevel% neq 0 (
    echo [WARN] El servicio se instalo pero no pudo iniciarse ahora.
    echo Revisa los logs en: %PROJECT_DIR%\logs\
) else (
    echo [OK] Servicio iniciado correctamente.
)

echo.
echo ============================================================
echo  INSTALACION COMPLETADA
echo.
echo  Comandos utiles:
echo    Ver estado:   sc query %SERVICE_NAME%
echo    Detener:      net stop %SERVICE_NAME%
echo    Iniciar:      net start %SERVICE_NAME%
echo    Desinstalar:  nssm remove %SERVICE_NAME% confirm
echo    Ver logs:     %PROJECT_DIR%\logs\
echo ============================================================
echo.
pause
