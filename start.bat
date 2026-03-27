@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

if not exist ".venv" (
    echo Missing virtual environment at %SCRIPT_DIR%.venv
    echo Create it first, then install requirements.
    pause
    exit /b 1
)

set "PYTHON=%SCRIPT_DIR%.venv\Scripts\python.exe"
set "FLASK=%SCRIPT_DIR%.venv\Scripts\flask.exe"

if not exist "%PYTHON%" (
    echo Python not found in .venv. Did you create it?
    pause
    exit /b 1
)

"%PYTHON%" -c "import flask" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Flask is not installed in the virtual environment.
    echo Run: .venv\Scripts\activate ^&^& pip install -r requirements.txt
    pause
    exit /b 1
)

if "%HOST%"=="" set "HOST=127.0.0.1"
if "%PORT%"=="" (
    set "PORT=5000"
    set "PORT_WAS_SET=0"
) else (
    set "PORT_WAS_SET=1"
)

:: Check if the initial port is free
"%PYTHON%" -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('%HOST%', %PORT%)); s.close()" >nul 2>&1
if %ERRORLEVEL% equ 0 goto :launch

if "%PORT_WAS_SET%"=="1" (
    echo Port %PORT% is already in use on %HOST%.
    pause
    exit /b 1
)

echo Port %PORT% is busy on %HOST%. Searching for a free port...

for /L %%P in (5001, 1, 5020) do (
    "%PYTHON%" -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('%HOST%', %%P)); s.close()" >nul 2>&1
    if !ERRORLEVEL! equ 0 (
        set "PORT=%%P"
        goto :launch
    )
)

echo Could not find a free port near 5000.
pause
exit /b 1

:launch
echo Starting IGReelScraper at http://%HOST%:%PORT%

set FLASK_APP=app.py
set FLASK_DEBUG=1

"%FLASK%" run --host=%HOST% --port=%PORT%
pause
