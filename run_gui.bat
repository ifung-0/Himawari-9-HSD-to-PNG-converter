@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "PROJECT_DIR=%~dp0"
set "PYTHON313=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
set "PYTHON_CMD="
set "SCRIPT="

cd /d "%PROJECT_DIR%" || (
    echo ERROR: Could not switch to project folder:
    echo %PROJECT_DIR%
    pause
    exit /b 1
)

rem Detect the main script. Prefer the canonical name, then the _claude working
rem copy. An explicit HIMAWARI_SCRIPT override always wins if it exists.
if defined HIMAWARI_SCRIPT if exist "%HIMAWARI_SCRIPT%" set "SCRIPT=%HIMAWARI_SCRIPT%"
if not defined SCRIPT if exist "%PROJECT_DIR%himawari_lowram_processor.py" set "SCRIPT=%PROJECT_DIR%himawari_lowram_processor.py"
if not defined SCRIPT if exist "%PROJECT_DIR%himawari_lowram_processor_claude.py" set "SCRIPT=%PROJECT_DIR%himawari_lowram_processor_claude.py"

if not defined SCRIPT (
    echo ERROR: Could not find himawari_lowram_processor.py
    echo        or himawari_lowram_processor_claude.py in:
    echo %PROJECT_DIR%
    pause
    exit /b 1
)

if defined HIMAWARI_PYTHON (
    "%HIMAWARI_PYTHON%" --version >nul 2>&1
    if errorlevel 1 (
        echo ERROR: HIMAWARI_PYTHON is set but could not be run:
        echo %HIMAWARI_PYTHON%
        pause
        exit /b 1
    )
    set "PYTHON_CMD=%HIMAWARI_PYTHON%"
    goto :launch
)

if exist "%PROJECT_DIR%.venv\Scripts\python.exe" (
    set "PYTHON_CMD=%PROJECT_DIR%.venv\Scripts\python.exe"
    goto :launch
)

if exist "%PYTHON313%" (
    set "PYTHON_CMD=%PYTHON313%"
    goto :launch
)

py -3.13 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.13"
    goto :launch
)

py -3.12 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.12"
    goto :launch
)

python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    goto :launch
)

echo ERROR: No Python executable found.
echo Install Python 3.13 (or 3.12), or run check_environment.bat for repair help.
pause
exit /b 1

:launch
echo Launching Himawari-8/9 Low-RAM Processor...
echo Project: %PROJECT_DIR%
echo Script:  %SCRIPT%
echo Python:  %PYTHON_CMD%
echo.

%PYTHON_CMD% "%SCRIPT%" %*
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo The GUI exited with error code %EXIT_CODE%.
    echo Run check_environment.bat if this was an environment or package error.
    pause
)

exit /b %EXIT_CODE%
