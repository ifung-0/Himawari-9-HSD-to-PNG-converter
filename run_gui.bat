@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" "himawari_lowram_processor.py" %*
    exit /b %errorlevel%
)

py -3.13 --version >nul 2>&1
if %errorlevel%==0 (
    py -3.13 "himawari_lowram_processor.py" %*
    exit /b %errorlevel%
)

py -3.12 --version >nul 2>&1
if %errorlevel%==0 (
    py -3.12 "himawari_lowram_processor.py" %*
    exit /b %errorlevel%
)

python "himawari_lowram_processor.py" %*
exit /b %errorlevel%

