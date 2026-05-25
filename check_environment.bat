@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" "check_environment.py" %*
    exit /b %errorlevel%
)

py -3.13 --version >nul 2>&1
if %errorlevel%==0 (
    py -3.13 "check_environment.py" %*
    exit /b %errorlevel%
)

py -3.12 --version >nul 2>&1
if %errorlevel%==0 (
    py -3.12 "check_environment.py" %*
    exit /b %errorlevel%
)

python "check_environment.py" %*
exit /b %errorlevel%

