@echo off
setlocal
cd /d "%~dp0"

set "CHECK_ARGS=%*"
if "%~1"=="" goto menu
goto run_checker

:menu
echo Himawari-9 Environment Tools
echo.
echo 1. Check environment
echo 2. Auto fix environment
echo.
choice /c 12 /n /m "Choose 1 or 2: "
if errorlevel 2 (
    set "CHECK_ARGS=--auto"
) else (
    set "CHECK_ARGS=--plain"
)

:run_checker
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" "check_environment.py" %CHECK_ARGS%
    set "EXIT_CODE=%errorlevel%"
    goto done
)

py -3.13 --version >nul 2>&1
if %errorlevel%==0 (
    py -3.13 "check_environment.py" %CHECK_ARGS%
    set "EXIT_CODE=%errorlevel%"
    goto done
)

py -3.12 --version >nul 2>&1
if %errorlevel%==0 (
    py -3.12 "check_environment.py" %CHECK_ARGS%
    set "EXIT_CODE=%errorlevel%"
    goto done
)

python "check_environment.py" %CHECK_ARGS%
set "EXIT_CODE=%errorlevel%"

:done
if "%~1"=="" pause
exit /b %EXIT_CODE%
