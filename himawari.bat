@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "PROJECT_DIR=%~dp0"

REM Find Python.
REM PYTHON_CMD holds the executable (a full path, or a bare command like "py"
REM or "python" that is resolved via PATH). PYTHON_ARGS holds any launcher flags
REM (e.g. "-3.13" for the py launcher). Keeping them separate lets every launch
REM quote "%PYTHON_CMD%" (so paths with spaces work) while still passing the
REM version flag - quoting the whole "py -3.13" string would look for a program
REM literally named "py -3.13" and fail.
set "PYTHON313=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
set "PYTHON_CMD="
set "PYTHON_ARGS="

if defined HIMAWARI_PYTHON (
    "%HIMAWARI_PYTHON%" --version >nul 2>&1
    if not errorlevel 1 set "PYTHON_CMD=%HIMAWARI_PYTHON%"
)

if not defined PYTHON_CMD (
    if exist "%PROJECT_DIR%.venv\Scripts\python.exe" set "PYTHON_CMD=%PROJECT_DIR%.venv\Scripts\python.exe"
)

if not defined PYTHON_CMD (
    if exist "%PYTHON313%" set "PYTHON_CMD=%PYTHON313%"
)

if not defined PYTHON_CMD (
    py -3.13 --version >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=py"
        set "PYTHON_ARGS=-3.13"
    )
)

if not defined PYTHON_CMD (
    py -3.12 --version >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=py"
        set "PYTHON_ARGS=-3.12"
    )
)

if not defined PYTHON_CMD (
    python --version >nul 2>&1
    if not errorlevel 1 set "PYTHON_CMD=python"
)

if not defined PYTHON_CMD (
    echo ERROR: No Python executable found.
    echo Install Python 3.13 or 3.12.
    pause
    exit /b 1
)

REM No arguments = interactive menu
if "%~1"=="" goto menu

REM With arguments - run action and exit
set "ACTION=%~1"

if /i "%ACTION%"=="install-reqs" goto install_reqs
if /i "%ACTION%"=="install" goto install_reqs
if /i "%ACTION%"=="quick-fix" goto quick_fix
if /i "%ACTION%"=="fix" goto quick_fix
if /i "%ACTION%"=="check-env" goto check_env
if /i "%ACTION%"=="doctor" goto check_env
if /i "%ACTION%"=="check" goto check_env
if /i "%ACTION%"=="cli" goto run_cli
if /i "%ACTION%"=="gui" goto run_gui
if /i "%ACTION%"=="simple" goto run_simple
if /i "%ACTION%"=="tui" goto run_tui
if /i "%ACTION%"=="help" goto help

echo Unknown action: %ACTION%
pause
exit /b 1

:menu
cls
echo =======================================================================
echo        Himawari-8/9 HSD to PNG Converter - Launcher
echo =======================================================================
echo.
echo   1. Install Requirements
echo   2. Quick Fix (repair environment)
echo   3. Check Environment (doctor)
echo   4. Run CLI
echo   5. Run GUI (full)
echo   6. Run Simple GUI
echo   7. Run TUI
echo   0. Exit
echo.

choice /c 12345670 /n /m "Choose an option (0-7): "
if errorlevel 8 exit /b 0
if errorlevel 7 goto run_tui
if errorlevel 6 goto run_simple
if errorlevel 5 goto run_gui
if errorlevel 4 goto run_cli
if errorlevel 3 goto check_env
if errorlevel 2 goto quick_fix
if errorlevel 1 goto install_reqs
goto menu

:install_reqs
echo Installing requirements...
"%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%install_requirements.py"
pause
goto menu

:quick_fix
echo Running Quick Fix...
"%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%check_environment.py" --fix
pause
goto menu

:check_env
echo Checking environment...
"%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%check_environment.py"
pause
goto menu

:run_cli
if not exist "%PROJECT_DIR%himawari_cli.py" (
    echo ERROR: himawari_cli.py not found
    pause
    goto menu
)
echo Launching CLI...
"%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%himawari_cli.py"
pause
goto menu

:run_gui
if defined HIMAWARI_SCRIPT (
    if exist "%HIMAWARI_SCRIPT%" (
        echo Launching GUI...
        "%PYTHON_CMD%" %PYTHON_ARGS% "%HIMAWARI_SCRIPT%"
        pause
        goto menu
    )
)
if exist "%PROJECT_DIR%himawari_lowram_processor.py" (
    echo Launching GUI...
    "%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%himawari_lowram_processor.py"
    pause
    goto menu
)
REM Fallback for a renamed working copy. The Python modules already accept a
REM himawari_lowram_processor_claude.py copy (see the _ALTERNATES lists in
REM check_environment.py / himawari_tui.py / himawari_lowram_simple.py); this
REM branch lets the launcher start that same renamed copy when the canonical
REM filename is absent, so it is intentionally kept, not dead code.
if exist "%PROJECT_DIR%himawari_lowram_processor_claude.py" (
    echo Launching GUI...
    "%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%himawari_lowram_processor_claude.py"
    pause
    goto menu
)
echo ERROR: Could not find GUI script
pause
goto menu

:run_simple
if not exist "%PROJECT_DIR%himawari_lowram_simple.py" (
    echo ERROR: himawari_lowram_simple.py not found
    pause
    goto menu
)
echo Launching Simple GUI...
"%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%himawari_lowram_simple.py"
pause
goto menu

:run_tui
if not exist "%PROJECT_DIR%himawari_tui.py" (
    echo ERROR: himawari_tui.py not found
    pause
    goto menu
)
echo Launching TUI...
"%PYTHON_CMD%" %PYTHON_ARGS% "%PROJECT_DIR%himawari_tui.py"
pause
goto menu

:help
echo Usage: himawari ^<action^> [args...]
echo.
echo Actions:
echo   install-reqs  Install Python requirements
echo   quick-fix     Repair environment
echo   check-env     Check environment (doctor)
echo   cli           Run the command-line interface
echo   gui           Run the graphical user interface (full)
echo   simple        Run the simplified graphical user interface
echo   tui           Run the text user interface
echo   help          Show this help
echo.
pause
goto menu