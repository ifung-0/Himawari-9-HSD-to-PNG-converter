@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "PROJECT_DIR=%~dp0"
set "PYTHON313=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"

cd /d "%PROJECT_DIR%" || (
    echo ERROR: Could not switch to project folder:
    echo %PROJECT_DIR%
    pause
    exit /b 1
)

call :detect_python
if not defined PYTHON_CMD (
    echo ERROR: No Python executable found.
    echo Install Python 3.13 (or 3.12), or run "himawari check-env" for repair help.
    pause
    exit /b 1
)

if "%~1"=="" goto menu

set "ACTION=%~1"
shift
set "ACTION_ARGS=%*"

if /i "%ACTION%"=="install-reqs" goto install_reqs
if /i "%ACTION%"=="install" goto install_reqs
if /i "%ACTION%"=="quick-fix" goto quick_fix
if /i "%ACTION%"=="fix" goto quick_fix
if /i "%ACTION%"=="check-env" goto check_env
if /i "%ACTION%"=="doctor" goto check_env
if /i "%ACTION%"=="check" goto check_env
if /i "%ACTION%"=="cli" goto run_cli
if /i "%ACTION%"=="gui" goto run_gui
if /i "%ACTION%"=="tui" goto run_tui
if /i "%ACTION%"=="help" goto help_screen

echo Unknown action: %ACTION%
echo Run "himawari help" for usage.
pause
exit /b 1

:menu
cls
echo =======================================================================
echo        Himawari-8/9 HSD to PNG Converter - Launcher
echo =======================================================================
echo.
echo Project: %PROJECT_DIR%
echo Python:  %PYTHON_CMD%
echo.
echo   1. Install Requirements
echo   2. Quick Fix (repair environment, install overlay data)
echo   3. Check Environment (doctor)
echo   4. Run CLI
echo   5. Run GUI
echo   6. Run TUI
echo.
echo   0. Exit
echo.
choice /c 1234560 /n /m "Choose an option (0-6): "

if errorlevel 7 exit /b 0
if errorlevel 6 goto run_tui
if errorlevel 5 goto run_gui
if errorlevel 4 goto run_cli
if errorlevel 3 goto check_env
if errorlevel 2 goto quick_fix
if errorlevel 1 goto install_reqs

exit /b 0

:install_reqs
echo Installing requirements...
%PYTHON_CMD% "%PROJECT_DIR%install_requirements.py" %ACTION_ARGS%
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
    echo.
    echo Install requirements exited with code %EXIT_CODE%.
    pause
)
exit /b %EXIT_CODE%

:quick_fix
if defined ACTION_ARGS (
    %PYTHON_CMD% "%PROJECT_DIR%check_environment.py" --fix %ACTION_ARGS%
) else (
    %PYTHON_CMD% "%PROJECT_DIR%check_environment.py" --fix
)
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
    echo.
    echo Quick fix exited with code %EXIT_CODE%.
    pause
)
exit /b %EXIT_CODE%

:check_env
%PYTHON_CMD% "%PROJECT_DIR%check_environment.py" %ACTION_ARGS%
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
    pause
)
exit /b %EXIT_CODE%

:run_cli
set "SCRIPT=%PROJECT_DIR%himawari_cli.py"

if not exist "%SCRIPT%" (
    echo ERROR: himawari_cli.py was not found.
    pause
    exit /b 1
)

echo Launching Himawari-9 CLI...
%PYTHON_CMD% "%SCRIPT%" %ACTION_ARGS%
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo The CLI exited with error code %EXIT_CODE%.
    echo Run "himawari check-env" if this was an environment or package error.
    pause
)
exit /b %EXIT_CODE%

:run_gui
set "SCRIPT="

if defined HIMAWARI_SCRIPT if exist "%HIMAWARI_SCRIPT%" set "SCRIPT=%HIMAWARI_SCRIPT%"
if not defined SCRIPT if exist "%PROJECT_DIR%himawari_lowram_processor.py" set "SCRIPT=%PROJECT_DIR%himawari_lowram_processor.py"
if not defined SCRIPT if exist "%PROJECT_DIR%himawari_lowram_processor_claude.py" set "SCRIPT=%PROJECT_DIR%himawari_lowram_processor_claude.py"

if not defined SCRIPT (
    echo ERROR: Could not find himawari_lowram_processor.py
    echo        or himawari_lowram_processor_claude.py.
    pause
    exit /b 1
)

echo Launching Himawari-8/9 Low-RAM Processor...
echo Script:  %SCRIPT%
echo.
%PYTHON_CMD% "%SCRIPT%" %ACTION_ARGS%
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo The GUI exited with error code %EXIT_CODE%.
    echo Run "himawari check-env" if this was an environment or package error.
    pause
)
exit /b %EXIT_CODE%

:run_tui
set "SCRIPT=%PROJECT_DIR%himawari_tui.py"

if not exist "%SCRIPT%" (
    echo ERROR: himawari_tui.py was not found.
    pause
    exit /b 1
)

echo Launching Himawari-9 TUI...
%PYTHON_CMD% "%SCRIPT%" %ACTION_ARGS%
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo The TUI exited with error code %EXIT_CODE%.
    echo Run "himawari check-env" if this was an environment or package error.
    pause
)
exit /b %EXIT_CODE%

:help_screen
echo Usage: himawari ^<action^> [args...]
echo.
echo Actions:
echo   install-reqs  Install Python requirements
echo   quick-fix     Repair environment (upgrade packages, install overlay data)
echo   check-env     Check environment (doctor)
echo   cli           Run the command-line interface
echo   gui           Run the graphical user interface
echo   tui           Run the text user interface
echo   help          Show this help
echo.
echo With no arguments, displays an interactive menu.
echo Pass "--help" to any action to see its specific options.
echo.
echo Examples:
echo   himawari check-env
echo   himawari check-env --plain
echo   himawari check-env --fix
echo   himawari install-reqs --upgrade
echo   himawari cli --input out.pgw
echo   himawari gui
echo   himawari tui
pause
exit /b 0

:detect_python
set "PYTHON_CMD="

if defined HIMAWARI_PYTHON (
    "%HIMAWARI_PYTHON%" --version >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=%HIMAWARI_PYTHON%"
        exit /b 0
    )
)

if exist "%PROJECT_DIR%.venv\Scripts\python.exe" (
    set "PYTHON_CMD=%PROJECT_DIR%.venv\Scripts\python.exe"
    exit /b 0
)

if exist "%PYTHON313%" (
    set "PYTHON_CMD=%PYTHON313%"
    exit /b 0
)

py -3.13 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.13"
    exit /b 0
)

py -3.12 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.12"
    exit /b 0
)

python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    exit /b 0
)

exit /b 1
