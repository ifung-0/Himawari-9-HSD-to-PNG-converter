@echo off
setlocal EnableExtensions

cd /d "%~dp0" || (
    echo ERROR: Could not switch to project folder:
    echo %~dp0
    pause
    exit /b 1
)

call "%~dp0check_environment.bat" %*
exit /b %ERRORLEVEL%
