@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ENV_NAME=data_sem2"
set "PYTHON_VERSION=3.10"

cd /d "%SCRIPT_DIR%\.."

call :find_conda
if errorlevel 1 goto :fail

call :ensure_env
if errorlevel 1 goto :fail

call :install_requirements
if errorlevel 1 goto :fail

call :run_smoke
if errorlevel 1 goto :fail

echo [OK] Environment "%ENV_NAME%" is ready.
goto :ok

:find_conda
rem Поиск conda в стандартных местах
set "CONDA_BAT="
if exist "D:\Programms\Coding\Miniconda3\Scripts\conda.exe" set "CONDA_BAT=D:\Programms\Coding\Miniconda3\Scripts\conda.exe"
if exist "C:\Users\%USERNAME%\miniconda3\Scripts\conda.exe" set "CONDA_BAT=C:\Users\%USERNAME%\miniconda3\Scripts\conda.exe"
if exist "C:\ProgramData\miniconda3\Scripts\conda.exe" set "CONDA_BAT=C:\ProgramData\miniconda3\Scripts\conda.exe"

if not defined CONDA_BAT (
    echo [ERROR] Conda not found
    exit /b 1
)
echo Found Conda: %CONDA_BAT%
exit /b 0

:ensure_env
rem Проверка существования окружения
call "%CONDA_BAT%" info --envs | findstr "%ENV_NAME%" >nul
if errorlevel 1 (
    echo Creating environment %ENV_NAME%...
    call "%CONDA_BAT%" create -n %ENV_NAME% python=%PYTHON_VERSION% -y
    if errorlevel 1 (
        echo [ERROR] Failed to create environment
        exit /b 1
    )
) else (
    echo Environment %ENV_NAME% already exists
)
exit /b 0

:install_requirements
rem Установка зависимостей из requirements.txt
if exist requirements.txt (
    echo Installing requirements...
    call "%CONDA_BAT%" run -n %ENV_NAME% pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install requirements
        exit /b 1
    )
) else (
    echo requirements.txt not found, skipping
)
exit /b 0

:run_smoke
rem Запуск smoke test
echo Running smoke test...
call "%CONDA_BAT%" run -n %ENV_NAME% python broken_env.py
if errorlevel 1 (
    echo [ERROR] Smoke test failed
    exit /b 1
)
echo Smoke test passed
exit /b 0

:fail
echo [ERROR] Setup failed.
set "EXIT_CODE=1"
goto :finish

:ok
set "EXIT_CODE=0"
goto :finish

:finish
echo.
if not "%NO_PAUSE%"=="1" (
  echo Press any key to close...
  pause >nul
)
exit /b %EXIT_CODE%