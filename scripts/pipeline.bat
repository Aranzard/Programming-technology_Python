@echo off
echo EARTHQUAKE PIPELINE
echo.
echo Select mode:
echo   1 - FULL (replace all data)
echo   2 - INCREMENTAL (add new data only)
echo.
set /p mode="Enter 1 or 2: "

cd /d D:\ULGU\Programming-technology\Python_2seminar\Programming-technology_Python

if "%mode%"=="1" (
    echo.
    echo [INFO] Running FULL pipeline...
    python src/pipeline.py --mode full
) else if "%mode%"=="2" (
    echo.
    echo [INFO] Running INCREMENTAL pipeline...
    python src/pipeline.py --mode incremental
) else (
    echo [ERROR] Invalid choice. Please run again.
    pause
    exit /b 1
)
    echo.
if %errorlevel% equ 0 (
    echo [SUCCESS] Pipeline completed!
) else (
    echo [ERROR] Pipeline failed with code %errorlevel%
)

echo.
pause