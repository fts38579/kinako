@echo off
setlocal
title Kawauso Manager Kinako - Build EXE
cd /d "%~dp0"

echo.
echo ============================================================
echo   Kawauso Manager Kinako - Build Start
echo ============================================================
echo.

echo [Step 0] Installing required libraries...
py -m pip install pyinstaller PyQt6 pyqtgraph matplotlib numpy pandas openpyxl ^
    selenium webdriver-manager TikTokLive beautifulsoup4 --quiet
if %ERRORLEVEL% neq 0 ( echo [ERROR] pip install failed & pause & exit /b 1 )
echo [OK] Libraries ready.
echo.

echo [Step 1/2] Building RELEASE EXE (--windowed, no console)...
py -m PyInstaller --onefile --windowed ^
  --name "kawauso-kinako" ^
  --distpath ".." ^
  --workpath "build" ^
  --specpath "." ^
  --hidden-import selenium.webdriver.chrome.webdriver ^
  --hidden-import selenium.webdriver.chrome.service ^
  --hidden-import selenium.webdriver.chrome.options ^
  --hidden-import webdriver_manager ^
  --hidden-import webdriver_manager.chrome ^
  --hidden-import pandas ^
  --hidden-import numpy ^
  --hidden-import openpyxl ^
  --hidden-import PyQt6 ^
  --hidden-import PyQt6.QtWidgets ^
  --hidden-import PyQt6.QtCore ^
  --hidden-import PyQt6.QtGui ^
  --hidden-import matplotlib ^
  --hidden-import asyncio ^
  --hidden-import multiprocessing ^
  --collect-all TikTokLive ^
  --collect-all selenium ^
  --collect-all webdriver_manager ^
  --collect-all bs4 ^
  --collect-all matplotlib ^
  --collect-all numpy ^
  --collect-all PyQt6 ^
  app.py
if %ERRORLEVEL% neq 0 ( echo [ERROR] Release build failed & pause & exit /b 1 )
echo [OK] Step 1/2 done.
echo.

echo [Step 2/2] Building DEBUG EXE (--console, with error output)...
py -m PyInstaller --onefile --console ^
  --name "kawauso-kinako-debug" ^
  --distpath ".." ^
  --workpath "build-debug" ^
  --specpath "." ^
  --hidden-import selenium.webdriver.chrome.webdriver ^
  --hidden-import selenium.webdriver.chrome.service ^
  --hidden-import selenium.webdriver.chrome.options ^
  --hidden-import webdriver_manager ^
  --hidden-import webdriver_manager.chrome ^
  --hidden-import pandas ^
  --hidden-import numpy ^
  --hidden-import openpyxl ^
  --hidden-import PyQt6 ^
  --hidden-import PyQt6.QtWidgets ^
  --hidden-import PyQt6.QtCore ^
  --hidden-import PyQt6.QtGui ^
  --hidden-import matplotlib ^
  --hidden-import asyncio ^
  --hidden-import multiprocessing ^
  --collect-all TikTokLive ^
  --collect-all selenium ^
  --collect-all webdriver_manager ^
  --collect-all bs4 ^
  --collect-all matplotlib ^
  --collect-all numpy ^
  --collect-all PyQt6 ^
  app.py
if %ERRORLEVEL% neq 0 ( echo [WARNING] Debug build failed (optional) )
echo [OK] Step 2/2 done.
echo.

echo ============================================================
echo   Build complete!
echo   [RELEASE] kawauso-kinako.exe       (project root)
echo   [DEBUG]   kawauso-kinako-debug.exe (project root, with console)
echo   ※ クラッシュする場合は kawauso-kinako-debug.exe で
echo     コンソール出力を確認してください
echo ============================================================
pause
endlocal
