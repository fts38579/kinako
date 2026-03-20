@echo off
setlocal
title カワウソマネージャー きなこ - ビルド
cd /d "%~dp0"

echo.
echo ============================================================
echo   カワウソマネージャー きなこ - EXE ビルド
echo ============================================================
echo.

echo [Step 1] 必要なライブラリをインストール中...
py -m pip install pyinstaller PyQt6 pyqtgraph matplotlib numpy pandas openpyxl selenium webdriver-manager TikTokLive beautifulsoup4 --quiet
if %ERRORLEVEL% neq 0 ( echo [ERROR] pip install に失敗しました & pause & exit /b 1 )
echo [OK] ライブラリ準備完了
echo.

echo [Step 2] EXE をビルド中... (数分かかります)
py -m PyInstaller --onefile --windowed --name "kawauso-kinako" --distpath ".." --workpath "build" --specpath "." --hidden-import selenium.webdriver.chrome.webdriver --hidden-import selenium.webdriver.chrome.service --hidden-import selenium.webdriver.chrome.options --hidden-import webdriver_manager --hidden-import webdriver_manager.chrome --hidden-import pandas --hidden-import numpy --hidden-import openpyxl --hidden-import PyQt6 --hidden-import PyQt6.QtWidgets --hidden-import PyQt6.QtCore --hidden-import PyQt6.QtGui --hidden-import matplotlib --hidden-import asyncio --hidden-import multiprocessing --collect-all TikTokLive --collect-all selenium --collect-all webdriver_manager --collect-all bs4 --collect-all matplotlib --collect-all numpy --collect-all PyQt6 app.py
if %ERRORLEVEL% neq 0 ( echo [ERROR] ビルドに失敗しました & pause & exit /b 1 )
echo.

echo ============================================================
echo   ビルド完了！
echo   kawauso-kinako.exe をプロジェクトフォルダに配置しました
echo ============================================================
pause
endlocal
