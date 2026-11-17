@echo off
cd /d "%~dp0"
hosting\runtime\node.exe hosting\launch.mjs
if errorlevel 1 pause
