@echo off
setlocal
cd /d "%~dp0"
title Jai Shree Ram

rem Automatically remove Windows download protection (Unblock) from all files in this folder
powershell -NoProfile -Command "Get-ChildItem -LiteralPath '%~dp0' -Recurse | Unblock-File" >nul 2>nul

where pyw >nul 2>nul
if not errorlevel 1 (
  set "RUNNER=pyw -3"
  goto :launch
)
where pythonw >nul 2>nul
if not errorlevel 1 (
  set "RUNNER=pythonw"
  goto :launch
)
where py >nul 2>nul
if not errorlevel 1 (
  set "RUNNER=py -3"
  goto :launch
)
set "RUNNER=python"

:launch
start "" %RUNNER% BreezeEasyApp.py
exit /b 0
