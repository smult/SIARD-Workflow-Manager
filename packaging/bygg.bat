@echo off
REM Bygger ferdig programpakke (zip) i dist\ - se packaging\build_release.py
REM   bygg.bat                       alle komponenter (LibreOffice, Ghostscript, Siegfried)
REM   bygg.bat --komponenter ingen   bare programmet
cd /d "%~dp0.."
python packaging\build_release.py %*
pause
