@echo off
echo Installerer avhengigheter...
pip install customtkinter pyinstaller
echo.
echo Starter SIARD Workflow Manager...
python main.py
pause
