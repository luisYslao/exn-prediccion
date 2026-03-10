@echo off
cd /d C:\ruta\a\tu\proyecto

call .venv\Scripts\activate
python src\scheduler_service.py

pause