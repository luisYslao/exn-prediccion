@echo off
cd /d C:\ruta\a\tu\proyecto

call .venv\Scripts\activate
python src\train_service.py

pause