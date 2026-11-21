@echo off
call .venv\Scripts\activate.bat
python -m unittest discover -s tests -v
