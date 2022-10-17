echo off
chcp 65001

cd %~dp0
call "venv\Scripts\activate.bat"

cd %~dp0
TITLE OmsDBChecker
python OmsDBChecker.py

pause