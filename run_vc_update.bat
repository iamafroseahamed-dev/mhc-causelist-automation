@echo off

cd /d "C:\Users\aahame425\OneDrive - Comcast\Desktop\Project Code Repositories\mhc-causelist-automation"

if not exist logs mkdir logs

set "LOG_FILE=logs\vc_refresh_%date:~-4%%date:~4,2%%date:~7,2%.log"

echo ============================================================>> "%LOG_FILE%"
echo VC launcher start: %date% %time%>> "%LOG_FILE%"

call .venv\Scripts\activate

set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

python update_vc_links.py >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo VC launcher end: %date% %time% exit_code=%EXIT_CODE%>> "%LOG_FILE%"

exit /b %EXIT_CODE%