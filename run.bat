@echo off

cd /d "C:\Users\aahame425\OneDrive - Comcast\Desktop\Project Code Repositories\mhc-causelist-automation"

if not exist logs mkdir logs

call .venv\Scripts\activate

python mhc_causelist_automation.py >> logs\cause_list_%date:~-4%%date:~4,2%%date:~7,2%.log 2>&1