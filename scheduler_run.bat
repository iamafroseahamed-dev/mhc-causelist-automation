@echo off
setlocal enabledelayedexpansion

set "WORKDIR=C:\Users\Afrose Ahamed\OneDrive\Desktop\mhc-causelist-automation"
set "PYEXE=C:\Users\Afrose Ahamed\AppData\Local\Programs\Python\Python312\python.exe"
set "LOGDIR=%WORKDIR%\logs"
set "LOGFILE=%LOGDIR%\run.log"
set "LOCKFILE=%LOGDIR%\scheduler_run.lock"

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

rem --- simple lock to avoid overlapping runs ---
if exist "%LOCKFILE%" (
  echo [%date% %time%] Another run seems to be in progress. Exiting.>>"%LOGFILE%" 2>&1
  exit /b 0
)

echo %date% %time%>"%LOCKFILE%"

cd /d "%WORKDIR%"

%PYEXE% "%WORKDIR%\main.py" >> "%LOGFILE%" 2>&1
set "RC=%ERRORLEVEL%"

del /q "%LOCKFILE%" >nul 2>&1
exit /b %RC%

