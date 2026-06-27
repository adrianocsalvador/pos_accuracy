@echo off
setlocal EnableExtensions
cd /d "%~dp0"

REM Uso:
REM   build_translations.bat          sync .ts + gera .qm
REM   build_translations.bat qm-only  so gera .qm
REM   build_translations.bat pylupdate  (legado; nao recomendado)
set "QM_ONLY=0"
set "USE_PYLUPDATE=0"
if /i "%~1"=="qm-only" set "QM_ONLY=1"
if /i "%~1"=="pylupdate" set "USE_PYLUPDATE=1"

REM Pasta do QGIS: defina QGIS_INSTALL se nao estiver em "Program Files\QGIS 3.*"
REM NUNCA chame OSGeo4W.bat da raiz aqui: abre shell interativa e interrompe este script.
REM ProgramW6432 = Program Files 64-bit (cmd 32-bit usa ProgramFiles errado)
if not defined QGIS_INSTALL (
  for /d %%D in ("%ProgramW6432%\QGIS 3.*" "%ProgramFiles%\QGIS 3.*" "%ProgramFiles(x86)%\QGIS 3.*") do (
    if exist "%%~fD\bin\qgis.exe" set "QGIS_INSTALL=%%~fD"
  )
)
if not defined QGIS_INSTALL (
  for /d %%D in ("%ProgramW6432%\QGIS 3.*" "%ProgramFiles%\QGIS 3.*" "%ProgramFiles(x86)%\QGIS 3.*") do (
    if exist "%%~fD\bin\o4w_env.bat" set "QGIS_INSTALL=%%~fD"
  )
)

REM Atualiza PATH (pylupdate); lrelease usa caminho absoluto em :find_lrelease
if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\bin\o4w_env.bat" (
  call "%QGIS_INSTALL%\bin\o4w_env.bat"
  cd /d "%~dp0"
)

set "PYLU="
if "%QM_ONLY%"=="1" goto :find_lr_entry
if not "%USE_PYLUPDATE%"=="1" goto :find_lr_entry
for /f "delims=" %%A in ('where pylupdate5 2^>nul') do set "PYLU=pylupdate5" & goto :py_ok
for /f "delims=" %%A in ('where pylupdate6 2^>nul') do set "PYLU=pylupdate6" & goto :py_ok
echo ERRO: pylupdate5/pylupdate6 nao encontrado no PATH.
echo Defina QGIS_INSTALL para a pasta do QGIS ^(ex.: C:\Program Files\QGIS 3.34.9^) e volte a correr.
if defined QGIS_INSTALL echo Ou confirme: "%QGIS_INSTALL%\bin\o4w_env.bat"
exit /b 1
:py_ok

:find_lr_entry
call :find_lrelease
if errorlevel 1 exit /b 1

if "%QM_ONLY%"=="1" goto :do_lrelease

if "%USE_PYLUPDATE%"=="1" goto :run_pylupdate_legacy

echo.
echo === sync_translations.py (contexto unico PositionalAccuracyPlugin) ===
call :run_sync_translations
if errorlevel 1 (
  echo ERRO: sync_translations.py falhou.
  exit /b 1
)
if not exist pos_accuracy_en.ts (
  echo ERRO: pos_accuracy_en.ts nao gerado.
  exit /b 1
)
if not exist pos_accuracy_es_ES.ts (
  echo ERRO: pos_accuracy_es_ES.ts nao gerado.
  exit /b 1
)

goto :do_lrelease

:run_pylupdate_legacy
echo.
echo === %PYLU% -noobsolete pos_accuracy.pro ===
%PYLU% -noobsolete pos_accuracy.pro
if errorlevel 1 (
  echo ERRO: pylupdate falhou.
  exit /b 1
)

:do_lrelease

echo.
echo === "%LRELEASE%" pos_accuracy_en.ts -^> pos_accuracy_en.qm ===
"%LRELEASE%" pos_accuracy_en.ts -qm pos_accuracy_en.qm
if errorlevel 1 (
  echo ERRO: lrelease falhou - en.
  exit /b 1
)

echo.
echo === "%LRELEASE%" pos_accuracy_es_ES.ts -^> pos_accuracy_es_ES.qm ===
"%LRELEASE%" pos_accuracy_es_ES.ts -qm pos_accuracy_es_ES.qm
if errorlevel 1 (
  echo ERRO: lrelease falhou - es_ES.
  exit /b 1
)

echo.
echo OK: pos_accuracy_en e pos_accuracy_es_ES - ficheiros .ts e .qm atualizados.
exit /b 0


:run_sync_translations
if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" (
  "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" "%~dp0sync_translations.py"
  exit /b %ERRORLEVEL%
)
if exist "%QGIS_INSTALL%\apps\Python312\python.exe" (
  "%QGIS_INSTALL%\apps\Python312\python.exe" "%~dp0sync_translations.py"
  exit /b %ERRORLEVEL%
)
where py >nul 2>&1 && py -3.12 "%~dp0sync_translations.py" && exit /b 0
where python >nul 2>&1 && python "%~dp0sync_translations.py" && exit /b 0
echo ERRO: Python nao encontrado para sync_translations.py
exit /b 1


:find_lrelease
if defined LRELEASE if exist "%LRELEASE%" exit /b 0
set "LRELEASE="
for /f "delims=" %%A in ('where lrelease 2^>nul') do (
  set "LRELEASE=%%A"
  goto :lr_done
)
if defined OSGEO4W_ROOT (
  if exist "%OSGEO4W_ROOT%\apps\Qt5\bin\lrelease.exe" set "LRELEASE=%OSGEO4W_ROOT%\apps\Qt5\bin\lrelease.exe"
)
if not defined LRELEASE if defined OSGEO4W_ROOT (
  if exist "%OSGEO4W_ROOT%\apps\Qt6\bin\lrelease.exe" set "LRELEASE=%OSGEO4W_ROOT%\apps\Qt6\bin\lrelease.exe"
)
if not defined LRELEASE if defined OSGEO4W_ROOT (
  if exist "%OSGEO4W_ROOT%\bin\lrelease.exe" set "LRELEASE=%OSGEO4W_ROOT%\bin\lrelease.exe"
)
if not defined LRELEASE if defined QGIS_INSTALL (
  if exist "%QGIS_INSTALL%\apps\Qt5\bin\lrelease.exe" set "LRELEASE=%QGIS_INSTALL%\apps\Qt5\bin\lrelease.exe"
)
if not defined LRELEASE if defined QGIS_INSTALL (
  if exist "%QGIS_INSTALL%\apps\Qt6\bin\lrelease.exe" set "LRELEASE=%QGIS_INSTALL%\apps\Qt6\bin\lrelease.exe"
)
if not defined LRELEASE if defined QGIS_INSTALL (
  if exist "%QGIS_INSTALL%\bin\lrelease.exe" set "LRELEASE=%QGIS_INSTALL%\bin\lrelease.exe"
)
if not defined LRELEASE if defined QGIS_INSTALL (
  for /f "delims=" %%F in ('where /r "%QGIS_INSTALL%" lrelease.exe 2^>nul') do (
    set "LRELEASE=%%F"
    goto :lr_done
  )
)
if not defined LRELEASE if defined QGIS_INSTALL (
  for /f "delims=" %%F in ('where /r "%QGIS_INSTALL%" lrelease-qt5.exe 2^>nul') do (
    set "LRELEASE=%%F"
    goto :lr_done
  )
)
if not defined LRELEASE (
  for /f "delims=" %%P in ('where %PYLU% 2^>nul') do (
    for %%I in ("%%~dpP..\apps\Qt5\bin\lrelease.exe") do if exist "%%~fI" set "LRELEASE=%%~fI"
    if not defined LRELEASE for %%I in ("%%~dpP..\..\Qt5\bin\lrelease.exe") do if exist "%%~fI" set "LRELEASE=%%~fI"
    if not defined LRELEASE for %%I in ("%%~dpP..\apps\Qt6\bin\lrelease.exe") do if exist "%%~fI" set "LRELEASE=%%~fI"
    if not defined LRELEASE for %%I in ("%%~dpP..\..\Qt6\bin\lrelease.exe") do if exist "%%~fI" set "LRELEASE=%%~fI"
  )
)
REM QGIS standalone normalmente NAO inclui lrelease; fallback PySide6 (pip install --user PySide6)
if not defined LRELEASE for /f "delims=" %%A in ('where pyside6-lrelease 2^>nul') do (
  set "LRELEASE=%%A"
  goto :lr_done
)
if not defined LRELEASE if exist "%APPDATA%\Python\Python312\Scripts\pyside6-lrelease.exe" set "LRELEASE=%APPDATA%\Python\Python312\Scripts\pyside6-lrelease.exe"
if not defined LRELEASE if exist "%LOCALAPPDATA%\Programs\Python\Python312\Scripts\pyside6-lrelease.exe" set "LRELEASE=%LOCALAPPDATA%\Programs\Python\Python312\Scripts\pyside6-lrelease.exe"
if not defined LRELEASE if exist "%APPDATA%\Python\Python311\Scripts\pyside6-lrelease.exe" set "LRELEASE=%APPDATA%\Python\Python311\Scripts\pyside6-lrelease.exe"
if not defined LRELEASE if exist "%LOCALAPPDATA%\Programs\Python\Python311\Scripts\pyside6-lrelease.exe" set "LRELEASE=%LOCALAPPDATA%\Programs\Python\Python311\Scripts\pyside6-lrelease.exe"
REM Qt do QGIS via QLibraryInfo (where /r pode falhar; algumas builds nao trazem lrelease no disco)
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python312\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python312\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python311\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python311\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python310\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python310\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python39\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python39\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python38\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python38\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
:lr_done
if not defined LRELEASE (
  echo ERRO: lrelease.exe nao encontrado.
  if defined QGIS_INSTALL echo QGIS_INSTALL=%QGIS_INSTALL%
  echo.
  echo O instalador QGIS normalmente nao inclui lrelease. Opcoes:
  echo   1^) py -3.12 -m pip install --user PySide6
  echo      ^(usa pyside6-lrelease em %%APPDATA%%\Python\Python312\Scripts^)
  echo   2^) Defina LRELEASE=caminho\para\lrelease.exe antes de correr este .bat
  echo   3^) Instale Qt Linguist / Qt SDK com ferramentas de traducao
  if not defined QGIS_INSTALL echo Defina tambem: set QGIS_INSTALL=C:\Program Files\QGIS 3.34.9
  exit /b 1
)
exit /b 0
