@echo off
setlocal EnableExtensions
cd /d "%~dp0"

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
for /f "delims=" %%A in ('where pylupdate5 2^>nul') do set "PYLU=pylupdate5" & goto :py_ok
for /f "delims=" %%A in ('where pylupdate6 2^>nul') do set "PYLU=pylupdate6" & goto :py_ok
echo ERRO: pylupdate5/pylupdate6 nao encontrado no PATH.
echo Defina QGIS_INSTALL para a pasta do QGIS ^(ex.: C:\Program Files\QGIS 3.34.9^) e volte a correr.
if defined QGIS_INSTALL echo Ou confirme: "%QGIS_INSTALL%\bin\o4w_env.bat"
exit /b 1
:py_ok

call :find_lrelease
if errorlevel 1 exit /b 1

echo.
echo === %PYLU% pos_accuracy.pro ===
%PYLU% pos_accuracy.pro
if errorlevel 1 (
  echo ERRO: pylupdate falhou.
  exit /b 1
)

echo.
echo === "%LRELEASE%" pos_accuracy_en.ts -^> pos_accuracy_en.qm ===
"%LRELEASE%" pos_accuracy_en.ts -qm pos_accuracy_en.qm
if errorlevel 1 (
  echo ERRO: lrelease falhou.
  exit /b 1
)

echo.
echo OK: pos_accuracy_en.ts atualizado e pos_accuracy_en.qm gerado.
exit /b 0


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
    goto :lr_done
  )
)
REM Qt do QGIS via QLibraryInfo (where /r pode falhar; algumas builds nao trazem lrelease no disco)
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python312\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python312\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python311\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python311\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python310\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python310\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python39\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python39\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
if not defined LRELEASE if defined QGIS_INSTALL if exist "%QGIS_INSTALL%\apps\Python38\python.exe" for /f "delims=" %%F in ('"%QGIS_INSTALL%\apps\Python38\python.exe" "%~dp0find_lrelease_via_qgis.py" "%QGIS_INSTALL%" 2^>nul') do set "LRELEASE=%%F"
:lr_done
if not defined LRELEASE (
  echo ERRO: lrelease.exe nao encontrado.
  if defined QGIS_INSTALL (
    echo QGIS_INSTALL=%QGIS_INSTALL%
    echo Instale o QGIS completo ou Qt tools; ou defina LRELEASE=caminho\para\lrelease.exe antes de correr este .bat
  ) else (
    echo Defina: set QGIS_INSTALL=C:\Program Files\QGIS 3.34.9
  )
  exit /b 1
)
exit /b 0
