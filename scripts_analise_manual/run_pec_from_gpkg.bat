@echo off
setlocal EnableExtensions
cd /d "%~dp0"

REM Calcula PEC/EP a partir de Result.gpkg (pec_from_gpkg.py).
REM Uso:
REM   run_pec_from_gpkg.bat
REM   run_pec_from_gpkg.bat "C:\caminho\para\Result.gpkg"
REM
REM Variaveis opcionais antes de executar:
REM   set QGIS_INSTALL=C:\Program Files\QGIS 3.34.9

if not defined QGIS_INSTALL (
  for /d %%D in ("%ProgramW6432%\QGIS 3.*" "%ProgramFiles%\QGIS 3.*" "%ProgramFiles(x86)%\QGIS 3.*") do (
    if exist "%%~fD\bin\python-qgis-ltr.bat" set "QGIS_INSTALL=%%~fD"
  )
)
if not defined QGIS_INSTALL (
  for /d %%D in ("%ProgramW6432%\QGIS 3.*" "%ProgramFiles%\QGIS 3.*" "%ProgramFiles(x86)%\QGIS 3.*") do (
    if exist "%%~fD\bin\o4w_env.bat" set "QGIS_INSTALL=%%~fD"
  )
)

if not defined QGIS_INSTALL (
  echo ERRO: QGIS nao encontrado.
  echo Defina QGIS_INSTALL, por exemplo:
  echo   set QGIS_INSTALL=C:\Program Files\QGIS 3.34.9
  exit /b 1
)

set "PYQGIS_BAT=%QGIS_INSTALL%\bin\python-qgis-ltr.bat"
if not exist "%PYQGIS_BAT%" set "PYQGIS_BAT=%QGIS_INSTALL%\bin\python-qgis.bat"
if not exist "%PYQGIS_BAT%" (
  echo ERRO: python-qgis-ltr.bat / python-qgis.bat nao encontrado em:
  echo   %QGIS_INSTALL%\bin
  exit /b 1
)

set "SCRIPT=%~dp0pec_from_gpkg.py"
if not exist "%SCRIPT%" (
  echo ERRO: script nao encontrado:
  echo   %SCRIPT%
  exit /b 1
)

if "%~1"=="" (
  set "GPKG_ARG="
) else (
  set "GPKG_ARG=%~1"
)
if "%~2"=="" (
  set "REF_GPKG_ARG="
) else (
  set "REF_GPKG_ARG=%~2"
)

echo.
echo QGIS_INSTALL=%QGIS_INSTALL%
echo Script=%SCRIPT%
if defined GPKG_ARG (
  echo GPKG=%GPKG_ARG%
) else (
  echo GPKG=padrao: Results\Geral_sem_compatibilizacao\Result.gpkg
)
if defined REF_GPKG_ARG (
  echo REF_GPKG=%REF_GPKG_ARG%
) else (
  echo REF_GPKG=padrao: Data\Selecao_v2_z.gpkg
)
echo.

if defined GPKG_ARG (
  if defined REF_GPKG_ARG (
    call "%PYQGIS_BAT%" "%SCRIPT%" "%GPKG_ARG%" "%REF_GPKG_ARG%"
  ) else (
    call "%PYQGIS_BAT%" "%SCRIPT%" "%GPKG_ARG%"
  )
) else (
  call "%PYQGIS_BAT%" "%SCRIPT%"
)

set "EXIT_CODE=%ERRORLEVEL%"
echo.
if %EXIT_CODE% equ 0 (
  echo OK: PEC/EP concluido.
) else (
  echo ERRO: pec_from_gpkg.py terminou com codigo %EXIT_CODE%.
)
exit /b %EXIT_CODE%
