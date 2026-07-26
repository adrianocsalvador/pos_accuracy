@echo off
setlocal EnableExtensions
cd /d "%~dp0"

REM Gera PDF de pares homologos (vista 2D + perfis por classe).
REM Uso:
REM   run_homologous_profiles_pdf.bat
REM   run_homologous_profiles_pdf.bat "C:\caminho\Result.gpkg"
REM   run_homologous_profiles_pdf.bat "Result.gpkg" --scale 50 --limit 5
REM
REM Variaveis opcionais:
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

set "SCRIPT=%~dp0report_homologous_profiles_pdf.py"
if not exist "%SCRIPT%" (
  echo ERRO: script nao encontrado:
  echo   %SCRIPT%
  exit /b 1
)

echo.
echo QGIS_INSTALL=%QGIS_INSTALL%
echo Script=%SCRIPT%
echo.

call "%PYQGIS_BAT%" "%SCRIPT%" %*
set "EXIT_CODE=%ERRORLEVEL%"
echo.
if %EXIT_CODE% equ 0 (
  echo OK: PDF de pares homologos concluido.
) else (
  echo ERRO: report_homologous_profiles_pdf.py terminou com codigo %EXIT_CODE%.
)
exit /b %EXIT_CODE%
