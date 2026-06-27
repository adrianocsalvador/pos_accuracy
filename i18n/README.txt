Traduções do plugin MDE AP - Acurácia Posicional (textos fonte em pt_BR; traduções via Qt Linguist)
==========================================

Editor (VS Code / Cursor): ficheiros em i18n/*.ts são XML Qt, não TypeScript.
O repositório inclui .vscode/settings.json para associar **/i18n/*.ts ao formato XML.

Contexto Qt (obrigatório em todos os .ts): PositionalAccuracyPlugin

No código Python:
  - tr_ui("texto") ou self.tr("texto") no painel (Wd1) — strings visíveis em pt_BR por defeito
  - self.tr("texto") na classe PositionalAccuracyPlugin (menu, etc.)

O QTranslator carrega pos_accuracy_<locale>.qm a partir da pasta i18n/
(p.ex. pos_accuracy_en.qm para inglês, pos_accuracy_pt_BR.qm para português do Brasil).
Idioma de desenvolvimento (textos fonte no código): pt_BR (não usar «pt» genérico).

Fluxo recomendado
-----------------
1) Atualizar .ts e gerar .qm (recomendado):
     cd i18n
     build_translations.bat

   O script sync_translations.py extrai strings do Python para um unico
   contexto PositionalAccuracyPlugin (evita problema do pylupdate com Wd1).

2) So recompilar .qm apos editar traducoes no Qt Linguist:
     build_translations.bat qm-only

3) lrelease: QGIS standalone muitas vezes nao o inclui. O .bat procura
   pyside6-lrelease (py -3.12 -m pip install --user PySide6) ou lrelease do OSGeo4W.

4) Recarregar o plugin no QGIS apos gerar pos_accuracy_en.qm.

Nota: Se não existir .qm para o idioma do QGIS, as mensagens em português do código
continuam visíveis (idioma de partida).
