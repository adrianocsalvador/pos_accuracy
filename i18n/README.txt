Traduções do plugin AP Acuracia Posicional (textos fonte em pt_BR; traduções via Qt Linguist)
==========================================

Editor (VS Code / Cursor): ficheiros em i18n/*.ts são XML Qt, não TypeScript.
O repositório inclui .vscode/settings.json para associar **/i18n/*.ts ao formato XML.

Contexto Qt (obrigatório em todos os .ts): PositionalAccuracyPlugin

No código Python:
  - tr_ui("texto") ou self.tr("texto") no painel (Wd1) — strings visíveis em pt_BR por defeito
  - self.tr("texto") na classe PositionalAccuracyPlugin (menu, etc.)

O QTranslator carrega pos_accuracy_<locale>.qm a partir da pasta i18n/
(p.ex. pos_accuracy_en.qm para inglês, pos_accuracy_pt_BR.qm para português do Brasil).

Fluxo recomendado
-----------------
1) Instalar ferramentas Qt (pylupdate5 e lrelease), ou usar as do OSGeo4W / instalador QGIS.

2) Após alterar strings traduzíveis:
     cd i18n
     pylupdate5 pos_accuracy.pro

3) Abrir o(s) ficheiro(s) .ts no Qt Linguist, preencher traduções e gravar.

4) Gerar binários .qm:
     lrelease pos_accuracy_en.ts -qm pos_accuracy_en.qm

5) Colocar o .qm na pasta i18n/ e recarregar o plugin.

Nota: Se não existir .qm para o idioma do QGIS, as mensagens em português do código
continuam visíveis (idioma de partida).
