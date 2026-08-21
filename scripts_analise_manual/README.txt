scripts_analise_manual — análise manual e dados de teste
========================================================

Pasta versionada com dados de teste (Data/, Results/, Audit/) e scripts de batch
para validação fora do painel QGIS.

O plugin NÃO depende desta pasta. Constantes PEC, auditoria PDF e QGIS headless
estão em mods/:
  mod_pec_constants.py      DIC_PEC_MM, DIC_EQ_*, DIC_PEC_V (=EQ×coef), …
  mod_standalone_qgis.py    init/exit QGIS, load_result_layer
  mod_gen_audit.py          gerador de PDFs de auditoria (+ CLI: python -m mods.mod_gen_audit)

Estrutura
---------

  Data/              GPKG de referência (ex.: Selecao_v2_z.gpkg)
  Results/
    Geral_linear/              método linear (antes Geral_scale)
    Geral_proximidade/         método por proximidade (antes Geral_less_dist)
    Geral_sem_compatibilizacao/  sem compatibilização (antes Geral_none)
  Audit/             PDFs de auditoria gerados em lote (_gen_audit_folder.py)
  manual_paths.py    paths locais (LINES_GPKG, RESULTS_*, …)

Scripts nesta pasta (no git)
----------------------------

  pec_from_gpkg.py           PEC/EP a partir de Result.gpkg (importa de mods/)
  pec_master_buffer_duplo.py legado (QGIS com projeto aberto)
  rebuild_pec_buffers.py     recria Result.gpkg + Profile_*.csv
  _gen_audit_folder.py       lote H+V → Audit/
  run_pec_from_gpkg.bat      atalho para pec_from_gpkg.py

Empacotamento do plugin (ZIP plugins.qgis.org)
----------------------------------------------

  Na raiz do repositório:  py -3 package_plugin.py
  Saída: dist/pos_accuracy-<versão>.zip (só o runtime). Este é o único gerador oficial.

Scripts em scripts_aux/ (local, gitignored)
-------------------------------------------

  _gen_audit_h_scale.py, _gen_audit_v_standard.py
  run_gen_audit.bat          CLI → python -m mods.mod_gen_audit
  export_report_*.py, run_export_report_*.bat

Aliases de método (CLI e inferência por pasta)
----------------------------------------------

  linear / scale / 0              → Geral_linear
  proximidade / less_dist / 1     → Geral_proximidade
  sem_compatibilizacao / none / 2 → Geral_sem_compatibilizacao

Nomes antigos de pasta (Geral_scale, Geral_less_dist, Geral_none) ainda são
reconhecidos ao inferir o método a partir do nome da pasta Results/.
