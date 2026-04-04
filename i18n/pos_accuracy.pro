# Atualizar ficheiros .ts a partir do código Python (PyQt5 / QGIS):
#   cd i18n
#   pylupdate5 pos_accuracy.pro
#
# Editar traduções no Qt Linguist, depois gerar .qm:
#   lrelease pos_accuracy_en.ts -qm pos_accuracy_en.qm

CODECFORTR = UTF-8

SOURCES = ../mods/mod_mde_positional_accuracy.py \
          ../mods/mod_settings.py

TRANSLATIONS = pos_accuracy_en.ts
