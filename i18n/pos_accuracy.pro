# Fluxo principal: build_translations.bat (sync_translations.py + lrelease).
# pylupdate5 abaixo e legado / fallback; sync gera pos_accuracy_en.ts e pos_accuracy_es_ES.ts.

CODECFORTR = UTF-8

SOURCES = ../mods/mod_positional_accuracy.py \
          ../mods/mod_settings.py \
          ../mods/mod_language_dlg.py

TRANSLATIONS = pos_accuracy_en.ts \
               pos_accuracy_es_ES.ts
