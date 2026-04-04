# -*- coding: utf-8 -*-
"""Contexto e função de tradução partilhados (evita import circular entre módulos do plugin)."""
from qgis.PyQt.QtCore import QCoreApplication

# Deve coincidir com <name> no ficheiro .ts do Qt Linguist
PLUGIN_I18N_CONTEXT = 'MDEPositionalAccuracy'


def tr_ui(text: str) -> str:
    return QCoreApplication.translate(PLUGIN_I18N_CONTEXT, text)
