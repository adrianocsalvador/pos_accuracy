# UTF8
"""Paths locais da análise manual (dados e Results/ em scripts_analise_manual/)."""

from __future__ import annotations

import os

_MANUAL_DIR = os.path.dirname(os.path.abspath(__file__))
PLUGIN_ROOT = os.path.dirname(_MANUAL_DIR)

DATA_DIR = os.path.join(_MANUAL_DIR, 'Data')
RESULTS_DIR = os.path.join(_MANUAL_DIR, 'Results')
AUDIT_DIR = os.path.join(_MANUAL_DIR, 'Audit')

LINES_GPKG = os.path.join(DATA_DIR, 'Selecao_v2_z.gpkg')

RESULTS_LINEAR = os.path.join(RESULTS_DIR, 'Geral_linear')
RESULTS_PROXIMIDADE = os.path.join(RESULTS_DIR, 'Geral_proximidade')
RESULTS_SEM_COMPAT = os.path.join(RESULTS_DIR, 'Geral_sem_compatibilizacao')

DEFAULT_RESULT_GPKG = os.path.join(RESULTS_SEM_COMPAT, 'Result.gpkg')
DEFAULT_RESULT_GPKG_LINEAR = os.path.join(RESULTS_LINEAR, 'Result.gpkg')

DEFAULT_AUDIT_VERTICAL_SMOKE = os.path.join(
    RESULTS_LINEAR, 'Audit_vertical_smoke.pdf'
)
DEFAULT_AUDIT_HORIZONTAL_SMOKE = os.path.join(
    RESULTS_LINEAR, 'Audit_horizontal_smoke.pdf'
)

RESULTS_DIRS_ALL = [RESULTS_LINEAR, RESULTS_PROXIMIDADE, RESULTS_SEM_COMPAT]
