# UTF8
"""Gera Audit_vertical_* com o nome padrão do plugin (3 métodos)."""
from __future__ import annotations

import os
import sys
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PLUGIN_ROOT = os.path.dirname(_SCRIPT_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
if _PLUGIN_ROOT not in sys.path:
    sys.path.insert(0, _PLUGIN_ROOT)

from pec_from_gpkg import exit_standalone_qgis, init_standalone_qgis  # noqa: E402
from report_homologous_profiles_pdf import (  # noqa: E402
    generate_pdfs_for_results_dir,
)

RESULTS_DIRS = [
    os.path.join(_SCRIPT_DIR, 'Results', 'Geral_linear'),
    os.path.join(_SCRIPT_DIR, 'Results', 'Geral_proximidade'),
    os.path.join(_SCRIPT_DIR, 'Results', 'Geral_sem_compatibilizacao'),
]


def main():
    init_standalone_qgis()
    try:
        ts_note = datetime.now().strftime('%Y-%m-%d_%H%M')
        print(f'Início: {ts_note}')
        for results_dir in RESULTS_DIRS:
            print(f'\n=== {results_dir} ===')
            generate_pdfs_for_results_dir(results_dir)
            # Remove nomes antigos Audit_perfis_*
            for name in os.listdir(results_dir):
                if name.startswith('Audit_perfis_') and name.endswith('.pdf'):
                    path = os.path.join(results_dir, name)
                    os.remove(path)
                    print(f'Removido antigo: {name}')
    finally:
        exit_standalone_qgis()


if __name__ == '__main__':
    main()
