# UTF8
"""Gera Audit_horizontal_* só para Geral_linear."""
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

from pec_from_gpkg import (  # noqa: E402
    exit_standalone_qgis,
    init_standalone_qgis,
    load_result_layer,
)
from report_homologous_profiles_pdf import (  # noqa: E402
    DEFAULT_LINES_GPKG,
    _build_test_pair_index,
    _load_layer_cache,
    _models_from_result,
    _resolve_norm_type,
    _safe_filename_token,
    _scales_from_result,
    generate_horizontal_pdf,
)

RESULTS_DIR = os.path.join(_SCRIPT_DIR, 'Results', 'Geral_linear')


def main():
    gpkg = os.path.join(RESULTS_DIR, 'Result.gpkg')
    init_standalone_qgis()
    try:
        norm = _resolve_norm_type('auto', results_dir=RESULTS_DIR)
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        layer = load_result_layer(gpkg)
        print(f'Feições: {layer.featureCount()}')
        lines = _load_layer_cache(DEFAULT_LINES_GPKG)
        pair_index = _build_test_pair_index(lines)
        models = _models_from_result(layer)
        print(f'ts={ts} models={models}')
        for model in models:
            for scale in _scales_from_result(layer, model=model):
                out = os.path.join(
                    RESULTS_DIR,
                    f'Audit_horizontal_{_safe_filename_token(model)}_{int(scale)}_{ts}.pdf',
                )
                generate_horizontal_pdf(
                    gpkg_path=gpkg,
                    lines_gpkg=DEFAULT_LINES_GPKG,
                    out_path=out,
                    scale=int(scale),
                    model=model,
                    norm_type=norm,
                    result_layer=layer,
                    lines_cache=lines,
                    pair_index=pair_index,
                )
    finally:
        exit_standalone_qgis()


if __name__ == '__main__':
    main()
