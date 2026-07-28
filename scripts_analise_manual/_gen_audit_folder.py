# UTF8
"""Gera audits H+V dos três métodos para scripts_analise_manual/Audit/."""
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

from manual_paths import LINES_GPKG  # noqa: E402
from mods.mod_standalone_qgis import (  # noqa: E402
    exit_standalone_qgis,
    init_standalone_qgis,
    load_result_layer,
)
from mods.mod_gen_audit import (  # noqa: E402
    _build_test_pair_index,
    _load_layer_cache,
    _models_from_result,
    _norm_filename_slug,
    _norm_label,
    _resolve_norm_type,
    _safe_filename_token,
    _scales_from_result,
    generate_horizontal_pdf,
    generate_pdf,
)

OUT_DIR = os.path.join(_SCRIPT_DIR, 'Audit')
RESULTS_DIRS = [
    os.path.join(_SCRIPT_DIR, 'Results', 'Geral_linear'),
    os.path.join(_SCRIPT_DIR, 'Results', 'Geral_proximidade'),
    os.path.join(_SCRIPT_DIR, 'Results', 'Geral_sem_compatibilizacao'),
]


def _gen_vertical(results_dir, lines, pair_index, ts):
    gpkg = os.path.join(results_dir, 'Result.gpkg')
    norm = _resolve_norm_type('auto', results_dir=results_dir)
    slug = _norm_filename_slug(norm)
    print(f'\n=== VERTICAL  {os.path.basename(results_dir)}  |  {_norm_label(norm)} ===')
    layer = load_result_layer(gpkg)
    print(f'Feições: {layer.featureCount()}')
    for model in _models_from_result(layer):
        for scale in _scales_from_result(layer, model=model):
            out = os.path.join(
                OUT_DIR,
                f'Audit_vertical_{_safe_filename_token(model)}_{slug}_{int(scale)}_{ts}.pdf',
            )
            print(f'→ {os.path.basename(out)}')
            generate_pdf(
                gpkg_path=gpkg,
                lines_gpkg=LINES_GPKG,
                out_path=out,
                scale=int(scale),
                model=model,
                norm_type=norm,
                result_layer=layer,
                lines_cache=lines,
                pair_index=pair_index,
            )


def _gen_horizontal(results_dir, lines, pair_index, ts):
    """H uma vez (nome sem método); DM_H não depende da progressiva."""
    gpkg = os.path.join(results_dir, 'Result.gpkg')
    norm = _resolve_norm_type('auto', results_dir=results_dir)
    print(f'\n=== HORIZONTAL  {os.path.basename(results_dir)}  |  {_norm_label(norm)} ===')
    layer = load_result_layer(gpkg)
    print(f'Feições: {layer.featureCount()}')
    for model in _models_from_result(layer):
        for scale in _scales_from_result(layer, model=model):
            out = os.path.join(
                OUT_DIR,
                f'Audit_horizontal_{_safe_filename_token(model)}_{int(scale)}_{ts}.pdf',
            )
            print(f'→ {os.path.basename(out)}')
            generate_horizontal_pdf(
                gpkg_path=gpkg,
                lines_gpkg=LINES_GPKG,
                out_path=out,
                scale=int(scale),
                model=model,
                norm_type=norm,
                result_layer=layer,
                lines_cache=lines,
                pair_index=pair_index,
            )


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for name in os.listdir(OUT_DIR):
        if name.startswith('Audit_') and name.endswith('.pdf'):
            os.remove(os.path.join(OUT_DIR, name))
            print(f'Removido antigo: {name}')

    init_standalone_qgis()
    try:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        print(f'OUT={OUT_DIR}')
        print(f'ts={ts}')
        lines = _load_layer_cache(LINES_GPKG)
        pair_index = _build_test_pair_index(lines)
        print(f'Pares indexados: {len(pair_index)}')

        # Horizontal (independente do método) a partir de Geral_linear
        _gen_horizontal(RESULTS_DIRS[0], lines, pair_index, ts)

        # Vertical: linear, proximidade e sem compatibilização
        for results_dir in RESULTS_DIRS:
            _gen_vertical(results_dir, lines, pair_index, ts)

        print(f'\nConcluído. PDFs em: {OUT_DIR}')
    finally:
        exit_standalone_qgis()


if __name__ == '__main__':
    main()
