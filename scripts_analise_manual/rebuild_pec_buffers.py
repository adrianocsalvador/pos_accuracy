# UTF8
"""
Recalcula buffers PEC (Result.gpkg + Profile_*.csv) em modo standalone.

Usa build_compatibilized_profile_geometries (plugin) — less_dist sem inversão.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import statistics
import time
from itertools import zip_longest

from PyQt5.QtCore import QVariant
from qgis.core import (
    QgsFeature,
    QgsField,
    QgsFields,
    QgsGeometry,
    QgsPointXY,
    QgsProject,
    QgsSpatialIndex,
    QgsVectorFileWriter,
    QgsVectorLayer,
    QgsWkbTypes,
)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PLUGIN_ROOT = os.path.dirname(_SCRIPT_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
if _PLUGIN_ROOT not in sys.path:
    sys.path.insert(0, _PLUGIN_ROOT)

from mods.mod_worker_threads import (  # noqa: E402
    PROFILE_PROG_OFFSET,
    _profile_line_points,
    build_compatibilized_profile_geometries,
    orient_line_high_to_low,
)
from manual_paths import LINES_GPKG  # noqa: E402
from mods.mod_pec_constants import (  # noqa: E402
    DIC_NAME_LAYER,
    DIC_PEC_MM,
    DIC_PEC_V,
    NORM_BY_METHOD,
    RESULTS_FOLDER_BY_METHOD,
)
from mods.mod_standalone_qgis import (  # noqa: E402
    exit_standalone_qgis,
    init_standalone_qgis,
)

DEFAULT_LINES_GPKG = LINES_GPKG
BUFFER_SEGMENTS = 20


def _load_named_layers(gpkg_path):
    meta = QgsVectorLayer(gpkg_path)
    if not meta.isValid():
        raise RuntimeError(f'GPKG inválido:\n{gpkg_path}')
    layers = {}
    for sub in meta.dataProvider().subLayers():
        parts = sub.split('!!::!!')
        if len(parts) < 2:
            continue
        lname = parts[1]
        layer = QgsVectorLayer(f'{gpkg_path}|layername={lname}', lname, 'ogr')
        if layer.isValid():
            layers[lname] = layer
    return layers


def _create_memory_result_layer(crs_authid):
    """Camada em memória; o GPKG só é escrito no fim (OGR commit é pouco fiável)."""
    prefix = '__Buffer_Test__'
    mem = QgsVectorLayer(f'polygon?crs={crs_authid}&index=yes', prefix, 'memory')
    schema = QgsFields()
    schema.append(QgsField('id_ref', QVariant.Int))
    schema.append(QgsField('scale', QVariant.Int))
    schema.append(QgsField('class', QVariant.String))
    schema.append(QgsField('layer_ref', QVariant.String))
    schema.append(QgsField('Test_name', QVariant.String))
    schema.append(QgsField('Area_Test', QVariant.Double))
    schema.append(QgsField('Area_Ref', QVariant.Double))
    schema.append(QgsField('Area_Inter', QVariant.Double))
    schema.append(QgsField('DM_H', QVariant.Double))
    schema.append(QgsField('OUT_H', QVariant.Bool))
    schema.append(QgsField('Area_Test_Prof', QVariant.Double))
    schema.append(QgsField('Area_Ref_Prof', QVariant.Double))
    schema.append(QgsField('Area_Inter_Prof', QVariant.Double))
    schema.append(QgsField('DM_V', QVariant.Double))
    schema.append(QgsField('OUT_V', QVariant.Bool))
    schema.append(QgsField('Cota_Media_r', QVariant.Double))
    schema.append(QgsField('Cota_Media_t', QVariant.Double))
    mem.dataProvider().addAttributes(schema)
    mem.updateFields()
    return mem


def _write_result_gpkg(mem_layer, gpkg_path):
    """Grava via ficheiro temporário (evita WinError 32 se o GPKG estiver aberto no QGIS)."""
    os.makedirs(os.path.dirname(gpkg_path) or '.', exist_ok=True)
    tmp_path = gpkg_path + '.__new__'
    if os.path.isfile(tmp_path):
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteFile
    opts.driverName = 'GPKG'
    opts.layerName = '__Buffer_Test__'
    err = QgsVectorFileWriter.writeAsVectorFormat(
        layer=mem_layer, fileName=tmp_path, options=opts
    )
    code = err[0] if isinstance(err, tuple) else err
    if code != QgsVectorFileWriter.NoError:
        msg = err[1] if isinstance(err, tuple) and len(err) > 1 else err
        raise RuntimeError(f'Falha ao gravar {tmp_path}: {msg}')
    check = QgsVectorLayer(f'{tmp_path}|layername=__Buffer_Test__', 'chk', 'ogr')
    if not check.isValid() or check.featureCount() != mem_layer.featureCount():
        raise RuntimeError(
            f'GPKG incompleto: mem={mem_layer.featureCount()} '
            f'gpkg={check.featureCount() if check.isValid() else "inválido"}'
        )
    # Substitui o destino (com retries se outro processo tiver o ficheiro aberto)
    last_err = None
    for attempt in range(1, 6):
        try:
            if os.path.isfile(gpkg_path):
                os.remove(gpkg_path)
            os.replace(tmp_path, gpkg_path)
            return gpkg_path
        except OSError as exc:
            last_err = exc
            time.sleep(min(1.5 * attempt, 8.0))
    # Fallback: grava Result_rebuild.gpkg (QGIS costuma ter Result.gpkg aberto)
    fallback = os.path.join(os.path.dirname(gpkg_path) or '.', 'Result_rebuild.gpkg')
    if os.path.isfile(fallback):
        try:
            os.remove(fallback)
        except OSError:
            pass
    os.replace(tmp_path, fallback)
    print(
        f'AVISO: {gpkg_path} em uso ({last_err}).\n'
        f'       Gravado em: {fallback}\n'
        f'       Feche a camada no QGIS e substitua manualmente se necessário.',
        flush=True,
    )
    return fallback


def _mean_z(geom):
    pts = _profile_line_points(geom)
    zs = [p.z() for p in pts if math.isfinite(p.z())]
    return statistics.mean(zs) if zs else float('nan')


def _write_profile_csv(path, layer_ref, id_ref, len_r, len_t, cm_r, cm_t, geom_prof_r, geom_prof_t):
    def _xy(geom):
        if geom is None or geom.isEmpty():
            return []
        g = QgsGeometry(geom)
        try:
            pts = g.asMultiPolyline()[0] if g.isMultipart() else g.asPolyline()
        except Exception:
            return []
        return [(p.x(), p.y()) for p in pts]

    rows_r = _xy(geom_prof_r)
    rows_t = _xy(geom_prof_t)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(
            f'\n {layer_ref} - {id_ref} | len(r) = {len_r} | len(t) {len_t}\n'
            f'Cota_Media_r {cm_r} | Cota_Media_t {cm_t}\n'
        )
        for r_, t_ in zip_longest(rows_r, rows_t):
            f.write(
                f'{round(r_[0], 2) if r_ else ""}; {round(r_[1], 2) if r_ else ""}; '
                f'{round(t_[0], 2) if t_ else ""}; {round(t_[1], 2) if t_ else ""}; \n'
            )


def rebuild(
    method='less_dist',
    lines_gpkg=DEFAULT_LINES_GPKG,
    results_dir=None,
):
    key = str(method).strip().lower()
    if key not in NORM_BY_METHOD:
        raise RuntimeError(f'method inválido: {method}')
    norm_type = NORM_BY_METHOD[key]
    folder = RESULTS_FOLDER_BY_METHOD[norm_type]
    if results_dir is None:
        results_dir = os.path.join(_SCRIPT_DIR, 'Results', folder)
    gpkg_out = os.path.join(results_dir, 'Result.gpkg')
    os.makedirs(results_dir, exist_ok=True)

    print(f'Método: {method} (norm_type={norm_type}) → {folder}')
    print(f'Linhas: {lines_gpkg}')
    print(f'Saída:  {gpkg_out}')

    layers = _load_named_layers(lines_gpkg)
    missing = []
    for test_name, mapping in DIC_NAME_LAYER.items():
        for l_test, l_ref in mapping.items():
            if l_test not in layers:
                missing.append(l_test)
            if l_ref not in layers:
                missing.append(l_ref)
    if missing:
        raise RuntimeError(f'Camadas em falta no GPKG: {sorted(set(missing))}')

    # CRS a partir da primeira camada de ref
    sample = layers['sei_cumeadas_z']
    crs_authid = sample.crs().authid() or 'EPSG:31983'
    layer_bt = _create_memory_result_layer(crs_authid)
    provider = layer_bt.dataProvider()

    scales = sorted(DIC_PEC_V.keys())
    classes = list(DIC_PEC_MM['H'].keys())
    n_added = 0

    for test_name, mapping in DIC_NAME_LAYER.items():
        print(f'\nDEM: {test_name}')
        profile_path = os.path.join(results_dir, f'Profile_{test_name}.csv')
        with open(profile_path, 'w', encoding='utf-8') as f:
            f.write('')

        for l_test_name, l_ref_name in mapping.items():
            print(f'  Layer: {l_test_name} → {l_ref_name}')
            l_test = layers[l_test_name]
            l_ref = layers[l_ref_name]
            index_ref = QgsSpatialIndex(l_ref.getFeatures())

            for feat_t in l_test.getFeatures():
                geom_t = orient_line_high_to_low(QgsGeometry(feat_t.geometry()))
                if not geom_t or geom_t.isEmpty() or geom_t.length() <= 0:
                    continue
                pm = geom_t.interpolate(geom_t.length() / 2.0)
                nearest = index_ref.nearestNeighbor(pm.asPoint(), 1)
                if not nearest:
                    continue
                feat_r = l_ref.getFeature(nearest[0])
                if not feat_r.isValid() or not feat_r.hasGeometry():
                    continue
                geom_r = orient_line_high_to_low(QgsGeometry(feat_r.geometry()))
                if geom_r.isEmpty() or geom_r.length() <= 0:
                    continue

                profiles = build_compatibilized_profile_geometries(
                    geom_r, geom_t, norm_type
                )
                if not profiles:
                    continue
                cm_r = _mean_z(geom_r)
                cm_t = _mean_z(geom_t)
                _write_profile_csv(
                    profile_path,
                    l_ref_name,
                    int(feat_r.id()),
                    geom_r.length(),
                    geom_t.length(),
                    cm_r,
                    cm_t,
                    profiles['geom_prof_r'],
                    profiles['geom_prof_t'],
                )
                geom_prof_r = profiles['geom_prof_r']
                geom_prof_t = profiles['geom_prof_t']

                for scale_ in scales:
                    for class_ in classes:
                        pec_h = scale_ * DIC_PEC_MM['H'][class_]['pec']
                        pec_v = DIC_PEC_V[scale_][class_]['pec']

                        geom_bt = geom_t.buffer(pec_h, BUFFER_SEGMENTS)
                        geom_br = geom_r.buffer(pec_h, BUFFER_SEGMENTS)
                        geom_i = geom_bt.intersection(geom_br)
                        area_bt = geom_bt.area() or 0.0
                        dm_h = (
                            math.pi * pec_h * (geom_br.area() - geom_i.area()) / area_bt
                            if area_bt
                            else float('nan')
                        )

                        geom_prof_br = geom_prof_r.buffer(pec_v, BUFFER_SEGMENTS)
                        geom_prof_bt = geom_prof_t.buffer(pec_v, BUFFER_SEGMENTS)
                        geom_prof_i = geom_prof_bt.intersection(geom_prof_br)
                        area_pt = geom_prof_bt.area() or 0.0
                        dm_v = (
                            math.pi
                            * pec_v
                            * (geom_prof_br.area() - geom_prof_i.area())
                            / area_pt
                            if area_pt
                            else float('nan')
                        )

                        feat_bt = QgsFeature(layer_bt.fields())
                        feat_bt.setGeometry(geom_bt)
                        feat_bt.setAttributes([
                            int(feat_r.id()),
                            int(scale_),
                            str(class_),
                            str(l_ref_name),
                            str(test_name),
                            float(geom_bt.area()),
                            float(geom_br.area()),
                            float(geom_i.area()),
                            float(dm_h) if math.isfinite(dm_h) else None,
                            False,
                            float(geom_prof_bt.area()),
                            float(geom_prof_br.area()),
                            float(geom_prof_i.area()) if geom_prof_i else 0.0,
                            float(dm_v) if math.isfinite(dm_v) else None,
                            False,
                            float(cm_r) if math.isfinite(cm_r) else None,
                            float(cm_t) if math.isfinite(cm_t) else None,
                        ])
                        provider.addFeatures([feat_bt])
                        n_added += 1

                if n_added and n_added % 200 == 0:
                    print(f'    … {n_added} feições', flush=True)

    layer_bt.updateExtents()
    print(f'\nA gravar GPKG ({n_added} feições)…', flush=True)
    written = _write_result_gpkg(layer_bt, gpkg_out)

    print(f'Feições gravadas: {n_added}')
    print(f'Result.gpkg: {written}')
    return written, results_dir


def main(argv=None):
    p = argparse.ArgumentParser(description='Rebuild Result.gpkg (buffers PEC).')
    p.add_argument(
        '--method',
        default='less_dist',
        choices=sorted(NORM_BY_METHOD.keys()),
    )
    p.add_argument('--lines', default=DEFAULT_LINES_GPKG)
    p.add_argument(
        '--results-dir',
        default=None,
        help='Pasta de saída (default: Results/Geral_linear|proximidade|sem_compatibilizacao)',
    )
    args = p.parse_args(argv)
    init_standalone_qgis()
    try:
        written, _results_dir = rebuild(
            method=args.method,
            lines_gpkg=args.lines,
            results_dir=args.results_dir,
        )
        print(f'OK: {written}')
    finally:
        exit_standalone_qgis()


if __name__ == '__main__':
    main()
