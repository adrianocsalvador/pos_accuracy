# UTF8
"""
Lê feições da camada de resultados (__Buffer_Test__) em um GPKG produzido por
pec_master_buffer_duplo.py, identifica outliers (planimétrico e altimétrico)
e calcula PEC e EP por combinação Test_name / escala / classe.
"""

import math
import os
import statistics

from PyQt5.QtCore import QVariant
from qgis.core import QgsApplication, QgsField, QgsProject, QgsSpatialIndex, QgsVectorLayer
from scipy.stats import shapiro

_qgs_app = None


def init_standalone_qgis():
    """Inicializa QGIS headless (evita avisos Qt ao rodar via .bat)."""
    global _qgs_app
    if _qgs_app is not None or QgsApplication.instance():
        return
    _qgs_app = QgsApplication([], False)
    _qgs_app.initQgis()


def exit_standalone_qgis():
    global _qgs_app
    if _qgs_app is not None:
        _qgs_app.exitQgis()
        _qgs_app = None

# ---------------------------------------------------------------------------
# Parâmetros PEC / EP (mesmos valores de pec_master_buffer_duplo.py)
# ---------------------------------------------------------------------------
DIC_PEC_MM = {
    'H': {
        'A': {'pec': 0.28, 'ep': 0.17},
        'B': {'pec': 0.5, 'ep': 0.3},
        'C': {'pec': 0.8, 'ep': 0.5},
        'D': {'pec': 1.0, 'ep': 0.6},
    },
    'V': {
        'A': {'pec': 0.27, 'ep': 0.17},
        'B': {'pec': 0.5, 'ep': 0.33},
        'C': {'pec': 0.6, 'ep': 0.4},
        'D': {'pec': 0.75, 'ep': 0.5},
    },
}
DIC_PEC_V = {
    50: {
        'A': {'pec': 5.0, 'ep': 3.33},
        'B': {'pec': 10.0, 'ep': 6.66},
        'C': {'pec': 12.0, 'ep': 8.0},
        'D': {'pec': 15.0, 'ep': 10.0},
    },
    100: {
        'A': {'pec': 13.7, 'ep': 8.33},
        'B': {'pec': 25.00, 'ep': 16.66},
        'C': {'pec': 30.0, 'ep': 20.0},
        'D': {'pec': 37.5, 'ep': 25.0},
    },
    250: {
        'A': {'pec': 27.0, 'ep': 16.67},
        'B': {'pec': 50.0, 'ep': 33.33},
        'C': {'pec': 60.0, 'ep': 40.0},
        'D': {'pec': 75.0, 'ep': 50.0},
    },
}

# EQ altimétrico por escala nominal (mesmo fator dic_pec_v do plugin)
DIC_EQ_V = {
    50: 20,
    100: 50,
    250: 100,
}

LAYER_NAME = '__Buffer_Test__'
EXTENT_REF_FIELD = 'extent_ref'
CLASS_ORDER = ['A', 'B', 'C', 'D']

TABLE_COLUMNS_PLAN = [
    'Modelo',
    'Escala',
    'Classe',
    'Outliers',
    'Quant. Amostras Válidas',
    'Extensão Amostras Válidas',
    'Teste PEC quant',
    'Resultado quant',
    'Teste PEC ext',
    'Resultado ext',
    'Teste EP',
    'Resultado EP',
    'array outliers',
    'array_reprovados',
]

TABLE_COLUMNS_ALT = [
    'Modelo',
    'Escala',
    'EQ',
    'Classe',
    'Outliers',
    'Quant. Amostras Válidas',
    'Extensão Amostras Válidas',
    'Teste PEC quant',
    'Resultado quant',
    'Teste PEC ext',
    'Resultado ext',
    'Teste EP',
    'Resultado EP',
    'array outliers',
    'array_reprovados',
]

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_GPKG = os.path.join(_SCRIPT_DIR, 'Results', 'Geral_none', 'Result.gpkg')
DEFAULT_REF_GPKG = os.path.join(_SCRIPT_DIR, 'Data', 'Selecao_v2_z.gpkg')


def load_result_layer(gpkg_path, layer_name=LAYER_NAME, add_to_project=False):
    """Abre a camada de resultados no GPKG."""
    uri = f'{gpkg_path}|layername={layer_name}'
    layer = QgsVectorLayer(uri, layer_name, 'ogr')
    if not layer.isValid():
        raise RuntimeError(
            f'Não foi possível abrir a camada "{layer_name}" em:\n{gpkg_path}'
        )
    if add_to_project:
        QgsProject.instance().addMapLayer(layer, False)
    return layer


def _group_key(feat):
    return f"{feat['Test_name']}-{feat['scale']}-{feat['class']}"


def _parse_group_key(key):
    test_, scale_str, class_ = key.split('-')
    return test_, int(scale_str), class_


def _sort_group_keys(keys):
    return sorted(
        keys,
        key=lambda k: (
            _parse_group_key(k)[0],
            _parse_group_key(k)[1],
            CLASS_ORDER.index(_parse_group_key(k)[2]),
        ),
    )


def _is_valid_extent(val):
    if val is None:
        return False
    try:
        v = float(val)
        return math.isfinite(v) and v > 0
    except (TypeError, ValueError):
        return False


def _float_attr(feat, name, default=None):
    """Converte atributo numérico da feição; trata NULL / QVariant."""
    if name not in feat.fields().names():
        return default
    val = feat[name]
    if val is None:
        return default
    try:
        if hasattr(val, 'isNull') and val.isNull():
            return default
    except Exception:
        pass
    try:
        v = float(val)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _load_ref_layer_cache(ref_gpkg):
    """Cache de camadas de referência: índice espacial e comprimento por fid."""
    meta = QgsVectorLayer(ref_gpkg)
    if not meta.isValid():
        raise RuntimeError(f'Não foi possível abrir o GPKG de referência:\n{ref_gpkg}')

    cache = {}
    for sub in meta.dataProvider().subLayers():
        parts = sub.split('!!::!!')
        if len(parts) < 2:
            continue
        lname = parts[1]
        uri = f'{ref_gpkg}|layername={lname}'
        layer = QgsVectorLayer(uri, lname, 'ogr')
        if not layer.isValid():
            continue
        lengths = {}
        features = list(layer.getFeatures())
        index = QgsSpatialIndex()
        for feat in features:
            index.addFeature(feat)
            geom = feat.geometry()
            if geom and not geom.isEmpty():
                lengths[feat.id()] = geom.length()
        cache[lname] = {
            'layer': layer,
            'index': index,
            'lengths': lengths,
        }
    return cache


def _ref_line_length(ref_cache, layer_ref, id_ref):
    """Comprimento da linha de referência via id_ref; None se inválido."""
    if layer_ref not in ref_cache:
        return None, None
    try:
        fid = int(id_ref)
    except (TypeError, ValueError):
        return None, None
    lengths = ref_cache[layer_ref]['lengths']
    if fid in lengths:
        return fid, lengths[fid]
    feat = ref_cache[layer_ref]['layer'].getFeature(fid)
    if feat.isValid():
        geom = feat.geometry()
        if geom and not geom.isEmpty():
            return fid, geom.length()
    return None, None


def _match_ref_feature(feat, ref_cache):
    """Encontra a feição de referência mais próxima do centróide do buffer."""
    lname = feat['layer_ref']
    if lname not in ref_cache:
        return None, None
    geom = feat.geometry()
    if not geom or geom.isEmpty():
        return None, None
    pt = geom.centroid().asPoint()
    nearest_ids = ref_cache[lname]['index'].nearestNeighbor(pt, 1)
    if not nearest_ids:
        return None, None
    fid = nearest_ids[0]
    length = ref_cache[lname]['lengths'].get(fid)
    return fid, length


def extent_ref_is_ready(layer):
    """True se extent_ref existe e está preenchido em todas as feições."""
    if layer.fields().indexOf(EXTENT_REF_FIELD) < 0:
        return False
    for feat in layer.getFeatures():
        if not _is_valid_extent(feat[EXTENT_REF_FIELD]):
            return False
    return True


def ensure_extent_ref(layer, ref_gpkg=DEFAULT_REF_GPKG):
    """
    Atualiza id_ref e preenche extent_ref cruzando com Selecao_v2_z.gpkg.
    Só executa se extent_ref ainda não existir ou estiver incompleto.
    """
    if extent_ref_is_ready(layer):
        print('extent_ref já preenchido — cruzamento com linhas de referência ignorado.')
        return

    if not os.path.isfile(ref_gpkg):
        raise RuntimeError(f'GPKG de referência não encontrado:\n{ref_gpkg}')

    print(f'Cruzando buffers com linhas de referência:\n{ref_gpkg}')
    ref_cache = _load_ref_layer_cache(ref_gpkg)

    if layer.fields().indexOf(EXTENT_REF_FIELD) < 0:
        if not layer.isEditable():
            layer.startEditing()
        layer.addAttribute(QgsField(EXTENT_REF_FIELD, QVariant.Double))
        layer.updateFields()

    if not layer.isEditable():
        layer.startEditing()

    updated = 0
    for feat in layer.getFeatures():
        if _is_valid_extent(feat[EXTENT_REF_FIELD]):
            continue
        fid, length = _ref_line_length(ref_cache, feat['layer_ref'], feat['id_ref'])
        if length is None:
            fid, length = _match_ref_feature(feat, ref_cache)
        if fid is None or length is None:
            continue
        feat.setAttribute('id_ref', int(fid))
        feat.setAttribute(EXTENT_REF_FIELD, float(length))
        layer.updateFeature(feat)
        updated += 1

    layer.commitChanges()
    print(f'Feições atualizadas (id_ref / extent_ref): {updated}')


def reset_outlier_flag(layer, out_field):
    """Zera OUT_H ou OUT_V antes de recalcular outliers."""
    if not layer.isEditable():
        layer.startEditing()
    for feat in layer.getFeatures():
        feat.setAttribute(out_field, False)
        layer.updateFeature(feat)
    layer.commitChanges()


def build_stats_dict(layer, dm_tag, out_field, exclude_outliers=True):
    """
    Agrupa valores por Test_name / escala / classe para uma dimensão (H ou V).
    Inclui extent_ref (m) e id_ref de cada amostra.
    """
    dic_stats = {}
    dm_field = 'DM_H' if dm_tag == 'dm_h' else 'DM_V'
    for feat in layer.getFeatures():
        if exclude_outliers and feat[out_field]:
            continue
        key = _group_key(feat)
        id_ref = _float_attr(feat, 'id_ref')
        dm = _float_attr(feat, dm_field)
        if id_ref is None or dm is None:
            continue
        if key not in dic_stats:
            dic_stats[key] = {
                'ids': [],
                'id_refs': [],
                'values': [],
                'extents': [],
                'd_cota': [],
            }
        dic_stats[key]['ids'].append(feat.id())
        dic_stats[key]['id_refs'].append(int(id_ref))
        dic_stats[key]['values'].append(dm)
        extent = _float_attr(feat, EXTENT_REF_FIELD, default=0.0)
        if not _is_valid_extent(extent):
            extent = 0.0
        dic_stats[key]['extents'].append(float(extent))
        cm_t = _float_attr(feat, 'Cota_Media_t')
        cm_r = _float_attr(feat, 'Cota_Media_r')
        if cm_t is not None and cm_r is not None:
            dic_stats[key]['d_cota'].append(cm_t - cm_r)
    return dic_stats


def outlier_id_refs_by_group(layer, out_field):
    """Lista id_ref dos outliers por grupo."""
    ids_by_group = {}
    for feat in layer.getFeatures():
        if not feat[out_field]:
            continue
        key = _group_key(feat)
        id_ref = int(feat['id_ref'])
        ids_by_group.setdefault(key, set()).add(id_ref)
    return {k: sorted(v) for k, v in ids_by_group.items()}


def count_outliers_by_group(layer, out_field):
    """Conta outliers por grupo Test_name / escala / classe."""
    counts = {}
    for feat in layer.getFeatures():
        if not feat[out_field]:
            continue
        key = _group_key(feat)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _iqr_bounds(values):
    quant_ = statistics.quantiles(data=values)
    iqr_ = quant_[2] - quant_[0]
    upper = quant_[2] + 1.5 * iqr_
    lower = quant_[0] - 1.5 * iqr_
    return lower, upper


def mark_outliers(layer, dic_stats, out_field, value_key='values'):
    """Marca outliers por IQR para a dimensão em análise."""
    if not layer.isEditable():
        layer.startEditing()

    for key in dic_stats:
        values = dic_stats[key][value_key]
        ids = dic_stats[key]['ids']
        if len(values) < 2:
            continue
        lower, upper = _iqr_bounds(values)
        for i, val in enumerate(values):
            if val < lower or val > upper:
                feat = layer.getFeature(ids[i])
                feat.setAttribute(out_field, True)
                layer.updateFeature(feat)

    layer.commitChanges()


def detect_outliers_for_dimension(layer, dm_tag, out_field):
    """Identifica outliers e retorna estatísticas após exclusão."""
    reset_outlier_flag(layer, out_field)
    dic_stats = build_stats_dict(layer, dm_tag, out_field, exclude_outliers=False)
    mark_outliers(layer, dic_stats, out_field)
    return build_stats_dict(layer, dm_tag, out_field, exclude_outliers=True)


def check_norm(values):
    if len(values) < 3:
        return False
    result_ = shapiro(values)
    return result_[0] >= result_[1]


def rms(values):
    if len(values) < 2:
        return float('nan')
    sun_ = sum(v ** 2 for v in values)
    return (sun_ / (len(values) - 1)) ** 0.5


def pec_test_limit(pec_):
    """Arredonda o limiar PEC para inteiro antes do teste."""
    return int(round(float(pec_)))


def perc_pec_quant(values, pec_):
    """Percentual de amostras (quantidade) com valor <= PEC."""
    if not values:
        return 0.0
    pec_lim = pec_test_limit(pec_)
    count_ = sum(1 for v in values if v <= pec_lim)
    return count_ / len(values)


def perc_pec_ext(values, extents, pec_):
    """Percentual da extensão total com valor <= PEC."""
    pec_lim = pec_test_limit(pec_)
    total_ext = sum(extents)
    if total_ext <= 0:
        return 0.0
    ok_ext = sum(ext for v, ext in zip(values, extents) if v <= pec_lim)
    return ok_ext / total_ext


def _total_extent_km(extents):
    return int(round(sum(extents) / 1000.0))


def _pec_ep_limits(scale_, class_, dimension):
    if dimension == 'H':
        pec = round(scale_ * DIC_PEC_MM['H'][class_]['pec'], 2)
        ep = round(scale_ * DIC_PEC_MM['H'][class_]['ep'], 2)
    else:
        pec = round(DIC_PEC_V[scale_][class_]['pec'], 2)
        ep = round(DIC_PEC_V[scale_][class_]['ep'], 2)
    return pec, ep


def _format_table_row(cells):
    return '\t'.join(str(c) for c in cells)


def _format_id_list(ids):
    if not ids:
        return ''
    return ', '.join(str(i) for i in ids)


def _reprovados_id_refs(values, id_refs, pec_):
    """id_ref das amostras válidas com DM > PEC (limiar inteiro)."""
    pec_lim = pec_test_limit(pec_)
    return sorted({id_ref for v, id_ref in zip(values, id_refs) if v > pec_lim})


def _format_outlier_ids(outlier_id_refs):
    return _format_id_list(outlier_id_refs)


def _row_head(test_, scale_, class_, dimension):
    if dimension == 'V':
        return [test_, scale_, DIC_EQ_V.get(scale_, ''), class_]
    return [test_, scale_, class_]


def print_results_table(title, rows, columns):
    """Imprime tabela tabulada no formato solicitado."""
    print()
    print('=' * 120)
    print(title)
    print('=' * 120)
    print(_format_table_row(columns))
    for row in rows:
        print(_format_table_row(row))
    print()


def build_result_rows(layer, dic_stats, dimension, out_field):
    """Monta linhas da tabela para planimetria (H) ou altimetria (V)."""
    outlier_counts = count_outliers_by_group(layer, out_field)
    outlier_ids = outlier_id_refs_by_group(layer, out_field)
    rows = []

    for key in _sort_group_keys(dic_stats.keys()):
        test_, scale_, class_ = _parse_group_key(key)
        values = dic_stats[key]['values']
        extents = dic_stats[key]['extents']
        n_out = outlier_counts.get(key, 0)
        n_valid = len(values)
        ext_km = _total_extent_km(extents)
        out_ids = outlier_ids.get(key, [])
        id_refs = dic_stats[key]['id_refs']

        pec_, ep_ = _pec_ep_limits(scale_, class_, dimension)
        pec_lim = pec_test_limit(pec_)
        reprov_ids = _reprovados_id_refs(values, id_refs, pec_) if values else []

        if not values or not check_norm(values):
            rows.append(_row_head(test_, scale_, class_, dimension) + [
                n_out,
                n_valid,
                ext_km,
                'NORMALIDADE - FALHOU',
                'FALHOU',
                'NORMALIDADE - FALHOU',
                'FALHOU',
                '',
                'FALHOU',
                _format_outlier_ids(out_ids),
                _format_id_list(reprov_ids),
            ])
            continue

        perc_q = perc_pec_quant(values, pec_)
        perc_e = perc_pec_ext(values, extents, pec_)
        pec_ok_q = perc_q >= 0.90
        pec_ok_e = perc_e >= 0.90
        rms_ = rms(values)
        ep_ok = math.isfinite(rms_) and rms_ <= ep_

        teste_pec_q = f'{round(perc_q * 100)} % <= {pec_lim}'
        teste_pec_e = f'{round(perc_e * 100)} % <= {pec_lim}'
        resultado_q = 'PASSOU' if pec_ok_q else 'FALHOU'
        resultado_e = 'PASSOU' if pec_ok_e else 'FALHOU'
        cmp_ = '<=' if ep_ok else '>'
        rms_show = round(rms_, 2) if math.isfinite(rms_) else 'n/d'
        teste_ep = f'{rms_show} {cmp_} {ep_} EP'
        resultado_ep = 'PASSOU' if ep_ok else 'FALHOU'

        rows.append(_row_head(test_, scale_, class_, dimension) + [
            n_out,
            n_valid,
            ext_km,
            teste_pec_q,
            resultado_q,
            teste_pec_e,
            resultado_e,
            teste_ep,
            resultado_ep,
            _format_outlier_ids(out_ids),
            _format_id_list(reprov_ids),
        ])

    return rows


def write_results_table(path, plan_rows, alt_rows, plan_cota_lines=None):
    """Grava relatório em texto com as duas tabelas."""
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write('ANÁLISE PLANIMÉTRICA\n')
        f.write(_format_table_row(TABLE_COLUMNS_PLAN) + '\n')
        for row in plan_rows:
            f.write(_format_table_row(row) + '\n')

        f.write('\nANÁLISE ALTIMÉTRICA\n')
        f.write(_format_table_row(TABLE_COLUMNS_ALT) + '\n')
        for row in alt_rows:
            f.write(_format_table_row(row) + '\n')

        if plan_cota_lines:
            f.write('\nDIFERENÇA MÉDIA DE COTA (planimetria, amostras válidas H)\n')
            for line in plan_cota_lines:
                f.write(line + '\n')

        f.write('\n')


def _cota_summary_lines(dic_stats):
    lines = []
    for key in _sort_group_keys(dic_stats.keys()):
        test_, scale_, class_ = _parse_group_key(key)
        list_cota = dic_stats[key]['d_cota']
        if list_cota:
            mean_ = round(statistics.mean(list_cota), 1)
            lines.append(f'{test_}\t{scale_}\t{class_}\td_cota_media = {mean_}')
    return lines


def run(
    gpkg_path=DEFAULT_GPKG,
    ref_gpkg=DEFAULT_REF_GPKG,
    results_txt=None,
    add_layer_to_project=False,
):
    """
    Pipeline completo: carrega GPKG → extent_ref → outliers → PEC/EP → Results.txt
    """
    if results_txt is None:
        results_txt = os.path.join(os.path.dirname(gpkg_path), 'Results.txt')

    print(f'Carregando: {gpkg_path}')
    layer = load_result_layer(gpkg_path, add_to_project=add_layer_to_project)
    print(f'Feições na camada: {layer.featureCount()}')

    ensure_extent_ref(layer, ref_gpkg=ref_gpkg)

    print('\nIdentificando outliers planimétricos (DM_H)...')
    dic_stats_h = detect_outliers_for_dimension(layer, 'dm_h', 'OUT_H')
    n_out_h = sum(1 for f in layer.getFeatures() if f['OUT_H'])
    print(f'Outliers planimétricos (OUT_H): {n_out_h}')

    plan_rows = build_result_rows(layer, dic_stats_h, 'H', 'OUT_H')
    print_results_table('ANÁLISE PLANIMÉTRICA', plan_rows, TABLE_COLUMNS_PLAN)

    print('Identificando outliers altimétricos (DM_V)...')
    dic_stats_v = detect_outliers_for_dimension(layer, 'dm_v', 'OUT_V')
    n_out_v = sum(1 for f in layer.getFeatures() if f['OUT_V'])
    print(f'Outliers altimétricos (OUT_V): {n_out_v}')

    alt_rows = build_result_rows(layer, dic_stats_v, 'V', 'OUT_V')
    print_results_table('ANÁLISE ALTIMÉTRICA', alt_rows, TABLE_COLUMNS_ALT)

    cota_lines = _cota_summary_lines(dic_stats_h)
    write_results_table(results_txt, plan_rows, alt_rows, cota_lines)
    print(f'Relatório gravado em: {results_txt}')

    if add_layer_to_project:
        layer.triggerRepaint()
    return layer, {'H': dic_stats_h, 'V': dic_stats_v}, results_txt


if __name__ == '__main__' or __name__ == '__builtin__':
    import sys

    gpkg_path = DEFAULT_GPKG
    ref_gpkg = DEFAULT_REF_GPKG
    if len(sys.argv) > 1:
        gpkg_path = sys.argv[1]
    if len(sys.argv) > 2:
        ref_gpkg = sys.argv[2]
    elif os.environ.get('GPKG_PATH'):
        gpkg_path = os.environ['GPKG_PATH']
    if os.environ.get('REF_GPKG'):
        ref_gpkg = os.environ['REF_GPKG']

    add_to_project = __name__ == '__builtin__'
    if __name__ == '__main__':
        init_standalone_qgis()
    try:
        run(gpkg_path=gpkg_path, ref_gpkg=ref_gpkg, add_layer_to_project=add_to_project)
    finally:
        if __name__ == '__main__':
            exit_standalone_qgis()
