# UTF8
"""
Gera PDF de pares homólogos: vista 2D + perfis por classe A–D.

Layout por página (1 par × 1 escala):
  Coluna 1 — mapa XY (altura de 4 linhas)
  Coluna 2 — painéis empilhados A/B/C/D com:
    ref + buffer V, teste sem compatibilização, teste compatibilizada + buffer V

Batch (1 PDF por modelo×escala):
  report_homologous_profiles_pdf.py --results-dirs Results/Geral_linear Results/Geral_proximidade
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import datetime

import matplotlib

matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.ticker import FuncFormatter, MultipleLocator
import matplotlib.pyplot as plt
from qgis.core import Qgis, QgsGeometry, QgsPointXY, QgsSpatialIndex, QgsVectorLayer

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PLUGIN_ROOT = os.path.dirname(_SCRIPT_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
if _PLUGIN_ROOT not in sys.path:
    sys.path.insert(0, _PLUGIN_ROOT)

from mods.mod_worker_threads import (  # noqa: E402
    PROFILE_PROG_OFFSET,
    build_compatibilized_profile_geometries,
    orient_line_high_to_low,
)

from pec_from_gpkg import (  # noqa: E402
    CLASS_ORDER,
    DIC_EQ_V,
    DIC_NAME_LAYER,
    DIC_PEC_MM,
    DIC_PEC_V,
    exit_standalone_qgis,
    init_standalone_qgis,
    load_result_layer,
)

DEFAULT_GPKG = os.path.join(_SCRIPT_DIR, 'Results', 'Geral_linear', 'Result.gpkg')
DEFAULT_LINES_GPKG = os.path.join(_SCRIPT_DIR, 'Data', 'Selecao_v2_z.gpkg')
DEFAULT_OUT = os.path.join(
    _SCRIPT_DIR, 'Results', 'Geral_linear', 'Audit_vertical_smoke.pdf'
)
DEFAULT_OUT_H = os.path.join(
    _SCRIPT_DIR, 'Results', 'Geral_linear', 'Audit_horizontal_smoke.pdf'
)

NORM_SCALE = 0
NORM_LESS_DIST = 1
NORM_NONE = 2
BUFFER_SEGMENTS = 20
MAP_GRID_STEP_M = 500.0  # reticulado 2D (X e Y) em metros

# Cores
COLOR_REF = '#1f77b4'
COLOR_TEST = '#d62728'
COLOR_TEST_RAW = '#ff7f0e'
COLOR_BUF_REF = '#1f77b4'
COLOR_BUF_TEST = '#d62728'

# Logo do plugin (canto superior esquerdo de cada página)
PLUGIN_LOGO_PATH = os.path.join(_PLUGIN_ROOT, 'icons', 'icon_bfn.png')
_LOGO_CACHE = None


def _load_plugin_logo():
    """Carrega icons/icon_bfn.png (cache); None se indisponível."""
    global _LOGO_CACHE
    if _LOGO_CACHE is False:
        return None
    if _LOGO_CACHE is not None:
        return _LOGO_CACHE
    path = PLUGIN_LOGO_PATH
    if not os.path.isfile(path):
        svg = os.path.join(_PLUGIN_ROOT, 'icons', 'icon_bfn.svg')
        path = svg if os.path.isfile(svg) else ''
    if not path:
        _LOGO_CACHE = False
        return None
    try:
        _LOGO_CACHE = plt.imread(path)
    except Exception:
        _LOGO_CACHE = False
        return None
    return _LOGO_CACHE


def _add_page_logo(fig):
    """Logo no canto superior esquerdo (todas as páginas do audit)."""
    img = _load_plugin_logo()
    if img is None:
        return
    # ~0.48" × 0.48" em A4 landscape; acima do GridSpec (top=0.92)
    ax_logo = fig.add_axes([0.012, 0.935, 0.042, 0.055])
    ax_logo.imshow(img, interpolation='nearest')
    ax_logo.set_axis_off()
    ax_logo.set_navigate(False)


def _line_buffer_round(geom, distance):
    """Buffer geométrico (mesmo critério do cálculo DM_V), tampas Round."""
    if geom is None or geom.isEmpty():
        return None
    return QgsGeometry(geom).buffer(
        float(distance),
        int(BUFFER_SEGMENTS),
        Qgis.EndCapStyle.Round,
        Qgis.JoinStyle.Round,
        2.0,
    )


def _shared_profile_limits(line_geoms, buffer_src_geoms, pec_v_max):
    """Bbox comum = todas as linhas de perfil + maior buffer V geométrico."""
    pec = float(pec_v_max)
    extent_geoms = [g for g in line_geoms if g]
    for g in buffer_src_geoms:
        if g is None or g.isEmpty():
            continue
        buf = _line_buffer_round(g, pec)
        if buf is not None and not buf.isEmpty():
            extent_geoms.append(buf)
    bbox = _bbox_from_geoms(extent_geoms)
    if not bbox:
        return None
    xmin, xmax, ymin, ymax = bbox
    # Margem extra em X para as tampas do buffer não serem cortadas
    pad_x = max((xmax - xmin) * 0.04, float(pec) * 2.0, 10.0)
    pad_y = max((ymax - ymin) * 0.08, float(pec) * 0.5, 2.0)
    return (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)


def _load_layer_cache(gpkg_path):
    """Cache de subcamadas: layer, index espacial, features por fid."""
    meta = QgsVectorLayer(gpkg_path)
    if not meta.isValid():
        raise RuntimeError(f'Não foi possível abrir o GPKG:\n{gpkg_path}')

    cache = {}
    for sub in meta.dataProvider().subLayers():
        parts = sub.split('!!::!!')
        if len(parts) < 2:
            continue
        lname = parts[1]
        uri = f'{gpkg_path}|layername={lname}'
        layer = QgsVectorLayer(uri, lname, 'ogr')
        if not layer.isValid():
            continue
        features = {}
        index = QgsSpatialIndex()
        for feat in layer.getFeatures():
            features[feat.id()] = feat
            index.addFeature(feat)
        cache[lname] = {
            'layer': layer,
            'index': index,
            'features': features,
        }
    return cache


def _build_test_pair_index(lines_cache):
    """
    Índice (Test_name, layer_ref, id_ref) -> (id_test, geom_t).
    Reproduz o pareamento do master: centro da teste → vizinho mais próximo na ref.
    """
    pair_index = {}
    for test_name, layer_map in DIC_NAME_LAYER.items():
        for l_test, l_ref in layer_map.items():
            if l_test not in lines_cache or l_ref not in lines_cache:
                continue
            t_info = lines_cache[l_test]
            r_index = lines_cache[l_ref]['index']
            for fid_t, feat_t in t_info['features'].items():
                geom_t = feat_t.geometry()
                if not geom_t or geom_t.isEmpty():
                    continue
                length = geom_t.length()
                if not math.isfinite(length) or length <= 0:
                    continue
                pm = geom_t.interpolate(length / 2.0)
                nearest = r_index.nearestNeighbor(pm.asPoint(), 1)
                if not nearest:
                    continue
                id_ref = nearest[0]
                key = (test_name, l_ref, int(id_ref))
                # Primeiro match vence (mesmo critério estável do smoke)
                if key not in pair_index:
                    pair_index[key] = (int(fid_t), QgsGeometry(geom_t))
    return pair_index


def _unique_pairs_from_result(result_layer, scale, model=None):
    """Pares únicos (Test_name, layer_ref, id_ref) presentes na escala (e modelo)."""
    seen = {}
    model_u = str(model).upper() if model else None
    for feat in result_layer.getFeatures():
        try:
            sc = int(feat['scale'])
        except (TypeError, ValueError):
            continue
        if sc != int(scale):
            continue
        test_name = str(feat['Test_name'] or '')
        if model_u and test_name.upper() != model_u:
            continue
        layer_ref = str(feat['layer_ref'] or '')
        try:
            id_ref = int(feat['id_ref'])
        except (TypeError, ValueError):
            continue
        if not test_name or not layer_ref:
            continue
        key = (test_name, layer_ref, id_ref)
        if key not in seen:
            seen[key] = True
    return sorted(seen.keys(), key=lambda k: (k[0], k[1], k[2]))


def _models_from_result(result_layer):
    names = set()
    for feat in result_layer.getFeatures():
        name = feat['Test_name']
        if name:
            names.add(str(name))
    return sorted(names)


def _scales_from_result(result_layer, model=None):
    scales = set()
    model_u = str(model).upper() if model else None
    for feat in result_layer.getFeatures():
        if model_u and str(feat['Test_name'] or '').upper() != model_u:
            continue
        try:
            scales.add(int(feat['scale']))
        except (TypeError, ValueError):
            continue
    return sorted(s for s in scales if s in DIC_PEC_V)


def _norm_label(norm_type):
    return {
        NORM_SCALE: 'Compatibilização Linear',
        NORM_LESS_DIST: 'Compatibilização por Proximidade',
        NORM_NONE: 'Sem Compatibilização',
    }.get(int(norm_type), str(norm_type))


def _norm_filename_slug(norm_type):
    """Slug curto para nomes de ficheiro: linear | proximidade | sem_normalizacao."""
    return {
        NORM_SCALE: 'linear',
        NORM_LESS_DIST: 'proximidade',
        NORM_NONE: 'sem_normalizacao',
    }.get(int(norm_type), f'norm{int(norm_type)}')


def _safe_filename_token(text, fallback='X'):
    token = ''.join(
        ch if ch.isalnum() or ch in ('-', '_') else '_'
        for ch in str(text or '')
    ).strip('_')
    return token or fallback


def _resolve_norm_type(norm_arg, results_dir=None):
    """Aceita 0/1/2, scale|linear|less_dist|proximidade|none|…, ou infere pela pasta."""
    if norm_arg is None or norm_arg == 'auto':
        base = os.path.basename(os.path.normpath(results_dir or ''))
        low = base.lower()
        if 'less_dist' in low or 'proximidade' in low:
            return NORM_LESS_DIST
        if (
            'sem_compatibilizacao' in low
            or 'sem_normalizacao' in low
            or low.endswith('_none')
            or 'geral_none' in low
        ):
            return NORM_NONE
        # Geral_linear (e aliases antigos Geral_scale) / default
        return NORM_SCALE
    key = str(norm_arg).strip().lower()
    mapping = {
        '0': NORM_SCALE,
        'scale': NORM_SCALE,
        'linear': NORM_SCALE,
        '1': NORM_LESS_DIST,
        'less_dist': NORM_LESS_DIST,
        'less-dist': NORM_LESS_DIST,
        'proximidade': NORM_LESS_DIST,
        '2': NORM_NONE,
        'none': NORM_NONE,
        'sem_compatibilizacao': NORM_NONE,
        'sem_normalizacao': NORM_NONE,
    }
    if key not in mapping:
        raise RuntimeError(
            f'norm inválido: {norm_arg!r} '
            f'(use scale|linear|less_dist|proximidade|none|sem_compatibilizacao ou 0|1|2)'
        )
    return mapping[key]


def _dm_attrs_by_class(result_layer, test_name, layer_ref, id_ref, scale):
    """DM_V / OUT_V / DM_H / OUT_H por classe para o par/escala."""
    out = {}
    for feat in result_layer.getFeatures():
        try:
            if int(feat['scale']) != int(scale):
                continue
            if int(feat['id_ref']) != int(id_ref):
                continue
        except (TypeError, ValueError):
            continue
        if str(feat['Test_name']) != test_name or str(feat['layer_ref']) != layer_ref:
            continue
        class_ = str(feat['class'] or '')
        if class_ not in CLASS_ORDER:
            continue
        out[class_] = {
            'DM_V': _float_or_none(feat['DM_V']),
            'OUT_V': bool(feat['OUT_V']) if feat['OUT_V'] is not None else False,
            'DM_H': _float_or_none(feat['DM_H']),
            'OUT_H': bool(feat['OUT_H']) if feat['OUT_H'] is not None else False,
            'Area_Ref_Prof': _float_or_none(feat['Area_Ref_Prof']),
            'Area_Test_Prof': _float_or_none(feat['Area_Test_Prof']),
            'Area_Inter_Prof': _float_or_none(feat['Area_Inter_Prof']),
            'Area_Ref': _float_or_none(feat['Area_Ref']),
            'Area_Test': _float_or_none(feat['Area_Test']),
            'Area_Inter': _float_or_none(feat['Area_Inter']),
        }
    return out


def _float_or_none(val):
    if val is None:
        return None
    try:
        if hasattr(val, 'isNull') and val.isNull():
            return None
    except Exception:
        pass
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _polyline_xy(geom):
    """Lista de (x, y) de uma LineString / MultiLineString."""
    if geom is None or geom.isEmpty():
        return []
    g = QgsGeometry(geom)
    try:
        if g.isMultipart():
            parts = g.asMultiPolyline()
            pts = parts[0] if parts else []
        else:
            pts = g.asPolyline()
    except Exception:
        return []
    return [(p.x(), p.y()) for p in pts]


def _line_start_xy(geom):
    """Primeiro vértice da linha (= progressiva 0 na referência)."""
    xy = _polyline_xy(geom)
    return xy[0] if xy else None


def _shift_profile_geometry(geom, offset=PROFILE_PROG_OFFSET):
    """Remove o offset das progressivas (PROFILE_PROG_OFFSET) para o eixo começar em 0."""
    xy = _polyline_xy(geom)
    if len(xy) < 2:
        return None
    pts = [QgsPointXY(x - float(offset), y) for x, y in xy]
    return QgsGeometry.fromPolylineXY(pts)


def _polygon_rings_xy(geom):
    """Lista de anéis [(x,y), ...] de um polígono / multipolígono (incl. buffer)."""
    if geom is None or geom.isEmpty():
        return []
    g = QgsGeometry(geom)
    rings = []

    def _append_poly(poly):
        if not poly:
            return
        # poly = [exterior, hole1, ...]
        exterior = poly[0] if isinstance(poly[0], (list, tuple)) else poly
        if exterior and len(exterior) >= 3:
            rings.append([(p.x(), p.y()) for p in exterior])

    try:
        # Preferir multipolígono — buffers de linhas longas costumam ser MultiPolygon
        mp = g.asMultiPolygon()
        if mp:
            for poly in mp:
                _append_poly(poly)
            if rings:
                return rings
    except Exception:
        pass
    try:
        poly = g.asPolygon()
        _append_poly(poly)
        if rings:
            return rings
    except Exception:
        pass
    # Fallback: extrair anéis via WKT / constGet
    try:
        geom_out = g
        if not g.isMultipart():
            # força multipolygon para iteração uniforme
            pass
        for part in range(geom_out.constGet().numGeometries() if hasattr(geom_out.constGet(), 'numGeometries') else 0):
            part_g = QgsGeometry(geom_out.constGet().geometryN(part).clone())
            poly = part_g.asPolygon()
            _append_poly(poly)
    except Exception:
        pass
    return rings


def _plot_polygon(ax, geom, *, facecolor, edgecolor, alpha=0.25, linewidth=0.8, zorder=1, label=None):
    rings = _polygon_rings_xy(geom)
    first = True
    for ring in rings:
        if len(ring) < 3:
            continue
        ax.add_patch(
            MplPolygon(
                ring,
                closed=True,
                facecolor=facecolor,
                edgecolor=edgecolor,
                alpha=alpha,
                linewidth=linewidth,
                zorder=zorder,
                label=label if first else None,
                clip_on=False,
            )
        )
        first = False
    return len(rings) > 0


def _bbox_from_geoms(geoms):
    xs, ys = [], []
    for g in geoms:
        if g is None or g.isEmpty():
            continue
        bb = g.boundingBox()
        xs.extend([bb.xMinimum(), bb.xMaximum()])
        ys.extend([bb.yMinimum(), bb.yMaximum()])
    if not xs:
        return None
    return min(xs), max(xs), min(ys), max(ys)


def _plot_line(ax, geom, *, color, linewidth=1.2, linestyle='-', label=None, zorder=3):
    xy = _polyline_xy(geom)
    if len(xy) < 2:
        return
    xs, ys = zip(*xy)
    ax.plot(
        xs,
        ys,
        color=color,
        linewidth=linewidth,
        linestyle=linestyle,
        label=label,
        zorder=zorder,
    )





def _set_equal_aspect_pad(ax, geoms, pad_ratio=0.05):
    xs, ys = [], []
    for g in geoms:
        if g is None or g.isEmpty():
            continue
        bb = g.boundingBox()
        xs.extend([bb.xMinimum(), bb.xMaximum()])
        ys.extend([bb.yMinimum(), bb.yMaximum()])
    if not xs:
        return
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)
    dx = max(xmax - xmin, 1.0)
    dy = max(ymax - ymin, 1.0)
    pad_x = dx * pad_ratio
    pad_y = dy * pad_ratio
    ax.set_xlim(xmin - pad_x, xmax + pad_x)
    ax.set_ylim(ymin - pad_y, ymax + pad_y)
    ax.set_aspect('equal', adjustable='datalim')


def _apply_map_grid(ax, step_m=MAP_GRID_STEP_M):
    """Reticulado igual em X e Y; coordenadas Norte (Y) como inteiros."""
    step = float(step_m)
    ax.xaxis.set_major_locator(MultipleLocator(step))
    ax.yaxis.set_major_locator(MultipleLocator(step))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _pos: f'{int(round(v))}'))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _pos: f'{int(round(v))}'))
    ax.grid(True, which='major', alpha=0.35)


def _draw_page(
    pdf,
    *,
    test_name,
    layer_ref,
    id_ref,
    id_test,
    scale,
    geom_r,
    geom_t,
    profiles_scale,
    profiles_raw,
    dm_by_class,
    norm_label='Compatibilização Linear',
    norm_type=NORM_SCALE,
    pec_v_table=None,
    eq_v_table=None,
):
    geom_r = orient_line_high_to_low(geom_r)
    geom_t = orient_line_high_to_low(geom_t)
    k_t = profiles_scale.get('k_t') if profiles_scale else None
    k_str = f'{k_t:.2f}' if k_t is not None and math.isfinite(k_t) else 'n/d'
    show_k = int(norm_type) == NORM_SCALE and k_str != 'n/d'
    title = (
        f'{test_name}; 1:{int(scale) * 1000}; {norm_label}; {layer_ref}; '
        f'id_ref={id_ref}; id_test={id_test}'
    )
    if show_k:
        title += f'; fator de escala k={k_str}'

    pec_lookup = pec_v_table if pec_v_table is not None else DIC_PEC_V
    eq_lookup = eq_v_table if eq_v_table is not None else DIC_EQ_V

    fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
    # left menor + wspace maior: evita «Cota (m)» invadir a Vista 2D
    gs = GridSpec(
        4,
        2,
        figure=fig,
        width_ratios=[0.72, 1.0],
        left=0.04,
        right=0.985,
        top=0.92,
        bottom=0.07,
        wspace=0.08,
        hspace=0.32 * 2 / 3,
    )

    ax_2d = fig.add_subplot(gs[:, 0])
    _plot_line(ax_2d, geom_r, color=COLOR_REF, linewidth=1.6, label='Referência', zorder=4)
    _plot_line(ax_2d, geom_t, color=COLOR_TEST, linewidth=1.4, label='Teste', zorder=5)
    prog0 = _line_start_xy(geom_r)
    if prog0 is not None:
        ax_2d.plot(
            prog0[0],
            prog0[1],
            marker='o',
            markersize=7 * 2 / 3,
            linestyle='None',
            color='#111111',
            markerfacecolor='#ffffff',
            markeredgecolor='#111111',
            markeredgewidth=0.8,
            label='Progressiva Zero',
            zorder=6,
        )
    _set_equal_aspect_pad(ax_2d, [geom_r, geom_t])
    _apply_map_grid(ax_2d, MAP_GRID_STEP_M)
    ax_2d.set_title('Vista 2D', fontsize=9)
    ax_2d.set_xlabel('X (m)', fontsize=8)
    ax_2d.set_ylabel('Norte (Y)', fontsize=8, rotation=90, labelpad=2)
    ax_2d.tick_params(axis='y', labelsize=7, pad=1)
    ax_2d.tick_params(axis='x', labelsize=7)
    for lab in ax_2d.get_yticklabels():
        lab.set_rotation(90)
        lab.set_va('center')
        lab.set_ha('right')
    ax_2d.legend(loc='upper right', fontsize=7, framealpha=0.9)

    geom_prof_r = profiles_scale['geom_prof_r'] if profiles_scale else None
    geom_prof_t = profiles_scale['geom_prof_t'] if profiles_scale else None
    geom_prof_t_raw = profiles_raw['geom_prof_t'] if profiles_raw else None
    # Progressivas sem o offset +10000 usado no cálculo interno
    if geom_prof_r is not None:
        geom_prof_r = _shift_profile_geometry(geom_prof_r)
    if geom_prof_t is not None:
        geom_prof_t = _shift_profile_geometry(geom_prof_t)
    if geom_prof_t_raw is not None:
        geom_prof_t_raw = _shift_profile_geometry(geom_prof_t_raw)

    # Enquadramento comum = bbox do maior buffer V geométrico (classe D)
    scale_pecs = pec_lookup.get(int(scale)) or {}

    def _pec_of(class_name):
        raw = scale_pecs.get(class_name)
        if isinstance(raw, dict):
            return float(raw.get('pec', 0.0))
        return float(raw) if raw is not None else 0.0

    pec_v_max = max((_pec_of(c) for c in CLASS_ORDER), default=1.0)
    line_geoms = [g for g in (geom_prof_r, geom_prof_t, geom_prof_t_raw) if g]
    buf_src = [g for g in (geom_prof_r, geom_prof_t) if g]
    shared_limits = _shared_profile_limits(line_geoms, buf_src, pec_v_max)

    for i, class_ in enumerate(CLASS_ORDER):
        ax = fig.add_subplot(gs[i, 1])
        pec_v = _pec_of(class_)
        attrs = dm_by_class.get(class_) or {}
        dm_v = attrs.get('DM_V')
        dm_str = f'{dm_v:.2f}' if dm_v is not None else 'n/d'

        area_ref = attrs.get('Area_Ref_Prof')
        area_test = attrs.get('Area_Test_Prof')
        area_inter = attrs.get('Area_Inter_Prof')
        buf_r = None
        buf_t = None

        if geom_prof_r is not None and not geom_prof_r.isEmpty():
            buf_r = _line_buffer_round(geom_prof_r, pec_v)
            drawn = _plot_polygon(
                ax,
                buf_r,
                facecolor=COLOR_BUF_REF,
                edgecolor=COLOR_BUF_REF,
                alpha=0.28,
                linewidth=0.9,
                zorder=1,
                label='Buffer Ref',
            )
            if not drawn:
                print(f'    AVISO: buffer ref classe {class_} não desenhado')
            _plot_line(
                ax,
                geom_prof_r,
                color=COLOR_REF,
                linewidth=1.2,
                label='Ref',
                zorder=4,
            )
            if buf_r is not None and not buf_r.isEmpty() and area_ref is None:
                area_ref = buf_r.area()

        if geom_prof_t_raw is not None:
            _plot_line(
                ax,
                geom_prof_t_raw,
                color=COLOR_TEST_RAW,
                linewidth=1.0,
                linestyle='--',
                label='Teste original',
                zorder=5,
            )

        if geom_prof_t is not None and not geom_prof_t.isEmpty():
            buf_t = _line_buffer_round(geom_prof_t, pec_v)
            drawn = _plot_polygon(
                ax,
                buf_t,
                facecolor=COLOR_BUF_TEST,
                edgecolor=COLOR_BUF_TEST,
                alpha=0.22,
                linewidth=0.9,
                zorder=2,
                label='Buffer Teste c.',
            )
            if not drawn:
                print(f'    AVISO: buffer teste classe {class_} não desenhado')
            _plot_line(
                ax,
                geom_prof_t,
                color=COLOR_TEST,
                linewidth=1.2,
                label='Teste compat.',
                zorder=6,
            )
            if buf_t is not None and not buf_t.isEmpty() and area_test is None:
                area_test = buf_t.area()
            if (
                area_inter is None
                and buf_r is not None
                and buf_t is not None
                and not buf_r.isEmpty()
                and not buf_t.isEmpty()
            ):
                inter = buf_t.intersection(buf_r)
                if inter and not inter.isEmpty():
                    area_inter = inter.area()

        if shared_limits:
            ax.set_xlim(shared_limits[0], shared_limits[1])
            ax.set_ylim(shared_limits[2], shared_limits[3])
        ax.set_clip_on(False)

        eq_m = eq_lookup.get(int(scale), '')
        ax.set_title(
            f'Classe {class_}  |  PEC-PCD (Vertical)={pec_v:.1f} m'
            f'  |  EQ = {eq_m}m  |  dm = {dm_str}',
            fontsize=8,
            loc='left',
        )

        def _area_fmt(v, label):
            return (
                f'{label} = {v:.1f} m²'
                if v is not None and math.isfinite(float(v))
                else f'{label} = n/d'
            )

        ax.text(
            0.5,
            0.97,
            '  |  '.join([
                _area_fmt(area_ref, 'Área Ref'),
                _area_fmt(area_test, 'Área Teste'),
                _area_fmt(area_inter, 'Área Inter'),
            ]),
            transform=ax.transAxes,
            fontsize=6,
            va='top',
            ha='center',
            bbox={
                'boxstyle': 'round,pad=0.25',
                'facecolor': 'white',
                'edgecolor': 'none',
                'alpha': 0.15,
            },
            zorder=10,
        )
        ax.tick_params(labelsize=6, pad=1)
        ax.grid(True, alpha=0.25)
        if i == 0:
            # Ordem fixa da legenda (inferior esquerda do perfil classe A)
            legend_order = [
                'Ref',
                'Buffer Ref',
                'Teste original',
                'Teste compat.',
                'Buffer Teste c.',
            ]
            by_label = {
                lab: hand
                for hand, lab in zip(*ax.get_legend_handles_labels())
            }
            handles = [by_label[lab] for lab in legend_order if lab in by_label]
            labels = [lab for lab in legend_order if lab in by_label]
            if handles:
                ax.legend(
                    handles,
                    labels,
                    loc='lower left',
                    fontsize=5.5,
                    framealpha=0.9,
                    ncol=5,
                    columnspacing=0.9,
                    handlelength=1.6,
                    handletextpad=0.4,
                    borderpad=0.35,
                    labelspacing=0.2,
                )
        if i == len(CLASS_ORDER) - 1:
            ax.set_xlabel('Progressiva (m)', fontsize=7)
        ax.set_ylabel('Cota (m)', fontsize=7, labelpad=2)

    fig.suptitle(title, fontsize=9, x=0.545)
    _add_page_logo(fig)
    fig.text(
        0.98,
        0.008,
        (
            '*O comportamento estranho do buffer no início e final de cada perfil '
            'é decorrente da diferença entre as escalas Horizontal e Vertical.'
        ),
        ha='right',
        va='bottom',
        fontsize=6.5,
    )
    pdf.savefig(fig)
    plt.close(fig)


def _pec_h_of(scale, class_, pec_h_table=None):
    if pec_h_table is not None:
        raw = (pec_h_table.get(int(scale)) or {}).get(class_)
        if isinstance(raw, dict):
            return float(raw.get('pec', 0.0))
        return float(raw) if raw is not None else 0.0
    return float(int(scale) * DIC_PEC_MM['H'][class_]['pec'])


def _draw_page_horizontal(
    pdf,
    *,
    test_name,
    layer_ref,
    id_ref,
    id_test,
    scale,
    geom_r,
    geom_t,
    dm_by_class,
    norm_label='Compatibilização Linear',
    norm_type=NORM_SCALE,
    pec_h_table=None,
    k_t=None,
):
    """Página planimétrica: matriz 2×2 (A B / C D) com buffers H e áreas."""
    geom_r = orient_line_high_to_low(geom_r)
    geom_t = orient_line_high_to_low(geom_t)
    k_str = f'{k_t:.2f}' if k_t is not None and math.isfinite(k_t) else 'n/d'
    show_k = int(norm_type) == NORM_SCALE and k_str != 'n/d'
    title = (
        f'{test_name}; 1:{int(scale) * 1000}; {norm_label}; {layer_ref}; '
        f'id_ref={id_ref}; id_test={id_test}'
    )
    if show_k:
        title += f'; fator de escala k={k_str}'

    pec_h_max = max(_pec_h_of(scale, c, pec_h_table) for c in CLASS_ORDER)
    extent_geoms = [g for g in (geom_r, geom_t) if g and not g.isEmpty()]
    for g in (geom_r, geom_t):
        if g is None or g.isEmpty():
            continue
        buf = _line_buffer_round(g, pec_h_max)
        if buf is not None and not buf.isEmpty():
            extent_geoms.append(buf)
    shared_bbox = _bbox_from_geoms(extent_geoms)
    shared_limits = None
    if shared_bbox:
        xmin, xmax, ymin, ymax = shared_bbox
        pad = max((xmax - xmin) * 0.05, (ymax - ymin) * 0.05, pec_h_max * 0.5, 10.0)
        shared_limits = (xmin - pad, xmax + pad, ymin - pad, ymax + pad)

    fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
    gs = GridSpec(
        2,
        2,
        figure=fig,
        left=0.06,
        right=0.985,
        top=0.92,
        bottom=0.07,
        wspace=0.12 / 3,
        hspace=0.22 * 2 / 3,
    )

    for i, class_ in enumerate(CLASS_ORDER):
        ax = fig.add_subplot(gs[i // 2, i % 2])
        pec_h = _pec_h_of(scale, class_, pec_h_table)
        attrs = dm_by_class.get(class_) or {}
        dm_h = attrs.get('DM_H')
        dm_str = f'{dm_h:.2f}' if dm_h is not None else 'n/d'

        area_ref = attrs.get('Area_Ref')
        area_test = attrs.get('Area_Test')
        area_inter = attrs.get('Area_Inter')
        buf_r = buf_t = None

        if geom_r is not None and not geom_r.isEmpty():
            buf_r = _line_buffer_round(geom_r, pec_h)
            _plot_polygon(
                ax,
                buf_r,
                facecolor=COLOR_BUF_REF,
                edgecolor=COLOR_BUF_REF,
                alpha=0.28,
                linewidth=0.45,
                zorder=1,
                label='Buffer Ref',
            )
            _plot_line(
                ax, geom_r, color=COLOR_REF, linewidth=0.75, label='Referência', zorder=4
            )
            if buf_r is not None and not buf_r.isEmpty() and area_ref is None:
                area_ref = buf_r.area()

        if geom_t is not None and not geom_t.isEmpty():
            buf_t = _line_buffer_round(geom_t, pec_h)
            _plot_polygon(
                ax,
                buf_t,
                facecolor=COLOR_BUF_TEST,
                edgecolor=COLOR_BUF_TEST,
                alpha=0.22,
                linewidth=0.45,
                zorder=2,
                label='Buffer Teste',
            )
            _plot_line(
                ax, geom_t, color=COLOR_TEST, linewidth=0.65, label='Teste', zorder=5
            )
            if buf_t is not None and not buf_t.isEmpty() and area_test is None:
                area_test = buf_t.area()
            if (
                area_inter is None
                and buf_r is not None
                and buf_t is not None
                and not buf_r.isEmpty()
                and not buf_t.isEmpty()
            ):
                inter = buf_t.intersection(buf_r)
                if inter and not inter.isEmpty():
                    area_inter = inter.area()

        prog0 = _line_start_xy(geom_r)
        if prog0 is not None:
            ax.plot(
                prog0[0],
                prog0[1],
                marker='o',
                markersize=7 * 2 / 3,
                linestyle='None',
                color='#111111',
                markerfacecolor='#ffffff',
                markeredgecolor='#111111',
                markeredgewidth=0.8,
                label='Progressiva Zero',
                zorder=6,
            )

        if shared_limits:
            ax.set_xlim(shared_limits[0], shared_limits[1])
            ax.set_ylim(shared_limits[2], shared_limits[3])
            ax.set_aspect('equal', adjustable='datalim')
        else:
            _set_equal_aspect_pad(ax, [geom_r, geom_t])
        _apply_map_grid(ax, MAP_GRID_STEP_M)

        ax.set_title(
            f'Classe {class_}  |  PEC-PCD (Horizontal)={pec_h:.2f} m  |  dm = {dm_str}',
            fontsize=8,
            loc='left',
        )

        def _area_fmt(v, label):
            return (
                f'{label} = {v:.1f} m²'
                if v is not None and math.isfinite(float(v))
                else f'{label} = n/d'
            )

        ax.text(
            0.5,
            0.97,
            '  |  '.join([
                _area_fmt(area_ref, 'Área Ref'),
                _area_fmt(area_test, 'Área Teste'),
                _area_fmt(area_inter, 'Área Inter'),
            ]),
            transform=ax.transAxes,
            fontsize=6,
            va='top',
            ha='center',
            bbox={
                'boxstyle': 'round,pad=0.25',
                'facecolor': 'white',
                'edgecolor': 'none',
                'alpha': 0.15,
            },
            zorder=10,
        )
        ax.tick_params(labelsize=6, pad=1)
        for lab in ax.get_yticklabels():
            lab.set_rotation(90)
            lab.set_va('center')
            lab.set_ha('right')
        if i // 2 == 1:
            ax.set_xlabel('X (m)', fontsize=7)
        if i % 2 == 0:
            ax.set_ylabel('Norte (Y)', fontsize=7, rotation=90, labelpad=2)

        if i == 0:
            legend_order = [
                'Referência',
                'Buffer Ref',
                'Teste',
                'Buffer Teste',
                'Progressiva Zero',
            ]
            by_label = {
                lab: hand
                for hand, lab in zip(*ax.get_legend_handles_labels())
            }
            handles = [by_label[lab] for lab in legend_order if lab in by_label]
            labels = [lab for lab in legend_order if lab in by_label]
            if handles:
                ax.legend(
                    handles,
                    labels,
                    loc='lower left',
                    fontsize=5,
                    framealpha=0.9,
                    ncol=len(handles),
                    columnspacing=0.7,
                    handlelength=1.2,
                    handletextpad=0.3,
                    borderpad=0.25,
                )

    fig.suptitle(title, fontsize=9, x=0.545)
    _add_page_logo(fig)
    fig.text(
        0.98,
        0.008,
        '*Buffers planimétricos (PEC-PCD Horizontal) por classe A–D.',
        ha='right',
        va='bottom',
        fontsize=6.5,
    )
    pdf.savefig(fig)
    plt.close(fig)


def generate_pdf(
    gpkg_path=DEFAULT_GPKG,
    lines_gpkg=DEFAULT_LINES_GPKG,
    out_path=DEFAULT_OUT,
    scale=50,
    limit=None,
    model=None,
    norm_type=NORM_SCALE,
    result_layer=None,
    lines_cache=None,
    pair_index=None,
):
    if not os.path.isfile(gpkg_path):
        raise RuntimeError(f'Result.gpkg não encontrado:\n{gpkg_path}')
    if not os.path.isfile(lines_gpkg):
        raise RuntimeError(f'GPKG de linhas não encontrado:\n{lines_gpkg}')
    if int(scale) not in DIC_PEC_V:
        raise RuntimeError(f'Escala inválida: {scale} (use {sorted(DIC_PEC_V)})')

    own_result = result_layer is None
    if own_result:
        print(f'Carregando resultados: {gpkg_path}')
        result_layer = load_result_layer(gpkg_path)
        print(f'Feições Result: {result_layer.featureCount()}')

    if lines_cache is None:
        print(f'Carregando linhas: {lines_gpkg}')
        lines_cache = _load_layer_cache(lines_gpkg)
        print(f'Camadas de linhas: {sorted(lines_cache.keys())}')

    if pair_index is None:
        print('Indexando pares teste↔ref…')
        pair_index = _build_test_pair_index(lines_cache)
        print(f'Pares indexados: {len(pair_index)}')

    norm_label = _norm_label(norm_type)
    pairs = _unique_pairs_from_result(result_layer, scale, model=model)
    model_txt = model or 'TODOS'
    print(f'Pares {model_txt} @ escala {scale} (norm={norm_label}): {len(pairs)}')
    if limit is not None and limit > 0:
        pairs = pairs[: int(limit)]
    print(f'Pares a desenhar: {len(pairs)} → {out_path}')

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    drawn = 0
    skipped = 0

    with PdfPages(out_path) as pdf:
        for test_name, layer_ref, id_ref in pairs:
            if layer_ref not in lines_cache:
                print(f'  SKIP: camada ref ausente {layer_ref}')
                skipped += 1
                continue
            feat_r = lines_cache[layer_ref]['features'].get(id_ref)
            if feat_r is None or not feat_r.hasGeometry():
                print(f'  SKIP: id_ref={id_ref} não encontrado em {layer_ref}')
                skipped += 1
                continue
            geom_r = QgsGeometry(feat_r.geometry())

            key = (test_name, layer_ref, id_ref)
            paired = pair_index.get(key)
            if not paired:
                print(f'  SKIP: sem teste para {key}')
                skipped += 1
                continue
            id_test, geom_t = paired

            profiles_compat = build_compatibilized_profile_geometries(
                geom_r, geom_t, int(norm_type)
            )
            profiles_raw = build_compatibilized_profile_geometries(
                geom_r, geom_t, NORM_NONE
            )
            if not profiles_compat:
                print(f'  SKIP: perfis inválidos para {key}')
                skipped += 1
                continue

            dm_by_class = _dm_attrs_by_class(
                result_layer, test_name, layer_ref, id_ref, scale
            )
            if drawn == 0 or (drawn + 1) % 25 == 0:
                print(
                    f'  Página {drawn + 1}/{len(pairs)}: {test_name} | '
                    f'{layer_ref} | id_ref={id_ref}'
                )
            _draw_page(
                pdf,
                test_name=test_name,
                layer_ref=layer_ref,
                id_ref=id_ref,
                id_test=id_test,
                scale=scale,
                geom_r=geom_r,
                geom_t=geom_t,
                profiles_scale=profiles_compat,
                profiles_raw=profiles_raw,
                dm_by_class=dm_by_class,
                norm_label=norm_label,
                norm_type=norm_type,
            )
            drawn += 1

    print(f'PDF gravado: {out_path}  |  páginas={drawn}  |  ignorados={skipped}')
    return out_path, drawn, skipped


def generate_audit_pdfs_from_pairs(
    *,
    out_dir,
    test_name,
    pairs,
    scales,
    norm_type=NORM_SCALE,
    pec_v_table=None,
    eq_v_table=None,
    timestamp=None,
    log=None,
    progress=None,
):
    """
    Gera Audit_vertical_{modelo}_{linear|proximidade}_{escala}_{timestamp}.pdf

    pairs: lista de dicts com chaves
      id_ref, id_test, layer_ref, geom_r, geom_t,
      dm_by_scale (opcional): {scale: {class: {DM_V, Area_*}}}
    progress: callback opcional(msg) chamado após cada par×escala
    """
    _log = log or (lambda msg: print(msg))
    _progress = progress if callable(progress) else None
    if not pairs:
        _log('Auditoria vertical: sem pares homólogos.')
        return []
    os.makedirs(out_dir or '.', exist_ok=True)
    norm_label = _norm_label(norm_type)
    norm_slug = _norm_filename_slug(norm_type)
    pec_lookup = pec_v_table if pec_v_table is not None else DIC_PEC_V
    eq_lookup = eq_v_table if eq_v_table is not None else DIC_EQ_V
    safe_model = _safe_filename_token(test_name, 'MODELO')
    ts = str(timestamp).strip() if timestamp else ''
    if not ts:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')

    outputs = []
    for scale in scales:
        scale = int(scale)
        if scale not in pec_lookup:
            _log(f'Auditoria vertical: escala {scale} sem PEC-V — ignorada.')
            if _progress:
                for _pair in pairs:
                    _progress(f'V 1:{scale * 1000} (ignorada)')
            continue
        out_name = f'Audit_vertical_{safe_model}_{norm_slug}_{scale}_{ts}.pdf'
        out_path = os.path.join(out_dir, out_name)
        drawn = 0
        skipped = 0
        _log(f'Auditoria vertical: {len(pairs)} pares → {out_path}')
        with PdfPages(out_path) as pdf:
            for pair in pairs:
                geom_r = pair.get('geom_r')
                geom_t = pair.get('geom_t')
                id_ref = pair.get('id_ref')
                if geom_r is None or geom_t is None:
                    skipped += 1
                    if _progress:
                        _progress(f'V 1:{scale * 1000} id_ref={id_ref} (skip)')
                    continue
                if geom_r.isEmpty() or geom_t.isEmpty():
                    skipped += 1
                    if _progress:
                        _progress(f'V 1:{scale * 1000} id_ref={id_ref} (skip)')
                    continue
                profiles_compat = build_compatibilized_profile_geometries(
                    geom_r, geom_t, int(norm_type)
                )
                profiles_raw = build_compatibilized_profile_geometries(
                    geom_r, geom_t, NORM_NONE
                )
                if not profiles_compat:
                    skipped += 1
                    if _progress:
                        _progress(f'V 1:{scale * 1000} id_ref={id_ref} (skip)')
                    continue
                dm_by_scale = pair.get('dm_by_scale') or {}
                dm_by_class = dm_by_scale.get(scale) or dm_by_scale.get(str(scale)) or {}
                _draw_page(
                    pdf,
                    test_name=str(test_name),
                    layer_ref=str(pair.get('layer_ref') or ''),
                    id_ref=id_ref,
                    id_test=pair.get('id_test'),
                    scale=scale,
                    geom_r=geom_r,
                    geom_t=geom_t,
                    profiles_scale=profiles_compat,
                    profiles_raw=profiles_raw,
                    dm_by_class=dm_by_class,
                    norm_label=norm_label,
                    norm_type=norm_type,
                    pec_v_table=pec_lookup,
                    eq_v_table=eq_lookup,
                )
                drawn += 1
                if _progress:
                    _progress(f'V 1:{scale * 1000} id_ref={id_ref}')
        _log(f'PDF gravado: {out_path}  |  páginas={drawn}  |  ignorados={skipped}')
        outputs.append((out_path, drawn, skipped))
    return outputs


def generate_audit_horizontal_pdfs_from_pairs(
    *,
    out_dir,
    test_name,
    pairs,
    scales,
    norm_type=NORM_SCALE,
    pec_h_table=None,
    timestamp=None,
    log=None,
    progress=None,
):
    """
    Gera Audit_horizontal_{modelo}_{escala}_{timestamp}.pdf
    (matriz 2×2 por classe; método de compatibilização não entra no nome).
    progress: callback opcional(msg) chamado após cada par×escala
    """
    _log = log or (lambda msg: print(msg))
    _progress = progress if callable(progress) else None
    if not pairs:
        _log('Auditoria horizontal: sem pares homólogos.')
        return []
    os.makedirs(out_dir or '.', exist_ok=True)
    norm_label = _norm_label(norm_type)
    safe_model = _safe_filename_token(test_name, 'MODELO')
    ts = str(timestamp).strip() if timestamp else ''
    if not ts:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')

    outputs = []
    for scale in scales:
        scale = int(scale)
        out_name = f'Audit_horizontal_{safe_model}_{scale}_{ts}.pdf'
        out_path = os.path.join(out_dir, out_name)
        drawn = 0
        skipped = 0
        _log(f'Auditoria horizontal: {len(pairs)} pares → {out_path}')
        with PdfPages(out_path) as pdf:
            for pair in pairs:
                geom_r = pair.get('geom_r')
                geom_t = pair.get('geom_t')
                id_ref = pair.get('id_ref')
                if geom_r is None or geom_t is None:
                    skipped += 1
                    if _progress:
                        _progress(f'H 1:{scale * 1000} id_ref={id_ref} (skip)')
                    continue
                if geom_r.isEmpty() or geom_t.isEmpty():
                    skipped += 1
                    if _progress:
                        _progress(f'H 1:{scale * 1000} id_ref={id_ref} (skip)')
                    continue
                k_t = None
                if int(norm_type) == NORM_SCALE:
                    profiles = build_compatibilized_profile_geometries(
                        geom_r, geom_t, int(norm_type)
                    )
                    if profiles:
                        k_t = profiles.get('k_t')
                dm_by_scale = pair.get('dm_by_scale') or {}
                dm_by_class = dm_by_scale.get(scale) or dm_by_scale.get(str(scale)) or {}
                _draw_page_horizontal(
                    pdf,
                    test_name=str(test_name),
                    layer_ref=str(pair.get('layer_ref') or ''),
                    id_ref=id_ref,
                    id_test=pair.get('id_test'),
                    scale=scale,
                    geom_r=geom_r,
                    geom_t=geom_t,
                    dm_by_class=dm_by_class,
                    norm_label=norm_label,
                    norm_type=norm_type,
                    pec_h_table=pec_h_table,
                    k_t=k_t,
                )
                drawn += 1
                if _progress:
                    _progress(f'H 1:{scale * 1000} id_ref={id_ref}')
        _log(f'PDF gravado: {out_path}  |  páginas={drawn}  |  ignorados={skipped}')
        outputs.append((out_path, drawn, skipped))
    return outputs


def generate_horizontal_pdf(
    gpkg_path=DEFAULT_GPKG,
    lines_gpkg=DEFAULT_LINES_GPKG,
    out_path=DEFAULT_OUT_H,
    scale=50,
    limit=None,
    model=None,
    norm_type=NORM_SCALE,
    result_layer=None,
    lines_cache=None,
    pair_index=None,
):
    """PDF planimétrico (2×2) a partir de Result.gpkg — usado no smoke."""
    if not os.path.isfile(gpkg_path):
        raise RuntimeError(f'Result.gpkg não encontrado:\n{gpkg_path}')
    if not os.path.isfile(lines_gpkg):
        raise RuntimeError(f'GPKG de linhas não encontrado:\n{lines_gpkg}')

    own_result = result_layer is None
    if own_result:
        print(f'Carregando resultados: {gpkg_path}')
        result_layer = load_result_layer(gpkg_path)
        print(f'Feições Result: {result_layer.featureCount()}')

    if lines_cache is None:
        print(f'Carregando linhas: {lines_gpkg}')
        lines_cache = _load_layer_cache(lines_gpkg)

    if pair_index is None:
        print('Indexando pares teste↔ref…')
        pair_index = _build_test_pair_index(lines_cache)

    norm_label = _norm_label(norm_type)
    pairs = _unique_pairs_from_result(result_layer, scale, model=model)
    model_txt = model or 'TODOS'
    print(f'Pares H {model_txt} @ escala {scale} (norm={norm_label}): {len(pairs)}')
    if limit is not None and limit > 0:
        pairs = pairs[: int(limit)]
    print(f'Pares a desenhar: {len(pairs)} → {out_path}')

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    drawn = 0
    skipped = 0
    with PdfPages(out_path) as pdf:
        for test_name, layer_ref, id_ref in pairs:
            if layer_ref not in lines_cache:
                skipped += 1
                continue
            feat_r = lines_cache[layer_ref]['features'].get(id_ref)
            if feat_r is None or not feat_r.hasGeometry():
                skipped += 1
                continue
            geom_r = QgsGeometry(feat_r.geometry())
            key = (test_name, layer_ref, id_ref)
            paired = pair_index.get(key)
            if not paired:
                skipped += 1
                continue
            id_test, geom_t = paired
            k_t = None
            if int(norm_type) == NORM_SCALE:
                profiles = build_compatibilized_profile_geometries(
                    geom_r, geom_t, int(norm_type)
                )
                if profiles:
                    k_t = profiles.get('k_t')
            dm_by_class = _dm_attrs_by_class(
                result_layer, test_name, layer_ref, id_ref, scale
            )
            if drawn == 0 or (drawn + 1) % 25 == 0:
                print(
                    f'  Página {drawn + 1}/{len(pairs)}: {test_name} | '
                    f'{layer_ref} | id_ref={id_ref}'
                )
            _draw_page_horizontal(
                pdf,
                test_name=test_name,
                layer_ref=layer_ref,
                id_ref=id_ref,
                id_test=id_test,
                scale=scale,
                geom_r=geom_r,
                geom_t=geom_t,
                dm_by_class=dm_by_class,
                norm_label=norm_label,
                norm_type=norm_type,
                k_t=k_t,
            )
            drawn += 1

    print(f'PDF gravado: {out_path}  |  páginas={drawn}  |  ignorados={skipped}')
    return out_path, drawn, skipped


def generate_pdfs_for_results_dir(
    results_dir,
    lines_gpkg=DEFAULT_LINES_GPKG,
    norm_type=None,
    limit=None,
    models=None,
    scales=None,
):
    """Gera Audit_vertical_{MODELO}_{linear|proximidade}_{ESCALA}_{ts}.pdf."""
    gpkg_path = os.path.join(results_dir, 'Result.gpkg')
    if not os.path.isfile(gpkg_path):
        raise RuntimeError(f'Result.gpkg não encontrado em:\n{results_dir}')

    norm_type = _resolve_norm_type(
        'auto' if norm_type is None else norm_type,
        results_dir=results_dir,
    )
    print(f'\n=== Pasta: {results_dir}  |  norm={_norm_label(norm_type)} ===')

    result_layer = load_result_layer(gpkg_path)
    print(f'Feições Result: {result_layer.featureCount()}')
    lines_cache = _load_layer_cache(lines_gpkg)
    pair_index = _build_test_pair_index(lines_cache)
    print(f'Pares indexados: {len(pair_index)}')

    ts = datetime.now().strftime('%Y-%m-%d_%H%M')
    norm_slug = _norm_filename_slug(norm_type)

    model_list = list(models) if models else _models_from_result(result_layer)
    outputs = []
    for model in model_list:
        scale_list = (
            list(scales)
            if scales
            else _scales_from_result(result_layer, model=model)
        )
        for scale in scale_list:
            safe_model = _safe_filename_token(model, 'MODELO')
            out_path = os.path.join(
                results_dir,
                f'Audit_vertical_{safe_model}_{norm_slug}_{int(scale)}_{ts}.pdf',
            )
            outputs.append(
                generate_pdf(
                    gpkg_path=gpkg_path,
                    lines_gpkg=lines_gpkg,
                    out_path=out_path,
                    scale=int(scale),
                    limit=limit,
                    model=model,
                    norm_type=norm_type,
                    result_layer=result_layer,
                    lines_cache=lines_cache,
                    pair_index=pair_index,
                )
            )
    return outputs


def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description='PDF de pares homólogos (vista 2D + perfis por classe).'
    )
    p.add_argument(
        'gpkg',
        nargs='?',
        default=None,
        help='Result.gpkg OU pasta Results (com --batch)',
    )
    p.add_argument(
        '--lines',
        default=DEFAULT_LINES_GPKG,
        help='GPKG com linhas Z de ref e teste (Selecao_v2_z.gpkg)',
    )
    p.add_argument('--out', default=None, help='Caminho do PDF (modo simples)')
    p.add_argument('--scale', type=int, default=None, choices=sorted(DIC_PEC_V.keys()))
    p.add_argument('--model', default=None, help='Filtrar modelo (ex.: ANADEM)')
    p.add_argument(
        '--norm',
        default='auto',
        help='scale|linear|less_dist|proximidade|none|… (ou 0|1|2); auto infere pela pasta',
    )
    p.add_argument(
        '--limit',
        type=int,
        default=0,
        help='Nº máx. de pares (0 = todos)',
    )
    p.add_argument(
        '--batch',
        action='store_true',
        help='Gera um PDF por modelo×escala na pasta do Result.gpkg',
    )
    p.add_argument(
        '--axis',
        choices=('vertical', 'horizontal', 'v', 'h'),
        default='vertical',
        help='vertical=perfis A–D empilhados; horizontal=matriz 2×2 planimétrica',
    )
    p.add_argument(
        '--results-dirs',
        nargs='+',
        default=None,
        help='Pastas Results para --batch (ex.: Results/Geral_linear Results/Geral_proximidade)',
    )
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    limit = None if args.limit == 0 else args.limit
    init_standalone_qgis()
    try:
        if args.results_dirs:
            for results_dir in args.results_dirs:
                generate_pdfs_for_results_dir(
                    results_dir,
                    lines_gpkg=args.lines,
                    norm_type=args.norm,
                    limit=limit,
                    models=[args.model] if args.model else None,
                    scales=[args.scale] if args.scale else None,
                )
            return

        gpkg_path = args.gpkg or DEFAULT_GPKG
        if os.path.isdir(gpkg_path):
            results_dir = gpkg_path
            gpkg_path = os.path.join(results_dir, 'Result.gpkg')
            batch = True
        else:
            results_dir = os.path.dirname(os.path.abspath(gpkg_path))
            batch = bool(args.batch)

        if batch:
            generate_pdfs_for_results_dir(
                results_dir,
                lines_gpkg=args.lines,
                norm_type=args.norm,
                limit=limit,
                models=[args.model] if args.model else None,
                scales=[args.scale] if args.scale else None,
            )
            return

        norm_type = _resolve_norm_type(args.norm, results_dir=results_dir)
        scale = args.scale if args.scale is not None else 50
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        axis = str(args.axis).lower()
        is_h = axis in ('horizontal', 'h')
        prefix = 'Audit_horizontal' if is_h else 'Audit_vertical'
        if is_h:
            out_path = args.out or os.path.join(
                results_dir,
                f'{prefix}_{_safe_filename_token(args.model or "ALL")}_{scale}_{ts}.pdf',
            )
        else:
            out_path = args.out or os.path.join(
                results_dir,
                f'{prefix}_{_safe_filename_token(args.model or "ALL")}'
                f'_{_norm_filename_slug(norm_type)}_{scale}_{ts}.pdf',
            )
        if is_h:
            generate_horizontal_pdf(
                gpkg_path=gpkg_path,
                lines_gpkg=args.lines,
                out_path=out_path,
                scale=scale,
                limit=limit,
                model=args.model,
                norm_type=norm_type,
            )
        else:
            generate_pdf(
                gpkg_path=gpkg_path,
                lines_gpkg=args.lines,
                out_path=out_path,
                scale=scale,
                limit=limit,
                model=args.model,
                norm_type=norm_type,
            )
    finally:
        exit_standalone_qgis()


if __name__ == '__main__':
    main()
