import os
import shutil
import sqlite3
import uuid
import tempfile
import math
import statistics

from osgeo.ogr import wkbTIN
from qgis.PyQt.QtCore import QThread, pyqtSignal, QObject
from qgis import processing
from qgis.core import (QgsApplication, QgsCoordinateReferenceSystem, QgsFeature, QgsVectorFileWriter,
                       QgsFields, QgsField, QgsVectorLayer, QgsRasterLayer, QgsCoordinateTransformContext,
                       QgsWkbTypes, QgsGeometry, QgsLineString, QgsPointXY, QgsProcessingContext,
                       QgsProcessingFeedback, QgsMapLayer, QgsProject)

# |dm_h| ou |dm_v| acima disto é tratado como erro numérico / geometria; → NaN e WARNING no log.
DM_ABS_MAX_SANE = 1000.0
PROFILE_PROG_OFFSET = 10000.0

# Fórmula DM (buffer duplo) — índice em step_dm_formula / dm_formula
DM_FORMULA_ORIGINAL = 0  # eq:dm-buffer-duplo
DM_FORMULA_MEDIA = 1  # eq:dm-buffer-duplo-media

try:
    from .mod_pec_constants import (
        ACCURACY_STANDARD_BR,
        ACCURACY_STANDARD_CE90,
        CE90_THRESHOLD_DECIMALS,
        CLASS_CE90,
        CLASS_LE90,
        EP_RATIO_H,
        EP_RATIO_V,
        ce90_threshold_decimals,
    )
except ImportError:  # pragma: no cover
    ACCURACY_STANDARD_BR = 0
    ACCURACY_STANDARD_CE90 = 1
    CE90_THRESHOLD_DECIMALS = 1
    CLASS_CE90 = 'CE90'
    CLASS_LE90 = 'LE90'
    EP_RATIO_H = 0.17 / 0.28
    EP_RATIO_V = 0.17 / 0.27

    def ce90_threshold_decimals(pixel_m):
        try:
            px = float(pixel_m)
        except (TypeError, ValueError):
            return CE90_THRESHOLD_DECIMALS
        if px > 0.0 and px < 5.0:
            return 2
        return CE90_THRESHOLD_DECIMALS


def _ce90_pec_limit(pec_, decimals=CE90_THRESHOLD_DECIMALS):
    try:
        return round(float(pec_), int(decimals))
    except (TypeError, ValueError):
        return 0.0


def _ce90_check_norm(values):
    if len(values) < 3:
        return False
    try:
        from scipy.stats import shapiro
        result = shapiro(values)
        return result[0] >= result[1]
    except Exception:
        return False


def _ce90_mark_outliers_iqr(group, dm_key, outlier_key):
    vals = []
    for rec in group.values():
        try:
            v = float(rec.get(dm_key))
        except (TypeError, ValueError):
            continue
        if math.isfinite(v):
            vals.append(v)
    if len(vals) < 2:
        for rec in group.values():
            rec[outlier_key] = False
        return
    quant_ = statistics.quantiles(data=vals)
    iqr_ = quant_[2] - quant_[0]
    ls_ = quant_[2] + 1.5 * iqr_
    li_ = quant_[0] - 1.5 * iqr_
    for rec in group.values():
        try:
            v = float(rec.get(dm_key))
        except (TypeError, ValueError):
            rec[outlier_key] = False
            continue
        if not math.isfinite(v):
            rec[outlier_key] = False
        elif v < li_ or v > ls_:
            rec[outlier_key] = True
        else:
            rec[outlier_key] = False


def _ce90_group_evaluate(group, dm_key, pec_raw, ep_raw, decimals=CE90_THRESHOLD_DECIMALS):
    """Avalia grupo CE90/LE90. Retorna (ok, info)."""
    outlier_key = 'outlier_h' if dm_key == 'dm_h' else 'outlier_v'
    _ce90_mark_outliers_iqr(group, dm_key, outlier_key)
    values = []
    extents = []
    n_out = 0
    for rec in group.values():
        if rec.get(outlier_key):
            n_out += 1
            continue
        try:
            v = float(rec.get(dm_key))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(v):
            continue
        values.append(v)
        try:
            ext = float(rec.get('extent_ref') or 0.0)
        except (TypeError, ValueError):
            ext = 0.0
        extents.append(ext if math.isfinite(ext) and ext > 0 else 0.0)

    pec_lim = _ce90_pec_limit(pec_raw, decimals)
    try:
        ep_v = float(ep_raw)
    except (TypeError, ValueError):
        ep_v = float('nan')
    info = {
        'n_outliers': n_out,
        'n_valid': len(values),
        'pec_lim': pec_lim,
        'ep': ep_v,
        'perc_q': 0.0,
        'perc_e': 0.0,
        'rms': float('nan'),
        'norm_ok': False,
        'pec_ok_q': False,
        'pec_ok_e': False,
        'ep_ok': False,
    }
    if not values:
        return False, info
    info['norm_ok'] = _ce90_check_norm(values)
    if not info['norm_ok']:
        return False, info
    perc_q = sum(1 for v in values if v <= pec_lim) / len(values)
    total_ext = sum(extents)
    if total_ext <= 0:
        perc_e = 0.0
    else:
        perc_e = sum(
            ext for v, ext in zip(values, extents) if v <= pec_lim
        ) / total_ext
    rms_ = math.sqrt(sum(v * v for v in values) / len(values))
    info['perc_q'] = perc_q
    info['perc_e'] = perc_e
    info['rms'] = rms_
    info['pec_ok_q'] = perc_q >= 0.90
    info['pec_ok_e'] = perc_e >= 0.90
    info['ep_ok'] = math.isfinite(rms_) and math.isfinite(ep_v) and rms_ <= ep_v
    ok = info['pec_ok_q'] and info['pec_ok_e'] and info['ep_ok']
    return ok, info


def _ce90_group_passes(group, dm_key, pec_raw, ep_raw, decimals=CE90_THRESHOLD_DECIMALS):
    """Aprova se normalidade + PEC quant/ext ≥90% + RMS ≤ EP."""
    ok, _info = _ce90_group_evaluate(
        group, dm_key, pec_raw, ep_raw, decimals=decimals)
    return ok


def calc_dm_buffer_duplo(area_a1, area_a2, area_a3, x, formula=DM_FORMULA_ORIGINAL):
    """
    Discrepância média por buffer duplo.

    A1 — área do buffer da feição de teste
    A2 — área do buffer da feição de referência
    A3 — área da interseção dos buffers
    x  — PEC (raio do buffer) da escala/classe

    formula 0 (eq:dm-buffer-duplo):
      dm = π · x · (A2 − A3) / A1
    formula 1 (eq:dm-buffer-duplo-media):
      dm = π · x · ((A1+A2)/2 − A3) / ((A1+A2)/2)
    """
    try:
        a1 = float(area_a1)
        a2 = float(area_a2)
        a3 = float(area_a3)
        xv = float(x)
    except (TypeError, ValueError, OverflowError):
        return float('nan')
    if not (
        math.isfinite(a1)
        and math.isfinite(a2)
        and math.isfinite(a3)
        and math.isfinite(xv)
    ):
        return float('nan')
    try:
        formula_i = int(formula)
    except (TypeError, ValueError):
        formula_i = DM_FORMULA_ORIGINAL
    if formula_i == DM_FORMULA_MEDIA:
        denom = 0.5 * (a1 + a2)
        if denom <= 0:
            return float('nan')
        dm = math.pi * xv * (denom - a3) / denom
    else:
        if a1 <= 0:
            return float('nan')
        dm = math.pi * xv * (a2 - a3) / a1
    if not math.isfinite(dm):
        return float('nan')
    return dm


def _profile_line_points(geom: QgsGeometry):
    wkbt = geom.wkbType()
    if wkbt == QgsWkbTypes.LineString or wkbt == QgsWkbTypes.LineStringZ:
        return geom.constGet().points()
    return geom.constGet()[0].points()


def orient_line_high_to_low(geom):
    """
    Garante progressiva 0 no extremo de maior cota (montante / crista→vale).

    Se Z do 1.º vértice <= Z do último, inverte a linha. Aplica-se a hidrografia
    e a cumeadas. Sem Z finitos, devolve a geometria inalterada.
    """
    if geom is None:
        return geom
    g = QgsGeometry(geom)
    if g.isEmpty():
        return g
    try:
        pts = _profile_line_points(g)
    except Exception:
        return g
    if len(pts) < 2:
        return g
    try:
        z0 = float(pts[0].z())
        z1 = float(pts[-1].z())
    except Exception:
        return g
    if not math.isfinite(z0) or not math.isfinite(z1):
        return g
    if z0 > z1:
        return g
    return QgsGeometry(QgsLineString(list(reversed(pts))))


def build_compatibilized_profile_geometries(geom_r, geom_t, norm_type: int):
    """
    Perfil (progressiva, cota) após compatibilização — mesma lógica que BufferThread.calc_dm_v.

    Antes do cálculo, ref e teste são orientadas alta→baixa (orient_line_high_to_low).

    norm_type:
      0 scale — progressiva na teste, escalada por k=len_r/len_t; inverte se digitada
                em sentido oposto à ref (ci).
      1 less_dist — progressiva = projeção na ref (lineLocatePoint); sem inversão.
      2 none — progressiva na teste sem escala; inverte se ci.

    Devolve dict com geom_prof_r, geom_prof_t, k_t ou None se inválido.
    """
    geom_r = orient_line_high_to_low(QgsGeometry(geom_r))
    geom_t = orient_line_high_to_low(QgsGeometry(geom_t))
    len_r = geom_r.length()
    len_t = geom_t.length()
    if (
        not math.isfinite(len_r)
        or not math.isfinite(len_t)
        or len_r <= 0
        or len_t <= 0
    ):
        return None

    ps_r = _profile_line_points(geom_r)
    ps_t = _profile_line_points(geom_t)
    if not ps_r or not ps_t:
        return None

    gpr0 = QgsGeometry.fromPointXY(QgsPointXY(ps_r[0]))
    gpr1 = QgsGeometry.fromPointXY(QgsPointXY(ps_r[-1]))

    list_prof_r = []
    for p_ in ps_r:
        dist_ = round(geom_r.lineLocatePoint(QgsGeometry(p_)), 2)
        z_ = round(p_.z(), 2)
        list_prof_r.append(QgsPointXY(dist_ + PROFILE_PROG_OFFSET, z_))
    geom_prof_r = QgsGeometry.fromPolylineXY(list_prof_r)

    k_t = len_r / len_t
    if not math.isfinite(k_t) or abs(k_t) > 1e12:
        return None

    gpt0 = QgsGeometry.fromPointXY(QgsPointXY(ps_t[0]))
    ci = gpt0.distance(gpr0) > gpt0.distance(gpr1)

    list_prog_cota_t = []
    for p_ in ps_t:
        z_ = round(p_.z(), 2)
        if norm_type == 0:
            dist_ = geom_t.lineLocatePoint(QgsGeometry(p_))
            if ci:
                dist_ = round((len_t - dist_) * k_t, 2)
            else:
                dist_ = round(dist_ * k_t, 2)
        elif norm_type == 1:
            # Progressiva pela projeção na ref — sem inversão (eixo já é o da ref).
            dist_ = round(geom_r.lineLocatePoint(QgsGeometry(p_)), 2)
        else:
            dist_ = geom_t.lineLocatePoint(QgsGeometry(p_))
            if ci:
                dist_ = round(len_t - dist_, 2)
            else:
                dist_ = round(dist_, 2)
        if not list_prog_cota_t:
            list_prog_cota_t.append([dist_ + PROFILE_PROG_OFFSET, z_])
        elif dist_ != list_prog_cota_t[-1][0]:
            list_prog_cota_t.append([dist_ + PROFILE_PROG_OFFSET, z_])

    list_prog_cota_t.sort()
    list_prof_t = [
        QgsPointXY(float(vet_[0]), float(vet_[1]))
        for vet_ in list_prog_cota_t
    ]
    geom_prof_t = QgsGeometry.fromPolylineXY(list_prof_t)

    return {
        'geom_prof_r': geom_prof_r,
        'geom_prof_t': geom_prof_t,
        'k_t': k_t,
    }


def _finite_dm_scalar(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    if not math.isfinite(v) or abs(v) > DM_ABS_MAX_SANE:
        return None
    return v

_GRASS_PROVIDER_IDS = ('grass', 'grass7')
_MORPHOLOGY_GRASS_ALGORITHMS = ('r.watershed', 'r.to.vect', 'v.to.lines', 'r.thin')


def _run_processing(alg_or_name, params, feedback=None, context=None):
    """
    processing.run seguro em QThread.

    Sem context, o Processing chama createContext() → iface.mapCanvas().fullExtent()
    na thread de fundo → access violation no Windows.
    """
    if context is None:
        context = QgsProcessingContext()
    if feedback is None:
        feedback = QgsProcessingFeedback()
    return processing.run(alg_or_name, params, context=context, feedback=feedback)


def resolve_grass_algorithm(tool_name: str) -> str:
    """Resolve grass:tool vs grass7:tool depending on the installed QGIS version."""
    registry = QgsApplication.processingRegistry()
    for provider_id in _GRASS_PROVIDER_IDS:
        algo_id = f'{provider_id}:{tool_name}'
        if registry.algorithmById(algo_id):
            return algo_id
    raise RuntimeError(
        f'Algoritmo GRASS não disponível: {tool_name}. '
        'Instale ou ative o provider GRASS em Configurações → Processamento → Providers.'
    )


def resolve_morphology_grass_tools():
    """Return dict short_name -> full algorithm id; raises if any tool is missing."""
    return {name: resolve_grass_algorithm(name) for name in _MORPHOLOGY_GRASS_ALGORITHMS}


def inspect_grass_processing():
    """Diagnóstico do provider GRASS no Processing (instalação, ativação, algoritmos de morfologia)."""
    registry = QgsApplication.processingRegistry()
    info = {
        'ok': False,
        'provider_id': '',
        'provider_long_name': '',
        'provider_active': False,
        'provider_can_activate': False,
        'algorithms': {name: None for name in _MORPHOLOGY_GRASS_ALGORITHMS},
    }
    provider = None
    for provider_id in _GRASS_PROVIDER_IDS:
        provider = registry.providerById(provider_id)
        if provider is not None:
            info['provider_id'] = provider_id
            break
    if provider is None:
        return info
    try:
        info['provider_long_name'] = provider.longName() or provider_id
    except Exception:
        info['provider_long_name'] = info['provider_id']
    try:
        info['provider_can_activate'] = bool(provider.canBeActivated())
    except Exception:
        info['provider_can_activate'] = False
    try:
        info['provider_active'] = bool(provider.isActive())
    except Exception:
        info['provider_active'] = False
    if info['provider_active']:
        for name in _MORPHOLOGY_GRASS_ALGORITHMS:
            for pid in _GRASS_PROVIDER_IDS:
                algo_id = f'{pid}:{name}'
                if registry.algorithmById(algo_id):
                    info['algorithms'][name] = algo_id
                    break
        info['ok'] = all(info['algorithms'].values())
    return info


def _recommend_grass_memory_gb(mem_status) -> float:
    """Valor sugerido para max_memo_grass (GB) conforme RAM total do PC."""
    if not mem_status:
        return 1.0
    total = mem_status['total_mb']
    if total <= 8192:
        return 0.5
    if total <= 12288:
        return 1.0
    if total <= 16384:
        return 1.5
    if total <= 32768:
        return 3.0
    return 4.0


def _grass_memory_advice(mem_status, configured_gb: float) -> str:
    """Texto de recomendação para o log do plugin."""
    if not mem_status:
        return ''
    rec = _recommend_grass_memory_gb(mem_status)
    lines = [
        f'RAM total: {mem_status["total_mb"]} MB (~{mem_status["total_mb"] / 1024:.1f} GB).',
        f'Recomendação para «Limite de Memória Grass GIS»: {rec:g} GB '
        f'(definição atual: {configured_gb:g} GB).',
    ]
    if configured_gb > rec:
        lines.append(
            f'A definição atual ({configured_gb:g} GB) é alta para este PC — '
            f'risco de falha no r.watershed com RAM já em {mem_status["load_pct"]}%.'
        )
    if mem_status['load_pct'] >= 80:
        need_free = max(2048, int(mem_status['total_mb'] * 0.35))
        lines.append(
            f'Antes da morfologia, liberte RAM até ter pelo menos ~{need_free} MB livres '
            f'(agora: {mem_status["avail_mb"]} MB).'
        )
    return ' '.join(lines)


def _windows_memory_status():
    """RAM livre e % em uso (Windows). Retorna None se indisponível."""
    if os.name != 'nt':
        return None
    try:
        import ctypes

        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ('dwLength', ctypes.c_ulong),
                ('dwMemoryLoad', ctypes.c_ulong),
                ('ullTotalPhys', ctypes.c_ulonglong),
                ('ullAvailPhys', ctypes.c_ulonglong),
                ('ullTotalPageFile', ctypes.c_ulonglong),
                ('ullAvailPageFile', ctypes.c_ulonglong),
                ('ullTotalVirtual', ctypes.c_ulonglong),
                ('ullAvailVirtual', ctypes.c_ulonglong),
                ('ullAvailExtendedVirtual', ctypes.c_ulonglong),
            ]

        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
            return None
        return {
            'load_pct': int(stat.dwMemoryLoad),
            'total_mb': int(stat.ullTotalPhys // (1024 * 1024)),
            'avail_mb': int(stat.ullAvailPhys // (1024 * 1024)),
        }
    except Exception:
        return None


def _cap_grass_memory_mb(configured_mb, mem_status):
    """
    Limita o parâmetro GRASS memory à RAM livre (o GRASS também usa RAM fora desse valor).
    Retorna (mb_ajustado, mensagem_aviso ou '').
    """
    if not mem_status:
        return configured_mb, ''
    avail = mem_status['avail_mb']
    load = mem_status['load_pct']
    # Reservar margem para QGIS/GDAL/SO; usar até ~55% da RAM livre reportada.
    safe_cap = max(512, int(avail * 0.55))
    capped = min(configured_mb, safe_cap)
    capped = max(512, (capped // 256) * 256)
    notes = []
    if load >= 85:
        notes.append(
            f'RAM do sistema em {load}% ({avail} MB livres de {mem_status["total_mb"]} MB). '
            f'O r.watershed pode falhar — feche outras aplicações e tente de novo.'
        )
    if capped < configured_mb:
        notes.append(
            f'Parâmetro GRASS memory ajustado de {configured_mb} para {capped} MB '
            f'(RAM livre: {avail} MB).'
        )
    return capped, '\n'.join(notes)


def _grass_watershed_memory_efficient(configured_mb, effective_mb, mem_status) -> bool:
    """Modo -m (mais lento, menos RAM) quando a carga do sistema é elevada."""
    if not mem_status:
        return False
    if mem_status['load_pct'] >= 75:
        return True
    if configured_mb >= 2048 and effective_mb < configured_mb * 0.65:
        return True
    return False


class _ProcessingFeedbackCapture(QgsProcessingFeedback):
    """Captura mensagens do Processing/GRASS para diagnóstico em falhas silenciosas."""

    def __init__(self):
        super().__init__()
        self.lines = []

    def pushWarning(self, warning, *args, **kwargs):
        self.lines.append(('WARNING', str(warning)))
        super().pushWarning(warning)

    def pushInfo(self, info, *args, **kwargs):
        # QGIS 3.34: pushInfo(info) — sem parâmetro detailed (QGIS 4.2+).
        self.lines.append(('INFO', str(info)))
        super().pushInfo(info)

    def reportError(self, error, fatal=False, *args, **kwargs):
        self.lines.append(('ERROR', str(error)))
        try:
            super().reportError(error, fatal)
        except TypeError:
            super().reportError(error)

    def summary(self, max_lines=12) -> str:
        if not self.lines:
            return ''
        tail = self.lines[-max_lines:]
        return '\n  '.join(f'[{lvl}] {txt}' for lvl, txt in tail)


class PolygonThread(QThread):
    sig_status = pyqtSignal(dict, name='Status for processing bar')

    def __init__(self, main, parent, key_=None, dic_=None):
        QThread.__init__(self, parent)

        self.main = main
        self.parent = parent
        self.key_ = key_
        self.file_path = dic_['file_path']
        self.gpkg_path = dic_['gpkg']
        self.tab = dic_['layer']

        self.srid_ref = dic_['srid_ref']
        self.srid = dic_['srid']

        self.nr_procs = 6
        self.cur = None
        self.conn = None


    def run(self):
        self.sig_status.emit({'key': self.key_, 'quant': self.nr_procs})
        nr_ = 0
        tool_ = ''
        # Temp dir for native:* outputs (GPKG) — avoid layer objects in thread
        caminho_temp_poly = os.path.join(tempfile.gettempdir(), f'QGIS3-{str(uuid.uuid4())[:8]}')
        os.makedirs(caminho_temp_poly, exist_ok=True)

        # 1 "gdal:rastercalculator"
        try:
            nr_ += 1  # 1
            # mdt_layer = QgsRasterLayer(self.file_path, 'MDT')
            params = {
                'INPUT_A': f'{self.file_path}',
                'BAND_A': 1,
                'INPUT_B': None, 'BAND_B': None,
                'INPUT_C': None, 'BAND_C': None,
                'INPUT_D': None, 'BAND_D': None,
                'INPUT_E': None, 'BAND_E': None,
                'INPUT_F': None, 'BAND_F': None,
                'FORMULA': 'A > -100',
                'NO_DATA': 0,
                'EXTENT_OPT': 0,
                'PROJWIN': None,
                'RTYPE': 11,
                'OPTIONS': None,
                'EXTRA': '',
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }
            tool_ = "gdal:rastercalculator"
            result_calc = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 2 "gdal:polygonize"
        try:
            nr_ += 1  # 2
            params = {
                'INPUT': result_calc['OUTPUT'],
                'BAND': 1,
                'FIELD': 'DN',
                'EIGHT_CONNECTEDNESS': False,
                'EXTRA': '',
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }
            tool_ = "gdal:polygonize"
            result_poly = _run_processing(tool_, params)
            # print('result_poly', result_poly)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 3 "native:assignprojection" — output to GPKG (thread-safe)
        try:
            nr_ += 1
            out_assignpro = os.path.join(caminho_temp_poly, 'assignpro.gpkg')
            params = {
                'INPUT': result_poly['OUTPUT'],
                'CRS': QgsCoordinateReferenceSystem(self.srid),
                'OUTPUT': out_assignpro
            }
            tool_ = "native:assignprojection"
            result_setpro = _run_processing(tool_, params)
            result_setpro = {'OUTPUT': out_assignpro}
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 4 "native:reprojectlayer" — output to GPKG (thread-safe)
        nr_ += 1
        if self.srid_ref != self.srid:
            try:
                out_repro = os.path.join(caminho_temp_poly, 'reproject.gpkg')
                params = {
                    'INPUT': result_setpro['OUTPUT'],
                    'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4674'),
                    'CONVERT_CURVED_GEOMETRIES': False,
                    'OUTPUT': out_repro
                }
                tool_ = "native:reprojectlayer"
                result_repro = _run_processing(tool_, params)
                result_repro = {'OUTPUT': out_repro}
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            except Exception as e:
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
                return
        else:
            result_repro = result_setpro
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})

        # 5 "native:buffer" — output to GPKG (thread-safe)
        try:
            nr_ += 1  # 5
            out_buffer = os.path.join(caminho_temp_poly, 'buffer.gpkg')
            params = {
                'INPUT': result_repro['OUTPUT'],
                'DISTANCE': 0,
                'SEGMENTS': 5,
                'END_CAP_STYLE': 0,
                'JOIN_STYLE': 0,
                'MITER_LIMIT': 2,
                'DISSOLVE': True,
                'SEPARATE_DISJOINT': False,
                'OUTPUT': out_buffer}
            tool_ = "native:buffer"
            result_bff = _run_processing(tool_, params)
            result_bff = {'OUTPUT': out_buffer}
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        wkt_ = ''
        # 6 'geometry' — load from GPKG path for iteration (path from step 5)
        try:
            nr_ += 1  # 6
            layer_ = QgsVectorLayer(result_bff['OUTPUT'], 'buffer', 'ogr')
            tool_ = 'geometry'
            for i, feat_ in enumerate(layer_.getFeatures()):
                geom_ = feat_.geometry()
                geom_.convertToSingleType()
                feat_out = QgsFeature()
                if geom_:
                    feat_out.setGeometry(geom_)

                self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': f'{tool_} nr:{i + 1}', 'feat': feat_})
                # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            # self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        if self.nr_procs:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': ':) FINALIZADO LIMITE (:'
            })
        else:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': 'NENHUM PROCESSO SELECIONADO'
            })

class MorphologyThread(QThread):
    sig_status = pyqtSignal(dict, name='Status for processing bar')

    def __init__(self, main, parent, key_=None, dic_=None):
        QThread.__init__(self, parent)

        self.main = main
        self.parent = parent
        self.key_ = key_
        self.file_path = dic_['file_path']
        self.gpkg_path = dic_['gpkg']
        self.boudary = dic_['layer']
        self.max_memo = dic_['max_memo']
        self.max_px = dic_['max_px']

        self.srid_ref = dic_['srid_ref']
        self.srid = dic_['srid']
        self.morph_names = dic_['morph_names']
        self.gsd_ = dic_['gsd']

        self.nr_procs = 14
        self.cur = None
        self.conn = None

    @staticmethod
    def _processing_raster_path(val, context=None) -> str:
        if not val:
            return ''
        if isinstance(val, QgsMapLayer):
            s = val.source()
        else:
            s = str(val).strip()
            if context is not None:
                layer = context.getMapLayer(s)
                if layer is not None:
                    s = layer.source()
                else:
                    layer = QgsProject.instance().mapLayer(s)
                    if layer is not None:
                        s = layer.source()
        s = s.strip()
        pipe = s.find('|')
        path = s[:pipe].strip() if pipe >= 0 else s
        if path.startswith('file://'):
            path = path[7:]
        return os.path.normpath(path) if path else ''

    @staticmethod
    def _raster_has_usable_extent(path: str) -> bool:
        """True se o GeoTIFF existe, é válido e não é um raster vazio/1x1 (falha silenciosa do GRASS)."""
        if not path or not os.path.isfile(path) or os.path.getsize(path) < 400:
            return False
        layer = QgsRasterLayer(path, 'watershed_check')
        if not layer.isValid():
            return False
        w, h = layer.width(), layer.height()
        if w < 2 or h < 2:
            return False
        ext = layer.extent()
        if ext.isEmpty() or ext.width() <= 0 or ext.height() <= 0:
            return False
        # Região dummy típica quando o GRASS exporta um raster inválido (ex.: 0–1).
        if ext.width() <= 1.0 and ext.height() <= 1.0 and abs(ext.xMinimum()) < 2 and abs(ext.yMinimum()) < 2:
            return False
        return True

    def _watershed_basin_stream_exist(self, result_watershed, context=None) -> bool:
        if not result_watershed:
            return False
        for k in ('basin', 'stream'):
            p = self._processing_raster_path(result_watershed.get(k), context)
            if not self._raster_has_usable_extent(p):
                return False
        return True

    def run(self):
        self.sig_status.emit({'key': self.key_, 'quant': self.nr_procs})
        nr_ = 0

        try:
            grass_tools = resolve_morphology_grass_tools()
        except RuntimeError as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # Temp dir for native:* outputs (GPKG) — avoid layer objects in thread
        caminho_temp_morph = os.path.join(tempfile.gettempdir(), f'QGIS3-{str(uuid.uuid4())[:8]}')
        os.makedirs(caminho_temp_morph, exist_ok=True)

        # 1 'gdal:cliprasterbymasklayer'
        try:
            nr_ += 1
            tool_ = 'gdal:cliprasterbymasklayer'
            params = {
                'INPUT': f'{self.file_path}',
                'MASK':f'{self.boudary}',
                'SOURCE_CRS':QgsCoordinateReferenceSystem(self.srid),
                'TARGET_CRS':QgsCoordinateReferenceSystem(self.srid_ref),
                'TARGET_EXTENT':None,
                'NODATA':None,
                'ALPHA_BAND':False,
                'CROP_TO_CUTLINE':True,
                'KEEP_RESOLUTION':False,
                'SET_RESOLUTION':False,
                'X_RESOLUTION':None,
                'Y_RESOLUTION':None,
                'MULTITHREADING':False,
                'OPTIONS':'',
                'DATA_TYPE':0,
                'EXTRA':'',
                'OUTPUT': 'TEMPORARY_OUTPUT',
            }
            result_clip = _run_processing(tool_, params)
            # Nome amigável do MDE (não o path do TEMP do clip) — usado nos Audits PDF.
            dem_label = os.path.splitext(os.path.basename(self.file_path.split('|')[0]))[0]
            dem_label = dem_label.strip() or (self.morph_names[0] if self.morph_names else 'MDE')
            self.sig_status.emit({
                'key': self.key_,
                'value': nr_,
                'msg': tool_,
                'model': dem_label,
            })
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 2 "grass: r.watershed"
        try:
            nr_ += 1  # 1
            tool_ = grass_tools['r.watershed']
            basin_path = os.path.join(caminho_temp_morph, 'watershed_basin.tif')
            stream_path = os.path.join(caminho_temp_morph, 'watershed_stream.tif')
            proc_context = QgsProcessingContext()
            proc_feedback = _ProcessingFeedbackCapture()
            configured_grass_mb = int(round(self.max_memo * 1024))
            mem_status = _windows_memory_status()
            grass_mem_mb, mem_warn = _cap_grass_memory_mb(configured_grass_mb, mem_status)
            memory_efficient = _grass_watershed_memory_efficient(
                configured_grass_mb, grass_mem_mb, mem_status)
            if mem_warn:
                self.sig_status.emit({
                    'key': self.key_,
                    'warn': f'RAM {mem_status["load_pct"]}%' if mem_status else 'RAM',
                    'log_warning': mem_warn,
                })
            params = {
                'elevation':result_clip['OUTPUT'],
                'depression':None,
                'flow':None,
                'disturbed_land':None,
                'blocking':None,
                'threshold':self.max_px,
                'max_slope_length':None,
                'convergence':5,
                'memory': grass_mem_mb,
                '-s':True,
                '-m': memory_efficient,
                '-4':False,
                '-a':False,
                '-b':False,
                # Só basin/stream — pedir 9 rasters TEMPORARY aumenta falhas no export GRASS/Windows.
                'accumulation': None,
                'drainage': None,
                'basin': basin_path,
                'stream': stream_path,
                'half_basin': None,
                'length_slope': None,
                'slope_steepness': None,
                'tci': None,
                'spi': None,
                'GRASS_REGION_PARAMETER':None,
                'GRASS_REGION_CELLSIZE_PARAMETER':0,
                'GRASS_RASTER_FORMAT_OPT':'',
                'GRASS_RASTER_FORMAT_META':''
            }

            result_watershed = _run_processing(
                tool_, params, context=proc_context, feedback=proc_feedback)
            # Retentativas: modo -m, depois menos memory (atualiza RAM a cada tentativa).
            min_grass_mem_mb = 512
            attempt = 0
            while (
                not self._watershed_basin_stream_exist(result_watershed, proc_context)
                and attempt < 6
            ):
                attempt += 1
                prev_mb = grass_mem_mb
                prev_m = memory_efficient
                mem_status = _windows_memory_status()
                if not memory_efficient:
                    memory_efficient = True
                    grass_mem_mb, _ = _cap_grass_memory_mb(configured_grass_mb, mem_status)
                elif grass_mem_mb > min_grass_mem_mb:
                    grass_mem_mb = max(min_grass_mem_mb, grass_mem_mb - 512)
                else:
                    break
                params['memory'] = grass_mem_mb
                params['-m'] = memory_efficient
                proc_feedback = _ProcessingFeedbackCapture()
                ram_hint = ''
                if mem_status and mem_status['load_pct'] >= 85:
                    ram_hint = (
                        f' RAM do sistema em {mem_status["load_pct"]}% '
                        f'({mem_status["avail_mb"]} MB livres) — provável causa.'
                    )
                mode_hint = ' -m' if memory_efficient and not prev_m else ''
                self.sig_status.emit({
                    'key': self.key_,
                    'warn': f'{tool_} {grass_mem_mb} MB',
                    'log_warning': (
                        f'{tool_}: basin/stream em falta (tentativa {attempt}, '
                        f'memory={prev_mb} MB{mode_hint}).{ram_hint} '
                        f'Nova tentativa: memory={grass_mem_mb} MB'
                        f'{", modo -m" if memory_efficient else ""}.'
                    ),
                })
                result_watershed = _run_processing(
                    tool_, params, context=proc_context, feedback=proc_feedback)
            if not self._watershed_basin_stream_exist(result_watershed, proc_context):
                b = self._processing_raster_path(
                    (result_watershed or {}).get('basin'), proc_context)
                s = self._processing_raster_path(
                    (result_watershed or {}).get('stream'), proc_context)
                grass_log = proc_feedback.summary()
                detail = (
                    f'{tool_} não gerou basin/stream no disco (basin={b!r}, stream={s!r}) '
                    f'após retentativas até memory={grass_mem_mb} MB.'
                )
                if mem_status:
                    detail += (
                        f'\nRAM ao iniciar: {mem_status["load_pct"]}% em uso, '
                        f'{mem_status["avail_mb"]} MB livres de {mem_status["total_mb"]} MB.'
                    )
                if grass_log:
                    detail += f'\nÚltimas mensagens GRASS/Processing:\n  {grass_log}'
                detail += (
                    '\nSe a RAM estava acima de ~85%, feche outras aplicações e reinicie o QGIS. '
                    'Verifique também provider GRASS ativo e espaço em disco em %TEMP%.'
                )
                self.sig_status.emit({
                    'key': self.key_,
                    'value': nr_,
                    'error': detail,
                })
                return
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        morph_type_idx = 0  # Cumeadas

        # 3 "grass: r.to.vect"
        try:
            nr_ += 1  # 1
            tool_ = grass_tools['r.to.vect']
            params = {
                'input': result_watershed['basin'],
                'type': 2,
                'column': 'value',
                '-s': False,
                '-v': False,
                '-z': False,
                '-b': False,
                '-t': False,
                'output': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER': None,
                'GRASS_REGION_CELLSIZE_PARAMETER': 0,
                'GRASS_OUTPUT_TYPE_PARAMETER': 0,
                'GRASS_VECTOR_DSCO': '',
                'GRASS_VECTOR_LCO': '',
                'GRASS_VECTOR_EXPORT_NOCAT': False}
            result_basian_vect = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 4 "native:fixgeometries" — output to GPKG (thread-safe)
        try:
            nr_ += 1  # 1
            result_fix_gpkg = os.path.join(caminho_temp_morph, 'fixgeometries.gpkg')
            tool_ = "native:fixgeometries"
            params = {
                'INPUT': result_basian_vect['output'],
                'METHOD': 1,
                'OUTPUT': result_fix_gpkg,
            }
            result_fix = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 5 "grass: v.to.lines"
        try:
            nr_ += 1  # 1
            tool_ = grass_tools['v.to.lines']
            params = {
                'input': result_fix_gpkg,
                'method': None,
                'output': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER': None,
                'GRASS_SNAP_TOLERANCE_PARAMETER': -1,
                'GRASS_MIN_AREA_PARAMETER': 0.0001,
                'GRASS_OUTPUT_TYPE_PARAMETER': 0,
                'GRASS_VECTOR_DSCO': '',
                'GRASS_VECTOR_LCO': '',
                'GRASS_VECTOR_EXPORT_NOCAT': False
            }
            result_lines = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 6 "gdal:buffervectors"
        try:
            nr_ += 1  # 1
            tool_ = "gdal:buffervectors"
            params = {
                'INPUT': self.boudary,
                'GEOMETRY':'geom',
                'DISTANCE':-round(self.gsd_, 2),
                'FIELD':'',
                'DISSOLVE':False,
                'EXPLODE_COLLECTIONS':False,
                'OPTIONS':'',
                'OUTPUT': 'TEMPORARY_OUTPUT',
            }
            result_buffer = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 7 "native:clip"
        try:
            nr_ += 1  # 1
            tool_ = "native:clip"
            result_clipv_gpkg = os.path.join(caminho_temp_morph, f'clipv{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_lines['output'],
                'OVERLAY': result_buffer['OUTPUT'],
                'OUTPUT': result_clipv_gpkg
            }
            result_clip_v = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 8 "native:multiparttosingleparts"
        try:
            nr_ += 1  # 1
            tool_ = "native:multiparttosingleparts"
            result_single_gpkg = os.path.join(caminho_temp_morph, f'single{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_clip_v['OUTPUT'],
                'OUTPUT': result_single_gpkg
            }
            result_single = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return
        # 9 "native:densifygeometriesgivenaninterval"
        try:
            nr_ += 1  # 1
            tool_ = "native:densifygeometriesgivenaninterval"
            result_densified_gpkg = os.path.join(caminho_temp_morph, f'densified_cm{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_single['OUTPUT'],
                'INTERVAL': self.gsd_,
                'OUTPUT': result_densified_gpkg
            }
            result_densified = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 10 'native:setzfromraster - cm'
        try:
            nr_ += 1

            tool_ = 'native:setzfromraster'
            result_setz_gpkg = os.path.join(caminho_temp_morph, f'setzgeometries{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_densified['OUTPUT'],
                'RASTER': result_clip['OUTPUT'],
                'BAND': 1,
                'NODATA': 0,
                'SCALE': 1,
                'OFFSET': 0,
                'OUTPUT': f'{result_setz_gpkg}',
            }
            result_setz = _run_processing(tool_, params)
            dic_layer = {
                'gpkg': result_setz['OUTPUT'],
                'type': f'{self.morph_names[morph_type_idx]}_Z'
            }
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'layer': dic_layer})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        morph_type_idx = 1  # HN
        # 11 "grass: r.thin"
        try:
            nr_ += 1  # 1
            tool_ = grass_tools['r.thin']
            params = {
                'input': result_watershed['stream'],
                'iterations': 200,
                'output': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_CELLSIZE_PARAMETER': 0,
                'GRASS_RASTER_FORMAT_OPT': '',
                'GRASS_RASTER_FORMAT_META': '',
            }
            result_stream_thin = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 12 "grass: r.to.vect"
        try:
            nr_ += 1  # 1
            tool_ = grass_tools['r.to.vect']
            params = {
                'input': result_stream_thin['output'],
                'type':0,
                'column':'value',
                '-s':False,
                '-v':False,
                '-z':False,
                '-b':False,
                '-t':False,
                'output': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER':None,
                'GRASS_REGION_CELLSIZE_PARAMETER':0,
                'GRASS_OUTPUT_TYPE_PARAMETER':0,
                'GRASS_VECTOR_DSCO':'',
                'GRASS_VECTOR_LCO':'',
                'GRASS_VECTOR_EXPORT_NOCAT':False}
            result_stream_vect = _run_processing(tool_, params)

            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 13 "native:clip"
        try:
            nr_ += 1  # 1
            tool_ = "native:clip"
            result_clipv_gpkg1 = os.path.join(caminho_temp_morph, f'clipv{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_stream_vect['output'],
                'OVERLAY': result_buffer['OUTPUT'],
                'OUTPUT': result_clipv_gpkg1
            }
            result_clip_v1 = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 14 "native:multiparttosingleparts"
        try:
            nr_ += 1  # 1
            tool_ = "native:multiparttosingleparts"
            result_single_gpkg1 = os.path.join(caminho_temp_morph, f'single{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_clip_v1['OUTPUT'],
                'OUTPUT': result_single_gpkg1
            }
            result_single1 = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 15 "native:densifygeometriesgivenaninterval"
        try:
            nr_ += 1  # 1
            tool_ = "native:densifygeometriesgivenaninterval"
            result_densified_gpkg = os.path.join(caminho_temp_morph, f'densified_hn{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_single1['OUTPUT'],
                'INTERVAL': self.gsd_,
                'OUTPUT': result_densified_gpkg
            }
            result_densified = _run_processing(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 16 'native:setzfromraster - hn'
        try:
            nr_ += 1
            tool_ = 'native:setzfromraster'
            result_setz_gpkg = os.path.join(caminho_temp_morph, f'setzgeometries{morph_type_idx}.gpkg')
            params = {
                'INPUT': result_densified['OUTPUT'],
                'RASTER': result_clip['OUTPUT'],
                'BAND': 1,
                'NODATA': 0,
                'SCALE': 1,
                'OFFSET': 0,
                'OUTPUT': f'{result_setz_gpkg}',
            }
            result_setz = _run_processing(tool_, params)
            dic_layer = {
                'gpkg': result_setz['OUTPUT'],
                'type': f'{self.morph_names[morph_type_idx]}_Z'
            }
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'layer': dic_layer, 'start_task': True})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        if self.nr_procs:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': ':) FINALIZADO MORFOLOGIA (:'
            })
        else:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': 'NENHUM PROCESSO SELECIONADO'
            })

class BufferThread(QThread):
    sig_status = pyqtSignal(dict, name='Status for processing bar')

    def __init__(self, main, parent, key_=None, dic_=None):
        QThread.__init__(self, parent)

        self.main = main
        self.parent = parent

        self.dic_layers_line = dic_['dic_layers_line']
        self.list_scale = dic_.get('list_scale') or []
        self.dic_match = dic_['dic_match']
        self.dic_pec_mm = dic_['dic_pec_mm']
        self.dic_pec_v = dic_['dic_pec_v']
        self.norm_type = dic_['norm_type']
        try:
            self.dm_formula = int(dic_.get('dm_formula', DM_FORMULA_ORIGINAL))
        except (TypeError, ValueError):
            self.dm_formula = DM_FORMULA_ORIGINAL
        try:
            self.accuracy_standard = int(
                dic_.get('accuracy_standard', ACCURACY_STANDARD_BR))
        except (TypeError, ValueError):
            self.accuracy_standard = ACCURACY_STANDARD_BR
        try:
            self.gsd = float(dic_.get('gsd') or 0.0)
        except (TypeError, ValueError):
            self.gsd = 0.0
        try:
            self.ce90_max_h = float(dic_.get('ce90_max_h', 5.0))
        except (TypeError, ValueError):
            self.ce90_max_h = 5.0
        try:
            self.ce90_max_v = float(dic_.get('ce90_max_v', 2.0))
        except (TypeError, ValueError):
            self.ce90_max_v = 2.0
        self.write_buffer_layer = bool(dic_.get('write_buffer_layer', True))
        focus = str(dic_.get('recompute_focus') or 'full').strip().lower()
        self.recompute_focus = focus if focus in ('full', 'alt', 'dm') else 'full'

        self.dic_values = {}
        self.nr_procs = 0
        self._progress_count = 0
        if self.accuracy_standard == ACCURACY_STANDARD_CE90:
            n_pairs = sum(len(v) for v in self.dic_match.values())
            max_r = max(self.ce90_max_h, self.ce90_max_v) * max(self.gsd, 1e-6)
            dec = ce90_threshold_decimals(self.gsd)
            n_iters = max(8, int(math.ceil(math.log2(max(max_r / (10 ** (-dec)), 2)))) + 3)
            # Compatibilização: só LE90 muda de forma relevante; ainda assim recalculamos CE90
            # para manter dm_h coerente no relatório (sem regravar a camada).
            self.nr_procs = max(1, n_pairs * n_iters * 2)
        else:
            for tag_ in self.dic_match:
                for vet_ in self.dic_match[tag_]:
                    for scale_ in self.list_scale:
                        for class_ in self.dic_pec_mm['H']:
                            self.nr_procs += 1

    def _emit_progress(self, msg):
        self._progress_count += 1
        for key in (0, 1):
            self.sig_status.emit({
                'key': key,
                'value': self._progress_count,
                'msg': msg,
                'progress_only': True,
            })

    def _warn_dm_absurd(
        self,
        which: str,
        dm_raw: float,
        tag_: str,
        scale_,
        class_,
        layer_r_name: str,
        id_r,
        layer_t_name: str,
        id_t,
        detail_lines: list,
    ):
        parent = self.parent
        tr = getattr(parent, 'tr', lambda s: s)
        head = tr(
            '[Buffers] {0} fora do limite (|valor| ≤ {1}): {2} → NaN'
        ).format(which, int(DM_ABS_MAX_SANE), f'{dm_raw:.6g}')
        body = tr(
            '• morfologia: {0}  escala: {1}  classe: {2}\n'
            '• camada ref: {3}  fid_r: {4}\n'
            '• camada teste: {5}  fid_t: {6}\n'
        ).format(tag_, scale_, class_, layer_r_name, id_r, layer_t_name, id_t)
        extra = '\n'.join('• ' + ln for ln in detail_lines if ln)
        msg = head + '\n' + body + (extra + '\n' if extra else '')
        self.sig_status.emit({'key': 0, 'warn': which, 'log_warning': msg})

    def calc_dm_v(
        self,
        scale_,
        class_,
        geom_r,
        geom_t,
        *,
        tag_: str,
        layer_r_name: str,
        layer_t_name: str,
        id_r,
        id_t,
        pec_v=None,
    ):
        # create profile geometries with (progressive, elevation) coordinates for ref and test
        if pec_v is None:
            pec_v = self.dic_pec_v[scale_] * self.dic_pec_mm['V'][class_]['pec']
        # ep_v = self.dic_pec_v[scale_] * self.dic_pec_mm['V'][class_]['ep']

        profiles = build_compatibilized_profile_geometries(geom_r, geom_t, self.norm_type)
        if profiles is None:
            return float('nan')
        geom_prof_r = profiles['geom_prof_r']
        geom_prof_t = profiles['geom_prof_t']
        k_t = profiles['k_t']
        len_r = geom_r.length()
        len_t = geom_t.length()

        geom_prof_br = geom_prof_r.buffer(pec_v, 20)
        geom_prof_bt = geom_prof_t.buffer(pec_v, 20)
        # print('geom_prof_bt=', geom_prof_bt)

        geom_prof_i = geom_prof_bt.intersection(geom_prof_br)
        area_br_p = geom_prof_br.area()
        area_i_p = geom_prof_i.area()
        area_bt = geom_prof_bt.area()
        dm_prof = calc_dm_buffer_duplo(
            area_bt, area_br_p, area_i_p, pec_v, self.dm_formula
        )
        if not math.isfinite(dm_prof):
            return float('nan')
        if abs(dm_prof) > DM_ABS_MAX_SANE:
            self._warn_dm_absurd(
                'dm_v',
                dm_prof,
                tag_,
                scale_,
                class_,
                layer_r_name,
                id_r,
                layer_t_name,
                id_t,
                [
                    f'pec_v={pec_v!r} len_ref={len_r!r} len_teste={len_t!r} k_t={k_t!r}',
                    f'dm_formula={self.dm_formula}',
                    f'áreas perfil (ref/teste/inter): {area_br_p!r} / {area_bt!r} / {area_i_p!r}',
                ],
            )
            return float('nan')
        return dm_prof

    def _collect_match_pairs(self):
        pairs = []
        for tag_ in self.dic_match:
            layer_r = self.dic_layers_line[tag_][0]
            layer_t = self.dic_layers_line[tag_][1]
            self.sig_status.emit(
                {'logonly': f'---{tag_}---{layer_r.name()}---{layer_t.name()}---'}
            )
            for i, vet_ in enumerate(self.dic_match[tag_]):
                id_r = vet_[0]
                feat_r = layer_r.getFeature(id_r)
                geom_r = orient_line_high_to_low(QgsGeometry(feat_r.geometry()))
                id_t = vet_[1]
                feat_t = layer_t.getFeature(id_t)
                geom_t = orient_line_high_to_low(QgsGeometry(feat_t.geometry()))
                try:
                    len_r = float(vet_[4]) if len(vet_) > 4 else geom_r.length()
                except (TypeError, ValueError, IndexError):
                    len_r = geom_r.length()
                if not math.isfinite(len_r) or len_r <= 0:
                    len_r = geom_r.length()
                pairs.append({
                    'tag': tag_,
                    'i': i,
                    'vet': vet_,
                    'id_r': id_r,
                    'id_t': id_t,
                    'geom_r': geom_r,
                    'geom_t': geom_t,
                    'layer_r': layer_r,
                    'layer_t': layer_t,
                    'extent_ref': (
                        float(len_r) if math.isfinite(len_r) and len_r > 0 else 0.0
                    ),
                })
        return pairs

    def _ce90_snap_radius(self, radius) -> float:
        """Raio na grade de precisão (= valor mostrado / usado no buffer)."""
        dec = ce90_threshold_decimals(self.gsd)
        try:
            r = float(radius)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(r) or r <= 0:
            return 0.0
        return round(r, dec)

    def _build_dm_h_group(self, pairs, radius, class_name=CLASS_CE90):
        radius = self._ce90_snap_radius(radius)
        group = {}
        pair_feats = []
        count_ = 0
        for p in pairs:
            count_ += 1
            geom_r = p['geom_r']
            geom_t = p['geom_t']
            layer_r = p['layer_r']
            layer_t = p['layer_t']
            id_r = p['id_r']
            id_t = p['id_t']
            tag_ = p['tag']
            vet_ = p['vet']
            group[count_] = {
                'layer_r': layer_r.name(),
                'morph_tag': tag_,
                'fid_r': vet_[0],
                'layer_t': layer_t.name(),
                'fid_t': vet_[1],
                'extent_ref': p['extent_ref'],
                'dm_v': float('nan'),
            }
            geom_br = geom_r.buffer(radius, 20)
            geom_bt = geom_t.buffer(radius, 20)
            geom_i = geom_bt.intersection(geom_br)
            if self.write_buffer_layer:
                feat_br = QgsFeature()
                feat_br.setGeometry(geom_br)
                feat_br.setAttributes(
                    [radius, class_name, id_r, layer_r.name()])
                feat_bt = QgsFeature()
                feat_bt.setGeometry(geom_bt)
                feat_bt.setAttributes(
                    [radius, class_name, id_t, layer_t.name()])
                feat_i = QgsFeature()
                feat_i.setGeometry(geom_i)
                feat_i.setAttributes(
                    [radius, class_name, None, 'Intersecao'])
                pair_feats.extend((feat_br, feat_bt, feat_i))
            area_bt = geom_bt.area()
            area_br = geom_br.area()
            area_i = geom_i.area()
            dm_h = calc_dm_buffer_duplo(
                area_bt, area_br, area_i, radius, self.dm_formula
            )
            if math.isfinite(dm_h) and abs(dm_h) > DM_ABS_MAX_SANE:
                self._warn_dm_absurd(
                    'dm_h', dm_h, tag_, radius, class_name,
                    layer_r.name(), id_r, layer_t.name(), id_t,
                    [
                        f'pec_h={radius!r} dm_formula={self.dm_formula}',
                        f'áreas buffer ref/teste/inter: {area_br!r} / {area_bt!r} / {area_i!r}',
                    ],
                )
                dm_h = float('nan')
            group[count_]['dm_h'] = dm_h
            dec = ce90_threshold_decimals(self.gsd)
            self._emit_progress(f'CE90 {radius:.{dec}f} m — {tag_} {p["i"]}')
        if pair_feats:
            dec = ce90_threshold_decimals(self.gsd)
            payload = {
                'key': 0,
                'value': self._progress_count,
                'msg': self.parent.tr(
                    'CE90 {0} m — {1} geometrias'
                ).format(f'{radius:.{dec}f}', len(pair_feats)),
            }
            if self.write_buffer_layer:
                payload['feats_batch'] = pair_feats
            self.sig_status.emit(payload)
        return group

    def _build_dm_v_group(self, pairs, radius, class_name=CLASS_LE90):
        radius = self._ce90_snap_radius(radius)
        group = {}
        count_ = 0
        for p in pairs:
            count_ += 1
            geom_r = p['geom_r']
            geom_t = p['geom_t']
            layer_r = p['layer_r']
            layer_t = p['layer_t']
            tag_ = p['tag']
            vet_ = p['vet']
            group[count_] = {
                'layer_r': layer_r.name(),
                'morph_tag': tag_,
                'fid_r': vet_[0],
                'layer_t': layer_t.name(),
                'fid_t': vet_[1],
                'extent_ref': p['extent_ref'],
                'dm_h': float('nan'),
            }
            dm_v = self.calc_dm_v(
                radius,
                class_name,
                geom_r,
                geom_t,
                tag_=tag_,
                layer_r_name=layer_r.name(),
                layer_t_name=layer_t.name(),
                id_r=p['id_r'],
                id_t=p['id_t'],
                pec_v=radius,
            )
            group[count_]['dm_v'] = dm_v
            dec = ce90_threshold_decimals(self.gsd)
            self._emit_progress(f'LE90 {radius:.{dec}f} m — {tag_} {p["i"]}')
        return group

    def _binary_search_threshold(self, pairs, max_r, dimension):
        """Busca o menor raio (m) que aprova PEC+EP; 0 = reprovado (sem cálculo).

        Retorna (result_r, best_group, ok, trials, trials_meta).
        trials_meta[raio] = {ciclo, ok} onde ciclo = RODADA da busca
        (RODADA 1 = 0/meio/máx; RODADA 2 = próximo meio; …).
        """
        tr = getattr(self.parent, 'tr', lambda s: s)
        dec = ce90_threshold_decimals(self.gsd)
        step = 10 ** (-dec)
        max_r = round(float(max_r), dec)
        trials = {}
        trials_meta = {}
        if max_r <= 0:
            return None, {}, False, trials, trials_meta

        build = self._build_dm_h_group if dimension == 'H' else self._build_dm_v_group
        class_name = CLASS_CE90 if dimension == 'H' else CLASS_LE90
        ep_ratio = EP_RATIO_H if dimension == 'H' else EP_RATIO_V
        label = 'CE90' if dimension == 'H' else 'LE90'
        gsd = self.gsd if self.gsd and self.gsd > 0 else None

        def fmt_m(v):
            return f'{float(v):.{dec}f}'

        def empty_fail_group():
            """Grupo vazio para limiar 0 (reprovado sem buffer/DM)."""
            return {}

        def evaluate(radius, ciclo):
            if radius is None:
                return False, {}
            try:
                radius = float(radius)
            except (TypeError, ValueError):
                return False, {}
            ciclo = int(ciclo)
            if radius <= 0:
                key = 0.0
                group = empty_fail_group()
                ok = False
                trials[key] = group
                trials_meta[key] = {'ciclo': ciclo, 'ok': False}
                self.sig_status.emit({
                    'logonly': tr(
                        '{0} RODADA {1}: valor=0.0 m → FALHOU '
                        '(limite inferior; sem buffer/DM)'
                    ).format(label, ciclo),
                })
                return False, group

            # Valor mostrado = valor do buffer (mesma grade de precisão).
            key = self._ce90_snap_radius(radius)
            if key in trials_meta:
                ok = bool(trials_meta[key].get('ok'))
                group = trials.get(key) or {}
                # Reutiliza resultado já calculado; mantém ciclo original na meta
                self.sig_status.emit({
                    'logonly': tr(
                        '{0} RODADA {1}: valor={2} m → {3} '
                        '(já avaliado na RODADA {4})'
                    ).format(
                        label,
                        ciclo,
                        fmt_m(key),
                        tr('PASSOU') if ok else tr('FALHOU'),
                        trials_meta[key].get('ciclo', '?'),
                    ),
                })
                return ok, group

            group = build(pairs, key, class_name=class_name)
            ep_raw = round(key * ep_ratio, dec)
            ok, info = _ce90_group_evaluate(
                group,
                'dm_h' if dimension == 'H' else 'dm_v',
                key,
                ep_raw,
                decimals=dec,
            )
            trials[key] = group
            trials_meta[key] = {'ciclo': ciclo, 'ok': bool(ok)}
            px_txt = ''
            if gsd:
                px_txt = tr(
                    ' ({0:.1f} pixels do MDE de teste)'
                ).format(key / gsd)
            if not info['n_valid']:
                detail = tr('sem amostras válidas')
            elif not info['norm_ok']:
                detail = tr('normalidade FALHOU, {0} amostras').format(
                    info['n_valid'])
            else:
                rms_show = (
                    round(info['rms'], 2) if math.isfinite(info['rms'])
                    else float('nan')
                )
                detail = tr(
                    'n={0} (out={1}); quant {2:.0f}% <= {3}; '
                    'ext {4:.0f}% <= {3}; RMS={5} EP={6}'
                ).format(
                    info['n_valid'],
                    info['n_outliers'],
                    info['perc_q'] * 100,
                    info['pec_lim'],
                    info['perc_e'] * 100,
                    rms_show if math.isfinite(rms_show) else tr('n/d'),
                    info['ep'],
                )
            self.sig_status.emit({
                'logonly': tr(
                    '{0} RODADA {1}: valor={2} m{3} → {4} [{5}]'
                ).format(
                    label,
                    ciclo,
                    fmt_m(key),
                    px_txt,
                    tr('PASSOU') if ok else tr('FALHOU'),
                    detail,
                ),
            })
            return ok, group

        self.sig_status.emit({
            'logonly': tr(
                '--- {0}: busca entre 0.0 m e {1} m (passo {2:g} m) ---'
            ).format(label, fmt_m(max_r), step),
        })

        # RODADA 1: 0 (reprovado), intermediário e máximo (ex.: 0, 90, 180)
        mid1 = round(max_r / 2.0, dec)
        self.sig_status.emit({
            'logonly': tr(
                '{0} RODADA 1: candidatos = 0.0 , {1} , {2} m '
                '(mínimo, meio do intervalo, máximo)'
            ).format(label, fmt_m(mid1), fmt_m(max_r)),
        })
        evaluate(0.0, 1)
        ok_max, group_max = evaluate(max_r, 1)
        ok_mid, group_mid = (False, {})
        if mid1 > 0 and abs(mid1 - max_r) > step / 2:
            ok_mid, group_mid = evaluate(mid1, 1)

        if not ok_max:
            self.sig_status.emit({
                'logonly': tr(
                    '{0}: reprovado no máximo {1} m — limiar não encontrado.'
                ).format(label, fmt_m(max_r)),
            })
            return max_r, group_max, False, trials, trials_meta

        if ok_mid:
            low, high = 0.0, mid1
            best_r, best_group = mid1, group_mid
            self.sig_status.emit({
                'logonly': tr(
                    '{0} após RODADA 1: {1} m PASSOU → '
                    'próxima busca no intervalo ({2} , {3}] m'
                ).format(label, fmt_m(mid1), fmt_m(low), fmt_m(high)),
            })
        else:
            low, high = mid1, max_r
            best_r, best_group = max_r, group_max
            self.sig_status.emit({
                'logonly': tr(
                    '{0} após RODADA 1: {1} m FALHOU e {2} m PASSOU → '
                    'próxima busca no intervalo ({3} , {4}] m'
                ).format(
                    label, fmt_m(mid1), fmt_m(max_r),
                    fmt_m(low), fmt_m(high)),
            })

        ciclo = 1
        while high - low > step + 1e-12:
            ciclo += 1
            mid = round((low + high) / 2.0, dec)
            if mid <= low or mid >= high:
                # Meio caiu na borda: avança um passo a partir de low
                mid = round(low + step, dec)
                if mid >= high or mid <= low:
                    break
            self.sig_status.emit({
                'logonly': tr(
                    '{0} RODADA {1}: intervalo ({2} , {3}] m → '
                    'candidato = meio = {4} m'
                ).format(
                    label, ciclo, fmt_m(low), fmt_m(high), fmt_m(mid)),
            })
            ok, group = evaluate(mid, ciclo)
            if ok:
                best_r, best_group = mid, group
                high = mid
                self.sig_status.emit({
                    'logonly': tr(
                        '{0} RODADA {1}: {2} m PASSOU → '
                        'reduz intervalo para ({3} , {4}] m'
                    ).format(
                        label, ciclo, fmt_m(mid), fmt_m(low), fmt_m(high)),
                })
            else:
                low = mid
                self.sig_status.emit({
                    'logonly': tr(
                        '{0} RODADA {1}: {2} m FALHOU → '
                        'sobe intervalo para ({3} , {4}] m'
                    ).format(
                        label, ciclo, fmt_m(mid), fmt_m(low), fmt_m(high)),
                })

        # Limiar = menor valor na grade que passou (= high / best_r já na grade)
        result = round(float(best_r), dec)
        if result > max_r:
            result = max_r
        # Se por algum motivo o best ainda não está na meta, garante avaliação
        if round(result, dec) not in trials_meta:
            ciclo += 1
            self.sig_status.emit({
                'logonly': tr(
                    '{0} RODADA {1}: confirma limiar {2} m'
                ).format(label, ciclo, fmt_m(result)),
            })
            ok, group = evaluate(result, ciclo)
            if ok:
                best_group = group
            else:
                r = result
                while r < max_r - 1e-12:
                    r = round(r + step, dec)
                    ciclo += 1
                    self.sig_status.emit({
                        'logonly': tr(
                            '{0} RODADA {1}: {2} m FALHOU → tenta '
                            'próximo passo {3} m'
                        ).format(label, ciclo, fmt_m(result), fmt_m(r)),
                    })
                    ok, group = evaluate(r, ciclo)
                    if ok:
                        result, best_group = r, group
                        break
                else:
                    result, best_group = max_r, group_max
        elif not trials_meta[round(result, dec)].get('ok'):
            # best_r inconsistente: sobe na grade até passar
            r = result
            while r < max_r - 1e-12:
                r = round(r + step, dec)
                if r in trials_meta and trials_meta[r].get('ok'):
                    result = r
                    best_group = trials.get(r) or best_group
                    break
                ciclo += 1
                ok, group = evaluate(r, ciclo)
                if ok:
                    result, best_group = r, group
                    break
            else:
                result, best_group = max_r, group_max

        ciclo_final = (trials_meta.get(round(result, dec)) or {}).get(
            'ciclo', ciclo)
        px_final = ''
        if gsd:
            px_final = tr(
                ' ({0:.1f} pixels do MDE de teste)'
            ).format(result / gsd)
        self.sig_status.emit({
            'logonly': tr(
                '{0} limiar adotado: {1} m (RODADA {2}){3}'
            ).format(label, fmt_m(result), ciclo_final, px_final),
        })
        return result, best_group, True, trials, trials_meta

    def _run_ce90_le90(self):
        pairs = self._collect_match_pairs()
        dec = ce90_threshold_decimals(self.gsd)
        self.ce90_meta = {
            'gsd': self.gsd,
            'threshold_decimals': dec,
            'max_h_px': self.ce90_max_h,
            'max_v_px': self.ce90_max_v,
            'final_h': None,
            'final_v': None,
            'ok_h': False,
            'ok_v': False,
            'trials_h': {},
            'trials_v': {},
        }
        if not pairs:
            self.dic_values = {}
            return
        max_h = self.ce90_max_h * self.gsd
        max_v = self.ce90_max_v * self.gsd
        tr = getattr(self.parent, 'tr', lambda s: s)
        self.sig_status.emit({
            'logonly': tr(
                'CE90/LE90: {0} pares; pixel MDE teste={1:.3f} m; '
                'precisão limiar={2} casa(s) decimal(is); '
                'máx. H={3:g} pixels do MDE de teste ({4} m); '
                'máx. V={5:g} pixels do MDE de teste ({6} m).'
            ).format(
                len(pairs), self.gsd, dec,
                self.ce90_max_h, f'{max_h:.{dec}f}',
                self.ce90_max_v, f'{max_v:.{dec}f}',
            ),
        })
        r_h, group_h, ok_h, trials_h, meta_h = self._binary_search_threshold(
            pairs, max_h, 'H')
        r_v, group_v, ok_v, trials_v, meta_v = self._binary_search_threshold(
            pairs, max_v, 'V')
        self.ce90_meta.update({
            'final_h': r_h,
            'final_v': r_v,
            'ok_h': bool(ok_h),
            'ok_v': bool(ok_v),
            'trials_h': meta_h,
            'trials_v': meta_v,
            'max_h_m': max_h,
            'max_v_m': max_v,
        })
        self.dic_values = {}
        for key_h, group in trials_h.items():
            self.dic_values.setdefault(key_h, {})[CLASS_CE90] = group
        for key_v, group in trials_v.items():
            self.dic_values.setdefault(key_v, {})[CLASS_LE90] = group
        if r_h is not None:
            key_h = round(float(r_h), dec)
            self.dic_values.setdefault(key_h, {})[CLASS_CE90] = group_h
        if r_v is not None:
            key_v = round(float(r_v), dec)
            self.dic_values.setdefault(key_v, {})[CLASS_LE90] = group_v

    def run(self):
        for i in [0, 1]:
            self.sig_status.emit({'key': i, 'quant': self.nr_procs})
        nr_ = 0
        count_ = 0
        try:
            if self.accuracy_standard == ACCURACY_STANDARD_CE90:
                self._run_ce90_le90()
                self.sig_status.emit({
                    'key': 0,
                    'dic_values': self.dic_values,
                    'ce90_meta': getattr(self, 'ce90_meta', None),
                })
            else:
                for tag_ in self.dic_match:
                    layer_r = self.dic_layers_line[tag_][0]
                    layer_t = self.dic_layers_line[tag_][1]
                    self.sig_status.emit(
                        {'logonly': f'---{tag_}---{layer_r.name()}---{layer_t.name()}---'}
                    )
                    for i, vet_ in enumerate(self.dic_match[tag_]):
                        # print('vet_', vet_)
                        id_r = vet_[0]
                        feat_r = layer_r.getFeature(id_r)
                        geom_r = orient_line_high_to_low(QgsGeometry(feat_r.geometry()))
                        id_t = vet_[1]
                        feat_t = layer_t.getFeature(id_t)
                        geom_t = orient_line_high_to_low(QgsGeometry(feat_t.geometry()))
                        pair_feats = []
                        for scale_ in self.list_scale:
                            if scale_ not in self.dic_values:
                                self.dic_values[scale_] = {}
                            # print('scale_', scale_)
                            for class_ in self.dic_pec_mm['H']:
                                if class_ not in self.dic_values[scale_]:
                                    self.dic_values[scale_][class_] = {}
                                # print('class_', class_)
                                count_ += 1
                                self.dic_values[scale_][class_][count_] = {}
                                pec_h = scale_ * self.dic_pec_mm['H'][class_]['pec']
                                # ep_h = scale_ * self.dic_pec_mm['H'][class_]['ep']

                                self.dic_values[scale_][class_][count_]['layer_r'] = layer_r.name()
                                self.dic_values[scale_][class_][count_]['morph_tag'] = tag_
                                self.dic_values[scale_][class_][count_]['fid_r'] = vet_[0]
                                self.dic_values[scale_][class_][count_]['layer_t'] = layer_t.name()
                                self.dic_values[scale_][class_][count_]['fid_t'] = vet_[1]
                                try:
                                    len_r = float(vet_[4]) if len(vet_) > 4 else geom_r.length()
                                except (TypeError, ValueError, IndexError):
                                    len_r = geom_r.length()
                                if not math.isfinite(len_r) or len_r <= 0:
                                    len_r = geom_r.length()
                                self.dic_values[scale_][class_][count_]['extent_ref'] = (
                                    float(len_r) if math.isfinite(len_r) and len_r > 0 else 0.0
                                )

                                geom_br = geom_r.buffer(pec_h, 20)
                                geom_bt = geom_t.buffer(pec_h, 20)
                                geom_i = geom_bt.intersection(geom_br)
                                if self.write_buffer_layer:
                                    feat_br = QgsFeature()
                                    feat_br.setGeometry(geom_br)
                                    feat_br.setAttributes([scale_, class_, id_r, layer_r.name()])
                                    feat_bt = QgsFeature()
                                    feat_bt.setGeometry(geom_bt)
                                    feat_bt.setAttributes([scale_, class_, id_t, layer_t.name()])
                                    feat_i = QgsFeature()
                                    feat_i.setGeometry(geom_i)
                                    feat_i.setAttributes([scale_, class_, None, 'Intersecao'])
                                    pair_feats.extend((feat_br, feat_bt, feat_i))
                                # CÁLCULO DO DM HORIZONTAL (A1=teste, A2=ref, A3=interseção)
                                area_bt = geom_bt.area()
                                area_br = geom_br.area()
                                area_i = geom_i.area()
                                dm_h = calc_dm_buffer_duplo(
                                    area_bt, area_br, area_i, pec_h, self.dm_formula
                                )
                                if math.isfinite(dm_h) and abs(dm_h) > DM_ABS_MAX_SANE:
                                    self._warn_dm_absurd(
                                        'dm_h',
                                        dm_h,
                                        tag_,
                                        scale_,
                                        class_,
                                        layer_r.name(),
                                        id_r,
                                        layer_t.name(),
                                        id_t,
                                        [
                                            f'pec_h={pec_h!r} dm_formula={self.dm_formula}',
                                            f'áreas buffer ref/teste/inter: {area_br!r} / {area_bt!r} / {area_i!r}',
                                        ],
                                    )
                                    dm_h = float('nan')
                                self.dic_values[scale_][class_][count_]['dm_h'] = dm_h
                                dm_v = self.calc_dm_v(
                                    scale_,
                                    class_,
                                    geom_r,
                                    geom_t,
                                    tag_=tag_,
                                    layer_r_name=layer_r.name(),
                                    layer_t_name=layer_t.name(),
                                    id_r=id_r,
                                    id_t=id_t,
                                )
                                self.dic_values[scale_][class_][count_]['dm_v'] = dm_v

                                prog_msg = f'{tag_} {i} {scale_} - {class_}'
                                self.sig_status.emit({
                                    'key': 0,
                                    'value': count_,
                                    'msg': prog_msg,
                                    'progress_only': True,
                                })
                                self.sig_status.emit({
                                    'key': 1,
                                    'value': count_,
                                    'msg': prog_msg,
                                    'progress_only': True,
                                })

                        if pair_feats:
                            payload = {
                                'key': 0,
                                'value': count_,
                                'msg': self.parent.tr(
                                    '{0} par {1} (fid_r={2}, fid_t={3}) — {4} geometrias'
                                ).format(tag_, i, id_r, id_t, len(pair_feats)),
                            }
                            if self.write_buffer_layer:
                                payload['feats_batch'] = pair_feats
                            self.sig_status.emit(payload)

                self.sig_status.emit({'key': 0, 'dic_values': self.dic_values})
        except Exception as e:
            for i in [0, 1]:
                self.sig_status.emit({'key': i, 'value': nr_, 'error': e})
            return


        if self.nr_procs:
            for i in [0, 1]:
                self.sig_status.emit({
                    'key': i,
                    'end': self.nr_procs,
                    'msg': ':) FINALIZADO BUFFERS (:'
                })
        else:
            for i in [0, 1]:
                self.sig_status.emit({
                    'key': i,
                    'end': self.nr_procs,
                    'msg': 'NENHUM PROCESSO SELECIONADO'
                })

class Worker(QObject):
    """ Worker that manages a processing thread and signals when it's done """
    finished = pyqtSignal(int)  # Signal to notify when a task is done

    def __init__(self, key_, dic_, parent):
        super().__init__()
        self.key_ = key_
        self.dic_ = dic_
        self.parent = parent  # Reference to the main class
        self.process_thread = None

    def start(self):
        """ Start the appropriate processing thread asynchronously """
        if self.dic_['step'] == 'polygon' :
            self.process_thread = PolygonThread(main=self.dic_['main'], parent=self.dic_['parent'], key_=self.key_,
                                                dic_=self.dic_)
        elif self.dic_['step'] == 'morphology':
            self.process_thread = MorphologyThread(main=self.dic_['main'], parent=self.dic_['parent'], key_=self.key_,
                                                dic_=self.dic_)
        elif self.dic_['step'] == 'buffers':
            self.process_thread = BufferThread(main=self.dic_['main'], parent=self.dic_['parent'], key_=self.key_,
                                                dic_=self.dic_)

        self.process_thread.sig_status.connect(self.dic_['parent'].update_bar)
        self.process_thread.finished.connect(lambda: self.finished.emit(self.key_))  # Notify when done
        self.process_thread.start()

    def stop(self, wait_ms=8000):
        """Para o thread de processamento (Plugin Reloader / unload)."""
        th = self.process_thread
        if not th:
            return
        if th.isRunning():
            th.requestInterruption()
            if not th.wait(wait_ms):
                th.terminate()
                th.wait(2000)

