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
                       QgsFields, QgsField, QgsVectorLayer, QgsCoordinateTransformContext, QgsWkbTypes,
                       QgsGeometry, QgsPointXY, QgsProcessingContext, QgsProcessingFeedback, QgsMapLayer,
                       QgsProject)

# |dm_h| ou |dm_v| acima disto é tratado como erro numérico / geometria; → NaN e WARNING no log.
DM_ABS_MAX_SANE = 1000.0

_GRASS_PROVIDER_IDS = ('grass', 'grass7')
_MORPHOLOGY_GRASS_ALGORITHMS = ('r.watershed', 'r.to.vect', 'v.to.lines', 'r.thin')


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
            result_calc = processing.run(tool_, params)
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
            result_poly = processing.run(tool_, params)
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
            result_setpro = processing.run(tool_, params)
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
                result_repro = processing.run(tool_, params)
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
            result_bff = processing.run(tool_, params)
            result_bff = {'OUTPUT': out_buffer}
            print('result_bff', result_bff)
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

    def _watershed_basin_stream_exist(self, result_watershed, context=None) -> bool:
        if not result_watershed:
            return False
        for k in ('basin', 'stream'):
            p = self._processing_raster_path(result_watershed.get(k), context)
            if not p or not os.path.isfile(p):
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
            print(tool_, self.key_, self.file_path)
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
            result_clip = processing.run(tool_, params)
            print('result_clip', result_clip)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'model': result_clip['OUTPUT']})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 2 "grass: r.watershed"
        try:
            print(result_clip['OUTPUT'], self.max_px, self.max_memo * 1024)
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
                'accumulation': 'TEMPORARY_OUTPUT',
                'drainage': 'TEMPORARY_OUTPUT',
                'basin': basin_path,
                'stream': stream_path,
                'half_basin': 'TEMPORARY_OUTPUT',
                'length_slope': 'TEMPORARY_OUTPUT',
                'slope_steepness': 'TEMPORARY_OUTPUT',
                'tci': 'TEMPORARY_OUTPUT',
                'spi': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER':None,
                'GRASS_REGION_CELLSIZE_PARAMETER':0,
                'GRASS_RASTER_FORMAT_OPT':'',
                'GRASS_RASTER_FORMAT_META':''
            }

            result_watershed = processing.run(
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
                result_watershed = processing.run(
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
            result_basian_vect = processing.run(tool_, params)
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
            result_fix = processing.run(tool_, params)
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
            result_lines = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 6 "gdal:buffervectors"
        print(-round(self.gsd_, 2), self.boudary)
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
            result_buffer = processing.run(tool_, params)
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
            result_clip_v = processing.run(tool_, params)
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
            result_single = processing.run(tool_, params)
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
            result_densified = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 10 'native:setzfromraster - cm'
        try:
            nr_ += 1

            tool_ = 'native:setzfromraster'
            result_setz_gpkg = os.path.join(caminho_temp_morph, f'setzgeometries{morph_type_idx}.gpkg')
            print(tool_, self.key_, self.file_path)
            params = {
                'INPUT': result_densified['OUTPUT'],
                'RASTER': result_clip['OUTPUT'],
                'BAND': 1,
                'NODATA': 0,
                'SCALE': 1,
                'OFFSET': 0,
                'OUTPUT': f'{result_setz_gpkg}',
            }
            result_setz = processing.run(tool_, params)
            dic_layer = {
                'gpkg': result_setz['OUTPUT'],
                'type': f'{self.morph_names[morph_type_idx]}_Z'
            }
            print('result_setz', result_setz)
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
            result_stream_thin = processing.run(tool_, params)
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
            result_stream_vect = processing.run(tool_, params)

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
            result_clip_v1 = processing.run(tool_, params)
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
            result_single1 = processing.run(tool_, params)
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
            result_densified = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 16 'native:setzfromraster - hn'
        try:
            nr_ += 1
            tool_ = 'native:setzfromraster'
            result_setz_gpkg = os.path.join(caminho_temp_morph, f'setzgeometries{morph_type_idx}.gpkg')
            print(tool_, self.key_, self.file_path)
            params = {
                'INPUT': result_densified['OUTPUT'],
                'RASTER': result_clip['OUTPUT'],
                'BAND': 1,
                'NODATA': 0,
                'SCALE': 1,
                'OFFSET': 0,
                'OUTPUT': f'{result_setz_gpkg}',
            }
            result_setz = processing.run(tool_, params)
            dic_layer = {
                'gpkg': result_setz['OUTPUT'],
                'type': f'{self.morph_names[morph_type_idx]}_Z'
            }
            print('result_setz', result_setz)
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
        self.list_scale = dic_['list_scale']
        self.dic_match = dic_['dic_match']
        self.dic_pec_mm = dic_['dic_pec_mm']
        self.dic_pec_v = dic_['dic_pec_v']
        self.norm_type = dic_['norm_type']

        self.dic_values = {}
        self.nr_procs = 0
        for tag_ in self.dic_match:
            for vet_ in self.dic_match[tag_]:
                for scale_ in self.list_scale:
                    for class_ in self.dic_pec_mm['H']:
                        self.nr_procs += 1

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
    ):
        # create profile geometries with (progressive, elevation) coordinates for ref and test
        pec_v = self.dic_pec_v[scale_] * self.dic_pec_mm['V'][class_]['pec']
        # ep_v = self.dic_pec_v[scale_] * self.dic_pec_mm['V'][class_]['ep']

        len_r = geom_r.length()
        wkbt_ = geom_r.wkbType()
        # GETTING LIST OF POINTS
        if wkbt_ == QgsWkbTypes.LineString or wkbt_ == QgsWkbTypes.LineStringZ:
            ps_r = geom_r.constGet().points()
        else:
            ps_r = geom_r.constGet()[0].points()

        gpr0 = QgsGeometry().fromPointXY(QgsPointXY(ps_r[0]))
        gpr1 = QgsGeometry().fromPointXY(QgsPointXY(ps_r[-1]))
        list_prof_r = []
        list_prog_cota_r = []
        list_cota_r = []
        for p_ in ps_r:
            dist_ = round(geom_r.lineLocatePoint(QgsGeometry(p_)), 2)
            z_ = round(p_.z(), 2)
            list_prof_r.append(QgsPointXY(dist_ + 10000, z_))
            list_prog_cota_r.append([dist_ + 10000, z_])
            list_cota_r.append(z_)
        geom_prof_r = QgsGeometry().fromPolylineXY(list_prof_r)
        # print('r-', list_prog_cota_r)

        len_t = geom_t.length()
        if (
            not math.isfinite(len_r)
            or not math.isfinite(len_t)
            or len_r <= 0
            or len_t <= 0
        ):
            return float('nan')

        if geom_t.wkbType() == QgsWkbTypes.LineString or geom_t.wkbType() == QgsWkbTypes.LineStringZ:
            ps_t = geom_t.constGet().points()
        else:
            ps_t = geom_t.constGet()[0].points()
        list_prof_t = []
        list_prog_cota_t = []
        list_cota_t = []
        # k_t is used to scale prog
        k_t = len_r / len_t
        if not math.isfinite(k_t) or abs(k_t) > 1e12:
            return float('nan')
        gpt0 = QgsGeometry().fromPointXY(QgsPointXY(ps_t[0]))
        if gpt0.distance(gpr0) > gpt0.distance(gpr1):
            ci = True
        else:
            ci = False
        for p_ in ps_t:
            # if feat_r.id() == 8:
            #     print(p_)
            # # print(p_)
            z_ = round(p_.z(), 2)

            # DIST FROM SCALE METHOD OR FROM LESS DISTANCE METHOD
            if self.norm_type == 0:  # Linear — fator escalar k_t nas progressivas
                dist_ = geom_t.lineLocatePoint(QgsGeometry(p_))
                if ci: # Need to invert,
                    dist_ = round((len_t - dist_) * k_t, 2)
                else:
                    dist_ = round(dist_ * k_t, 2)
            elif self.norm_type == 1:  # Por Proximidade — progressiva da ref. no ponto mais próximo
                dist_ = geom_r.lineLocatePoint(QgsGeometry(p_))
                if ci:
                    dist_ = round((len_r - dist_) * k_t, 2)
                else:
                    dist_ = round(dist_, 2)
            else:  # Sem Normalização
                dist_ = geom_t.lineLocatePoint(QgsGeometry(p_))
                if ci:
                    dist_ = round(len_t - dist_, 2)
                else:
                    dist_ = round(dist_, 2)

            # list_prof_t.append(QgsPointXY(dist_, z_))
            if not list_prog_cota_t:
                list_prog_cota_t.append([dist_ + 10000, z_])
                list_cota_t.append(z_)
            elif dist_ != list_prog_cota_t[-1][0]:
                list_prog_cota_t.append([dist_ + 10000, z_])
                list_cota_t.append(z_)
        list_prog_cota_t = sorted(list_prog_cota_t)
        # print('t-', list_prog_cota_t)
        list_prof_t = []

        for vet_ in list_prog_cota_t:
            list_prof_t.append(QgsPointXY(float(vet_[0]), float(vet_[1])))
        geom_prof_t = QgsGeometry().fromPolylineXY(list_prof_t)
        cm_r = statistics.mean(list_cota_r)
        cm_t = statistics.mean(list_cota_t)
        # with open(path_txt_profile, 'a') as prof_file:
        #     prof_file.write(f'\n {l_ref_name} - {feat_r.id()} | len(r) = {len_r} | len(t) {len_t}\n'
        #                     f'Cota_Media_r {cm_r} | Cota_Media_t {cm_t}\n')
        #     for r_, t_ in zip_longest(list_prog_cota_r, list_prog_cota_t):
        #         prof_file.write(
        #             f'{round(r_[0], 2) if r_ else ""}; {round(r_[1], 2) if r_ else ""}; {round(t_[0], 2) if t_ else ""}; {round(t_[1], 2) if t_ else ""}; \n')
        geom_prof_br = geom_prof_r.buffer(pec_v, 20)
        geom_prof_bt = geom_prof_t.buffer(pec_v, 20)
        # print('geom_prof_bt=', geom_prof_bt)

        geom_prof_i = geom_prof_bt.intersection(geom_prof_br)
        area_br_p = geom_prof_br.area()
        area_i_p = geom_prof_i.area()
        area_bt = geom_prof_bt.area()
        if not math.isfinite(area_bt) or area_bt <= 0:
            return float('nan')
        dm_prof = math.pi * pec_v * (area_br_p - area_i_p) / area_bt
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
                    f'áreas perfil (ref/teste/inter): {area_br_p!r} / {area_bt!r} / {area_i_p!r}',
                ],
            )
            return float('nan')
        return dm_prof

    def run(self):
        for i in [0, 1]:
            self.sig_status.emit({'key': i, 'quant': self.nr_procs})
        nr_ = 0
        count_ = 0
        try:
            for tag_ in self.dic_match:
                print('tag_', tag_)
                layer_r = self.dic_layers_line[tag_][0]
                layer_t = self.dic_layers_line[tag_][1]
                print('layer_r', layer_r)
                print('layer_t', layer_t)
                self.sig_status.emit(
                    {'logonly': f'---{tag_}---{layer_r.name()}---{layer_t.name()}---'}
                )
                for i, vet_ in enumerate(self.dic_match[tag_]):
                    # print('vet_', vet_)
                    id_r = vet_[0]
                    feat_r = layer_r.getFeature(id_r)
                    geom_r = QgsGeometry(feat_r.geometry())
                    id_t = vet_[1]
                    feat_t = layer_t.getFeature(id_t)
                    geom_t = QgsGeometry(feat_t.geometry())
                    self.sig_status.emit(
                        {'logonly': f' \n-- {tag_} - idr {id_r} - idt {id_t} --'}
                    )
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
                            feat_br = QgsFeature()
                            feat_br.setGeometry(geom_br)
                            feat_br.setAttributes([count_ + 10000, scale_, class_, id_r, layer_r.name()])

                            geom_bt = geom_t.buffer(pec_h, 20)
                            feat_bt = QgsFeature()
                            feat_bt.setGeometry(geom_bt)
                            feat_bt.setAttributes([count_ + 20000, scale_, class_, id_t, layer_t.name()])

                            geom_i = geom_bt.intersection(geom_br)
                            feat_i = QgsFeature()
                            feat_i.setGeometry(geom_i)
                            feat_i.setAttributes([count_ + 30000, scale_, class_, None, 'Intersecao'])

                            dic_feats = { 'feat_br': feat_br, 'feat_bt': feat_bt, 'feat_i': feat_i}
                            # CÁLCULO DO DM HORIZONTAL (área do buffer teste nula → divisão indefinida / explosão numérica)
                            area_bt = geom_bt.area()
                            area_br = geom_br.area()
                            area_i = geom_i.area()
                            if (
                                not math.isfinite(area_bt)
                                or area_bt <= 0
                                or not math.isfinite(area_br)
                                or not math.isfinite(area_i)
                            ):
                                dm_h = float('nan')
                            else:
                                dm_h = math.pi * pec_h * (area_br - area_i) / area_bt
                                if not math.isfinite(dm_h):
                                    dm_h = float('nan')
                                elif abs(dm_h) > DM_ABS_MAX_SANE:
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
                                            f'pec_h={pec_h!r}',
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

                            print(scale_, class_, id_r, id_t, round(dm_h, 2), round(dm_v, 2))

                            self.sig_status.emit(
                                {'key': 0,
                                 'value': count_,
                                 'msg': f'{tag_} {i} {scale_} - {class_}',
                                 'feats': dic_feats}
                            )
                            self.sig_status.emit(
                                {'key': 1,
                                 'value': count_,
                                 'msg': f'{tag_} {i} {scale_} - {class_}'}
                            )

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

