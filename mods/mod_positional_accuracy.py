# -*- coding: utf-8 -*-
import datetime
import html
import json
import math
import os
import sqlite3
import statistics
from queue import Queue, Empty
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
# from sys import prefix

from osgeo import ogr
from qgis.PyQt.QtCore import (QSettings, Qt, QSize, QTranslator, QCoreApplication, QEvent, QThreadPool, QDateTime,
                              QVariant)
from qgis.PyQt.QtGui import (
    QPixmap, QIcon, QFont, QPalette, QColor, QTextCharFormat, QBrush, QTextOption,
    QTextDocument,
)
from qgis.PyQt.QtPrintSupport import QPrinter
from qgis.PyQt.QtWidgets import (QAction, QScrollArea, QGridLayout, QPushButton, QLabel, QWidget, QSizePolicy,
                                 QSpacerItem, QDockWidget, QSplitter, QComboBox, QLineEdit, QDialog, QFrame, QCheckBox,
                                 QHBoxLayout, QVBoxLayout, QFileDialog, QTableWidget,
                                 QProgressBar, QDateEdit, QWidget, QVBoxLayout, QPushButton, QPlainTextEdit,
                                 QMessageBox)
from qgis.core import (QgsVectorFileWriter, QgsWkbTypes, QgsCoordinateTransformContext, QgsCoordinateReferenceSystem,
                       QgsCoordinateTransform, QgsGeometry, QgsPointXY,
                       QgsFeature, QgsVectorLayer, QgsRasterLayer, QgsFields, QgsField, QgsProject,
                       QgsMapLayerProxyModel, QgsLayerTreeLayer, QgsDistanceArea)
from qgis.gui import QgsMapLayerComboBox
from .mod_aux_tools import AuxTools#, Obs2, Logger
from .mod_login import Database
from .mod_worker_threads import Worker
from .mod_settings import SettingsDlg
from .plugin_i18n import PLUGIN_I18N_CONTEXT, tr_ui

plugin_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.join(plugin_path, 'libs')))
# Arquivo de projeto: GeoPackage com extensão composta (conteúdo GPKG)
PROJECT_EXT = '.pa.gpkg'


def geometry_area_square_meters(geom: QgsGeometry, crs: QgsCoordinateReferenceSystem) -> float:
    """Área no elipsoide em m² (independente das unidades planas do CRS da camada)."""
    if geom is None or geom.isEmpty():
        return 0.0
    da = QgsDistanceArea()
    da.setSourceCrs(crs, QgsProject.instance().transformContext())
    # QGIS 3: não existe setEllipsoidalMode; setEllipsoid activa área/distância elipsoidal.
    ell = QgsProject.instance().ellipsoid()
    da.setEllipsoid(ell if ell else 'WGS84')
    return float(da.measureArea(geom))


def _icon_from_icons_file(filename: str) -> QIcon:
    """QIcon a partir de icons/<filename> se existir."""
    path_ = os.path.normpath(os.path.join(plugin_path, 'icons', filename))
    if os.path.isfile(path_):
        return QIcon(path_)
    return QIcon()


def _strip_project_ext(path: str) -> str:
    """Caminho sem o sufixo PROJECT_EXT (nome base lógico e ficheiro .gpkg paralelo do OGR)."""
    if not path:
        return path
    pl = path.lower()
    low = PROJECT_EXT.lower()
    if pl.endswith(low):
        return path[: -len(PROJECT_EXT)]
    return os.path.splitext(path)[0]


def resolve_saved_project_file_on_disk(saved_path: str) -> str:
    """Resolve caminho do ficheiro de projeto (.pa.gpkg) no disco."""
    if not saved_path:
        return ''
    p = os.path.normpath(os.path.abspath(saved_path))
    if os.path.isfile(p):
        return p
    return p


def project_file_filter_i18n() -> str:
    return (
        f'{tr_ui("Projeto MDE-AP (*.pa.gpkg)")};;{tr_ui("Todos (*.*)")}'
    )

# Tabela sem geometria no ficheiro de projeto: inicio/fim = data e hora local (SQLite 'YYYY-MM-DD HH:MM:SS') ou NULL
PIPELINE_ETAPAS_TABLE = 'pa_pipeline_etapas'
PIPELINE_DATETIME_FMT = '%Y-%m-%d %H:%M:%S'
PIPELINE_ETAPAS_DEF = (
    (1, 'poligonos_limites'),
    (2, 'dem_intersecao'),
    (3, 'morfologia_referencia'),
    (4, 'morfologia_teste'),
    (5, 'correspondencia_linhas'),
    (6, 'buffers'),
)

# Snapshot da última avaliação concluída (PEC): comparação em nova «Avaliar» para retomar só o necessário
PIPELINE_SNAPSHOT_ETAPA = '__pipeline_last_ok__'
PIPELINE_SNAPSHOT_CAMPO = 'config_json'

# Bloco de Config → primeira etapa a repetir (o resto da cadeia segue como hoje)
STEP_KEY_TO_RESTART_ETAPA = {
    'step_morfologia': 'morfologia_referencia',
    'step_match': 'correspondencia_linhas',
    'step_buffers': 'buffers',
    'step_normalize_prog': 'buffers',
}


def _pipeline_etapa_order_index():
    return {name: i for i, (_, name) in enumerate(PIPELINE_ETAPAS_DEF)}


def load_pipeline_last_ok_snapshot(mdepa_path: str) -> dict:
    """Último estado de parâmetros + DEMs após uma avaliação concluída com sucesso."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return {}
    if not ensure_pa_settings_table(mdepa_path):
        return {}
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ? AND campo = ?',
                (PIPELINE_SNAPSHOT_ETAPA, PIPELINE_SNAPSHOT_CAMPO),
            )
            row = cur.fetchone()
            if not row or row[0] is None or str(row[0]).strip() == '':
                return {}
            return json.loads(row[0])
        finally:
            conn.close()
    except (sqlite3.Error, json.JSONDecodeError, TypeError, ValueError):
        return {}


def save_pipeline_last_ok_snapshot(mdepa_path: str, flat: dict) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    try:
        payload = json.dumps(flat, sort_keys=True, ensure_ascii=False)
        conn = sqlite3.connect(mdepa_path)
        try:
            conn.execute(
                f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                    VALUES (?, ?, ?)
                    ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                (PIPELINE_SNAPSHOT_ETAPA, PIPELINE_SNAPSHOT_CAMPO, payload),
            )
            conn.commit()
        finally:
            conn.close()
    except (sqlite3.Error, TypeError):
        return False
    return True


def _coerce_snapshot_flat_for_dem_keys(flat: dict) -> dict:
    """Snapshots antigos usavam raster_0/raster_1; alinhar para dem_*."""
    if not flat:
        return flat
    d = dict(flat)
    for i in (0, 1):
        rk, dk = f'raster_{i}', f'dem_{i}'
        if dk not in d and rk in d:
            d[dk] = d[rk]
    return d


def _normalize_dem_sources_in_flat(flat: dict) -> dict:
    """Cópia com dem_0/dem_1 normalizados para comparação (caminhos Windows / URI)."""
    if not flat:
        return flat
    d = dict(flat)
    for i in (0, 1):
        k = f'dem_{i}'
        d[k] = _normalize_layer_source_compare(str(d.get(k) or ''))
    return d


def build_flat_snapshot_from_mdepa_stored_settings(mdepa_path: str) -> dict:
    """Estado de config gravado no .pa.gpkg (step_*, DEMs, workflow), sem depender do PEC concluído.

    Usar **antes** de `persist_project_config_from_widgets` para comparar com os valores atuais
    dos widgets e obter a etapa de retomada correta quando ainda não existe `__pipeline_last_ok__`.
    """
    out: dict = {}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f"SELECT etapa, campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa LIKE 'step_%%'"
            )
            for etapa, campo, valor in cur.fetchall():
                if not etapa or valor is None:
                    continue
                out[f'{etapa}.{campo}'] = str(valor)
        finally:
            conn.close()
    except sqlite3.Error:
        return {}
    dems = load_dem_sources_from_project_path(mdepa_path)
    out['dem_0'] = dems.get(0, '') or ''
    out['dem_1'] = dems.get(1, '') or ''
    wf = load_workflow_ui_from_mdepa_path(mdepa_path)
    out['workflow.study_mode'] = str(int(wf.get('study_mode', 0)))
    out['workflow.pairs_mode'] = str(int(wf.get('pairs_mode', 0)))
    out['workflow.outliers_mode'] = str(int(wf.get('outliers_mode', 0)))
    out['workflow.study_layer_source'] = (wf.get('study_layer_source') or '').strip()
    return out


def compute_restart_etapa_from_snapshots(flat_now: dict, flat_was: dict):
    """Devolve (restart, extra). restart: None=completo desde polígonos; str=etapa; '__noop__'=sem alterações."""
    if not flat_was:
        return None, None
    flat_was = _coerce_snapshot_flat_for_dem_keys(flat_was)
    flat_now_d = _normalize_dem_sources_in_flat(flat_now)
    flat_was_d = _normalize_dem_sources_in_flat(flat_was)
    for i in (0, 1):
        if flat_now_d.get(f'dem_{i}') != flat_was_d.get(f'dem_{i}'):
            return None, 'dem'
    w_src_now = _normalize_layer_source_compare(str(flat_now.get('workflow.study_layer_source') or ''))
    w_src_was = _normalize_layer_source_compare(str(flat_was.get('workflow.study_layer_source') or ''))
    for wk in (
        'workflow.study_mode',
        'workflow.pairs_mode',
        'workflow.outliers_mode',
    ):
        if flat_now.get(wk) != flat_was.get(wk):
            return None, 'workflow'
    if w_src_now != w_src_was:
        return None, 'workflow'
    changed_steps = set()
    for k in set(flat_now) | set(flat_was):
        if k.startswith('dem_'):
            continue
        if flat_now.get(k) != flat_was.get(k):
            head = k.split('.', 1)[0]
            if head.startswith('step_'):
                changed_steps.add(head)
    if not changed_steps:
        return '__noop__', None
    ord_idx = _pipeline_etapa_order_index()
    candidates = []
    for sk in changed_steps:
        et = STEP_KEY_TO_RESTART_ETAPA.get(sk)
        if et:
            candidates.append(et)
    if not candidates:
        return None, 'unknown_step'
    return min(candidates, key=lambda e: ord_idx[e]), None

# Parâmetros do diálogo Config (por projeto): etapa = chave step_* em dic_param, campo = chave do field
PA_SETTINGS_TABLE = 'pa_settings'
# Fontes DEM (QgsRasterLayer.source()): etapa na tabela de settings, campo '0' | '1'
SETTINGS_ETAPA_DEM_SOURCES = 'dem_sources'
SETTINGS_ETAPA_DEM_SOURCES_LEGACY = 'raster_sources'

# Grupo na árvore de camadas: tem de ser estável entre locales e entre sessões (findGroup).
# Não usar self.tr() aqui — ao mudar o idioma o QGIS procuraria outro nome e criaria um grupo vazio.
PLUGIN_LAYER_TREE_ROOT_GROUP = '__MDE_AP__'
# Nome usado em versões anteriores para locale não pt (evitar grupo órfão após correção).
PLUGIN_LAYER_TREE_ROOT_GROUP_LEGACY = ('__DEM_PA__',)


def _ensure_pa_settings_table_conn(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''CREATE TABLE IF NOT EXISTS {PA_SETTINGS_TABLE} (
            etapa TEXT NOT NULL,
            campo TEXT NOT NULL,
            valor TEXT NOT NULL,
            PRIMARY KEY (etapa, campo)
        )'''
    )


def ensure_pa_settings_table(mdepa_path: str) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            _ensure_pa_settings_table_conn(conn)
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def load_plugin_settings_from_mdepa_path(mdepa_path: str, dic_param: dict) -> int:
    """Lê linhas (etapa, campo, valor) e atualiza dic_param[*]['fields'][*]['value']. Retorna quantos campos atualizou."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return 0
    if not ensure_pa_settings_table(mdepa_path):
        return 0
    n = 0
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT etapa, campo, valor FROM {PA_SETTINGS_TABLE}')
            for etapa, campo, valor in cur.fetchall():
                if etapa not in dic_param:
                    continue
                block = dic_param[etapa]
                if not isinstance(block, dict) or 'fields' not in block:
                    continue
                if campo not in block['fields']:
                    continue
                meta = block['fields'][campo]
                if valor is None:
                    continue
                if 'list' in meta:
                    try:
                        meta['value'] = int(valor)
                    except (TypeError, ValueError):
                        try:
                            meta['value'] = int(float(valor))
                        except (TypeError, ValueError):
                            continue
                else:
                    meta['value'] = str(valor)
                n += 1
        finally:
            conn.close()
    except sqlite3.Error:
        return n
    return n


def save_plugin_settings_to_mdepa_path(mdepa_path: str, dic_param: dict) -> bool:
    """Grava todos os fields dos step_* na tabela (upsert)."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for step, block in dic_param.items():
                if not isinstance(step, str) or not step.startswith('step_'):
                    continue
                if not isinstance(block, dict) or 'fields' not in block:
                    continue
                for campo, meta in block['fields'].items():
                    val = meta.get('value')
                    conn.execute(
                        f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                            VALUES (?, ?, ?)
                            ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                        (step, campo, str(val)),
                    )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def _normalize_layer_source_compare(source: str) -> str:
    """Comparação aproximada de caminhos de ficheiro (ex.: Windows)."""
    if not source:
        return ''
    s = source.strip()
    pipe = s.find('|')
    path_part = s[:pipe] if pipe >= 0 else s
    if path_part and os.path.isfile(path_part):
        return os.path.normcase(os.path.normpath(os.path.abspath(path_part)))
    return s


def find_dem_layer_in_project(source_str: str):
    """Devolve QgsRasterLayer (DEM) já no projeto com a mesma fonte que source_str, ou None."""
    if not source_str:
        return None
    proj = QgsProject.instance()
    norm = _normalize_layer_source_compare(source_str)
    for layer in proj.mapLayers().values():
        if not isinstance(layer, QgsRasterLayer):
            continue
        src = layer.source()
        if src == source_str:
            return layer
        if norm and _normalize_layer_source_compare(src) == norm:
            return layer
    return None


def find_vector_layer_in_project(source_str: str):
    """Devolve QgsVectorLayer já no projeto com a mesma fonte que source_str, ou None."""
    if not source_str:
        return None
    proj = QgsProject.instance()
    norm = _normalize_layer_source_compare(source_str)
    for layer in proj.mapLayers().values():
        if not isinstance(layer, QgsVectorLayer):
            continue
        src = layer.source()
        if src == source_str:
            return layer
        if norm and _normalize_layer_source_compare(src) == norm:
            return layer
    return None


def load_dem_sources_from_project_path(mdepa_path: str) -> dict:
    """Lê fontes guardadas para os slots 0 e 1 (referência / teste)."""
    out = {}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for etapa_key in (SETTINGS_ETAPA_DEM_SOURCES, SETTINGS_ETAPA_DEM_SOURCES_LEGACY):
                cur = conn.execute(
                    f'SELECT campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ?',
                    (etapa_key,),
                )
                for campo, valor in cur.fetchall():
                    try:
                        k = int(campo)
                    except (TypeError, ValueError):
                        continue
                    if k in (0, 1) and k not in out:
                        out[k] = '' if valor is None else str(valor)
        finally:
            conn.close()
    except sqlite3.Error:
        return out
    return out


def save_dem_sources_to_project_path(mdepa_path: str, key_to_source: dict) -> bool:
    """Persiste QgsRasterLayer.source() dos DEMs (slots 0 e 1; string vazia mantém a linha)."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for k in (0, 1):
                val = (key_to_source.get(k) or '').strip()
                conn.execute(
                    f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                        VALUES (?, ?, ?)
                        ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                    (SETTINGS_ETAPA_DEM_SOURCES, str(k), val),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


# Modo de fluxo (UI): etapa fixa, campos study_mode | pairs_mode | outliers_mode | study_layer_source
WORKFLOW_UI_ETAPA = 'workflow_ui'


def save_workflow_ui_to_mdepa_path(
    mdepa_path: str,
    study_mode: int,
    pairs_mode: int,
    outliers_mode: int,
    study_layer_source: str,
) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    rows = (
        ('study_mode', str(int(study_mode))),
        ('pairs_mode', str(int(pairs_mode))),
        ('outliers_mode', str(int(outliers_mode))),
        ('study_layer_source', (study_layer_source or '').strip()),
    )
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for campo, val in rows:
                conn.execute(
                    f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                        VALUES (?, ?, ?)
                        ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                    (WORKFLOW_UI_ETAPA, campo, val),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def load_workflow_ui_from_mdepa_path(mdepa_path: str) -> dict:
    out = {'study_mode': 0, 'pairs_mode': 0, 'outliers_mode': 0, 'study_layer_source': ''}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ?',
                (WORKFLOW_UI_ETAPA,),
            )
            for campo, valor in cur.fetchall():
                if campo == 'study_layer_source':
                    out['study_layer_source'] = '' if valor is None else str(valor)
                elif campo in ('study_mode', 'pairs_mode', 'outliers_mode'):
                    try:
                        out[campo] = int(valor)
                    except (TypeError, ValueError):
                        pass
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


# Estatísticas do painel (área, extensões, nº pares): etapa fixa em pa_settings
PANEL_STATS_ETAPA = 'panel_stats'


def save_panel_stats_to_mdepa_path(
    mdepa_path: str,
    area: str,
    ext_min: str,
    ext_match: str,
    pair_nr: str,
) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    rows = (
        ('area', area or ''),
        ('ext_min', ext_min or ''),
        ('ext_match', ext_match or ''),
        ('pair_nr', pair_nr or ''),
    )
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for campo, val in rows:
                conn.execute(
                    f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                        VALUES (?, ?, ?)
                        ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                    (PANEL_STATS_ETAPA, campo, val),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def load_panel_stats_from_mdepa_path(mdepa_path: str) -> dict:
    out = {'area': '', 'ext_min': '', 'ext_match': '', 'pair_nr': ''}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ?',
                (PANEL_STATS_ETAPA,),
            )
            for campo, valor in cur.fetchall():
                if campo in out:
                    out[campo] = '' if valor is None else str(valor)
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


# QSettings do plugin: só pasta para diálogo Abrir/Novo (não reabrir projeto no rótulo)
AUX_LAST_PROJECT_DIR_KEY = 'last_project_dir'


def pipeline_datetime_now_local() -> str:
    """Data e hora atual (local), sem microssegundos — compatível com datetime() do SQLite."""
    return datetime.datetime.now().replace(microsecond=0).strftime(PIPELINE_DATETIME_FMT)


def project_data_dir(project_file: str) -> str:
    """Pasta auxiliar ao lado do .pa.gpkg: mesmo nome base lógico (sem PROJECT_EXT)."""
    project_file = os.path.abspath(project_file)
    parent = os.path.dirname(project_file)
    stem = os.path.basename(_strip_project_ext(project_file))
    return os.path.join(parent, stem)


def normalize_project_pa_file(project_path: str) -> None:
    """Se o OGR criar `stem.gpkg` paralelo ao ficheiro pedido, consolidar no .pa.gpkg."""
    if not project_path or not project_path.lower().endswith(PROJECT_EXT.lower()):
        return
    root = _strip_project_ext(project_path)
    alt_gpkg = root + '.gpkg'
    has_project = os.path.isfile(project_path)
    has_gpkg = os.path.isfile(alt_gpkg)
    if not has_gpkg:
        return
    try:
        if not has_project:
            os.replace(alt_gpkg, project_path)
        else:
            if os.path.getmtime(alt_gpkg) > os.path.getmtime(project_path):
                os.remove(project_path)
                os.replace(alt_gpkg, project_path)
            else:
                os.remove(alt_gpkg)
    except OSError:
        pass


class PositionalAccuracyPlugin:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        self.name_ = 'MDE - Acurácia Posicional'
        # Save reference to the QGIS interface
        self.iface = iface
        # Traduções: tenta locale completo (ex. pt_BR), depois dois caracteres (ex. pt)
        self.translator = None
        locale_full = (QSettings().value('locale/userLocale') or '') or ''
        locale_full = str(locale_full).strip()
        locale_tag = locale_full.replace('-', '_')
        base = os.path.join(plugin_path, 'i18n')
        qm_candidates = [
            os.path.join(base, f'pos_accuracy_{locale_tag}.qm'),
            os.path.join(base, f'pos_accuracy_{locale_full[:2]}.qm') if len(locale_full) >= 2 else None,
            os.path.join(base, 'pos_accuracy_en.qm'),
        ]
        for qm_path in qm_candidates:
            if qm_path and os.path.isfile(qm_path):
                self.translator = QTranslator()
                if self.translator.load(qm_path):
                    QCoreApplication.installTranslator(self.translator)
                else:
                    self.translator = None
                break

        # Declare instance attributes

        self.actions = []
        self.menu = self.tr(f'&T {self.name_}')
        self.dic_prj_conn = {}
        self.dic_icon = {}

        # Check if plugin was started the first time in current QGIS session
        # Must be set in initGui() to survive plugin reloads
        self.first_start = None

    # noinspection PyMethodMayBeStatic
    def tr(self, message):
        """Get the translation for a string using Qt translation API.

        We implement this ourselves since we do not inherit QObject.

        :param message: String for translation.
        :type message: str, QString

        :returns: Translated version of message.
        :rtype: QString
        """
        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate(PLUGIN_I18N_CONTEXT, message)

    def add_action(self, icon_path, text, callback, enabled_flag=True, add_to_menu=True, add_to_toolbar=True,
                   status_tip=None, whats_this=None, parent=None):
        """Add a toolbar icon to the toolbar.

        :param icon_path: Path to the icon for this action. Can be a resource
            path (e.g. ':/plugins/foo/bar.png') or a normal file system path.
        :type icon_path: str

        :param text: Text that should be shown in menu items for this action.
        :type text: str

        :param callback: Function to be called when the action is triggered.
        :type callback: function

        :param enabled_flag: A flag indicating if the action should be enabled
            by default. Defaults to True.
        :type enabled_flag: bool

        :param add_to_menu: Flag indicating whether the action should also
            be added to the menu. Defaults to True.
        :type add_to_menu: bool

        :param add_to_toolbar: Flag indicating whether the action should also
            be added to the toolbar. Defaults to True.
        :type add_to_toolbar: bool

        :param status_tip: Optional text to show in a popup when mouse pointer
            hovers over the action.
        :type status_tip: str

        :param parent: Parent widget for the new action. Defaults None.
        :type parent: QWidget

        :param whats_this: Optional text to show in the status bar when the
            mouse pointer hovers over the action.

        :returns: The action that was created. Note that the action is also
            added to self.actions list.
        :rtype: QAction
        """

        icon = QIcon()
        icon.addPixmap(QPixmap(icon_path))
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            # Adds plugin icon to Plugins toolbar
            self.iface.addToolBarIcon(action)

        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)

        self.actions.append(action)

        return action

    def initGui(self):
        print('initGui')
        """Create the menu entries and toolbar icons inside the QGIS GUI."""
        # self.dock = QDockWidget('T - Inventário de Via.')

        # QDockWidget: painel customizado; QgsAdvancedDigitizingDockWidget é para CAD e pode esconder nosso conteúdo
        self.dock1 = QDockWidget()
        self.title1 = f'{self.name_}.'
        self.dock1.setWindowTitle(self.title1)

        self.wd1 = Wd1(self.iface, parent=self.dock1, main=self)
        self.wd1.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.dock1.setWidget(self.wd1)
        self.dock1.setObjectName(f"{self.title1} Panel")
        self.dock1.setMinimumHeight(60)

        self.iface.addDockWidget(Qt.LeftDockWidgetArea, self.dock1)
        icon_path = os.path.join(plugin_path, 'icons/icon_bfn.png')

        self.add_action(
            icon_path,
            text=self.tr(''),
            callback=self.call_vs,
            parent=self.iface.mainWindow())

        self.first_start = True

    def unload(self):
        print('unload')
        """Remove GUI e libera recursos (compatível com Plugin Reloader)."""
        if getattr(self, 'wd1', None):
            self.wd1.unload_cleanup()

        for action in list(self.actions):
            self.iface.removePluginMenu(self.menu, action)
            self.iface.removeToolBarIcon(action)
        self.actions.clear()

        if getattr(self, 'dock1', None):
            self.iface.removeDockWidget(self.dock1)
            self.dock1.setParent(None)
            self.dock1.deleteLater()
            self.dock1 = None
        self.wd1 = None

        if getattr(self, 'translator', None):
            QCoreApplication.removeTranslator(self.translator)
            self.translator = None

    def call_vs(self):
        if not self.dock1.isVisible():
            self.dock1.setVisible(True)
        # self.inv_wd.get_inv()

    @classmethod
    def plugin_version(self):
        meta_file = plugin_path + "/metadata.txt"
        # print(meta_file)
        with open(meta_file) as meta:
            mt = meta.readlines()
            for l_ in mt:
                if l_[:8] == "version=":
                    return l_[8:].replace('\n', '')
        return '0.0'


class Wd1(QWidget):
    def __init__(self, iface, parent=None, main=None):

        super(Wd1, self).__init__(parent)
        # Save reference to the QGIS interface
        self.iface = iface
        self.parent = parent
        self.main = main
        name_ = self.main.name_.replace(' ', '_')
        self.setObjectName(f'Wd_{name_}')

        if os.getlogin() == 'adria':
            self.iface.actionShowPythonDialog().trigger()

        self.dic_prj = \
            {'path': '',  # pasta de dados (logs, exports): vizinha ao .pa.gpkg, mesmo nome base
             'project_file': '',  # caminho absoluto do arquivo .pa.gpkg
             'dems': {
                 0: {
                     'type': 'Referencia',
                     'obj_cbx': None,
                     'obj_pb': None,
                     'obj_prog_bar': None,
                     'geom_status': False},
                 1: {
                     'type': 'Teste',
                     'obj_cbx': None,
                     'obj_pb': None,
                     'obj_prog_bar': None,
                     'geom_status': False},
             },
             'matchs': {
                 'obj_prog_bar': None,
             },
             'standard': {
                 'name': 'pa_accuracy',
                 'files': {
                     'log': '.log',
                     'result_txt': '_result.txt',
                     'result_prof': '_prof.csv',
                 }}}
        self.dic_match = {}
        self.srid = None
        self.crs_epsg = None
        self.gpkg_path = ''
        self.workers = None
        self.task_queue = None
        self.folder_out_path = ''
        self.list_add_tool = ['...', 'add_folder', 'add_files', 'clear']

        self.max_threads = 3  # Limit to 3 concurrent tasks

        self.task_queue = Queue()  # Task queue
        self.threads_running = 0  # Track active threads
        self.active_workers = {}  # Keep track of active workers
        self._workflow_pause = None  # None | 'post_intersection' | 'post_pairs_review'
        self._panel_stats_cache = {'area': '', 'ext_min': '', 'ext_match': '', 'pair_nr': ''}
        self._pec_report_pec_intro = ''  # nota de outliers (PEC) para o relatório PDF
        self._pec_report_plan_rows = []  # linhas da tabela PEC planimétrico
        self._pec_report_alt_rows = []  # linhas da tabela PEC altimétrico

        self.dic_lb_texts = {
            'area': tr_ui('Área de estudo: {} km²'),
            'ext_min': tr_ui('Extensão mínima da amostra: {} km'),
            'ext_match': tr_ui('Extensão da Amostra: {} km'),
            'pair_nr': tr_ui('Número de pares homólogos: {}'),
        }
        self.aux_tools = AuxTools(parent=self)
        lg = self.create_layout()
        self.setLayout(lg)
        self.dic_pec_mm = {
            'H': {
                'A': {
                    'pec': 0.28,
                    'ep': 0.17
                },
                'B': {
                    'pec': 0.5,
                    'ep': 0.3
                },
                'C': {
                    'pec': 0.8,
                    'ep': 0.5
                },
                'D': {
                    'pec': 1.0,
                    'ep': 0.6
                },
            },
            'V': {
                'A': {
                    'pec': 0.27,
                    'ep': 0.17
                },
                'B': {
                    'pec': 0.5,
                    'ep': 0.33
                },
                'C': {
                    'pec': 0.6,
                    'ep': 0.4
                },
                'D': {
                    'pec': 0.75,
                    'ep': 0.5
                },
            },
        }
        self.dic_pec_v = {
            1: 1,
            2: 1,
            5: 2,
            10: 5,
            25: 10,
            50: 20,
            100: 50,
            250: 100,
            500: 200,
            1000: 200,
        }
        self.list_norm_type = [
            tr_ui('Escalar'), tr_ui('Mínima Distância'), tr_ui('Sem Normalização')]
        self.settings_dlg = SettingsDlg(main=parent, parent=self)
        self.list_morph = ['Cumeada', 'Hidrografia_Numerica']

        self.intersection_name = '__Limit_Intersecao__'
        self.buffer_name = '__Buffers__'
        self.match_lines_layer_name = '__Linhas_de_Correspondencia__'
        self.layer_buffers = None

    def tr(self, message):
        """Textos traduzíveis do painel (mesmo contexto que o menu do plugin)."""
        return tr_ui(message)

    def create_layout(self):
        gl_tool = QGridLayout()
        gl_tool.setContentsMargins(0, 0, 0, 0)
        gl_tool.setSpacing(1)

        self.lb_session_logo = QLabel()
        self.lb_session_logo.setFixedSize(QSize(40, 40))
        pixmap_ = QPixmap(os.path.join(plugin_path, 'icons/icon_bfn.png'))
        scaled_ = pixmap_.scaled(self.lb_session_logo.size(), Qt.KeepAspectRatio)
        self.lb_session_logo.setPixmap(scaled_)
        r_ = 0
        gl_tool.addWidget(self.lb_session_logo, r_, 0, Qt.AlignVCenter)

        row_hdr = QHBoxLayout()
        row_hdr.setContentsMargins(0, 0, 0, 0)
        row_hdr.setSpacing(6)
        row_hdr.addStretch(1)
        self.pb_config = QPushButton()
        self.pb_config.setToolTip(self.tr('Config'))
        self.pb_config.setIcon(_icon_from_icons_file('icon_config.png'))
        self.pb_config.setIconSize(QSize(30, 30))
        self.pb_config.setFixedSize(32, 32)
        self.pb_config.setCursor(Qt.PointingHandCursor)
        self.pb_config.setFocusPolicy(Qt.StrongFocus)
        self.pb_config.setStyleSheet(
            'QPushButton { border: 1px solid #9e9e9e; border-radius: 2px; padding: 0; margin: 0; background: palette(base); }'
            'QPushButton:hover { background: palette(alternate-base); }'
            'QPushButton:pressed { background: palette(mid); }')
        row_hdr.addWidget(self.pb_config, 0, Qt.AlignVCenter)
        self.lb_version = QLabel(f'v{self.main.plugin_version()}')
        self.lb_version.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        row_hdr.addWidget(self.lb_version, 0, Qt.AlignVCenter)
        gl_tool.addLayout(row_hdr, r_, 1, 1, 3)

        r_ += 1
        sep_line = QFrame()
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 4)

        r_ += 1
        gl_prj = QGridLayout()
        self.lb_title_proj = QLabel(self.tr('Projeto (.pa.gpkg):'))
        gl_prj.addWidget(self.lb_title_proj, 0, 0)
        self.lb_status_proj = QLabel(self.tr('Não definido'))
        gl_prj.addWidget(self.lb_status_proj,  0, 1)
        gl_tool.addLayout(gl_prj, r_, 0, 1, 4)

        r_ += 1
        self.lb_path_proj = QLabel('~~~')
        self.lb_path_proj.setWordWrap(True)
        gl_tool.addWidget(self.lb_path_proj, r_, 0, 1, 3)
        _proj_btn_style = (
            'QPushButton { border: 1px solid #9e9e9e; border-radius: 2px; padding: 0; margin: 0; '
            'min-width: 32px; max-width: 32px; min-height: 32px; max-height: 32px; '
            'background: palette(base); }'
            'QPushButton:hover { background: palette(alternate-base); }'
            'QPushButton:pressed { background: palette(mid); }')
        lay_proj_btns = QHBoxLayout()
        lay_proj_btns.setContentsMargins(0, 0, 0, 0)
        lay_proj_btns.setSpacing(4)
        self.pb_project_new = QPushButton()
        _ic_new = _icon_from_icons_file('icon_new.png')
        if _ic_new.isNull():
            self.pb_project_new.setText('+')
            _f_plus = self.pb_project_new.font()
            _f_plus.setBold(True)
            self.pb_project_new.setFont(_f_plus)
        else:
            self.pb_project_new.setIcon(_ic_new)
            self.pb_project_new.setIconSize(QSize(24, 24))
        self.pb_project_new.setStyleSheet(_proj_btn_style)
        self.pb_project_new.setToolTip(self.tr('Novo projeto…'))
        self.pb_project_new.setCursor(Qt.PointingHandCursor)
        self.pb_project_new.clicked.connect(self.new_project_dialog)
        lay_proj_btns.addWidget(self.pb_project_new)
        self.pb_project_open = QPushButton()
        _ic_open = _icon_from_icons_file('icon_open.png')
        if _ic_open.isNull():
            self.pb_project_open.setText('...')
        else:
            self.pb_project_open.setIcon(_ic_open)
            self.pb_project_open.setIconSize(QSize(24, 24))
        self.pb_project_open.setStyleSheet(_proj_btn_style)
        self.pb_project_open.setToolTip(self.tr('Abrir projeto…'))
        self.pb_project_open.setCursor(Qt.PointingHandCursor)
        self.pb_project_open.clicked.connect(self.open_project_dialog)
        lay_proj_btns.addWidget(self.pb_project_open)
        w_proj_btns = QWidget()
        w_proj_btns.setLayout(lay_proj_btns)
        gl_tool.addWidget(w_proj_btns, r_, 3, Qt.AlignRight | Qt.AlignTop)

        r_ += 1
        sep_line = QFrame(self)
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 4)

        for key_ in self.dic_prj['dems']:

            r_ += 1
            lb_title_ = QLabel(
                self.tr('MDE de referência:') if key_ == 0 else self.tr('MDE de teste:'))
            gl_tool.addWidget(lb_title_, r_, 0)
            obj_pb = QPushButton(self.tr('info'))
            obj_pb.setMaximumWidth(32)
            gl_tool.addWidget(obj_pb, r_, 3, 1, 1, Qt.AlignRight)
            self.dic_prj['dems'][key_]['obj_pb'] = obj_pb
            r_ += 1
            obj_cbx = QgsMapLayerComboBox(self)
            obj_cbx.setFilters(QgsMapLayerProxyModel.RasterLayer)
            obj_cbx.setAllowEmptyLayer(True)
            gl_tool.addWidget(obj_cbx, r_, 0, 1, 3)
            self.dic_prj['dems'][key_]['obj_cbx'] = obj_cbx
            r_ += 1
            obj_prog_bar = QProgressBar(self)
            gl_tool.addWidget(obj_prog_bar, r_, 0, 1, 4)
            self.dic_prj['dems'][key_]['obj_prog_bar'] = obj_prog_bar

        r_ += 1
        sep_line = QFrame(self)
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 4)  

        
        r_ += 1        
        r_start_run = r_
        gl_tool.addWidget(QLabel(self.tr('Área de estudos:')), r_, 0)
        r_ += 1
        self.cbx_workflow_study = QComboBox(self)
        self.cbx_workflow_study.addItems([
            self.tr('Calcular pela interseção dos MDEs'),
            self.tr('Editar após interseção'),
            self.tr('Selecionar de uma camada'),
        ])
        gl_tool.addWidget(self.cbx_workflow_study, r_, 0, 1, 2)
        r_ += 1
        self.lb_area = QLabel(self.dic_lb_texts['area'].format('—'))
        gl_tool.addWidget(self.lb_area, r_, 0)
        r_ += 1
        self.lb_ext_min = QLabel(self.dic_lb_texts['ext_min'].format('—'))
        gl_tool.addWidget(self.lb_ext_min, r_, 0)

        r_ += 1
        self.lb_study_layer = QLabel(self.tr('Camada polígono (área de estudo):'))
        gl_tool.addWidget(self.lb_study_layer, r_, 0)
        r_ += 1
        self.cbx_study_area_layer = QgsMapLayerComboBox(self)
        self.cbx_study_area_layer.setFilters(QgsMapLayerProxyModel.PolygonLayer)
        self.cbx_study_area_layer.setAllowEmptyLayer(True)
        self.lb_study_layer.setVisible(False)
        self.cbx_study_area_layer.setVisible(False)
        gl_tool.addWidget(self.cbx_study_area_layer, r_, 0, 1, 2)

        r_ += 1
        sep_line_wf = QFrame(self)
        sep_line_wf.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line_wf, r_, 0, 1, 2)

        r_ += 1
        gl_tool.addWidget(QLabel(self.tr('Seleção de pares homólogos:')), r_, 0)
        r_ += 1
        self.cbx_workflow_pairs = QComboBox(self)
        self.cbx_workflow_pairs.addItems([
            self.tr('Automática'),
            self.tr('Revisar'),
        ])
        gl_tool.addWidget(self.cbx_workflow_pairs, r_, 0, 1, 2)

        r_ += 1
        self.lb_ext_match = QLabel()
        gl_tool.addWidget(self.lb_ext_match, r_, 0)
        r_ += 1
        self.lb_pair_nr = QLabel()
        gl_tool.addWidget(self.lb_pair_nr, r_, 0)
        self._refresh_extent_and_pairs_labels()

        r_ += 1
        sep_line_wf = QFrame(self)
        sep_line_wf.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line_wf, r_, 0, 1, 2)

        r_ += 1
        gl_tool.addWidget(QLabel(self.tr('PEC — outliers:')), r_, 0)
        r_ += 1
        self.cbx_workflow_outliers = QComboBox(self)
        self.cbx_workflow_outliers.addItems([
            self.tr('Remover automaticamente'),
            self.tr('Avaliar individualmente'),
            self.tr('Usar todos'),
        ])
        gl_tool.addWidget(self.cbx_workflow_outliers, r_, 0, 1, 2)

        r_ += 1
        sep_line_wf = QFrame(self)
        sep_line_wf.setFrameShape(QFrame.VLine)
        gl_tool.addWidget(sep_line_wf, r_start_run, 2, r_ - r_start_run, 1, Qt.AlignHCenter)

        self.pb_proc = QPushButton(self.tr('Avaliar'))
        gl_tool.addWidget(self.pb_proc, r_start_run, 3, r_ - r_start_run, 1, Qt.AlignHCenter)
        self._refresh_proc_button()

        r_ += 1
        self.lb_log = QLabel(self.tr('LOG:'))
        gl_tool.addWidget(self.lb_log, r_, 0, 1, 1)
        r_ += 1
        self.pte_log = QPlainTextEdit ()
        self.pte_log.setReadOnly(True)  # Logs are read-only
        self.pte_log.setWordWrapMode(QTextOption.WordWrap)  # Prevents long lines from wrapping
        self.pte_log.setBackgroundVisible(False)  # Optional, makes it look cleaner
        self.pte_log.setFont(QFont("Monospace", 8))  # Use a monospace font for better alignment
        gl_tool.addWidget(self.pte_log, r_, 0, 1, 4)

        lg_sa = QGridLayout()
        lg_sa.setContentsMargins(0, 0, 0, 0)
        lg_sa.setSpacing(0)
        lg_sa.addLayout(gl_tool, 0, 0)

        self.trigger_actions()
        return lg_sa

    def trigger_actions(self):
        for key_ in self.dic_prj['dems']:
            self.dic_prj['dems'][key_]['obj_pb'].clicked.connect(partial(self.log_dem_layer_info, key_=key_))
            self.dic_prj['dems'][key_]['obj_cbx'].layerChanged.connect(self.persist_dem_layer_selection)
        self.cbx_workflow_study.currentIndexChanged.connect(self._on_workflow_study_changed)
        self.cbx_workflow_study.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_workflow_pairs.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_workflow_outliers.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_study_area_layer.layerChanged.connect(self._persist_workflow_ui_if_project)
        self.pb_proc.clicked.connect(self.exec_analyze)
        self.pb_config.clicked.connect(self.open_settings)

    def set_project_paths(self, project_file: str):
        """Define arquivo .pa.gpkg e pasta de dados (logs etc.) com o mesmo nome base."""
        project_file = os.path.normpath(os.path.abspath(project_file))
        pl = project_file.lower()
        if pl.endswith(PROJECT_EXT.lower()):
            pass
        else:
            root, ext = os.path.splitext(project_file)
            if ext.lower() == '.gpkg':
                if root.lower().endswith('.pa'):
                    project_file = project_file
                else:
                    project_file = root + PROJECT_EXT
            else:
                project_file = (root + PROJECT_EXT) if ext else project_file + PROJECT_EXT
        self.dic_prj['project_file'] = project_file
        self.gpkg_path = project_file
        data_dir = project_data_dir(project_file)
        self.dic_prj['path'] = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.lb_path_proj.setText(project_file)

    def _dialog_start_dir(self) -> str:
        d = self.aux_tools.get_(key_=AUX_LAST_PROJECT_DIR_KEY)
        if d and isinstance(d, str) and os.path.isdir(d):
            return d
        legacy = self.aux_tools.get_(key_='project_file')
        if legacy and isinstance(legacy, str):
            parent = os.path.dirname(legacy)
            if parent and os.path.isdir(parent):
                return parent
        return ''

    def _persist_panel_stats_to_mdepa(self):
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        c = self._panel_stats_cache
        save_panel_stats_to_mdepa_path(
            pf,
            c.get('area', ''),
            c.get('ext_min', ''),
            c.get('ext_match', ''),
            c.get('pair_nr', ''),
        )

    def _reset_panel_stats_ui(self):
        self._panel_stats_cache = {'area': '', 'ext_min': '', 'ext_match': '', 'pair_nr': ''}
        self.lb_area.setText(self.dic_lb_texts['area'].format('—'))
        self.lb_ext_min.setText(self.dic_lb_texts['ext_min'].format('—'))
        self._refresh_extent_and_pairs_labels()

    def _refresh_extent_and_pairs_labels(self):
        em_raw = (self._panel_stats_cache.get('ext_match') or '').strip()
        em_m = self._float_from_panel_str(em_raw)
        if em_m is not None and em_m > 0:
            em_km = round(em_m / 1000.0, 1)
            em_disp = str(em_km)
        else:
            em_disp = '—'
        pr = (self._panel_stats_cache.get('pair_nr') or '').strip()
        self.lb_ext_match.setText(self.dic_lb_texts['ext_match'].format(em_disp))
        self.lb_pair_nr.setText(self.dic_lb_texts['pair_nr'].format(pr or '—'))

    def restore_panel_stats_from_project(self):
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        data = load_panel_stats_from_mdepa_path(pf)
        self._panel_stats_cache = {k: (data.get(k) or '') for k in self._panel_stats_cache}
        self.lb_area.setText(
            self.dic_lb_texts['area'].format(self._panel_stats_cache['area'] or '—'))
        self.lb_ext_min.setText(
            self.dic_lb_texts['ext_min'].format(self._panel_stats_cache['ext_min'] or '—'))
        self._refresh_extent_and_pairs_labels()

    def open_project_dialog(self):
        start_dir = self._dialog_start_dir()
        path, _ = QFileDialog.getOpenFileName(
            self, self.tr('Abrir projeto'), start_dir, project_file_filter_i18n())
        if not path:
            return
        if not os.path.isfile(path):
            self.log_message(self.tr('Arquivo não encontrado: {0}').format(path), 'ERROR')
            return
        self.set_project_paths(path)
        self.ensure_pipeline_etapas_table()
        self.aux_tools.save_(
            key_=AUX_LAST_PROJECT_DIR_KEY,
            value_=os.path.dirname(self.dic_prj['project_file']),
        )
        self.check_prj_folder(self.dic_prj['project_file'])
        self.reload_settings_from_project_file()
        self.log_message(
            self.tr('Projeto aberto: {0}').format(self.dic_prj['project_file']), 'INFO')

    def new_project_dialog(self):
        # Mesma lógica que «Abrir projeto»: last_project_dir (gravado ao abrir/criar), não a chave legacy project_file.
        start_dir = self._dialog_start_dir()
        # Sem PROJECT_EXT no nome sugerido: no Windows o QFileDialog costuma acrescentar a extensão do filtro.
        suggest = os.path.join(start_dir, 'novo_projeto') if start_dir else 'novo_projeto'
        path, _ = QFileDialog.getSaveFileName(
            self, self.tr('Novo projeto'), suggest, project_file_filter_i18n())
        if not path:
            return
        path = os.path.normpath(os.path.abspath(path))
        dup_ext = (PROJECT_EXT + PROJECT_EXT).lower()
        while path.lower().endswith(dup_ext):
            path = path[:-len(PROJECT_EXT)]
        pl = path.lower()
        if not pl.endswith(PROJECT_EXT.lower()):
            root, ext = os.path.splitext(path)
            if ext.lower() == '.gpkg':
                if not root.lower().endswith('.pa'):
                    path = root + PROJECT_EXT
            else:
                path = root + PROJECT_EXT
        if os.path.isfile(path):
            self.log_message(
                self.tr('Já existe um arquivo com esse nome. Escolha outro nome ou use Abrir projeto.'),
                'ERROR')
            return
        parent = os.path.dirname(path)
        if not parent or not os.path.isdir(parent):
            self.log_message(self.tr('Diretório inválido para salvar o projeto.'), 'ERROR')
            return
        self.set_project_paths(path)
        proj_crs = QgsProject.instance().crs()
        crs_auth = proj_crs.authid() if proj_crs.isValid() else 'EPSG:4326'
        try:
            if not self._create_empty_mdepa_shell(path):
                raise RuntimeError('create_empty_mdepa_shell retornou False')
            if not self.ensure_pipeline_etapas_table():
                raise RuntimeError('ensure_pipeline_etapas_table retornou False')
        except Exception as e:
            if os.path.isfile(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
            root = _strip_project_ext(path)
            stray_gpkg = root + '.gpkg'
            if os.path.isfile(stray_gpkg):
                try:
                    os.remove(stray_gpkg)
                except OSError:
                    pass
            self.dic_prj['project_file'] = ''
            self.gpkg_path = ''
            self.dic_prj['path'] = ''
            self.lb_path_proj.setText('~~~')
            self.log_message(self.tr('Não foi possível criar o projeto: {0}').format(e), 'ERROR')
            return
        self.aux_tools.save_(key_=AUX_LAST_PROJECT_DIR_KEY, value_=os.path.dirname(path))
        self.check_prj_folder(path)
        self._reset_panel_stats_ui()
        self.persist_project_config_from_widgets(log_values=False)
        self.reload_settings_from_project_file()
        self.log_message(
            self.tr('Novo projeto criado: {0} (CRS inicial: {1})').format(path, crs_auth), 'INFO')

    def reload_settings_from_project_file(self):
        """Restaura dic_param: defaults → QSettings → valores gravados no .pa.gpkg (por projeto)."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        dlg = self.settings_dlg
        dlg.apply_defaults_to_values()
        dlg.get_dic_from_settings()
        load_plugin_settings_from_mdepa_path(pf, dlg.dic_param)
        dlg.sync_widgets_from_dic_param()
        self.restore_dem_layers_from_project()
        self.restore_workflow_ui_from_project()
        self.restore_panel_stats_from_project()

    def persist_dem_layer_selection(self):
        """Grava QgsRasterLayer.source() dos combos no .pa.gpkg (slots 0 e 1)."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        key_to_source = {}
        for key_ in (0, 1):
            cbx = self.dic_prj['dems'][key_]['obj_cbx']
            ly = cbx.currentLayer() if cbx else None
            if isinstance(ly, QgsRasterLayer):
                key_to_source[key_] = ly.source()
            else:
                key_to_source[key_] = ''
        save_dem_sources_to_project_path(pf, key_to_source)

    def restore_dem_layers_from_project(self):
        """Carrega DEMs guardados no .pa.gpkg se ainda não estiverem no projeto QGIS."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        sources = load_dem_sources_from_project_path(pf)
        if not sources:
            return
        for key_ in (0, 1):
            cbx = self.dic_prj['dems'][key_]['obj_cbx']
            uri = (sources.get(key_) or '').strip()
            cbx.blockSignals(True)
            try:
                if not uri:
                    cbx.setLayer(None)
                    continue
                existing = find_dem_layer_in_project(uri)
                if existing is not None:
                    cbx.setLayer(existing)
                    continue
                base = os.path.basename(uri.split('|')[0].strip()) or f'DEM_{key_}'
                label = self.dic_prj['dems'][key_]['type']
                layer_name = f'{label} ({base})'
                rl = QgsRasterLayer(uri, layer_name)
                if not rl.isValid():
                    self.log_message(
                        self.tr('Não foi possível carregar o DEM: {0}').format(uri), 'ERROR')
                    cbx.setLayer(None)
                    continue
                QgsProject.instance().addMapLayer(rl)
                cbx.setLayer(rl)
            finally:
                cbx.blockSignals(False)

    def save_plugin_settings_to_project(self, dic_param: dict) -> bool:
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return False
        return save_plugin_settings_to_mdepa_path(pf, dic_param)

    def persist_project_config_from_widgets(self, log_values: bool = False):
        """Atualiza dic_param a partir dos widgets, grava QSettings e .pa.gpkg (parâmetros + MDEs)."""
        dlg = self.settings_dlg
        dlg.flush_widgets_to_dic_param(log_values=log_values)
        dic_save = {}
        for item_i in dlg.dic_param:
            if not item_i.startswith('step_'):
                continue
            dic_save[item_i] = {
                item_j: dlg.dic_param[item_i]['fields'][item_j]['value']
                for item_j in dlg.dic_param[item_i]['fields']
            }
        dlg.aux_tools.save_dic(dic_=dic_save, key_='dic_param')
        self.save_plugin_settings_to_project(dlg.dic_param)
        self.persist_dem_layer_selection()
        # Não limpar _workflow_pause aqui: senão um segundo exec_analyze (reentrância/Qt)
        # vê pausa já limpa e faz reprocessamento completo em vez de Continuar → morfologia/buffers.
        self._persist_workflow_ui_if_project(clear_workflow_pause=False)
        self._persist_panel_stats_to_mdepa()

    def _flatten_run_snapshot(self) -> dict:
        """Parâmetros dos steps + fontes DEM (para comparar com a última avaliação concluída)."""
        out = {}
        dlg = self.settings_dlg
        for sk, block in dlg.dic_param.items():
            if not isinstance(sk, str) or not sk.startswith('step_'):
                continue
            if not isinstance(block, dict) or 'fields' not in block:
                continue
            for fk, meta in block['fields'].items():
                if isinstance(meta, dict):
                    out[f'{sk}.{fk}'] = str(meta.get('value', ''))
        for i in (0, 1):
            cbx = self.dic_prj['dems'][i]['obj_cbx']
            ly = cbx.currentLayer() if cbx else None
            out[f'dem_{i}'] = ly.source() if isinstance(ly, QgsRasterLayer) else ''
        out['workflow.study_mode'] = str(self.cbx_workflow_study.currentIndex())
        out['workflow.pairs_mode'] = str(self.cbx_workflow_pairs.currentIndex())
        out['workflow.outliers_mode'] = str(self.cbx_workflow_outliers.currentIndex())
        sly = self.cbx_study_area_layer.currentLayer()
        out['workflow.study_layer_source'] = (
            sly.source() if isinstance(sly, QgsVectorLayer) else '')
        return out

    def _refresh_proc_button(self):
        if self._workflow_pause == 'post_intersection':
            self.pb_proc.setText(self.tr('Continuar'))
            self.pb_proc.setToolTip(
                self.tr('Continuar para morfologia após editar a área de interseção.'))
            ic = _icon_from_icons_file('icon_continuar.png')
        elif self._workflow_pause == 'post_pairs_review':
            self.pb_proc.setText(self.tr('Continuar'))
            self.pb_proc.setToolTip(
                self.tr('Continuar para gerar buffers após rever os pares.'))
            ic = _icon_from_icons_file('icon_continuar.png')
        else:
            self.pb_proc.setText(self.tr('Avaliar'))
            self.pb_proc.setToolTip(self.tr('Executar ou retomar a análise.'))
            ic = _icon_from_icons_file('icon_avaliar.png')
        if not ic.isNull():
            self.pb_proc.setIcon(ic)
            self.pb_proc.setIconSize(QSize(22, 22))
        else:
            self.pb_proc.setIcon(QIcon())

    def _on_workflow_study_changed(self, idx):
        show = idx == 2
        self.lb_study_layer.setVisible(show)
        self.cbx_study_area_layer.setVisible(show)

    def _persist_workflow_ui_if_project(self, clear_workflow_pause: bool = True):
        if clear_workflow_pause and self._workflow_pause is not None:
            self._workflow_pause = None
            self._refresh_proc_button()
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        ly = self.cbx_study_area_layer.currentLayer()
        src = ly.source() if isinstance(ly, QgsVectorLayer) else ''
        save_workflow_ui_to_mdepa_path(
            pf,
            self.cbx_workflow_study.currentIndex(),
            self.cbx_workflow_pairs.currentIndex(),
            self.cbx_workflow_outliers.currentIndex(),
            src,
        )

    def restore_workflow_ui_from_project(self):
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        data = load_workflow_ui_from_mdepa_path(pf)
        widgets = (
            self.cbx_workflow_study,
            self.cbx_workflow_pairs,
            self.cbx_workflow_outliers,
            self.cbx_study_area_layer,
        )
        for w in widgets:
            w.blockSignals(True)
        try:
            self.cbx_workflow_study.setCurrentIndex(
                max(0, min(int(data.get('study_mode', 0)), 2)))
            self.cbx_workflow_pairs.setCurrentIndex(
                max(0, min(int(data.get('pairs_mode', 0)), 1)))
            self.cbx_workflow_outliers.setCurrentIndex(
                max(0, min(int(data.get('outliers_mode', 0)), 2)))
            src = (data.get('study_layer_source') or '').strip()
            if src:
                vl = find_vector_layer_in_project(src)
                self.cbx_study_area_layer.setLayer(vl)
            else:
                self.cbx_study_area_layer.setLayer(None)
        finally:
            for w in widgets:
                w.blockSignals(False)
        self._on_workflow_study_changed(self.cbx_workflow_study.currentIndex())

    def _count_outliers_flagged(self, dic_values):
        n = 0
        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                for count_ in dic_values[scale_][class_]:
                    if dic_values[scale_][class_][count_].get('outlier'):
                        n += 1
        return n

    def apply_study_area_from_map_layer(self) -> bool:
        self.log_message(self.tr('ÁREA DE ESTUDO A PARTIR DA CAMADA'), 'INFO')
        ly = self.cbx_study_area_layer.currentLayer()
        if not isinstance(ly, QgsVectorLayer) or not ly.isValid():
            self.log_message(self.tr('Selecione uma camada de polígonos válida.'), 'ERROR')
            return False
        if ly.geometryType() != QgsWkbTypes.PolygonGeometry:
            self.log_message(self.tr('A camada de área de estudo tem de ser poligonal.'), 'ERROR')
            return False
        tgt_crs = QgsCoordinateReferenceSystem(self.crs_epsg)
        if not tgt_crs.isValid():
            self.log_message(self.tr('CRS do MDE de referência inválido.'), 'ERROR')
            return False
        xform = QgsCoordinateTransform(ly.crs(), tgt_crs, QgsProject.instance())
        geoms = []
        for f in ly.getFeatures():
            g = f.geometry()
            if g is None or g.isEmpty():
                continue
            g2 = QgsGeometry(g)
            try:
                g2.transform(xform)
            except Exception:
                self.log_message(
                    self.tr('Falha ao reprojetar geometrias para o CRS do projeto.'), 'ERROR')
                return False
            geoms.append(g2)
        if not geoms:
            self.log_message(
                self.tr('A camada de área de estudo não tem polígonos válidos.'), 'ERROR')
            return False
        union_g = QgsGeometry.unaryUnion(geoms)
        if union_g.isEmpty():
            self.log_message(self.tr('União da área de estudo está vazia.'), 'ERROR')
            return False
        self._clear_features_from_limit_layers()
        for key_ in (0, 1):
            name = f'__Limit_{self.dic_prj["dems"][key_]["type"]}__'
            layer = self._resolve_limit_layer_for_editing(name)
            if layer is None:
                self.log_message(
                    self.tr('Camada de limite indisponível: {0}').format(name), 'ERROR')
                return False
            feat = QgsFeature(layer.fields())
            feat.setGeometry(QgsGeometry(union_g))
            layer.startEditing()
            layer.addFeature(feat)
            layer.commitChanges()
            layer.updateExtents()
            layer.triggerRepaint()
            self.dic_prj['dems'][key_]['geom_status'] = True
        self.run_polygon_intersection()
        return True

    def _gpkg_layer_valid(self, layer_name: str) -> bool:
        """True se a camada nomeada existe e abre no projeto .pa.gpkg atual."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        uri = f'{self.gpkg_path}|layername={layer_name}'
        vl = QgsVectorLayer(uri, layer_name, 'ogr')
        return vl.isValid()

    def _create_empty_mdepa_shell(self, path: str) -> bool:
        """GPKG vazio (sem camadas de limite); tabelas vetoriais na primeira Avaliar → _ensure_limit_vector_tables_in_mdepa."""
        try:
            drv = ogr.GetDriverByName('GPKG')
            if drv is None:
                return False
            ds = drv.CreateDataSource(path)
            if ds is None:
                return False
            ds = None
            normalize_project_pa_file(path)
            return os.path.isfile(path)
        except Exception:
            return False

    def _plugin_layer_tree_group(self):
        """Garante o grupo raiz do plugin na árvore (nome técnico fixo; ver PLUGIN_LAYER_TREE_ROOT_GROUP)."""
        root = QgsProject.instance().layerTreeRoot()
        for gname in (PLUGIN_LAYER_TREE_ROOT_GROUP,) + PLUGIN_LAYER_TREE_ROOT_GROUP_LEGACY:
            grp = root.findGroup(gname)
            if grp:
                return grp
        return root.insertGroup(0, PLUGIN_LAYER_TREE_ROOT_GROUP)

    def _gpkg_has_any_vector_layer(self) -> bool:
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        ds = ogr.Open(self.gpkg_path, 0)
        if ds is None:
            return False
        try:
            return ds.GetLayerCount() > 0
        finally:
            ds = None

    def _ensure_limit_vector_tables_in_mdepa(self, crs_authid: str) -> bool:
        """Garante no ficheiro .pa.gpkg as três camadas de polígono vazias (sem as carregar no mapa)."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        r = f'__Limit_{self.dic_prj["dems"][0]["type"]}__'
        t = f'__Limit_{self.dic_prj["dems"][1]["type"]}__'
        i = self.intersection_name
        if self._gpkg_layer_valid(r) and self._gpkg_layer_valid(t) and self._gpkg_layer_valid(i):
            return True

        data_dir = self.dic_prj.get('path')
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
        crs = QgsCoordinateReferenceSystem(crs_authid)
        if not crs.isValid():
            crs = QgsCoordinateReferenceSystem('EPSG:4326')
        ctx = QgsCoordinateTransformContext()
        crs_s = crs.authid()

        if not self._gpkg_layer_valid(r):
            if not self._gpkg_has_any_vector_layer():
                opt = QgsVectorFileWriter.SaveVectorOptions()
                opt.driverName = 'GPKG'
                opt.layerName = r
                writer_ = QgsVectorFileWriter.create(
                    self.gpkg_path,
                    QgsFields(),
                    QgsWkbTypes.Polygon,
                    crs,
                    ctx,
                    opt)
                if writer_.hasError() != QgsVectorFileWriter.NoError:
                    return False
                del writer_
            else:
                mem = QgsVectorLayer(f'polygon?crs={crs_s}&index=yes', r, 'memory')
                mem.dataProvider().addAttributes(QgsFields())
                mem.updateFields()
                opt = QgsVectorFileWriter.SaveVectorOptions()
                opt.driverName = 'GPKG'
                opt.layerName = r
                opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
                QgsVectorFileWriter.writeAsVectorFormat(
                    layer=mem, fileName=self.gpkg_path, options=opt)
            normalize_project_pa_file(self.gpkg_path)

        if not self._gpkg_layer_valid(t):
            mem = QgsVectorLayer(f'polygon?crs={crs_s}&index=yes', t, 'memory')
            mem.dataProvider().addAttributes(QgsFields())
            mem.updateFields()
            opt = QgsVectorFileWriter.SaveVectorOptions()
            opt.driverName = 'GPKG'
            opt.layerName = t
            opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
            QgsVectorFileWriter.writeAsVectorFormat(
                layer=mem, fileName=self.gpkg_path, options=opt)
            normalize_project_pa_file(self.gpkg_path)

        if not self._gpkg_layer_valid(i):
            mem = QgsVectorLayer(f'polygon?crs={crs_s}&index=yes', i, 'memory')
            pr_ = mem.dataProvider()
            sch = QgsFields()
            sch.append(QgsField('fid', QVariant.Int))
            sch.append(QgsField('AREA', QVariant.Double))
            pr_.addAttributes(sch)
            mem.updateFields()
            opt = QgsVectorFileWriter.SaveVectorOptions()
            opt.driverName = 'GPKG'
            opt.layerName = i
            opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
            QgsVectorFileWriter.writeAsVectorFormat(
                layer=mem, fileName=self.gpkg_path, options=opt)
            normalize_project_pa_file(self.gpkg_path)

        return (
            self._gpkg_layer_valid(r)
            and self._gpkg_layer_valid(t)
            and self._gpkg_layer_valid(i))

    def _ensure_limit_layers_for_analysis(self) -> bool:
        """Só entra no QGIS o que já existe no .pa.gpkg: cria tabelas vazias no ficheiro e depois carrega com OGR."""
        if not self._ensure_limit_vector_tables_in_mdepa(self.crs_epsg):
            return False
        proj = QgsProject.instance()
        for key_ in (0, 1):
            name = f'__Limit_{self.dic_prj["dems"][key_]["type"]}__'
            if not proj.mapLayersByName(name):
                self.get_gpkg_layer(prefix_=name, gpkg_path=self.gpkg_path)
        iname = self.intersection_name
        if not proj.mapLayersByName(iname):
            self.get_gpkg_layer(prefix_=iname, gpkg_path=self.gpkg_path)
        return True

    def _clear_gpkg_vector_layer_features(self, layer_name: str) -> bool:
        """Esvazia feições da camada nomeada no .pa.gpkg (camada do mapa ou URI OGR)."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        lyr = None
        for L in QgsProject.instance().mapLayersByName(layer_name):
            if L.isValid():
                lyr = L
                break
        if lyr is None:
            uri = f'{self.gpkg_path}|layername={layer_name}'
            lyr = QgsVectorLayer(uri, layer_name, 'ogr')
        if lyr is None or not lyr.isValid():
            return False
        lyr.startEditing()
        ids = [f.id() for f in lyr.getFeatures()]
        if ids:
            lyr.deleteFeatures(ids)
        if not lyr.commitChanges():
            lyr.rollBack()
            return False
        lyr.updateExtents()
        lyr.triggerRepaint()
        return True

    def _clear_features_from_limit_layers(self):
        """Nova análise: esvazia limites e interseção no .pa.gpkg antes de gerar polígonos."""
        names = (
            f'__Limit_{self.dic_prj["dems"][0]["type"]}__',
            f'__Limit_{self.dic_prj["dems"][1]["type"]}__',
            self.intersection_name,
        )
        for nm in names:
            self._clear_gpkg_vector_layer_features(nm)

    def _morphology_gpkg_layer_names(self):
        type_0 = self.dic_prj['dems'][0]['type']
        type_1 = self.dic_prj['dems'][1]['type']
        return [f'__{m}_Z_{t}__' for m in self.list_morph for t in (type_0, type_1)]

    def _remove_project_layers_named(self, *layer_names: str) -> None:
        proj = QgsProject.instance()
        for nm in layer_names:
            for lyr in list(proj.mapLayersByName(nm)):
                try:
                    proj.removeMapLayer(lyr.id())
                except Exception:
                    pass

    def _pipeline_reset_timestamps_from_ordem(self, min_ordem: int) -> None:
        path = self.gpkg_path
        if not path or not os.path.isfile(path):
            return
        try:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    f'''UPDATE {PIPELINE_ETAPAS_TABLE}
                        SET inicio = NULL, fim = NULL
                        WHERE ordem >= ?''',
                    (min_ordem,),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    def _clear_pec_report_cache(self) -> None:
        self._pec_report_pec_intro = ''
        self._pec_report_plan_rows = []
        self._pec_report_alt_rows = []

    def _sanitize_pipeline_for_restart_immediate(self, restart) -> None:
        """Antes de retomar a cadeia: limpa camadas GPKG, rótulos do painel e variáveis obsoletas."""
        self._workflow_pause = None
        self._refresh_proc_button()
        self.layer_buffers = None
        mn = self.match_lines_layer_name
        bn = self.buffer_name
        morph_names = self._morphology_gpkg_layer_names()

        known = (None, 'morfologia_referencia', 'correspondencia_linhas', 'buffers')
        if restart not in known:
            self.log_message(
                self.tr(
                    'Tipo de retomada não reconhecido ({0}); aplicando limpeza completa.'
                ).format(repr(restart)),
                'WARNING',
            )
            restart = None

        if restart == 'buffers':
            self._clear_gpkg_vector_layer_features(bn)
            self._remove_project_layers_named(bn)
            self._clear_pec_report_cache()
            self._pipeline_reset_timestamps_from_ordem(6)
            self.log_message(
                self.tr(
                    'Buffers e PEC serão refeitos: camada de buffers limpa.'
                ),
                'INFO',
            )
            return

        if restart == 'correspondencia_linhas':
            self._clear_gpkg_vector_layer_features(mn)
            self._clear_gpkg_vector_layer_features(bn)
            self._remove_project_layers_named(mn, bn)
            self.dic_match = {}
            self._panel_stats_cache['ext_match'] = ''
            self._panel_stats_cache['pair_nr'] = ''
            self._refresh_extent_and_pairs_labels()
            self._persist_panel_stats_to_mdepa()
            self._clear_pec_report_cache()
            self._pipeline_reset_timestamps_from_ordem(5)
            self.log_message(
                self.tr(
                    'Correspondência e buffers serão refeitos: linhas de correspondência e buffers limpos; '
                    'pares e extensão da amostra repostos.'
                ),
                'INFO',
            )
            return

        if restart == 'morfologia_referencia':
            for nm in morph_names:
                self._clear_gpkg_vector_layer_features(nm)
            self._clear_gpkg_vector_layer_features(mn)
            self._clear_gpkg_vector_layer_features(bn)
            self._remove_project_layers_named(*morph_names, mn, bn)
            self.dic_match = {}
            for key_ in (0, 1):
                self.dic_prj['dems'][key_].pop('model', None)
            self._panel_stats_cache['ext_match'] = ''
            self._panel_stats_cache['pair_nr'] = ''
            self._refresh_extent_and_pairs_labels()
            self._persist_panel_stats_to_mdepa()
            self._clear_pec_report_cache()
            self._pipeline_reset_timestamps_from_ordem(3)
            self.log_message(
                self.tr(
                    'Morfologia e etapas seguintes serão refeitas: camadas de morfologia, '
                    'linhas de correspondência e buffers foram limpos; pares e extensão da amostra repostos.'
                ),
                'INFO',
            )
            return

        # restart is None — reprocessamento completo desde limites/interseção
        self._clear_features_from_limit_layers()
        for nm in morph_names:
            self._clear_gpkg_vector_layer_features(nm)
        self._clear_gpkg_vector_layer_features(mn)
        self._clear_gpkg_vector_layer_features(bn)
        self._remove_project_layers_named(*morph_names, mn, bn)
        self.dic_match = {}
        for key_ in (0, 1):
            self.dic_prj['dems'][key_]['geom_status'] = False
            self.dic_prj['dems'][key_].pop('model', None)
        self._reset_panel_stats_ui()
        self._persist_panel_stats_to_mdepa()
        self._clear_pec_report_cache()
        self._pipeline_reset_timestamps_from_ordem(1)
        self.log_message(
            self.tr(
                'Reprocessamento completo: limites, morfologia, correspondência e buffers '
                'foram limpos; estatísticas do painel repostas.'
            ),
            'INFO',
        )

    def check_prj_folder(self, project_file):
        """Atualiza rótulos conforme o ficheiro de projeto (.pa.gpkg) existe."""
        project_file = project_file or self.dic_prj.get('project_file')
        if not project_file:
            return
        if not self.dic_prj.get('path'):
            self.set_project_paths(project_file)
        data_dir = self.dic_prj['path']
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError as e:
            self.log_message(
                self.tr('Não foi possível criar pasta de dados: {0} ({1})').format(data_dir, e), 'ERROR')
            return

        if os.path.isfile(project_file):
            self.lb_status_proj.setText(self.tr('Projeto OK'))
            self.lb_status_proj.setStyleSheet('color: green;')
            self.dic_prj['status'] = 1
        else:
            self.lb_status_proj.setText(self.tr('Arquivo .pa.gpkg ausente'))
            self.lb_status_proj.setStyleSheet('color: red;')
            self.dic_prj['status'] = 0

    def log_message(self, message: str, level: str = "INFO"):
        """
        Appends a new log message with a timestamp and color coding.
        """
        timestamp = QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss")
        if message:
            log_entry = f"[{timestamp}] [{level}]\n  {message}\n"
        else:
            log_entry = f""

        # Determine the color based on the log level
        if level == "INFO":
            color = QColor("black")
        elif level == "WARNING":
            color = QColor("darkorange")
        elif level == "ERROR":
            color = QColor("red")
        else:
            color = QColor("gray")

        # Apply color to the text
        cursor = self.pte_log.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        cursor.insertText(log_entry)

        # Set format for the newly inserted text
        format = QTextCharFormat()
        format.setForeground(QBrush(color))
        cursor.movePosition(cursor.MoveOperation.StartOfLine, cursor.MoveMode.KeepAnchor)
        cursor.mergeCharFormat(format)

        # Scroll to the bottom automatically
        self.pte_log.verticalScrollBar().setValue(self.pte_log.verticalScrollBar().maximum())

        data_dir = self.dic_prj.get('path')
        if not data_dir:
            return
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError:
            return
        dic_st = self.dic_prj["standard"]
        log_path = os.path.join(data_dir, f'{dic_st["name"]}{dic_st["files"]["log"]}')
        with open(log_path, "a", encoding='utf-8', errors='replace') as file:
            file.write(log_entry)

    def log_dem_layer_info(self, key_: int):
        if self.dic_prj['dems'][key_]['obj_cbx']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()

            mss_ = self.tr('=======================================\n')
            mss_ += self.tr('  INFORMAÇÕES DO MDE — {0}\n').format(
                self.dic_prj['dems'][key_]['type'].upper())
            mss_ += self.tr('  Nome da camada: {0}\n').format(layer_.name())
            mss_ += self.tr('  Caminho da fonte: {0}\n').format(layer_.source())
            mss_ += self.tr('  Válida: {0}\n').format(layer_.isValid())
            mss_ += self.tr('  SRC: {0}\n').format(layer_.crs().authid())
            mss_ += self.tr('  Largura (px): {0}\n').format(layer_.width())
            mss_ += self.tr('  Altura (px): {0}\n').format(layer_.height())
            mss_ += self.tr('  Número de bandas: {0}\n').format(layer_.bandCount())
            mss_ += self.tr('  Extensão: {0}\n').format(layer_.extent().snappedToGrid(0.001))
            mss_ += self.tr('  Tamanho do pixel X: {0:.3f}\n').format(layer_.rasterUnitsPerPixelX())
            mss_ += self.tr('  Tamanho do pixel Y: {0:.3f}\n').format(layer_.rasterUnitsPerPixelY())
            mss_ += self.tr('=======================================\n')
            self.log_message(mss_, 'INFO')
        else:
            self.log_message(
                self.tr('MDE ({0}) NÃO DEFINIDO').format(self.dic_prj['dems'][key_]['type']),
                'ERROR')

    def task_done(self, key_):
        """ Called when a thread finishes processing, allowing another to start """
        self.threads_running -= 1  # Reduce active thread count
        if key_ in self.active_workers:
            del self.active_workers[key_]  # Remove from active workers

        # Start next task if there are pending tasks in the queue
        if not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    def start_task(self, key_, dic_):
        """ Start a worker task and track it """
        worker = Worker(key_, dic_, self)
        worker.finished.connect(self.task_done)  # Connect finished signal
        self.active_workers[key_] = worker
        worker.start()  # Start processing
        self.threads_running += 1

    def unload_cleanup(self):
        """Para workers, esvazia fila e fecha diálogos antes do unload / Plugin Reloader."""
        if getattr(self, 'settings_dlg', None):
            try:
                self.settings_dlg.reject()
            except Exception:
                try:
                    self.settings_dlg.close()
                except Exception:
                    pass
        for key_ in list(self.active_workers.keys()):
            worker = self.active_workers.pop(key_, None)
            if not worker:
                continue
            th = getattr(worker, 'process_thread', None)
            if th is not None:
                try:
                    th.sig_status.disconnect(self.update_bar)
                except TypeError:
                    try:
                        th.sig_status.disconnect()
                    except TypeError:
                        pass
            try:
                worker.finished.disconnect(self.task_done)
            except TypeError:
                try:
                    worker.finished.disconnect()
                except TypeError:
                    pass
            worker.stop()
        while True:
            try:
                self.task_queue.get_nowait()
            except Empty:
                break
        self.threads_running = 0

    def exec_analyze(self):
        if not self.dic_prj.get('project_file'):
            self.log_message(
                self.tr('Defina o projeto (.pa.gpkg): menu ⋯ → Abrir ou Novo.'), 'ERROR')
            return
        pf = self.dic_prj['project_file']
        if not os.path.isfile(pf):
            self.log_message(self.tr('O arquivo .pa.gpkg do projeto não existe.'), 'ERROR')
            return

        layer_ref = self.dic_prj['dems'][0]['obj_cbx'].currentLayer()
        layer_test = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
        if not isinstance(layer_ref, QgsRasterLayer) or not layer_ref.isValid():
            self.log_message(
                self.tr('Selecione o MDE de referência (DEM válido).'), 'ERROR')
            return
        if not isinstance(layer_test, QgsRasterLayer) or not layer_test.isValid():
            self.log_message(
                self.tr('Selecione o MDE de teste (DEM válido).'), 'ERROR')
            return

        self.crs_epsg = layer_ref.crs().authid()

        if self._workflow_pause == 'post_intersection':
            self.persist_project_config_from_widgets(log_values=False)
            self._workflow_pause = None
            self._refresh_proc_button()
            self.define_morphology(0)
            return
        if self._workflow_pause == 'post_pairs_review':
            self.persist_project_config_from_widgets(log_values=False)
            self._workflow_pause = None
            self._refresh_proc_button()
            self.define_buffers()
            return

        if self.threads_running > 0 or len(self.active_workers) > 0:
            self.persist_project_config_from_widgets(log_values=False)
            self.log_message(
                self.tr('Aguarde o fim da análise em curso antes de nova avaliação.'), 'WARNING')
            return

        # Comparar com o estado gravado **antes** de persistir no .pa.gpkg; senão não há baseline
        # e só o snapshot do PEC concluído faria retomada parcial (mudar só match parecia «do zero»).
        dlg = self.settings_dlg
        dlg.flush_widgets_to_dic_param(log_values=False)
        flat_now = self._flatten_run_snapshot()
        flat_was_pec = load_pipeline_last_ok_snapshot(pf)
        if flat_was_pec:
            flat_was = flat_was_pec
        else:
            flat_was = build_flat_snapshot_from_mdepa_stored_settings(pf)
            if not any(k.startswith('step_') for k in flat_was):
                flat_was = {}
        restart, _reason = compute_restart_etapa_from_snapshots(flat_now, flat_was)
        if restart == '__noop__':
            self.persist_project_config_from_widgets(log_values=False)
            self.log_message(
                self.tr(
                    'Parâmetros e MDEs inalterados (última avaliação concluída ou configuração gravada no projeto).'),
                'INFO',
            )
            return

        self.persist_project_config_from_widgets(log_values=False)

        self.create_gpkg()
        self._sanitize_pipeline_for_restart_immediate(restart)

        if restart is None:
            self.log_message(
                self.tr('Reprocessamento completo desde polígonos de limite e interseção.'), 'INFO')
            if self.cbx_workflow_study.currentIndex() == 2:
                if not self.apply_study_area_from_map_layer():
                    return
                return
            self.define_intersection()
        elif restart == 'morfologia_referencia':
            self.log_message(self.tr('Retomando a partir da morfologia (parâmetros alterados).'), 'INFO')
            self.define_morphology(0)
        elif restart == 'correspondencia_linhas':
            self.log_message(
                self.tr('Retomando a partir da correspondência de linhas (parâmetros alterados).'), 'INFO')
            self.matching_lines()
        elif restart == 'buffers':
            self.log_message(
                self.tr('Retomando a partir dos buffers (parâmetros alterados).'), 'INFO')
            if not self.dic_match:
                self.matching_lines()
            else:
                self.define_buffers()
        else:
            self.define_intersection()

    def run_polygon_intersection(self):
        status_0 = self.dic_prj['dems'][0]['geom_status']
        status_1 = self.dic_prj['dems'][1]['geom_status']
        if status_0 and status_1:
            mss_ = self.tr('CALCULANDO ÁREA DE INTERSEÇÃO DOS MDEs')
            self.log_message(mss_, 'INFO')

            layer_0 = self.get_gpkg_layer(prefix_= f'__Limit_{self.dic_prj["dems"][0]["type"]}__')
            layer_1 = self.get_gpkg_layer(prefix_= f'__Limit_{self.dic_prj["dems"][1]["type"]}__')
            layer_i = self.get_gpkg_layer(prefix_= self.intersection_name)

            layer_i.startEditing()
            ids_i = [f.id() for f in layer_i.getFeatures()]
            if ids_i:
                layer_i.deleteFeatures(ids_i)
            layer_i.commitChanges()

            sum_area_m2 = 0.0
            crs_i = layer_i.crs()

            for feat_0 in layer_0.getFeatures():
                geom_0 = feat_0.geometry()
                for feat_1 in layer_1.getFeatures():
                    geom_1 = feat_1.geometry()
                    intersec_ = geom_0.intersection(geom_1)
                    area_m2 = geometry_area_square_meters(intersec_, crs_i)
                    sum_area_m2 += area_m2
                    feat_i = QgsFeature()
                    feat_i.setGeometry(intersec_)
                    count = layer_i.featureCount()
                    # Campo AREA: sempre metros quadrados (etiquetas no mapa: ver estilo __Limit_Intersecao__)
                    feat_i.setAttributes([count + 1, area_m2])
                    layer_i.startEditing()
                    layer_i.addFeature(feat_i)
                    layer_i.commitChanges()
                    layer_i.updateExtents()
                    layer_i.triggerRepaint()
            sum_area_km2 = sum_area_m2 / 1_000_000.0
            area_disp = round(sum_area_km2, 4)
            self.lb_area.setText(self.dic_lb_texts['area'].format(area_disp))
            ext_m = 2.0176 * (sum_area_m2 ** 0.5478)
            ext_km = round(ext_m / 1000.0, 1)
            self.lb_ext_min.setText(self.dic_lb_texts['ext_min'].format(ext_km))
            self._panel_stats_cache['area'] = str(area_disp)
            self._panel_stats_cache['ext_min'] = str(ext_km)
            self._persist_panel_stats_to_mdepa()
            mss_ = self.tr('ÁREA DE INTERSEÇÃO DOS MDEs DEFINIDA\n')
            mss_ += self.tr('=======================================\n')
            self.log_message(mss_, 'INFO')
            if self.cbx_workflow_study.currentIndex() == 1:
                self._workflow_pause = 'post_intersection'
                self._refresh_proc_button()
                self.log_message(
                    self.tr(
                        'Edite a camada de interseção se necessário e prima Continuar para morfologia.'),
                    'INFO')
            else:
                self.define_morphology(0)

    def ensure_pipeline_etapas_table(self, gpkg_path=None):
        """Cria tabela de etapas e insere linhas previstas (idempotente). Sem SpatiaLite — só SQLite."""
        path = gpkg_path or self.gpkg_path
        if not path or not os.path.isfile(path):
            return False
        try:
            conn = sqlite3.connect(path)
            try:
                # inicio/fim: data e hora local armazenada como TEXT no formato PIPELINE_DATETIME_FMT
                conn.execute(
                    f'''CREATE TABLE IF NOT EXISTS {PIPELINE_ETAPAS_TABLE} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ordem INTEGER NOT NULL UNIQUE,
                        etapa TEXT NOT NULL UNIQUE,
                        inicio TEXT DEFAULT NULL,
                        fim TEXT DEFAULT NULL
                    )'''
                )
                _ensure_pa_settings_table_conn(conn)
                for old_n, new_n in (
                    ('raster_intersecao', 'dem_intersecao'),
                    ('matching_linhas', 'correspondencia_linhas'),
                ):
                    row_new = conn.execute(
                        f'SELECT 1 FROM {PIPELINE_ETAPAS_TABLE} WHERE etapa = ?', (new_n,)
                    ).fetchone()
                    if row_new:
                        conn.execute(
                            f'DELETE FROM {PIPELINE_ETAPAS_TABLE} WHERE etapa = ?', (old_n,))
                    else:
                        conn.execute(
                            f'UPDATE {PIPELINE_ETAPAS_TABLE} SET etapa = ? WHERE etapa = ?',
                            (new_n, old_n),
                        )
                for ordem, etapa in PIPELINE_ETAPAS_DEF:
                    conn.execute(
                        f'''INSERT OR IGNORE INTO {PIPELINE_ETAPAS_TABLE}
                            (ordem, etapa, inicio, fim) VALUES (?, ?, NULL, NULL)''',
                        (ordem, etapa),
                    )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            return False
        return True

    def pipeline_set_etapa_inicio(self, etapa: str, gpkg_path=None, quando=None):
        """Grava data/hora de início da etapa (padrão: agora local)."""
        path = gpkg_path or self.gpkg_path
        if not path or not os.path.isfile(path):
            return False
        ts = quando if quando is not None else pipeline_datetime_now_local()
        try:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    f'UPDATE {PIPELINE_ETAPAS_TABLE} SET inicio = ? WHERE etapa = ?',
                    (ts, etapa),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            return False
        return True

    def pipeline_set_etapa_fim(self, etapa: str, gpkg_path=None, quando=None):
        """Grava data/hora de fim da etapa (padrão: agora local)."""
        path = gpkg_path or self.gpkg_path
        if not path or not os.path.isfile(path):
            return False
        ts = quando if quando is not None else pipeline_datetime_now_local()
        try:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    f'UPDATE {PIPELINE_ETAPAS_TABLE} SET fim = ? WHERE etapa = ?',
                    (ts, etapa),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            return False
        return True

    def create_gpkg(self):
        """Garante ficheiro de projeto, tabelas auxiliares, tabelas de limite e carrega-as no mapa a partir do GPKG."""
        if not self.gpkg_path:
            self.log_message(self.tr('Caminho do projeto (.pa.gpkg) indefinido.'), 'ERROR')
            return
        data_dir = self.dic_prj.get('path')
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
        if not os.path.isfile(self.gpkg_path):
            self.log_message(self.tr('O arquivo .pa.gpkg do projeto não existe.'), 'ERROR')
            return
        if not self.ensure_pipeline_etapas_table():
            self.log_message(self.tr('Falha ao garantir tabelas auxiliares no .pa.gpkg.'), 'ERROR')
            return
        if not self._ensure_limit_layers_for_analysis():
            self.log_message(self.tr('Falha ao preparar camadas de limite no projeto.'), 'ERROR')

    def gpkg_conn(self, gpkg_path_=''):
        if not gpkg_path_:
            gpkg_path_ = self.gpkg_path
        conn_ = sqlite3.connect(gpkg_path_)  # , isolation_level=None)
        conn_.row_factory = sqlite3.Row
        conn_.enable_load_extension(True)
        conn_.load_extension('mod_spatialite')
        conn_.execute('SELECT load_extension("mod_spatialite")')
        conn_.execute('pragma journal_mode=wal')
        # cur_ = conn_.cursor()
        return conn_

    def gpkg_close_conn(self, conn_=None, cur_=None):
        print('close conn')
        if conn_:
            conn_.close()
        if cur_:
            cur_.close()

    def get_gpkg_layer(self, prefix_='', gpkg_path='', show=True):
        print('get_gpkg_layer', prefix_)
        if not gpkg_path:
            gpkg_path = self.gpkg_path
        self.node_group = self._plugin_layer_tree_group()

        conn = None
        layer_ = None
        if prefix_:
            layer_ = QgsProject.instance().mapLayersByName(prefix_)
            if layer_:
                print(f'Layer {prefix_} já carregada')
                if isinstance(layer_, list):
                    layer_ = layer_[0]
                return layer_

            conn = self.gpkg_conn(gpkg_path)
            uri_ = f'{gpkg_path}|layername={prefix_}'

            layer_ = QgsVectorLayer(uri_, prefix_, 'ogr')
            conn.commit()
            style_path = os.path.join(plugin_path, r'styles', f'{prefix_}.qml')
            layer_.loadNamedStyle(style_path)
            layer_.triggerRepaint()
            if show:
                QgsProject.instance().addMapLayer(layer_, False)
                layer_node = QgsLayerTreeLayer(layer_)
                self.node_group.insertChildNode(0, layer_node)
        self.gpkg_close_conn(conn)

        return layer_

    def define_intersection(self):
        for key_ in self.dic_prj['dems']:
            self.dic_prj['dems'][key_]['geom_status'] = False
        self._clear_features_from_limit_layers()
        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('DEFININDO POLÍGONOS')
        self.log_message(mss_, 'INFO')
        for key_ in self.dic_prj['dems']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()
            dic_ = {
                'file_path': layer_.source(),
                'step': 'polygon',
                'srid_ref': self.crs_epsg,
                'srid': layer_.crs().authid(),
                'gpkg':self.gpkg_path,
                'layer':  f'__Limit_{self.dic_prj["dems"][0]["type"]}__',
                'parent': self,
                'main': self.main
            }

            # Add tasks to queue
            self.task_queue.put((key_, dic_))

        # Start up to max_threads tasks
        while self.threads_running < self.max_threads and not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    def define_morphology(self, key_=0):
        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('DEFININDO ELEMENTOS DE MORFOLOGIA DO TERRENO - {0}').format(
            self.dic_prj['dems'][key_]['type'])
        self.log_message(mss_, 'INFO')
        layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()
        gsd_ = layer_.rasterUnitsPerPixelX()
        dic_param_morphology = self.settings_dlg.dic_param['step_morfologia']['fields']
        max_px = int(float(dic_param_morphology['max_basin_area']['value']) / (gsd_ ** 2))
        dic_ = {
            'file_path': layer_.source(),
            'step': 'morphology',
            'srid_ref': self.crs_epsg,
            'srid': layer_.crs().authid(),
            'gpkg':self.gpkg_path,
            'layer':  self.get_gpkg_layer(prefix_= self.intersection_name).source(),
            'max_px': max_px,
            'max_memo': float(dic_param_morphology['max_memo_grass']['value']),
            'morph_names':self.list_morph,
            'gsd': gsd_,
            'parent': self,
            'main': self.main
        }

        # Add tasks to queue
        self.task_queue.put((key_, dic_))

        # Start up to max_threads tasks
        while self.threads_running < self.max_threads and not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    @staticmethod
    def _line_midpoint_xy(geom: QgsGeometry):
        """Ponto ao longo da linha a meio do comprimento (2D)."""
        if geom is None or geom.isEmpty() or geom.type() != QgsWkbTypes.LineGeometry:
            return None
        L = geom.length()
        if L <= 0:
            return None
        g2 = QgsGeometry(geom)
        mid = g2.interpolate(L / 2.0)
        if mid is None or mid.isEmpty():
            return None
        return mid.asPoint()

    def _sync_match_lines_layer_from_dic_match(self) -> bool:
        """Grava no GPKG linhas teste→referência (meio a meio) e metadados dos pares."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        if not self.dic_match:
            return False
        crs = QgsCoordinateReferenceSystem(self.crs_epsg or 'EPSG:4326')
        if not crs.isValid():
            crs = QgsCoordinateReferenceSystem('EPSG:4326')
        crs_s = crs.authid()
        type_r = self.dic_prj['dems'][0]['type']
        type_t = self.dic_prj['dems'][1]['type']
        mem = QgsVectorLayer(
            f'LineString?crs={crs_s}&index=yes', self.match_lines_layer_name, 'memory')
        sch = QgsFields()
        sch.append(QgsField('tipo', QVariant.String, len=64))
        sch.append(QgsField('fid_r', QVariant.Int))
        sch.append(QgsField('fid_t', QVariant.Int))
        sch.append(QgsField('dist_m', QVariant.Double))
        sch.append(QgsField('per_r', QVariant.Double))
        sch.append(QgsField('len_r', QVariant.Double))
        pr_ = mem.dataProvider()
        pr_.addAttributes(sch)
        mem.updateFields()
        mem.startEditing()
        n_ok = 0
        for tag_ in self.dic_match:
            layer_r = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_r}__')
            layer_t = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_t}__')
            if layer_r is None or layer_t is None or not layer_r.isValid() or not layer_t.isValid():
                self.log_message(
                    self.tr('[__Linhas_de_Correspondencia__] Camadas de morfologia indisponíveis para tipo {0}.').format(
                        tag_), 'WARNING')
                continue
            for vet_ in self.dic_match[tag_]:
                if len(vet_) < 5:
                    continue
                try:
                    fid_r = int(vet_[0])
                    fid_t = int(vet_[1])
                except (TypeError, ValueError):
                    continue
                fr = layer_r.getFeature(fid_r)
                ft = layer_t.getFeature(fid_t)
                if not fr.hasGeometry() or not ft.hasGeometry():
                    continue
                pt_t = self._line_midpoint_xy(QgsGeometry(ft.geometry()))
                pt_r = self._line_midpoint_xy(QgsGeometry(fr.geometry()))
                if pt_t is None or pt_r is None:
                    continue
                line_g = QgsGeometry.fromPolylineXY([pt_t, pt_r])
                if line_g.isEmpty():
                    continue
                feat = QgsFeature(mem.fields())
                feat.setGeometry(line_g)
                try:
                    dist_m = float(vet_[2])
                    per_r = float(vet_[3])
                    len_r = float(vet_[4])
                except (TypeError, ValueError, IndexError):
                    dist_m, per_r, len_r = 0.0, 0.0, fr.geometry().length()
                feat.setAttributes([str(tag_), fid_r, fid_t, dist_m, per_r, len_r])
                mem.addFeature(feat)
                n_ok += 1
        mem.commitChanges()
        if n_ok == 0:
            self.log_message(
                self.tr('[__Linhas_de_Correspondencia__] Nenhuma linha de ligação foi criada.'), 'WARNING')
            return False
        opt = QgsVectorFileWriter.SaveVectorOptions()
        opt.driverName = 'GPKG'
        opt.layerName = self.match_lines_layer_name
        opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        try:
            QgsVectorFileWriter.writeAsVectorFormat(
                layer=mem,
                fileName=self.gpkg_path,
                options=opt,
            )
        except Exception as e:
            self.log_message(
                self.tr('[__Linhas_de_Correspondencia__] Falha ao gravar no GPKG: {0}').format(e), 'ERROR')
            return False
        normalize_project_pa_file(self.gpkg_path)
        proj = QgsProject.instance()
        for lyr in list(proj.mapLayersByName(self.match_lines_layer_name)):
            try:
                proj.removeMapLayer(lyr.id())
            except Exception:
                pass
        self.get_gpkg_layer(prefix_=self.match_lines_layer_name, gpkg_path=self.gpkg_path)
        self.log_message(
            self.tr('[__Linhas_de_Correspondencia__] {0} ligações gravadas (edite antes de Continuar se estiver em revisão).').format(
                n_ok), 'INFO')
        return True

    def _dic_match_from_match_lines_layer(self):
        """Reconstrói dic_match a partir da camada editável de correspondência (GPKG)."""
        lyr = self._resolve_limit_layer_for_editing(self.match_lines_layer_name)
        if lyr is None or not lyr.isValid():
            return None
        if lyr.featureCount() == 0:
            return {}
        type_r = self.dic_prj['dems'][0]['type']
        type_t = self.dic_prj['dems'][1]['type']
        out = {}
        n_skip = 0
        for feat in lyr.getFeatures():
            if not feat.hasGeometry():
                n_skip += 1
                continue
            tipo = feat.attribute('tipo')
            if tipo is None or str(tipo).strip() == '':
                n_skip += 1
                continue
            tag_ = str(tipo).strip()
            try:
                fid_r = int(feat.attribute('fid_r'))
                fid_t = int(feat.attribute('fid_t'))
            except (TypeError, ValueError):
                n_skip += 1
                continue
            layer_r = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_r}__')
            layer_t = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_t}__')
            if layer_r is None or layer_t is None or not layer_r.isValid() or not layer_t.isValid():
                n_skip += 1
                continue
            fr = layer_r.getFeature(fid_r)
            ft = layer_t.getFeature(fid_t)
            if not fr.hasGeometry() or not ft.hasGeometry():
                n_skip += 1
                continue
            gr = QgsGeometry(fr.geometry())
            gt = QgsGeometry(ft.geometry())
            len_r = feat.attribute('len_r')
            try:
                len_r = float(len_r)
            except (TypeError, ValueError):
                len_r = gr.length()
            dist_m = feat.attribute('dist_m')
            try:
                dist_m = float(dist_m)
            except (TypeError, ValueError):
                dist_m = round(gr.distance(gt), 2)
            per_r = feat.attribute('per_r')
            try:
                per_r = float(per_r)
            except (TypeError, ValueError):
                per_r = 0.0
            row = [fid_r, fid_t, round(dist_m, 2), round(per_r, 2), float(len_r)]
            out.setdefault(tag_, []).append(row)
        if n_skip:
            self.log_message(
                self.tr('[__Linhas_de_Correspondencia__] {0} feição(ões) ignoradas.').format(n_skip), 'WARNING')
        return out

    @staticmethod
    def _float_from_panel_str(val):
        try:
            if val is None or str(val).strip() == '':
                return None
            return float(val)
        except (TypeError, ValueError):
            return None

    def _total_sample_extent_m_from_dic_match(self, dm=None) -> float:
        """Soma dos comprimentos da linha de referência por par (último valor da linha = LEN em m do CRS)."""
        dm = dm if dm is not None else getattr(self, 'dic_match', None) or {}
        s = 0.0
        for key_ in dm:
            for vet_ in dm[key_]:
                if not vet_:
                    continue
                try:
                    s += float(vet_[-1])
                except (TypeError, ValueError, IndexError):
                    continue
        return s

    def _resolve_ext_min_km_sample_gate(self):
        """Extensão mínima (km) para o gate antes dos buffers.

        Prioriza o valor gravado no .pa.gpkg (`panel_stats.ext_min`), igual ao que o painel
        mostra após abrir o projeto; o cache em memória só entra se o ficheiro ainda não
        tiver valor válido (ex.: interseção acabada de calcular na mesma sessão).
        Devolve None se não houver limite (fluxos antigos, sem interseção, ou ≤ 0).
        """
        pf = self.dic_prj.get('project_file')
        disk_raw = ''
        disk_val = None
        if pf and os.path.isfile(pf):
            data = load_panel_stats_from_mdepa_path(pf)
            disk_raw = (data.get('ext_min') or '').strip()
            disk_val = self._float_from_panel_str(disk_raw)
        cache_raw = (self._panel_stats_cache.get('ext_min') or '').strip()
        cache_val = self._float_from_panel_str(cache_raw)

        if disk_val is not None and disk_val > 0:
            if cache_raw != disk_raw:
                self._panel_stats_cache['ext_min'] = disk_raw
                self.lb_ext_min.setText(
                    self.dic_lb_texts['ext_min'].format(disk_raw or '—'))
            return disk_val
        if cache_val is not None and cache_val > 0:
            return cache_val
        return None

    def _check_sample_extent_vs_minimum(self) -> bool:
        """False interrompe antes dos buffers: extensão acumulada da amostra < extensão mínima recomendada."""
        ext_min_km = self._resolve_ext_min_km_sample_gate()
        if ext_min_km is None:
            return True
        dm = getattr(self, 'dic_match', None) or {}
        n_pairs = sum(len(dm[k]) for k in dm)
        ext_m = self._total_sample_extent_m_from_dic_match(dm)
        ext_km = ext_m / 1000.0
        if n_pairs == 0:
            self.log_message(
                self.tr(
                    'Não há pares homólogos válidos. O processamento foi interrompido antes dos buffers.\n\n'
                    'Sugestões:\n'
                    '• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas.\n'
                    '• Afrouxar a correspondência: aumentar a distância máxima entre centróides (pixels do MDE de teste) '
                    'e o percentual de diferença de área entre os envelopes mínimos.'),
                'ERROR',
            )
            return False
        if ext_km < ext_min_km:
            self.log_message(
                self.tr(
                    'A extensão total da amostra ({0} km) é menor que a extensão mínima recomendada ({1} km). '
                    'O processamento foi interrompido antes dos buffers.\n\n'
                    'Sugestões:\n'
                    '• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas e maior extensão acumulada.\n'
                    '• Afrouxar a correspondência: aumentar a distância máxima entre centróides (pixels do MDE de teste) '
                    'e o percentual de diferença de área entre os envelopes mínimos.'
                ).format(round(ext_km, 4), round(ext_min_km, 4)),
                'ERROR',
            )
            return False
        return True

    def matching_lines(self):
        print('matching_lines')
        conn = self.gpkg_conn()
        curs = conn.cursor()
        dic_param_match = self.settings_dlg.dic_param['step_match']['fields']
        dist_max_px = float(dic_param_match['dist_max']['value'])
        layer_test = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
        if not isinstance(layer_test, QgsRasterLayer) or not layer_test.isValid():
            self.log_message(
                self.tr('MDE de teste inválido — não é possível aplicar a distância máxima em pixels.'), 'ERROR')
            return
        gsd_test = layer_test.rasterUnitsPerPixelX()
        if not gsd_test or gsd_test <= 0:
            self.log_message(
                self.tr('GSD do MDE de teste inválido — não é possível converter pixels em distância no mapa.'), 'ERROR')
            return
        dist_max = dist_max_px * gsd_test
        area_percent = float(dic_param_match['percent_area']['value']) / 100
        type_0 = self.dic_prj["dems"][0]["type"]
        type_1 = self.dic_prj["dems"][1]["type"]
        morph_0 = self.list_morph[0]
        morph_1 = self.list_morph[1]
        sql_ = f"""
        WITH
            ct as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)) len from __{morph_0}_Z_{type_1}__),
            cr as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)) len  from __{morph_0}_Z_{type_0}__),
            ht as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)) len  from __{morph_1}_Z_{type_1}__),
            hr as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)
            ) len  from __{morph_1}_Z_{type_0}__)
        SELECT 
            '{morph_0}' TIPO, 
            cr.fid fidr, 
            ct.fid fidt, 
            ROUND(ST_DISTANCE(ct.centroid, cr.centroid),2) as DIST,  
            ROUND(ABS(ST_AREA(ct.eogeom) - ST_AREA(cr.eogeom))/ ST_AREA(ct.eogeom),2) PER, 
            cr.len LEN
            FROM ct, cr
            WHERE 
                ST_DISTANCE(ct.centroid, cr.centroid) < {dist_max}
                AND (ABS(ST_AREA(ct.eogeom) - ST_AREA(cr.eogeom))/ ST_AREA(ct.eogeom)) < {area_percent}
        UNION
        SELECT 
            '{morph_1}' TIPO, 
            hr.fid fidr, 
            ht.fid fidt, 
            ROUND(ST_DISTANCE(ht.centroid, hr.centroid),2) as DIST, 
            ROUND(ABS(ST_AREA(ht.eogeom) - ST_AREA(hr.eogeom))/ ST_AREA(ht.eogeom),2) PER, 
            hr.len LEN
            FROM ht, hr
            WHERE 
                ST_DISTANCE(ht.centroid, hr.centroid) < {dist_max}
                AND (ABS(ST_AREA(ht.eogeom) - ST_AREA(hr.eogeom))/ ST_AREA(ht.eogeom)) < {area_percent}
            ORDER BY 1,4 ASC;
        """
        result_ = curs.execute(sql_)
        result_fa = result_.fetchall()
        try:
            curs.close()
            conn.close()
        except Exception:
            pass
        self.dic_match = {}
        for row_ in result_fa:
            for j, col_ in enumerate(row_):
                if j == 0:
                    tag_ = col_
                    if tag_ not in self.dic_match:
                        self.dic_match[tag_] = [[]]
                        k = 0
                    else:
                        self.dic_match[tag_].append([])
                        k += 1
                else:
                    self.dic_match[tag_][k].append(col_)

            #     print(col_,end='\t')
            # print()
        ext_sum = 0
        for key_ in self.dic_match:
            print(f'------{key_}')
            for vet_ in self.dic_match[key_]:
                # print(vet_)
                ext_sum += vet_[-1]
        print('Extensão Total da Amostra:', ext_sum)
        n_pairs = sum(len(self.dic_match[k]) for k in self.dic_match)
        self._panel_stats_cache['ext_match'] = str(round(ext_sum, 1))
        self._panel_stats_cache['pair_nr'] = str(n_pairs)
        self._refresh_extent_and_pairs_labels()
        self._persist_panel_stats_to_mdepa()
        self._sync_match_lines_layer_from_dic_match()
        # print('dic_match', self.dic_match)
        if self.cbx_workflow_pairs.currentIndex() == 1:
            self._workflow_pause = 'post_pairs_review'
            self._refresh_proc_button()
            self.log_message(
                self.tr(
                    'Camada __Linhas_de_Correspondencia__: {0} pares. Edite, remova ou adicione linhas '
                    '(meio teste → meio referência); atributos: tipo, fid_r, fid_t. Prima Continuar.'
                ).format(n_pairs),
                'INFO')
            return
        self.define_buffers()

    def create_buffers_layer(self):

        layer_0 = QgsVectorLayer(f'multipolygon?crs={self.crs_epsg}&index=yes', self.buffer_name, "memory")
        schema_ = QgsFields()
        schema_.append(QgsField('scale', QVariant.Int))
        schema_.append(QgsField('class', QVariant.String))
        schema_.append(QgsField('id_origem', QVariant.Int))
        schema_.append(QgsField('camada_origem', QVariant.String))
        pr_ = layer_0.dataProvider()
        pr_.addAttributes(schema_)
        layer_0.updateFields()

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        options.layerName = self.buffer_name
        QgsVectorFileWriter.writeAsVectorFormat(
            layer=layer_0,
            fileName=self.gpkg_path,
            options=options)
        normalize_project_pa_file(self.gpkg_path)
        layer_ = self.get_gpkg_layer(prefix_=self.buffer_name, gpkg_path=self.gpkg_path)
        return (layer_)

    def _log_buffer_geom_diag_once(self, message: str):
        if not hasattr(self, '_buffer_geom_diag_counts'):
            self._buffer_geom_diag_counts = {}
        n = self._buffer_geom_diag_counts.get(message, 0) + 1
        self._buffer_geom_diag_counts[message] = n
        if n == 1:
            self.log_message(
                self.tr('[__Buffers__] {0}').format(message), 'WARNING')

    def _geometry_for_buffers_layer(self, geom: QgsGeometry, layer: QgsVectorLayer):
        """Alinha geometria ao tipo da camada (2D MultiPolygon típico) antes de addFeature."""
        if geom is None or geom.isNull() or geom.isEmpty():
            self._log_buffer_geom_diag_once(
                self.tr('Geometria vazia ou nula — feição ignorada.'))
            return None
        g = QgsGeometry(geom)
        tgt = layer.wkbType()
        want_z = QgsWkbTypes.hasZ(tgt)
        want_m = QgsWkbTypes.hasM(tgt)
        want_multi = QgsWkbTypes.isMultiType(tgt)
        # Linhas 3D → buffers PolygonZ; camada __Buffers__ é multpolygon 2D no GPKG
        if not want_z and hasattr(g, 'dropZValue'):
            try:
                g.dropZValue()
            except Exception:
                pass
        if not want_m and hasattr(g, 'dropMValue'):
            try:
                g.dropMValue()
            except Exception:
                pass
        if g.isEmpty():
            self._log_buffer_geom_diag_once(
                self.tr('Geometria vazia após remover Z/M — feição ignorada.'))
            return None
        if g.type() != QgsWkbTypes.PolygonGeometry:
            g_mv = g.makeValid()
            if not g_mv.isEmpty() and g_mv.type() == QgsWkbTypes.PolygonGeometry:
                g = g_mv
            else:
                self._log_buffer_geom_diag_once(
                    self.tr(
                        'Geometria não poligonal ({0}); makeValid não produziu polígono — ignorada.'
                    ).format(QgsWkbTypes.displayString(geom.wkbType())))
                return None
        if want_multi and not QgsWkbTypes.isMultiType(g.wkbType()):
            # collectGeometries só em QGIS recente; fromMultiPolygonXY é estável em 3.x
            cg = getattr(QgsGeometry, 'collectGeometries', None)
            if cg is not None:
                g_col = cg([g])
                if g_col is None or g_col.isEmpty():
                    self._log_buffer_geom_diag_once(
                        self.tr('Falha ao promover polígono simples a MultiPolygon — ignorada.'))
                    return None
                g = g_col
            else:
                poly_xy = g.asPolygon()
                if not poly_xy or not poly_xy[0]:
                    self._log_buffer_geom_diag_once(
                        self.tr('Polígono sem anéis — não foi possível formar MultiPolygon.'))
                    return None
                g = QgsGeometry.fromMultiPolygonXY([poly_xy])
                if g.isEmpty():
                    self._log_buffer_geom_diag_once(
                        self.tr('Falha ao promover polígono simples a MultiPolygon — ignorada.'))
                    return None
        if not g.isGeosValid():
            g = g.makeValid()
            if g.isEmpty() or g.type() != QgsWkbTypes.PolygonGeometry:
                self._log_buffer_geom_diag_once(
                    self.tr('Geometria inválida após makeValid — ignorada.'))
                return None
        if not want_z and QgsWkbTypes.hasZ(g.wkbType()) and hasattr(g, 'dropZValue'):
            try:
                g.dropZValue()
            except Exception:
                pass
        if g.isEmpty():
            return None
        return g

    def get_list_scale(self):
        def get_gsd():
            layer_ = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
            if layer_:
                return layer_.rasterUnitsPerPixelX()
            return
        max_scale_from_set = self.settings_dlg.dic_param['step_buffers']['fields']['max_scale']['value']
        min_scale_from_set = self.settings_dlg.dic_param['step_buffers']['fields']['min_scale']['value']
        print('max_scale_from_set', max_scale_from_set, 'min_scale_from_set', min_scale_from_set)

        if  max_scale_from_set < len(self.dic_pec_v):
            print(f'getting max_scale_from_set: {max_scale_from_set}')
            max_scale = max_scale_from_set
        else:
            gsd_ = get_gsd()
            for i, scale_ in enumerate(self.dic_pec_v):
                if self.dic_pec_mm['H']['A']['pec'] * scale_ > gsd_/2:
                    max_scale = i - 1
                    print('else max', list(self.dic_pec_v)[max_scale])
                    break
        if min_scale_from_set < len(self.dic_pec_v):
            print(f'getting min_scale_from_set: {min_scale_from_set}')
            min_scale = min_scale_from_set
        else:
            if not gsd_:
                gsd_ = get_gsd()
            for i, scale_ in enumerate(self.dic_pec_v):
                if self.dic_pec_mm['H']['A']['pec'] * scale_ > gsd_ * 2:
                    min_scale = i
                    print('else min',list(self.dic_pec_v)[min_scale])
                    break
        print('min_scale', min_scale, 'max_scale', max_scale)
        max_scale_idx = max(min(max_scale, min_scale), 0)
        min_scale_idx = min(max(max_scale, min_scale), len(self.dic_pec_v) -1)
        print('max_scale_idx', max_scale_idx, 'min_scale_idx', min_scale_idx)
        list_ = []
        for i in range (max_scale_idx, min_scale_idx + 1):
            list_.append(list(self.dic_pec_v)[i])
        return list_

    def define_buffers(self):
        self._buffer_geom_diag_counts = {}
        self._buffers_layer_target_logged = False
        rebuilt = self._dic_match_from_match_lines_layer()
        if rebuilt is not None:
            if not rebuilt:
                self.log_message(
                    self.tr(
                        'Define buffers: a camada __Linhas_de_Correspondencia__ está vazia ou sem pares válidos.'),
                    'ERROR')
                return
            self.dic_match = rebuilt
            ext_sum = 0
            for key_ in self.dic_match:
                for vet_ in self.dic_match[key_]:
                    if vet_:
                        ext_sum += float(vet_[-1])
            self._panel_stats_cache['ext_match'] = str(round(ext_sum, 1))
            self._panel_stats_cache['pair_nr'] = str(
                sum(len(self.dic_match[k]) for k in self.dic_match))
            self._refresh_extent_and_pairs_labels()
            self._persist_panel_stats_to_mdepa()
        if not self._check_sample_extent_vs_minimum():
            if self.cbx_workflow_pairs.currentIndex() == 1:
                self._workflow_pause = 'post_pairs_review'
                self._refresh_proc_button()
            return
        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('DEFININDO BUFFERS')
        self.log_message(mss_, 'INFO')
        list_scale = self.get_list_scale()
        dic_layers_line = {}
        for tag_ in self.dic_match:
            dic_layers_line[tag_] = {}
            for i in [0, 1]:
                type_ = self.dic_prj["dems"][i]["type"]
                layer_name = f'__{tag_}_Z_{type_}__'
                layer_ = self.get_gpkg_layer(layer_name)
                dic_layers_line[tag_].update({i: layer_})
        # list_layers_buffer = self.create_buffers_layer()
        norm_type = self.settings_dlg.dic_param['step_normalize_prog']['fields']['norm_type']['value']
        dic_={
            'step': 'buffers',
            'dic_layers_line': dic_layers_line,
            'list_scale': list_scale,
            'dic_match': self.dic_match,
            'dic_pec_mm': self.dic_pec_mm,
            'dic_pec_v': self.dic_pec_v,
            'norm_type': norm_type,
            'parent': self,
            'main': self.main}
        # Add tasks to queue
        key_ = 3
        self.task_queue.put((key_, dic_))

        # Start up to max_threads tasks
        while self.threads_running < self.max_threads and not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    def update_dic_vectors(self, dic_values):
        dic_vectors = {}
        for scale_ in dic_values:
            dic_vectors[scale_] = {}
            for class_ in dic_values[scale_]:
                dic_vectors[scale_][class_] = {'H': [], 'V': []}
                for count_ in dic_values[scale_][class_]:
                    if not dic_values[scale_][class_][count_].get('outlier', False):
                        dh = dic_values[scale_][class_][count_]['dm_h']
                        dv = dic_values[scale_][class_][count_]['dm_v']
                        if isinstance(dh, (int, float)) and math.isfinite(dh):
                            dic_vectors[scale_][class_]['H'].append(dh)
                        if isinstance(dv, (int, float)) and math.isfinite(dv):
                            dic_vectors[scale_][class_]['V'].append(dv)
        return dic_vectors

    def check_outliers(self, dic_values):
        dic_stats = self.update_dic_vectors(dic_values)
        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                h_vals = [
                    x for x in dic_stats[scale_][class_]['H']
                    if isinstance(x, (int, float)) and math.isfinite(x)]
                if len(h_vals) < 2:
                    for count_ in dic_values[scale_][class_]:
                        v_ = dic_values[scale_][class_][count_]['dm_h']
                        dic_values[scale_][class_][count_]['outlier'] = (
                            not isinstance(v_, (int, float)) or not math.isfinite(v_))
                    continue
                quant_ = statistics.quantiles(data=h_vals)
                iqr_ = quant_[2] - quant_[0]
                ls_ = quant_[2] + 1.5 * iqr_
                li_ = quant_[0] - 1.5 * iqr_
                for count_ in dic_values[scale_][class_]:
                    v_ = dic_values[scale_][class_][count_]['dm_h']
                    if not isinstance(v_, (int, float)) or not math.isfinite(v_):
                        dic_values[scale_][class_][count_]['outlier'] = True
                    elif v_ < li_ or v_ > ls_:
                        dic_values[scale_][class_][count_]['outlier'] = True
                    else:
                        dic_values[scale_][class_][count_]['outlier'] = False

    def calc_pec(self, dic_values):
        self._pec_report_plan_rows = []
        self._pec_report_alt_rows = []
        om = self.cbx_workflow_outliers.currentIndex()
        om_names = (
            self.tr('Remover automaticamente'),
            self.tr('Avaliar individualmente'),
            self.tr('Usar todos'),
        )
        self._pec_report_pec_intro = self.tr('Tratamento de outliers (PEC): {0}').format(
            om_names[om] if 0 <= om < len(om_names) else str(om))

        dic_vectors = self.update_dic_vectors(dic_values)
        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('CALCULANDO PEC PLANIMÉTRICO')
        self.log_message(mss_, 'INFO')

        print(mss_)
        for scale_ in dic_vectors:
            for class_ in dic_vectors[scale_]:
                pec_h = round(scale_ * self.dic_pec_mm['H'][class_]['pec'], 2)
                ep_h = round(scale_ * self.dic_pec_mm['H'][class_]['ep'], 2)
                list_ = dic_vectors[scale_][class_]['H']
                perc_pec_ = self.perc_pec(vet_=list_, pec_=pec_h)
                pec_ok = perc_pec_ >= 0.90
                if pec_ok:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - OK,').format(
                        scale_, class_, round(perc_pec_ * 100), pec_h)
                else:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - FALHOU,').format(
                        scale_, class_, round(perc_pec_ * 100), pec_h)

                rms_ = self.rms(list_)
                ep_ok = math.isfinite(rms_) and rms_ <= ep_h
                rms_show = round(rms_, 2) if math.isfinite(rms_) else self.tr('n/d')
                if ep_ok:
                    str_ += self.tr('| {0} < {1} EP - OK, {2}').format(
                        rms_show, ep_h, len(list_))

                else:
                    str_ += self.tr('| {0} > {1} EP - FALHOU, {2}').format(
                        rms_show, ep_h, len(list_))
                print(str_)
                self.log_message(str_, 'INFO')
                self._pec_report_plan_rows.append(
                    (scale_, class_, round(perc_pec_ * 100), pec_h, pec_ok,
                     round(rms_, 2) if math.isfinite(rms_) else float('nan'),
                     ep_h, ep_ok, len(list_)))

        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('CALCULANDO PEC ALTIMÉTRICO')
        self.log_message(mss_, 'INFO')
        print(mss_)
        for scale_ in dic_vectors:
            for class_ in dic_vectors[scale_]:
                pec_v = round(self.dic_pec_v[scale_] * self.dic_pec_mm['V'][class_]['pec'], 2)
                ep_v = round(self.dic_pec_v[scale_] * self.dic_pec_mm['V'][class_]['ep'], 2)
                list_ = dic_vectors[scale_][class_]['V']
                perc_pec_ = self.perc_pec(vet_=list_, pec_=pec_v)
                pec_ok = perc_pec_ >= 0.90
                if pec_ok:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - OK, ').format(
                        scale_, class_, round(perc_pec_ * 100), pec_v)
                else:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - FALHOU,').format(
                        scale_, class_, round(perc_pec_ * 100), pec_v)

                rms_ = self.rms(list_)
                ep_ok = math.isfinite(rms_) and rms_ <= ep_v
                rms_show = round(rms_, 2) if math.isfinite(rms_) else self.tr('n/d')
                if ep_ok:
                    str_ += self.tr('| {0} < {1} EP - OK, {2}').format(
                        rms_show, ep_v, len(list_))

                else:
                    str_ += self.tr('| {0} > {1} EP - FALHOU, {2}').format(
                        rms_show, ep_v, len(list_))
                print(str_)
                self.log_message(str_, 'INFO')
                self._pec_report_alt_rows.append(
                    (scale_, class_, round(perc_pec_ * 100), pec_v, pec_ok,
                     round(rms_, 2) if math.isfinite(rms_) else float('nan'),
                     ep_v, ep_ok, len(list_)))

    def _format_param_value_for_report(self, meta):
        if not isinstance(meta, dict):
            return ''
        val = meta.get('value')
        if 'list' in meta:
            lst = meta['list']
            try:
                idx = int(val)
            except (TypeError, ValueError):
                try:
                    idx = int(float(val))
                except (TypeError, ValueError):
                    idx = 0
            if isinstance(lst, (list, tuple)) and 0 <= idx < len(lst):
                return str(lst[idx])
            return str(val)
        return '' if val is None else str(val)

    def _report_extent_intersection_html(self) -> str:
        lyr = self._resolve_limit_layer_for_editing(self.intersection_name)
        if lyr is None or not lyr.isValid():
            return html.escape(self.tr('(camada de interseção indisponível)'))
        ext = lyr.extent()
        if ext.isEmpty():
            return html.escape(self.tr('(extensão vazia — execute a interseção dos MDEs)'))
        crs = lyr.crs()
        rows = [
            html.escape(self.tr('CRS: {0}').format(crs.authid())),
            html.escape(
                self.tr('Envelope (CRS do projeto): Xmin={0}, Ymin={1}, Xmax={2}, Ymax={3}').format(
                    round(ext.xMinimum(), 3),
                    round(ext.yMinimum(), 3),
                    round(ext.xMaximum(), 3),
                    round(ext.yMaximum(), 3),
                )),
        ]
        if crs.isValid() and crs.authid().upper() not in ('EPSG:4326', 'OGC:CRS84'):
            try:
                xform = QgsCoordinateTransform(
                    crs,
                    QgsCoordinateReferenceSystem('EPSG:4326'),
                    QgsProject.instance(),
                )
                rect = xform.transformBoundingBox(ext)
                rows.append(
                    html.escape(
                        self.tr('Envelope aproximado (WGS84 / EPSG:4326): '
                                'lon_min={0}, lat_min={1}, lon_max={2}, lat_max={3}').format(
                            round(rect.xMinimum(), 6),
                            round(rect.yMinimum(), 6),
                            round(rect.xMaximum(), 6),
                            round(rect.yMaximum(), 6),
                        )))
            except Exception:
                rows.append(html.escape(self.tr('(transformação para WGS84 indisponível)')))
        return '<br/>'.join(rows)

    def _build_pec_results_tables_html(self) -> str:
        """Duas tabelas (planimétrico / altimétrico), mesmo estilo que o resto do relatório."""
        intro = (getattr(self, '_pec_report_pec_intro', '') or '').strip()
        plan_rows = getattr(self, '_pec_report_plan_rows', None) or []
        alt_rows = getattr(self, '_pec_report_alt_rows', None) or []

        def ok_txt(ok):
            return self.tr('OK') if ok else self.tr('FALHOU')

        hdrs = [
            self.tr('Escala'),
            self.tr('Classe'),
            self.tr('% < lim. PEC'),
            self.tr('Lim. PEC (m)'),
            self.tr('PEC'),
            self.tr('RMS (m)'),
            self.tr('Lim. EP (m)'),
            self.tr('EP'),
            self.tr('n'),
        ]
        th = ''.join(
            '<th>{}</th>'.format(html.escape(h)) for h in hdrs)

        def rows_html(rows):
            parts = []
            for (
                scale_, class_, perc_pct, lim_pec, pec_ok, rms_, lim_ep, ep_ok, n_,
            ) in rows:
                escala = self.tr('1:{0}.000').format(scale_)
                parts.append(
                    '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>'
                    '<td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
                        html.escape(escala),
                        html.escape(str(class_)),
                        html.escape(str(int(perc_pct))),
                        html.escape(str(lim_pec)),
                        html.escape(ok_txt(pec_ok)),
                        html.escape(str(rms_) if math.isfinite(rms_) else '—'),
                        html.escape(str(lim_ep)),
                        html.escape(ok_txt(ep_ok)),
                        html.escape(str(n_)),
                    ))
            return ''.join(parts)

        chunks = []
        if intro:
            chunks.append('<p>{}</p>'.format(html.escape(intro)))

        if plan_rows:
            chunks.append(
                '<h3>{}</h3>\n<table>\n<tr>{}</tr>\n{}</table>'.format(
                    html.escape(self.tr('PEC planimétrico')),
                    th,
                    rows_html(plan_rows),
                ))
        if alt_rows:
            chunks.append(
                '<h3>{}</h3>\n<table>\n<tr>{}</tr>\n{}</table>'.format(
                    html.escape(self.tr('PEC altimétrico')),
                    th,
                    rows_html(alt_rows),
                ))

        if not plan_rows and not alt_rows:
            chunks.append(
                '<p>{}</p>'.format(html.escape(
                    self.tr(
                        '(ainda não há resultados de PEC nesta sessão — execute a análise até ao fim.)'))))

        return '\n'.join(chunks)

    def _build_pdf_report_html(self) -> str:
        title = html.escape(self.tr('Relatório — MDE Acuracia Posicional'))
        when = html.escape(
            QDateTime.currentDateTime().toString('yyyy-MM-dd HH:mm:ss'))
        proj_path = self.dic_prj.get('project_file') or ''
        proj_esc = html.escape(proj_path)
        crs_ = html.escape(self.crs_epsg or self.tr('(não definido)'))
        study = html.escape(self.cbx_workflow_study.currentText())
        pairs = html.escape(self.cbx_workflow_pairs.currentText())
        outl = html.escape(self.cbx_workflow_outliers.currentText())
        sly = self.cbx_study_area_layer.currentLayer()
        study_ly = html.escape(
            sly.name() if isinstance(sly, QgsVectorLayer) else self.tr('(nenhuma)'))

        dem_rows = []
        for i in (0, 1):
            cbx = self.dic_prj['dems'][i]['obj_cbx']
            ly = cbx.currentLayer() if cbx else None
            label = self.dic_prj['dems'][i]['type']
            if isinstance(ly, QgsRasterLayer) and ly.isValid():
                dem_rows.append(
                    '<tr><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
                        html.escape(str(label)),
                        html.escape(ly.name()),
                        html.escape(ly.source()[:500]),
                    ))
            else:
                dem_rows.append(
                    '<tr><td>{}</td><td colspan="2">{}</td></tr>'.format(
                        html.escape(str(label)),
                        html.escape(self.tr('(não selecionado)'))))

        param_rows = []
        dlg = self.settings_dlg
        for sk, block in dlg.dic_param.items():
            if not isinstance(sk, str) or not sk.startswith('step_'):
                continue
            if not isinstance(block, dict) or 'fields' not in block:
                continue
            sec_lab = html.escape(block.get('label', sk))
            param_rows.append(
                f'<tr><td colspan="2" style="background:#eee"><b>{sec_lab}</b></td></tr>')
            for fk, meta in block['fields'].items():
                if not isinstance(meta, dict):
                    continue
                fl = html.escape(meta.get('label', fk))
                fv = html.escape(self._format_param_value_for_report(meta))
                param_rows.append(f'<tr><td>{fl}</td><td>{fv}</td></tr>')

        stats_block = '<br/>'.join([
            html.escape(self.lb_area.text()),
            html.escape(self.lb_ext_min.text()),
            html.escape(self.lb_ext_match.text()),
        ])

        pec_body = self._build_pec_results_tables_html()

        loc_block = self._report_extent_intersection_html()

        css = (
            'body { font-family: Segoe UI, Arial, sans-serif; font-size: 10pt; } '
            'h1 { font-size: 16pt; } h2 { font-size: 12pt; margin-top: 14px; border-bottom: 1px solid #444; } '
            'table { border-collapse: collapse; width: 100%; margin: 6px 0; } '
            'td, th { border: 1px solid #ccc; padding: 4px 6px; vertical-align: top; } '
            'th { background: #f0f0f0; }'
        )
        return f'''<!DOCTYPE html><html><head><meta charset="utf-8"/><style>{css}</style></head><body>
<h1>{title}</h1>
<p><b>{html.escape(self.tr('Data/hora:'))}</b> {when}</p>
<p><b>{html.escape(self.tr('Ficheiro de projeto:'))}</b> {proj_esc}</p>
<p><b>{html.escape(self.tr('CRS de referência (análise):'))}</b> {crs_}</p>

<h2>{html.escape(self.tr('1. Localização da área de estudo'))}</h2>
<p>{loc_block}</p>

<h2>{html.escape(self.tr('2. Fluxo de trabalho'))}</h2>
<table>
<tr><th>{html.escape(self.tr('Opção'))}</th><th>{html.escape(self.tr('Valor'))}</th></tr>
<tr><td>{html.escape(self.tr('Área de estudos'))}</td><td>{study}</td></tr>
<tr><td>{html.escape(self.tr('Pares homólogos'))}</td><td>{pairs}</td></tr>
<tr><td>{html.escape(self.tr('Outliers (PEC)'))}</td><td>{outl}</td></tr>
<tr><td>{html.escape(self.tr('Camada polígono (se aplicável)'))}</td><td>{study_ly}</td></tr>
</table>

<h2>{html.escape(self.tr('3. Modelos digitais de elevação (MDE)'))}</h2>
<table>
<tr><th>{html.escape(self.tr('Papel'))}</th><th>{html.escape(self.tr('Nome'))}</th><th>{html.escape(self.tr('Fonte (início)'))}</th></tr>
{"".join(dem_rows)}
</table>

<h2>{html.escape(self.tr('4. Parâmetros de processamento'))}</h2>
<table>
<tr><th>{html.escape(self.tr('Parâmetro'))}</th><th>{html.escape(self.tr('Valor'))}</th></tr>
{"".join(param_rows)}
</table>

<h2>{html.escape(self.tr('5. Estatísticas do painel'))}</h2>
<p>{stats_block}</p>

<h2>{html.escape(self.tr('6. Resultados PEC'))}</h2>
{pec_body}
</body></html>'''

    def export_project_pdf_report(self):
        """Gera PDF na pasta de dados do projeto: homónima ao .pa.gpkg (`project_data_dir`)."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            self.log_message(
                self.tr('Defina um projeto (.pa.gpkg) para exportar o relatório PDF.'), 'ERROR')
            return None
        data_dir = project_data_dir(pf)
        self.dic_prj['path'] = data_dir
        try:
            self.settings_dlg.flush_widgets_to_dic_param(log_values=False)
        except Exception:
            pass
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError as e:
            self.log_message(
                self.tr('Não foi possível criar a pasta do projeto: {0}').format(e), 'ERROR')
            return None
        stem = os.path.basename(_strip_project_ext(pf)) or 'projeto'
        ts = QDateTime.currentDateTime().toString('yyyy-MM-dd_HHmm')
        safe_stem = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in stem)[:80]
        fn = f'Relatorio_MDE_PA_{safe_stem}_{ts}.pdf'
        out_path = os.path.normpath(os.path.join(data_dir, fn))
        html_doc = self._build_pdf_report_html()
        try:
            printer = QPrinter(QPrinter.HighResolution)
            printer.setOutputFormat(QPrinter.PdfFormat)
            printer.setOutputFileName(out_path)
            doc = QTextDocument()
            doc.setDefaultFont(QFont('Segoe UI', 10))
            doc.setHtml(html_doc)
            doc.print_(printer)
        except Exception as e:
            self.log_message(self.tr('Falha ao gerar PDF: {0}').format(e), 'ERROR')
            return None
        return out_path

    def rms(self, vet_):
        vals = [v for v in vet_ if isinstance(v, (int, float)) and math.isfinite(v)]
        n = len(vals)
        if n < 2:
            return float('nan')
        sun_ = sum(v ** 2 for v in vals)
        return (sun_ / (n - 1)) ** 0.5

    def perc_pec(self, vet_, pec_):
        vals = [v for v in vet_ if isinstance(v, (int, float)) and math.isfinite(v)]
        if not vals:
            return 0.0
        count_ = sum(1 for v_ in vals if v_ < pec_)
        return count_ / len(vals)

    def _resolve_limit_layer_for_editing(self, layer_name: str):
        """Camada de limite no projeto (válida) ou carregada do .pa.gpkg; remove stubs inválidos."""
        proj = QgsProject.instance()
        for lyr in proj.mapLayersByName(layer_name):
            if lyr.isValid():
                return lyr
            try:
                proj.removeMapLayer(lyr.id())
            except Exception:
                pass
        lyr = self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)
        if lyr is not None and lyr.isValid():
            return lyr
        if self._ensure_limit_vector_tables_in_mdepa(self.crs_epsg):
            lyr = self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)
            if lyr is not None and lyr.isValid():
                return lyr
        return None

    def update_bar(self, dic_):
        if dic_.get('logonly', None):
            self.log_message(dic_['logonly'])
            return
        key_ = dic_['key']
        prog_bar = self.dic_prj['dems'][key_]['obj_prog_bar']
        palette = QPalette()
        palette.setColor(QPalette.Highlight, QColor(Qt.cyan))
        prog_bar.setPalette(palette)
        if 'error' in dic_:
            prog_bar.setFormat(str(dic_['error']))
            palette.setColor(QPalette.Highlight, QColor(Qt.red))
            prog_bar.setPalette(palette)
            if key_ == 3:
                self.log_message(
                    self.tr('Buffer - {0}').format(dic_['error']), level='ERROR')
            else:
                self.log_message(
                    self.tr('{0} {1} - {2}').format(
                        self.dic_prj['dems'][dic_['key']]['type'],
                        dic_['value'],
                        dic_['error']),
                    level='ERROR')
        elif 'warn' in dic_:
            prog_bar.setFormat(str(dic_['warn']))
            palette.setColor(QPalette.Highlight, QColor(Qt.lightGray))
            prog_bar.setPalette(palette)
            if dic_.get('log_warning'):
                self.log_message(str(dic_['log_warning']), 'WARNING')
        elif 'quant' in dic_:
            prog_bar.setRange(0, dic_['quant'])
            prog_bar.setValue(0)
            # self.log.info(True, f"set range {key_} 0 - {dic_['quant']}", pretty=True)
            palette.setColor(QPalette.Highlight, QColor(Qt.yellow))
            prog_bar.setPalette(palette)
        elif 'value' in dic_:
            prog_bar.setValue(dic_['value'])
            prog_bar.setFormat(f"{dic_['value']} - {dic_['msg']}")
            type_ = self.dic_prj['dems'][dic_['key']]['type']
            self.log_message(
                self.tr('{0} {1} - {2}').format(type_, dic_['value'], dic_['msg']))
            # print('dic_:', dic_)
            if 'feat' in dic_:
                if dic_['value'] == 6:
                    layer_name = f'__Limit_{self.dic_prj["dems"][key_]["type"]}__'
                    layer = self._resolve_limit_layer_for_editing(layer_name)
                    if layer is None:
                        self.log_message(
                            tr_ui('Camada de limite indisponível: {0}').format(layer_name),
                            'ERROR')
                        return
                    count = layer.featureCount()
                    feat_ = dic_['feat']
                    feat_.setAttributes([count + 1])
                    # print(feat_, feat_.geometry())

                    layer.startEditing()
                    layer.addFeature(feat_)
                    layer.commitChanges()
                    layer.updateExtents()
                    layer.triggerRepaint()

                    self.dic_prj['dems'][dic_['key']]['geom_status'] = True
                    self.run_polygon_intersection()
            elif 'feats' in dic_:
                if not self.layer_buffers:
                    self.layer_buffers = self.create_buffers_layer()
                lyr = self.layer_buffers
                if not getattr(self, '_buffers_layer_target_logged', False):
                    self._buffers_layer_target_logged = True
                    self.log_message(
                        self.tr(
                            '[__Buffers__] Tipo de geometria da camada: {0}. '
                            'Cada feição é verificada antes de inserir.'
                        ).format(QgsWkbTypes.displayString(lyr.wkbType())),
                        'INFO',
                    )
                lyr.startEditing()
                n_ok, n_skip, n_add_fail = 0, 0, 0
                for key_feat in dic_['feats']:
                    feat_ = dic_['feats'][key_feat]
                    g0 = feat_.geometry()
                    g_adj = self._geometry_for_buffers_layer(g0, lyr)
                    if g_adj is None:
                        n_skip += 1
                        continue
                    feat_adj = QgsFeature(feat_)
                    feat_adj.setGeometry(g_adj)
                    if not lyr.addFeature(feat_adj):
                        n_add_fail += 1
                        self._log_buffer_geom_diag_once(
                            self.tr(
                                'addFeature falhou (origem {0}): {1}'
                            ).format(
                                key_feat,
                                QgsWkbTypes.displayString(g_adj.wkbType()),
                            ))
                    else:
                        n_ok += 1
                if n_skip or n_add_fail:
                    self.log_message(
                        self.tr(
                            '[__Buffers__] Lote: {0} adicionadas, {1} ignoradas (geometria), '
                            '{2} rejeitadas pelo fornecedor.'
                        ).format(n_ok, n_skip, n_add_fail),
                        'WARNING' if (n_skip + n_add_fail) else 'INFO',
                    )
                if not lyr.commitChanges():
                    errs = lyr.commitErrors()
                    self.log_message(
                        self.tr('[__Buffers__] commitChanges falhou:\n{0}').format(
                            '\n'.join(errs) if errs else self.tr('(sem detalhe)')),
                        'ERROR',
                    )
                    lyr.rollBack()
                else:
                    lyr.updateExtents()
                    lyr.triggerRepaint()
            elif 'layer' in dic_:
                if isinstance(dic_['layer']['gpkg'], str):
                    datasource = ogr.Open(dic_['layer']['gpkg'])
                    for i in range(datasource.GetLayerCount()):
                        layer = datasource.GetLayerByIndex(i)
                    layer_prefix = datasource.GetLayerByIndex(0).GetName()
                    layer = self.get_gpkg_layer(prefix_=layer_prefix, gpkg_path=dic_['layer']['gpkg'], show=False)
                else:
                    layer = dic_['layer']['gpkg']
                layer_name = f'__{dic_["layer"]["type"]}_{self.dic_prj["dems"][key_]["type"]}__'
                options = QgsVectorFileWriter.SaveVectorOptions()
                options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
                output_fields = QgsFields()
                for field in layer.fields():
                    # Check if the field name is 'fid' (case-insensitive check for robustness)
                    if field.name().lower() != 'fid':
                        output_fields.append(QgsField(field.name(), field.type()))  # Append
                options.fields = output_fields
                options.layerName = layer_name
                QgsVectorFileWriter.writeAsVectorFormat(
                    layer=layer,
                    fileName=self.gpkg_path,
                    options=options)
                normalize_project_pa_file(self.gpkg_path)
                self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)
            if 'start_task' in dic_:
                if key_ == 0:
                    self.define_morphology(1)
                elif key_ == 1:
                    self.matching_lines()
            if 'model' in dic_:
                print(f'dem {key_}:', dic_['model'])
                self.dic_prj["dems"][key_]['model'] = dic_['model']
        elif 'dic_values' in dic_:
            dv = dic_['dic_values']
            om = self.cbx_workflow_outliers.currentIndex()
            if om == 0:
                self.check_outliers(dv)
            elif om == 1:
                self.check_outliers(dv)
                n_out = self._count_outliers_flagged(dv)
                QMessageBox.information(
                    self,
                    self.tr('Outliers (PEC)'),
                    self.tr(
                        'Foram identificados {0} valores atípicos (excluídos do cálculo PEC). '
                        'Prima OK para continuar.').format(n_out),
                )
            self.calc_pec(dv)
            if self.gpkg_path:
                save_pipeline_last_ok_snapshot(self.gpkg_path, self._flatten_run_snapshot())
            pdf_path = self.export_project_pdf_report()
            if pdf_path:
                self.log_message(self.tr('Relatório PDF exportado: {0}').format(pdf_path), 'INFO')
            # print(dic_['dic_values'])
        elif 'end' in dic_:
            palette.setColor(QPalette.Highlight, QColor(Qt.darkGreen))
            prog_bar.setValue(dic_['end'])
            prog_bar.setFormat(dic_['msg'])
            prog_bar.setPalette(palette)
            # self.db.commit_()

    def open_settings(self):
        if not self.settings_dlg:
            self.settings_dlg = SettingsDlg(main=self.parent, parent=self)
            self.reload_settings_from_project_file()
        self.settings_dlg.show()

