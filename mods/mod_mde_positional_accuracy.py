# -*- coding: utf-8 -*-
import datetime
import json
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
)
from qgis.PyQt.QtWidgets import (QAction, QScrollArea, QGridLayout, QPushButton, QLabel, QWidget, QSizePolicy,
                                 QSpacerItem, QDockWidget, QSplitter, QComboBox, QLineEdit, QDialog, QFrame, QCheckBox,
                                 QHBoxLayout, QVBoxLayout, QFileDialog, QTableWidget,
                                 QProgressBar, QDateEdit, QWidget, QVBoxLayout, QPushButton, QPlainTextEdit,
                                 QMessageBox)
from qgis.core import (QgsVectorFileWriter, QgsWkbTypes, QgsCoordinateTransformContext, QgsCoordinateReferenceSystem,
                       QgsCoordinateTransform, QgsGeometry,
                       QgsFeature, QgsVectorLayer, QgsRasterLayer, QgsFields, QgsField, QgsProject,
                       QgsMapLayerProxyModel, QgsLayerTreeLayer, Qgis)
from qgis.gui import QgsMapLayerComboBox
from .mod_aux_tools import AuxTools#, Obs2, Logger
from .mod_login import Database
from .mod_mde_pa_threads import Worker
from .mod_settings import SettingsDlg
from .plugin_i18n import PLUGIN_I18N_CONTEXT, tr_ui

plugin_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.join(plugin_path, 'libs')))
# Arquivo de projeto: GeoPackage com extensão composta (conteúdo GPKG)
PROJECT_EXT = '.mdepa.gpkg'

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
    """Se o caminho guardado não existir, tenta o .mdepa.gpkg após migração a partir de .mdepa."""
    if not saved_path:
        return ''
    p = os.path.normpath(os.path.abspath(saved_path))
    if os.path.isfile(p):
        return p
    pl = p.lower()
    if pl.endswith('.mdepa') and not pl.endswith(PROJECT_EXT.lower()):
        alt = p + '.gpkg'
        if os.path.isfile(alt):
            return alt
    return p


def project_file_filter_i18n() -> str:
    return (
        f'{tr_ui("Projeto MDE-PA (*.mdepa.gpkg)")};;{tr_ui("Todos (*.*)")}'
    )

# Tabela sem geometria no ficheiro de projeto: inicio/fim = data e hora local (SQLite 'YYYY-MM-DD HH:MM:SS') ou NULL
PIPELINE_ETAPAS_TABLE = 'mdepa_pipeline_etapas'
PIPELINE_DATETIME_FMT = '%Y-%m-%d %H:%M:%S'
PIPELINE_ETAPAS_DEF = (
    (1, 'poligonos_limites'),
    (2, 'interseccao_mdes'),
    (3, 'morfologia_referencia'),
    (4, 'morfologia_teste'),
    (5, 'matching_linhas'),
    (6, 'buffers'),
)

# Snapshot da última avaliação concluída (PEC): comparação em nova «Avaliar» para retomar só o necessário
PIPELINE_SNAPSHOT_ETAPA = '__pipeline_last_ok__'
PIPELINE_SNAPSHOT_CAMPO = 'config_json'

# Bloco de Config → primeira etapa a repetir (o resto da cadeia segue como hoje)
STEP_KEY_TO_RESTART_ETAPA = {
    'step_morfologia': 'morfologia_referencia',
    'step_match': 'matching_linhas',
    'step_buffers': 'buffers',
    'step_normalize_prog': 'buffers',
}


def _pipeline_etapa_order_index():
    return {name: i for i, (_, name) in enumerate(PIPELINE_ETAPAS_DEF)}


def load_pipeline_last_ok_snapshot(mdepa_path: str) -> dict:
    """Último estado de parâmetros + MDEs após uma avaliação concluída com sucesso."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return {}
    if not ensure_mdepa_settings_table(mdepa_path):
        return {}
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT valor FROM {MDEPA_SETTINGS_TABLE} WHERE etapa = ? AND campo = ?',
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
    if not ensure_mdepa_settings_table(mdepa_path):
        return False
    try:
        payload = json.dumps(flat, sort_keys=True, ensure_ascii=False)
        conn = sqlite3.connect(mdepa_path)
        try:
            conn.execute(
                f'''INSERT INTO {MDEPA_SETTINGS_TABLE} (etapa, campo, valor)
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


def compute_restart_etapa_from_snapshots(flat_now: dict, flat_was: dict):
    """Devolve (restart, extra). restart: None=completo desde polígonos; str=etapa; '__noop__'=sem alterações."""
    if not flat_was:
        return None, None
    for i in (0, 1):
        if flat_now.get(f'mde_{i}') != flat_was.get(f'mde_{i}'):
            return None, 'mde'
    for wk in (
        'workflow.study_mode',
        'workflow.pairs_mode',
        'workflow.outliers_mode',
        'workflow.study_layer_source',
    ):
        if flat_now.get(wk) != flat_was.get(wk):
            return None, 'workflow'
    changed_steps = set()
    for k in set(flat_now) | set(flat_was):
        if k.startswith('mde_'):
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
MDEPA_SETTINGS_TABLE = 'mdepa_settings'
# Caminhos/fontes dos MDE (QgsRasterLayer.source()): etapa fixa, campo '0' | '1'
MDEPA_MDE_ETAPA = 'mde_raster'


def _ensure_mdepa_settings_table_conn(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''CREATE TABLE IF NOT EXISTS {MDEPA_SETTINGS_TABLE} (
            etapa TEXT NOT NULL,
            campo TEXT NOT NULL,
            valor TEXT NOT NULL,
            PRIMARY KEY (etapa, campo)
        )'''
    )


def ensure_mdepa_settings_table(mdepa_path: str) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            _ensure_mdepa_settings_table_conn(conn)
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
    if not ensure_mdepa_settings_table(mdepa_path):
        return 0
    n = 0
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT etapa, campo, valor FROM {MDEPA_SETTINGS_TABLE}')
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
    if not ensure_mdepa_settings_table(mdepa_path):
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
                        f'''INSERT INTO {MDEPA_SETTINGS_TABLE} (etapa, campo, valor)
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


def _normalize_raster_source_compare(source: str) -> str:
    """Comparação aproximada de caminhos de ficheiro (ex.: Windows)."""
    if not source:
        return ''
    s = source.strip()
    pipe = s.find('|')
    path_part = s[:pipe] if pipe >= 0 else s
    if path_part and os.path.isfile(path_part):
        return os.path.normcase(os.path.normpath(os.path.abspath(path_part)))
    return s


def find_raster_layer_in_project(source_str: str):
    """Devolve QgsRasterLayer já no projeto com a mesma fonte que source_str, ou None."""
    if not source_str:
        return None
    proj = QgsProject.instance()
    norm = _normalize_raster_source_compare(source_str)
    for layer in proj.mapLayers().values():
        if not isinstance(layer, QgsRasterLayer):
            continue
        src = layer.source()
        if src == source_str:
            return layer
        if norm and _normalize_raster_source_compare(src) == norm:
            return layer
    return None


def find_vector_layer_in_project(source_str: str):
    """Devolve QgsVectorLayer já no projeto com a mesma fonte que source_str, ou None."""
    if not source_str:
        return None
    proj = QgsProject.instance()
    norm = _normalize_raster_source_compare(source_str)
    for layer in proj.mapLayers().values():
        if not isinstance(layer, QgsVectorLayer):
            continue
        src = layer.source()
        if src == source_str:
            return layer
        if norm and _normalize_raster_source_compare(src) == norm:
            return layer
    return None


def load_mde_sources_from_mdepa_path(mdepa_path: str) -> dict:
    """Lê fontes guardadas para os slots 0 e 1 (referência / teste)."""
    out = {}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_mdepa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT campo, valor FROM {MDEPA_SETTINGS_TABLE} WHERE etapa = ?',
                (MDEPA_MDE_ETAPA,),
            )
            for campo, valor in cur.fetchall():
                try:
                    k = int(campo)
                except (TypeError, ValueError):
                    continue
                if k in (0, 1):
                    out[k] = '' if valor is None else str(valor)
        finally:
            conn.close()
    except sqlite3.Error:
        return out
    return out


def save_mde_sources_to_mdepa_path(mdepa_path: str, key_to_source: dict) -> bool:
    """Grava fontes dos MDE (sempre os slots 0 e 1; valor vazio mantém linha com string vazia)."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_mdepa_settings_table(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for k in (0, 1):
                val = (key_to_source.get(k) or '').strip()
                conn.execute(
                    f'''INSERT INTO {MDEPA_SETTINGS_TABLE} (etapa, campo, valor)
                        VALUES (?, ?, ?)
                        ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                    (MDEPA_MDE_ETAPA, str(k), val),
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
    if not ensure_mdepa_settings_table(mdepa_path):
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
                    f'''INSERT INTO {MDEPA_SETTINGS_TABLE} (etapa, campo, valor)
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
    if not ensure_mdepa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT campo, valor FROM {MDEPA_SETTINGS_TABLE} WHERE etapa = ?',
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


def pipeline_datetime_now_local() -> str:
    """Data e hora atual (local), sem microssegundos — compatível com datetime() do SQLite."""
    return datetime.datetime.now().replace(microsecond=0).strftime(PIPELINE_DATETIME_FMT)


def project_data_dir(project_file: str) -> str:
    """Pasta auxiliar ao lado do .mdepa.gpkg: mesmo nome base lógico (sem PROJECT_EXT)."""
    project_file = os.path.abspath(project_file)
    parent = os.path.dirname(project_file)
    stem = os.path.basename(_strip_project_ext(project_file))
    return os.path.join(parent, stem)


def normalize_project_mdepa_file(project_path: str) -> None:
    """Se o OGR criar `stem.gpkg` paralelo ao ficheiro pedido, consolidar no .mdepa.gpkg."""
    if not project_path or not project_path.lower().endswith(PROJECT_EXT.lower()):
        return
    root = _strip_project_ext(project_path)
    alt_gpkg = root + '.gpkg'
    has_mdepa = os.path.isfile(project_path)
    has_gpkg = os.path.isfile(alt_gpkg)
    if not has_gpkg:
        return
    try:
        if not has_mdepa:
            os.replace(alt_gpkg, project_path)
        else:
            if os.path.getmtime(alt_gpkg) > os.path.getmtime(project_path):
                os.remove(project_path)
                os.replace(alt_gpkg, project_path)
            else:
                os.remove(alt_gpkg)
    except OSError:
        pass


class MDEPositionalAccuracy:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        self.name_ = 'MDE-Positional Accuracy'
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
            {'path': '',  # pasta de dados (logs, exports): vizinha ao .mdepa.gpkg, mesmo nome base
             'project_file': '',  # caminho absoluto do arquivo .mdepa.gpkg
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
                 'name': 'MDE_PA_proj',
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

        self.dic_lb_texts = {
            'area': tr_ui('Área da Interseção dos Modelos: {}'),
            'ext_min': tr_ui('Extensão Mínima da Amostra: {}'),
            'ext_match': tr_ui('Extensão da Amostra: {}'),
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

        last_pf = self.aux_tools.get_(key_='project_file')
        if last_pf and isinstance(last_pf, str):
            existing = resolve_saved_project_file_on_disk(last_pf)
            if os.path.isfile(existing):
                self.set_project_paths(existing)
                if os.path.normpath(os.path.abspath(last_pf)) != self.dic_prj['project_file']:
                    self.aux_tools.save_(
                        key_='project_file', value_=self.dic_prj['project_file'])
                self.ensure_pipeline_etapas_table()
                self.check_prj_folder(self.dic_prj['project_file'])
                self.reload_settings_from_project_file()

        self.intersection_name = '__Limit_Intersecao__'
        self.buffer_name = '__Buffers__'
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
        gl_tool.addLayout(row_hdr, r_, 1, 1, 2)

        r_ += 1
        sep_line = QFrame()
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 3)

        r_ += 1
        gl_prj = QGridLayout()
        self.lb_title_proj = QLabel(self.tr('Projeto (.mdepa.gpkg):'))
        gl_prj.addWidget(self.lb_title_proj, 0, 0)
        self.lb_status_proj = QLabel(self.tr('Não definido'))
        gl_prj.addWidget(self.lb_status_proj,  0, 1)
        gl_tool.addLayout(gl_prj, r_, 0, 1, 3)

        r_ += 1
        self.lb_path_proj = QLabel('~~~')
        self.lb_path_proj.setWordWrap(True)
        gl_tool.addWidget(self.lb_path_proj, r_, 0, 1, 2)
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
        gl_tool.addWidget(w_proj_btns, r_, 2, Qt.AlignRight | Qt.AlignTop)

        r_ += 1
        sep_line = QFrame(self)
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 3)

        for key_ in self.dic_prj['dems']:

            r_ += 1
            lb_title_ = QLabel(
                self.tr('Modelo de referência:') if key_ == 0 else self.tr('Modelo de teste:'))
            gl_tool.addWidget(lb_title_, r_, 0)
            obj_pb = QPushButton(self.tr('info'))
            obj_pb.setMaximumWidth(32)
            gl_tool.addWidget(obj_pb, r_, 2)
            self.dic_prj['dems'][key_]['obj_pb'] = obj_pb
            r_ += 1
            obj_cbx = QgsMapLayerComboBox(self)
            obj_cbx.setFilters(QgsMapLayerProxyModel.RasterLayer)
            obj_cbx.setAllowEmptyLayer(True)
            gl_tool.addWidget(obj_cbx, r_, 0, 1, 3)
            self.dic_prj['dems'][key_]['obj_cbx'] = obj_cbx
            r_ += 1
            obj_prog_bar = QProgressBar(self)
            gl_tool.addWidget(obj_prog_bar, r_, 0, 1, 3)
            self.dic_prj['dems'][key_]['obj_prog_bar'] = obj_prog_bar

        r_ += 1
        sep_line = QFrame(self)
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 3)
        r_ += 1
        self.lb_area = QLabel(self.dic_lb_texts['area'].format(''))
        gl_tool.addWidget(self.lb_area, r_, 0)
        r_ += 1
        self.lb_ext_min = QLabel(self.dic_lb_texts['ext_min'].format(''))
        gl_tool.addWidget(self.lb_ext_min, r_, 0)
        r_ += 1
        self.lb_ext_match = QLabel(self.dic_lb_texts['ext_match'].format(''))
        gl_tool.addWidget(self.lb_ext_match, r_, 0)
        # r_ += 1
        # lb_title_ = QLabel(f"Buffers:")
        # gl_tool.addWidget(lb_title_, r_, 0)
        # obj_pb = QPushButton('info')
        # obj_pb.setMaximumWidth(40)
        # gl_tool.addWidget(obj_pb, r_, 2)
        # self.dic_prj['dems'][key_]['obj_pb'] = obj_pb
        # r_ += 1
        # obj_cbx = QgsMapLayerComboBox(self)
        # obj_cbx.setFilters(QgsMapLayerProxyModel.RasterLayer)
        # gl_tool.addWidget(obj_cbx, r_, 0, 1, 3)
        # self.dic_prj['dems'][key_]['obj_cbx'] = obj_cbx
        # r_ += 1
        # self.buffer_prog_bar = QProgressBar(self)
        # gl_tool.addWidget(self.buffer_prog_bar, r_, 0, 1, 3)

        r_ += 1
        sep_line_wf = QFrame(self)
        sep_line_wf.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line_wf, r_, 0, 1, 3)

        r_ += 1
        gl_tool.addWidget(QLabel(self.tr('Área de estudos:')), r_, 0)
        r_ += 1
        self.cbx_workflow_study = QComboBox(self)
        self.cbx_workflow_study.addItems([
            self.tr('Calcular pela interseção dos MDEs'),
            self.tr('Editar após interseção'),
            self.tr('Selecionar de uma camada'),
        ])
        gl_tool.addWidget(self.cbx_workflow_study, r_, 0, 1, 3)

        r_ += 1
        self.lb_study_layer = QLabel(self.tr('Camada polígono (área de estudo):'))
        gl_tool.addWidget(self.lb_study_layer, r_, 0)
        r_ += 1
        self.cbx_study_area_layer = QgsMapLayerComboBox(self)
        self.cbx_study_area_layer.setFilters(QgsMapLayerProxyModel.PolygonLayer)
        self.cbx_study_area_layer.setAllowEmptyLayer(True)
        self.lb_study_layer.setVisible(False)
        self.cbx_study_area_layer.setVisible(False)
        gl_tool.addWidget(self.cbx_study_area_layer, r_, 0, 1, 3)

        r_ += 1
        gl_tool.addWidget(QLabel(self.tr('Seleção de pares homólogos:')), r_, 0)
        r_ += 1
        self.cbx_workflow_pairs = QComboBox(self)
        self.cbx_workflow_pairs.addItems([
            self.tr('Automática'),
            self.tr('Revisar automática'),
        ])
        gl_tool.addWidget(self.cbx_workflow_pairs, r_, 0, 1, 3)

        r_ += 1
        gl_tool.addWidget(QLabel(self.tr('PEC — outliers:')), r_, 0)
        r_ += 1
        self.cbx_workflow_outliers = QComboBox(self)
        self.cbx_workflow_outliers.addItems([
            self.tr('Remover automaticamente'),
            self.tr('Avaliar individualmente'),
            self.tr('Usar todos'),
        ])
        gl_tool.addWidget(self.cbx_workflow_outliers, r_, 0, 1, 3)

        r_ += 1
        self.pb_proc = QPushButton(self.tr('Avaliar'))
        gl_tool.addWidget(self.pb_proc, r_, 0, 1, 3, Qt.AlignHCenter)
        self._refresh_proc_button()

        r_ += 1
        self.lb_log = QLabel(self.tr('LOG:'))
        gl_tool.addWidget(self.lb_log, r_, 0, 1, 3)
        r_ += 1
        self.pte_log = QPlainTextEdit ()
        self.pte_log.setReadOnly(True)  # Logs are read-only
        self.pte_log.setWordWrapMode(QTextOption.WordWrap)  # Prevents long lines from wrapping
        self.pte_log.setBackgroundVisible(False)  # Optional, makes it look cleaner
        self.pte_log.setFont(QFont("Monospace", 8))  # Use a monospace font for better alignment
        gl_tool.addWidget(self.pte_log, r_, 0, 1, 3)

        lg_sa = QGridLayout()
        lg_sa.setContentsMargins(0, 0, 0, 0)
        lg_sa.setSpacing(0)
        lg_sa.addLayout(gl_tool, 0, 0)

        self.trigger_actions()
        return lg_sa

    def trigger_actions(self):
        for key_ in self.dic_prj['dems']:
            self.dic_prj['dems'][key_]['obj_pb'].clicked.connect(partial(self.log_mde_inf, key_=key_))
            self.dic_prj['dems'][key_]['obj_cbx'].layerChanged.connect(self.persist_mde_layer_selection)
        self.cbx_workflow_study.currentIndexChanged.connect(self._on_workflow_study_changed)
        self.cbx_workflow_study.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_workflow_pairs.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_workflow_outliers.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_study_area_layer.layerChanged.connect(self._persist_workflow_ui_if_project)
        self.pb_proc.clicked.connect(self.exec_analyze)
        self.pb_config.clicked.connect(self.open_settings)

    def set_project_paths(self, project_file: str):
        """Define arquivo .mdepa.gpkg e pasta de dados (logs etc.) com o mesmo nome base."""
        project_file = os.path.normpath(os.path.abspath(project_file))
        pl = project_file.lower()
        if pl.endswith(PROJECT_EXT.lower()):
            pass
        elif pl.endswith('.mdepa') and not pl.endswith(PROJECT_EXT.lower()):
            legacy = project_file
            migrated = legacy + '.gpkg'
            if os.path.isfile(migrated):
                project_file = migrated
            elif os.path.isfile(legacy):
                try:
                    os.replace(legacy, migrated)
                    project_file = migrated
                except OSError:
                    project_file = legacy
            else:
                project_file = migrated
        else:
            root, ext = os.path.splitext(project_file)
            if ext.lower() == '.gpkg':
                if root.lower().endswith('.mdepa'):
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

    def open_project_dialog(self):
        last = self.aux_tools.get_(key_='project_file')
        start_dir = os.path.dirname(last) if last and isinstance(last, str) else ''
        path, _ = QFileDialog.getOpenFileName(
            self, self.tr('Abrir projeto'), start_dir, project_file_filter_i18n())
        if not path:
            return
        if not os.path.isfile(path):
            self.log_message(self.tr('Arquivo não encontrado: {0}').format(path), 'ERROR')
            return
        self.set_project_paths(path)
        self.ensure_pipeline_etapas_table()
        self.aux_tools.save_(key_='project_file', value_=self.dic_prj['project_file'])
        self.check_prj_folder(self.dic_prj['project_file'])
        self.reload_settings_from_project_file()
        self.log_message(
            self.tr('Projeto aberto: {0}').format(self.dic_prj['project_file']), 'INFO')

    def new_project_dialog(self):
        last = self.aux_tools.get_(key_='project_file')
        start_dir = os.path.dirname(last) if last and isinstance(last, str) else ''
        suggest = os.path.join(start_dir, f'novo_projeto{PROJECT_EXT}') if start_dir else f'novo_projeto{PROJECT_EXT}'
        path, _ = QFileDialog.getSaveFileName(
            self, self.tr('Novo projeto'), suggest, project_file_filter_i18n())
        if not path:
            return
        path = os.path.normpath(os.path.abspath(path))
        pl = path.lower()
        if not pl.endswith(PROJECT_EXT.lower()):
            if pl.endswith('.mdepa'):
                path = path + '.gpkg'
            elif pl.endswith('.gpkg'):
                root, _ = os.path.splitext(path)
                if not root.lower().endswith('.mdepa'):
                    path = root + PROJECT_EXT
            else:
                path = path + PROJECT_EXT
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
        self.aux_tools.save_(key_='project_file', value_=path)
        self.check_prj_folder(path)
        self.persist_project_config_from_widgets(log_values=False)
        self.reload_settings_from_project_file()
        self.log_message(
            self.tr('Novo projeto criado: {0} (CRS inicial: {1})').format(path, crs_auth), 'INFO')

    def reload_settings_from_project_file(self):
        """Restaura dic_param: defaults → QSettings → valores gravados no .mdepa (por projeto)."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        dlg = self.settings_dlg
        dlg.apply_defaults_to_values()
        dlg.get_dic_from_settings()
        load_plugin_settings_from_mdepa_path(pf, dlg.dic_param)
        dlg.sync_widgets_from_dic_param()
        self.restore_mde_layers_from_project()
        self.restore_workflow_ui_from_project()

    def persist_mde_layer_selection(self):
        """Grava QgsRasterLayer.source() dos combos no .mdepa (slots 0 e 1)."""
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
        save_mde_sources_to_mdepa_path(pf, key_to_source)

    def restore_mde_layers_from_project(self):
        """Carrega rasters guardados no .mdepa se ainda não estiverem no projeto QGIS."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        sources = load_mde_sources_from_mdepa_path(pf)
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
                existing = find_raster_layer_in_project(uri)
                if existing is not None:
                    cbx.setLayer(existing)
                    continue
                base = os.path.basename(uri.split('|')[0].strip()) or f'MDE_{key_}'
                label = self.dic_prj['dems'][key_]['type']
                layer_name = f'{label} ({base})'
                rl = QgsRasterLayer(uri, layer_name)
                if not rl.isValid():
                    self.log_message(
                        self.tr('Não foi possível carregar o raster: {0}').format(uri), 'ERROR')
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
        """Atualiza dic_param a partir dos widgets, grava QSettings e .mdepa (parâmetros + MDEs)."""
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
        self.persist_mde_layer_selection()
        self._persist_workflow_ui_if_project()

    def _flatten_run_snapshot(self) -> dict:
        """Parâmetros dos steps + fontes MDE (para comparar com a última avaliação concluída)."""
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
            out[f'mde_{i}'] = ly.source() if isinstance(ly, QgsRasterLayer) else ''
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

    def _persist_workflow_ui_if_project(self):
        if self._workflow_pause is not None:
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
            self.log_message(self.tr('CRS do modelo de referência inválido.'), 'ERROR')
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
        """True se a camada nomeada existe e abre no .mdepa atual."""
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
            normalize_project_mdepa_file(path)
            return os.path.isfile(path)
        except Exception:
            return False

    def _mde_pa_group(self):
        root = QgsProject.instance().layerTreeRoot()
        grp = root.findGroup('__MDE_PA__')
        if not grp:
            grp = root.insertGroup(0, '__MDE_PA__')
        return grp

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
        """Garante no ficheiro .mdepa as três camadas de polígono vazias (sem as carregar no mapa)."""
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
            normalize_project_mdepa_file(self.gpkg_path)

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
            normalize_project_mdepa_file(self.gpkg_path)

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
            normalize_project_mdepa_file(self.gpkg_path)

        return (
            self._gpkg_layer_valid(r)
            and self._gpkg_layer_valid(t)
            and self._gpkg_layer_valid(i))

    def _ensure_limit_layers_for_analysis(self) -> bool:
        """Só entra no QGIS o que já existe no .mdepa: cria tabelas vazias no ficheiro e depois carrega com OGR."""
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

    def _clear_features_from_limit_layers(self):
        """Nova análise: esvazia limites e interseção no projeto antes de gerar polígonos."""
        proj = QgsProject.instance()
        names = (
            f'__Limit_{self.dic_prj["dems"][0]["type"]}__',
            f'__Limit_{self.dic_prj["dems"][1]["type"]}__',
            self.intersection_name,
        )
        for nm in names:
            for lyr in proj.mapLayersByName(nm):
                lyr.startEditing()
                ids = [f.id() for f in lyr.getFeatures()]
                if ids:
                    lyr.deleteFeatures(ids)
                lyr.commitChanges()

    def check_prj_folder(self, project_file):
        """Atualiza rótulos conforme o ficheiro de projeto (.mdepa.gpkg) existe."""
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
            self.lb_status_proj.setText(self.tr('Arquivo .mdepa.gpkg ausente'))
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

    def log_mde_inf(self, key_: int):
        if self.dic_prj['dems'][key_]['obj_cbx']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()

            mss_ = self.tr('=======================================\n')
            mss_ += self.tr('  INFORMAÇÕES DO MODELO DE {0}\n').format(
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
                self.tr('MODELO DE {0} NÃO DEFINIDO').format(self.dic_prj['dems'][key_]['type']),
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
                self.tr('Defina o projeto (.mdepa.gpkg): menu ⋯ → Abrir ou Novo.'), 'ERROR')
            return
        pf = self.dic_prj['project_file']
        if not os.path.isfile(pf):
            self.log_message(self.tr('O arquivo .mdepa.gpkg do projeto não existe.'), 'ERROR')
            return

        layer_ref = self.dic_prj['dems'][0]['obj_cbx'].currentLayer()
        layer_test = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
        if not isinstance(layer_ref, QgsRasterLayer) or not layer_ref.isValid():
            self.log_message(
                self.tr('Selecione o modelo de referência (raster válido).'), 'ERROR')
            return
        if not isinstance(layer_test, QgsRasterLayer) or not layer_test.isValid():
            self.log_message(
                self.tr('Selecione o modelo de teste (raster válido).'), 'ERROR')
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

        self.persist_project_config_from_widgets(log_values=False)

        if self.threads_running > 0 or len(self.active_workers) > 0:
            self.log_message(
                self.tr('Aguarde o fim da análise em curso antes de nova avaliação.'), 'WARNING')
            return

        flat_now = self._flatten_run_snapshot()
        flat_was = load_pipeline_last_ok_snapshot(pf)
        restart, _reason = compute_restart_etapa_from_snapshots(flat_now, flat_was)
        if restart == '__noop__':
            self.log_message(
                self.tr('Parâmetros e MDEs inalterados desde a última avaliação concluída.'), 'INFO')
            return

        self.create_gpkg()

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
        elif restart == 'matching_linhas':
            self.log_message(
                self.tr('Retomando a partir do emparelhamento de linhas (parâmetros alterados).'), 'INFO')
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

            sum_area = 0

            for feat_0 in layer_0.getFeatures():
                geom_0 = feat_0.geometry()
                for feat_1 in layer_1.getFeatures():
                    geom_1 = feat_1.geometry()
                    intersec_ = geom_0.intersection(geom_1)
                    sum_area += intersec_.area()
                    feat_i = QgsFeature()
                    feat_i.setGeometry(intersec_)
                    count = layer_i.featureCount()
                    feat_i.setAttributes([count + 1, intersec_.area()])
                    layer_i.startEditing()
                    layer_i.addFeature(feat_i)
                    layer_i.commitChanges()
                    layer_i.updateExtents()
                    layer_i.triggerRepaint()
            area_units = QgsProject.instance().areaUnits()
            Qgis.DistanceUnit


            self.lb_area.setText(self.dic_lb_texts['area'].format(round(sum_area, 1)))
            ext_ = round(2.0176*sum_area**0.5478, 1)
            dist_units = QgsProject.instance().distanceUnits()
            self.lb_ext_min.setText(self.dic_lb_texts['ext_min'].format(ext_))
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
                _ensure_mdepa_settings_table_conn(conn)
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
            self.log_message(self.tr('Caminho do projeto (.mdepa.gpkg) indefinido.'), 'ERROR')
            return
        data_dir = self.dic_prj.get('path')
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
        if not os.path.isfile(self.gpkg_path):
            self.log_message(self.tr('O arquivo .mdepa.gpkg do projeto não existe.'), 'ERROR')
            return
        if not self.ensure_pipeline_etapas_table():
            self.log_message(self.tr('Falha ao garantir tabelas auxiliares no .mdepa.gpkg.'), 'ERROR')
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
        self.node_group = self._mde_pa_group()

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

    def matching_lines(self):
        print('matching_lines')
        conn = self.gpkg_conn()
        curs = conn.cursor()
        dic_param_match = self.settings_dlg.dic_param['step_match']['fields']
        dist_max = float(dic_param_match['dist_max']['value'])
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
        self.lb_ext_match.setText(self.dic_lb_texts['ext_match'].format(round(ext_sum, 1)))
        # print('dic_match', self.dic_match)
        if self.cbx_workflow_pairs.currentIndex() == 1:
            self._workflow_pause = 'post_pairs_review'
            self._refresh_proc_button()
            n_pairs = sum(len(self.dic_match[k]) for k in self.dic_match)
            self.log_message(
                self.tr(
                    'Pares homólogos: {0} grupos; rever no mapa e prima Continuar.').format(n_pairs),
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
        normalize_project_mdepa_file(self.gpkg_path)
        layer_ = self.get_gpkg_layer(prefix_=self.buffer_name, gpkg_path=self.gpkg_path)
        return (layer_)

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
                    if not dic_values[scale_][class_][count_].get('outlier',False):
                        dic_vectors[scale_][class_]['H'].append(dic_values[scale_][class_][count_]['dm_h'])
                        dic_vectors[scale_][class_]['V'].append(dic_values[scale_][class_][count_]['dm_v'])
        return dic_vectors

    def check_outliers(self, dic_values):
        dic_stats = self.update_dic_vectors(dic_values)
        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                quant_ = statistics.quantiles(data=dic_stats[scale_][class_]['H'])
                iqr_ = quant_[2] - quant_[0]
                ls_ = quant_[2] + 1.5 * iqr_
                li_ = quant_[0] - 1.5 * iqr_
                for count_ in dic_values[scale_][class_]:
                    v_ = dic_values[scale_][class_][count_]['dm_h']
                    if v_ < li_ or v_ > ls_:
                        dic_values[scale_][class_][count_]['outlier'] = True
                    else:
                        dic_values[scale_][class_][count_]['outlier'] = False

    def calc_pec(self, dic_values):
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
                if perc_pec_ >= 0.90:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - OK,').format(
                        scale_, class_, round(perc_pec_ * 100), pec_h)
                else:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - FALHOU,').format(
                        scale_, class_, round(perc_pec_ * 100), pec_h)

                rms_ = self.rms(list_)
                if rms_ <= ep_h:
                    str_ += self.tr('| {0} < {1} EP - OK, {2}').format(
                        round(rms_, 2), ep_h, len(list_))

                else:
                    str_ += self.tr('| {0} > {1} EP - FALHOU, {2}').format(
                        round(rms_, 2), ep_h, len(list_))
                print(str_)
                self.log_message(str_, 'INFO')

        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('CALCULANDO PEC ALTIMÉTRICO')
        self.log_message(mss_, 'INFO')
        print(mss_)
        for scale_ in dic_vectors:
            for class_ in dic_vectors[scale_]:
                pec_v = round(self.dic_pec_v[scale_] * self.dic_pec_mm['V'][class_]['pec'], 2)
                ep_v = round(self.dic_pec_v[scale_] *  self.dic_pec_mm['V'][class_]['ep'], 2)
                list_ = dic_vectors[scale_][class_]['V']
                perc_pec_ = self.perc_pec(vet_=list_, pec_=pec_v)
                if perc_pec_ >= 0.90:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - OK, ').format(
                        scale_, class_, round(perc_pec_ * 100), pec_v)
                else:
                    str_ = self.tr('1:{0}.000-{1}= {2}% < {3} PEC - FALHOU,').format(
                        scale_, class_, round(perc_pec_ * 100), pec_v)

                rms_ = self.rms(list_)
                if rms_ <= ep_v:
                    str_ += self.tr('| {0} < {1} EP - OK, {2}').format(
                        round(rms_, 2), ep_v, len(list_))

                else:
                    str_ += self.tr('| {0} > {1} EP - FALHOU, {2}').format(
                        round(rms_, 2), ep_v, len(list_))
                print(str_)
                self.log_message(str_, 'INFO')

    def rms(self, vet_):
        sun_ = 0
        for v_ in vet_:
            sun_ += v_ ** 2
        rms_ = (sun_ / (len(vet_) - 1)) ** 0.5
        return rms_

    def perc_pec(self, vet_, pec_):
        count_ = 0
        for v_ in vet_:
            if v_ < pec_:
                count_ += 1
        return count_ / len(vet_)

    def _resolve_limit_layer_for_editing(self, layer_name: str):
        """Camada de limite no projeto (válida) ou carregada do .mdepa; remove stubs inválidos."""
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
                self.layer_buffers.startEditing()
                count = self.layer_buffers.featureCount()
                for key_feat in dic_['feats']:
                    feat_ = dic_['feats'][key_feat]
                    self.layer_buffers.addFeature(feat_)
                self.layer_buffers.commitChanges()
                self.layer_buffers.updateExtents()
                self.layer_buffers.triggerRepaint()
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
                normalize_project_mdepa_file(self.gpkg_path)
                self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)
            if 'start_task' in dic_:
                if key_ == 0:
                    self.define_morphology(1)
                elif key_ == 1:
                    self.matching_lines()
            if 'model' in dic_:
                print(f'modelo {key_}:', dic_['model'])
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

