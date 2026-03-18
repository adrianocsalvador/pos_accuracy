# -*- coding: utf-8 -*-
import datetime
import json
import os
import sqlite3
import math
import statistics
from queue import Queue
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
# from sys import prefix

from osgeo import ogr
from qgis.PyQt.QtCore import (QSettings, Qt, QSize, QTranslator, QCoreApplication, QEvent, QThreadPool, QDateTime,
                              QVariant)
from qgis.PyQt.QtGui import QPixmap, QIcon, QFont, QPalette, QColor, QTextCharFormat, QBrush, QTextOption
from qgis.PyQt.QtWidgets import (QAction, QScrollArea, QGridLayout, QPushButton, QLabel, QWidget, QSizePolicy,
                                 QSpacerItem, QDockWidget, QSplitter, QComboBox, QLineEdit, QDialog, QFrame, QCheckBox,
                                 QHBoxLayout, QVBoxLayout, QFileDialog, QTableWidget,
                                 QProgressBar, QDateEdit, QWidget, QVBoxLayout, QPushButton, QPlainTextEdit)
from qgis.core import (QgsVectorFileWriter, QgsWkbTypes, QgsCoordinateTransformContext, QgsCoordinateReferenceSystem,
                       QgsFeature, QgsVectorLayer, QgsFields, QgsField, QgsProject, QgsMapLayerProxyModel,
                       QgsLayerTreeLayer, Qgis)
from qgis.gui import QgsMapLayerComboBox
from .mod_aux_tools import AuxTools#, Obs2, Logger
from .mod_login import Database
from .mod_mde_pa_threads import Worker
from .mod_settings import SettingsDlg

plugin_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.join(plugin_path, 'libs')))


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
        # initialize locale
        locale = QSettings().value('locale/userLocale')[0:2]
        locale_path = os.path.join(
            plugin_path,
            'i18n',
            '{}_{}.qm'.format(self.name_.replace(' ', ''), locale))

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)

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
        return QCoreApplication.translate(self.name_.replace(' ', ''), message)

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
        """Removes the plugin menu item and icon from QGIS GUI."""
        self.dock1.setVisible(False)
        for action in self.actions:
            self.iface.removePluginMenu(
                self.tr(self.title1),
                action)
            self.iface.removeToolBarIcon(action)

        for x in self.iface.mainWindow().findChildren(QDockWidget):
            if self.title1.lower() in x.objectName().lower():
                self.dock1.deleteLater()
                self.dock1.setParent(None)
                del x

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
        # self.dic_debugger = {
        #     'user': 'adria',
        #     'log_state': True,
        #     'plugin_name': f'Wd_{name_}'
        # }
        if os.getlogin() == 'adria':
            self.iface.actionShowPythonDialog().trigger()
        #     self.log = Logger(self.main.name_)

        self.dic_prj = \
            {'path': '',
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
                     'prj': '.gpkg',
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
        # self.dic_epsg = {
        #     '-25(S)': 31985,
        #     '-24(S)': 31984,
        #     '-23(S)': 31983,
        #     '-22(S)': 31982,
        #     '-21(S)': 31981,
        #     '-20(S)': 31980,
        #     '-19(S)': 31979,
        #     '-18(S)': 31978,
        #     '18(N)': 31972,
        #     '19(N)': 31973,
        #     '20(N)': 31974,
        #     '21(N)': 31975,
        #     '22(N)': 31976,
        #     '23(N)': 6210,
        #     '24(N)': 6211}

        self.max_threads = 3  # Limit to 3 concurrent tasks
        # self.thread_pool = QThreadPool.globalInstance()  # Use QThreadPool for task management
        # self.thread_pool.setMaxThreadCount(self.max_threads)

        self.task_queue = Queue()  # Task queue
        self.threads_running = 0  # Track active threads
        self.active_workers = {}  # Keep track of active workers

        self.dic_lb_texts = {'area': "Área da Interseção dos Modelos: {}", 'ext': "Extensão Mínima da Amostra: {}"}
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
        self.settings_dlg = SettingsDlg(main=parent, parent=self)
        self.list_morph = ['Cumeada', 'Hidrografia_Numerica']
        self.list_norm_type = ['Escalar', 'Mínima Distância', 'Sem Normalização']
        self.intersection_name = '__Limit_Intersecao__'
        self.buffer_name = '__Buffers__'
        self.layer_buffers = None

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
        gl_tool.addWidget(self.lb_session_logo, r_, 0)

        self.lb_version = QLabel(f'v{self.main.plugin_version()}')
        self.lb_version.setAlignment(Qt.AlignRight)
        gl_tool.addWidget(self.lb_version, r_, 2)

        r_ += 1
        sep_line = QFrame()
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 3)

        r_ += 1
        gl_prj = QGridLayout()
        self.lb_title_proj = QLabel('Projeto:')
        gl_prj.addWidget(self.lb_title_proj, 0, 0)
        self.lb_status_proj = QLabel('Não Definido')
        gl_prj.addWidget(self.lb_status_proj,  0, 1)
        self.pb_clear_prj_folder = QPushButton('Limpar Pasta')
        self.pb_clear_prj_folder.setVisible(False)
        self.pb_clear_prj_folder.setToolTip(
            'Este Botão irá excluir os arquivos com nomes que são utilizados no projeto apenas. \nNomes fora do padrão NÂO serão removidos')
        gl_prj.addWidget(self.pb_clear_prj_folder, 0, 2)
        gl_tool.addLayout(gl_prj, r_, 0, 1, 3)

        r_ += 1
        self.lb_path_proj = QLabel('~~~')
        gl_tool.addWidget(self.lb_path_proj, r_, 0, 1, 2)
        self.pb_define_proj = QPushButton('...')
        self.pb_define_proj.setMaximumWidth(40)
        gl_tool.addWidget(self.pb_define_proj, r_, 2)

        for key_ in self.dic_prj['dems']:

            r_ += 1
            sep_line = QFrame(self)
            sep_line.setFrameShape(QFrame.HLine)
            gl_tool.addWidget(sep_line, r_, 0, 1, 3)
            r_ += 1
            lb_title_ = QLabel( f"Modelo de {self.dic_prj['dems'][key_]['type']}:")
            gl_tool.addWidget(lb_title_, r_, 0)
            obj_pb = QPushButton('info')
            obj_pb.setMaximumWidth(40)
            gl_tool.addWidget(obj_pb, r_, 2)
            self.dic_prj['dems'][key_]['obj_pb'] = obj_pb
            r_ += 1
            obj_cbx = QgsMapLayerComboBox(self)
            obj_cbx.setFilters(QgsMapLayerProxyModel.RasterLayer)
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
        self.lb_ext = QLabel(self.dic_lb_texts['ext'].format(''))
        gl_tool.addWidget(self.lb_ext, r_, 0)
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
        self.pb_proc = QPushButton('Avaliar')
        gl_tool.addWidget(self.pb_proc, r_, 1, 1, 1)
        self.pb_config = QPushButton('Config')
        # self.pb_config.setEnabled(False)
        gl_tool.addWidget(self.pb_config, r_, 0, 1, 1)

        r_ += 1
        self.lb_log = QLabel('LOG:')
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
        self.pb_define_proj.clicked.connect(partial(self.get_folder, key_='dir_prj'))
        self.pb_clear_prj_folder.clicked.connect(self.clear_prj_folder)
        for key_ in self.dic_prj['dems']:
            self.dic_prj['dems'][key_]['obj_pb'].clicked.connect(partial(self.log_mde_inf, key_=key_))

        # self.cb_db.highlighted.connect(self.start_cb)
        # self.cb_db.activated.connect(self.update_parameters)
        # self.pb_conn.clicked.connect(self.connect_db)
        #
        # self.cb_add_tool.activated.connect(self.cb_add_activated)
        # self.tw_data.cellClicked.connect(self.tw_cell_clicked)
        # self.pb_server_folder.clicked.connect(self.get_folder_out)z
        self.pb_proc.clicked.connect(self.exec_analyze)
        self.pb_config.clicked.connect(self.open_settings)

    def get_folder(self, key_='dir_prj'):
        print('get_folder')
        dir_ = self.aux_tools.get_(key_=key_)
        # Get Directory using QFileDialog
        source_folder = QFileDialog.getExistingDirectory(directory=dir_)
        if source_folder and os.path.exists(source_folder):
            self.aux_tools.save_(key_=key_, value_=source_folder)
            if key_ == 'dir_prj':
                self.dic_prj['path'] = source_folder
                self.check_prj_folder(source_folder)
            else:
                print(f'CHAVE "{key_}" DESCONHECIDA')
        else:
            print(f'"{source_folder}" INVÁLIDO')
            self.log_message(f'"{source_folder}" INVÁLIDO', 'ERROR')

    def check_prj_folder(self, source_folder):
        if os.path.exists(source_folder):
            dic_st = self.dic_prj["standard"]
            self.lb_path_proj.setText(f'{source_folder}/{dic_st["name"]}{dic_st["files"]["prj"]}')
            self.log_message(f'Pasta do Projeto definida: {source_folder}', 'INFO')


            list_ = os.listdir(source_folder)
            chk_ = False
            for file_ in list_:
                for st_key in self.dic_prj['standard']['files']:
                    if file_.upper() == f'{dic_st["name"]}{dic_st["files"][st_key]}'.upper():
                        self.lb_status_proj.setText('Projeto já existe. Defina outra pasta ou clique -> ')
                        self.lb_status_proj.setStyleSheet("color: red;")
                        self.pb_clear_prj_folder.setVisible(True)
                        chk_ = True
                        self.log_message(f'ARQUIVO PADRÃO JÁ EXISTE: "{dic_st["name"]}{dic_st["files"][st_key]}" ', 'ERROR')
            if chk_:
                self.dic_prj['status'] = 0
            else:
                self.dic_prj['status'] = 1
                self.lb_status_proj.setText('OK')
                self.lb_status_proj.setStyleSheet("color: blue;")
                self.pb_clear_prj_folder.setVisible(False)

        else:
            print(f'"{source_folder}" INVÁLIDO')
            self.log_message(f'"{source_folder}" INVÁLIDO', 'ERROR')
            return

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

        dic_st = self.dic_prj["standard"]
        source_folder = self.dic_prj['path']
        log_path = os.path.join(source_folder, f'{dic_st["name"]}{dic_st["files"]["log"]}')

        with open(log_path, "a") as file:
            file.write(log_entry)

    def log_mde_inf(self, key_: int):
        if self.dic_prj['dems'][key_]['obj_cbx']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()

            mss_ = f'=======================================\n'
            mss_ += f'  INFORMAÇÕES DO MODELO DE {self.dic_prj["dems"][key_]["type"].upper()}\n'
            mss_ += f'  Layer name: {layer_.name()}\n'
            mss_ += f'  Source path: {layer_.source()}\n'
            mss_ += f'  Is valid: {layer_.isValid()}\n'
            mss_ += f'  CRS: {layer_.crs().authid()}\n'
            mss_ += f'  Width (pixels): {layer_.width()}\n'
            mss_ += f'  Height (pixels): {layer_.height()}\n'
            mss_ += f'  Band count: {layer_.bandCount()}\n'
            mss_ += f'  Extent (string): {layer_.extent().snappedToGrid(0.001)}\n'
            mss_ += f'  Pixel size X: {layer_.rasterUnitsPerPixelX()}\n'
            mss_ += f'  Pixel size Y: {layer_.rasterUnitsPerPixelY()}\n'
            mss_ += f'=======================================\n'
            self.log_message(mss_, 'INFO')
        else:
            self.log_message(f"MODELO DE {self.dic_prj['dems'][key_]['type']} NÃO DEFINIDO", "ERROR")

    def clear_prj_folder(self):
        dic_st = self.dic_prj["standard"]
        source_folder = self.dic_prj['path']
        self.log_message('', 'INFO')
        self.log_message(f'REMOVENDO ARQUIVOS "{source_folder}"', 'INFO')
        for st_key in self.dic_prj['standard']['files']:
            file_path = os.path.join(source_folder, f'{dic_st["name"]}{dic_st["files"][st_key]}')
            if os.path.exists(file_path):
                os.remove(file_path)
                self.log_message(f'REMOVIDO: "{dic_st["name"]}{dic_st["files"][st_key]}" ', 'INFO')
        self.dic_prj['status'] = 1
        self.lb_status_proj.setText('OK')
        self.lb_status_proj.setStyleSheet("color: blue;")
        self.pb_clear_prj_folder.setVisible(False)

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

    def exec_analyze(self):
        layer_ref = self.dic_prj['dems'][0]['obj_cbx'].currentLayer()

        self.crs_epsg = layer_ref.crs().authid()

        self.create_gpkg()
        self.define_intersection()

    def run_polygon_intersection(self):
        status_0 = self.dic_prj['dems'][0]['geom_status']
        status_1 = self.dic_prj['dems'][1]['geom_status']
        if status_0 and status_1:
            mss_ = f'CALCULANDO AREA DE INTERSEÇÃO DOS MDEs'
            self.log_message(mss_, 'INFO')

            layer_0 = self.get_gpkg_layer(prefix_= f'__Limit_{self.dic_prj["dems"][0]["type"]}__')
            layer_1 = self.get_gpkg_layer(prefix_= f'__Limit_{self.dic_prj["dems"][1]["type"]}__')
            layer_i = self.get_gpkg_layer(prefix_= self.intersection_name)

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
            self.lb_ext.setText(self.dic_lb_texts['ext'].format(ext_))
            mss_ = f'AREA DE INTERSEÇÃO DOS MDEs DEFINIDA\n'
            mss_ += f'=======================================\n'
            self.log_message(mss_, 'INFO')
            self.define_morphology()

    def create_gpkg(self):
        dic_st = self.dic_prj["standard"]
        source_folder = self.dic_prj['path']
        self.gpkg_path = os.path.join(source_folder, f'{dic_st["name"]}{dic_st["files"]["prj"]}')
        options_ = QgsVectorFileWriter.SaveVectorOptions()
        options_.driverName = "GPKG"
        layer_r_name = f'__Limit_{self.dic_prj["dems"][0]["type"]}__'
        options_.layerName = layer_r_name
        writer_ = QgsVectorFileWriter.create(
            self.gpkg_path,
            QgsFields(),
            QgsWkbTypes.Polygon,
            QgsCoordinateReferenceSystem(self.crs_epsg),
            QgsCoordinateTransformContext(),
            options_)
        assert writer_.hasError() == QgsVectorFileWriter.NoError
        del writer_  # to flush
        self.get_gpkg_layer(prefix_=layer_r_name, gpkg_path=self.gpkg_path)

        layer_t_name = f'__Limit_{self.dic_prj["dems"][1]["type"]}__'
        layer_ = QgsVectorLayer(f'polygon?crs={self.crs_epsg}&index=yes', layer_t_name, "memory")
        pr_ = layer_.dataProvider()
        pr_.addAttributes(QgsFields())
        layer_.updateFields()

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        options.layerName = layer_t_name
        QgsVectorFileWriter.writeAsVectorFormat(
            layer=layer_,
            fileName=self.gpkg_path,
            options=options)
        self.get_gpkg_layer(prefix_=layer_t_name, gpkg_path=self.gpkg_path)

        layer_i_name = self.intersection_name
        layer_ = QgsVectorLayer(f'polygon?crs={self.crs_epsg}&index=yes', layer_i_name, "memory")
        pr_ = layer_.dataProvider()
        schema_ = QgsFields()
        schema_.append(QgsField('AREA', QVariant.Double))
        pr_.addAttributes(schema_)
        layer_.updateFields()

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        options.layerName = layer_i_name
        QgsVectorFileWriter.writeAsVectorFormat(
            layer=layer_,
            fileName=self.gpkg_path,
            options=options)
        self.get_gpkg_layer(prefix_=layer_i_name, gpkg_path=self.gpkg_path)

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
        self.node_group = QgsProject.instance().layerTreeRoot().findGroup('__MDE_PA__')
        if not self.node_group:
            self.node_group = QgsProject.instance().layerTreeRoot().insertGroup(0, '__MDE_PA__')

        conn = None
        layer_ = None
        # cur = None
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
                # self.node_group.insertLayer(layer_, 0)
        self.gpkg_close_conn(conn)

        return layer_

    def define_intersection(self):
        mss_ = f'=======================================\n'
        mss_ += f'DEFININDO POLÍGONOS'
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
        mss_ = f'=======================================\n'
        mss_ += f'DEFININDO ELEMENTOS DE MORFOLOGIA DO TERRENO - {self.dic_prj['dems'][key_]['type']}'
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
                print(vet_)
                ext_sum += vet_[-1]
        print('Extensão Total da Amostra:', ext_sum)
        # print('dic_match', self.dic_match)
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
        mss_ = f'=======================================\n'
        mss_ += f'DEFININDO BUFFERS'
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
                dic_vectors[scale_][class_] = []
                for count_ in dic_values[scale_][class_]:
                    if not dic_values[scale_][class_][count_].get('outlier',False):
                        dic_vectors[scale_][class_].append(dic_values[scale_][class_][count_]['dm_h'])
        return dic_vectors

    def check_outliers(self, dic_values):
        dic_stats = self.update_dic_vectors(dic_values)
        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                quant_ = statistics.quantiles(data=dic_stats[scale_][class_])
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
        mss_ = f'=======================================\n'
        mss_ += f'CALCULANDO PEC PLANIMÉTRICO'
        self.log_message(mss_, 'INFO')
        dic_vectors = self.update_dic_vectors(dic_values)
        for scale_ in dic_vectors:
            for class_ in dic_vectors[scale_]:
                pec_h = round(scale_ * self.dic_pec_mm['H'][class_]['pec'], 2)
                ep_h = round(scale_ * self.dic_pec_mm['H'][class_]['ep'], 2)
                list_ = dic_vectors[scale_][class_]
                perc_pec_ = self.perc_pec(vet_=list_, pec_=pec_h)
                if perc_pec_ >= 0.90:
                    str_ = f"1:{scale_}.000-{class_}= {round(perc_pec_ * 100)}% < {pec_h} PEC - OK    , "
                else:
                    str_ = f"1:{scale_}.000-{class_}= {round(perc_pec_ * 100)}% < {pec_h} PEC - FALHOU,"

                rms_ = self.rms(list_)
                if rms_ <= ep_h:
                    str_ += f'| {round(rms_, 2)} < {ep_h} EP -     OK, {len(list_)}'

                else:
                    str_ += f'| {round(rms_, 2)} > {ep_h} EP - FALHOU, {len(list_)}'
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

    # def create_buffers(self):
    #     list_layer_buffer = self.create_buffers_layer()
    #     layer_bt.startEditing()
    #     layer_br.startEditing()
    #     list_scale = self.get_list_scale()
    #     for tag_ in self.dic_match:
    #         layer_r_name = f'__{tag_}_Z_{self.dic_prj["dems"][0]["type"]}__'
    #         layer_r = self.get_gpkg_layer(layer_r_name)
    #         layer_t_name = f'__{tag_}_Z_{self.dic_prj["dems"][1]["type"]}__'
    #         layer_t = self.get_gpkg_layer(layer_t_name)
    #
    #         for vet_ in self.dic_match[tag_]:
    #             id_r = vet_[0]
    #             feat_r = layer_r.getFeature(id_r)
    #             geom_r = feat_r.geometry()
    #             id_t = vet_[1]
    #             feat_t = layer_t.getFeature(id_t)
    #             geom_t = feat_t.geometry()
    #             for scale_ in list_scale:
    #                 for class_ in self.dic_pec_mm['H']:
    #                     pec_h = scale_ * self.dic_pec_mm['H'][class_]['pec']
    #                     ep_h = scale_ * self.dic_pec_mm['H'][class_]['ep']
    #                     geom_bt = geom_t.buffer(pec_h, 20)
    #                     feat_bt = QgsFeature()
    #                     feat_bt.setGeometry(geom_bt)
    #
    #                     geom_br = geom_r.buffer(pec_h, 20)
    #                     feat_br = QgsFeature()
    #                     feat_br.setGeometry(geom_br)
    #
    #                     geom_i = geom_bt.intersection(geom_br)
    #                     # CÁLCULO DO DM HORIZONTAL
    #                     dm_ = math.pi * pec_h * (geom_br.area() - geom_i.area()) / geom_bt.area()
    #                     feat_bt.setAttributes([
    #                         len(layer_bt) + 1,
    #                         feat_r.id(),
    #                         scale_,
    #                         class_,
    #                         layer_r_name,
    #                         layer_t_name,
    #                         geom_bt.area(),
    #                         geom_br.area(),
    #                         geom_i.area(),
    #                             dm_,
    #                             False,
    #                         0,
    #                         0,
    #                         0,
    #                         0,
    #                         False,
    #                         0,
    #                         0
    #                     ])
    #                     layer_bt.addFeature(feat_bt)
    #                     layer_bt.commitChanges(stopEditing=False)
    #                     layer_bt.triggerRepaint()
    #
    #                     feat_br.setAttributes([
    #                         len(layer_br) + 1,
    #                         feat_r.id(),
    #                         scale_,
    #                         class_
    #                     ])
    #                     layer_br.addFeature(feat_br)
    #                     layer_br.commitChanges(stopEditing=False)
    #                     layer_br.triggerRepaint()
    #     layer_bt.commitChanges()
    #     layer_br.commitChanges()

    def update_bar(self, dic_):
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
                self.log_message(f"Buffer - {dic_['error']}", level='ERROR')
            else:
                self.log_message(f"{self.dic_prj['dems'][dic_['key']]['type']} {dic_['value']} - {dic_['error']}", level='ERROR')
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
            self.log_message(f"{type_} {dic_['value']} - {dic_['msg']}")
            # print('dic_:', dic_)
            if 'feat' in dic_:
                if dic_['value'] == 6:
                    layer_name = f'__Limit_{self.dic_prj["dems"][key_]["type"]}__'
                    layer = QgsProject.instance().mapLayersByName(layer_name)[0]
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
            self.check_outliers(dic_['dic_values'])
            self.calc_pec(dic_['dic_values'])

        elif 'end' in dic_:
            palette.setColor(QPalette.Highlight, QColor(Qt.darkGreen))
            prog_bar.setValue(dic_['end'])
            prog_bar.setFormat(dic_['msg'])
            prog_bar.setPalette(palette)
            # self.db.commit_()

    def open_settings(self):
        if not self.settings_dlg:
            self.settings_dlg = SettingsDlg(main=self.parent, parent=self)
        self.settings_dlg.show()


class QLabelEvent(QLabel):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        self.end = None
        self.event_button = None

    def mousePressEvent(self, event):
        print('mousePressEvent', event.button(), event.type(), QEvent.User, event.type() == QEvent.User)
        if event.type() == 2:  # QEvent.User:
            self.event_button = event.button()

    def mouseMoveEvent(self, event):
        print('mouseMoveEvent', event, 'event_button=', self.event_button)

        if self.end:
            dx = event.x() - self.end.x()
            dy = event.y() - self.end.y()
            if self.event_button == 1:
                self.parent.rot_inc_zoom_pan(dx=0.1 * dx, dy=-0.1 * dy)
            elif self.event_button == 2:
                self.parent.rot_inc_zoom_pan(di=-0.1 * dy)
        self.end = event.pos()

    def mouseReleaseEvent(self, event):
        print('mouseReleaseEvent')
        self.end = None
        self.event_button = None
        # print(self.end, self.event_button)

    def wheelEvent(self, event):
        # print(event.angleDelta().y())
        self.parent.rot_inc_zoom_pan(dz=-event.angleDelta().y() / 120)
