# -*- coding: utf-8 -*-
import datetime
import json
import os
import sqlite3
from queue import Queue
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from sys import prefix

from osgeo import ogr
from qgis.PyQt.QtCore import QSettings, Qt, QSize, QTranslator, QCoreApplication, QEvent, QThreadPool, QDateTime
from qgis.PyQt.QtGui import QPixmap, QIcon, QFont, QPalette, QColor, QTextCharFormat, QBrush, QTextOption
from qgis.PyQt.QtWidgets import (QAction, QScrollArea, QGridLayout, QPushButton, QLabel, QWidget, QSizePolicy,
                                 QSpacerItem, QDockWidget, QSplitter, QComboBox, QLineEdit, QDialog, QFrame, QCheckBox,
                                 QHBoxLayout, QVBoxLayout, QFileDialog, QTableWidget,
                                 QProgressBar, QDateEdit, QWidget, QVBoxLayout, QPushButton, QPlainTextEdit)
from qgis._core import QgsVectorFileWriter, QgsWkbTypes, QgsCoordinateTransformContext, QgsCoordinateReferenceSystem, \
    QgsFeature
from qgis.core import QgsVectorLayer, QgsFields, QgsField, QgsProject, QgsMapLayerProxyModel
from qgis.gui import QgsAdvancedDigitizingDockWidget, QgsMapLayerComboBox
from .mod_aux_tools import AuxTools, Obs2, Logger
from .mod_login import Database
from .mod_mde_pa_threads import Worker

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

        self.dock1 = QgsAdvancedDigitizingDockWidget(self.iface.mapCanvas())
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

        self.dic_prj = {'path': '',
                        'dems': {
                            0 : {
                                'type': 'Referencia',
                                'obj_cbx':None,
                                'obj_pb':None,
                                'obj_prog_bar' : None,
                                'geom_status' : False},
                            1 : {
                                'type': 'Teste',
                                'obj_cbx':None,
                                'obj_pb':None,
                                'obj_prog_bar' : None,
                                'geom_status' : False},
                        },
                        'matchs' : {
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
        self.srid = None
        self.crs_epsg = None
        self.gpkg_path = ''
        self.workers = None
        self.task_queue = None
        self.layer_aux = None
        self.folder_out_path = ''
        self.first_start_cb = True
        self.dic_dbs = {}
        self.filter_dlg = None
        self.db = None
        self.count_commit = 0
        self.list_add_tool = ['...', 'add_folder', 'add_files', 'clear']
        self.list_data = []
        self.dic_mime_type = {'surfaces': ['.tif'], 'point_clouds': ['.las', '.laz']}
        self.dic_obj = {}
        self.dic_epsg = {
            '-25(S)': 31985,
            '-24(S)': 31984,
            '-23(S)': 31983,
            '-22(S)': 31982,
            '-21(S)': 31981,
            '-20(S)': 31980,
            '-19(S)': 31979,
            '-18(S)': 31978,
            '18(N)': 31972,
            '19(N)': 31973,
            '20(N)': 31974,
            '21(N)': 31975,
            '22(N)': 31976,
            '23(N)': 6210,
            '24(N)': 6211}

        self.max_threads = 3  # Limit to 3 concurrent tasks
        self.thread_pool = QThreadPool.globalInstance()  # Use QThreadPool for task management
        self.thread_pool.setMaxThreadCount(self.max_threads)

        self.task_queue = Queue()  # Task queue
        self.threads_running = 0  # Track active threads
        self.active_workers = {}  # Keep track of active workers

        # Save reference to the QGIS interface
        self.iface = iface
        self.parent = parent
        self.main = main
        name_ = self.main.name_.replace(' ', '_')
        self.setObjectName(f'Wd_{name_}')
        self.dic_debugger = {
            'user': 'adria',
            'log_state': True,
            'plugin_name': f'Wd_{name_}'
        }
        if os.getlogin() == self.dic_debugger['user']:
            self.iface.actionShowPythonDialog().trigger()
            self.log = Logger(self.main.name_)

        self.canvas = self.iface.mapCanvas()
        self.aux_tools = AuxTools(parent=self)
        lg = self.create_layout()
        self.setLayout(lg)

    def create_layout(self):
        gl_1 = QGridLayout()
        gl_1.setContentsMargins(0, 0, 0, 0)
        gl_1.setSpacing(1)
        spt_left = QSplitter(Qt.Horizontal)

        gl_1.addWidget(spt_left, 0, 0)

        wd_tool = QWidget()
        sp_ = QSizePolicy()
        sp_.setHorizontalPolicy(QSizePolicy.Minimum)
        sp_.setHorizontalStretch(0)
        sp_.setVerticalPolicy(QSizePolicy.Expanding)
        wd_tool.setSizePolicy(sp_)
        gl_tool = QGridLayout()
        gl_tool.setContentsMargins(0, 0, 0, 0)
        wd_tool.setLayout(gl_tool)
        spt_left.addWidget(wd_tool)

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
            lb_title_ = QLabel( f'Modelo de {self.dic_prj['dems'][key_]['type']}:')
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

        # self.pb_define_ref = QPushButton('...')
        # self.pb_define_ref.setMaximumWidth(40)
        # gl_tool.addWidget(self.pb_define_ref, r_, 2)

        # r_ += 1
        # self.lb_title_test = QLabel('Modelo de Teste:')
        # gl_tool.addWidget(self.lb_title_test, r_, 0)
        # self.pb_inf_test = QPushButton('info')
        # self.pb_inf_test.setMaximumWidth(40)
        # gl_tool.addWidget(self.pb_inf_test, r_, 2)
        # r_ += 1
        # self.cbx_model_test = QgsMapLayerComboBox()
        # self.cbx_model_test.setFilters(QgsMapLayerProxyModel.RasterLayer)
        # gl_tool.addWidget(self.cbx_model_test, r_, 0, 1, 3)
        # # self.pb_define_test = QPushButton('...')
        # # self.pb_define_test.setMaximumWidth(40)
        # # gl_tool.addWidget(self.pb_define_test, r_, 2)

        r_ += 1
        self.pb_proc = QPushButton('Avaliar')
        gl_tool.addWidget(self.pb_proc, r_, 1, 1, 1)
        self.pb_config = QPushButton('Config')
        self.pb_config.setEnabled(False)
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

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setMinimumHeight(400)
        scroll_area.setLayout(gl_1)
        lg_sa = QGridLayout()
        lg_sa.setContentsMargins(0, 0, 0, 0)
        lg_sa.setSpacing(0)
        lg_sa.addWidget(scroll_area)

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
        log_path = os.path.join(source_folder, f'{dic_st["name"]}{dic_st["files"]['log']}')

        with open(log_path, "a") as file:
            file.write(log_entry)


    def log_mde_inf(self, key_: int):
        if self.dic_prj['dems'][key_]['obj_cbx']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()

            mss_ = f'=======================================\n'
            mss_ += f'  INFORMAÇÕES DO MODELO DE {self.dic_prj['dems'][key_]['type'].upper()}\n'
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
            self.log_message(f'MODELO DE {self.dic_prj['dems'][key_]['type']} NÃO DEFINIDO', 'ERROR')

            
    # def clear_log(self):
    #     """
    #     Clears all text from the log widget.
    #     """
    #     self.pte_log.clear()

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

    # def get_files(self):
    #     print('get_files')
    #     dir_ = self.aux_tools.get_(key_='dir')
    #     filter = "Point Cloud (*.las *.laz) ;; MDT (*.tif)"
    #     list_path, _ = QFileDialog.getOpenFileNames(None, 'Arquivos', dir_, filter)
    #     list_out = []
    #     if list_path:
    #         for i, path_ in enumerate(list_path):
    #             file_dir = os.path.dirname(path_)
    #             file_name = os.path.basename(path_)
    #             if i == 0:
    #                 self.aux_tools.save_(key_='dir', value_=file_dir)
    #             list_out.append([file_name, file_dir])
    #     return list_out

    # def tw_cell_clicked(self, r_, c_):
    #     print('tw_cell_clicked', r_, c_)
    #     if c_ == 1:
    #         rm_ = self.list_data.pop(r_)
    #         print('removio - ', rm_[0])
    #     self.update_tw()

    # def get_folder_out(self):
    #     print('get_folder_out')
    #     self.folder_out_path = self.get_folder('dir_out')
    #     if self.folder_out_path and os.path.exists(self.folder_out_path):
    #         folder2 = os.path.basename(self.folder_out_path)
    #         folder1 = os.path.basename(os.path.dirname(self.folder_out_path))
    #         self.lb_path.setText(f'.../{folder1}/{folder2}')


    def task_done(self, key_):
        """ Called when a thread finishes processing, allowing another to start """
        self.threads_running -= 1  # Reduce active thread count
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
            layer_i = self.get_gpkg_layer(prefix_= '__Limit_Intersecao__')[0]


            for feat_0 in layer_0[0].getFeatures():
                geom_0 = feat_0.geometry()
                for feat_1 in layer_1[0].getFeatures():
                    geom_1 = feat_1.geometry()
                    intersec_ = geom_0.intersection(geom_1)
                    feat_i = QgsFeature()
                    feat_i.setGeometry(intersec_)
                    count = layer_i.featureCount()
                    feat_i.setAttributes([count + 1])
                    layer_i.startEditing()
                    layer_i.addFeature(feat_i)
                    layer_i.commitChanges()
                    layer_i.updateExtents()
                    layer_i.triggerRepaint()
            mss_ = f'AREA DE INTERSEÇÃO DOS MDEs DEFINIDA\n'
            mss_ += f'=======================================\n'
            self.log_message(mss_, 'INFO')
            self.define_morphology()


    def create_gpkg(self):
        dic_st = self.dic_prj["standard"]
        source_folder = self.dic_prj['path']
        self.gpkg_path = os.path.join(source_folder, f'{dic_st["name"]}{dic_st["files"]['prj']}')
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

        layer_i_name = '__Limit_Intersecao__'
        layer_ = QgsVectorLayer(f'polygon?crs={self.crs_epsg}&index=yes', layer_i_name, "memory")
        pr_ = layer_.dataProvider()
        pr_.addAttributes(QgsFields())
        layer_.updateFields()

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        options.layerName = layer_i_name
        QgsVectorFileWriter.writeAsVectorFormat(
            layer=layer_,
            fileName=self.gpkg_path,
            options=options)
        self.get_gpkg_layer(prefix_=layer_i_name, gpkg_path=self.gpkg_path)


    def get_gpkg_layer(self, prefix_='', gpkg_path='', show=True):
        def gpkg_conn(gpkg_path_):
            conn_ = sqlite3.connect(gpkg_path_)  # , isolation_level=None)
            conn_.row_factory = sqlite3.Row
            conn_.enable_load_extension(True)
            conn_.load_extension('mod_spatialite')
            conn_.execute('SELECT load_extension("mod_spatialite")')
            conn_.execute('pragma journal_mode=wal')
            # cur_ = conn_.cursor()
            return conn_

        def gpkg_close_conn(conn_=None, cur_=None):
            print('close conn')
            if conn_:
                conn_.close()
            if cur_:
                cur_.close()

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
                return layer_

            conn = gpkg_conn(gpkg_path)
            uri_ = f'{gpkg_path}|layername={prefix_}'

            layer_ = QgsVectorLayer(uri_, prefix_, 'ogr')
            conn.commit()
            style_path = os.path.join(plugin_path, r'styles', f'{prefix_}.qml')
            layer_.loadNamedStyle(style_path)
            layer_.triggerRepaint()
            if show:
                QgsProject.instance().addMapLayer(layer_, False)
                self.node_group.addLayer(layer_)
        gpkg_close_conn(conn)

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

    def define_morphology(self):
        mss_ = f'=======================================\n'
        mss_ += f'DEFININDO ELEMENTOS DE MORFOLOGIA DO TERRENO'
        self.log_message(mss_, 'INFO')
        for key_ in self.dic_prj['dems']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()
            dic_ = {
                'file_path': layer_.source(),
                'step': 'morphology',
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

    def parse_list(self):
        """ Enqueue tasks and start only 3 at a time """
        for key_ in self.dic_prj['dems']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()
            dic_ = {
                'file_path': layer_.source(),
                'srid': self.dic_epsg.get(self.cb_epsg.currentText(), '-'),
                'log': self.log,
                'vrt': self.layer_aux,
                'parent': self,
                'main': self.main
            }

            # Add tasks to queue
            self.task_queue.put((key_, dic_))

        # Start up to max_threads tasks
        while self.threads_running < self.max_threads and not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

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
            self.log_message(f"{self.dic_prj['dems'][dic_['key']]['type']} {dic_['value']} - {dic_['msg']}")
            # self.count_commit += 1
            # if self.count_commit % 100 == 0:
            #     self.db.commit_()
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
            elif 'layer' in dic_:
                # print("dic_['layer']['gpkg']=", dic_['layer']['gpkg'])
                if isinstance(dic_['layer']['gpkg'], str):
                    datasource = ogr.Open(dic_['layer']['gpkg'])
                    # for i in range(datasource.GetLayerCount()):
                    #     layer = datasource.GetLayerByIndex(i)
                    #     print(f"- {layer.GetName()}")
                    layer_prefix = datasource.GetLayerByIndex(0).GetName()
                    layer = self.get_gpkg_layer(prefix_=layer_prefix, gpkg_path=dic_['layer']['gpkg'], show=False)
                else:
                    layer = dic_['layer']['gpkg']
                layer_name = f'__{dic_['layer']['type']}_{self.dic_prj["dems"][key_]["type"]}__'
                options = QgsVectorFileWriter.SaveVectorOptions()
                options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
                options.layerName = layer_name
                QgsVectorFileWriter.writeAsVectorFormat(
                    layer=layer,
                    fileName=self.gpkg_path,
                    options=options)
                self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)

        elif 'end' in dic_:
            palette.setColor(QPalette.Highlight, QColor(Qt.darkGreen))
            prog_bar.setValue(dic_['end'])
            prog_bar.setFormat(dic_['msg'])
            prog_bar.setPalette(palette)
            # self.db.commit_()


class SettingsDbDlg(QDialog):
    """DB Form"""

    def __init__(self, main=None, parent=None):
        super().__init__(parent)
        self.setObjectName('SettingsDbDlg')
        self.main = main
        self.parent = parent
        # self.parent_dlg = parent
        self.setWindowTitle('Informacoes do Banco de Dados')
        self.setWindowIcon(QIcon(":/plugins/mod_cut_pan/icons/icon_cut.png"))
        self.dic_param = None
        self.icon_eye = None
        self.icon_eyex = None
        self.aux_tools = AuxTools(parent=self)
        geom = self.aux_tools.get_geometry()
        if geom:
            self.restoreGeometry(geom)
        else:
            x_, y_, w_, h_ = 100, 100, 300, 300
            self.setGeometry(x_, y_, w_, h_)
        dlgLayout = self.create_layout_db()
        self.setLayout(dlgLayout)
        self.current_idx = 0
        self.db = None

    def create_layout_db(self):
        print("create_layout_db")
        r_ = 0
        gl_ = QGridLayout()

        self.cb_name = QComboBox(self)
        # self.cb_name.
        self.update_cb_name()
        gl_.addWidget(self.cb_name, r_, 2, 1, 2)

        self.dic_param = \
            {
                'conn': {
                    'name': {
                        'value': 'api_qgis_teste',
                        'label': 'Nome Conexão:',
                    },
                    'host': {
                        'value': 'vmdbtst01.topo.local',
                        'label': 'HOST:',
                    },
                    'port': {
                        'value': '5433',
                        'label': 'PORTA:',
                    },
                    'db': {
                        'value': 'api_qgis_teste',
                        'label': 'BANCO:',
                    },
                    'user': {
                        'value': 'django',
                        'label': 'Usuário:',
                    },
                    'pass': {
                        'value': '',
                        'label': 'Senha:',
                    },
                    'plugin_version': self.main.plugin_version(),
                },

                'sch_metapoly': {
                    'alias': ['metapoly'],
                    'label': 'Tabela Metapoly',
                    'tab': {
                        'alias': ['metapoly']},
                    'fields': {
                        'fld_name': {
                            'alias': ['name'],
                            'label': 'Campo: Nome'},
                        'fld_path': {
                            'alias': ['path'],
                            'label': 'Campo: Caminho'},
                        'fld_type': {
                            'alias': ['type'],
                            'label': 'Campo: Tipo'},
                        'fld_date': {
                            'alias': ['date'],
                            'label': 'Campo: Data'},
                        'fld_valid': {
                            'alias': ['valid'],
                            'label': 'Campo: Validade'},
                        'fld_srid': {
                            'alias': ['srid'],
                            'label': 'Campo: SRID'},

                    },
                },
            }

        for i, tag_ in enumerate(self.dic_param['conn']):
            if i != 0:
                r_ += 1
            if tag_ == 'plugin_version':
                continue
            label_ = self.dic_param['conn'][tag_]['label']
            lb_ = QLabel(label_)
            lb_.setObjectName('lb_' + tag_.lower())
            # self.dic_obj.update({'lb_' + name_.lower(): lb_})
            gl_.addWidget(lb_, r_, 0, 1, 1)
            le_ = QLineEdit(self)
            le_.setText(self.dic_param['conn'][tag_]['value'])
            le_.setObjectName('le_' + tag_.lower())
            gl_.addWidget(le_, r_, 1, 1, 1)
            if tag_ == 'pass':
                le_.setEchoMode(QLineEdit.Password)
                icon_path_eye = os.path.join(plugin_path, 'icons/icon_eye.png')
                self.icon_eye = QIcon(icon_path_eye)
                self.action_pass = le_.addAction(self.icon_eye, QLineEdit.TrailingPosition)
            elif tag_ == 'pass':
                le_.textChanged.connect(self.check_exists_name)
            r_ += 1

        r_ += 1
        self.pb_test = QPushButton(self)
        self.pb_test.setIcon(self.parent.icon_conx)
        gl_.addWidget(self.pb_test, r_, 3, 1, 1)

        r_ += 1
        frame1 = QFrame(self)
        frame1.setFrameShape(QFrame.HLine)
        gl_.addWidget(frame1, r_, 1, 1, 3)

        for i, item_i in enumerate(self.dic_param):
            if item_i[:4] == 'sch_':
                r_ += 1
                lb_ = QLabel(self.dic_param[item_i]['label'])
                lb_.setFont(QFont('MS Shell Dlg 2', 14))
                lb_.setObjectName(item_i.replace('sch', 'lb'))
                lb_.setMinimumWidth(25)
                gl_.addWidget(lb_, r_, 0, 1, 2)
                if 'chk' in self.dic_param[item_i]:
                    chk_ = QCheckBox(self.dic_param[item_i]['chk']['label'])
                    chk_.setCheckState(self.dic_param[item_i]['chk']['status'])
                    chk_.setObjectName('chk_' + item_i.lower())
                    chk_.setTristate(False)
                    gl_.addWidget(chk_, r_, 2, 1, 1)

                r_ += 1
                lb_ = QLabel('Esquema:')
                lb_.setObjectName('lb_' + item_i.lower())
                # self.dic_obj.update({'lb_' + name_.lower(): lb_})
                gl_.addWidget(lb_, r_, 0)
                cbx_sch = QComboBox(self)
                cbx_sch.setMinimumWidth(25)
                cbx_name = 'cbx_' + item_i.lower()
                cbx_sch.setObjectName(cbx_name)
                # self.update_cbx(cbx_=cbx_sch, alias=self.dic_param[item_i]['alias'])
                gl_.addWidget(cbx_sch, r_, 1)

                r_ += 1
                lb_ = QLabel('Tabela:')
                lb_.setObjectName('lb_' + item_i.lower().replace('sch', 'tab'))
                # self.dic_obj.update({'lb_' + name_.lower(): lb_})
                gl_.addWidget(lb_, r_, 0)
                cbx_tab = QComboBox(self)
                cbx_tab.setMinimumWidth(25)
                cbx_name = 'cbx_' + item_i.lower().replace('sch', 'tab')
                cbx_tab.setObjectName(cbx_name)
                # self.update_cbx(sch_=cbx_sch, cbx_=cbx_tab, alias=self.dic_param[item_i]['tab']['alias'])
                gl_.addWidget(cbx_tab, r_, 1)

                cbx_sch.currentIndexChanged.connect(partial(self.update_cbx,
                                                            sch_=cbx_sch,
                                                            cbx_=cbx_tab,
                                                            alias=self.dic_param[item_i]['tab']['alias']))

                for j, item_j in enumerate(self.dic_param[item_i]['fields']):
                    r_ += 1
                    if 'status' in self.dic_param[item_i]['fields'][item_j]:
                        cb_ = QCheckBox(self.dic_param[item_i]['fields'][item_j]['label'], self)
                        cb_.setObjectName(item_j.replace('fld', 'cb'))
                        cb_.setChecked(bool(self.dic_param[item_i]['fields'][item_j]['status']))
                        gl_.addWidget(cb_, r_, 0, 1, 1)
                        if self.dic_param[item_i]['fields'][item_j]['status']:
                            cb_ = self.findChild(QCheckBox, item_j.replace('fld', 'cb'))
                            cb_.setChecked(True)
                    else:
                        lb_ = QLabel(self.dic_param[item_i]['fields'][item_j]['label'])
                        lb_.setObjectName('lb_' + item_j.lower())
                    # self.dic_obj.update({'lb_' + name_.lower(): lb_})
                    gl_.addWidget(lb_, r_, 0)
                    cbx_ = QComboBox(self)
                    cbx_.setMinimumWidth(25)
                    cbx_name = 'cbx_' + item_j.lower()
                    cbx_.setObjectName(cbx_name)
                    # self.update_cbx(tab_=cbx_tab, sch_=cbx_sch, cbx_=cbx_,
                    #                 alias=self.dic_param[item_i]['fields'][item_j]['alias'])
                    gl_.addWidget(cbx_, r_, 1)
                    cbx_tab.currentIndexChanged.connect(partial(self.update_cbx,
                                                                tab_=cbx_tab,
                                                                sch_=cbx_sch,
                                                                cbx_=cbx_,
                                                                alias=self.dic_param[item_i]['fields'][item_j][
                                                                    'alias']))

        r_ += 1
        frame2 = QFrame(self)
        frame2.setFrameShape(QFrame.HLine)
        gl_.addWidget(frame2, r_, 1, 1, 3)

        r_ += 1
        hl_ = QHBoxLayout()

        self.pb_exp = QPushButton("Exportar", self)
        self.pb_exp.setEnabled(True)
        hl_.addWidget(self.pb_exp)

        self.pb_imp = QPushButton("Importar", self)
        # self.pb_imp.setEnabled(False)
        hl_.addWidget(self.pb_imp)

        self.pb_save = QPushButton("Salvar", self)
        # self.pb_save.setEnabled(False)
        hl_.addWidget(self.pb_save)

        self.pb_remove = QPushButton("Remover", self)
        # self.pb_remove.setEnabled(False)
        hl_.addWidget(self.pb_remove)
        gl_.addLayout(hl_, r_, 0, 1, 4)

        self.trigger_actions_db()

        r_ += 1
        self.lb_topo_logo = QLabel()
        self.lb_topo_logo.setMinimumSize(QSize(100, 30))
        self.lb_topo_logo.setMaximumSize(QSize(100, 30))
        self.lb_topo_logo.setText("")
        icon_path = os.path.join(plugin_path, 'icons/topo_logo.png')
        self.lb_topo_logo.setPixmap(QPixmap(icon_path))
        self.lb_topo_logo.setScaledContents(True)
        self.lb_topo_logo.setAlignment(Qt.AlignBottom | Qt.AlignLeading | Qt.AlignLeft)
        self.lb_topo_logo.setObjectName("lb_topo_logo")
        gl_.addWidget(self.lb_topo_logo, r_, 0, 1, 1)

        base_widget = QWidget()
        base_widget.setLayout(gl_)

        sla_ = QScrollArea(self)
        # gl_.addWidget(sla_)
        # sla_.setLayout(gl_)
        sla_.setWidgetResizable(True)
        sla_.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        sla_.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        sla_.setWidget(base_widget)

        vl_ = QVBoxLayout(self)
        vl_.addWidget(sla_)

        return vl_

    def trigger_actions_db(self):
        print("trigger_actions_db")
        self.pb_save.clicked.connect(self.append_update_db)
        self.pb_test.clicked.connect(self.test_connection)
        self.pb_remove.clicked.connect(self.remove_db)
        # self.le_name.textChanged.connect(self.check_exists_name)
        self.cb_name.activated.connect(self.fill_inf)
        self.action_pass.triggered.connect(self.toggle_visibility)
        # self.cb_use_file.toggled.connect(self.enable_priority)
        # self.cb_use_vinc.toggled.connect(self.enable_vinc)
        self.pb_exp.clicked.connect(self.export_db_inf)
        self.pb_imp.clicked.connect(self.import_db_inf)
        # self.cb_pop_media.toggled.connect(self.enable_media)

    def toggle_visibility(self):
        le_pass = self.findChild(QLineEdit, 'le_pass')
        if le_pass.echoMode() == QLineEdit.Normal:
            le_pass.setEchoMode(QLineEdit.Password)
            if not self.icon_eye:
                icon_path_eye = os.path.join(plugin_path, 'icons/icon_eye.png')
                self.icon_eye = QIcon(icon_path_eye)
            self.action_pass.setIcon(self.icon_eye)
        else:
            le_pass.setEchoMode(QLineEdit.Normal)
            if not self.icon_eyex:
                icon_path_eyex = os.path.join(plugin_path, 'icons/icon_eyex.png')
                self.icon_eyex = QIcon(icon_path_eyex)
            self.action_pass.setIcon(self.icon_eyex)

    def fill_inf(self):
        print('fill_inf')
        self.pb_remove.setEnabled(True)
        # conn_name = self.cb_name.currentText()
        conn_name = self.cb_name.currentText()
        if conn_name == '...':
            self.clear_values()
            self.db = None
            return
        elif not conn_name:
            return
        dic_base = self.dic_param
        if conn_name in self.parent.dic_dbs:
            dic_parent = self.parent.dic_dbs[conn_name]
            for i, item_i in enumerate(dic_base):
                if item_i not in dic_parent:
                    dic_parent[item_i] = dic_base[item_i]
                    continue
                for j, item_j in enumerate(dic_base[item_i]):
                    if item_j not in dic_parent[item_i] or item_j == 'plugin_version':
                        dic_parent[item_i][item_j] = dic_base[item_i][item_j]
                        continue
                    for k, item_k in enumerate(dic_base[item_i][item_j]):
                        if item_k not in dic_parent[item_i][item_j]:
                            dic_parent[item_i][item_j][item_k] = dic_base[item_i][item_j][item_k]

            aux_list_dic_i = list(dic_parent)
            for i, item_i in enumerate(aux_list_dic_i):
                if item_i not in dic_base:
                    dic_parent.pop(item_i)
                    continue
                aux_list_dic_j = list(dic_parent[item_i])
                for j, item_j in enumerate(aux_list_dic_j):
                    if item_j not in dic_base[item_i]:
                        self.dic_param[item_i].pop(item_j)
                        continue
                    elif item_j == 'plugin_version':
                        continue
                    aux_list_dic_k = list(dic_parent[item_i][item_j])
                    for k, item_k in enumerate(aux_list_dic_k):
                        if item_k and item_k not in dic_base[item_i][item_j]:
                            self.dic_param[item_i][item_j].pop(item_k)
            self.dic_param = dic_parent
        for tag_1 in self.dic_param['conn']:
            # print('le_name=', le_name)
            if tag_1 == 'plugin_version':
                continue
            le_name = f'le_{tag_1}'
            le_obj = self.findChild(QLineEdit, le_name)
            le_obj.setText(self.dic_param['conn'][tag_1]['value'])

        if not self.db:
            self.create_conn()
            if self.db and not self.db.is_connected():
                return
            elif not self.db:
                return
        else:
            if self.db.conn_name != self.dic_param['conn']['name']['value']:
                self.db.close()
                self.db = self.create_conn()
                if not self.db:
                    return

        for tag_0 in self.dic_param:
            if tag_0 == 'conn':
                continue

            # for tag_1 in self.dic_param[tag_0]:
            cbx_name = 'cbx_' + tag_0.lower()
            cbx_sch = self.findChild(QComboBox, cbx_name)
            cbx_sch.clear()
            self.update_cbx(cbx_=cbx_sch, alias=self.dic_param[tag_0]['alias'])
            if 'chk' in self.dic_param[tag_0]:
                # chk_ = QCheckBox(self.dic_param[tag_0]['chk']['label'])
                # print('chk', self.dic_param[tag_0]['chk']['status'])
                chk_name = 'chk_' + tag_0.lower()
                chk_obj = self.findChild(QCheckBox, chk_name)
                chk_obj.setCheckState(self.dic_param[tag_0]['chk']['status'])

            cbx_name = cbx_name.replace('sch', 'tab')
            cbx_tab = self.findChild(QComboBox, cbx_name)
            cbx_tab.clear()
            self.update_cbx(sch_=cbx_sch, cbx_=cbx_tab, alias=self.dic_param[tag_0]['tab']['alias'])

            for tag_1 in self.dic_param[tag_0]['fields']:
                cbx_name = 'cbx_' + tag_1.lower()
                cbx_field = self.findChild(QComboBox, cbx_name)
                cbx_field.clear()
                self.update_cbx(tab_=cbx_tab, sch_=cbx_sch, cbx_=cbx_field,
                                alias=self.dic_param[tag_0]['fields'][tag_1]['alias'])

    def update_cb_name(self, cur_=''):
        print('update_cb_name', cur_)
        self.cb_name.setEnabled(True)
        self.cb_name.clear()
        list_ = list(self.parent.dic_dbs)
        if cur_ and cur_ not in list_:
            list_.append(cur_)

        self.cb_name.addItems(['...'] + sorted(list_))
        if cur_:
            self.cb_name.setCurrentText(cur_)
            # self.fill_inf()

    def clear_values(self):
        print('clear_values')
        for tag_0 in self.dic_param:
            if tag_0 == 'conn':
                for tag_1 in self.dic_param['conn']:
                    # print('le_name=', le_name)
                    if tag_1 == 'plugin_version':
                        continue
                    le_name = f'le_{tag_1}'
                    le_obj = self.findChild(QLineEdit, le_name)
                    le_obj.setText('')
            else:
                # for tag_1 in self.dic_param[tag_0]:
                cbx_name = 'cbx_' + tag_0.lower()
                cbx_obj = self.findChild(QComboBox, cbx_name)
                cbx_obj.clear()
                cbx_name = cbx_name.replace('sch', 'tab')
                cbx_obj = self.findChild(QComboBox, cbx_name)
                cbx_obj.clear()
                for field_ in self.dic_param[tag_0]['fields']:
                    cbx_name = 'cbx_' + field_.lower()
                    cbx_obj = self.findChild(QComboBox, cbx_name)
                    cbx_obj.clear()

    def update_dic_conn(self, conn_only=False):
        print('update_dic_conn')
        for tag_0 in self.dic_param:
            if tag_0 == 'conn':
                for tag_1 in self.dic_param['conn']:
                    if tag_1 == 'plugin_version':
                        self.dic_param['conn'][tag_1] = self.main.plugin_version()
                        continue
                    # print('le_name=', le_name)
                    le_name = f'le_{tag_1}'
                    le_obj = self.findChild(QLineEdit, le_name)
                    le_text = le_obj.text()
                    if le_text:
                        if le_name == 'le_name':
                            le_text_name = le_obj.text()
                        if le_text != self.dic_param['conn'][tag_1]['value']:
                            self.dic_param['conn'][tag_1]['value'] = le_text
            elif not conn_only:
                cbx_name = 'cbx_' + tag_0.lower()
                cbx_obj = self.findChild(QComboBox, cbx_name)
                cbx_txt = cbx_obj.currentText()
                if cbx_txt != self.dic_param[tag_0]['alias'][0]:
                    vet_ = self.dic_param[tag_0]['alias']
                    if cbx_txt in vet_:
                        vet_.remove(cbx_txt)
                    self.dic_param[tag_0]['alias'] = [cbx_txt] + vet_
                if 'chk' in self.dic_param[tag_0]:
                    chk_name = 'chk_' + tag_0.lower()
                    chk_obj = self.findChild(QCheckBox, chk_name)
                    self.dic_param[tag_0]['chk']['status'] = 2 if chk_obj.checkState() else False
                cbx_name = cbx_name.replace('sch', 'tab')
                cbx_obj = self.findChild(QComboBox, cbx_name)
                cbx_txt = cbx_obj.currentText()
                if cbx_txt != self.dic_param[tag_0]['tab']['alias'][0]:
                    vet_ = self.dic_param[tag_0]['tab']['alias']
                    if cbx_txt in vet_:
                        vet_.remove(cbx_txt)
                    self.dic_param[tag_0]['tab']['alias'] = [cbx_txt] + vet_
                for field_ in self.dic_param[tag_0]['fields']:
                    cbx_name = 'cbx_' + field_.lower()
                    cbx_obj = self.findChild(QComboBox, cbx_name)
                    cbx_txt = cbx_obj.currentText()
                    if cbx_txt != self.dic_param[tag_0]['fields'][field_]['alias'][0]:
                        vet_ = self.dic_param[tag_0]['fields'][field_]['alias']
                        if cbx_txt in vet_:
                            vet_.remove(cbx_txt)
                        self.dic_param[tag_0]['fields'][field_]['alias'] = [cbx_txt] + vet_
        return {le_text_name: self.dic_param}

    def append_update_db(self):
        print("append_update_db")
        dic_ = self.update_dic_conn()
        self.parent.dic_dbs.update(dic_)
        self.parent.set_db_names(cur_=list(dic_)[0])
        self.parent.save_db_inf()
        # self.parent.save_prj_inf()
        self.parent.update_parameters()
        # self.parent.connect_db()
        # self.update_cb_name(cur_=list(dic_)[0])

        self.close()

    def remove_db(self):
        print('remove_db')
        if self.cb_name.currentText() != '...':
            # idx_list = self.cb_name.currentIndex() - 1
            del self.parent.dic_dbs[self.cb_name.currentText()]
            self.parent.save_db_inf()
            # self.parent.get_db_inf()

            self.cb_name.clear()
            self.cb_name.addItems(['...'] + sorted(list(self.parent.dic_dbs)))
            self.cb_name.setCurrentText('...')
            self.fill_inf()

    def check_exists_name(self):
        print("check_exists_name")
        if self.le_name.text():
            self.pb_save.setEnabled(True)
        if self.le_name.text() in self.parent.dic_db:
            self.pb_save.setText("Substituir")
            return True
        else:
            self.pb_save.setText("Salvar")
            return False

    def create_conn(self):
        print('create_conn')
        # try:
        self.update_dic_conn()
        self.db = Database(parent=self, main=self.main, dic_conn=self.dic_param['conn'])
        if self.db.is_connected():
            self.pb_test.setIcon(self.parent.icon_conn)
            self.pb_test.setText("")
            return True
        else:
            self.db = None
            return False
        # except:
        #     self.db = None
        #     return False

    def test_connection(self):
        self.update_dic_conn(conn_only=True)
        try:
            chk_conn = self.create_conn()
            if not chk_conn or (self.db and not self.db.is_connected()):
                self.pb_test.setIcon(self.parent.icon_conx)
                # print(self.dic_param['conn'])
                self.pb_test.setText("FALHOU")
                return False
            # self.pb_test.setIcon(self.parent.icon_conn)
            le_obj = self.findChild(QLineEdit, 'le_name')
            le_text = le_obj.text()
            self.update_cb_name(cur_=le_text)
            self.update_dic_conn()
            self.fill_inf()
            return True
        except:
            self.pb_test.setIcon(self.parent.icon_conx)
            # print(self.dic_param['conn'])
            self.pb_test.setText("FALHOU")
            return False

    def export_db_inf(self):
        print('export_db_inf')
        le_obj = self.findChild(QLineEdit, 'le_name')
        le_text = le_obj.text()
        if not le_text:
            return
        self.w = QWidget()
        filter = "DBs inf (*.idb)"
        str_dir_ = self.aux_tools.get_(key_='dir_exp')

        if str_dir_:
            file_ = os.path.join(str_dir_, le_text)
        else:
            file_ = le_text
        path_file = QFileDialog.getSaveFileName(self.w, 'Exportar Arquivo', file_, filter)
        if path_file and path_file[0]:
            self.aux_tools.save_(key_='dir_exp', value_=os.path.dirname(path_file[0]))
            str_ = json.dumps(self.dic_param)
            bin_ = Obs2().str_encode(str_)
            with open(path_file[0], "wb") as outfile:
                outfile.write(bin_)

    def import_db_inf(self):
        print('import_db_inf')
        self.w = QWidget()
        filter = "DBs inf (*.idb)"
        str_dir_ = self.aux_tools.get_(key_='dir_exp')
        # Get filename using QFileDialog
        path_db_inf, _ = QFileDialog.getOpenFileName(self.w, 'Abrir Informações', str_dir_, filter)
        if not os.path.exists(path_db_inf):
            return
        with open(path_db_inf, 'rb') as infile:
            bin_ = infile.read()
            infile.close()
        str_ = Obs2().str_decode(bin_)
        self.dic_param = json.loads(str_)
        self.fill_inf()

    def update_cbx(self, sch_=None, tab_=None, cbx_=None, alias=[]):
        # print('update_cbx', cbx_, alias)

        def loop_set_cbx():
            # print(list_)
            for alias_i in alias:
                if alias_i and alias_i in list_:
                    cbx_.setCurrentText(alias_i)
                    return
                for name_ in list_:
                    if alias_i and alias_i in name_:
                        cbx_.setCurrentText(name_)
                        return

        cbx_.clear()
        if sch_ and sch_.currentText() and tab_ and tab_.currentText():
            list_ = self.aux_tools.get_columns(db=self.db, sch_=sch_.currentText(), tab_=tab_.currentText())
        elif sch_ and sch_.currentText():
            list_ = self.aux_tools.get_tables(db=self.db, sch_=sch_.currentText())
        else:
            list_ = self.aux_tools.get_schemas(db=self.db)
        # print('list_=', list_)
        cbx_.addItems(list_)
        loop_set_cbx()

    def closeEvent(self, evt):
        print('closeEvent')
        self.aux_tools.save_geometry(self)


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
