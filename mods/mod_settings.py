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
from qgis.core import QgsVectorFileWriter, QgsWkbTypes, QgsCoordinateTransformContext, QgsCoordinateReferenceSystem, \
    QgsFeature, QgsVectorLayer, QgsFields, QgsField, QgsProject, QgsMapLayerProxyModel, QgsLayerTreeLayer
from qgis.gui import QgsMapLayerComboBox
from .mod_aux_tools import AuxTools
plugin_path = os.path.dirname(os.path.dirname(__file__))

class SettingsDlg(QDialog):
    """Settings Form"""

    def __init__(self, main=None, parent=None):
        super().__init__(parent)
        self.setObjectName('SettingsDlg')
        self.main = main
        self.parent = parent
        # self.parent_dlg = parent
        self.setWindowTitle('Parâmetros')
        self.setWindowIcon(QIcon(":/plugins/mod_cut_pan/icons/icon_cut.png")) ##
        self.dic_param = None
        self.aux_tools = AuxTools(parent=self)
        geom = self.aux_tools.get_geometry()
        if geom:
            self.restoreGeometry(geom)
        else:
            x_, y_, w_, h_ = 100, 100, 300, 300
            self.setGeometry(x_, y_, w_, h_)
        self.list_scale = list(self.parent.dic_pec_v)
        self.dic_param = \
            {
                'step_morfologia': {
                    'label': 'Definições para geração de Morfologia',
                    'fields': {
                        'max_basin_area': {
                            'label': 'Máxima Área das Bacias (m²)',
                            'value': '675000',
                            'default': '675000',
                            'obj': None},
                        'max_memo_grass': {
                            'label': 'Limite de Memória para Grass GIS (GB)',
                            'value': '4',
                            'default': '4',
                            'obj': None},
                    },
                },
                'step_buffers':{
                    'label': 'Definições para geração Buffers',
                    'fields': {
                        'max_scale': {
                            'label': 'Máxima Escala',
                            'list':self.list_scale,
                            'string': '1:{}.000',
                            'value': 1,
                            'default': 1,
                            'obj': None},
                        'min_scale': {
                            'label': 'Mínima Escala',
                            'list': self.list_scale,
                            'string': '1:{}.000',
                            'value': 3,
                            'default': 3,
                            'obj': None},
                    },
                },
            }
        self.get_dic_from_settings()
        dlgLayout = self.create_layout()
        self.setLayout(dlgLayout)

    def get_dic_from_settings(self):
        dic_from_settings = self.aux_tools.get_dic(key_='dic_param')
        for key_i in dic_from_settings:
            if key_i in self.dic_param:
                for key_j in dic_from_settings[key_i]:
                    if key_j in self.dic_param[key_i]['fields']:
                        value_ = dic_from_settings[key_i][key_j]
                        self.dic_param[key_i]['fields'][key_j]['value'] = value_

    def create_layout(self):
        print("create_layout_db")
        r_ = 0
        gl_ = QGridLayout()

        for i, item_i in enumerate(self.dic_param):
            if item_i.startswith('step_'):
                r_ += 1
                lb_ = QLabel(self.dic_param[item_i]['label'])
                lb_.setFont(QFont('MS Shell Dlg 2', 14))
                lb_.setObjectName(item_i.replace('sch', 'lb'))
                lb_.setMinimumWidth(25)
                gl_.addWidget(lb_, r_, 0, 1, 3)
                # if 'chk' in self.dic_param[item_i]:
                #     chk_ = QCheckBox(self.dic_param[item_i]['chk']['label'])
                #     chk_.setCheckState(self.dic_param[item_i]['chk']['status'])
                #     chk_.setObjectName('chk_' + item_i.lower())
                #     chk_.setTristate(False)
                #     gl_.addWidget(chk_, r_, 2, 1, 1)
                #
                # r_ += 1
                # lb_ = QLabel('Esquema:')
                # lb_.setObjectName('lb_' + item_i.lower())
                # # self.dic_obj.update({'lb_' + name_.lower(): lb_})
                # gl_.addWidget(lb_, r_, 0)
                # cbx_sch = QComboBox(self)
                # cbx_sch.setMinimumWidth(25)
                # cbx_name = 'cbx_' + item_i.lower()
                # cbx_sch.setObjectName(cbx_name)
                # # self.update_cbx(cbx_=cbx_sch, alias=self.dic_param[item_i]['alias'])
                # gl_.addWidget(cbx_sch, r_, 1)
                #
                # r_ += 1
                # lb_ = QLabel('Tabela:')
                # lb_.setObjectName('lb_' + item_i.lower().replace('sch', 'tab'))
                # # self.dic_obj.update({'lb_' + name_.lower(): lb_})
                # gl_.addWidget(lb_, r_, 0)
                # cbx_tab = QComboBox(self)
                # cbx_tab.setMinimumWidth(25)
                # cbx_name = 'cbx_' + item_i.lower().replace('sch', 'tab')
                # cbx_tab.setObjectName(cbx_name)
                # # self.update_cbx(sch_=cbx_sch, cbx_=cbx_tab, alias=self.dic_param[item_i]['tab']['alias'])
                # gl_.addWidget(cbx_tab, r_, 1)
                #
                # cbx_sch.currentIndexChanged.connect(partial(self.update_cbx,
                #                                             sch_=cbx_sch,
                #                                             cbx_=cbx_tab,
                #                                             alias=self.dic_param[item_i]['tab']['alias']))

                for j, item_j in enumerate(self.dic_param[item_i]['fields']):
                    r_ += 1
                    # if 'status' in self.dic_param[item_i]['fields'][item_j]:
                    #     cb_ = QCheckBox(self.dic_param[item_i]['fields'][item_j]['label'], self)
                    #     cb_.setObjectName(item_j.replace('fld', 'cb'))
                    #     cb_.setChecked(bool(self.dic_param[item_i]['fields'][item_j]['status']))
                    #     gl_.addWidget(cb_, r_, 0, 1, 1)
                    #     if self.dic_param[item_i]['fields'][item_j]['status']:
                    #         cb_ = self.findChild(QCheckBox, item_j.replace('fld', 'cb'))
                    #         cb_.setChecked(True)
                    # else:
                    lb_ = QLabel(self.dic_param[item_i]['fields'][item_j]['label'])
                    lb_.setObjectName('lb_' + item_j.lower())
                    gl_.addWidget(lb_, r_, 1)
                    if 'list' in self.dic_param[item_i]['fields'][item_j]:
                        cmb_ = QComboBox(self)
                        if 'string' in self.dic_param[item_i]['fields'][item_j]:
                            list_ = ['']
                            string_ = self.dic_param[item_i]['fields'][item_j]['string']
                            for value_ in self.dic_param[item_i]['fields'][item_j]['list']:
                                list_.append(string_.format(value_))
                        else:
                            list_ = self.dic_param[item_i]['fields'][item_j]['list']
                        cmb_.addItems(list_)
                        index_ = int(self.dic_param[item_i]['fields'][item_j]['value'])
                        print('index_', index_)
                        cmb_.setCurrentIndex(index_)
                        self.dic_param[item_i]['fields'][item_j]['obj'] = cmb_
                        gl_.addWidget(cmb_, r_, 2)

                    else:
                        le_ = QLineEdit(self.dic_param[item_i]['fields'][item_j]['value'])
                        le_.setObjectName('le_' + item_j.lower())
                        self.dic_param[item_i]['fields'][item_j]['obj'] = le_
                        gl_.addWidget(le_, r_, 2)
                    # cbx_ = QComboBox(self)
                    # cbx_.setMinimumWidth(25)
                    # cbx_name = 'cbx_' + item_j.lower()
                    # cbx_.setObjectName(cbx_name)
                    # # self.update_cbx(tab_=cbx_tab, sch_=cbx_sch, cbx_=cbx_,
                    # #                 alias=self.dic_param[item_i]['fields'][item_j]['alias'])
                    # gl_.addWidget(cbx_, r_, 1)
                    # cbx_tab.currentIndexChanged.connect(partial(self.update_cbx,
                    #                                             tab_=cbx_tab,
                    #                                             sch_=cbx_sch,
                    #                                             cbx_=cbx_,
                    #                                             alias=self.dic_param[item_i]['fields'][item_j][
                    #                                                 'alias']))

        r_ += 1
        frame2 = QFrame(self)
        frame2.setFrameShape(QFrame.HLine)
        gl_.addWidget(frame2, r_, 0, 1, 3)

        r_ += 1
        gl_.setRowStretch(r_, 1)

        r_ += 1
        hl_ = QHBoxLayout()

        # self.pb_exp = QPushButton("Exportar", self)
        # self.pb_exp.setEnabled(False)
        # hl_.addWidget(self.pb_exp)
        #
        # self.pb_imp = QPushButton("Importar", self)
        # self.pb_imp.setEnabled(False)
        # hl_.addWidget(self.pb_imp)

        self.pb_rest = QPushButton("Restaurar", self)
        # self.pb_remove.setEnabled(False)
        hl_.addWidget(self.pb_rest)

        self.pb_save = QPushButton("Salvar", self)
        # self.pb_save.setEnabled(False)
        hl_.addWidget(self.pb_save)

        gl_.addLayout(hl_, r_, 1, 1, 2)

        # r_ += 1
        # self.lb_topo_logo = QLabel()
        # self.lb_topo_logo.setMinimumSize(QSize(100, 30))
        # self.lb_topo_logo.setMaximumSize(QSize(100, 30))
        # self.lb_topo_logo.setText("")
        # icon_path = os.path.join(plugin_path, 'icons/topo_logo.png')
        # self.lb_topo_logo.setPixmap(QPixmap(icon_path))
        # self.lb_topo_logo.setScaledContents(True)
        # self.lb_topo_logo.setAlignment(Qt.AlignBottom | Qt.AlignLeading | Qt.AlignLeft)
        # self.lb_topo_logo.setObjectName("lb_topo_logo")
        # gl_.addWidget(self.lb_topo_logo, r_, 0, 1, 1)

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

        self.trigger_actions()
        return vl_

    def trigger_actions(self):
        print("trigger_actions")
        self.pb_save.clicked.connect(self.set_dic_param)
        self.pb_rest.clicked.connect(self.rest_default)
        # self.pb_remove.clicked.connect(self.remove_dic_param)
        # self.pb_exp.clicked.connect(self.export_inf)
        # self.pb_imp.clicked.connect(self.import_inf)

    def set_dic_param(self):
        dic_save = {}
        for i, item_i in enumerate(self.dic_param):
            dic_save[item_i] = {}
            if item_i.startswith('step_'):
                for j, item_j in enumerate(self.dic_param[item_i]['fields']):
                    if 'list' in self.dic_param[item_i]['fields'][item_j]:
                        QComboBox().currentIndex()
                        value_ = self.dic_param[item_i]['fields'][item_j]['obj'].currentIndex()
                    else:
                        value_ = self.dic_param[item_i]['fields'][item_j]['obj'].text()
                    dic_save[item_i][item_j] = value_
                    self.dic_param[item_i]['fields'][item_j]['value'] = value_

        self.aux_tools.save_dic(dic_=dic_save,key_='dic_param')
        self.close()

    def rest_default(self):
        for i, item_i in enumerate(self.dic_param):
            if item_i.startswith('step_'):
                for j, item_j in enumerate(self.dic_param[item_i]['fields']):
                    default_ = self.dic_param[item_i]['fields'][item_j]['default']
                    if 'list' in self.dic_param[item_i]['fields'][item_j]:
                        self.dic_param[item_i]['fields'][item_j]['obj'].setCurrentIndex(default_)
                    else:
                        self.dic_param[item_i]['fields'][item_j]['obj'].setText(default_)
                    self.dic_param[item_i]['fields'][item_j]['value'] = default_

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

    # def export_inf(self):
    #     print('export_inf')
    #     le_obj = self.findChild(QLineEdit, 'le_name')
    #     le_text = le_obj.text()
    #     if not le_text:
    #         return
    #     self.w = QWidget()
    #     filter = "DBs inf (*.idb)"
    #     str_dir_ = self.aux_tools.get_(key_='dir_exp')
    #
    #     if str_dir_:
    #         file_ = os.path.join(str_dir_, le_text)
    #     else:
    #         file_ = le_text
    #     path_file = QFileDialog.getSaveFileName(self.w, 'Exportar Arquivo', file_, filter)
    #     if path_file and path_file[0]:
    #         self.aux_tools.save_(key_='dir_exp', value_=os.path.dirname(path_file[0]))
    #         str_ = json.dumps(self.dic_param)
    #         bin_ = Obs2().str_encode(str_)
    #         with open(path_file[0], "wb") as outfile:
    #             outfile.write(bin_)
    #
    # def import_inf(self):
    #     print('import_db_inf')
    #     self.w = QWidget()
    #     filter = "DBs inf (*.idb)"
    #     str_dir_ = self.aux_tools.get_(key_='dir_exp')
    #     # Get filename using QFileDialog
    #     path_db_inf, _ = QFileDialog.getOpenFileName(self.w, 'Abrir Informações', str_dir_, filter)
    #     if not os.path.exists(path_db_inf):
    #         return
    #     with open(path_db_inf, 'rb') as infile:
    #         bin_ = infile.read()
    #         infile.close()
    #     str_ = Obs2().str_decode(bin_)
    #     self.dic_param = json.loads(str_)
    #     self.fill_inf()

    def closeEvent(self, evt):
        print('closeEvent')
        self.aux_tools.save_geometry(self)

