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
from .plugin_i18n import tr_ui

plugin_path = os.path.dirname(os.path.dirname(__file__))

class SettingsDlg(QDialog):
    """Settings Form"""

    def __init__(self, main=None, parent=None):
        super().__init__(parent)
        self.setObjectName('SettingsDlg')
        self.main = main
        self.parent = parent
        # self.parent_dlg = parent
        self.setWindowTitle(tr_ui('Parâmetros'))
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
                    'label': tr_ui('Definições para Geração de Morfologia'),
                    'fields': {
                        'max_basin_area': {
                            'label': tr_ui('Máxima Área das Bacias (m²)'),
                            'value': '675000',
                            'default': '675000',
                            'obj': None},
                        'max_memo_grass': {
                            'label': tr_ui('Limite de Memória para Grass GIS (GB)'),
                            'value': '4',
                            'default': '4',
                            'obj': None},
                    },
                },
                'step_match': {
                    'label': tr_ui('Definições para Seleção dos Pares'),
                    'fields': {
                        'dist_max': {
                            'label': tr_ui('Distância máxima entre centróides (pixels do MDE de teste)'),
                            'value': '3',
                            'default': '3',
                            'obj': None},
                        'percent_area': {
                            'label': tr_ui('Diferença % entre área dos mínimos envelopes'),
                            'value': '10',
                            'default': '10',
                            'obj': None},
                    },
                },
                'step_buffers': {
                    'label': tr_ui('Definições para Geração Buffers'),
                    'fields': {
                        'max_scale': {
                            'label': tr_ui('Máxima Escala'),
                            'list': self.list_scale,
                            'string': '1:{}.000',
                            'value': 10,
                            'default': 10,
                            'obj': None},
                        'min_scale': {
                            'label': tr_ui('Mínima Escala'),
                            'list': self.list_scale,
                            'string': '1:{}.000',
                            'value': 10,
                            'default': 10,
                            'obj': None},
                    },
                },
                'step_normalize_prog': {
                    'label': tr_ui('Definições para Normalização de Progressivas'),
                    'fields': {
                        'norm_type': {
                            'label': tr_ui('Método para Normalização'),
                            'list': self.parent.list_norm_type,
                            'value': 0,
                            'default': 0,
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

    def apply_defaults_to_values(self):
        """Copia 'default' → 'value' em cada field dos step_* (sem tocar nos widgets)."""
        for item_i, block in self.dic_param.items():
            if not isinstance(item_i, str) or not item_i.startswith('step_'):
                continue
            if not isinstance(block, dict) or 'fields' not in block:
                continue
            for item_j, meta in block['fields'].items():
                if isinstance(meta, dict) and 'default' in meta:
                    meta['value'] = meta['default']

    def sync_widgets_from_dic_param(self):
        """Atualiza QComboBox/QLineEdit a partir de dic_param (após carregar do .pa.gpkg)."""
        for item_i, block in self.dic_param.items():
            if not isinstance(item_i, str) or not item_i.startswith('step_'):
                continue
            if not isinstance(block, dict) or 'fields' not in block:
                continue
            for item_j, meta in block['fields'].items():
                if not isinstance(meta, dict):
                    continue
                obj = meta.get('obj')
                if obj is None:
                    continue
                val = meta.get('value')
                if 'list' in meta:
                    try:
                        idx = int(val)
                    except (TypeError, ValueError):
                        try:
                            idx = int(float(val))
                        except (TypeError, ValueError):
                            idx = 0
                    n = obj.count()
                    if n > 0:
                        obj.setCurrentIndex(max(0, min(idx, n - 1)))
                else:
                    obj.setText('' if val is None else str(val))

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
                
                for j, item_j in enumerate(self.dic_param[item_i]['fields']):
                    r_ += 1

                    lb_ = QLabel(self.dic_param[item_i]['fields'][item_j]['label'])
                    lb_.setObjectName('lb_' + item_j.lower())
                    gl_.addWidget(lb_, r_, 1)
                    if 'list' in self.dic_param[item_i]['fields'][item_j]:
                        cmb_ = QComboBox(self)
                        if 'string' in self.dic_param[item_i]['fields'][item_j]:
                            list_ = []
                            string_ = self.dic_param[item_i]['fields'][item_j]['string']
                            for value_ in self.dic_param[item_i]['fields'][item_j]['list']:
                                list_.append(string_.format(value_))
                            list_.append('')
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
   

        r_ += 1
        frame2 = QFrame(self)
        frame2.setFrameShape(QFrame.HLine)
        gl_.addWidget(frame2, r_, 0, 1, 3)

        r_ += 1
        gl_.setRowStretch(r_, 1)

        r_ += 1
        hl_ = QHBoxLayout()


        self.pb_rest = QPushButton(tr_ui('Restaurar'), self)
        # self.pb_remove.setEnabled(False)
        hl_.addWidget(self.pb_rest)

        self.pb_save = QPushButton(tr_ui('Salvar'), self)
        # self.pb_save.setEnabled(False)
        hl_.addWidget(self.pb_save)

        gl_.addLayout(hl_, r_, 1, 1, 2)


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

    def flush_widgets_to_dic_param(self, log_values: bool = False):
        """Copia o estado atual dos widgets para dic_param[*]['fields'][*]['value']."""
        for item_i in self.dic_param:
            if not item_i.startswith('step_'):
                continue
            for item_j in self.dic_param[item_i]['fields']:
                meta = self.dic_param[item_i]['fields'][item_j]
                obj = meta.get('obj')
                if obj is None:
                    continue
                if 'list' in meta:
                    value_ = obj.currentIndex()
                else:
                    value_ = obj.text()
                meta['value'] = value_
                if log_values:
                    self.parent.log_message(f'{item_i} - {item_j} : "{value_}"')

    def set_dic_param(self):
        self.parent.persist_project_config_from_widgets(log_values=True)
        list_scale = self.parent.get_list_scale()
        print('list_scale:', list_scale)

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


    def closeEvent(self, evt):
        print('closeEvent')
        self.aux_tools.save_geometry(self)

