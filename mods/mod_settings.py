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
                                 QProgressBar, QDateEdit, QWidget, QVBoxLayout, QPushButton, QPlainTextEdit,
                                 QRadioButton, QButtonGroup, QDoubleSpinBox)
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
                        'accuracy_standard': {
                            'label': '',
                            'type': 'radio',
                            'list': self.parent.list_accuracy_standard,
                            'value': 0,
                            'default': 0,
                            'obj': None},
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
                        'ce90_max_h': {
                            'label': tr_ui('Máximo Horizontal (pixels do MDE de teste)'),
                            'type': 'doublespin',
                            'value': 5.0,
                            'default': 5.0,
                            'min': 0.1,
                            'max': 100.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                        'ce90_max_v': {
                            'label': tr_ui('Máximo Vertical (pixels do MDE de teste)'),
                            'type': 'doublespin',
                            'value': 2.0,
                            'default': 2.0,
                            'min': 0.1,
                            'max': 100.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                        'show_buffers_on_map': {
                            'label': tr_ui('Mostrar buffers no mapa durante o processamento'),
                            'list': [tr_ui('Não'), tr_ui('Sim')],
                            'value': 0,
                            'default': 0,
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
                'step_dm_formula': {
                    'label': tr_ui('Fórmula para cálculo da Discrepância Média'),
                    'fields': {
                        'dm_formula': {
                            'label': '',
                            'type': 'radio',
                            'list': self.parent.list_dm_formula,
                            'tooltips': self.parent.list_dm_formula_tooltips,
                            'value': 0,
                            'default': 0,
                            'obj': None},
                    },
                },
                'step_audit_report': {
                    'label': tr_ui('Relatório de Auditoria'),
                    'fields': {
                        'audit_horizontal': {
                            'label': tr_ui('Horizontal'),
                            'type': 'checkbox',
                            'value': 0,
                            'default': 0,
                            'enabled': True,
                            'obj': None},
                        'audit_vertical': {
                            'label': tr_ui('Vertical'),
                            'type': 'checkbox',
                            'value': 0,
                            'default': 0,
                            'enabled': True,
                            'obj': None},
                    },
                },
            }
        self.get_dic_from_settings()
        dlgLayout = self.create_layout()
        self.setLayout(dlgLayout)
        # Restaurar depois do layout — senão o sizeHint do formulário sobrescreve.
        self._restore_window_geometry()

    def _restore_window_geometry(self):
        geom = self.aux_tools.get_geometry()
        if geom:
            self.restoreGeometry(geom)
        else:
            self.setGeometry(100, 100, 480, 560)

    def _save_window_geometry(self):
        self.aux_tools.save_geometry(self)

    def _retranslate_dic_param(self):
        """Actualiza rótulos traduzíveis em dic_param (valores dos widgets mantêm-se)."""
        dp = self.dic_param
        dp['step_morfologia']['label'] = tr_ui('Definições para Geração de Morfologia')
        dp['step_morfologia']['fields']['max_basin_area']['label'] = tr_ui(
            'Máxima Área das Bacias (m²)')
        dp['step_morfologia']['fields']['max_memo_grass']['label'] = tr_ui(
            'Limite de Memória para Grass GIS (GB)')
        dp['step_match']['label'] = tr_ui('Definições para Seleção dos Pares')
        dp['step_match']['fields']['dist_max']['label'] = tr_ui(
            'Distância máxima entre centróides (pixels do MDE de teste)')
        dp['step_match']['fields']['percent_area']['label'] = tr_ui(
            'Diferença % entre área dos mínimos envelopes')
        dp['step_buffers']['label'] = tr_ui('Definições para Geração Buffers')
        if 'accuracy_standard' in dp['step_buffers']['fields']:
            dp['step_buffers']['fields']['accuracy_standard']['list'] = (
                self.parent.list_accuracy_standard)
        dp['step_buffers']['fields']['max_scale']['label'] = tr_ui('Máxima Escala')
        dp['step_buffers']['fields']['min_scale']['label'] = tr_ui('Mínima Escala')
        if 'ce90_max_h' in dp['step_buffers']['fields']:
            dp['step_buffers']['fields']['ce90_max_h']['label'] = tr_ui(
                'Máximo Horizontal (pixels do MDE de teste)')
        if 'ce90_max_v' in dp['step_buffers']['fields']:
            dp['step_buffers']['fields']['ce90_max_v']['label'] = tr_ui(
                'Máximo Vertical (pixels do MDE de teste)')
        dp['step_buffers']['fields']['show_buffers_on_map']['label'] = tr_ui(
            'Mostrar buffers no mapa durante o processamento')
        dp['step_buffers']['fields']['show_buffers_on_map']['list'] = [
            tr_ui('Não'), tr_ui('Sim')]
        dp['step_normalize_prog']['label'] = tr_ui(
            'Definições para Normalização de Progressivas')
        dp['step_normalize_prog']['fields']['norm_type']['label'] = tr_ui(
            'Método para Normalização')
        dp['step_normalize_prog']['fields']['norm_type']['list'] = self.parent.list_norm_type
        if 'step_dm_formula' in dp:
            dp['step_dm_formula']['label'] = tr_ui(
                'Fórmula para cálculo da Discrepância Média')
            dp['step_dm_formula']['fields']['dm_formula']['list'] = (
                self.parent.list_dm_formula)
            dp['step_dm_formula']['fields']['dm_formula']['tooltips'] = (
                self.parent.list_dm_formula_tooltips)
        if 'step_audit_report' in dp:
            dp['step_audit_report']['label'] = tr_ui('Relatório de Auditoria')
            dp['step_audit_report']['fields']['audit_horizontal']['label'] = tr_ui(
                'Horizontal')
            dp['step_audit_report']['fields']['audit_vertical']['label'] = tr_ui(
                'Vertical')

    def apply_language_live(self):
        """Actualiza textos da janela de parâmetros após mudança de idioma."""
        self.flush_widgets_to_dic_param()
        self._retranslate_dic_param()
        self.setWindowTitle(tr_ui('Parâmetros'))
        self.pb_rest.setText(tr_ui('Restaurar'))
        self.pb_save.setText(tr_ui('Salvar'))
        for item_i, block in self.dic_param.items():
            if not item_i.startswith('step_'):
                continue
            lb_sec = block.get('label_obj')
            if lb_sec is None:
                lb_sec = self.findChild(QLabel, item_i.replace('sch', 'lb'))
            if lb_sec is not None:
                lb_sec.setText(block['label'])
            for item_j, meta in block['fields'].items():
                lb_f = meta.get('label_obj')
                if lb_f is None:
                    lb_f = self.findChild(QLabel, 'lb_' + item_j.lower())
                if lb_f is not None:
                    lb_f.setText(meta.get('label') or '')
                obj = meta.get('obj')
                if obj is None:
                    continue
                if meta.get('type') == 'checkbox':
                    continue
                if meta.get('type') == 'radio':
                    tips = meta.get('tooltips') or []
                    labels = meta.get('list') or []
                    for btn in obj.buttons():
                        idx = obj.id(btn)
                        if 0 <= idx < len(labels):
                            btn.setText(labels[idx])
                        if 0 <= idx < len(tips):
                            btn.setToolTip(tips[idx])
                    continue
                if meta.get('type') == 'doublespin':
                    continue
                if 'list' not in meta:
                    continue
                idx = obj.currentIndex()
                obj.blockSignals(True)
                obj.clear()
                if 'string' in meta:
                    for value_ in meta['list']:
                        obj.addItem(meta['string'].format(value_))
                    obj.addItem('')
                elif item_j == 'norm_type':
                    obj.addItems(list(self.parent.list_norm_type))
                elif item_j == 'show_buffers_on_map':
                    obj.addItems(list(meta['list']))
                else:
                    obj.addItems([str(x) for x in meta['list']])
                if 0 <= idx < obj.count():
                    obj.setCurrentIndex(idx)
                obj.blockSignals(False)
        self._sync_buffer_standard_visibility()

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
                if meta.get('type') == 'checkbox':
                    try:
                        checked = bool(int(val))
                    except (TypeError, ValueError):
                        checked = bool(val)
                    obj.blockSignals(True)
                    obj.setChecked(checked)
                    obj.blockSignals(False)
                elif meta.get('type') == 'radio':
                    try:
                        idx = int(val)
                    except (TypeError, ValueError):
                        idx = 0
                    btn = obj.button(idx)
                    if btn is None and obj.buttons():
                        btn = obj.button(0)
                    if btn is not None:
                        btn.setChecked(True)
                elif meta.get('type') == 'doublespin':
                    try:
                        obj.setValue(float(val))
                    except (TypeError, ValueError):
                        try:
                            obj.setValue(float(meta.get('default', 0)))
                        except (TypeError, ValueError):
                            pass
                elif 'list' in meta:
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
        self._sync_buffer_standard_visibility()

    def create_layout(self):
        r_ = 0
        gl_ = QGridLayout()

        for i, item_i in enumerate(self.dic_param):
            if item_i.startswith('step_'):
                r_ += 1
                lb_ = QLabel(self.dic_param[item_i]['label'])
                lb_.setFont(QFont('MS Shell Dlg 2', 14))
                lb_.setObjectName(item_i.replace('sch', 'lb'))
                lb_.setMinimumWidth(25)
                self.dic_param[item_i]['label_obj'] = lb_
                gl_.addWidget(lb_, r_, 0, 1, 3)
                
                for j, item_j in enumerate(self.dic_param[item_i]['fields']):
                    r_ += 1

                    meta = self.dic_param[item_i]['fields'][item_j]
                    field_label = meta.get('label') or ''
                    if field_label:
                        lb_ = QLabel(field_label)
                        lb_.setObjectName('lb_' + item_j.lower())
                        meta['label_obj'] = lb_
                        gl_.addWidget(lb_, r_, 1)
                    else:
                        meta['label_obj'] = None
                    if meta.get('type') == 'checkbox':
                        cb_ = QCheckBox(self)
                        try:
                            checked = bool(int(meta.get('value', 0)))
                        except (TypeError, ValueError):
                            checked = bool(meta.get('value'))
                        cb_.setChecked(checked)
                        if meta.get('enabled') is False:
                            cb_.setEnabled(False)
                            if field_label:
                                lb_.setEnabled(False)
                        meta['obj'] = cb_
                        gl_.addWidget(cb_, r_, 2)
                    elif meta.get('type') == 'radio':
                        bg = QButtonGroup(self)
                        bg.setExclusive(True)
                        vl_radio = QVBoxLayout()
                        vl_radio.setContentsMargins(0, 0, 0, 0)
                        tips = meta.get('tooltips') or []
                        try:
                            selected = int(meta.get('value', 0))
                        except (TypeError, ValueError):
                            selected = 0
                        for idx, text in enumerate(meta.get('list') or []):
                            rb = QRadioButton(str(text), self)
                            if idx < len(tips):
                                rb.setToolTip(str(tips[idx]))
                            bg.addButton(rb, idx)
                            if idx == selected:
                                rb.setChecked(True)
                            vl_radio.addWidget(rb)
                        if bg.checkedId() < 0 and bg.buttons():
                            bg.button(0).setChecked(True)
                        meta['obj'] = bg
                        gl_.addLayout(vl_radio, r_, 1, 1, 2)
                    elif meta.get('type') == 'doublespin':
                        sp = QDoubleSpinBox(self)
                        sp.setMinimum(float(meta.get('min', 0.0)))
                        sp.setMaximum(float(meta.get('max', 100.0)))
                        sp.setDecimals(int(meta.get('decimals', 2)))
                        sp.setSingleStep(float(meta.get('step', 0.5)))
                        try:
                            sp.setValue(float(meta.get('value', meta.get('default', 0))))
                        except (TypeError, ValueError):
                            sp.setValue(float(meta.get('default', 0)))
                        meta['obj'] = sp
                        gl_.addWidget(sp, r_, 2)
                    elif 'list' in meta:
                        cmb_ = QComboBox(self)
                        if 'string' in meta:
                            list_ = []
                            string_ = meta['string']
                            for value_ in meta['list']:
                                list_.append(string_.format(value_))
                            list_.append('')
                        else:
                            list_ = meta['list']
                        cmb_.addItems(list_)
                        index_ = int(meta['value'])
                        cmb_.setCurrentIndex(index_)
                        meta['obj'] = cmb_
                        gl_.addWidget(cmb_, r_, 2)

                    else:
                        le_ = QLineEdit(meta['value'])
                        le_.setObjectName('le_' + item_j.lower())
                        meta['obj'] = le_
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
        self._wire_accuracy_standard_visibility()
        return vl_

    def _set_field_row_visible(self, meta, visible: bool):
        lb = meta.get('label_obj')
        if lb is not None:
            lb.setVisible(visible)
        obj = meta.get('obj')
        if obj is None:
            return
        if meta.get('type') == 'radio':
            for btn in obj.buttons():
                btn.setVisible(visible)
        else:
            obj.setVisible(visible)

    def _sync_buffer_standard_visibility(self):
        fields = self.dic_param.get('step_buffers', {}).get('fields', {})
        std = fields.get('accuracy_standard', {})
        bg = std.get('obj')
        try:
            is_ce90 = int(bg.checkedId()) == 1 if bg is not None else False
        except (TypeError, ValueError):
            is_ce90 = False
        for key in ('max_scale', 'min_scale'):
            if key in fields:
                self._set_field_row_visible(fields[key], not is_ce90)
        for key in ('ce90_max_h', 'ce90_max_v'):
            if key in fields:
                self._set_field_row_visible(fields[key], is_ce90)

    def _wire_accuracy_standard_visibility(self):
        fields = self.dic_param.get('step_buffers', {}).get('fields', {})
        bg = fields.get('accuracy_standard', {}).get('obj')
        if bg is None:
            return
        bg.buttonClicked.connect(lambda *_: self._sync_buffer_standard_visibility())
        self._sync_buffer_standard_visibility()

    def trigger_actions(self):
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
                if meta.get('type') == 'checkbox':
                    value_ = 1 if obj.isChecked() else 0
                elif meta.get('type') == 'radio':
                    value_ = obj.checkedId()
                    if value_ < 0:
                        value_ = 0
                elif meta.get('type') == 'doublespin':
                    value_ = float(obj.value())
                elif 'list' in meta:
                    value_ = obj.currentIndex()
                else:
                    value_ = obj.text()
                meta['value'] = value_
                if log_values:
                    self.parent.log_message(f'{item_i} - {item_j} : "{value_}"')

    def set_dic_param(self):
        self.parent.persist_project_config_from_widgets(log_values=True)
        self.close()

    def rest_default(self):
        for i, item_i in enumerate(self.dic_param):
            if item_i.startswith('step_'):
                for j, item_j in enumerate(self.dic_param[item_i]['fields']):
                    default_ = self.dic_param[item_i]['fields'][item_j]['default']
                    meta = self.dic_param[item_i]['fields'][item_j]
                    obj = meta.get('obj')
                    if obj is None:
                        meta['value'] = default_
                        continue
                    if meta.get('type') == 'checkbox':
                        obj.setChecked(bool(int(default_)))
                    elif meta.get('type') == 'radio':
                        btn = obj.button(int(default_))
                        if btn is None and obj.buttons():
                            btn = obj.button(0)
                        if btn is not None:
                            btn.setChecked(True)
                    elif meta.get('type') == 'doublespin':
                        obj.setValue(float(default_))
                    elif 'list' in meta:
                        obj.setCurrentIndex(default_)
                    else:
                        obj.setText(default_)
                    meta['value'] = default_
        self._sync_buffer_standard_visibility()

    def fill_inf(self):
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
        self._save_window_geometry()
        super().closeEvent(evt)

    def hideEvent(self, evt):
        # Janela reutilizada com show()/hide() — grava posição/tamanho
        self._save_window_geometry()
        super().hideEvent(evt)

