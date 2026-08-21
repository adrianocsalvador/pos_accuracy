import os

from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtGui import QIcon, QFont
from qgis.PyQt.QtWidgets import (
    QScrollArea, QGridLayout, QPushButton, QLabel, QWidget, QSizePolicy,
    QSpacerItem, QComboBox, QLineEdit, QDialog, QFrame, QCheckBox,
    QHBoxLayout, QVBoxLayout, QRadioButton, QButtonGroup, QDoubleSpinBox, QSpinBox,
)
from .mod_aux_tools import AuxTools
from .plugin_i18n import tr_ui

plugin_path = os.path.dirname(os.path.dirname(__file__))


def _narrow_value_widget(widget, *, fraction: float = 1.0 / 3.0, floor: int = 56):
    """Largura dos spin/combo da 2.ª coluna ≈ 1/3 do sizeHint (mínimo e máximo)."""
    if widget is None:
        return
    hint = max(widget.minimumSizeHint().width(), widget.sizeHint().width(), 1)
    width = max(floor, int(hint * fraction))
    widget.setMinimumWidth(width)
    widget.setMaximumWidth(width)
    widget.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)


# Combos expansíveis ocupam 80 % da 2.ª coluna (20 % livres à direita)
COMBO_EXPAND_FILL = 0.80


def _expanding_value_host(widget, *, fill: float = COMBO_EXPAND_FILL, floor: int = 56):
    """Combo que cresce com a janela, limitado a `fill` da 2.ª coluna."""
    if widget is None:
        return None
    hint = max(widget.minimumSizeHint().width(), widget.sizeHint().width(), 1)
    widget.setMinimumWidth(max(floor, min(hint, 120)))
    widget.setMaximumWidth(16777215)
    widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
    fill = min(1.0, max(0.1, float(fill)))
    host = QWidget()
    hl = QHBoxLayout(host)
    hl.setContentsMargins(0, 0, 0, 0)
    hl.setSpacing(0)
    num = max(1, int(round(fill * 100)))
    den = max(0, 100 - num)
    hl.addWidget(widget, num)
    if den:
        hl.addStretch(den)
    return host


class SettingsDlg(QDialog):
    """Settings Form"""

    def __init__(self, main=None, parent=None):
        super().__init__(parent)
        self.setObjectName('SettingsDlg')
        self.main = main
        self.parent = parent
        # self.parent_dlg = parent
        self.setWindowTitle(tr_ui('Parâmetros'))
        self.setWindowIcon(QIcon(os.path.join(plugin_path, 'icons', 'icon_config.png')))
        self.dic_param = None
        self.aux_tools = AuxTools(parent=self)
        self.list_scale = list(self.parent.dic_pec_v)
        self.dic_param = \
            {
                'step_morfologia': {
                    'label': tr_ui('Configurações para Geração de Morfologia'),
                    'tooltip': tr_ui(
                        'Extração das feições lineares (cumeadas e hidrografia) por watershed (GRASS).'),
                    'fields': {
                        'max_basin_area': {
                            'label': tr_ui('Máxima Área das Bacias (m²)'),
                            'tooltip': tr_ui(
                                'Controla a densidade das linhas: diminuir a área gera mais feições; '
                                'aumentar gera menos. Padrão: 675000 m².'),
                            'type': 'spin',
                            'value': 675000,
                            'default': 675000,
                            'min': 1000,
                            'max': 50000000,
                            'step': 1000,
                            'obj': None},
                        'max_memo_grass': {
                            'label': tr_ui('Limite de Memória para Grass GIS (GB)'),
                            'tooltip': tr_ui(
                                'Memória máxima do GRASS no r.watershed. Diminua se falhar. '
                                'Padrão: 4 GB.'),
                            'type': 'doublespin',
                            'value': 4.0,
                            'default': 4.0,
                            'min': 0.5,
                            'max': 128.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                    },
                },
                'step_match': {
                    'label': tr_ui('Configurações para Seleção dos Pares'),
                    'tooltip': tr_ui(
                        'Filtros para formar pares homólogos entre linhas de referência e de teste.'),
                    'fields': {
                        'dist_max': {
                            'label': tr_ui('Distância máxima entre centróides (pixels do MDE de teste)'),
                            'tooltip': tr_ui(
                                'Filtro inicial: distância máxima entre centróides, em pixels do MDE de teste. '
                                'Aumentar tende a aumentar candidatos; diminuir pode enviesar a amostra '
                                'ou não atingir o mínimo. Padrão: 3 px.'),
                            'type': 'doublespin',
                            'value': 3.0,
                            'default': 3.0,
                            'min': 0.5,
                            'max': 100.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                        'percent_area': {
                            'label': tr_ui('Diferença % entre área dos mínimos envelopes'),
                            'tooltip': tr_ui(
                                'Segundo filtro: geometrias semelhantes têm envelopes com áreas semelhantes, '
                                'reduzindo a influência de erros posicionais. Aumentar ou diminuir tem o '
                                'mesmo efeito que na distância entre centróides. Padrão: 10 %.'),
                            'type': 'doublespin',
                            'value': 10.0,
                            'default': 10.0,
                            'min': 0.5,
                            'max': 100.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                        'percent_length': {
                            'label': tr_ui(
                                'Diferença % entre os comprimentos dos mínimos envelopes'),
                            'tooltip': tr_ui(
                                'Filtro pelo lado maior dos envelopes orientados. Pares homólogos devem ter '
                                'comprimentos de envelope semelhantes. Padrão: 5 %.'),
                            'type': 'doublespin',
                            'value': 5.0,
                            'default': 5.0,
                            'min': 0.5,
                            'max': 100.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                        'min_extent_px': {
                            'label': tr_ui(
                                'Extensão mínima da feição de teste (pixels do MDE de teste)'),
                            'tooltip': tr_ui(
                                'Descarta linhas de teste mais curtas que este comprimento (pixels × GSD do teste), '
                                'evitando amostras pouco representativas. Padrão: 10 px.'),
                            'type': 'doublespin',
                            'value': 10.0,
                            'default': 10.0,
                            'min': 0.5,
                            'max': 1000.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                    },
                },
                'step_buffers': {
                    'label': tr_ui('Configurações para Geração Buffers'),
                    'tooltip': tr_ui(
                        'Raios de buffer e padrão de acurácia (PEC-PCD ou CE90/LE90).'),
                    'fields': {
                        'accuracy_standard': {
                            'label': '',
                            'tooltip': tr_ui(
                                'PEC-PCD classifica nas escalas e classes A–D. '
                                'CE90/LE90 busca o menor limiar (m) que cumpre os critérios de aprovação marcados.'),
                            'type': 'radio',
                            'layout': 'horizontal',
                            'list': self.parent.list_accuracy_standard,
                            'value': 0,
                            'default': 0,
                            'obj': None},
                        'max_scale': {
                            'label': tr_ui('Máxima Escala'),
                            'tooltip': tr_ui(
                                'Maior escala (maior detalhe) da avaliação PEC-PCD, p.ex. 1:10.000.'),
                            'list': self.list_scale,
                            'string': '1:{}.000',
                            'expand': True,
                            'value': 10,
                            'default': 10,
                            'obj': None},
                        'min_scale': {
                            'label': tr_ui('Mínima Escala'),
                            'tooltip': tr_ui(
                                'Menor escala (menor detalhe) da avaliação PEC-PCD. '
                                'A análise percorre da máxima à mínima.'),
                            'list': self.list_scale,
                            'string': '1:{}.000',
                            'expand': True,
                            'value': 10,
                            'default': 10,
                            'obj': None},
                        'ce90_max_h': {
                            'label': tr_ui('Máximo Horizontal (pixels do MDE de teste)'),
                            'tooltip': tr_ui(
                                'Teto da busca do CE90, em pixels do MDE de teste (× GSD = metros). '
                                'Padrão: 5 px.'),
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
                            'tooltip': tr_ui(
                                'Teto da busca do LE90, em pixels do MDE de teste (× GSD = metros). '
                                'Padrão: 2 px.'),
                            'type': 'doublespin',
                            'value': 2.0,
                            'default': 2.0,
                            'min': 0.1,
                            'max': 100.0,
                            'decimals': 1,
                            'step': 0.5,
                            'obj': None},
                        'ce90_pass_criteria': {
                            'label': tr_ui('Critérios para aprovação'),
                            'bold_label': True,
                            'tooltip': tr_ui(
                                'Critérios da busca recursiva do limiar CE90/LE90. '
                                'Só os marcados contam para a aprovação '
                                '(a normalidade continua sempre obrigatória). '
                                'Padrão: Quantitativo, Extensão e RMS (EP).'),
                            'type': 'checkbox_group',
                            'layout': 'vertical',
                            'list': [
                                tr_ui('Quantitativo'),
                                tr_ui('Extensão'),
                                tr_ui('RMS (EP)'),
                            ],
                            'value': 7,
                            'default': 7,
                            'obj': None},
                        'show_buffers_on_map': {
                            'label': tr_ui('Mostrar buffers no mapa durante o processamento'),
                            'tooltip': tr_ui(
                                'Se marcado, a camada de buffers aparece no mapa enquanto é gerada '
                                '(pode tornar o processamento mais lento). Padrão: desmarcado.'),
                            'type': 'checkbox',
                            'value': 0,
                            'default': 0,
                            'obj': None},
                    },
                },
                'step_normalize_prog': {
                    'label': tr_ui('Configurações para Compatibilização de Progressivas'),
                    'tooltip': tr_ui(
                        'Compatibilização das progressivas dos perfis altimétricos (referência vs teste).'),
                    'fields': {
                        'norm_type': {
                            'label': tr_ui('Método para Compatibilização'),
                            'tooltip': tr_ui(
                                'Linear: reescala o comprimento do perfil de teste. '
                                'Por proximidade: associa pontos pela menor distância. '
                                'Sem compatibilização: usa as progressivas originais.'),
                            'list': self.parent.list_norm_type,
                            'expand': True,
                            'value': 0,
                            'default': 0,
                            'obj': None},
                    },
                },
                'step_dm_formula': {
                    'label': tr_ui('Fórmula para cálculo da Discrepância Média'),
                    'tooltip': tr_ui(
                        'Equação da discrepância média (DM) a partir das áreas dos buffers duplos.'),
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
                    'tooltip': tr_ui(
                        'Gera PDF de auditoria (e sempre o CSV correspondente) '
                        'para conferir buffers e DM. Pode ativar só o planimétrico, '
                        'só o altimétrico, ou ambos.'),
                    'fields': {
                        'audit_horizontal': {
                            'label': tr_ui('Horizontal (PDF)'),
                            'tooltip': tr_ui(
                                'Gera o PDF de auditoria planimétrica (buffers no plano XY). '
                                'O CSV correspondente é sempre gravado.'),
                            'type': 'checkbox',
                            'value': 0,
                            'default': 0,
                            'enabled': True,
                            'obj': None},
                        'audit_vertical': {
                            'label': tr_ui('Vertical (PDF)'),
                            'tooltip': tr_ui(
                                'Gera o PDF de auditoria altimétrica (perfis cota × progressiva). '
                                'O CSV correspondente é sempre gravado.'),
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
        dp['step_morfologia']['label'] = tr_ui('Configurações para Geração de Morfologia')
        dp['step_morfologia']['tooltip'] = tr_ui(
            'Extração das feições lineares (cumeadas e hidrografia) por watershed (GRASS).')
        dp['step_morfologia']['fields']['max_basin_area']['label'] = tr_ui(
            'Máxima Área das Bacias (m²)')
        dp['step_morfologia']['fields']['max_basin_area']['tooltip'] = tr_ui(
            'Controla a densidade das linhas: diminuir a área gera mais feições; '
            'aumentar gera menos. Padrão: 675000 m².')
        dp['step_morfologia']['fields']['max_memo_grass']['label'] = tr_ui(
            'Limite de Memória para Grass GIS (GB)')
        dp['step_morfologia']['fields']['max_memo_grass']['tooltip'] = tr_ui(
            'Memória máxima do GRASS no r.watershed. Diminua se falhar. '
            'Padrão: 4 GB.')
        dp['step_match']['label'] = tr_ui('Configurações para Seleção dos Pares')
        dp['step_match']['tooltip'] = tr_ui(
            'Filtros para formar pares homólogos entre linhas de referência e de teste.')
        dp['step_match']['fields']['dist_max']['label'] = tr_ui(
            'Distância máxima entre centróides (pixels do MDE de teste)')
        dp['step_match']['fields']['dist_max']['tooltip'] = tr_ui(
            'Filtro inicial: distância máxima entre centróides, em pixels do MDE de teste. '
            'Aumentar tende a aumentar candidatos; diminuir pode enviesar a amostra '
            'ou não atingir o mínimo. Padrão: 3 px.')
        dp['step_match']['fields']['percent_area']['label'] = tr_ui(
            'Diferença % entre área dos mínimos envelopes')
        dp['step_match']['fields']['percent_area']['tooltip'] = tr_ui(
            'Segundo filtro: geometrias semelhantes têm envelopes com áreas semelhantes, '
            'reduzindo a influência de erros posicionais. Padrão: 10 %.')
        if 'percent_length' in dp['step_match']['fields']:
            dp['step_match']['fields']['percent_length']['label'] = tr_ui(
                'Diferença % entre os comprimentos dos mínimos envelopes')
            dp['step_match']['fields']['percent_length']['tooltip'] = tr_ui(
                'Filtro pelo lado maior dos envelopes orientados. Pares homólogos devem ter '
                'comprimentos de envelope semelhantes. Padrão: 5 %.')
        if 'min_extent_px' in dp['step_match']['fields']:
            dp['step_match']['fields']['min_extent_px']['label'] = tr_ui(
                'Extensão mínima da feição de teste (pixels do MDE de teste)')
            dp['step_match']['fields']['min_extent_px']['tooltip'] = tr_ui(
                'Descarta linhas de teste mais curtas que este comprimento (pixels × GSD do teste). '
                'Padrão: 10 px.')
        dp['step_buffers']['label'] = tr_ui('Configurações para Geração Buffers')
        dp['step_buffers']['tooltip'] = tr_ui(
            'Raios de buffer e padrão de acurácia (PEC-PCD ou CE90/LE90).')
        if 'accuracy_standard' in dp['step_buffers']['fields']:
            dp['step_buffers']['fields']['accuracy_standard']['list'] = (
                self.parent.list_accuracy_standard)
            dp['step_buffers']['fields']['accuracy_standard']['tooltip'] = tr_ui(
                'PEC-PCD classifica nas escalas e classes A–D. '
                'CE90/LE90 busca o menor limiar (m) que cumpre os critérios de aprovação marcados.')
        dp['step_buffers']['fields']['max_scale']['label'] = tr_ui('Máxima Escala')
        dp['step_buffers']['fields']['max_scale']['tooltip'] = tr_ui(
            'Maior escala (maior detalhe) da avaliação PEC-PCD, p.ex. 1:10.000.')
        dp['step_buffers']['fields']['min_scale']['label'] = tr_ui('Mínima Escala')
        dp['step_buffers']['fields']['min_scale']['tooltip'] = tr_ui(
            'Menor escala (menor detalhe) da avaliação PEC-PCD.')
        if 'ce90_max_h' in dp['step_buffers']['fields']:
            dp['step_buffers']['fields']['ce90_max_h']['label'] = tr_ui(
                'Máximo Horizontal (pixels do MDE de teste)')
            dp['step_buffers']['fields']['ce90_max_h']['tooltip'] = tr_ui(
                'Teto da busca do CE90, em pixels do MDE de teste. Padrão: 5 px.')
        if 'ce90_max_v' in dp['step_buffers']['fields']:
            dp['step_buffers']['fields']['ce90_max_v']['label'] = tr_ui(
                'Máximo Vertical (pixels do MDE de teste)')
            dp['step_buffers']['fields']['ce90_max_v']['tooltip'] = tr_ui(
                'Teto da busca do LE90, em pixels do MDE de teste. Padrão: 2 px.')
        if 'ce90_pass_criteria' in dp['step_buffers']['fields']:
            dp['step_buffers']['fields']['ce90_pass_criteria']['label'] = tr_ui(
                'Critérios para aprovação')
            dp['step_buffers']['fields']['ce90_pass_criteria']['tooltip'] = tr_ui(
                'Critérios da busca recursiva do limiar CE90/LE90. '
                'Só os marcados contam para a aprovação '
                '(a normalidade continua sempre obrigatória). '
                'Padrão: Quantitativo, Extensão e RMS (EP).')
            dp['step_buffers']['fields']['ce90_pass_criteria']['list'] = [
                tr_ui('Quantitativo'),
                tr_ui('Extensão'),
                tr_ui('RMS (EP)'),
            ]
        dp['step_buffers']['fields']['show_buffers_on_map']['label'] = tr_ui(
            'Mostrar buffers no mapa durante o processamento')
        dp['step_buffers']['fields']['show_buffers_on_map']['tooltip'] = tr_ui(
            'Se marcado, a camada de buffers aparece no mapa enquanto é gerada '
            '(pode tornar o processamento mais lento). Padrão: desmarcado.')
        dp['step_normalize_prog']['label'] = tr_ui(
            'Configurações para Compatibilização de Progressivas')
        dp['step_normalize_prog']['tooltip'] = tr_ui(
            'Compatibilização das progressivas dos perfis altimétricos (referência vs teste).')
        dp['step_normalize_prog']['fields']['norm_type']['label'] = tr_ui(
            'Método para Compatibilização')
        dp['step_normalize_prog']['fields']['norm_type']['tooltip'] = tr_ui(
            'Linear: reescala o comprimento do perfil de teste. '
            'Por proximidade: associa pontos pela menor distância. '
            'Sem compatibilização: usa as progressivas originais.')
        dp['step_normalize_prog']['fields']['norm_type']['list'] = self.parent.list_norm_type
        if 'step_dm_formula' in dp:
            dp['step_dm_formula']['label'] = tr_ui(
                'Fórmula para cálculo da Discrepância Média')
            dp['step_dm_formula']['tooltip'] = tr_ui(
                'Equação da discrepância média (DM) a partir das áreas dos buffers duplos.')
            dp['step_dm_formula']['fields']['dm_formula']['list'] = (
                self.parent.list_dm_formula)
            dp['step_dm_formula']['fields']['dm_formula']['tooltips'] = (
                self.parent.list_dm_formula_tooltips)
        if 'step_audit_report' in dp:
            dp['step_audit_report']['label'] = tr_ui('Relatório de Auditoria')
            dp['step_audit_report']['tooltip'] = tr_ui(
                'Gera PDF de auditoria (e sempre o CSV correspondente) '
                'para conferir buffers e DM. Pode ativar só o planimétrico, '
                'só o altimétrico, ou ambos.')
            dp['step_audit_report']['fields']['audit_horizontal']['label'] = tr_ui(
                'Horizontal (PDF)')
            dp['step_audit_report']['fields']['audit_horizontal']['tooltip'] = tr_ui(
                'Gera o PDF de auditoria planimétrica (buffers no plano XY). '
                'O CSV correspondente é sempre gravado.')
            dp['step_audit_report']['fields']['audit_vertical']['label'] = tr_ui(
                'Vertical (PDF)')
            dp['step_audit_report']['fields']['audit_vertical']['tooltip'] = tr_ui(
                'Gera o PDF de auditoria altimétrica (perfis cota × progressiva). '
                'O CSV correspondente é sempre gravado.')
            dp['step_audit_report']['fields'].pop('audit_csv_only', None)
    def apply_language_live(self):
        """Actualiza textos da janela de parâmetros após mudança de idioma."""
        self.flush_widgets_to_dic_param()
        self._retranslate_dic_param()
        self.setWindowTitle(tr_ui('Parâmetros'))
        self.pb_rest.setText(tr_ui('Restaurar'))
        self.pb_rest.setToolTip(tr_ui('Repõe todos os parâmetros desta janela nos valores padrão.'))
        self.pb_save.setText(tr_ui('Salvar'))
        self.pb_save.setToolTip(tr_ui('Grava os parâmetros no projeto (.pa.gpkg) e fecha a janela.'))
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
                if meta.get('type') == 'checkbox_group':
                    labels = meta.get('list') or []
                    item_lbs = meta.get('item_label_objs') or []
                    if item_lbs:
                        for idx, lb_item in enumerate(item_lbs):
                            if 0 <= idx < len(labels):
                                lb_item.setText(str(labels[idx]))
                    else:
                        for idx, cb in enumerate(obj or []):
                            if 0 <= idx < len(labels):
                                cb.setText(str(labels[idx]))
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
                if meta.get('type') == 'spin':
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
                else:
                    obj.addItems([str(x) for x in meta['list']])
                if 0 <= idx < obj.count():
                    obj.setCurrentIndex(idx)
                obj.blockSignals(False)
        self._sync_buffer_standard_visibility()
        self._apply_param_tooltips()

    def get_dic_from_settings(self):
        dic_from_settings = self.aux_tools.get_dic(key_='dic_param')
        for key_i in dic_from_settings:
            if key_i in self.dic_param:
                for key_j in dic_from_settings[key_i]:
                    if key_j in self.dic_param[key_i]['fields']:
                        value_ = dic_from_settings[key_i][key_j]
                        meta = self.dic_param[key_i]['fields'][key_j]
                        ftype = meta.get('type')
                        if ftype in ('spin', 'checkbox', 'checkbox_group'):
                            try:
                                value_ = int(float(value_))
                            except (TypeError, ValueError):
                                value_ = meta.get('default', 0)
                        elif ftype == 'doublespin':
                            try:
                                value_ = float(value_)
                            except (TypeError, ValueError):
                                value_ = meta.get('default', 0.0)
                        meta['value'] = value_

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
                elif meta.get('type') == 'checkbox_group':
                    try:
                        mask = int(float(val))
                    except (TypeError, ValueError):
                        try:
                            mask = int(float(meta.get('default', 7)))
                        except (TypeError, ValueError):
                            mask = 7
                    for idx, cb in enumerate(obj or []):
                        cb.blockSignals(True)
                        cb.setChecked(bool(mask & (1 << idx)))
                        cb.blockSignals(False)
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
                elif meta.get('type') == 'spin':
                    try:
                        obj.setValue(int(float(val)))
                    except (TypeError, ValueError):
                        try:
                            obj.setValue(int(meta.get('default', 0)))
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
        # Coluna de rótulos (1) mais larga; coluna de widgets (2) ~1/3 do espaço extra
        gl_.setColumnStretch(1, 2)
        gl_.setColumnStretch(2, 1)

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
                        lb_.setWordWrap(True)
                        if meta.get('bold_label'):
                            font = lb_.font()
                            font.setBold(True)
                            lb_.setFont(font)
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
                    elif meta.get('type') == 'checkbox_group':
                        try:
                            mask = int(meta.get('value', meta.get('default', 7)))
                        except (TypeError, ValueError):
                            mask = 7
                        cbs = []
                        item_labels = []
                        for idx, text in enumerate(meta.get('list') or []):
                            r_ += 1
                            item_lb = QLabel(str(text))
                            item_lb.setWordWrap(True)
                            item_lb.setContentsMargins(18, 0, 0, 0)
                            gl_.addWidget(item_lb, r_, 1)
                            item_labels.append(item_lb)
                            cb = QCheckBox(self)
                            cb.setChecked(bool(mask & (1 << idx)))
                            gl_.addWidget(cb, r_, 2)
                            cbs.append(cb)
                        meta['obj'] = cbs
                        meta['item_label_objs'] = item_labels
                    elif meta.get('type') == 'radio':
                        bg = QButtonGroup(self)
                        bg.setExclusive(True)
                        if meta.get('layout') == 'horizontal':
                            radio_layout = QHBoxLayout()
                            radio_layout.setSpacing(16)
                        else:
                            radio_layout = QVBoxLayout()
                        radio_layout.setContentsMargins(0, 0, 0, 0)
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
                            radio_layout.addWidget(rb)
                        if meta.get('layout') == 'horizontal':
                            radio_layout.addStretch(1)
                        if bg.checkedId() < 0 and bg.buttons():
                            bg.button(0).setChecked(True)
                        meta['obj'] = bg
                        gl_.addLayout(radio_layout, r_, 1, 1, 2)
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
                        _narrow_value_widget(sp)
                        meta['obj'] = sp
                        gl_.addWidget(sp, r_, 2)
                    elif meta.get('type') == 'spin':
                        sp = QSpinBox(self)
                        sp.setMinimum(int(meta.get('min', 0)))
                        sp.setMaximum(int(meta.get('max', 100)))
                        sp.setSingleStep(int(meta.get('step', 1)))
                        try:
                            sp.setValue(int(float(meta.get('value', meta.get('default', 0)))))
                        except (TypeError, ValueError):
                            sp.setValue(int(meta.get('default', 0)))
                        _narrow_value_widget(sp)
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
                        if meta.get('expand'):
                            host = _expanding_value_host(cmb_)
                            meta['obj'] = cmb_
                            meta['value_host'] = host
                            gl_.addWidget(host, r_, 2)
                        else:
                            _narrow_value_widget(cmb_)
                            meta['obj'] = cmb_
                            gl_.addWidget(cmb_, r_, 2)

                    else:
                        le_ = QLineEdit(str(meta['value']))
                        le_.setObjectName('le_' + item_j.lower())
                        meta['obj'] = le_
                        gl_.addWidget(le_, r_, 2)
   

        r_ += 1
        frame2 = QFrame(self)
        frame2.setFrameShape(QFrame.Shape.HLine)
        gl_.addWidget(frame2, r_, 0, 1, 3)

        r_ += 1
        gl_.setRowStretch(r_, 1)

        r_ += 1
        hl_ = QHBoxLayout()


        self.pb_rest = QPushButton(tr_ui('Restaurar'), self)
        self.pb_rest.setToolTip(tr_ui('Repõe todos os parâmetros desta janela nos valores padrão.'))
        # self.pb_remove.setEnabled(False)
        hl_.addWidget(self.pb_rest)

        self.pb_save = QPushButton(tr_ui('Salvar'), self)
        self.pb_save.setToolTip(tr_ui('Grava os parâmetros no projeto (.pa.gpkg) e fecha a janela.'))
        # self.pb_save.setEnabled(False)
        hl_.addWidget(self.pb_save)

        gl_.addLayout(hl_, r_, 1, 1, 2)


        base_widget = QWidget()
        base_widget.setLayout(gl_)

        sla_ = QScrollArea(self)
        # gl_.addWidget(sla_)
        # sla_.setLayout(gl_)
        sla_.setWidgetResizable(True)
        sla_.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        sla_.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        sla_.setWidget(base_widget)

        vl_ = QVBoxLayout(self)
        vl_.addWidget(sla_)

        self.trigger_actions()
        self._wire_accuracy_standard_visibility()
        self._apply_param_tooltips()
        self._wire_param_change_clear_highlight()
        return vl_

    def _wire_param_change_clear_highlight(self):
        """Ao alterar qualquer parâmetro, remove o destaque de extensão da amostra no painel."""
        parent = self.parent
        if parent is None or not hasattr(parent, '_clear_sample_extent_highlight'):
            return
        clear_hl = parent._clear_sample_extent_highlight
        for item_i, block in self.dic_param.items():
            if not isinstance(item_i, str) or not item_i.startswith('step_'):
                continue
            if not isinstance(block, dict) or 'fields' not in block:
                continue
            for _fj, meta in block['fields'].items():
                if not isinstance(meta, dict):
                    continue
                obj = meta.get('obj')
                if obj is None:
                    continue
                ftype = meta.get('type')
                if meta.get('type') == 'checkbox':
                    try:
                        obj.toggled.disconnect(clear_hl)
                    except (TypeError, RuntimeError):
                        pass
                    obj.toggled.connect(clear_hl)
                elif meta.get('type') == 'checkbox_group':
                    for cb in (obj or []):
                        try:
                            cb.toggled.disconnect(clear_hl)
                        except (TypeError, RuntimeError):
                            pass
                        cb.toggled.connect(clear_hl)
                elif ftype == 'radio':
                    try:
                        obj.idClicked.disconnect(clear_hl)
                    except (TypeError, RuntimeError):
                        pass
                    try:
                        obj.buttonClicked.disconnect(clear_hl)
                    except (TypeError, RuntimeError):
                        pass
                    # Qt5: buttonClicked; Qt6: idClicked — ligar o que existir
                    if hasattr(obj, 'idClicked'):
                        obj.idClicked.connect(clear_hl)
                    else:
                        obj.buttonClicked.connect(clear_hl)
                elif ftype in ('spin', 'doublespin'):
                    try:
                        obj.valueChanged.disconnect(clear_hl)
                    except (TypeError, RuntimeError):
                        pass
                    obj.valueChanged.connect(clear_hl)
                elif 'list' in meta:
                    try:
                        obj.currentIndexChanged.disconnect(clear_hl)
                    except (TypeError, RuntimeError):
                        pass
                    obj.currentIndexChanged.connect(clear_hl)
                else:
                    try:
                        obj.textChanged.disconnect(clear_hl)
                    except (TypeError, RuntimeError):
                        pass
                    obj.textChanged.connect(clear_hl)

    def _apply_param_tooltips(self):
        """Aplica tooltip do campo ao rótulo e ao widget (e à secção)."""
        for item_i, block in self.dic_param.items():
            if not isinstance(item_i, str) or not item_i.startswith('step_'):
                continue
            sec_tip = block.get('tooltip') or ''
            lb_sec = block.get('label_obj')
            if lb_sec is not None and sec_tip:
                lb_sec.setToolTip(sec_tip)
            for _fj, meta in (block.get('fields') or {}).items():
                if not isinstance(meta, dict):
                    continue
                tip = meta.get('tooltip') or ''
                if not tip:
                    continue
                lb_f = meta.get('label_obj')
                if lb_f is not None:
                    lb_f.setToolTip(tip)
                obj = meta.get('obj')
                if obj is None:
                    continue
                if meta.get('type') == 'radio':
                    per_opt = meta.get('tooltips') or []
                    for btn in obj.buttons():
                        idx = obj.id(btn)
                        if 0 <= idx < len(per_opt) and per_opt[idx]:
                            continue
                        if not btn.toolTip():
                            btn.setToolTip(tip)
                elif meta.get('type') == 'checkbox_group':
                    for cb in (obj or []):
                        cb.setToolTip(tip)
                    for lb_item in meta.get('item_label_objs') or []:
                        lb_item.setToolTip(tip)
                else:
                    obj.setToolTip(tip)

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
        elif meta.get('type') == 'checkbox_group':
            for cb in (obj or []):
                cb.setVisible(visible)
            for lb_item in meta.get('item_label_objs') or []:
                lb_item.setVisible(visible)
        else:
            host = meta.get('value_host')
            if host is not None:
                host.setVisible(visible)
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
        for key in ('ce90_max_h', 'ce90_max_v', 'ce90_pass_criteria'):
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
                elif meta.get('type') == 'checkbox_group':
                    mask = 0
                    for idx, cb in enumerate(obj or []):
                        if cb.isChecked():
                            mask |= (1 << idx)
                    value_ = mask
                elif meta.get('type') == 'radio':
                    value_ = obj.checkedId()
                    if value_ < 0:
                        value_ = 0
                elif meta.get('type') == 'doublespin':
                    value_ = float(obj.value())
                elif meta.get('type') == 'spin':
                    value_ = int(obj.value())
                elif 'list' in meta:
                    value_ = obj.currentIndex()
                else:
                    value_ = obj.text()
                meta['value'] = value_
                if log_values:
                    self.parent.log_message(f'{item_i} - {item_j} : "{value_}"')

    def set_dic_param(self):
        self.parent.persist_project_config_from_widgets(log_values=True)
        if hasattr(self.parent, '_clear_sample_extent_highlight'):
            self.parent._clear_sample_extent_highlight()
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
                    elif meta.get('type') == 'checkbox_group':
                        try:
                            mask = int(default_)
                        except (TypeError, ValueError):
                            mask = 7
                        for idx, cb in enumerate(obj or []):
                            cb.setChecked(bool(mask & (1 << idx)))
                    elif meta.get('type') == 'radio':
                        btn = obj.button(int(default_))
                        if btn is None and obj.buttons():
                            btn = obj.button(0)
                        if btn is not None:
                            btn.setChecked(True)
                    elif meta.get('type') == 'doublespin':
                        obj.setValue(float(default_))
                    elif meta.get('type') == 'spin':
                        obj.setValue(int(default_))
                    elif 'list' in meta:
                        obj.setCurrentIndex(default_)
                    else:
                        obj.setText(str(default_))
                    meta['value'] = default_
        self._sync_buffer_standard_visibility()
        if hasattr(self.parent, '_clear_sample_extent_highlight'):
            self.parent._clear_sample_extent_highlight()

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

