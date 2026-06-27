# -*- coding: utf-8 -*-
"""Janela de seleção de idioma da interface do plugin."""
from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtWidgets import (
    QComboBox, QDialog, QFrame, QGridLayout, QLabel, QPushButton, QVBoxLayout, QWidget,
)

from .mod_aux_tools import AuxTools
from .plugin_i18n import (
    LOCALE_AUTO,
    locale_qgis_option_label,
    saved_ui_locale,
    save_ui_locale,
    tr_ui,
    translation_file_status,
    ui_locale_choices,
)


class LanguageDlg(QDialog):
    """Idioma da interface (separado dos parâmetros de processamento)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        self.setObjectName('LanguageDlg')
        self.setWindowTitle(tr_ui('Idioma da interface'))
        self.aux_tools = AuxTools(parent=self)
        geom = self.aux_tools.get_geometry()
        if geom:
            self.restoreGeometry(geom)
        else:
            self.setGeometry(120, 120, 480, 120)
        self._ui_locale_ready = False
        self.setLayout(self._build_layout())
        self._ui_locale_ready = True

    def _build_layout(self):
        gl_ = QGridLayout()
        lb_lang = QLabel(tr_ui('Idioma da interface'))
        lb_lang.setObjectName('lb_ui_locale')
        gl_.addWidget(lb_lang, 0, 0)
        self.cmb_locale = QComboBox(self)
        self._populate_locale_combo(select_code=saved_ui_locale() or LOCALE_AUTO)
        gl_.addWidget(self.cmb_locale, 0, 1)
        self.lb_locale_qm_status = QLabel()
        self.lb_locale_qm_status.setObjectName('lb_locale_qm_status')
        self.lb_locale_qm_status.setWordWrap(True)
        gl_.addWidget(self.lb_locale_qm_status, 1, 0, 1, 2)
        self._refresh_locale_qm_status()
        self.cmb_locale.currentIndexChanged.connect(self._on_ui_locale_changed)

        frame = QFrame(self)
        frame.setFrameShape(QFrame.HLine)
        pb_close = QPushButton(tr_ui('Fechar'), self)
        pb_close.clicked.connect(self.close)

        base = QWidget()
        base.setLayout(gl_)
        vl = QVBoxLayout(self)
        vl.addWidget(base)
        vl.addWidget(frame)
        vl.addWidget(pb_close, 0, Qt.AlignRight)
        return vl

    def _populate_locale_combo(self, select_code: str = None):
        if select_code is None:
            select_code = saved_ui_locale() or LOCALE_AUTO
        self.cmb_locale.blockSignals(True)
        self.cmb_locale.clear()
        self.cmb_locale.addItem(locale_qgis_option_label(), LOCALE_AUTO)
        for code, label in ui_locale_choices():
            self.cmb_locale.addItem(label, code)
        loc_idx = self.cmb_locale.findData(select_code)
        self.cmb_locale.setCurrentIndex(loc_idx if loc_idx >= 0 else 0)
        self.cmb_locale.blockSignals(False)

    def _current_locale_code(self) -> str:
        code = self.cmb_locale.currentData()
        if code is None:
            code = self.cmb_locale.itemData(self.cmb_locale.currentIndex())
        return code or LOCALE_AUTO

    def _refresh_locale_qm_status(self):
        status = translation_file_status(self._current_locale_code())
        self.lb_locale_qm_status.setText(status['label'])
        self.lb_locale_qm_status.setToolTip(status['tooltip'])
        if status.get('is_dev_source'):
            self.lb_locale_qm_status.setStyleSheet('color: palette(text);')
        elif status['found']:
            self.lb_locale_qm_status.setStyleSheet('color: #1a7f1a;')
        else:
            self.lb_locale_qm_status.setStyleSheet('color: #aa6600;')

    def _on_ui_locale_changed(self, _index=0):
        if not self._ui_locale_ready:
            return
        save_ui_locale(self._current_locale_code())
        self._refresh_locale_qm_status()
        if self.parent:
            self.parent.apply_ui_language(
                refresh_open_language=True, refresh_open_settings=True)

    def apply_language_live(self):
        """Actualiza textos desta janela após mudança de idioma."""
        self.setWindowTitle(tr_ui('Idioma da interface'))
        lb_lang = self.findChild(QLabel, 'lb_ui_locale')
        if lb_lang is not None:
            lb_lang.setText(tr_ui('Idioma da interface'))
        self._populate_locale_combo(select_code=self._current_locale_code())
        self._refresh_locale_qm_status()

    def closeEvent(self, evt):
        self.aux_tools.save_geometry(self)
        super().closeEvent(evt)
