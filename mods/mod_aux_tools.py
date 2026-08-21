# -*- coding: utf-8 -*-
import json

from qgis.PyQt.QtCore import QSettings

from .plugin_i18n import SETTINGS_APP, SETTINGS_ORG
from .mod_exc import ignore_exc


class AuxTools:
    def __init__(self, iface=None, parent=None):
        self.iface = iface
        self.parent = parent
        self.menu_ = f'{self.parent.objectName()}' if parent is not None else 'AuxTools'
        # Mesmo armazenamento do resto do plugin (locale, etc.)
        self.settings = QSettings(SETTINGS_ORG, SETTINGS_APP)

    def save_geometry(self, wd_=None):
        target = wd_ if wd_ is not None else self.parent
        if target is None:
            return
        self.settings.setValue(f'{self.menu_}/geometry', target.saveGeometry())

    def get_geometry(self):
        key_new = f'{self.menu_}/geometry'
        geom = self.settings.value(key_new)
        if geom:
            return geom
        # Compat: chave antiga no mesmo QSettings
        geom = self.settings.value(f'{self.menu_}/geom')
        if geom:
            return geom
        # Compat: QSettings(objectName) usado em versões anteriores
        try:
            legacy = QSettings(self.menu_)
            geom = legacy.value(f'{self.menu_}/geom') or legacy.value('geom')
            if geom:
                self.settings.setValue(key_new, geom)
                return geom
        except Exception as _exc:
            ignore_exc(_exc)
        return None

    def get_(self, key_=''):
        path = f'{self.menu_}/{key_}'
        v = self.settings.value(path)
        if v is not None and v != '':
            return v
        try:
            legacy = QSettings(self.menu_)
            return legacy.value(path)
        except Exception:
            return None

    def save_(self, value_='', key_=''):
        self.settings.setValue(f'{self.menu_}/{key_}', value_)

    def save_dic(self, dic_={}, key_=''):
        str_dic = json.dumps(dic_)
        self.settings.setValue(f'{self.menu_}/{key_}', str_dic)

    def get_dic(self, key_=''):
        path = f'{self.menu_}/{key_}'
        str_dic = self.settings.value(path)
        if not str_dic or str_dic == '{}':
            try:
                legacy = QSettings(self.menu_)
                str_dic = legacy.value(path)
            except Exception:
                str_dic = None
        if str_dic and str_dic != '{}':
            try:
                return json.loads(str_dic)
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
        return {}

    def get_w_size(self):
        try:
            dw = int(self.settings.value(f'{self.menu_}/width'))
            dh = int(self.settings.value(f'{self.menu_}/height'))
            x0 = int(self.settings.value(f'{self.menu_}/x'))
            y0 = int(self.settings.value(f'{self.menu_}/y'))
            if y0:
                return x0, y0, dw, dh
        except Exception as _exc:
            ignore_exc(_exc)
        dw, dh, x0, y0 = 372, 265, 100, 100
        self.settings.setValue(f'{self.menu_}/width', dw)
        self.settings.setValue(f'{self.menu_}/height', dh)
        self.settings.setValue(f'{self.menu_}/x', x0)
        self.settings.setValue(f'{self.menu_}/y', y0)
        return x0, y0, dw, dh

    def save_w_size(self, wd_=None):
        if not wd_:
            return
        self.settings.setValue(f'{self.menu_}/x', wd_.pos().x())
        self.settings.setValue(f'{self.menu_}/y', wd_.pos().y())
        self.settings.setValue(f'{self.menu_}/width', wd_.width())
        self.settings.setValue(f'{self.menu_}/height', wd_.height())
