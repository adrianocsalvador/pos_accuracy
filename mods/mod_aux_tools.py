# -*- coding: utf-8 -*-
import os

from PyQt5.QtCore import QSettings, QPropertyAnimation, QRect
# import getpass
# import tempfile
# import logging
# from logging.handlers import RotatingFileHandler
# from datetime import datetime
# import pprint
import json

class AuxTools:
    def __init__(self, iface=None, parent=None):
        self.iface = iface
        self.parent = parent
        self.menu_ = f'{self.parent.objectName()}'
        self.settings = QSettings(self.menu_)


    def save_geometry(self, wd_=None):
        self.settings.setValue(f"{self.menu_}/geom", wd_.saveGeometry())

    def get_geometry(self):
        return self.settings.value(f"{self.menu_}/geom")

    def get_(self, key_=''):
        print("get_", f"{self.menu_}/{key_}")
        lc_ = self.settings.value(f"{self.menu_}/{key_}")
        return lc_

    def save_(self, value_='', key_=''):
        print('save_', f"{self.menu_}/{key_}")
        self.settings.setValue(f"{self.menu_}/{key_}", value_)

    def save_dic(self, dic_={}, key_=''):
        print('save_dic', f"{self.menu_}/{key_}")
        str_dic = json.dumps(dic_)
        self.settings.setValue(f"{self.menu_}/{key_}", str_dic)

    def get_dic(self, key_=''):
        print('get_dic', f"{self.menu_}/{key_}")
        str_dic = self.settings.value(f"{self.menu_}/{key_}")
        if str_dic and str_dic != '{}':
            dic_base = json.loads(str_dic)
        else:
            dic_base = {}
        return dic_base

    def get_w_size(self):
        print("02-getting_w_size")
        try:
            dw = int(self.settings.value(f"{self.menu_}/width"))
            dh = int(self.settings.value(f"{self.menu_}/height"))
            x0 = int(self.settings.value(f"{self.menu_}/x"))
            y0 = int(self.settings.value(f"{self.menu_}/y"))
            print("-->", x0, y0, dw, dh)
            if y0:
                return x0, y0, dw, dh
        except:
            pass
        dw = 372
        dh = 265
        x0 = 100
        y0 = 100
        self.settings.setValue(f"{self.menu_}/width", dw)
        self.settings.setValue(f"{self.menu_}/height", dh)
        self.settings.setValue(f"{self.menu_}/x", x0)
        self.settings.setValue(f"{self.menu_}/y", y0)
        return x0, y0, dw, dh

    def save_w_size(self, wd_=None):
        if wd_:
            print("save_w_size")
            x0 = wd_.pos().x()
            y0 = wd_.pos().y()
            dw = wd_.width()
            dh = wd_.height()
            self.settings.setValue(f"{self.menu_}/x", x0)
            self.settings.setValue(f"{self.menu_}/y", y0)
            self.settings.setValue(f"{self.menu_}/width", dw)
            self.settings.setValue(f"{self.menu_}/height", dh)
            print('save<--', x0, y0, dw, dh, f"{self.menu_}/x")



# class Logger:
#     _instance = None  # Armazena a instância única do logger
#
#     def __new__(cls, plugin_name: str, log_dir: str = None):
#         """ Implementa o padrão Singleton para evitar múltiplas instâncias. """
#         if cls._instance is None:
#             cls._instance = super().__new__(cls)
#             cls._instance._initialize(plugin_name, log_dir)
#         return cls._instance
#
#     def _initialize(self, plugin_name: str, log_dir: str):
#         """ Inicializa o logger apenas uma vez. """
#         self.plugin_name = plugin_name
#         # self.user_target = user_target
#         # self.current_user = getpass.getuser()
#
#         # Define o diretório de logs (padrão: diretório temporário do sistema)
#         if log_dir is None:
#             log_dir = tempfile.gettempdir()  # Usa o diretório temporário do sistema
#         os.makedirs(log_dir, exist_ok=True)
#
#         # Nome do arquivo de log com timestamp
#         timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#         log_dir = os.path.join(log_dir, f'LOG_{plugin_name}')
#         print('log_dir', log_dir)
#         if not os.path.exists(log_dir):
#             os.mkdir(log_dir)
#         log_file = os.path.join(log_dir, f"{plugin_name}_{timestamp}.log")
#
#         # Criar logger
#         self.logger = logging.getLogger(plugin_name)
#         self.logger.setLevel(logging.DEBUG)
#         self.logger.handlers.clear()  # Evita múltiplos handlers
#
#         # Formatter
#         formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
#
#         # Console Handler
#         # if self.current_user.lower() == self.user_target.lower():
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(logging.INFO)
#         console_handler.setFormatter(formatter)
#         self.logger.addHandler(console_handler)
#
#         # File Handler (apenas para o usuário alvo)
#         # if self.current_user.lower() == self.user_target.lower():
#             # file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5)
#         file_handler = RotatingFileHandler(log_file)
#         file_handler.setLevel(logging.DEBUG)
#         file_handler.setFormatter(formatter)
#         self.logger.addHandler(file_handler)
#
#     @classmethod
#     def log(cls, level: str, chk: bool, *args, pretty: bool = False, fmt: str = ''):
#         """
#         Loga a mensagem usando formatação preguiçosa. Se `pretty` for True, aplica pprint.pformat
#         em cada argumento.
#         """
#
#         if cls._instance is not None and chk:
#             logger = cls._instance.logger
#             if pretty:
#                 # Converte cada argumento com pprint.pformat
#                 args = tuple(pprint.pformat(arg) for arg in args)
#             # O logger fará a conversão dos placeholders só se necessário
#             if not fmt:
#                 fmt = " ".join("%s" for _ in args) if args else ""
#             getattr(logger, level.lower(), logger.info)(fmt, *args)
#
#     @classmethod
#     def debug(cls, chk: bool, *args, pretty: bool = False):
#         cls.log("debug", chk, *args, pretty=pretty)
#
#     @classmethod
#     def info(cls, chk: bool, *args, pretty: bool = False):
#         cls.log("info", chk, *args, pretty=pretty)
#
#     @classmethod
#     def warning(cls, chk: bool, *args, pretty: bool = False):
#         cls.log("warning", chk, *args, pretty=pretty)
#
#     @classmethod
#     def error(cls, chk: bool, *args, pretty: bool = False):
#         cls.log("error", chk, *args, pretty=pretty)
#
#     @classmethod
#     def critical(cls, chk: bool, *args, pretty: bool = False):
#         cls.log("critical", chk, *args, pretty=pretty)
