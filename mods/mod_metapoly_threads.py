import os
import shutil

from PyQt5.QtCore import QThread, pyqtSignal, QRunnable, QObject
from qgis import processing
from qgis.core import QgsCoordinateReferenceSystem, QgsFeature


class PCThread(QThread):
    sig_status = pyqtSignal(dict, name='Status for processing bar')

    def __init__(self, main, parent, key_=None, dic_=None):
        QThread.__init__(self, parent)

        self.main = main
        self.parent = parent
        self.key_ = key_
        self.file_path = dic_['file_path']
        self.dic_db = dic_['info']
        self.db = dic_['db']
        self.dest_ = dic_['dest']
        self.log = dic_['log']
        self.date = dic_['date']
        self.srid = dic_['srid']
        self.fuse = dic_['fuse']
        self.log.info(True, f'PCThread: {key_} __init__', pretty=True)
        # self.stop = False
        self.nr_procs = 0
        self.nr_procs += 7 if self.db else 0
        self.nr_procs += 1 if self.dest_ else 0
        self.dic_path_aliases = {
            '//bsbtopo08/dados': '//mnt/tiles',
        }

    def run(self):
        type_ = 'copc' if 'copc.la' in self.file_path else 'pdal'
        self.sig_status.emit({'key': self.key_, 'quant': self.nr_procs})
        nr_ = 0
        tool_ = ''
        geom_ = None
        if self.dest_:
            nr_ += 1
            if type_ == 'copc':
                if os.path.exists(os.path.join(self.dest_, os.path.basename(self.file_path))):
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'warn': 'Arquivo já existe no destino'})
                    self.log.warning(True, f'PCThread: {self.key_} copy: Arquivo já existe no destino',
                                     pretty=True)
                else:
                    shutil.copy2(self.file_path, self.dest_)
                    self.log.info(True, f'PCThread: {self.key_} copied', pretty=True)
            else:
                if os.path.exists(os.path.join(self.dest_, f'{os.path.basename(self.file_path)[:-4]}.copc.laz')):
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'warn': 'Arquivo já existe no destino'})
                    self.log.warning(True, f'PCThread: {self.key_} copc create: Arquivo já existe no destino',
                                     pretty=True)
                else:
                    try:
                        file_ = self.file_path.replace("\\", "/")
                        params = {
                            'LAYERS': [f'pdal://{file_}'],
                            'OUTPUT': self.dest_}
                        tool_ = "pdal:createcopc"
                        processing.run(tool_, params)
                        self.log.info(True, f'PCThread: {self.key_} copc created', pretty=True)
                        self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    except Exception as e:
                        self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                        self.log.error(True, f'PCThread: {self.key_} copc create: {e}', pretty=True)
                        return
        if self.db:
            # Get db data
            self.log.info(True, f'PCThread: db = {self.key_}', pretty=True)
            sch_ = self.dic_db['sch']
            tab_ = self.dic_db['tab']
            fld_name = self.dic_db['fields']['fld_name']
            name_ = os.path.basename(self.file_path) if type_ == 'copc' else \
                f'{os.path.basename(self.file_path)[:-4]}.copc.laz'
            fld_path = self.dic_db['fields']['fld_path']
            path_ = os.path.join(self.dest_, name_) if self.dest_ else ''
            sql_ = f"""SELECT 1 FROM {sch_}.{tab_} t WHERE t.{fld_name} = '{name_}';"""
            result_ = self.db.select_(sql_)
            print(result_)
            self.log.info(True, f'PCThread: {self.key_} result = {result_}', pretty=True)
            if result_ and result_[0]:
                self.log.warning(True, f'PCThread: {self.key_} db: Arquivo {name_} já existe no DB',
                                 pretty=True)
            else:
                # 1 "pdal:boundary"
                try:
                    nr_ += 1
                    params = {
                        'INPUT': f'{type_}://{self.file_path}',
                        'RESOLUTION': 0.5,
                        'THRESHOLD': 1,
                        'FILTER_EXPRESSION': '',
                        'FILTER_EXTENT': None,
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "pdal:boundary"
                    self.log.info(True, f'PCThread: {self.key_} {tool_} started->', pretty=True)
                    result_boundary = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 2 "native:convexhull"
                try:
                    nr_ += 1
                    params = {
                        'INPUT': result_boundary['OUTPUT'],
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "native:convexhull"
                    result_cvhull = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 3 "native:assignprojection"
                try:
                    nr_ += 1
                    params = {
                        'INPUT': result_cvhull['OUTPUT'],
                        'CRS': QgsCoordinateReferenceSystem(f'EPSG:{self.srid}'),
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "native:assignprojection"
                    result_setpro = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 4 "native:reprojectlayer"
                try:
                    nr_ += 1
                    params = {
                        'INPUT': result_setpro['OUTPUT'],
                        'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4674'),
                        'CONVERT_CURVED_GEOMETRIES': False,
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "native:reprojectlayer"
                    result_repro = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 5 "native:buffer"
                try:
                    nr_ += 1
                    params = {
                        'INPUT': result_repro['OUTPUT'],
                        'DISTANCE': 0,
                        'SEGMENTS': 5,
                        'END_CAP_STYLE': 0,
                        'JOIN_STYLE': 0,
                        'MITER_LIMIT': 2,
                        'DISSOLVE': True,
                        'SEPARATE_DISJOINT': False,
                        'OUTPUT': 'TEMPORARY_OUTPUT'}
                    tool_ = "native:buffer"
                    result_bff = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                wkt_ = ''
                # 6 'wkt'
                try:
                    nr_ += 1
                    layer_ = result_bff['OUTPUT']
                    tool_ = 'wkt'
                    for i, feat_ in enumerate(layer_.getFeatures()):
                        geom_ = feat_.geometry()
                        geom_.convertToSingleType()
                        wkt_ = geom_.asWkt()
                        self.sig_status.emit({
                            'key': self.key_,
                            'value': nr_,
                            'msg': f'WKT feature {i + 1} / {len(layer_)}'
                        })
                        self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                        self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 7 'sql_insert'
                try:
                    nr_ += 1
                    tool_ = 'sql_insert'
                    fld_type = self.dic_db['fields']['fld_type']
                    fld_date = self.dic_db['fields']['fld_date']
                    fld_valid = self.dic_db['fields']['fld_valid']
                    fld_srid = self.dic_db['fields']['fld_srid']
                    server_path = path_
                    for _key_ in self.dic_path_aliases:
                        # print(_key_, self.dic_path_aliases[_key_])
                        server_path = server_path.replace(_key_, self.dic_path_aliases[_key_])
                    sql_ = f"""
                        INSERT INTO {sch_}.{tab_} (geom, {fld_name}, {fld_path}, {fld_type}, {fld_date}, {fld_valid}, {fld_srid})
                            SELECT 
                                ST_GeomFromText('{wkt_}') geom, 
                                '{name_}' {fld_name},
                                '{server_path if path_ else '-'}' {fld_path},
                                'LAS-ALS' {fld_type},
                                '{self.date}' {fld_date},
                                True {fld_valid},
                                {'Null' if self.srid == '-' else self.srid} {fld_srid}
                            RETURNING fid;"""
                    self.log.debug(True, f'PCThread: {self.key_} sql_= {sql_}', )
                    result_ = self.db.select_(sql_)
                    if result_ and result_[0]:
                        feat_ = QgsFeature()
                        if geom_:
                            feat_.setGeometry(geom_)
                            feat_.setAttributes([self.key_ + 1, name_, server_path, 'LAS-ALS', self.date, True,
                                                 self.srid])
                        self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'feat': feat_})
                        self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                        # self.db.commit_()
                        # self.db.close_()
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

        if self.nr_procs:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': ':) FINALIZADO (:'
            })
        else:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': 'NENHUM PROCESSO SELECIONADO'
            })


class TIFThread(QThread):
    sig_status = pyqtSignal(dict, name='Status for processing bar')

    def __init__(self, main, parent, key_=None, dic_=None):
        QThread.__init__(self, parent)

        self.main = main
        self.parent = parent
        self.key_ = key_
        self.file_path = dic_['file_path']
        self.dic_db = dic_['info']
        self.db = dic_['db']
        self.dest_ = dic_['dest']
        self.log = dic_['log']
        self.date = dic_['date']
        self.srid = dic_['srid']
        self.log.info(True, f'TIFThread: {key_} __init__', pretty=True)
        # self.stop = False
        self.nr_procs = 0
        self.nr_procs += 7 if self.db else 0
        self.nr_procs += 1 if self.dest_ else 0
        self.dic_path_aliases = {
            '//bsbtopo08/dados': '//mnt/tiles',
        }

    def run(self):
        self.sig_status.emit({'key': self.key_, 'quant': self.nr_procs})
        nr_ = 0
        tool_ = ''
        if self.dest_:
            nr_ += 1
            if os.path.exists(os.path.join(self.dest_, os.path.basename(self.file_path))):
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'warn': 'Arquivo já existe no destino'})
                self.log.warning(True, f'TIFThread: {self.key_} copy: Arquivo já existe no destino',
                                 pretty=True)
            else:
                shutil.copy2(self.file_path, self.dest_)
                self.log.info(True, f'TIFThread: {self.key_} copied', pretty=True)

        if self.db:
            # Get db data
            sch_ = self.dic_db['sch']
            tab_ = self.dic_db['tab']
            fld_name = self.dic_db['fields']['fld_name']
            name_ = os.path.basename(self.file_path)
            fld_path = self.dic_db['fields']['fld_path']
            path_ = os.path.join(self.dest_, name_) if self.dest_ else ''
            sql_ = f"""SELECT 1 FROM {sch_}.{tab_} t WHERE t.{fld_name} = '{name_}';"""
            result_ = self.db.select_(sql_)
            self.log.info(True, f'PCThread: {self.key_} result = {result_}', pretty=True)
            if result_ and result_[0]:
                self.log.warning(True, f'TIFThread: {self.key_} db: Arquivo {name_} já existe no DB',
                                 pretty=True)
            else:
                # 1 "gdal:rastercalculator"
                try:
                    nr_ += 1  # 1
                    # mdt_layer = QgsRasterLayer(self.file_path, 'MDT')
                    params = {
                        'INPUT_A': f'{self.file_path}',
                        'BAND_A': 1,
                        'INPUT_B': None, 'BAND_B': None,
                        'INPUT_C': None, 'BAND_C': None,
                        'INPUT_D': None, 'BAND_D': None,
                        'INPUT_E': None, 'BAND_E': None,
                        'INPUT_F': None, 'BAND_F': None,
                        'FORMULA': 'A > -100',
                        'NO_DATA': None,
                        'EXTENT_OPT': 0,
                        'PROJWIN': None,
                        'RTYPE': 11,
                        'OPTIONS': None,
                        'EXTRA': '',
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "gdal:rastercalculator"
                    result_calc = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'TIFThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'TIFThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 2 "gdal:polygonize"
                try:
                    nr_ += 1  # 2
                    params = {
                        'INPUT': result_calc['OUTPUT'],
                        'BAND': 1,
                        'FIELD': 'DN',
                        'EIGHT_CONNECTEDNESS': False,
                        'EXTRA': '',
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "gdal:polygonize"
                    result_poly = processing.run(tool_, params)
                    print('result_poly', result_poly)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'TIFThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'TIFThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 3 "native:assignprojection"
                try:
                    nr_ += 1
                    params = {
                        'INPUT': result_poly['OUTPUT'],
                        'CRS': QgsCoordinateReferenceSystem(f'EPSG:{self.srid}'),
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "native:assignprojection"
                    result_setpro = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 4 "native:reprojectlayer"
                try:
                    nr_ += 1
                    params = {
                        'INPUT': result_setpro['OUTPUT'],
                        'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4674'),
                        'CONVERT_CURVED_GEOMETRIES': False,
                        'OUTPUT': 'TEMPORARY_OUTPUT'
                    }
                    tool_ = "native:reprojectlayer"
                    result_repro = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'PCThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 5 "native:buffer"
                try:
                    nr_ += 1  # 5
                    params = {
                        'INPUT': result_repro['OUTPUT'],
                        'DISTANCE': 0,
                        'SEGMENTS': 5,
                        'END_CAP_STYLE': 0,
                        'JOIN_STYLE': 0,
                        'MITER_LIMIT': 2,
                        'DISSOLVE': True,
                        'SEPARATE_DISJOINT': False,
                        'OUTPUT': 'TEMPORARY_OUTPUT'}
                    tool_ = "native:buffer"
                    result_bff = processing.run(tool_, params)
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                    self.log.info(True, f'TIFThread: {self.key_} {tool_}', pretty=True)
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'TIFThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                wkt_ = ''
                # 6 'wkt'
                try:
                    nr_ += 1  # 6
                    layer_ = result_bff['OUTPUT']
                    # layer_ = result_poly['OUTPUT']
                    tool_ = 'wkt'
                    for i, feat_ in enumerate(layer_.getFeatures()):
                        geom_ = feat_.geometry()
                        geom_.convertToSingleType()
                        wkt_ = geom_.asWkt()
                        self.sig_status.emit({
                            'key': self.key_,
                            'value': nr_,
                            'msg': f'WKT feature {i + 1} / {len(layer_)}'
                        })
                        self.log.info(True, f'TIFThread: {self.key_} {tool_}', pretty=True)
                        self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'TIFThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

                # 7 'sql_insert'
                try:
                    nr_ += 1
                    tool_ = 'sql_insert'
                    fld_type = self.dic_db['fields']['fld_type']
                    fld_date = self.dic_db['fields']['fld_date']
                    fld_valid = self.dic_db['fields']['fld_valid']
                    fld_srid = self.dic_db['fields']['fld_srid']
                    server_path = path_
                    for _key_ in self.dic_path_aliases:
                        print(_key_, self.dic_path_aliases[_key_])
                        server_path = server_path.replace(_key_, self.dic_path_aliases[_key_])
                    
                    sql_ = f"""
                        INSERT INTO {sch_}.{tab_} (geom, {fld_name}, {fld_path}, {fld_type}, {fld_date}, {fld_valid}, {fld_srid})
                            SELECT 
                                ST_GeomFromText('{wkt_}') geom, 
                                '{name_}' {fld_name},
                                '{server_path}' {fld_path},
                                'MDT-TIF' {fld_type},
                                '{self.date}' {fld_date},
                                True {fld_valid},
                                {self.srid} {fld_srid}
                            RETURNING fid;"""
                    result_ = self.db.select_(sql_)
                    if result_ and result_[0]:
                        feat_ = QgsFeature()
                        if geom_:
                            feat_.setGeometry(geom_)
                            feat_.setAttributes([self.key_ + 1, name_, server_path, 'LAS-ALS', self.date, True, self.srid])

                        print('resultado:', result_[0])
                        self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'feat': feat_})
                        self.log.info(True, f'PCThread: {self.key_} {tool_}', pretty=True)

                        # self.db.commit_()
                except Exception as e:
                    self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                    self.log.error(True, f'TIFThread: {self.key_} {tool_}: {e}', pretty=True)
                    return

        if self.nr_procs:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': ':) FINALIZADO (:'
            })
        else:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': 'NENHUM PROCESSO SELECIONADO'
            })


class Worker(QObject):
    """ Worker that manages a processing thread and signals when it's done """
    finished = pyqtSignal(int)  # Signal to notify when a task is done

    def __init__(self, key_, dic_, parent):
        super().__init__()
        self.key_ = key_
        self.dic_ = dic_
        self.parent = parent  # Reference to the main class
        self.process_thread = None

    def start(self):
        """ Start the appropriate processing thread asynchronously """
        if self.dic_['file_path'][-4:].lower() in self.dic_['parent'].dic_mime_type['surfaces']:
            self.process_thread = TIFThread(main=self.dic_['main'], parent=self.dic_['parent'], key_=self.key_, dic_=self.dic_)
        elif self.dic_['file_path'][-4:].lower() in self.dic_['parent'].dic_mime_type['point_clouds']:
            self.process_thread = PCThread(main=self.dic_['main'], parent=self.dic_['parent'], key_=self.key_, dic_=self.dic_)
        else:
            return

        self.process_thread.sig_status.connect(self.dic_['parent'].update_bar)
        self.process_thread.finished.connect(lambda: self.finished.emit(self.key_))  # Notify when done
        self.process_thread.start()

