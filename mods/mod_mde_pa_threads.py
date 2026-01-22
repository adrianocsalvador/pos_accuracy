import os
import shutil
import sqlite3

from PyQt5.QtCore import QThread, pyqtSignal, QRunnable, QObject
from qgis import processing
from qgis.core import QgsCoordinateReferenceSystem, QgsFeature

class PolygonThread(QThread):
    sig_status = pyqtSignal(dict, name='Status for processing bar')

    def __init__(self, main, parent, key_=None, dic_=None):
        QThread.__init__(self, parent)

        self.main = main
        self.parent = parent
        self.key_ = key_
        self.file_path = dic_['file_path']
        self.gpkg_path = dic_['gpkg']
        self.tab = dic_['layer']

        self.srid_ref = dic_['srid_ref']
        self.srid = dic_['srid']

        self.nr_procs = 6
        self.cur = None
        self.conn = None


    def run(self):
        self.sig_status.emit({'key': self.key_, 'quant': self.nr_procs})
        nr_ = 0
        tool_ = ''

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
                'NO_DATA': 0,
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
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
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
            # print('result_poly', result_poly)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 3 "native:assignprojection"
        try:
            nr_ += 1
            params = {
                'INPUT': result_poly['OUTPUT'],
                'CRS': QgsCoordinateReferenceSystem(self.srid),
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }
            tool_ = "native:assignprojection"
            result_setpro = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 4 "native:reprojectlayer"
        nr_ += 1
        if self.srid_ref != self.srid:
            try:
                params = {
                    'INPUT': result_setpro['OUTPUT'],
                    'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4674'),
                    'CONVERT_CURVED_GEOMETRIES': False,
                    'OUTPUT': 'TEMPORARY_OUTPUT'
                }
                tool_ = "native:reprojectlayer"
                result_repro = processing.run(tool_, params)
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            except Exception as e:
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
                return
        else:
            result_repro = result_setpro
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})

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
            print('result_bff', result_bff)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        wkt_ = ''
        # 6 'geometry'
        try:
            nr_ += 1  # 6
            layer_ = result_bff['OUTPUT']
            # layer_ = result_poly['OUTPUT']
            tool_ = 'geometry'
            for i, feat_ in enumerate(layer_.getFeatures()):
                geom_ = feat_.geometry()
                geom_.convertToSingleType()
                feat_out = QgsFeature()
                if geom_:
                    feat_out.setGeometry(geom_)

                self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': f'{tool_} nr:{i + 1}', 'feat': feat_})
                # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            # self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
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

class MorphologyThread(QThread):
    sig_status = pyqtSignal(dict, name='Status for processing bar')

    def __init__(self, main, parent, key_=None, dic_=None):
        QThread.__init__(self, parent)

        self.main = main
        self.parent = parent
        self.key_ = key_
        self.file_path = dic_['file_path']
        self.gpkg_path = dic_['gpkg']
        self.tab = dic_['layer']

        self.srid_ref = dic_['srid_ref']
        self.srid = dic_['srid']

        self.nr_procs = 6
        self.cur = None
        self.conn = None


    def run(self):
        self.sig_status.emit({'key': self.key_, 'quant': self.nr_procs})
        nr_ = 0
        tool_ = 'gdal:buildvirtualraster'
        print(tool_, self.key_, self.file_path)
        try:
            nr_ += 1  # 1
            params = {
                'INPUT': f'{self.file_path}',
                'RESOLUTION': 1,
                'SEPARATE': False,
                'PROJ_DIFFERENCE': False,
                'ADD_ALPHA': False,
                'ASSIGN_CRS': None,
                'RESAMPLING': 3,
                'SRC_NODATA': '',
                'EXTRA': '',
                'OUTPUT': 'TEMPORARY_OUTPUT',
            }
            result_vrt = processing.run(tool_, params)
            print('result_calc', result_vrt)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)

        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 1 "grass: r.watershed"
        try:
            nr_ += 1  # 1
            tool_ = "grass7:r.watershed"
            params = {
                'distance_units' : 'meters',
                'area_units' : 'km2',
                'ellipsoid' : 'EPSG:7019',
                'elevation' : result_vrt['OUTPUT'],
                'threshold' : 5000,
                'convergence' : 5,
                'memory' : 50000,
                '-s' : 'false',
                '-m' : 'false',
                '-4' : 'false',
                '-a' : 'false',
                '-b' : 'false',
                'accumulation' : 'TEMPORARY_OUTPUT',
                'drainage' : 'TEMPORARY_OUTPUT',
                'basin' : 'TEMPORARY_OUTPUT',
                'stream' : 'TEMPORARY_OUTPUT',
                'half_basin' : 'TEMPORARY_OUTPUT',
                'length_slope' : 'TEMPORARY_OUTPUT',
                'slope_steepness' : 'TEMPORARY_OUTPUT',
                'tci' : 'TEMPORARY_OUTPUT',
                'spi' : 'TEMPORARY_OUTPUT',
                'GRASS_REGION_CELLSIZE_PARAMETER' : 0,
                'GRASS_RASTER_FORMAT_OPT' : '',
                'GRASS_RASTER_FORMAT_META' : '',
            }


            result_calc = processing.run(tool_, params)
            print('result_calc', result_calc)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            return
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
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
            # print('result_poly', result_poly)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 3 "native:assignprojection"
        try:
            nr_ += 1
            params = {
                'INPUT': result_poly['OUTPUT'],
                'CRS': QgsCoordinateReferenceSystem(self.srid),
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }
            tool_ = "native:assignprojection"
            result_setpro = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 4 "native:reprojectlayer"
        nr_ += 1
        if self.srid_ref != self.srid:
            try:
                params = {
                    'INPUT': result_setpro['OUTPUT'],
                    'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4674'),
                    'CONVERT_CURVED_GEOMETRIES': False,
                    'OUTPUT': 'TEMPORARY_OUTPUT'
                }
                tool_ = "native:reprojectlayer"
                result_repro = processing.run(tool_, params)
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            except Exception as e:
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
                return
        else:
            result_repro = result_setpro
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})

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
            print('result_bff', result_bff)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        wkt_ = ''
        # 6 'geometry'
        try:
            nr_ += 1  # 6
            layer_ = result_bff['OUTPUT']
            # layer_ = result_poly['OUTPUT']
            tool_ = 'geometry'
            for i, feat_ in enumerate(layer_.getFeatures()):
                geom_ = feat_.geometry()
                geom_.convertToSingleType()
                feat_out = QgsFeature()
                if geom_:
                    feat_out.setGeometry(geom_)

                self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': f'{tool_} nr:{i + 1}', 'feat': feat_})
                # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            # self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
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
        if self.dic_['step'] == 'polygon' :
            self.process_thread = PolygonThread(main=self.dic_['main'], parent=self.dic_['parent'], key_=self.key_,
                                                dic_=self.dic_)
        elif self.dic_['step'] == 'morphology':
            self.process_thread = MorphologyThread(main=self.dic_['main'], parent=self.dic_['parent'], key_=self.key_,
                                                dic_=self.dic_)

        self.process_thread.sig_status.connect(self.dic_['parent'].update_bar)
        self.process_thread.finished.connect(lambda: self.finished.emit(self.key_))  # Notify when done
        self.process_thread.start()

