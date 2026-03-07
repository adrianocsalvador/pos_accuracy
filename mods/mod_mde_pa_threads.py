import os
import shutil
import sqlite3
import uuid
import tempfile

from osgeo.ogr import wkbTIN
from qgis.PyQt.QtCore import QThread, pyqtSignal, QRunnable, QObject
from qgis import processing
from qgis.core import (QgsCoordinateReferenceSystem, QgsFeature, QgsVectorFileWriter, QgsFields, QgsField,
                       QgsVectorLayer, QgsCoordinateTransformContext, QgsWkbTypes)


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
        # Temp dir for native:* outputs (GPKG) — avoid layer objects in thread
        caminho_temp_poly = os.path.join(tempfile.gettempdir(), f'QGIS3-{str(uuid.uuid4())[:8]}')
        os.makedirs(caminho_temp_poly, exist_ok=True)

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

        # 3 "native:assignprojection" — output to GPKG (thread-safe)
        try:
            nr_ += 1
            out_assignpro = os.path.join(caminho_temp_poly, 'assignpro.gpkg')
            params = {
                'INPUT': result_poly['OUTPUT'],
                'CRS': QgsCoordinateReferenceSystem(self.srid),
                'OUTPUT': out_assignpro
            }
            tool_ = "native:assignprojection"
            result_setpro = processing.run(tool_, params)
            result_setpro = {'OUTPUT': out_assignpro}
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        # 4 "native:reprojectlayer" — output to GPKG (thread-safe)
        nr_ += 1
        if self.srid_ref != self.srid:
            try:
                out_repro = os.path.join(caminho_temp_poly, 'reproject.gpkg')
                params = {
                    'INPUT': result_setpro['OUTPUT'],
                    'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4674'),
                    'CONVERT_CURVED_GEOMETRIES': False,
                    'OUTPUT': out_repro
                }
                tool_ = "native:reprojectlayer"
                result_repro = processing.run(tool_, params)
                result_repro = {'OUTPUT': out_repro}
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
                # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
            except Exception as e:
                self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
                # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
                return
        else:
            result_repro = result_setpro
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})

        # 5 "native:buffer" — output to GPKG (thread-safe)
        try:
            nr_ += 1  # 5
            out_buffer = os.path.join(caminho_temp_poly, 'buffer.gpkg')
            params = {
                'INPUT': result_repro['OUTPUT'],
                'DISTANCE': 0,
                'SEGMENTS': 5,
                'END_CAP_STYLE': 0,
                'JOIN_STYLE': 0,
                'MITER_LIMIT': 2,
                'DISSOLVE': True,
                'SEPARATE_DISJOINT': False,
                'OUTPUT': out_buffer}
            tool_ = "native:buffer"
            result_bff = processing.run(tool_, params)
            result_bff = {'OUTPUT': out_buffer}
            print('result_bff', result_bff)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
            # self.log.info(True, f'PolygonThread: {self.key_} {tool_}', pretty=True)
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            # self.log.error(True, f'PolygonThread: {self.key_} {tool_}: {e}', pretty=True)
            return

        wkt_ = ''
        # 6 'geometry' — load from GPKG path for iteration (path from step 5)
        try:
            nr_ += 1  # 6
            layer_ = QgsVectorLayer(result_bff['OUTPUT'], 'buffer', 'ogr')
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
                'msg': ':) FINALIZADO LIMITE (:'
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
        self.boudary = dic_['layer']
        self.max_memo = dic_['max_memo']
        self.max_px = dic_['max_px']

        self.srid_ref = dic_['srid_ref']
        self.srid = dic_['srid']
        self.morph_names = dic_['morph_names']

        self.nr_procs = 7
        self.cur = None
        self.conn = None

    # def save_layer_in_gpkg(self, layer):
    #     """Save a QgsVectorLayer to a temporary GPKG file. Must be called from the main (GUI) thread only — QGIS layer/provider APIs are not thread-safe."""
    #     # diagnostics
    #     print("layer valid:", layer.isValid())
    #     print("provider:", layer.dataProvider().name())
    #     print("crs valid:", layer.crs().isValid(), "->", layer.crs().authid())
    #     print("wkbType:", QgsWkbTypes.displayString(layer.wkbType()))
    #
    #     # stop editing / commit pending changes
    #     if layer.isEditable():
    #         try:
    #             layer.commitChanges()
    #             print("Committed edits")
    #         except Exception:
    #             layer.rollBack()
    #             print("Rolled back edits")
    #
    #     caminho_temp = os.path.join(tempfile.gettempdir(), f'QGIS3-{str(uuid.uuid4())[:8]}')
    #     os.makedirs(caminho_temp, exist_ok=True)
    #     gpkg_path = os.path.join(caminho_temp, 'OUTPUT.GPKG')
    #
    #     options = QgsVectorFileWriter.SaveVectorOptions()
    #     options.driverName = "GPKG"
    #     options.layerName = 'OUTPUT'
    #     options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
    #     options.fileEncoding = 'UTF-8'
    #
    #     # Build output fields skipping 'fid' (case-insensitive)
    #     output_fields = QgsFields()
    #     for field in layer.fields():
    #         print('field.name()=', field.name())
    #         if field.name().lower() != 'fid':
    #             output_fields.append(QgsField(field.name(), field.type()))
    #     options.fields = output_fields
    #
    #     # write
    #     res, err = QgsVectorFileWriter.writeAsVectorFormatV2(
    #         layer,
    #         gpkg_path,
    #         QgsCoordinateTransformContext(),
    #         options
    #     )
    #
    #     if res == QgsVectorFileWriter.NoError:
    #         print(f"Successfully saved layer to {gpkg_path}")
    #     else:
    #         print("Error saving layer:", res, err)
    #
    #     return gpkg_path
    #
    # def save_layer_in_gpkg_(self, layer):
    #     print('save_layer_in_gpkg', 'layer:', layer)
    #
    #     caminho_temp = os.path.join(tempfile.gettempdir(), f'QGIS3-{str(uuid.uuid4())[:8]}')
    #     os.makedirs(caminho_temp, exist_ok=True)
    #     gpkg_path = os.path.join(caminho_temp, 'OUTPUT.GPKG')
    #
    #     options = QgsVectorFileWriter.SaveVectorOptions()
    #     options.driverName = "GPKG"
    #     options.layerName = 'OUTPUT'
    #     options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
    #     options.fileEncoding = 'UTF-8'
    #
    #     # Build output fields skipping 'fid' (case-insensitive)
    #     output_fields = QgsFields()
    #     # for field in layer.fields():
    #     #     if field.name().lower() != 'fid':
    #     #         output_fields.append(QgsField(field.name(), field.type()))
    #     options.fields = output_fields
    #
    #     # Write using V2 API
    #     res, err = QgsVectorFileWriter.writeAsVectorFormatV2(
    #         layer,
    #         gpkg_path,
    #         QgsCoordinateTransformContext(),
    #         options
    #     )
    #
    #     if res == QgsVectorFileWriter.NoError:
    #         print(f"Successfully saved layer to {gpkg_path}")
    #     else:
    #         print(f"Error saving layer: {err}")
    #
    #     return gpkg_path

    def run(self):
        self.sig_status.emit({'key': self.key_, 'quant': self.nr_procs})
        nr_ = 0

        # Temp dir for native:* outputs (GPKG) — avoid layer objects in thread
        caminho_temp_morph = os.path.join(tempfile.gettempdir(), f'QGIS3-{str(uuid.uuid4())[:8]}')
        os.makedirs(caminho_temp_morph, exist_ok=True)

        # 1 'gdal:cliprasterbymasklayer'
        try:
            nr_ += 1
            tool_ = 'gdal:cliprasterbymasklayer'
            print(tool_, self.key_, self.file_path)
            params = {
                'INPUT': f'{self.file_path}',
                'MASK':f'{self.boudary}',
                'SOURCE_CRS':None,
                'TARGET_CRS':None,
                'TARGET_EXTENT':None,
                'NODATA':None,
                'ALPHA_BAND':False,
                'CROP_TO_CUTLINE':True,
                'KEEP_RESOLUTION':False,
                'SET_RESOLUTION':False,
                'X_RESOLUTION':None,
                'Y_RESOLUTION':None,
                'MULTITHREADING':False,
                'OPTIONS':'',
                'DATA_TYPE':0,
                'EXTRA':'',
                'OUTPUT': 'TEMPORARY_OUTPUT',
            }
            result_vrt = processing.run(tool_, params)
            print('result_vrt', result_vrt)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'model': result_vrt['OUTPUT']})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 2 "grass: r.watershed"
        try:
            nr_ += 1  # 1
            tool_ = "grass7:r.watershed"
            params = {
                'elevation':result_vrt['OUTPUT'],
                'depression':None,
                'flow':None,
                'disturbed_land':None,
                'blocking':None,
                'threshold':self.max_px,
                'max_slope_length':None,
                'convergence':5,
                'memory':self.max_memo * 1024,
                '-s':True,
                '-m':False,
                '-4':False,
                '-a':False,
                '-b':False,
                'accumulation':'TEMPORARY_OUTPUT',
                'drainage':'TEMPORARY_OUTPUT',
                'basin':'TEMPORARY_OUTPUT',
                'stream':'TEMPORARY_OUTPUT',
                'half_basin':'TEMPORARY_OUTPUT',
                'length_slope':'TEMPORARY_OUTPUT',
                'slope_steepness':'TEMPORARY_OUTPUT',
                'tci':'TEMPORARY_OUTPUT',
                'spi':'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER':None,
                'GRASS_REGION_CELLSIZE_PARAMETER':0,
                'GRASS_RASTER_FORMAT_OPT':'',
                'GRASS_RASTER_FORMAT_META':''
            }

            result_watershed = processing.run(tool_, params)
            # print('result_calc', result_watershed)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 3 "grass: r.to.vect"
        try:
            nr_ += 1  # 1
            tool_ = "grass7:r.to.vect"
            params = {
                'input': result_watershed['basin'],
                'type': 2,
                'column': 'value',
                '-s': False,
                '-v': False,
                '-z': False,
                '-b': False,
                '-t': False,
                'output': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER': None,
                'GRASS_REGION_CELLSIZE_PARAMETER': 0,
                'GRASS_OUTPUT_TYPE_PARAMETER': 0,
                'GRASS_VECTOR_DSCO': '',
                'GRASS_VECTOR_LCO': '',
                'GRASS_VECTOR_EXPORT_NOCAT': False}
            result_basian_vect = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 4 "native:fixgeometries" — output to GPKG (thread-safe)
        try:
            nr_ += 1  # 1
            result_fix_gpkg = os.path.join(caminho_temp_morph, 'fixgeometries.gpkg')
            tool_ = "native:fixgeometries"
            params = {
                'INPUT': result_basian_vect['output'],
                'METHOD': 1,
                'OUTPUT': result_fix_gpkg,
            }
            result_fix = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # print("result_fix['OUTPUT']=", result_fix['OUTPUT'])
        # 5 "grass7:v.to.lines"
        try:
            nr_ += 1  # 1
            tool_ = "grass7:v.to.lines"
            params = {
                'input': result_fix_gpkg,
                'method': None,
                'output': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER': None,
                'GRASS_SNAP_TOLERANCE_PARAMETER': -1,
                'GRASS_MIN_AREA_PARAMETER': 0.0001,
                'GRASS_OUTPUT_TYPE_PARAMETER': 0,
                'GRASS_VECTOR_DSCO': '',
                'GRASS_VECTOR_LCO': '',
                'GRASS_VECTOR_EXPORT_NOCAT': False
            }
            result_lines = processing.run(tool_, params)
            dic_layer = {
                'gpkg': result_lines['output'],
                'type': self.morph_names[0]
            }
            print('dic_layer =', dic_layer)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'layer': dic_layer})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 6 "grass: r.thin"
        try:
            nr_ += 1  # 1
            tool_ = "grass7:r.thin"
            params = {
                'input': result_watershed['stream'],
                'iterations': 200,
                'output': 'TEMPORARY_OUTPUT',
                'GRASS_REGION_CELLSIZE_PARAMETER': 0,
                'GRASS_RASTER_FORMAT_OPT': '',
                'GRASS_RASTER_FORMAT_META': '',
            }
            result_stream_thin = processing.run(tool_, params)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        # 7 "grass: r.to.vect"
        try:
            nr_ += 1  # 1
            tool_ = "grass7:r.to.vect"
            params = {
                'input': result_stream_thin['output'],
                'type':0,
                'column':'value',
                '-s':False,
                '-v':False,
                '-z':False,
                '-b':False,
                '-t':False,
                'output':'TEMPORARY_OUTPUT',
                'GRASS_REGION_PARAMETER':None,
                'GRASS_REGION_CELLSIZE_PARAMETER':0,
                'GRASS_OUTPUT_TYPE_PARAMETER':0,
                'GRASS_VECTOR_DSCO':'',
                'GRASS_VECTOR_LCO':'',
                'GRASS_VECTOR_EXPORT_NOCAT':False}
            result_stream_vect = processing.run(tool_, params)
            dic_layer = {
                'gpkg': result_stream_vect['output'],
                'type': self.morph_names[1]
            }
            print('dic_layer =', dic_layer)
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'msg': tool_, 'layer':dic_layer, 'start_task': True})
        except Exception as e:
            self.sig_status.emit({'key': self.key_, 'value': nr_, 'error': e})
            return

        if self.nr_procs:
            self.sig_status.emit({
                'key': self.key_,
                'end': self.nr_procs,
                'msg': ':) FINALIZADO MORFOLOGIA (:'
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

