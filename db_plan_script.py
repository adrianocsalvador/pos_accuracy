import math
import os
import sqlite3

from PyQt5.QtCore import QVariant
from qgis._core import QgsFields, QgsField, QgsVectorFileWriter, QgsCoordinateReferenceSystem, \
    QgsCoordinateTransformContext, QgsVectorLayer, QgsGeometry, QgsFeature, QgsPointXY
from qgis.gui import QgsRubberBand, QgsMapCanvas
from qgis.core import QgsSpatialIndex, QgsWkbTypes, QgsProject
from qgis.utils import iface

def gpkg_conn():
    print('gpkg_conn')
    conn = sqlite3.connect(gpkg_path)  # , isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    conn.load_extension('mod_spatialite')
    conn.execute('SELECT load_extension("mod_spatialite")')
    conn.execute('pragma journal_mode=wal')
    return conn

bff_size = 80

dic_name_layer = {
    # 'Topo': {'Cumi_Topo': 'Cumi_Ref_Z', 'Hid_Num_Topo': 'Hid_Num_Ref_Z'},
                  'Gdem': {'Cumi_Gdem_Z': 'Cumi_Ref_Z', 'Hid_Num_Gdem_Z': 'Hid_Num_Ref_Z'}}
# rubberBand = QgsRubberBand(iface.QgsMapCanvas() ,QgsWkbTypes.LineGeometry)
base_dir = r'C:\Users\adria\OneDrive\Materiais\Mestrado\UVF\CIV972_Materiais\Artigo\MG-Viçosa-20231107T232412Z-001\MG-Viçosa\Scripts'
gpkg_path = os.path.join(base_dir, 'Test_CIV972.gpkg')

prefix_1 = "__Buffer_Test__"
prj_crs = QgsProject.instance().crs()
layer_1 = QgsVectorLayer(f'polygon?crs={prj_crs.authid()}&index=yes', prefix_1, "memory")
schema_ = QgsFields()
schema_.append(QgsField('id_ref', QVariant.Int))
schema_.append(QgsField('layer_ref', QVariant.String))
schema_.append(QgsField('Test_name', QVariant.String))
schema_.append(QgsField('Area_Test', QVariant.Double))
schema_.append(QgsField('Area_Ref', QVariant.Double))
schema_.append(QgsField('Area_Inter', QVariant.Double))
schema_.append(QgsField('DM', QVariant.Double))
schema_.append(QgsField('Area_Test_Prof', QVariant.Double))
schema_.append(QgsField('Area_Ref_Prof', QVariant.Double))
schema_.append(QgsField('Area_Inter_Prof', QVariant.Double))
schema_.append(QgsField('DM_Prof', QVariant.Double))
pr_ = layer_1.dataProvider()
pr_.addAttributes(schema_)
layer_1.updateFields()

options_ = QgsVectorFileWriter.SaveVectorOptions()
options_.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteFile
# options_.driverName = "GPKG"
options_.layerName = prefix_1
writer_ = QgsVectorFileWriter.writeAsVectorFormat(
    layer=layer_1,
    fileName=gpkg_path,
    options=options_
)
# # conn = gpkg_conn()
uri_ = f'{gpkg_path}|layername={prefix_1}'
layer_bt = QgsVectorLayer(uri_, prefix_1, 'ogr')
# # conn.commit()
# # conn.close()
layer_bt.triggerRepaint()
QgsProject.instance().addMapLayer(layer_bt, True)

# prefix_2 = "__Buffer_Ref__"
# layer_2 = QgsVectorLayer(f'polygon?crs={prj_crs.authid()}&index=yes', prefix_2, "memory")
# schema_ = QgsFields()
# schema_.append(QgsField('id_ref', QVariant.Int))
# schema_.append(QgsField('Test_name', QVariant.String))
# schema_.append(QgsField('Area_Ref', QVariant.Double))
# pr_ = layer_2.dataProvider()
# pr_.addAttributes(schema_)
# layer_2.updateFields()
#
# options_ = QgsVectorFileWriter.SaveVectorOptions()
# options_.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteFile
# # options_.driverName = "GPKG"
# options_.layerName = prefix_2
# writer_ = QgsVectorFileWriter.writeAsVectorFormat(
#     layer=layer_2,
#     fileName=gpkg_path,
#     options=options_
# )
# uri_ = f'{gpkg_path}|layername={prefix_2}'
# layer_br = QgsVectorLayer(uri_, prefix_2, 'ogr')
# # conn.commit()
# layer_br.triggerRepaint()
# QgsProject.instance().addMapLayer(layer_br, True)

def gpkg_conn():
    print('gpkg_conn')
    conn = sqlite3.connect(gpkg_path)  # , isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    conn.load_extension('mod_spatialite')
    conn.execute('SELECT load_extension("mod_spatialite")')
    conn.execute('pragma journal_mode=wal')
    cur = conn.cursor()

for test_ in dic_name_layer:
    for l_test_name in dic_name_layer[test_]:
        l_test = QgsProject.instance().mapLayersByName(l_test_name)[0]
        l_ref_name = dic_name_layer[test_][l_test_name]
        l_ref = QgsProject.instance().mapLayersByName(l_ref_name)[0]
        index_ref = QgsSpatialIndex(l_ref.getFeatures())
        layer_bt.startEditing()
        # layer_br.startEditing()
        for feat_t in l_test.getFeatures():
            geom_t = feat_t.geometry()
            pm_ = geom_t.interpolate(geom_t.length()/2.0)
            nearest_ids = index_ref.nearestNeighbor(pm_, 1)
            feat_r = l_ref.getFeature(nearest_ids[0])
            geom_r = feat_r.geometry()
            dist_ = geom_r.distance(geom_t)
            if dist_ < 2 * bff_size:
                geom_br = geom_r.buffer(bff_size, 20)
                geom_bt = geom_t.buffer(bff_size, 20)
                geom_i = geom_bt.intersection(geom_br)
                feat_bt = QgsFeature()
                feat_bt.setGeometry(geom_bt)
                dm_ = math.pi * bff_size * (geom_br.area() - geom_i.area()) / geom_bt.area()
                feat_bt.setAttributes([
                    len(layer_bt) + 1,
                    feat_r.id(),
                    l_ref_name,
                    test_,
                    geom_bt.area(),
                    geom_br.area(),
                    geom_i.area(),
                    dm_
                ])
                layer_bt.addFeature(feat_bt)

                len_r = geom_r.length()
                print(feat_r.id(), geom_r.wkbType())
                # if geom_r.wkbType() == QgsWkbTypes.LineString:
                #     ps_r = geom_r.constGet().points()
                # else:
                ps_r = geom_r.constGet()[0].points()
                list_prof_r = []
                for p_ in ps_r:
                    dist_ = geom_r.lineLocatePoint(QgsGeometry(p_))
                    z_ = p_.z()
                    list_prof_r.append(QgsPointXY(dist_, z_))
                geom_prof_r = QgsGeometry().fromPolylineXY(list_prof_r)
                geom_prof_br = geom_prof_r.buffer(bff_size, 20)

                len_t = geom_t.length()
                print(feat_t.id(), geom_t.wkbType(), QgsWkbTypes.LineString)
                if geom_t.wkbType() == QgsWkbTypes.LineString:
                    # print(geom_t)
                    continue
                    ps_t = geom_t.constGet().points()
                else:
                    ps_t = geom_t.constGet()[0].points()
                list_prof_t = []
                k_t = len_t / len_r
                for p_ in ps_t:
                    print(p_)
                    dist_ = geom_t.lineLocatePoint(QgsGeometry(p_)) * k_t
                    z_ = p_.z()
                    list_prof_t.append(QgsPointXY(dist_, z_))
                geom_prof_t = QgsGeometry().fromPolylineXY(list_prof_t)
                print(list_prof_t)
                geom_prof_bt = geom_prof_t.buffer(bff_size, 20)

                geom_prof_i = geom_prof_bt.intersection(geom_prof_br)
                dm_prof = math.pi * bff_size * (geom_prof_br.area() - geom_prof_i.area()) / geom_prof_bt.area()

                feat_bt.setAttributes([
                    len(layer_bt) + 1,
                    feat_r.id(),
                    l_ref_name,
                    test_,
                    geom_bt.area(),
                    geom_br.area(),
                    geom_i.area(),
                    dm_,
                    geom_prof_bt.area(),
                    geom_prof_br.area(),
                    geom_prof_i.area(),
                    dm_prof
                ])
                layer_bt.addFeature(feat_bt)

                # QgsGeometry().a
                # print(feat_t.id(), feat_r.id())
                # pass
            # print(feat_r)
        # print(l_test_name, l_test_name)
        layer_bt.commitChanges()
        # layer_br.commitChanges()
