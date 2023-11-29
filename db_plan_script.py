import math
import os
import sqlite3
import statistics

from PyQt5.QtCore import QVariant
from qgis.core import (QgsFields, QgsField, QgsVectorFileWriter, QgsVectorLayer, QgsGeometry, QgsFeature, QgsPointXY,
                       QgsSpatialIndex, QgsWkbTypes, QgsProject)

from scipy.stats import shapiro


def gpkg_conn():
    print('gpkg_conn')
    conn = sqlite3.connect(gpkg_path)  # , isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    conn.load_extension('mod_spatialite')
    conn.execute('SELECT load_extension("mod_spatialite")')
    conn.execute('pragma journal_mode=wal')
    return conn


dic_pec_mm = {
    'H': {
        'A': {
            'pec': 0.28,
            'ep': 0.17
        },
        'B': {
            'pec': 0.5,
            'ep': 0.3
        },
        'C': {
            'pec': 0.8,
            'ep': 0.5
        },
        'D': {
            'pec': 1.0,
            'ep': 0.6
        },
    },
    'V': {
        'A': {
            'pec': 0.27,
            'ep': 0.17
        },
        'B': {
            'pec': 0.5,
            'ep': 0.33
        },
        'C': {
            'pec': 0.6,
            'ep': 0.4
        },
        'D': {
            'pec': 0.75,
            'ep': 0.5
        },
    },

}
vet_scale = [100, 250]
# pec_ = 80
# ep_ = 50

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
schema_.append(QgsField('scale', QVariant.Int))
schema_.append(QgsField('class', QVariant.String))
schema_.append(QgsField('layer_ref', QVariant.String))
schema_.append(QgsField('Test_name', QVariant.String))
schema_.append(QgsField('Area_Test', QVariant.Double))
schema_.append(QgsField('Area_Ref', QVariant.Double))
schema_.append(QgsField('Area_Inter', QVariant.Double))
schema_.append(QgsField('DM_H', QVariant.Double))
schema_.append(QgsField('OUT_H', QVariant.Bool))
schema_.append(QgsField('Area_Test_Prof', QVariant.Double))
schema_.append(QgsField('Area_Ref_Prof', QVariant.Double))
schema_.append(QgsField('Area_Inter_Prof', QVariant.Double))
schema_.append(QgsField('DM_V', QVariant.Double))
schema_.append(QgsField('OUT_V', QVariant.Bool))
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


def update_dic():
    dic_stats = {}
    for feat_ in layer_bt.getFeatures():
        if feat_['OUT_H'] or feat_['OUT_V']:
            continue
        test_dsm = f"{feat_['Test_name']}-{feat_['scale']}-{feat_['class']}"
        if test_dsm not in dic_stats:
            dic_stats[test_dsm] = {'ids': [], 'dm_h': [], 'dm_v': []}
        dic_stats[test_dsm]['ids'].append(feat_.id())
        dic_stats[test_dsm]['dm_h'].append(feat_['DM_H'])
        dic_stats[test_dsm]['dm_v'].append(feat_['DM_V'])
    return dic_stats


def check_out(dic_stats):
    for test_dsm in dic_stats:
        for tag_ in dic_stats[test_dsm]:
            list_out_l = []
            if tag_ == 'ids':
                continue
            list_ = dic_stats[test_dsm][tag_]
            list_ids = dic_stats[test_dsm]['ids']
            quant_ = statistics.quantiles(data=list_)
            iqr_ = quant_[2] - quant_[0]
            ls_ = quant_[2] + 1.5 * iqr_
            li_ = quant_[0] - 1.5 * iqr_
            for i, v_ in enumerate(list_):
                if v_ < li_ or v_ > ls_:
                    id_ = list_ids[i]
                    list_out_l.append(id_)
                    feat_ = layer_bt.getFeature(id_)

                    feat_.setAttribute(name=tag_.replace('dm_', 'out_'), value=True)
                    layer_bt.startEditing()
                    layer_bt.updateFeature(feat_)
                    layer_bt.commitChanges()

            # print(test_dsm, tag_, list_out_l)


def check_norm(vet_):
    result_ = shapiro(vet_)
    return True if result_[0] >= result_[1] else False


def rms(vet_):
    sun_ = 0
    for v_ in vet_:
        sun_ += v_ ** 2
    rms_ = (sun_ / (len(vet_) - 1)) ** 0.5
    return rms_


def perc_pec(vet_, pec_):
    count_ = 0
    for v_ in vet_:
        if v_ < pec_:
            count_ += 1
    return count_ / len(vet_)


for test_ in dic_name_layer:
    for l_test_name in dic_name_layer[test_]:
        l_test = QgsProject.instance().mapLayersByName(l_test_name)[0]
        l_ref_name = dic_name_layer[test_][l_test_name]
        l_ref = QgsProject.instance().mapLayersByName(l_ref_name)[0]
        index_ref = QgsSpatialIndex(l_ref.getFeatures())
        layer_bt.startEditing()
        for scale_ in vet_scale:
            for class_ in dic_pec_mm['H']:
                pec_h = scale_ * dic_pec_mm['H'][class_]['pec']
                ep_h = scale_ * dic_pec_mm['H'][class_]['ep']
                pec_v = scale_ * dic_pec_mm['V'][class_]['pec']
                ep_v = scale_ * dic_pec_mm['V'][class_]['ep']

                # layer_br.startEditing()
                for feat_t in l_test.getFeatures():
                    geom_t = feat_t.geometry()
                    pm_ = geom_t.interpolate(geom_t.length() / 2.0)
                    nearest_ids = index_ref.nearestNeighbor(pm_, 1)
                    feat_r = l_ref.getFeature(nearest_ids[0])
                    geom_r = feat_r.geometry()
                    dist_ = geom_r.distance(geom_t)
                    if dist_ < 2 * pec_h:
                        geom_br = geom_r.buffer(pec_h, 20)
                        geom_bt = geom_t.buffer(pec_h, 20)
                        geom_i = geom_bt.intersection(geom_br)
                        feat_bt = QgsFeature()
                        feat_bt.setGeometry(geom_bt)
                        dm_ = math.pi * pec_h * (geom_br.area() - geom_i.area()) / geom_bt.area()
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
                        # print(feat_r.id(), geom_r.wkbType())
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
                        geom_prof_br = geom_prof_r.buffer(pec_v, 20)

                        len_t = geom_t.length()
                        # print(feat_t.id(), geom_t.wkbType(), QgsWkbTypes.LineString)
                        if geom_t.wkbType() == QgsWkbTypes.LineString:
                            # print(geom_t)
                            continue
                            ps_t = geom_t.constGet().points()
                        else:
                            ps_t = geom_t.constGet()[0].points()
                        list_prof_t = []
                        k_t = len_t / len_r
                        for p_ in ps_t:
                            # print(p_)
                            dist_ = geom_t.lineLocatePoint(QgsGeometry(p_)) * k_t
                            z_ = p_.z()
                            list_prof_t.append(QgsPointXY(dist_, z_))
                        geom_prof_t = QgsGeometry().fromPolylineXY(list_prof_t)
                        # print(list_prof_t)
                        geom_prof_bt = geom_prof_t.buffer(pec_v, 20)

                        geom_prof_i = geom_prof_bt.intersection(geom_prof_br)
                        dm_prof = math.pi * pec_v * (geom_prof_br.area() - geom_prof_i.area()) / geom_prof_bt.area()

                        feat_bt.setAttributes([
                            len(layer_bt) + 1,
                            feat_r.id(),
                            scale_,
                            class_,
                            l_ref_name,
                            test_,
                            geom_bt.area(),
                            geom_br.area(),
                            geom_i.area(),
                            dm_,
                            False,
                            geom_prof_bt.area(),
                            geom_prof_br.area(),
                            geom_prof_i.area(),
                            dm_prof,
                            False
                        ])
                        layer_bt.addFeature(feat_bt)

                        # QgsGeometry().a
                        # print(feat_t.id(), feat_r.id())
                        # pass
                    # print(feat_r)
                # print(l_test_name, l_test_name)
        layer_bt.commitChanges()
dic_stats = update_dic()
check_out(dic_stats)
dic_stats = update_dic()

for test_dsm in dic_stats:
    test_, scale_, class_ = test_dsm.split('-')
    scale_ = int(scale_)
    pec_h = scale_ * dic_pec_mm['H'][class_]['pec']
    ep_h = scale_ * dic_pec_mm['H'][class_]['ep']
    pec_v = scale_ * dic_pec_mm['V'][class_]['pec']
    ep_v = scale_ * dic_pec_mm['V'][class_]['ep']
    for tag_ in dic_stats[test_dsm]:
        if tag_ == 'ids':
            continue
        elif tag_ == 'dm_h':
            pec_ = pec_h
            ep_ = ep_h
        else:
            pec_ = pec_v
            ep_ = ep_v
        list_ = dic_stats[test_dsm][tag_]
        if check_norm(vet_=list_):
            perc_pec_ = perc_pec(vet_=list_, pec_=pec_)
            if perc_pec_ >= 0.90:
                print(test_dsm, tag_, round(perc_pec_ * 100), '% < ', pec_, ' PEC - OK', len(list_))
            else:
                print(test_dsm, tag_, round(perc_pec_ * 100), '% < ', pec_, ' PEC - FALHOU', len(list_))
            rms_ = rms(list_)
            if rms_ <= ep_:
                print(test_dsm, tag_, round(rms_, 2), ' < ', ep_, ' EP - OK', len(list_))
            else:
                print(test_dsm, tag_, round(rms_, 2), ' > ', ep_, ' EP - FALHOU', len(list_))
        else:
            print(test_dsm, tag_, ' NORMALIDADE - FALHOU')