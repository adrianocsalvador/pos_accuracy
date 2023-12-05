# script utilizado para classificação de MDEs (Aster GDEM, SRTM-X e TOPODATA) quanto ao padrão de extidão cartográfica,
# PEC-PCD, planimético e altimétrico, utilizando feições lineares pelo método do buffer duplo
import math
import os
import sqlite3
import statistics
from itertools import zip_longest
from PyQt5.QtCore import QVariant
from qgis.core import (QgsFields, QgsField, QgsVectorFileWriter, QgsVectorLayer, QgsGeometry, QgsFeature, QgsPointXY,
                       QgsSpatialIndex, QgsWkbTypes, QgsProject)
from scipy.stats import shapiro



# prefix_2 = "__Buffer_Ref__"
# layer_2 = QgsVectorLayer(f'polygon?crs={prj_crs.authid()}&index=yes', prefix_2, "memory")
# schema_2 = QgsFields()
# schema_2.append(QgsField('id_ref', QVariant.Int))
# schema_2.append(QgsField('Test_name', QVariant.String))
# schema_2.append(QgsField('scale', QVariant.Int))
# schema_2.append(QgsField('class', QVariant.String))
# schema_2.append(QgsField('Area_Ref', QVariant.Double))
# pr_2 = layer_2.dataProvider()
# pr_2.addAttributes(schema_2)
# layer_2.updateFields()
#
# options_ = QgsVectorFileWriter.SaveVectorOptions()
# options_.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteFile
# # options_.driverName = "GPKG"
# options_.layerName = prefix_2
# writer_ = QgsVectorFileWriter.writeAsVectorFormat(
#     layer=layer_2,
#     fileName=gpkg_ref,
#     options=options_
# )
# uri_ = f'{gpkg_ref}|layername={prefix_2}'
# layer_br = QgsVectorLayer(uri_, prefix_2, 'ogr')
# # conn.commit()
#
# style_path = os.path.join(os.path.dirname(gpkg_ref), '__Buffer_Ref__.qml')
# layer_br.loadNamedStyle(style_path)
# layer_br.triggerRepaint()
# QgsProject.instance().addMapLayer(layer_br, False)
# layer_group.addLayer(layer_br)

def gpkg_conn():
    print('gpkg_conn')
    conn = sqlite3.connect(gpkg_test)  # , isolation_level=None)
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
            dic_stats[test_dsm] = {'ids': [], 'dm_h': [], 'dm_v': [], 'd_cota': []}
        dic_stats[test_dsm]['ids'].append(feat_.id())
        dic_stats[test_dsm]['dm_h'].append(feat_['DM_H'])
        dic_stats[test_dsm]['dm_v'].append(feat_['DM_V'])
        dic_stats[test_dsm]['d_cota'].append(feat_['Cota_Media_t'] - feat_['Cota_Media_r'])
    return dic_stats


def check_out(dic_stats):
    for test_dsm in dic_stats:
        for tag_ in dic_stats[test_dsm]:
            list_out_l = []
            if tag_ == 'ids' or tag_ == 'd_cota':
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

def create_layer():
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
    schema_.append(QgsField('Cota_Media_r', QVariant.Double))
    schema_.append(QgsField('Cota_Media_t', QVariant.Double))
    pr_ = layer_1.dataProvider()
    pr_.addAttributes(schema_)
    layer_1.updateFields()

    options_ = QgsVectorFileWriter.SaveVectorOptions()
    options_.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteFile
    # options_.driverName = "GPKG"
    options_.layerName = prefix_1
    writer_ = QgsVectorFileWriter.writeAsVectorFormat(
        layer=layer_1,
        fileName=gpkg_test,
        options=options_
    )
    # # conn = gpkg_conn()
    uri_ = f'{gpkg_test}|layername={prefix_1}'
    layer_bt = QgsVectorLayer(uri_, prefix_1, 'ogr')
    # concat( "Test_name" , ' - ',  "scale", 'K-',  "class" , '-',  "OUT_H" or  "OUT_V"  )
    style_path = os.path.join(os.path.dirname(gpkg_test), '__Buffer_Test__.qml')
    layer_bt.loadNamedStyle(style_path)
    # # conn.commit()
    # # conn.close()
    layer_bt.triggerRepaint()
    QgsProject.instance().addMapLayer(layer_bt, False)
    return layer_bt

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
dic_pec_v = {

    50: {
        'A': {
            'pec': 5.0,
            'ep': 3.33
        },
        'B': {
            'pec': 10.0,
            'ep': 6.66
        },
        'C': {
            'pec': 12.0,
            'ep': 8.0
        },
        'D': {
            'pec': 15.0,
            'ep': 10.0
        },
    },
    100: {
        'A': {
            'pec': 13.7,
            'ep': 8.33
        },
        'B': {
            'pec': 25.00,
            'ep': 16.66
        },
        'C': {
            'pec': 30.0,
            'ep': 20.0
        },
        'D': {
            'pec': 37.5,
            'ep': 25.0
        },
    },
    250: {
        'A': {
            'pec': 27.0,
            'ep': 16.67
        },
        'B': {
            'pec': 50.0,
            'ep': 33.33
        },
        'C': {
            'pec': 60.0,
            'ep': 40.0
        },
        'D': {
            'pec': 75.0,
            'ep': 50.0
        },
    },

}

vet_scale = [50, 100, 250]

dic_name_layer = {
    'Topo': {'Cumi_Topo_Z': 'Cumi_Ref_Z', 'Hid_Num_Topo_Z': 'Hid_Num_Ref_Z'},
    'SrtmX': {'Cumi_Srtmx_Z': 'Cumi_Ref_Z', 'Hid_Num_Srtmx_Z': 'Hid_Num_Ref_Z'},
    'Gdem': {'Cumi_Gdem_Z': 'Cumi_Ref_Z', 'Hid_Num_Gdem_Z': 'Hid_Num_Ref_Z'}}

# rubberBand = QgsRubberBand(iface.QgsMapCanvas() ,QgsWkbTypes.LineGeometry)
base_dir = r'C:\Users\adria\OneDrive\Materiais\Mestrado\UVF\CIV972_Materiais\Artigo\MG-Viçosa-20231107T232412Z-001\MG-Viçosa\Scripts'
gpkg_test = os.path.join(base_dir, 'Test_CIV972.gpkg')
gpkg_ref = os.path.join(base_dir, 'Ref_CIV972.gpkg')

root = QgsProject.instance().layerTreeRoot()
layer_group = root.insertGroup(0, '__CIV972__')
layer_bt = create_layer()
layer_group.addLayer(layer_bt)

for test_ in dic_name_layer:
    path_txt_profile = os.path.join(os.path.dirname(gpkg_test), f'Profile_{test_}.csv')
    with open(path_txt_profile, "w") as prof_file:
        prof_file.write('')
    for l_test_name in dic_name_layer[test_]:
        l_test = QgsProject.instance().mapLayersByName(l_test_name)[0]
        l_ref_name = dic_name_layer[test_][l_test_name]
        l_ref = QgsProject.instance().mapLayersByName(l_ref_name)[0]
        index_ref = QgsSpatialIndex(l_ref.getFeatures())
        layer_bt.startEditing()
        # layer_br.startEditing()

        for feat_t in l_test.getFeatures():
            geom_t = feat_t.geometry()
            pm_ = geom_t.interpolate(geom_t.length() / 2.0)
            nearest_ids = index_ref.nearestNeighbor(pm_, 1)
            feat_r = l_ref.getFeature(nearest_ids[0])
            geom_r = feat_r.geometry()
            dist_ = geom_r.distance(geom_t)
            for scale_ in vet_scale:
                for class_ in dic_pec_mm['H']:
                    pec_h = scale_ * dic_pec_mm['H'][class_]['pec']
                    ep_h = scale_ * dic_pec_mm['H'][class_]['ep']
                    pec_v = dic_pec_v[scale_][class_]['pec']
                    ep_v = dic_pec_v[scale_][class_]['ep']
                    # if dist_ < 2 * pec_h:

                    geom_bt = geom_t.buffer(pec_h, 20)
                    feat_bt = QgsFeature()
                    feat_bt.setGeometry(geom_bt)

                    geom_br = geom_r.buffer(pec_h, 20)
                    # feat_br = QgsFeature()
                    # feat_br.setGeometry(geom_br)

                    geom_i = geom_bt.intersection(geom_br)

                    dm_ = math.pi * pec_h * (geom_br.area() - geom_i.area()) / geom_bt.area()
                    if scale_ == vet_scale[0] and class_ == list(dic_pec_mm['H'])[0]:
                        len_r = geom_r.length()
                        wkbt_ = geom_r.wkbType()
                        if wkbt_ == QgsWkbTypes.LineString or wkbt_ == QgsWkbTypes.LineStringZ:
                            ps_r = geom_r.constGet().points()
                        else:
                            ps_r = geom_r.constGet()[0].points()
                        gpr0 = QgsGeometry().fromPointXY(QgsPointXY(ps_r[0]))
                        gpr1 = QgsGeometry().fromPointXY(QgsPointXY(ps_r[-1]))
                        list_prof_r = []
                        list_prog_cota_r = []
                        list_cota_r = []
                        for p_ in ps_r:
                            dist_ = round(geom_r.lineLocatePoint(QgsGeometry(p_)), 2)
                            z_ = round(p_.z(), 2)
                            list_prof_r.append(QgsPointXY(dist_ + 10000, z_))
                            list_prog_cota_r.append([dist_ + 10000, z_])
                            list_cota_r.append(z_)
                        geom_prof_r = QgsGeometry().fromPolylineXY(list_prof_r)


                        len_t = geom_t.length()

                        if geom_t.wkbType() == QgsWkbTypes.LineString or geom_t.wkbType() == QgsWkbTypes.LineStringZ:
                            ps_t = geom_t.constGet().points()
                        else:
                            ps_t = geom_t.constGet()[0].points()
                        list_prof_t = []
                        list_prog_cota_t = []
                        list_cota_t = []
                        k_t = len_r / len_t
                        gpt0 = QgsGeometry().fromPointXY(QgsPointXY(ps_t[0]))
                        if gpt0.distance(gpr0) > gpt0.distance(gpr1):
                            ci = True
                        else:
                            ci = False
                        for p_ in ps_t:
                            if feat_r.id() == 8:
                                print(p_)
                            # print(p_)
                            z_ = round(p_.z(), 2)
                            dist_ = geom_t.lineLocatePoint(QgsGeometry(p_))
                            if ci:
                                dist_ = round((len_t - dist_) * k_t, 2)
                            else:
                                dist_ = round(dist_ * k_t, 2)

                            # list_prof_t.append(QgsPointXY(dist_, z_))
                            if not list_prog_cota_t:
                                list_prog_cota_t.append([dist_ + 10000, z_])
                                list_cota_t.append(z_)
                            elif dist_ != list_prog_cota_t[-1][0]:
                                list_prog_cota_t.append([dist_ + 10000, z_])
                                list_cota_t.append(z_)
                        list_prog_cota_t = sorted(list_prog_cota_t)
                        list_prof_t = []

                        for vet_ in list_prog_cota_t:
                            if feat_r.id() == 8:
                                print(vet_)
                            list_prof_t.append(QgsPointXY(float(vet_[0]), float(vet_[1])))
                        geom_prof_t = QgsGeometry().fromPolylineXY(list_prof_t)
                        cm_r = statistics.mean(list_cota_r)
                        cm_t = statistics.mean(list_cota_t)
                        with open(path_txt_profile, 'a') as prof_file:
                            prof_file.write(f'\n {l_ref_name} - {feat_r.id()} | len(r) = {len_r} | len(t) {len_t}\n'
                                            f'Cota_Media_r {cm_r} | Cota_Media_t {cm_t}\n')
                            for r_, t_ in zip_longest(list_prog_cota_r, list_prog_cota_t):
                                prof_file.write(
                                    f'{round(r_[0], 2) if r_ else ""}; {round(r_[1], 2) if r_ else ""}; {round(t_[0], 2) if t_ else ""}; {round(t_[1], 2) if t_ else ""}; \n')
                    geom_prof_br = geom_prof_r.buffer(pec_v, 20)
                    geom_prof_bt = geom_prof_t.buffer(pec_v, 20)
                    # print('geom_prof_bt=', geom_prof_bt)

                    geom_prof_i = geom_prof_bt.intersection(geom_prof_br)
                    # print('pec_v =', pec_v, geom_prof_br.area(), geom_prof_i.area(), geom_prof_bt.area())
                    dm_prof = math.pi * pec_v * (geom_prof_br.area() - geom_prof_i.area()) / geom_prof_bt.area() if geom_prof_bt.area() else 1

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
                        False,
                        cm_r,
                        cm_t
                    ])
                    layer_bt.addFeature(feat_bt)
                    layer_bt.commitChanges(stopEditing=False)
                    layer_bt.triggerRepaint()


                            # list_att_br = [
                            #     len(layer_br) + 1,
                            #     feat_r.id(),
                            #     test_,
                            #     scale_,
                            #     class_,
                            #     geom_br.area()
                            # ]
                            # # print('list_att_br=', list_att_br)
                            # # feat_br.setAttributes([len(layer_br) + 1, feat_r.id()])
                            # layer_br.addFeature(feat_br)
                            # layer_br.commitChanges(stopEditing=False)

                            # QgsGeometry().a
                            # print(feat_t.id(), feat_r.id())
                            # pass
                        # print(feat_r)
                    # print(l_test_name, l_test_name)
        layer_bt.commitChanges()
        # layer_br.commitChanges()
dic_stats = update_dic()
check_out(dic_stats)
dic_stats = update_dic()

path_result = os.path.join(os.path.dirname(gpkg_test), 'Results.txt')
with open(path_result, 'w') as file_result:
    file_result.write('')
for test_dsm in dic_stats:
    test_, scale_, class_ = test_dsm.split('-')
    scale_ = int(scale_)
    pec_h = round(scale_ * dic_pec_mm['H'][class_]['pec'], 2)
    ep_h = round(scale_ * dic_pec_mm['H'][class_]['ep'], 2)
    pec_v = round(dic_pec_v[scale_][class_]['pec'], 2)
    ep_v = round(dic_pec_v[scale_][class_]['ep'], 2)
    with open(path_result, 'a') as file_result:
        file_result.write(f'\n{test_dsm}\n')
    for tag_ in dic_stats[test_dsm]:
        if tag_ == 'dm_h':
            pec_ = pec_h
            ep_ = ep_h
        elif tag_ == 'dm_v':
            pec_ = pec_v
            ep_ = ep_v
        elif tag_ == 'ids':
            continue
        list_ = dic_stats[test_dsm][tag_]
        if tag_ == 'd_cota':
            str_ = f"{test_dsm}, {tag_}, d_cota_media = {round(statistics.mean(list_), 1)}\n"
        elif check_norm(vet_=list_):
            perc_pec_ = perc_pec(vet_=list_, pec_=pec_)
            if perc_pec_ >= 0.90:
                str_ = f"{test_dsm}, {tag_}, {round(perc_pec_ * 100)}, '% < ', {pec_}, ' PEC - OK', {len(list_)}\n"
                print(test_dsm, tag_, round(perc_pec_ * 100, 1), '% < ', pec_, ' PEC - OK', len(list_))
            else:
                str_ = f"{test_dsm}, {tag_}, {round(perc_pec_ * 100)}, '% < ', {pec_}, ' PEC - FALHOU', {len(list_)}\n"
                print(test_dsm, tag_, round(perc_pec_ * 100), '% < ', pec_, ' PEC - FALHOU', len(list_))
            rms_ = rms(list_)
            if rms_ <= ep_:
                str_ += f'{test_dsm}, {tag_}, {round(rms_, 2)}, " < ", {ep_}, " EP - OK", {len(list_)}\n'
                print(test_dsm, tag_, round(rms_, 2), ' < ', ep_, ' EP - OK', len(list_))
            else:
                str_ += f'{test_dsm}, {tag_}, {round(rms_, 2)}, " > ", {ep_}, " EP - FALHOU", {len(list_)}\n'
                print(test_dsm, tag_, round(rms_, 2), ' > ', ep_, ' EP - FALHOU', len(list_))
        else:
            str_ = f"{test_dsm}, {tag_}, NORMALIDADE - FALHOU\n"
            print(test_dsm, tag_, ' NORMALIDADE - FALHOU')
        with open(path_result, 'a') as file_result:
            file_result.write(str_)
with open(path_result, 'a') as file_result:
    file_result.write('\n\n')