import os

from PyQt5.QtCore import QVariant
from qgis._core import QgsFields, QgsField, QgsVectorFileWriter, QgsCoordinateReferenceSystem, \
    QgsCoordinateTransformContext
from qgis.gui import QgsRubberBand, QgsMapCanvas
from qgis.core import QgsSpatialIndex, QgsWkbTypes, QgsProject
from qgis.utils import iface
bff_size = 80

dic_name_layer = {'Topo': {'Cumi_Topo': 'Cumi', 'Hid_Num_Topo': 'Hid_Num'}}
# rubberBand = QgsRubberBand(iface.QgsMapCanvas() ,QgsWkbTypes.LineGeometry)
base_dir = r'C:\Users\adria\OneDrive\Materiais\Mestrado\UVF\CIV972_Materiais\Artigo\MG-Viçosa-20231107T232412Z-001\MG-Viçosa\Scripts'
gpkg_path = os.path.join(base_dir, 'Topo.gpkg')

schema_ = QgsFields()
schema_.append(QgsField('id_ref', QVariant.Int))
schema_.append(QgsField('Area_Test', QVariant.Double))
schema_.append(QgsField('Area_Ref', QVariant.Double))
schema_.append(QgsField('Area_Inter', QVariant.Double))

options_ = QgsVectorFileWriter.SaveVectorOptions()
options_.driverName = "GPKG"
options_.layerName = '__Buffer_Test_'
writer_ = QgsVectorFileWriter.create(
    gpkg_path,
    schema_,
    QgsWkbTypes.Polygon,
    QgsCoordinateReferenceSystem('EPSG:32723'),
    QgsCoordinateTransformContext(),
    options_)
assert writer_.hasError() == QgsVectorFileWriter.NoError
del writer_  # to flush

for test_ in dic_name_layer:
    for l_test_name in dic_name_layer[test_]:
        l_test = QgsProject.instance().mapLayersByName(l_test_name)[0]
        l_ref_name = dic_name_layer[test_][l_test_name]
        l_ref = QgsProject.instance().mapLayersByName(l_ref_name)[0]
        index_ref = QgsSpatialIndex(l_ref.getFeatures())
        for feat_t in l_test.getFeatures():
            geom_t = feat_t.geometry()
            pm_ = geom_t.interpolate(geom_t.length()/2.0)
            nearest_ids = index_ref.nearestNeighbor(pm_, 1)
            feat_r = l_ref.getFeature(nearest_ids[0])
            dist_ = feat_r.geometry().distance(geom_t)
            if dist_ < 2 * bff_size:
                print(feat_t.id(), feat_r.id())
                pass
            # print(feat_r)
        print(l_test_name, l_test_name)
