# UTF8
"""Utilitários QGIS headless para scripts standalone (.bat / análise manual)."""

from __future__ import annotations

from qgis.core import QgsApplication, QgsProject, QgsVectorLayer

from .mod_pec_constants import LAYER_NAME_BUFFER_TEST

_qgs_app = None


def init_standalone_qgis():
    """Inicializa QGIS headless (evita avisos Qt ao rodar via .bat)."""
    global _qgs_app
    if _qgs_app is not None or QgsApplication.instance():
        return
    _qgs_app = QgsApplication([], False)
    _qgs_app.initQgis()


def exit_standalone_qgis():
    global _qgs_app
    if _qgs_app is not None:
        _qgs_app.exitQgis()
        _qgs_app = None


def load_result_layer(
    gpkg_path,
    layer_name=LAYER_NAME_BUFFER_TEST,
    add_to_project=False,
):
    """Abre a camada de resultados no GPKG."""
    uri = f'{gpkg_path}|layername={layer_name}'
    layer = QgsVectorLayer(uri, layer_name, 'ogr')
    if not layer.isValid():
        raise RuntimeError(
            f'Não foi possível abrir a camada "{layer_name}" em:\n{gpkg_path}'
        )
    if add_to_project:
        QgsProject.instance().addMapLayer(layer, False)
    return layer
