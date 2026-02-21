# -*- coding: utf-8 -*-
"""
/***************************************************************************
                                 A QGIS plugin
                             -------------------
        begin                : 2025-11-01
        copyright            : (C) 2025 Adriano Caliman Salvador - BR
        email                : adriano.caliman@com.br
        git sha              : $Format:%H$
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 This script initializes the plugin, making it known to QGIS.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load Class in File.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    #

    from .mods.mod_mde_positional_accuracy import MDEPositionalAccuracy
    return MDEPositionalAccuracy(iface)
