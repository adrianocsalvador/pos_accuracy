# UTF8
"""Constantes PEC/EP e mapeamentos partilhados pelo plugin e scripts de análise manual."""

from __future__ import annotations

# Coeficientes PEC planimétrico (mm na escala) e altimétrico (× EQ)
DIC_PEC_MM = {
    'H': {
        'A': {'pec': 0.28, 'ep': 0.17},
        'B': {'pec': 0.5, 'ep': 0.3},
        'C': {'pec': 0.8, 'ep': 0.5},
        'D': {'pec': 1.0, 'ep': 0.6},
    },
    'V': {
        'A': {'pec': 0.27, 'ep': 0.17},
        'B': {'pec': 0.5, 'ep': 0.33},
        'C': {'pec': 0.6, 'ep': 0.4},
        'D': {'pec': 0.75, 'ep': 0.5},
    },
}

# EQ altimétrico por escala nominal (fator × DIC_PEC_MM['V'])
DIC_EQ_BY_NOMINAL_SCALE = {
    1: 1,
    2: 1,
    5: 2,
    10: 5,
    25: 10,
    50: 20,
    100: 50,
    250: 100,
    500: 100,
    1000: 100,
}

# Limites PEC/EP altimétricos absolutos (escalas 50, 100, 250)
DIC_PEC_V = {
    50: {
        'A': {'pec': 5.0, 'ep': 3.33},
        'B': {'pec': 10.0, 'ep': 6.66},
        'C': {'pec': 12.0, 'ep': 8.0},
        'D': {'pec': 15.0, 'ep': 10.0},
    },
    100: {
        'A': {'pec': 13.7, 'ep': 8.33},
        'B': {'pec': 25.00, 'ep': 16.66},
        'C': {'pec': 30.0, 'ep': 20.0},
        'D': {'pec': 37.5, 'ep': 25.0},
    },
    250: {
        'A': {'pec': 27.0, 'ep': 16.67},
        'B': {'pec': 50.0, 'ep': 33.33},
        'C': {'pec': 60.0, 'ep': 40.0},
        'D': {'pec': 75.0, 'ep': 50.0},
    },
}

DIC_PEC_ALT = DIC_PEC_V

DIC_EQ_V = {scale: DIC_EQ_BY_NOMINAL_SCALE[scale] for scale in DIC_PEC_V}

CLASS_ORDER = ['A', 'B', 'C', 'D']

# Padrão de acurácia (step_buffers.accuracy_standard)
ACCURACY_STANDARD_BR = 0  # PEC-PCD (Brasil)
ACCURACY_STANDARD_CE90 = 1  # CE90 / LE90 (busca por limiar × pixels do MDE de teste)

# Classes sintéticas no modo CE90/LE90 (chaves em dic_values)
CLASS_CE90 = 'CE90'
CLASS_LE90 = 'LE90'

# Razões EP/PEC da classe A (mesmos critérios de aprovação no modo CE90/LE90)
EP_RATIO_H = DIC_PEC_MM['H']['A']['ep'] / DIC_PEC_MM['H']['A']['pec']
EP_RATIO_V = DIC_PEC_MM['V']['A']['ep'] / DIC_PEC_MM['V']['A']['pec']

# Precisão do limiar na busca binária (metros)
# Pixel do MDE de teste < 5 m → 2 casas (ex.: 0,50 m); senão → 1 casa.
CE90_THRESHOLD_DECIMALS = 1
CE90_THRESHOLD_DECIMALS_FINE = 2
CE90_PIXEL_FINE_M = 5.0


def ce90_threshold_decimals(pixel_m) -> int:
    """Casas decimais do limiar CE90/LE90 conforme o pixel do MDE de teste."""
    try:
        px = float(pixel_m)
    except (TypeError, ValueError):
        return CE90_THRESHOLD_DECIMALS
    if px > 0.0 and px < CE90_PIXEL_FINE_M:
        return CE90_THRESHOLD_DECIMALS_FINE
    return CE90_THRESHOLD_DECIMALS

LAYER_NAME_BUFFER_TEST = '__Buffer_Test__'
EXTENT_REF_FIELD = 'extent_ref'

# Mapeamento camadas teste → referência (dataset manual Selecao_v2_z.gpkg)
DIC_NAME_LAYER = {
    'ANADEM': {
        'anadem_cumeadas_z': 'sei_cumeadas_z',
        'anadem_hidrografias_z': 'sei_hidrografias_z',
    },
    'NASADEM': {
        'nasadem_cumeadas_z': 'sei_cumeadas_z',
        'nasadem_hidrografias_z': 'sei_hidrografias_z',
    },
}

NORM_BY_METHOD = {
    'scale': 0,
    'linear': 0,
    'less_dist': 1,
    'proximidade': 1,
    'none': 2,
    'sem_compatibilizacao': 2,
    'sem_normalizacao': 2,
}

RESULTS_FOLDER_BY_METHOD = {
    0: 'Geral_linear',
    1: 'Geral_proximidade',
    2: 'Geral_sem_compatibilizacao',
}
