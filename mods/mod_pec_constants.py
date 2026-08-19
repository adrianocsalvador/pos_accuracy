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

# Alias: EQ por escala (todas as escalas nominais)
DIC_EQ_V = dict(DIC_EQ_BY_NOMINAL_SCALE)

CLASS_ORDER = ['A', 'B', 'C', 'D']


def pec_v_from_eq(scale, class_, *, eq_table=None, mm_table=None):
    """PEC/EP altimétricos = EQ(escala) × coeficientes mm. Retorna (pec, ep) ou (None, None)."""
    eq_map = eq_table if eq_table is not None else DIC_EQ_BY_NOMINAL_SCALE
    mm_map = mm_table if mm_table is not None else DIC_PEC_MM
    try:
        scale_key = int(scale)
    except (TypeError, ValueError):
        scale_key = scale
    eq = eq_map.get(scale_key)
    if eq is None:
        eq = eq_map.get(scale)
    mm = (mm_map.get('V') or {}).get(class_)
    if eq is None or not mm:
        return None, None
    try:
        pec = float(eq) * float(mm['pec'])
        ep = float(eq) * float(mm['ep'])
    except (TypeError, ValueError, KeyError):
        return None, None
    return round(pec, 2), round(ep, 2)


def _build_dic_pec_v_from_eq():
    """Tabela derivada (compatibilidade): escala → classe → {pec, ep}."""
    out = {}
    for scale in DIC_EQ_BY_NOMINAL_SCALE:
        by_class = {}
        for class_ in DIC_PEC_MM['V']:
            pec, ep = pec_v_from_eq(scale, class_)
            if pec is None:
                continue
            by_class[class_] = {'pec': pec, 'ep': ep}
        if by_class:
            out[int(scale)] = by_class
    return out


# Antes havia limites absolutos só para 50/100/250; agora tudo vem de EQ×coef.
DIC_PEC_V = _build_dic_pec_v_from_eq()
DIC_PEC_ALT = DIC_PEC_V

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
