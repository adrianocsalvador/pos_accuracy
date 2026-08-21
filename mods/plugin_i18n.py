# -*- coding: utf-8 -*-
"""Contexto, locale e tradução partilhados (evita import circular entre módulos do plugin)."""
from __future__ import annotations

import os

from qgis.PyQt.QtCore import QCoreApplication, QLocale, QSettings, QTranslator

# Deve coincidir com <name> no arquivo .ts do Qt Linguist
PLUGIN_I18N_CONTEXT = 'PositionalAccuracyPlugin'

# Idioma de desenvolvimento (textos fonte no código Python)
LOCALE_DEV = 'pt_BR'
LOCALE_ES = 'es_ES'

SETTINGS_ORG = 'PositionalAccuracyPlugin'
SETTINGS_APP = 'PositionalAccuracyPlugin'
SETTINGS_KEY_LOCALE = 'ui_locale'

LOCALE_AUTO = 'auto'

_active_translator: QTranslator | None = None


def plugin_i18n_dir() -> str:
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'i18n')


def _normalize_locale_tag(raw) -> str:
    if raw is None:
        return ''
    s = str(raw).strip()
    if not s or s.upper() == 'NULL':
        return ''
    return s.replace('-', '_')


def effective_locale_tag(raw_tag: str) -> str:
    """Normaliza tags QGIS; «pt» genérico mapeia para pt_BR (idioma de desenvolvimento)."""
    tag = _normalize_locale_tag(raw_tag)
    if tag == 'pt':
        return LOCALE_DEV
    return tag


def is_english_locale(tag: str) -> bool:
    if not tag:
        return False
    t = tag.lower().replace('-', '_')
    return t == 'en' or t.startswith('en_')


def is_dev_locale(tag: str) -> bool:
    t = effective_locale_tag(tag)
    return t == LOCALE_DEV


def system_locale_tag() -> str:
    return effective_locale_tag(QLocale.system().name())


def qgis_locale_tag() -> str:
    """Locale UI do QGIS (QgsSettings → QSettings → sistema)."""
    candidates = []
    try:
        from qgis.core import QgsSettings
        gs = QgsSettings()
        override = gs.value('locale/overrideFlag', False, type=bool)
        if override:
            candidates.append(gs.value('locale/userLocale', ''))
        else:
            candidates.append(gs.value('locale/globalLocale', ''))
            candidates.append(gs.value('locale/userLocale', ''))
    except Exception:
        # QGIS QgsSettings pode falhar fora do app
        pass

    try:
        qs = QSettings()
        candidates.append(qs.value('locale/userLocale'))
        candidates.append(qs.value('locale/globalLocale'))
    except Exception:
        # QSettings de sistema opcional
        pass

    candidates.append(QLocale.system().name())

    for raw in candidates:
        tag = effective_locale_tag(raw)
        if tag:
            return tag
    return ''


def saved_ui_locale() -> str:
    return str(QSettings(SETTINGS_ORG, SETTINGS_APP).value(SETTINGS_KEY_LOCALE) or '').strip()


def save_ui_locale(code: str) -> None:
    settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
    settings.setValue(SETTINGS_KEY_LOCALE, code or LOCALE_AUTO)
    settings.sync()


def resolve_ui_locale() -> str:
    """Locale efectivo para carregar .qm (preferência guardada ou detecção automática)."""
    saved = saved_ui_locale()
    if saved and saved != LOCALE_AUTO:
        if saved == 'pt':
            return LOCALE_DEV
        return saved
    return qgis_locale_tag() or system_locale_tag() or LOCALE_DEV


def _locale_install_plan(locale_code: str | None = None) -> tuple[str, bool]:
    """Devolve (tag para procurar .qm, permitir fallback para en)."""
    code = locale_code if locale_code is not None else (saved_ui_locale() or LOCALE_AUTO)
    if code == LOCALE_AUTO:
        tag = qgis_locale_tag() or system_locale_tag() or LOCALE_DEV
        return tag, is_english_locale(tag)
    if code == 'pt':
        return LOCALE_DEV, False
    if code == LOCALE_ES:
        return LOCALE_ES, False
    if is_english_locale(code):
        return code, True
    if code == LOCALE_DEV:
        return LOCALE_DEV, False
    return code, False


def _qm_candidates(locale_tag: str, *, allow_en_fallback: bool = True) -> list[str]:
    """Arquivos .qm a tentar; pt_BR não faz fallback para pt genérico."""
    base = plugin_i18n_dir()
    tag = effective_locale_tag(locale_tag)
    tags = []
    if tag:
        tags.append(tag)
        if '_' in tag:
            lang, region = tag.split('_', 1)
            # pt_BR: só pos_accuracy_pt_BR.qm (sem pos_accuracy_pt.qm)
            if not (lang.lower() == 'pt' and region.upper() == 'BR'):
                tags.append(lang)
        elif len(tag) >= 2 and tag.lower() != 'pt':
            tags.append(tag[:2])
    if allow_en_fallback:
        tags.append('en')
    seen: set[str] = set()
    out: list[str] = []
    for cand in tags:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        path = os.path.join(base, f'pos_accuracy_{cand}.qm')
        if os.path.isfile(path):
            out.append(path)
    return out


def install_plugin_translator(locale_code: str | None = None) -> QTranslator | None:
    """Carrega e instala o QTranslator do plugin; devolve None se não houver .qm."""
    global _active_translator
    remove_plugin_translator()
    tag, allow_en = _locale_install_plan(locale_code)
    for qm_path in _qm_candidates(tag, allow_en_fallback=allow_en):
        translator = QTranslator()
        if translator.load(qm_path):
            QCoreApplication.installTranslator(translator)
            _active_translator = translator
            return translator
    return None


def remove_plugin_translator() -> None:
    global _active_translator
    if _active_translator is not None:
        QCoreApplication.removeTranslator(_active_translator)
        _active_translator = None


def ui_locale_choices() -> list[tuple[str, str]]:
    """Opções fixas do combo (a entrada «Idioma do QGIS (…)» é acrescentada à parte)."""
    return [
        ('en', 'English'),
        (LOCALE_ES, 'Español'),
        (LOCALE_DEV, 'Português (Brasil)'),
    ]


def locale_qgis_option_label() -> str:
    """Rótulo dinâmico da opção que segue o locale do QGIS."""
    tag = qgis_locale_tag() or system_locale_tag() or LOCALE_DEV
    return tr_ui('Idioma do QGIS ({0})').format(tag)


def effective_ui_locale_tag(locale_code: str | None = None) -> str:
    """Tag completa do locale efectivo (botão [pt_BR], [en], …)."""
    code = locale_code if locale_code is not None else (saved_ui_locale() or LOCALE_AUTO)
    qm_path = resolved_translation_qm_path(code)
    if qm_path:
        base = os.path.basename(qm_path)
        prefix, suffix = 'pos_accuracy_', '.qm'
        if base.startswith(prefix) and base.endswith(suffix):
            return base[len(prefix):-len(suffix)]
    tag, _ = _locale_install_plan(code)
    return tag or LOCALE_DEV


def locale_button_label(locale_code: str | None = None) -> str:
    """Texto do botão de idioma: [pt_BR], [en], [es_ES], …"""
    return f'[{effective_ui_locale_tag(locale_code)}]'


def resolved_translation_qm_path(locale_code: str) -> str | None:
    """Primeiro .qm que seria carregado para a opção indicada no combo."""
    code = locale_code or LOCALE_AUTO
    tag, allow_en = _locale_install_plan(code)
    for qm_path in _qm_candidates(tag, allow_en_fallback=allow_en):
        if os.path.isfile(qm_path):
            return qm_path
    return None


def expected_qm_basename(locale_code: str) -> str:
    code = locale_code or LOCALE_AUTO
    if code == LOCALE_AUTO:
        tag = qgis_locale_tag() or system_locale_tag() or LOCALE_DEV
    elif code == 'pt':
        tag = LOCALE_DEV
    else:
        tag = effective_locale_tag(code)
    return f'pos_accuracy_{tag}.qm'


def translation_qm_help_tooltip() -> str:
    return tr_ui(
        'Para criar tradução num idioma ainda sem arquivo .qm:\n'
        '1. Copie i18n/pos_accuracy_en.ts para pos_accuracy_<locale>.ts '
        '(ex.: pos_accuracy_es_ES.ts).\n'
        '2. Traduza no Qt Linguist ou edite o .ts (contexto PositionalAccuracyPlugin).\n'
        '3. Compile: execute i18n/build_translations.bat qm-only '
        '(requer pyside6-lrelease ou lrelease do OSGeo4W).\n'
        '4. Confirme que pos_accuracy_<locale>.qm ficou na pasta i18n/ '
        'e recarregue o plugin.\n'
        'Use pos_accuracy_en.ts como modelo — é a tradução completa de referência.\n'
        'Idioma de desenvolvimento (textos fonte): pt_BR.')


def translation_file_status(locale_code: str) -> dict:
    """Estado do .qm para o label na janela de idioma."""
    code = locale_code or LOCALE_AUTO
    tooltip = translation_qm_help_tooltip()
    tag, _ = _locale_install_plan(code)
    qm_path = resolved_translation_qm_path(code)
    if qm_path:
        basename = os.path.basename(qm_path)
        return {
            'found': True,
            'is_dev_source': False,
            'basename': basename,
            'label': tr_ui('Tradução: {0}').format(basename),
            'tooltip': tooltip,
        }
    if is_dev_locale(tag):
        return {
            'found': False,
            'is_dev_source': True,
            'basename': '',
            'label': tr_ui('Idioma de desenvolvimento ({0})').format(LOCALE_DEV),
            'tooltip': tooltip,
        }
    expected = expected_qm_basename(code)
    return {
        'found': False,
        'is_dev_source': False,
        'basename': '',
        'label': tr_ui('{0} não encontrado').format(expected),
        'tooltip': tooltip,
    }


def tr_ui(text: str) -> str:
    return QCoreApplication.translate(PLUGIN_I18N_CONTEXT, text)
