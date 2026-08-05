# -*- coding: utf-8 -*-
import base64
import datetime
import glob
import html
import json
import math
import os
import sqlite3
import statistics
from queue import Queue, Empty
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
# from sys import prefix

from osgeo import ogr
from qgis.PyQt.QtCore import (QSettings, Qt, QSize, QTranslator, QCoreApplication, QEvent, QThreadPool, QDateTime,
                              QVariant, QRectF, QByteArray, QBuffer, QIODevice, QMarginsF, QUrl)
from qgis.PyQt.QtGui import (
    QPixmap, QIcon, QFont, QPalette, QColor, QTextCharFormat, QBrush, QTextOption,
    QTextDocument, QPainter, QDesktopServices, QCursor,
)
from qgis.PyQt.QtPrintSupport import QPrinter
from qgis.PyQt.QtWidgets import (QAction, QScrollArea, QGridLayout, QHBoxLayout, QPushButton, QLabel, QWidget, QSizePolicy,
                                 QSpacerItem, QDockWidget, QSplitter, QComboBox, QLineEdit, QDialog, QFrame, QCheckBox,
                                 QHBoxLayout, QVBoxLayout, QFileDialog, QTableWidget, QStyle, QStyleOptionButton,
                                 QProgressBar, QDateEdit, QWidget, QVBoxLayout, QPushButton, QPlainTextEdit,
                                 QMessageBox, QApplication)
from qgis.core import (QgsVectorFileWriter, QgsWkbTypes, QgsCoordinateTransformContext, QgsCoordinateReferenceSystem,
                       QgsCoordinateTransform, QgsGeometry, QgsPointXY,
                       QgsFeature, QgsVectorLayer, QgsRasterLayer, QgsFields, QgsField, QgsProject,
                       QgsMapLayerProxyModel, QgsLayerTreeLayer, QgsDistanceArea)
from qgis.gui import QgsMapLayerComboBox
from .mod_aux_tools import AuxTools#, Obs2, Logger
from .mod_login import Database
from .mod_worker_threads import (
    DM_ABS_MAX_SANE, Worker, inspect_grass_processing, _windows_memory_status,
    _grass_memory_advice, _recommend_grass_memory_gb,
    build_compatibilized_profile_geometries,
    orient_line_high_to_low,
)
from .mod_settings import SettingsDlg
from .mod_language_dlg import LanguageDlg
from .mod_pec_constants import (
    ACCURACY_STANDARD_BR,
    ACCURACY_STANDARD_CE90,
    CLASS_CE90,
    CLASS_LE90,
    DIC_EQ_BY_NOMINAL_SCALE,
    DIC_PEC_ALT,
    DIC_PEC_MM,
    EP_RATIO_H,
    EP_RATIO_V,
    ce90_threshold_decimals,
)
from .plugin_i18n import (
    LOCALE_AUTO,
    PLUGIN_I18N_CONTEXT,
    SETTINGS_APP,
    SETTINGS_ORG,
    install_plugin_translator,
    locale_button_label,
    remove_plugin_translator,
    saved_ui_locale,
    tr_ui,
)

SETTINGS_KEY_OPEN_REPORT = 'open_report_after_run'

plugin_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.join(plugin_path, 'libs')))
# Arquivo de projeto: GeoPackage com extensão composta (conteúdo GPKG)
PROJECT_EXT = '.pa.gpkg'
PLUGIN_DISPLAY_NAME = 'MDE AP - Acurácia Posicional'

# Mesmo limite que BufferThread (|dm_h|/|dm_v| acima → NaN + WARNING no log).
_MAX_PEC_MEASUREMENT_ABS = DM_ABS_MAX_SANE


def _coerce_finite_measurement_scalar(x):
    """float finito para estatísticas PEC; aceita escalares numpy; None se inválido ou fora do limite."""
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    if not math.isfinite(v) or abs(v) > _MAX_PEC_MEASUREMENT_ABS:
        return None
    return v


def _coerce_finite_extent_m(x):
    """Comprimento de linha de referência (m); sem limite de 1000 m (só aplica-se a DM)."""
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    return v


def pec_test_limit(pec_, decimals=None):
    """Limiar PEC para o teste: inteiro (PEC-PCD) ou casas decimais (CE90/LE90)."""
    try:
        v = float(pec_)
    except (TypeError, ValueError):
        return 0 if decimals is None else 0.0
    if decimals is None:
        return int(round(v))
    return round(v, int(decimals))


def perc_pec_quant(values, pec_, decimals=None):
    """Percentual de amostras com DM <= PEC."""
    if not values:
        return 0.0
    pec_lim = pec_test_limit(pec_, decimals=decimals)
    return sum(1 for v in values if v <= pec_lim) / len(values)


def perc_pec_ext(values, extents, pec_, decimals=None):
    """Percentual da extensão total com DM <= PEC."""
    pec_lim = pec_test_limit(pec_, decimals=decimals)
    total_ext = sum(extents)
    if total_ext <= 0:
        return 0.0
    ok_ext = sum(ext for v, ext in zip(values, extents) if v <= pec_lim)
    return ok_ext / total_ext


# Limites PEC/EP altimétricos por escala nominal e classe (mods/mod_pec_constants.py)

def check_norm_values(values):
    """Teste de normalidade (Shapiro); exige ≥3 amostras."""
    if len(values) < 3:
        return False
    try:
        from scipy.stats import shapiro
        result = shapiro(values)
        return result[0] >= result[1]
    except Exception:
        return False


def pec_alt_limits(scale_, class_, dic_pec_alt=None):
    """Retorna (pec, ep) altimétricos arredondados para escala/classe."""
    table = dic_pec_alt if dic_pec_alt is not None else DIC_PEC_ALT
    limits = table.get(scale_, {}).get(class_)
    if not limits:
        return None, None
    return round(float(limits['pec']), 2), round(float(limits['ep']), 2)


def geometry_area_square_meters(geom: QgsGeometry, crs: QgsCoordinateReferenceSystem) -> float:
    """Área no elipsoide em m² (independente das unidades planas do CRS da camada)."""
    if geom is None or geom.isEmpty():
        return 0.0
    da = QgsDistanceArea()
    da.setSourceCrs(crs, QgsProject.instance().transformContext())
    # QGIS 3: não existe setEllipsoidalMode; setEllipsoid activa área/distância elipsoidal.
    ell = QgsProject.instance().ellipsoid()
    da.setEllipsoid(ell if ell else 'WGS84')
    return float(da.measureArea(geom))


PLUGIN_ICON_BASENAME = 'icon_bfn'
PPGEC_LOGO_FILENAME = 'PPGEC2025.png'


def _ui_device_pixel_ratio() -> float:
    try:
        from qgis.PyQt.QtWidgets import QApplication
        app = QApplication.instance()
        if app:
            return float(app.devicePixelRatio() or 1.0)
    except Exception:
        pass
    return 1.0


def _resolve_ppgec_logo_path() -> str:
    path_ = os.path.normpath(os.path.join(plugin_path, 'icons', PPGEC_LOGO_FILENAME))
    return path_ if os.path.isfile(path_) else ''


def _resolve_plugin_icon_path(prefer_svg: bool = True) -> str:
    """Retorna icons/icon_bfn.svg (preferido) ou .png se existir no disco."""
    names = ('icon_bfn.svg', 'icon_bfn.png') if prefer_svg else ('icon_bfn.png', 'icon_bfn.svg')
    for name in names:
        path_ = os.path.normpath(os.path.join(plugin_path, 'icons', name))
        if os.path.isfile(path_):
            return path_
    return ''


def _icon_from_icons_file(filename: str = None) -> QIcon:
    """QIcon nítido: prefere SVG (vetorial) para a barra de ferramentas do QGIS."""
    if filename:
        path_ = os.path.normpath(os.path.join(plugin_path, 'icons', filename))
    else:
        path_ = _resolve_plugin_icon_path(prefer_svg=True)
    if path_ and os.path.isfile(path_):
        return QIcon(path_)
    return QIcon()


def _make_dem_info_button(parent: QWidget = None) -> QPushButton:
    """Botão de informação do MDE (ícone v.info.2.png)."""
    icon_size = QSize(22, 22)
    btn = QPushButton(parent)
    ic = _icon_from_icons_file('v.info.2.png')
    if not ic.isNull():
        btn.setIcon(ic)
    else:
        btn.setText('i')
    btn.setIconSize(icon_size)
    btn.setFixedSize(icon_size)
    btn.setFlat(True)
    btn.setCursor(Qt.PointingHandCursor)
    btn.setFocusPolicy(Qt.StrongFocus)
    btn.setStyleSheet(
        'QPushButton { border: none; background: transparent; padding: 0; margin: 0; }'
        'QPushButton:hover { background: rgba(0, 0, 0, 0.06); border-radius: 11px; }'
        'QPushButton:pressed { background: rgba(0, 0, 0, 0.10); border-radius: 11px; }'
    )
    return btn


def _pixmap_from_icon_path(
    path_: str,
    size: QSize,
    *,
    smooth: bool = True,
    margin_ratio: float = 0.0,
    force_dpr: float = None,
) -> QPixmap:
    """
    Renderiza ícone no tamanho pedido, com suporte HiDPI.
    SVG via QSvgRenderer (QPixmap não lê SVG de forma fiável).
    margin_ratio: inset relativo (ex. 0.08 = 8% de margem) para não cortar traços na borda.
    force_dpr: se definido (ex. 1.0), ignora o DPR do ecrã — útil para PNG embutido em HTML/PDF.
    """
    if not path_ or not os.path.isfile(path_):
        return QPixmap()

    dpr = float(force_dpr) if force_dpr is not None else _ui_device_pixel_ratio()
    phys = QSize(max(1, int(size.width() * dpr)), max(1, int(size.height() * dpr)))
    inset = max(0.0, min(float(margin_ratio), 0.45))

    if path_.lower().endswith('.svg'):
        try:
            from qgis.PyQt.QtSvg import QSvgRenderer
            renderer = QSvgRenderer(path_)
            if renderer.isValid():
                pm = QPixmap(phys)
                pm.fill(Qt.transparent)
                pm.setDevicePixelRatio(dpr)
                painter = QPainter(pm)
                try:
                    if inset > 0:
                        m = inset
                        target = QRectF(
                            phys.width() * m,
                            phys.height() * m,
                            phys.width() * (1.0 - 2.0 * m),
                            phys.height() * (1.0 - 2.0 * m),
                        )
                        renderer.render(painter, target)
                    else:
                        renderer.render(painter)
                finally:
                    painter.end()
                return pm
        except Exception:
            pass

    pm = QPixmap(path_)
    if pm.isNull():
        return QPixmap()
    mode = Qt.SmoothTransformation if smooth else Qt.FastTransformation
    inner_w = max(1, int(phys.width() * (1.0 - 2.0 * inset)))
    inner_h = max(1, int(phys.height() * (1.0 - 2.0 * inset)))
    scaled = pm.scaled(QSize(inner_w, inner_h), Qt.KeepAspectRatio, mode)
    canvas = QPixmap(phys)
    canvas.fill(Qt.transparent)
    canvas.setDevicePixelRatio(dpr)
    painter = QPainter(canvas)
    try:
        x = (phys.width() - scaled.width()) // 2
        y = (phys.height() - scaled.height()) // 2
        painter.drawPixmap(x, y, scaled)
    finally:
        painter.end()
    return canvas


REPORT_PDF_MARGIN_MM = 10

# Larguras das colunas PEC no PDF (% da tabela; ordem = _pec_row_data_cells).
# Planimétrica: escala, classe, outliers, nº válidas, ext.(km), PEC quant., PEC ext., EP.
REPORT_PEC_NCOL_PLAN = 11
REPORT_PEC_NCOL_ALT = 12
REPORT_PEC_COL_WIDTHS_PLAN = (8, 5, 6, 6, 7, 8, 8, 8, 8, 9, 8)
# Altimétrica: escala, EQ, classe, outliers, nº válidas, ext.(km), PEC quant., PEC ext., EP.
REPORT_PEC_COL_WIDTHS_ALT = (8, 5, 5, 6, 6, 7, 8, 8, 8, 8, 9, 8)
# Envelope oficial no relatório (SIRGAS2000 geodésico, Brasil).
REPORT_ENVELOPE_OFFICIAL_CRS_AUTH = 'EPSG:4674'
REPORT_ENVELOPE_OFFICIAL_LABEL = 'SIRGAS2000 / EPSG:4674'
# Ao alterar: manter exatamente REPORT_PEC_NCOL_* valores; width só nas células <td> do corpo.
# Índices das colunas «Resultado» (plan. / alt.) na ordem de _pec_row_data_cells.
REPORT_PEC_RESULT_COL_IDX_PLAN = (6, 8, 10)
REPORT_PEC_RESULT_COL_IDX_ALT = (7, 9, 11)
REPORT_PEC_N_RESULT_COLS = 3


def _pec_result_col_indices(altimetric: bool) -> tuple:
    return REPORT_PEC_RESULT_COL_IDX_ALT if altimetric else REPORT_PEC_RESULT_COL_IDX_PLAN


def _pec_result_flags_to_status(result_ok, altimetric: bool) -> dict:
    """Mapeia coluna → True (pass) / False (fail) a partir de flags semânticos."""
    if not isinstance(result_ok, (list, tuple)):
        return {}
    col_indices = _pec_result_col_indices(altimetric)
    if len(result_ok) != len(col_indices):
        return {}
    status = {}
    for col_i, flag in zip(col_indices, result_ok):
        if flag is not None:
            status[col_i] = bool(flag)
    return status


def _pec_unpack_data_row(row, altimetric: bool):
    """Linha PEC: lista de células ou dict {cells, result_ok}."""
    if isinstance(row, dict):
        cells = row.get('cells') or []
        if row.get('ce90_layout'):
            altimetric = False
        status = _pec_result_flags_to_status(row.get('result_ok'), altimetric)
        return list(cells), status
    return list(row), {}


def _pec_format_result_cell_html(value, cell_fn, *, passed=None) -> str:
    """Colore células de resultado do PDF (estado semântico, independente do idioma)."""
    esc = cell_fn(value)
    if passed is True:
        return f'<span class="pec-pass">{esc}</span>'
    if passed is False:
        return f'<span class="pec-fail">{esc}</span>'
    return esc


def _normalize_col_widths_pct(widths_pct):
    """Normaliza larguras para somar 100 % (evita quebra do layout HTML no QTextDocument)."""
    if not widths_pct:
        return ()
    xs = []
    for w in widths_pct:
        try:
            xs.append(max(0.0, float(w)))
        except (TypeError, ValueError):
            xs.append(0.0)
    total = sum(xs)
    if total <= 0:
        n = len(xs)
        return tuple(round(100.0 / n, 2) for _ in range(n))
    return tuple(round(100.0 * x / total, 2) for x in xs)


def _pec_report_col_widths(altimetric: bool, col_widths: dict = None):
    """Larguras normalizadas; ignora tuplas com contagem errada (evita PDF em branco)."""
    n = REPORT_PEC_NCOL_ALT if altimetric else REPORT_PEC_NCOL_PLAN
    if col_widths:
        key = 'alt' if altimetric else 'plan'
        override = col_widths.get(key)
        if override and len(override) == n:
            return _normalize_col_widths_pct(override)
    raw = REPORT_PEC_COL_WIDTHS_ALT if altimetric else REPORT_PEC_COL_WIDTHS_PLAN
    if not raw or len(raw) != n:
        return tuple(round(100.0 / n, 2) for _ in range(n))
    return _normalize_col_widths_pct(raw)


def _pec_th_width_attr(widths_pct, idx) -> str:
    if idx < 0 or idx >= len(widths_pct):
        return ''
    return f' width="{widths_pct[idx]:.1f}%"'


def _pec_row_tds_html(cells, widths_pct, cell_fn, *, result_status_by_col=None) -> str:
    """Células `<td>` com atributo width (compatível com QTextDocument; sem colgroup)."""
    widths = _normalize_col_widths_pct(widths_pct)
    status = result_status_by_col or {}
    parts = []
    for i, c in enumerate(cells):
        w_attr = _pec_th_width_attr(widths, i)
        if i in status:
            inner = _pec_format_result_cell_html(c, cell_fn, passed=status[i])
        else:
            inner = cell_fn(c)
        parts.append(f'<td{w_attr}>{inner}</td>')
    return ''.join(parts)


def _configure_report_pdf_printer(printer, margin_mm: float = REPORT_PDF_MARGIN_MM) -> None:
    """Margens reais da folha (mm); compatível com PyQt5/6 no QGIS."""
    margin_mm = float(margin_mm)
    try:
        from qgis.PyQt.QtGui import QPageLayout, QPageSize
        layout = QPageLayout(
            QPageSize(QPageSize.A4),
            QPageLayout.Portrait,
            QMarginsF(margin_mm, margin_mm, margin_mm, margin_mm),
            QPageLayout.Millimeter,
        )
        printer.setPageLayout(layout)
        return
    except Exception:
        pass
    try:
        printer.setPageSize(QPrinter.A4)
    except Exception:
        pass
    try:
        printer.setPageMargins(
            QMarginsF(margin_mm, margin_mm, margin_mm, margin_mm),
            QPrinter.Millimeter,
        )
    except (ImportError, TypeError):
        printer.setPageMargins(
            margin_mm, margin_mm, margin_mm, margin_mm, QPrinter.Millimeter,
        )


def _printer_page_rect_points(printer) -> QRectF:
    """Área imprimível em pontos (1 pt = 1/72 pol)."""
    rect = printer.pageRect(QPrinter.Point)
    if rect.isEmpty() or rect.width() < 10 or rect.height() < 10:
        rect = printer.paperRect(QPrinter.Point)
    return rect


def _apply_painter_page_point_scale(painter, printer, page_rect_pt: QRectF) -> None:
    """HighResolution usa pixels de dispositivo; o documento está em pontos."""
    dev = printer.pageRect(QPrinter.DevicePixel)
    if dev.isEmpty() or page_rect_pt.isEmpty():
        return
    sx = dev.width() / page_rect_pt.width()
    sy = dev.height() / page_rect_pt.height()
    if sx > 0.01 and sy > 0.01:
        painter.scale(sx, sy)


def _print_html_to_pdf(printer, html_doc: str, *, font_size: int = 11) -> None:
    """
    Imprime HTML no PDF respeitando as margens do QPrinter.

    No QGIS/PyQt5 o layout do QTextDocument nem sempre expõe pageRect();
    nesse caso usa print_() (com resolução em pontos) ou paginação manual escalada.
    """
    page_rect = _printer_page_rect_points(printer)

    doc = QTextDocument()
    doc.setDefaultFont(QFont('Segoe UI', font_size))
    doc.setDocumentMargin(0)
    doc.setPageSize(page_rect.size())
    doc.setHtml(html_doc)

    layout = doc.documentLayout()
    page_rect_fn = getattr(layout, 'pageRect', None)
    page_count_fn = getattr(layout, 'pageCount', None)

    if page_rect_fn is None or page_count_fn is None:
        doc.print_(printer)
        return

    page_h = float(page_rect.height())
    doc_h = float(doc.size().height())

    drew_any = False
    if doc_h > 1.0 and page_h > 1.0:
        painter = QPainter()
        if painter.begin(printer):
            try:
                _apply_painter_page_point_scale(painter, printer, page_rect)
                page_count = page_count_fn()
                for page_idx in range(page_count):
                    rect = page_rect_fn(page_idx)
                    if rect.isEmpty():
                        continue
                    if page_idx > 0:
                        printer.newPage()
                    painter.save()
                    painter.translate(0, -rect.top())
                    doc.drawContents(painter, rect)
                    painter.restore()
                    drew_any = True
            except Exception:
                drew_any = False
            finally:
                painter.end()

    if not drew_any:
        doc.print_(printer)


def write_pdf_from_html_doc(
    html_doc: str,
    pdf_path: str,
    *,
    margin_mm: float = None,
    font_size: int = None,
) -> str:
    """Grava PDF a partir de HTML (mesma pipeline do relatório no plugin)."""
    if margin_mm is None:
        margin_mm = REPORT_PDF_MARGIN_MM
    if font_size is None:
        font_size = REPORT_PDF_FONTS_DEFAULT['doc_default']
    pdf_path = os.path.normpath(os.path.abspath(pdf_path))
    os.makedirs(os.path.dirname(pdf_path) or '.', exist_ok=True)
    printer = QPrinter(QPrinter.HighResolution)
    printer.setOutputFormat(QPrinter.PdfFormat)
    printer.setOutputFileName(pdf_path)
    # 72 dpi → coordenadas do QTextDocument em pontos coincidem com o QPrinter.
    try:
        printer.setResolution(72)
    except Exception:
        pass
    _configure_report_pdf_printer(printer, margin_mm)
    _print_html_to_pdf(printer, html_doc, font_size=font_size)
    return pdf_path


def export_pdf_from_html_file(
    html_path: str,
    out_pdf: str = None,
    *,
    margin_mm: float = None,
) -> str:
    """Gera PDF a partir de ficheiro HTML exportado pelo plugin."""
    html_path = os.path.normpath(os.path.abspath(html_path))
    if not os.path.isfile(html_path):
        raise FileNotFoundError(html_path)
    with open(html_path, encoding='utf-8') as f:
        html_doc = f.read()
    if out_pdf:
        pdf_path = os.path.normpath(os.path.abspath(out_pdf))
    else:
        root, _ = os.path.splitext(html_path)
        pdf_path = root + '.pdf'
    return write_pdf_from_html_doc(html_doc, pdf_path, margin_mm=margin_mm)


def _parse_project_path_from_report_txt(txt: str) -> str:
    """Extrai caminho .pa.gpkg da linha «Ficheiro de projeto:» do relatório TXT."""
    prefixes = (
        'Ficheiro de projeto:',
        'Project file:',
    )
    for line in txt.splitlines():
        stripped = line.strip()
        for prefix in prefixes:
            if stripped.startswith(prefix):
                rest = stripped[len(prefix):].strip()
                if rest.startswith('\t'):
                    rest = rest[1:].strip()
                if rest:
                    return os.path.normpath(os.path.abspath(_report_txt_unescape_cell(rest)))
    return ''


def _companion_report_path(txt_path: str, ext: str) -> str:
    root, _ = os.path.splitext(os.path.normpath(os.path.abspath(txt_path)))
    return root + ext


# Formato TXT v1: parseável para regenerar PDF sem correr o pipeline.
REPORT_TX_V1_MARKER = '=== MDE-AP-REPORT v1 ==='
PROFILES_WKT_TX_V1_MARKER = '=== MDE-AP-PROFILES-WKT v1 ==='
REPORT_TX_V1_MARKER_LEGACY = '=== MDE-PA-REPORT v1 ==='
REPORT_TX_V1_MARKERS = (REPORT_TX_V1_MARKER, REPORT_TX_V1_MARKER_LEGACY)


def _report_txt_is_v1_marker(line: str) -> bool:
    return line in REPORT_TX_V1_MARKERS


def _report_txt_contains_v1_marker(txt: str) -> bool:
    return REPORT_TX_V1_MARKER in txt or REPORT_TX_V1_MARKER_LEGACY in txt
# Tamanhos de fonte do PDF/HTML (pt); o plugin usa estes valores por defeito.
REPORT_PDF_FONTS_DEFAULT = {
    'body': 7,
    'h1': 11,
    'h2': 9,
    'h3': 8,
    'pec': 6,
    'doc_default': 7,
}
_REPORT_TX_SECTION_IDS = (
    'location', 'workflow', 'dems', 'params', 'stats', 'pairs', 'pec',
)


def _report_txt_escape_cell(value) -> str:
    if value is None:
        return ''
    s = str(value)
    return s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '')


def _report_txt_unescape_cell(value: str) -> str:
    if not value:
        return ''
    out = []
    i = 0
    while i < len(value):
        if value[i] == '\\' and i + 1 < len(value):
            nxt = value[i + 1]
            if nxt == 't':
                out.append('\t')
                i += 2
                continue
            if nxt == 'n':
                out.append('\n')
                i += 2
                continue
            if nxt == '\\':
                out.append('\\')
                i += 2
                continue
        out.append(value[i])
        i += 1
    return ''.join(out)


def _report_txt_split_row(line: str) -> list:
    parts = line.split('\t')
    return [_report_txt_unescape_cell(p) for p in parts]


def _report_txt_join_row(parts) -> str:
    return '\t'.join(_report_txt_escape_cell(p) for p in parts)


def _report_lines_to_kv_rows(lines: list[str]) -> list[dict]:
    rows = []
    for ln in lines or []:
        s = (ln or '').strip()
        if not s:
            continue
        if ': ' in s:
            label, value = s.split(': ', 1)
            rows.append({'label': label.strip(), 'value': value.strip()})
        elif ':' in s:
            label, value = s.split(':', 1)
            rows.append({'label': label.strip(), 'value': value.strip()})
        else:
            rows.append({'label': '', 'value': s})
    return rows


def _report_sec_kv_rows(sec: dict) -> list[dict]:
    if not sec:
        return []
    rows = sec.get('rows')
    if rows is not None:
        return rows
    if sec.get('lines'):
        return _report_lines_to_kv_rows(sec['lines'])
    return []


def _pairs_section_rows(pairs_sec: dict) -> list[dict]:
    if not pairs_sec:
        return []
    rows = pairs_sec.get('rows')
    if rows is not None:
        return rows
    out = []
    norm = (pairs_sec.get('norm_label') or '').strip()
    if norm:
        out.append({
            'label': pairs_sec.get('norm_caption') or 'Método de normalização de progressivas',
            'value': norm,
        })
    for ln in pairs_sec.get('intro_lines') or []:
        out.extend(_report_lines_to_kv_rows([ln]))
    wkt = (pairs_sec.get('wkt_file') or '').strip()
    if wkt:
        out.append({
            'label': pairs_sec.get('wkt_file_caption') or 'Ficheiro WKT dos perfis',
            'value': wkt,
        })
    for ln in pairs_sec.get('stat_lines') or []:
        out.extend(_report_lines_to_kv_rows([ln]))
    return out


def _render_report_kv_table_html(
    header: list,
    rows: list,
    *,
    label_key: str = 'label',
    value_key: str = 'value',
    option_key: str = 'option',
) -> str:
    hdr = header or ['Campo', 'Valor']
    col1 = html.escape(str(hdr[0] if hdr else 'Campo'))
    col2 = html.escape(str(hdr[1] if len(hdr) > 1 else 'Valor'))
    body = ''
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if row.get('is_group'):
            body += (
                f'<tr><td colspan="2" style="background:#eee">'
                f'<b>{html.escape(str(row.get(label_key) or row.get(option_key) or ""))}</b>'
                f'</td></tr>'
            )
            continue
        k1 = row.get(label_key) or row.get(option_key) or ''
        k2 = row.get(value_key) or ''
        body += (
            f'<tr><td>{html.escape(str(k1))}</td>'
            f'<td>{html.escape(str(k2))}</td></tr>'
        )
    if not body:
        return ''
    return f'<table><tr><th>{col1}</th><th>{col2}</th></tr>{body}</table>'


def _location_section_envelope_rows(loc_sec: dict) -> list[dict]:
    rows = loc_sec.get('rows') or []
    return [r for r in rows if isinstance(r, dict) and 'xmin' in r]


def _render_location_section_html(loc_sec: dict) -> str:
    if not loc_sec:
        return ''
    envelope_rows = _location_section_envelope_rows(loc_sec)
    if envelope_rows:
        header = loc_sec.get('header') or ['Envelope', 'Xmin', 'Ymin', 'Xmax', 'Ymax']
        cols = header[:5] if len(header) >= 5 else ['Envelope', 'Xmin', 'Ymin', 'Xmax', 'Ymax']
        thead = ''.join(f'<th>{html.escape(str(h))}</th>' for h in cols)
        body = ''
        for row in envelope_rows:
            body += (
                f'<tr><td>{html.escape(str(row.get("label", "")))}</td>'
                f'<td>{html.escape(str(row.get("xmin", "")))}</td>'
                f'<td>{html.escape(str(row.get("ymin", "")))}</td>'
                f'<td>{html.escape(str(row.get("xmax", "")))}</td>'
                f'<td>{html.escape(str(row.get("ymax", "")))}</td></tr>'
            )
        for row in loc_sec.get('rows') or []:
            if not isinstance(row, dict) or 'xmin' in row:
                continue
            val = row.get('value', '')
            if val:
                body += (
                    f'<tr><td>{html.escape(str(row.get("label", "")))}</td>'
                    f'<td colspan="4">{html.escape(str(val))}</td></tr>'
                )
        return f'<table class="location-envelope"><tr>{thead}</tr>{body}</table>'
    return _render_report_kv_table_html(
        loc_sec.get('header') or ['Opção', 'Valor'],
        _report_sec_kv_rows(loc_sec),
    )


def format_full_report_txt(snapshot: dict) -> str:
    """Serializa snapshot completo (mesmo conteúdo lógico do PDF)."""
    lines = [REPORT_TX_V1_MARKER, '']
    meta = snapshot.get('meta') or {}
    labels = meta.get('labels') or {}
    title_key = labels.get('title', 'Título')
    dt_key = labels.get('datetime', 'Data/hora')
    proj_key = labels.get('project_file', 'Ficheiro de projeto')
    crs_key = labels.get('crs', 'CRS de referência (análise)')
    lines.append('[META]')
    lines.append(f'{title_key}:\t{_report_txt_escape_cell(meta.get("title", ""))}')
    lines.append(f'{dt_key}:\t{_report_txt_escape_cell(meta.get("datetime", ""))}')
    lines.append(f'{proj_key}:\t{_report_txt_escape_cell(meta.get("project_file", ""))}')
    lines.append(f'{crs_key}:\t{_report_txt_escape_cell(meta.get("crs", ""))}')
    lines.append('')

    sections = snapshot.get('sections') or {}
    for sid in _REPORT_TX_SECTION_IDS:
        sec = sections.get(sid)
        if not sec:
            continue
        lines.append(f'[SECTION {sid}]')
        lines.append(f'TITLE\t{_report_txt_escape_cell(sec.get("title", ""))}')
        if sid == 'location':
            loc_rows = sec.get('rows') or []
            envelope_rows = [r for r in loc_rows if isinstance(r, dict) and 'xmin' in r]
            if envelope_rows:
                hdr = sec.get('header') or ['Envelope', 'Xmin', 'Ymin', 'Xmax', 'Ymax']
                lines.append(f'HEADER\t{_report_txt_join_row(hdr[:5])}')
                for row in envelope_rows:
                    lines.append(f'ENVROW\t{_report_txt_join_row([
                        row.get("label", ""),
                        row.get("xmin", ""),
                        row.get("ymin", ""),
                        row.get("xmax", ""),
                        row.get("ymax", ""),
                    ])}')
                for row in loc_rows:
                    if isinstance(row, dict) and 'xmin' not in row and row.get('value'):
                        lines.append(f'ROW\t{_report_txt_join_row([row.get("label", ""), row.get("value", "")])}')
            else:
                stat_rows = _report_sec_kv_rows(sec)
                lines.append(f'HEADER\t{_report_txt_join_row(sec.get("header") or ["Opção", "Valor"])}')
                for row in stat_rows:
                    lines.append(f'ROW\t{_report_txt_join_row([row.get("label", ""), row.get("value", "")])}')
                if not stat_rows:
                    for ln in sec.get('lines') or []:
                        lines.append(f'LINE\t{_report_txt_escape_cell(ln)}')
        elif sid == 'workflow':
            lines.append(f'HEADER\t{_report_txt_join_row(sec.get("header") or ["Opção", "Valor"])}')
            for row in sec.get('rows') or []:
                lines.append(f'ROW\t{_report_txt_join_row([row.get("option", ""), row.get("value", "")])}')
        elif sid == 'dems':
            lines.append(f'HEADER\t{_report_txt_join_row(sec.get("header") or ["Papel", "Nome", "Fonte (início)"])}')
            for row in sec.get('rows') or []:
                lines.append(f'ROW\t{_report_txt_join_row([row.get("role", ""), row.get("name", ""), row.get("source", "")])}')
        elif sid == 'params':
            lines.append(f'HEADER\t{_report_txt_join_row(sec.get("header") or ["Parâmetro", "Valor"])}')
            for grp in sec.get('groups') or []:
                lines.append(f'GROUP\t{_report_txt_escape_cell(grp.get("label", ""))}')
                for fld in grp.get('fields') or []:
                    lines.append(f'PARAM\t{_report_txt_join_row([fld.get("label", ""), fld.get("value", "")])}')
        elif sid == 'stats':
            stat_rows = _report_sec_kv_rows(sec)
            lines.append(f'HEADER\t{_report_txt_join_row(sec.get("header") or ["Opção", "Valor"])}')
            for row in stat_rows:
                lines.append(f'ROW\t{_report_txt_join_row([row.get("label", ""), row.get("value", "")])}')
            if not stat_rows:
                for ln in sec.get('lines') or []:
                    lines.append(f'STAT\t{_report_txt_escape_cell(ln)}')
        elif sid == 'pairs':
            pair_rows = _pairs_section_rows(sec)
            lines.append(f'HEADER\t{_report_txt_join_row(sec.get("header") or ["Opção", "Valor"])}')
            for row in pair_rows:
                if row.get('is_group'):
                    lines.append(
                        f'GROUP\t{_report_txt_escape_cell(row.get("label", ""))}')
                else:
                    lines.append(
                        f'ROW\t{_report_txt_join_row([row.get("label", ""), row.get("value", "")])}')
            if not pair_rows:
                if sec.get('norm_label'):
                    lines.append(f'NORM\t{_report_txt_escape_cell(sec.get("norm_label", ""))}')
                if sec.get('wkt_file'):
                    lines.append(f'WKTFILE\t{_report_txt_escape_cell(sec["wkt_file"])}')
                for ln in sec.get('intro_lines') or []:
                    lines.append(f'PINTRO\t{_report_txt_escape_cell(ln)}')
                for ln in sec.get('stat_lines') or []:
                    lines.append(f'PSTAT\t{_report_txt_escape_cell(ln)}')
            if sec.get('empty_message'):
                lines.append(f'EMPTY\t{_report_txt_escape_cell(sec["empty_message"])}')
        elif sid == 'pec':
            intro = (sec.get('intro') or '').strip()
            if intro:
                lines.append(f'INTRO\t{_report_txt_escape_cell(intro)}')
            for k, v in (sec.get('header_labels') or {}).items():
                lines.append(f'LABEL\t{k}\t{_report_txt_escape_cell(v)}')
            for block_key, table_key in (('plan', 'plan'), ('alt', 'alt')):
                block = sec.get(table_key) or {}
                title = block.get('title', '')
                if title:
                    lines.append(f'SUBSECTION\t{_report_txt_escape_cell(title)}')
                for i, hdr in enumerate(block.get('header_rows') or []):
                    lines.append(f'PEC_HEADER\t{block_key}\t{i}\t{_report_txt_join_row(hdr)}')
                for row in block.get('data_rows') or []:
                    if isinstance(row, dict):
                        cells = row.get('cells') or []
                        lines.append(
                            f'PEC_ROW\t{block_key}\t{_report_txt_join_row(cells)}')
                        flags = row.get('result_ok')
                        if isinstance(flags, (list, tuple)) and len(flags) == REPORT_PEC_N_RESULT_COLS:
                            flag_cells = []
                            for flag in flags:
                                if flag is True:
                                    flag_cells.append('1')
                                elif flag is False:
                                    flag_cells.append('0')
                                else:
                                    flag_cells.append('-')
                            lines.append(
                                f'PEC_FLAGS\t{block_key}\t{_report_txt_join_row(flag_cells)}')
                    else:
                        lines.append(
                            f'PEC_ROW\t{block_key}\t{_report_txt_join_row(row)}')
            if sec.get('empty_message'):
                lines.append(f'EMPTY\t{_report_txt_escape_cell(sec["empty_message"])}')
        lines.append('')

    return '\n'.join(lines).rstrip() + '\n'


def _default_pec_results_header_labels() -> dict:
    """Rótulos PEC (PT); no plugin usa-se `Wd1._pec_results_header_labels()` traduzido."""
    return {
        'escala': 'Escala',
        'eq': 'EQ (m)',
        'classe': 'Classe',
        'outliers': 'Outliers',
        'amostras': 'Amostras Válidas',
        'quant': 'Quant.',
        'ext_km': 'Ext. (km)',
        'pec_group': 'PEC (90% d_i ≤ PEC-PCD)',
        'pec_quant': 'Quantitativo',
        'pec_ext': 'Extensão',
        'teste': 'Teste',
        'resultado': 'Resultado',
        'ep_group': 'EP (RMS ≤ EP)',
    }


def _pec_results_table_head_html(
    altimetric: bool = False,
    header_labels: dict = None,
    widths_pct=None,
) -> str:
    """Cabeçalho HTML em 3 níveis (colspan/rowspan; sem width no thead — compatível QTextDocument)."""
    lb = header_labels or _default_pec_results_header_labels()
    # CE90/LE90: mesma grelha planimétrica (valor | RODADA | …), sem coluna EQ/classe extra
    if lb.get('ce90_layout'):
        altimetric = False
    esc = html.escape
    _ = widths_pct

    def th(label, **attrs):
        attr_s = ' '.join(f'{k}="{v}"' for k, v in attrs.items())
        return f'<th {attr_s}>{esc(label)}</th>' if attr_s else f'<th>{esc(label)}</th>'

    if altimetric:
        row1 = (
            th(lb['escala'], rowspan='3')
            + th(lb['eq'], rowspan='3')
            + th(lb['classe'], rowspan='3')
            + th(lb['outliers'], rowspan='3')
            + f'<th colspan="2" rowspan="2">{esc(lb["amostras"])}</th>'
            + f'<th colspan="4">{esc(lb["pec_group"])}</th>'
            + f'<th colspan="2" rowspan="2">{esc(lb["ep_group"])}</th>'
        )
        row3 = (
            th(lb['quant'])
            + th(lb['ext_km'])
            + th(lb['teste'])
            + th(lb['resultado'])
            + th(lb['teste'])
            + th(lb['resultado'])
            + th(lb['teste'])
            + th(lb['resultado'])
        )
    else:
        row1 = (
            th(lb['escala'], rowspan='3')
            + th(lb['classe'], rowspan='3')
            + th(lb['outliers'], rowspan='3')
            + f'<th colspan="2" rowspan="2">{esc(lb["amostras"])}</th>'
            + f'<th colspan="4">{esc(lb["pec_group"])}</th>'
            + f'<th colspan="2" rowspan="2">{esc(lb["ep_group"])}</th>'
        )
        row3 = (
            th(lb['quant'])
            + th(lb['ext_km'])
            + th(lb['teste'])
            + th(lb['resultado'])
            + th(lb['teste'])
            + th(lb['resultado'])
            + th(lb['teste'])
            + th(lb['resultado'])
        )
    row2 = (
        f'<th colspan="2">{esc(lb["pec_quant"])}</th>'
        f'<th colspan="2">{esc(lb["pec_ext"])}</th>'
    )
    return f'<thead class="pec-thead">\n<tr>{row1}</tr>\n<tr>{row2}</tr>\n<tr>{row3}</tr>\n</thead>'


def _build_pec_results_tables_html_blocks(
    *,
    intro: str = '',
    plan_title: str = '7.1 PEC Planimétrico',
    alt_title: str = '7.2 PEC Altimétrico',
    plan_data_rows=None,
    alt_data_rows=None,
    empty_message: str = '',
    header_labels: dict = None,
    plan_header_labels: dict = None,
    alt_header_labels: dict = None,
    col_widths: dict = None,
    plan_page_break: bool = True,
    alt_page_break: bool = True,
) -> str:
    """HTML das tabelas PEC (plugin e script TXT→PDF usam a mesma função)."""
    plan_data_rows = plan_data_rows or []
    alt_data_rows = alt_data_rows or []
    plan_lb = plan_header_labels or header_labels
    alt_lb = alt_header_labels or header_labels

    def cell(v):
        return html.escape('' if v is None else str(v))

    def table_html(title, altimetric, rows, *, page_break=False, labels=None):
        if not rows:
            return ''
        labels = labels or header_labels
        layout_alt = bool(altimetric) and not bool((labels or {}).get('ce90_layout'))
        widths = _pec_report_col_widths(layout_alt, col_widths)
        thead = _pec_results_table_head_html(
            altimetric=layout_alt, header_labels=labels)
        body = ''
        for row in rows:
            cells, status = _pec_unpack_data_row(row, layout_alt)
            tds = _pec_row_tds_html(
                cells, widths, cell, result_status_by_col=status)
            body += f'<tr>{tds}</tr>'
        pb_cls = ' class="pec-section page-break"' if page_break else ' class="pec-section"'
        return (
            f'<h3{pb_cls}>{html.escape(title)}</h3>\n'
            f'<table class="pec-results">\n{thead}\n<tbody>\n{body}</tbody></table>'
        )

    chunks = []
    if (intro or '').strip():
        chunks.append(f'<p>{html.escape(intro.strip())}</p>')
    if plan_data_rows:
        chunks.append(table_html(
            plan_title, False, plan_data_rows,
            page_break=plan_page_break, labels=plan_lb))
    if alt_data_rows:
        chunks.append(table_html(
            alt_title, True, alt_data_rows,
            page_break=alt_page_break, labels=alt_lb))
    if not plan_data_rows and not alt_data_rows and empty_message:
        chunks.append(f'<p>{html.escape(empty_message)}</p>')
    return '\n'.join(chunks)


def _format_report_scalar_k(value, *, decimals: int = 2) -> str:
    if value is None:
        return ''
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ''
    if not math.isfinite(v):
        return ''
    return f'{v:.{int(decimals)}f}'


def _profile_geometry_wkt_for_report(geom, *, decimals: int = 2) -> str:
    """WKT do perfil compatibilizado (progressiva, cota) com casas decimais fixas."""
    if geom is None or geom.isEmpty():
        return ''
    try:
        g = QgsGeometry(geom)
        if g.isMultipart():
            parts = g.asMultiPolyline()
            pts = parts[0] if parts else []
        else:
            pts = g.asPolyline()
        if not pts:
            return ''
        dec = int(decimals)
        fmt = f'{{:.{dec}f}}'
        coords = ', '.join(
            f'{fmt.format(round(p.x(), dec))} {fmt.format(round(p.y(), dec))}'
            for p in pts
        )
        return f'LINESTRING ({coords})'
    except Exception:
        return ''


def format_profiles_wkt_txt(
    profile_data: dict,
    *,
    datetime_str: str = '',
    project_file: str = '',
    labels: dict = None,
) -> str:
    """TXT separado com WKT par a par (não faz parte do relatório principal)."""
    labels = labels or {}
    pairs = profile_data.get('pairs') or []
    if not pairs:
        return ''
    lbl_dt = labels.get('datetime', 'Data/hora')
    lbl_proj = labels.get('project_file', 'Ficheiro de projeto')
    lbl_norm = labels.get('norm_caption', 'Método de normalização de progressivas')
    lbl_pair = labels.get('pair', 'Par')
    lbl_ref_id = labels.get('ref_id', 'ref_id')
    lbl_layer_ref = labels.get('layer_ref', 'camada_ref')
    lbl_wkt_ref = labels.get('wkt_ref', 'wkt_ref')
    lbl_test_id = labels.get('test_id', 'test_id')
    lbl_layer_test = labels.get('layer_test', 'camada_test')
    lbl_wkt_test = labels.get('wkt_test', 'wkt_test')
    lbl_scalar = labels.get('scalar_k', 'Escalar k (linear)')

    lines = [PROFILES_WKT_TX_V1_MARKER, '']
    if datetime_str:
        lines.append(f'{lbl_dt}:\t{_report_txt_escape_cell(datetime_str)}')
    if project_file:
        lines.append(f'{lbl_proj}:\t{_report_txt_escape_cell(project_file)}')
    norm_label = (profile_data.get('norm_label') or '').strip()
    if norm_label:
        lines.append(f'{lbl_norm}:\t{_report_txt_escape_cell(norm_label)}')
    lines.append('')

    for par in pairs:
        n = par.get('n', '')
        tag = par.get('morph_tag', '')
        header = f'{lbl_pair} {n}'
        if tag:
            header += f' — {tag}'
        lines.append(header)
        lines.append(
            f'{lbl_ref_id}:\t{_report_txt_escape_cell(par.get("ref_id", ""))}\t'
            f'{lbl_layer_ref}:\t{_report_txt_escape_cell(par.get("layer_ref", ""))}')
        lines.append(f'{lbl_wkt_ref}:')
        lines.append(par.get('wkt_ref') or '')
        lines.append(
            f'{lbl_test_id}:\t{_report_txt_escape_cell(par.get("test_id", ""))}\t'
            f'{lbl_layer_test}:\t{_report_txt_escape_cell(par.get("layer_test", ""))}')
        lines.append(f'{lbl_wkt_test}:')
        lines.append(par.get('wkt_test') or '')
        sk = _format_report_scalar_k(par.get('scalar_k'), decimals=2)
        if sk:
            lines.append(f'{lbl_scalar}:\t{sk}')
        lines.append('')

    return '\n'.join(lines).rstrip() + '\n'


def _render_pairs_section_html(pairs_sec: dict) -> str:
    """HTML das estatísticas de pares (WKT num ficheiro .txt separado)."""
    if not pairs_sec:
        return ''
    rows = _pairs_section_rows(pairs_sec)
    table = _render_report_kv_table_html(
        pairs_sec.get('header') or ['Opção', 'Valor'],
        rows,
    )
    chunks = []
    if table:
        chunks.append(table)
    if not rows and not (pairs_sec.get('intro_lines') or pairs_sec.get('stat_lines')):
        empty = (pairs_sec.get('empty_message') or '').strip()
        if empty:
            chunks.append(f'<p>{html.escape(empty)}</p>')
    return '\n'.join(chunks)


def _normalize_report_meta_key(key: str) -> str:
    kl = (key or '').strip().lower()
    return {
        'título': 'title',
        'title': 'title',
        'data/hora': 'datetime',
        'date/time': 'datetime',
        'ficheiro de projeto': 'project_file',
        'project file': 'project_file',
        'crs de referência (análise)': 'crs',
        'reference crs (analysis)': 'crs',
    }.get(kl, (key or '').strip())


def parse_full_report_txt(txt: str) -> dict:
    """Lê relatório TXT v1; devolve snapshot para `render_pdf_report_html`."""
    if not _report_txt_contains_v1_marker(txt):
        raise ValueError(
            f'Formato de relatório não reconhecido (esperado marcador {REPORT_TX_V1_MARKER!r} '
            f'ou legado {REPORT_TX_V1_MARKER_LEGACY!r}).')

    snapshot = {
        'meta': {},
        'sections': {
            sid: {'title': ''} for sid in _REPORT_TX_SECTION_IDS
        },
    }
    current = None
    pec_sub = None

    for raw_line in txt.splitlines():
        line = raw_line.strip()
        if not line or _report_txt_is_v1_marker(line):
            continue
        if line == '[META]':
            current = 'meta'
            continue
        if line.startswith('[SECTION '):
            sid = line[len('[SECTION '):].rstrip(']').strip()
            if sid in snapshot['sections']:
                current = sid
                pec_sub = None
            else:
                current = None
            continue
        if current == 'meta':
            if ':' in line:
                k, v = line.split(':', 1)
                nk = _normalize_report_meta_key(k)
                snapshot['meta'][nk] = _report_txt_unescape_cell(v.strip().lstrip('\t'))
            continue
        if current is None:
            continue

        sec = snapshot['sections'][current]
        if line.startswith('TITLE\t'):
            sec['title'] = _report_txt_unescape_cell(line.split('\t', 1)[1])
            continue

        if current == 'location':
            if line.startswith('HEADER\t'):
                sec['header'] = _report_txt_split_row(line.split('\t', 1)[1])
            elif line.startswith('ENVROW\t'):
                cells = _report_txt_split_row(line.split('\t', 1)[1])
                sec.setdefault('rows', []).append({
                    'label': cells[0] if cells else '',
                    'xmin': cells[1] if len(cells) > 1 else '',
                    'ymin': cells[2] if len(cells) > 2 else '',
                    'xmax': cells[3] if len(cells) > 3 else '',
                    'ymax': cells[4] if len(cells) > 4 else '',
                })
            elif line.startswith('ROW\t'):
                cells = _report_txt_split_row(line.split('\t', 1)[1])
                sec.setdefault('rows', []).append({
                    'label': cells[0] if cells else '',
                    'value': cells[1] if len(cells) > 1 else '',
                })
            elif line.startswith('LINE\t'):
                sec.setdefault('lines', []).append(_report_txt_unescape_cell(line.split('\t', 1)[1]))
        elif current == 'workflow':
            if line.startswith('HEADER\t'):
                sec['header'] = _report_txt_split_row(line.split('\t', 1)[1])
            elif line.startswith('ROW\t'):
                cells = _report_txt_split_row(line.split('\t', 1)[1])
                sec.setdefault('rows', []).append({
                    'option': cells[0] if cells else '',
                    'value': cells[1] if len(cells) > 1 else '',
                })
        elif current == 'dems':
            if line.startswith('HEADER\t'):
                sec['header'] = _report_txt_split_row(line.split('\t', 1)[1])
            elif line.startswith('ROW\t'):
                cells = _report_txt_split_row(line.split('\t', 1)[1])
                sec.setdefault('rows', []).append({
                    'role': cells[0] if cells else '',
                    'name': cells[1] if len(cells) > 1 else '',
                    'source': cells[2] if len(cells) > 2 else '',
                })
        elif current == 'params':
            if line.startswith('HEADER\t'):
                sec['header'] = _report_txt_split_row(line.split('\t', 1)[1])
            elif line.startswith('GROUP\t'):
                sec.setdefault('groups', []).append({
                    'label': _report_txt_unescape_cell(line.split('\t', 1)[1]),
                    'fields': [],
                })
            elif line.startswith('PARAM\t'):
                cells = _report_txt_split_row(line.split('\t', 1)[1])
                groups = sec.setdefault('groups', [])
                if not groups:
                    groups.append({'label': '', 'fields': []})
                groups[-1]['fields'].append({
                    'label': cells[0] if cells else '',
                    'value': cells[1] if len(cells) > 1 else '',
                })
        elif current == 'stats':
            if line.startswith('HEADER\t'):
                sec['header'] = _report_txt_split_row(line.split('\t', 1)[1])
            elif line.startswith('ROW\t'):
                cells = _report_txt_split_row(line.split('\t', 1)[1])
                sec.setdefault('rows', []).append({
                    'label': cells[0] if cells else '',
                    'value': cells[1] if len(cells) > 1 else '',
                })
            elif line.startswith('STAT\t'):
                sec.setdefault('lines', []).append(_report_txt_unescape_cell(line.split('\t', 1)[1]))
        elif current == 'pairs':
            if line.startswith('HEADER\t'):
                sec['header'] = _report_txt_split_row(line.split('\t', 1)[1])
            elif line.startswith('ROW\t'):
                cells = _report_txt_split_row(line.split('\t', 1)[1])
                sec.setdefault('rows', []).append({
                    'label': cells[0] if cells else '',
                    'value': cells[1] if len(cells) > 1 else '',
                })
            elif line.startswith('GROUP\t'):
                sec.setdefault('rows', []).append({
                    'is_group': True,
                    'label': _report_txt_unescape_cell(line.split('\t', 1)[1]),
                })
            elif line.startswith('NORM\t'):
                sec['norm_label'] = _report_txt_unescape_cell(line.split('\t', 1)[1])
            elif line.startswith('WKTFILE\t'):
                sec['wkt_file'] = _report_txt_unescape_cell(line.split('\t', 1)[1])
            elif line.startswith('PINTRO\t'):
                sec.setdefault('intro_lines', []).append(
                    _report_txt_unescape_cell(line.split('\t', 1)[1]))
            elif line.startswith('PSTAT\t'):
                sec.setdefault('stat_lines', []).append(
                    _report_txt_unescape_cell(line.split('\t', 1)[1]))
            elif line.startswith('PAIR\t'):
                pass  # legado: WKT agora num ficheiro separado
            elif line.startswith('REF\t') or line.startswith('TEST\t') or line.startswith('SCALAR\t'):
                pass
            elif line.startswith('EMPTY\t'):
                sec['empty_message'] = _report_txt_unescape_cell(line.split('\t', 1)[1])
        elif current == 'pec':
            if line.startswith('INTRO\t'):
                sec['intro'] = _report_txt_unescape_cell(line.split('\t', 1)[1])
            elif line.startswith('LABEL\t'):
                parts = line.split('\t')
                if len(parts) >= 3:
                    sec.setdefault('header_labels', {})[parts[1]] = _report_txt_unescape_cell(
                        '\t'.join(parts[2:]))
            elif line.startswith('SUBSECTION\t'):
                title = _report_txt_unescape_cell(line.split('\t', 1)[1])
                pec_sub = 'alt' if 'altim' in title.lower() else 'plan'
                sec.setdefault(pec_sub, {'title': title, 'header_rows': [], 'data_rows': []})
                sec[pec_sub]['title'] = title
            elif line.startswith('PEC_HEADER\t'):
                parts = line.split('\t')
                if len(parts) >= 4:
                    block_key = parts[1]
                    sec.setdefault(block_key, {'title': '', 'header_rows': [], 'data_rows': []})
                    sec[block_key]['header_rows'].append(_report_txt_split_row('\t'.join(parts[3:])))
            elif line.startswith('PEC_ROW\t'):
                parts = line.split('\t')
                if len(parts) >= 3:
                    block_key = parts[1]
                    sec.setdefault(block_key, {'title': '', 'header_rows': [], 'data_rows': []})
                    sec[block_key]['data_rows'].append(
                        _report_txt_split_row('\t'.join(parts[2:])))
            elif line.startswith('PEC_FLAGS\t'):
                parts = line.split('\t')
                if len(parts) >= 3:
                    block_key = parts[1]
                    flag_cells = _report_txt_split_row('\t'.join(parts[2:]))
                    block = sec.setdefault(
                        block_key, {'title': '', 'header_rows': [], 'data_rows': []})
                    rows = block.get('data_rows') or []
                    if not rows:
                        continue
                    flags = []
                    for cell in flag_cells[:REPORT_PEC_N_RESULT_COLS]:
                        if cell == '1':
                            flags.append(True)
                        elif cell == '0':
                            flags.append(False)
                        else:
                            flags.append(None)
                    while len(flags) < REPORT_PEC_N_RESULT_COLS:
                        flags.append(None)
                    last = rows[-1]
                    if isinstance(last, dict):
                        last['result_ok'] = flags
                    else:
                        rows[-1] = {'cells': list(last), 'result_ok': flags}
            elif line.startswith('EMPTY\t'):
                sec['empty_message'] = _report_txt_unescape_cell(line.split('\t', 1)[1])

    return snapshot


def _render_pec_tables_html_from_snapshot(pec_sec: dict, *, col_widths: dict = None) -> str:
    """Tabelas PEC a partir do snapshot TXT (cabeçalho mesclado igual ao plugin)."""
    if not pec_sec:
        return ''
    plan = pec_sec.get('plan') or {}
    alt = pec_sec.get('alt') or {}
    return _build_pec_results_tables_html_blocks(
        intro=pec_sec.get('intro', ''),
        plan_title=plan.get('title', '7.1 PEC Planimétrico'),
        alt_title=alt.get('title', '7.2 PEC Altimétrico'),
        plan_data_rows=plan.get('data_rows'),
        alt_data_rows=alt.get('data_rows'),
        empty_message=pec_sec.get('empty_message', ''),
        header_labels=pec_sec.get('header_labels'),
        col_widths=col_widths,
        plan_page_break=True,
        alt_page_break=True,
    )


def _merge_report_pdf_fonts(fonts: dict = None) -> dict:
    out = dict(REPORT_PDF_FONTS_DEFAULT)
    if fonts:
        out.update(fonts)
    return out


def _report_pdf_css(fonts: dict = None) -> str:
    f = _merge_report_pdf_fonts(fonts)
    return (
        '@page { margin: 0; } '
        f'body {{ font-family: Segoe UI, Arial, sans-serif; font-size: {f["body"]}pt; '
        f'margin: 0; padding: 0; }} '
        f'h1 {{ font-size: {f["h1"]}pt; margin: 0 0 4px 0; }} '
        f'h2 {{ font-size: {f["h2"]}pt; margin-top: 14px; border-bottom: 1px solid #444; }} '
        f'h3.pec-section {{ font-size: {f["h3"]}pt; margin-top: 10px; }} '
        'h3.page-break { page-break-before: always; } '
        'table.report-header { border: none; margin: 0 0 8px 0; width: 100%; } '
        'table.report-header td { border: none; padding: 0; vertical-align: middle; } '
        'td.report-header-icon { width: 76px; min-width: 76px; padding: 2px 12px 2px 0; '
        'vertical-align: middle; overflow: visible; } '
        'img.report-logo { width: 64px; height: 64px; max-width: 64px; max-height: 64px; '
        'display: block; margin: 0; } '
        'table.location-envelope td, table.location-envelope th { text-align: center; } '
        'table.location-envelope td:first-child, table.location-envelope th:first-child '
        '{ text-align: left; } '
        'table { border-collapse: collapse; width: 100%; margin: 6px 0; } '
        f'table.pec-results {{ table-layout: auto; width: 100%; font-size: {f["pec"]}pt; }} '
        'table.pec-results th, table.pec-results td { text-align: center; } '
        'td, th { border: 1px solid #ccc; padding: 4px 6px; vertical-align: middle; } '
        'th { background: #f0f0f0; } '
        'table.pec-results thead th { background: #e8e8e8; font-weight: 600; } '
        'span.pec-pass { color: #2e7d32; font-weight: 600; } '
        'span.pec-fail { color: #c62828; font-weight: 600; } '
        'pre.pair-wkt { font-size: 6pt; white-space: pre-wrap; word-break: break-all; '
        'margin: 4px 0 8px 0; padding: 4px; background: #fafafa; border: 1px solid #ddd; }'
    )


def render_pdf_report_html(
    snapshot: dict, *, icon_uri: str = None, fonts: dict = None, col_widths: dict = None,
) -> str:
    """Gera HTML do relatório a partir do snapshot (plugin ou TXT parseado)."""
    meta = snapshot.get('meta') or {}
    sections = snapshot.get('sections') or {}
    labels = meta.get('labels') or {}
    title = html.escape(meta.get('title', ''))
    when = meta.get('datetime', '')
    proj_esc = meta.get('project_file', '')
    crs_ = meta.get('crs', '')
    lbl_dt = labels.get('datetime', 'Data/hora')
    lbl_proj = labels.get('project_file', 'Ficheiro de projeto')
    lbl_crs = labels.get('crs', 'CRS de referência (análise)')

    meta_hdr_option = labels.get('option', 'Opção')
    meta_hdr_value = labels.get('value', 'Valor')
    meta_table = _render_report_kv_table_html(
        [meta_hdr_option, meta_hdr_value],
        [
            {'label': lbl_dt, 'value': when},
            {'label': lbl_proj, 'value': proj_esc},
            {'label': lbl_crs, 'value': crs_},
        ],
    )

    loc_sec = sections.get('location') or {}
    loc_table = _render_location_section_html(loc_sec)

    wf_sec = sections.get('workflow') or {}
    wf_rows = []
    for row in wf_sec.get('rows') or []:
        wf_rows.append(
            '<tr><td>{}</td><td>{}</td></tr>'.format(
                html.escape(row.get('option', '')),
                html.escape(row.get('value', '')),
            ))
    wf_hdr = wf_sec.get('header') or ['Opção', 'Valor']
    wf_table = (
        f'<table><tr><th>{html.escape(wf_hdr[0])}</th>'
        f'<th>{html.escape(wf_hdr[1] if len(wf_hdr) > 1 else "Valor")}</th></tr>'
        f'{"".join(wf_rows)}</table>'
    )

    dem_sec = sections.get('dems') or {}
    dem_rows = []
    for row in dem_sec.get('rows') or []:
        name = row.get('name', '')
        source = row.get('source', '')
        if name or source:
            dem_rows.append(
                '<tr><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
                    html.escape(row.get('role', '')),
                    html.escape(name),
                    html.escape(source[:500]),
                ))
        else:
            dem_rows.append(
                '<tr><td>{}</td><td colspan="2">{}</td></tr>'.format(
                    html.escape(row.get('role', '')),
                    html.escape(name or source or ''),
                ))
    dem_hdr = dem_sec.get('header') or ['Papel', 'Nome', 'Fonte (início)']
    dem_table = (
        f'<table><tr><th>{html.escape(dem_hdr[0])}</th>'
        f'<th>{html.escape(dem_hdr[1] if len(dem_hdr) > 1 else "Nome")}</th>'
        f'<th>{html.escape(dem_hdr[2] if len(dem_hdr) > 2 else "Fonte")}</th></tr>'
        f'{"".join(dem_rows)}</table>'
    )

    par_sec = sections.get('params') or {}
    param_rows = []
    for grp in par_sec.get('groups') or []:
        param_rows.append(
            f'<tr><td colspan="2" style="background:#eee"><b>{html.escape(grp.get("label", ""))}</b></td></tr>')
        for fld in grp.get('fields') or []:
            param_rows.append(
                f'<tr><td>{html.escape(fld.get("label", ""))}</td>'
                f'<td>{html.escape(fld.get("value", ""))}</td></tr>')
    par_hdr = par_sec.get('header') or ['Parâmetro', 'Valor']
    param_table = (
        f'<table><tr><th>{html.escape(par_hdr[0])}</th>'
        f'<th>{html.escape(par_hdr[1] if len(par_hdr) > 1 else "Valor")}</th></tr>'
        f'{"".join(param_rows)}</table>'
    )

    stats_sec = sections.get('stats') or {}
    stats_table = _render_report_kv_table_html(
        stats_sec.get('header') or ['Opção', 'Valor'],
        _report_sec_kv_rows(stats_sec),
    )

    pairs_body = _render_pairs_section_html(sections.get('pairs') or {})

    pec_body = _render_pec_tables_html_from_snapshot(
        sections.get('pec') or {}, col_widths=col_widths)

    if icon_uri is None:
        icon_uri = _plugin_icon_png_data_uri(64)
    icon_cell = ''
    if icon_uri:
        icon_cell = (
            f'<td class="report-header-icon">'
            f'<img class="report-logo" src="{icon_uri}" width="64" height="64" alt=""/>'
            f'</td>'
        )

    css = _report_pdf_css(fonts)

    def h2(sec_key, default_title):
        sec = sections.get(sec_key) or {}
        return html.escape(sec.get('title') or default_title)

    return f'''<!DOCTYPE html><html><head><meta charset="utf-8"/><style>{css}</style></head><body>
<table class="report-header"><tr>
{icon_cell}
<td><h1>{title}</h1></td>
</tr></table>
{meta_table}

<h2>{h2("location", "1. Localização da área de estudo")}</h2>
{loc_table}

<h2>{h2("workflow", "2. Fluxo de trabalho")}</h2>
{wf_table}

<h2>{h2("dems", "3. Modelos digitais de elevação (MDE)")}</h2>
{dem_table}

<h2>{h2("params", "4. Parâmetros de processamento")}</h2>
{param_table}

<h2>{h2("stats", "5. Estatísticas do painel")}</h2>
{stats_table}

<h2>{h2("pairs", "6. Pares homólogos — estatísticas")}</h2>
{pairs_body}

<h2>{h2("pec", "7. Resultados PEC")}</h2>
{pec_body}
</body></html>'''


def export_pdf_from_txt_report(
    txt_path: str,
    out_pdf: str = None,
    *,
    margin_mm: float = None,
    recompute_pec: bool = False,
    fonts: dict = None,
    col_widths: dict = None,
) -> str:
    """
    Gera PDF a partir de um relatório .txt exportado pelo plugin.

    Por defeito (`recompute_pec=False`) interpreta o TXT v1 e gera o PDF
    (ideal para iterar formatação sem correr o pipeline).

    Com `recompute_pec=True`, relê o .pa.gpkg, recalcula PEC e exporta de novo.
    """
    txt_path = os.path.normpath(os.path.abspath(txt_path))
    if not os.path.isfile(txt_path):
        raise FileNotFoundError(txt_path)

    with open(txt_path, encoding='utf-8') as f:
        txt_body = f.read()

    if out_pdf:
        pdf_path = os.path.normpath(os.path.abspath(out_pdf))
    else:
        pdf_path = _companion_report_path(txt_path, '.pdf')

    if not recompute_pec:
        if _report_txt_contains_v1_marker(txt_body):
            snapshot = parse_full_report_txt(txt_body)
            html_doc = render_pdf_report_html(snapshot, fonts=fonts, col_widths=col_widths)
        else:
            html_path = _companion_report_path(txt_path, '.html')
            if os.path.isfile(html_path):
                return export_pdf_from_html_file(
                    html_path, out_pdf, margin_mm=margin_mm)
            raise ValueError(
                f'Relatório TXT sem marcador {REPORT_TX_V1_MARKER!r} e sem HTML homónimo.')
        merged = _merge_report_pdf_fonts(fonts)
        write_pdf_from_html_doc(
            html_doc, pdf_path, margin_mm=margin_mm, font_size=merged['doc_default'])
        html_path = _companion_report_path(txt_path, '.html')
        try:
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_doc)
        except OSError:
            pass
        return pdf_path

    pa_gpkg = _parse_project_path_from_report_txt(txt_body)
    if not pa_gpkg or not os.path.isfile(pa_gpkg):
        raise FileNotFoundError(
            f'Projeto .pa.gpkg não encontrado (linha «Ficheiro de projeto:» no TXT): {pa_gpkg!r}')

    iface = _ReportExportIfaceStub()
    main = _ReportExportMainStub()
    wd = Wd1(iface, parent=None, main=main)
    wd.set_project_paths(pa_gpkg)
    wd.reload_settings_from_project_file()
    wd.ensure_crs_from_reference_dem()

    dv = wd.recompute_dic_values_from_project()
    wd._apply_outlier_workflow(dv)
    wd.calc_pec(dv)

    html_doc = wd._build_pdf_report_html()
    write_pdf_from_html_doc(html_doc, pdf_path, margin_mm=margin_mm)

    html_path = _companion_report_path(txt_path, '.html')
    try:
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_doc)
    except OSError:
        pass

    return pdf_path


class _ReportExportMainStub:
    name_ = PLUGIN_DISPLAY_NAME


class _ReportExportIfaceStub:
    def actionShowPythonDialog(self):
        class _Act:
            @staticmethod
            def trigger():
                pass

        return _Act()


def export_pdf_report_from_pa_gpkg(
    pa_gpkg_path: str,
    out_path: str = None,
    *,
    recompute_pec: bool = True,
    margin_mm: float = None,
) -> tuple:
    """
    Exporta relatório PDF + TXT a partir de um projeto .pa.gpkg (uso em script / testes de layout).

    Recalcula PEC a partir das camadas de correspondência e morfologia no GPKG.
    Devolve (pdf_path, txt_path).
    """
    pa_gpkg_path = os.path.normpath(os.path.abspath(pa_gpkg_path))
    if not os.path.isfile(pa_gpkg_path):
        raise FileNotFoundError(pa_gpkg_path)

    iface = _ReportExportIfaceStub()
    main = _ReportExportMainStub()
    wd = Wd1(iface, parent=None, main=main)
    wd.set_project_paths(pa_gpkg_path)
    wd.reload_settings_from_project_file()
    wd.ensure_crs_from_reference_dem()

    if recompute_pec:
        dv = wd.recompute_dic_values_from_project()
        wd._apply_outlier_workflow(dv)
        wd.calc_pec(dv)

    paths = wd.export_project_reports_to(
        out_pdf=out_path,
        margin_mm=margin_mm if margin_mm is not None else REPORT_PDF_MARGIN_MM,
    )
    if not paths or not paths[0]:
        raise RuntimeError('Falha ao gerar relatório (ver mensagens no log do painel).')
    return paths


def _plugin_icon_png_data_uri(size_px: int = 64) -> str:
    """Ícone do plugin em data URI PNG para embutir no HTML do relatório PDF."""
    path_ = _resolve_plugin_icon_path(prefer_svg=True)
    if not path_:
        return ''
    # force_dpr=1.0: QTextDocument corta PNGs HiDPI quando width/height HTML ≠ pixels reais
    pm = _pixmap_from_icon_path(
        path_, QSize(size_px, size_px), margin_ratio=0.15, force_dpr=1.0)
    if pm.isNull():
        return ''
    ba = QByteArray()
    buf = QBuffer(ba)
    if not buf.open(QIODevice.WriteOnly):
        return ''
    if not pm.save(buf, 'PNG'):
        return ''
    return 'data:image/png;base64,' + base64.b64encode(bytes(ba)).decode('ascii')


def _strip_project_ext(path: str) -> str:
    """Caminho sem o sufixo PROJECT_EXT (nome base lógico e ficheiro .gpkg paralelo do OGR)."""
    if not path:
        return path
    pl = path.lower()
    low = PROJECT_EXT.lower()
    if pl.endswith(low):
        return path[: -len(PROJECT_EXT)]
    return os.path.splitext(path)[0]


def resolve_saved_project_file_on_disk(saved_path: str) -> str:
    """Resolve caminho do ficheiro de projeto (.pa.gpkg) no disco."""
    if not saved_path:
        return ''
    p = os.path.normpath(os.path.abspath(saved_path))
    if os.path.isfile(p):
        return p
    return p


def project_file_filter_i18n() -> str:
    return (
        f'{tr_ui("Projeto MDE-AP (*.pa.gpkg)")};;{tr_ui("Todos (*.*)")}'
    )

# Tabela sem geometria no ficheiro de projeto: inicio/fim = data e hora local (SQLite 'YYYY-MM-DD HH:MM:SS') ou NULL
PIPELINE_ETAPAS_TABLE = 'pa_pipeline_etapas'
PIPELINE_DATETIME_FMT = '%Y-%m-%d %H:%M:%S'
PIPELINE_ETAPAS_DEF = (
    (1, 'poligonos_limites'),
    (2, 'dem_intersecao'),
    (3, 'morfologia_referencia'),
    (4, 'morfologia_teste'),
    (5, 'correspondencia_linhas'),
    (6, 'buffers'),
)

# Snapshot da última avaliação concluída (PEC): comparação em nova «Avaliar» para retomar só o necessário
PIPELINE_SNAPSHOT_ETAPA = '__pipeline_last_ok__'
PIPELINE_SNAPSHOT_CAMPO = 'config_json'

# Bloco de Config → primeira etapa a repetir (o resto da cadeia segue como hoje)
STEP_KEY_TO_RESTART_ETAPA = {
    'step_morfologia': 'morfologia_referencia',
    'step_match': 'correspondencia_linhas',
    'step_buffers': 'buffers',
    'step_normalize_prog': 'buffers',
}


def _pipeline_etapa_order_index():
    return {name: i for i, (_, name) in enumerate(PIPELINE_ETAPAS_DEF)}


def pipeline_has_completed_any_etapa(mdepa_path: str) -> bool:
    """True se alguma linha de pa_pipeline_etapas tem `fim` preenchido (a cadeia já avançou alguma vez)."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'''SELECT 1 FROM {PIPELINE_ETAPAS_TABLE}
                    WHERE fim IS NOT NULL AND TRIM(fim) != ''
                    LIMIT 1'''
            )
            return cur.fetchone() is not None
        finally:
            conn.close()
    except sqlite3.Error:
        return False


def load_pipeline_last_ok_snapshot(mdepa_path: str) -> dict:
    """Último estado de parâmetros + DEMs após uma avaliação concluída com sucesso."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return {}
    if not ensure_pa_settings_table(mdepa_path):
        return {}
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ? AND campo = ?',
                (PIPELINE_SNAPSHOT_ETAPA, PIPELINE_SNAPSHOT_CAMPO),
            )
            row = cur.fetchone()
            if not row or row[0] is None or str(row[0]).strip() == '':
                return {}
            return json.loads(row[0])
        finally:
            conn.close()
    except (sqlite3.Error, json.JSONDecodeError, TypeError, ValueError):
        return {}


def save_pipeline_last_ok_snapshot(mdepa_path: str, flat: dict) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    try:
        payload = json.dumps(flat, sort_keys=True, ensure_ascii=False)
        conn = sqlite3.connect(mdepa_path)
        try:
            conn.execute(
                f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                    VALUES (?, ?, ?)
                    ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                (PIPELINE_SNAPSHOT_ETAPA, PIPELINE_SNAPSHOT_CAMPO, payload),
            )
            conn.commit()
        finally:
            conn.close()
    except (sqlite3.Error, TypeError):
        return False
    return True


def _coerce_snapshot_flat_for_dem_keys(flat: dict) -> dict:
    """Snapshots antigos usavam raster_0/raster_1; alinhar para dem_*."""
    if not flat:
        return flat
    d = dict(flat)
    for i in (0, 1):
        rk, dk = f'raster_{i}', f'dem_{i}'
        if dk not in d and rk in d:
            d[dk] = d[rk]
    return d


def _normalize_dem_sources_in_flat(flat: dict) -> dict:
    """Cópia com dem_0/dem_1 normalizados para comparação (caminhos Windows / URI)."""
    if not flat:
        return flat
    d = dict(flat)
    for i in (0, 1):
        k = f'dem_{i}'
        d[k] = _normalize_layer_source_compare(str(d.get(k) or ''))
    return d


def build_flat_snapshot_from_mdepa_stored_settings(mdepa_path: str) -> dict:
    """Estado de config gravado no .pa.gpkg (step_*, DEMs, workflow), sem depender do PEC concluído.

    Usar **antes** de `persist_project_config_from_widgets` para comparar com os valores atuais
    dos widgets e obter a etapa de retomada correta quando ainda não existe `__pipeline_last_ok__`.
    """
    out: dict = {}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f"SELECT etapa, campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa LIKE 'step_%%'"
            )
            for etapa, campo, valor in cur.fetchall():
                if not etapa or valor is None:
                    continue
                out[f'{etapa}.{campo}'] = str(valor)
        finally:
            conn.close()
    except sqlite3.Error:
        return {}
    dems = load_dem_sources_from_project_path(mdepa_path)
    out['dem_0'] = dems.get(0, '') or ''
    out['dem_1'] = dems.get(1, '') or ''
    wf = load_workflow_ui_from_mdepa_path(mdepa_path)
    out['workflow.study_mode'] = str(int(wf.get('study_mode', 0)))
    out['workflow.pairs_mode'] = str(int(wf.get('pairs_mode', 0)))
    out['workflow.outliers_mode'] = str(int(wf.get('outliers_mode', 0)))
    out['workflow.study_layer_source'] = (wf.get('study_layer_source') or '').strip()
    return out


def compute_restart_etapa_from_snapshots(flat_now: dict, flat_was: dict):
    """Devolve (restart, extra). restart: None=completo desde polígonos; str=etapa; '__noop__'=sem alterações."""
    if not flat_was:
        return None, None
    flat_was = _coerce_snapshot_flat_for_dem_keys(flat_was)
    flat_now_d = _normalize_dem_sources_in_flat(flat_now)
    flat_was_d = _normalize_dem_sources_in_flat(flat_was)
    for i in (0, 1):
        if flat_now_d.get(f'dem_{i}') != flat_was_d.get(f'dem_{i}'):
            return None, 'dem'
    w_src_now = _normalize_layer_source_compare(str(flat_now.get('workflow.study_layer_source') or ''))
    w_src_was = _normalize_layer_source_compare(str(flat_was.get('workflow.study_layer_source') or ''))
    for wk in (
        'workflow.study_mode',
        'workflow.pairs_mode',
        'workflow.outliers_mode',
    ):
        if flat_now.get(wk) != flat_was.get(wk):
            return None, 'workflow'
    if w_src_now != w_src_was:
        return None, 'workflow'
    changed_steps = set()
    for k in set(flat_now) | set(flat_was):
        if k.startswith('dem_'):
            continue
        if flat_now.get(k) != flat_was.get(k):
            head = k.split('.', 1)[0]
            if head.startswith('step_'):
                changed_steps.add(head)
    if not changed_steps:
        return '__noop__', None
    ord_idx = _pipeline_etapa_order_index()
    candidates = []
    for sk in changed_steps:
        et = STEP_KEY_TO_RESTART_ETAPA.get(sk)
        if et:
            candidates.append(et)
    if not candidates:
        return None, 'unknown_step'
    return min(candidates, key=lambda e: ord_idx[e]), None

# Parâmetros do diálogo Config (por projeto): etapa = chave step_* em dic_param, campo = chave do field
PA_SETTINGS_TABLE = 'pa_settings'
# Fontes DEM (QgsRasterLayer.source()): etapa na tabela de settings, campo '0' | '1'
SETTINGS_ETAPA_DEM_SOURCES = 'dem_sources'
SETTINGS_ETAPA_DEM_SOURCES_LEGACY = 'raster_sources'

# Grupo na árvore de camadas: prefixo fixo + nome do projeto (stem do .pa.gpkg).
# Não usar self.tr() no prefixo — ao mudar o idioma o QGIS procuraria outro nome.
PLUGIN_LAYER_TREE_ROOT_GROUP = '__MDE_AP__'
# Nome usado em versões anteriores para locale não pt (evitar grupo órfão após correção).
PLUGIN_LAYER_TREE_ROOT_GROUP_LEGACY = ('__DEM_PA__',)


def plugin_layer_tree_group_name(project_file: str = '') -> str:
    """Nome do grupo na árvore: __MDE_AP__ + stem do ficheiro de projeto (ex.: __MDE_AP__v6z)."""
    if not project_file:
        return PLUGIN_LAYER_TREE_ROOT_GROUP
    stem = os.path.basename(_strip_project_ext(os.path.normpath(project_file)))
    if not stem:
        return PLUGIN_LAYER_TREE_ROOT_GROUP
    return f'{PLUGIN_LAYER_TREE_ROOT_GROUP}{stem}'


def _norm_gpkg_path(path: str) -> str:
    if not path:
        return ''
    return os.path.normcase(os.path.normpath(os.path.abspath(path)))


def map_layer_gpkg_path(layer) -> str:
    """Caminho do .pa.gpkg a partir do source() OGR da camada no mapa."""
    if layer is None:
        return ''
    try:
        src = layer.source() or ''
    except Exception:
        return ''
    if not src:
        return ''
    return _norm_gpkg_path(src.split('|')[0].strip())


def project_layer_display_name(logical_name: str, project_file: str = '') -> str:
    """Nome único da camada no mapa QGIS (evita colisão entre vários .pa.gpkg abertos)."""
    if not logical_name:
        return logical_name
    if not project_file:
        return logical_name
    stem = os.path.basename(_strip_project_ext(os.path.normpath(project_file)))
    if not stem:
        return logical_name
    return f'{logical_name}@{stem}'


def gpkg_layer_uri(gpkg_path: str, layername: str) -> str:
    return f'{os.path.normpath(gpkg_path)}|layername={layername}'


def _ensure_pa_settings_table_conn(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''CREATE TABLE IF NOT EXISTS {PA_SETTINGS_TABLE} (
            etapa TEXT NOT NULL,
            campo TEXT NOT NULL,
            valor TEXT NOT NULL,
            PRIMARY KEY (etapa, campo)
        )'''
    )


def ensure_pa_settings_table(mdepa_path: str) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            _ensure_pa_settings_table_conn(conn)
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def load_plugin_settings_from_mdepa_path(mdepa_path: str, dic_param: dict) -> int:
    """Lê linhas (etapa, campo, valor) e atualiza dic_param[*]['fields'][*]['value']. Retorna quantos campos atualizou."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return 0
    if not ensure_pa_settings_table(mdepa_path):
        return 0
    n = 0
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT etapa, campo, valor FROM {PA_SETTINGS_TABLE}')
            for etapa, campo, valor in cur.fetchall():
                if etapa not in dic_param:
                    continue
                block = dic_param[etapa]
                if not isinstance(block, dict) or 'fields' not in block:
                    continue
                if campo not in block['fields']:
                    continue
                meta = block['fields'][campo]
                if valor is None:
                    continue
                if meta.get('type') == 'checkbox':
                    try:
                        meta['value'] = 1 if int(valor) else 0
                    except (TypeError, ValueError):
                        meta['value'] = 1 if str(valor).strip().lower() in (
                            '1', 'true', 'sim', 'yes',
                        ) else 0
                elif meta.get('type') == 'doublespin':
                    try:
                        meta['value'] = float(valor)
                    except (TypeError, ValueError):
                        continue
                elif meta.get('type') == 'radio' or 'list' in meta:
                    try:
                        meta['value'] = int(valor)
                    except (TypeError, ValueError):
                        try:
                            meta['value'] = int(float(valor))
                        except (TypeError, ValueError):
                            continue
                else:
                    meta['value'] = str(valor)
                n += 1
        finally:
            conn.close()
    except sqlite3.Error:
        return n
    return n


def save_plugin_settings_to_mdepa_path(mdepa_path: str, dic_param: dict) -> bool:
    """Grava todos os fields dos step_* na tabela (upsert)."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for step, block in dic_param.items():
                if not isinstance(step, str) or not step.startswith('step_'):
                    continue
                if not isinstance(block, dict) or 'fields' not in block:
                    continue
                for campo, meta in block['fields'].items():
                    val = meta.get('value')
                    conn.execute(
                        f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                            VALUES (?, ?, ?)
                            ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                        (step, campo, str(val)),
                    )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def _normalize_layer_source_compare(source: str) -> str:
    """Comparação aproximada de caminhos de ficheiro (ex.: Windows)."""
    if not source:
        return ''
    s = source.strip()
    pipe = s.find('|')
    path_part = s[:pipe] if pipe >= 0 else s
    if path_part and os.path.isfile(path_part):
        return os.path.normcase(os.path.normpath(os.path.abspath(path_part)))
    return s


def find_dem_layer_in_project(source_str: str):
    """Devolve QgsRasterLayer (DEM) já no projeto com a mesma fonte que source_str, ou None."""
    if not source_str:
        return None
    proj = QgsProject.instance()
    norm = _normalize_layer_source_compare(source_str)
    for layer in proj.mapLayers().values():
        if not isinstance(layer, QgsRasterLayer):
            continue
        src = layer.source()
        if src == source_str:
            return layer
        if norm and _normalize_layer_source_compare(src) == norm:
            return layer
    return None


def _raster_layer_gsd(layer):
    """GSD efetivo (m) a partir do tamanho de pixel; menor = melhor resolução."""
    if layer is None or not isinstance(layer, QgsRasterLayer) or not layer.isValid():
        return None
    try:
        gx = abs(float(layer.rasterUnitsPerPixelX()))
        gy = abs(float(layer.rasterUnitsPerPixelY()))
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(gx) or gx <= 0:
        return None
    if not math.isfinite(gy) or gy <= 0:
        gy = gx
    return math.sqrt(gx * gy)


def find_vector_layer_in_project(source_str: str):
    """Devolve QgsVectorLayer já no projeto com a mesma fonte que source_str, ou None."""
    if not source_str:
        return None
    proj = QgsProject.instance()
    norm = _normalize_layer_source_compare(source_str)
    for layer in proj.mapLayers().values():
        if not isinstance(layer, QgsVectorLayer):
            continue
        src = layer.source()
        if src == source_str:
            return layer
        if norm and _normalize_layer_source_compare(src) == norm:
            return layer
    return None


def load_dem_sources_from_project_path(mdepa_path: str) -> dict:
    """Lê fontes guardadas para os slots 0 e 1 (referência / teste)."""
    out = {}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for etapa_key in (SETTINGS_ETAPA_DEM_SOURCES, SETTINGS_ETAPA_DEM_SOURCES_LEGACY):
                cur = conn.execute(
                    f'SELECT campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ?',
                    (etapa_key,),
                )
                for campo, valor in cur.fetchall():
                    try:
                        k = int(campo)
                    except (TypeError, ValueError):
                        continue
                    if k in (0, 1) and k not in out:
                        out[k] = '' if valor is None else str(valor)
        finally:
            conn.close()
    except sqlite3.Error:
        return out
    return out


def save_dem_sources_to_project_path(mdepa_path: str, key_to_source: dict) -> bool:
    """Persiste QgsRasterLayer.source() dos DEMs (slots 0 e 1; string vazia mantém a linha)."""
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for k in (0, 1):
                val = (key_to_source.get(k) or '').strip()
                conn.execute(
                    f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                        VALUES (?, ?, ?)
                        ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                    (SETTINGS_ETAPA_DEM_SOURCES, str(k), val),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


# Modo de fluxo (UI): etapa fixa, campos study_mode | pairs_mode | outliers_mode | study_layer_source
WORKFLOW_UI_ETAPA = 'workflow_ui'


def save_workflow_ui_to_mdepa_path(
    mdepa_path: str,
    study_mode: int,
    pairs_mode: int,
    outliers_mode: int,
    study_layer_source: str,
) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    rows = (
        ('study_mode', str(int(study_mode))),
        ('pairs_mode', str(int(pairs_mode))),
        ('outliers_mode', str(int(outliers_mode))),
        ('study_layer_source', (study_layer_source or '').strip()),
    )
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for campo, val in rows:
                conn.execute(
                    f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                        VALUES (?, ?, ?)
                        ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                    (WORKFLOW_UI_ETAPA, campo, val),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def load_workflow_ui_from_mdepa_path(mdepa_path: str) -> dict:
    out = {'study_mode': 0, 'pairs_mode': 0, 'outliers_mode': 0, 'study_layer_source': ''}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ?',
                (WORKFLOW_UI_ETAPA,),
            )
            for campo, valor in cur.fetchall():
                if campo == 'study_layer_source':
                    out['study_layer_source'] = '' if valor is None else str(valor)
                elif campo in ('study_mode', 'pairs_mode', 'outliers_mode'):
                    try:
                        out[campo] = int(valor)
                    except (TypeError, ValueError):
                        pass
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


# Estatísticas do painel (área, extensões, nº pares): etapa fixa em pa_settings
PANEL_STATS_ETAPA = 'panel_stats'


def save_panel_stats_to_mdepa_path(
    mdepa_path: str,
    area: str,
    ext_min: str,
    ext_match: str,
    pair_nr: str,
) -> bool:
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return False
    if not ensure_pa_settings_table(mdepa_path):
        return False
    rows = (
        ('area', area or ''),
        ('ext_min', ext_min or ''),
        ('ext_match', ext_match or ''),
        ('pair_nr', pair_nr or ''),
    )
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            for campo, val in rows:
                conn.execute(
                    f'''INSERT INTO {PA_SETTINGS_TABLE} (etapa, campo, valor)
                        VALUES (?, ?, ?)
                        ON CONFLICT(etapa, campo) DO UPDATE SET valor = excluded.valor''',
                    (PANEL_STATS_ETAPA, campo, val),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return True


def load_panel_stats_from_mdepa_path(mdepa_path: str) -> dict:
    out = {'area': '', 'ext_min': '', 'ext_match': '', 'pair_nr': ''}
    if not mdepa_path or not os.path.isfile(mdepa_path):
        return out
    if not ensure_pa_settings_table(mdepa_path):
        return out
    try:
        conn = sqlite3.connect(mdepa_path)
        try:
            cur = conn.execute(
                f'SELECT campo, valor FROM {PA_SETTINGS_TABLE} WHERE etapa = ?',
                (PANEL_STATS_ETAPA,),
            )
            for campo, valor in cur.fetchall():
                if campo in out:
                    out[campo] = '' if valor is None else str(valor)
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


# QSettings do plugin: só pasta para diálogo Abrir/Novo (não reabrir projeto no rótulo)
AUX_LAST_PROJECT_DIR_KEY = 'last_project_dir'


def pipeline_datetime_now_local() -> str:
    """Data e hora atual (local), sem microssegundos — compatível com datetime() do SQLite."""
    return datetime.datetime.now().replace(microsecond=0).strftime(PIPELINE_DATETIME_FMT)


def project_data_dir(project_file: str) -> str:
    """Pasta auxiliar ao lado do .pa.gpkg: mesmo nome base lógico (sem PROJECT_EXT)."""
    project_file = os.path.abspath(project_file)
    parent = os.path.dirname(project_file)
    stem = os.path.basename(_strip_project_ext(project_file))
    return os.path.join(parent, stem)


def normalize_project_pa_file(project_path: str) -> None:
    """Se o OGR criar `stem.gpkg` paralelo ao ficheiro pedido, consolidar no .pa.gpkg."""
    if not project_path or not project_path.lower().endswith(PROJECT_EXT.lower()):
        return
    root = _strip_project_ext(project_path)
    alt_gpkg = root + '.gpkg'
    has_project = os.path.isfile(project_path)
    has_gpkg = os.path.isfile(alt_gpkg)
    if not has_gpkg:
        return
    try:
        if not has_project:
            os.replace(alt_gpkg, project_path)
        else:
            if os.path.getmtime(alt_gpkg) > os.path.getmtime(project_path):
                os.remove(project_path)
                os.replace(alt_gpkg, project_path)
            else:
                os.remove(alt_gpkg)
    except OSError:
        pass


class PositionalAccuracyPlugin:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        # Save reference to the QGIS interface
        self.iface = iface
        self.translator = install_plugin_translator(saved_ui_locale() or LOCALE_AUTO)
        self.name_ = tr_ui(PLUGIN_DISPLAY_NAME)

        # Declare instance attributes

        self.actions = []
        self.menu = self.tr('&T MDE AP - Acurácia Posicional')
        self.dic_prj_conn = {}
        self.dic_icon = {}

        # Check if plugin was started the first time in current QGIS session
        # Must be set in initGui() to survive plugin reloads
        self.first_start = None

    # noinspection PyMethodMayBeStatic
    def tr(self, message):
        """Get the translation for a string using Qt translation API.

        We implement this ourselves since we do not inherit QObject.

        :param message: String for translation.
        :type message: str, QString

        :returns: Translated version of message.
        :rtype: QString
        """
        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate(PLUGIN_I18N_CONTEXT, message)

    def add_action(self, icon_path, text, callback, enabled_flag=True, add_to_menu=True, add_to_toolbar=True,
                   status_tip=None, whats_this=None, parent=None):
        """Add a toolbar icon to the toolbar.

        :param icon_path: Path to the icon for this action. Can be a resource
            path (e.g. ':/plugins/foo/bar.png') or a normal file system path.
        :type icon_path: str

        :param text: Text that should be shown in menu items for this action.
        :type text: str

        :param callback: Function to be called when the action is triggered.
        :type callback: function

        :param enabled_flag: A flag indicating if the action should be enabled
            by default. Defaults to True.
        :type enabled_flag: bool

        :param add_to_menu: Flag indicating whether the action should also
            be added to the menu. Defaults to True.
        :type add_to_menu: bool

        :param add_to_toolbar: Flag indicating whether the action should also
            be added to the toolbar. Defaults to True.
        :type add_to_toolbar: bool

        :param status_tip: Optional text to show in a popup when mouse pointer
            hovers over the action.
        :type status_tip: str

        :param parent: Parent widget for the new action. Defaults None.
        :type parent: QWidget

        :param whats_this: Optional text to show in the status bar when the
            mouse pointer hovers over the action.

        :returns: The action that was created. Note that the action is also
            added to self.actions list.
        :rtype: QAction
        """

        icon = _icon_from_icons_file()
        if icon.isNull():
            icon = QIcon()
            icon.addPixmap(_pixmap_from_icon_path(icon_path, QSize(24, 24)))
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            # Adds plugin icon to Plugins toolbar
            self.iface.addToolBarIcon(action)

        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)

        self.actions.append(action)

        return action

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""
        # self.dock = QDockWidget('T - Inventário de Via.')

        # QDockWidget: painel customizado; QgsAdvancedDigitizingDockWidget é para CAD e pode esconder nosso conteúdo
        self.dock1 = QDockWidget()
        self.title1 = f'{self.name_}.'
        self.dock1.setWindowTitle(self.title1)

        self.wd1 = Wd1(self.iface, parent=self.dock1, main=self)
        self.wd1.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.dock1.setWidget(self.wd1)
        self.dock1.setObjectName(f"{self.title1} Panel")
        self.dock1.setMinimumHeight(60)

        self.iface.addDockWidget(Qt.LeftDockWidgetArea, self.dock1)
        icon_path = _resolve_plugin_icon_path(prefer_svg=True)

        self.add_action(
            icon_path,
            text=self.tr(''),
            callback=self.call_vs,
            parent=self.iface.mainWindow())

        self.first_start = True

    def unload(self):
        """Remove GUI e libera recursos (compatível com Plugin Reloader)."""
        if getattr(self, 'wd1', None):
            self.wd1.unload_cleanup()

        for action in list(self.actions):
            self.iface.removePluginMenu(self.menu, action)
            self.iface.removeToolBarIcon(action)
        self.actions.clear()

        if getattr(self, 'dock1', None):
            self.iface.removeDockWidget(self.dock1)
            self.dock1.setParent(None)
            self.dock1.deleteLater()
            self.dock1 = None
        self.wd1 = None

        if getattr(self, 'translator', None):
            remove_plugin_translator()
            self.translator = None

    def call_vs(self):
        if not self.dock1.isVisible():
            self.dock1.setVisible(True)
        # self.inv_wd.get_inv()

    @classmethod
    def plugin_version(self):
        meta_file = plugin_path + "/metadata.txt"
        # print(meta_file)
        with open(meta_file) as meta:
            mt = meta.readlines()
            for l_ in mt:
                if l_[:8] == "version=":
                    return l_[8:].replace('\n', '')
        return '0.0'


class Wd1(QWidget):
    def __init__(self, iface, parent=None, main=None):

        super(Wd1, self).__init__(parent)
        # Save reference to the QGIS interface
        self.iface = iface
        self.parent = parent
        self.main = main
        name_ = self.main.name_.replace(' ', '_')
        self.setObjectName(f'Wd_{name_}')

        self.dic_prj = \
            {'path': '',  # pasta de dados (logs, exports): vizinha ao .pa.gpkg, mesmo nome base
             'project_file': '',  # caminho absoluto do arquivo .pa.gpkg
             'dems': {
                 0: {
                     'type': 'Referencia',
                     'obj_cbx': None,
                     'obj_pb': None,
                     'obj_prog_bar': None,
                     'geom_status': False},
                 1: {
                     'type': 'Teste',
                     'obj_cbx': None,
                     'obj_pb': None,
                     'obj_prog_bar': None,
                     'geom_status': False},
             },
             'matchs': {
                 'obj_prog_bar': None,
             },
             'standard': {
                 'name': 'pa_accuracy',
                 'files': {
                     'log': '.log',
                     'result_txt': '_result.txt',
                     'result_prof': '_prof.csv',
                 }}}
        self.dic_match = {}
        self.srid = None
        self.crs_epsg = None
        self.gpkg_path = ''
        self.workers = None
        self.task_queue = None
        self.folder_out_path = ''
        self.list_add_tool = ['...', 'add_folder', 'add_files', 'clear']

        self.max_threads = 3  # Limit to 3 concurrent tasks

        self.task_queue = Queue()  # Task queue
        self.threads_running = 0  # Track active threads
        self.active_workers = {}  # Keep track of active workers
        self._workflow_pause = None  # None | 'post_intersection' | 'post_pairs_review'
        self._panel_stats_cache = {'area': '', 'ext_min': '', 'ext_match': '', 'pair_nr': ''}
        self._last_report_pdf_path = ''
        self._pec_report_pec_intro = ''  # nota de outliers (PEC) para o relatório PDF
        self._pec_report_plan_rows = []  # linhas da tabela PEC planimétrico
        self._pec_report_alt_rows = []  # linhas da tabela PEC altimétrico

        self.dic_lb_texts = {
            'area': tr_ui('Área de estudo: {} km²'),
            'ext_min': tr_ui('Extensão mínima da amostra: {} km'),
            'ext_match': tr_ui('Extensão da Amostra: {} km'),
            'pair_nr': tr_ui('Número de pares homólogos: {}'),
        }
        self.aux_tools = AuxTools(parent=self)
        lg = self.create_layout()
        self.setLayout(lg)
        self.dic_pec_mm = DIC_PEC_MM
        self.dic_pec_v = DIC_EQ_BY_NOMINAL_SCALE
        self.dic_pec_alt = DIC_PEC_ALT
        self.list_norm_type = [
            tr_ui('Linear'), tr_ui('Por Proximidade'), tr_ui('Sem Normalização')]
        self.list_accuracy_standard = [
            tr_ui('Padrão Brasileiro - PEC PCD'),
            tr_ui('CE90 e LE90'),
        ]
        self.list_dm_formula = [
            tr_ui('Equação original (eq:dm-buffer-duplo)'),
            tr_ui('Nova equação (eq:dm-buffer-duplo-media)'),
        ]
        self.list_dm_formula_tooltips = [
            tr_ui(
                'dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ\n\n'
                'dmᵢ — discrepância média do par i\n'
                'π — constante pi\n'
                'x — PEC (raio do buffer) da escala/classe\n'
                'A₁ — área do buffer da feição de teste\n'
                'A₂ — área do buffer da feição de referência\n'
                'A₃ — área da interseção dos buffers'
            ),
            tr_ui(
                'dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)\n\n'
                'A média (A₁ + A₂)/2 entra no numerador (no lugar de A₂) e no '
                'denominador (no lugar de A₁), tratando os dois erros de extensão '
                'com o mesmo peso.\n\n'
                'dmᵢ — discrepância média do par i\n'
                'π — constante pi\n'
                'x — PEC (raio do buffer) da escala/classe\n'
                'A₁ — área do buffer da feição de teste\n'
                'A₂ — área do buffer da feição de referência\n'
                'A₃ — área da interseção dos buffers'
            ),
        ]
        self.settings_dlg = SettingsDlg(main=parent, parent=self)
        self.language_dlg = None
        self.list_morph = ['Cumeada', 'Hidrografia_Numerica']

        self.intersection_name = '__Limit_Intersecao__'
        self.buffer_name = '__Buffers__'
        self.match_lines_layer_name = '__Linhas_de_Correspondencia__'
        self.layer_buffers = None

    def tr(self, message):
        """Textos traduzíveis do painel (mesmo contexto que o menu do plugin)."""
        return tr_ui(message)

    def create_layout(self):
        gl_tool = QGridLayout()
        gl_tool.setContentsMargins(2, 2, 2, 2)
        gl_tool.setSpacing(1)

        row_hdr = QGridLayout()
        row_hdr.setContentsMargins(0, 0, 0, 0)
        row_hdr.setSpacing(6)

        c_ = 0
        r_ = 0

        self.lb_session_logo = QLabel()
        self.lb_session_logo.setFixedSize(QSize(40, 40))
        self.lb_session_logo.setAlignment(Qt.AlignCenter)
        logo_path = _resolve_plugin_icon_path(prefer_svg=True)
        self.lb_session_logo.setPixmap(
            _pixmap_from_icon_path(logo_path, self.lb_session_logo.size(), margin_ratio=0.15))
        self.lb_session_logo.setScaledContents(False)
        row_hdr.addWidget(self.lb_session_logo, 0, c_, 2, 1, Qt.AlignLeft | Qt.AlignVCenter)
        c_ += 1

        center_box = QWidget()
        center_layout = QHBoxLayout(center_box)
        center_layout.setContentsMargins(0, 0, 0, 0)
        center_layout.setSpacing(0)
        center_layout.addStretch(1)
        self.lb_ppgec_logo = QLabel()
        self.lb_ppgec_logo.setFixedSize(QSize(90, 32))
        self.lb_ppgec_logo.setAlignment(Qt.AlignCenter)
        ppgec_path = _resolve_ppgec_logo_path()
        if ppgec_path:
            self.lb_ppgec_logo.setPixmap(
                _pixmap_from_icon_path(ppgec_path, self.lb_ppgec_logo.size(), margin_ratio=0.04))
        self.lb_ppgec_logo.setScaledContents(False)
        self.lb_ppgec_logo.setVisible(bool(ppgec_path))
        center_layout.addWidget(self.lb_ppgec_logo, 0, Qt.AlignBottom)
        center_layout.addStretch(1)
        row_hdr.addWidget(center_box, 0, c_, 2, 1)
        row_hdr.setColumnStretch(c_, 1)
        c_ += 1

        self.pb_config = QPushButton()
        self.pb_config.setToolTip(self.tr('Config'))
        self.pb_config.setIcon(_icon_from_icons_file('icon_config.png'))
        self.pb_config.setIconSize(QSize(25, 25))
        self.pb_config.setFixedSize(32, 32)
        self.pb_config.setCursor(Qt.PointingHandCursor)
        self.pb_config.setFocusPolicy(Qt.StrongFocus)
        self.pb_config.setStyleSheet(
            'QPushButton { border: 1px solid #9e9e9e; border-radius: 2px; padding: 0; margin: 0; background: palette(base); }'
            'QPushButton:hover { background: palette(alternate-base); }'
            'QPushButton:pressed { background: palette(mid); }')
        row_hdr.addWidget(self.pb_config, 0, c_, 2, 1, Qt.AlignBottom)

        c_ += 1
        self.lb_version = QLabel(f'v{self.main.plugin_version()}')
        self.lb_version.setAlignment(Qt.AlignBottom | Qt.AlignHCenter)
        row_hdr.addWidget(self.lb_version, 0, c_, 1, 1, Qt.AlignBottom | Qt.AlignHCenter)

        self.pb_lang = QPushButton()
        self.pb_lang.setFlat(True)
        self.pb_lang.setCursor(Qt.PointingHandCursor)
        self.pb_lang.setFocusPolicy(Qt.StrongFocus)
        self.pb_lang.setStyleSheet(
            'QPushButton { border: none; background: transparent; color: #00bcd4; '
            'font-weight: bold; padding: 2px 6px; min-width: 28px; }'
            'QPushButton:hover { color: #26c6da; }'
            'QPushButton:pressed { color: #00838f; }')
        self.pb_lang.setToolTip(self.tr('Alterar idioma da interface'))
        self.pb_lang.clicked.connect(self.open_language_dialog)
        self._refresh_ui_language_button()
        row_hdr.addWidget(self.pb_lang, 1, c_, 1, 1, Qt.AlignVCenter)

        gl_tool.addLayout(row_hdr, r_, 0, 1, 4)

        r_ += 1
        sep_line = QFrame()
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 4)

        r_ += 1
        gl_prj = QGridLayout()
        self.lb_title_proj = QLabel(self.tr('Projeto (.pa.gpkg):'))
        gl_prj.addWidget(self.lb_title_proj, 0, 0)
        self.lb_status_proj = QLabel(self.tr('Não definido'))
        gl_prj.addWidget(self.lb_status_proj,  0, 1)
        gl_tool.addLayout(gl_prj, r_, 0, 1, 4)

        r_ += 1
        self.lb_path_proj = QLabel('~~~')
        self.lb_path_proj.setWordWrap(True)
        gl_tool.addWidget(self.lb_path_proj, r_, 0, 1, 3)
        _proj_btn_style = (
            'QPushButton { border: 1px solid #9e9e9e; border-radius: 2px; padding: 0; margin: 0; '
            'min-width: 32px; max-width: 32px; min-height: 32px; max-height: 32px; '
            'background: palette(base); }'
            'QPushButton:hover { background: palette(alternate-base); }'
            'QPushButton:pressed { background: palette(mid); }')
        lay_proj_btns = QHBoxLayout()
        lay_proj_btns.setContentsMargins(0, 0, 0, 0)
        lay_proj_btns.setSpacing(4)
        self.pb_project_new = QPushButton()
        _ic_new = _icon_from_icons_file('icon_new.png')
        if _ic_new.isNull():
            self.pb_project_new.setText('+')
            _f_plus = self.pb_project_new.font()
            _f_plus.setBold(True)
            self.pb_project_new.setFont(_f_plus)
        else:
            self.pb_project_new.setIcon(_ic_new)
            self.pb_project_new.setIconSize(QSize(24, 24))
        self.pb_project_new.setStyleSheet(_proj_btn_style)
        self.pb_project_new.setToolTip(self.tr('Novo projeto…'))
        self.pb_project_new.setCursor(Qt.PointingHandCursor)
        self.pb_project_new.clicked.connect(self.new_project_dialog)
        lay_proj_btns.addWidget(self.pb_project_new)
        self.pb_project_open = QPushButton()
        _ic_open = _icon_from_icons_file('icon_open.png')
        if _ic_open.isNull():
            self.pb_project_open.setText('...')
        else:
            self.pb_project_open.setIcon(_ic_open)
            self.pb_project_open.setIconSize(QSize(24, 24))
        self.pb_project_open.setStyleSheet(_proj_btn_style)
        self.pb_project_open.setToolTip(self.tr('Abrir projeto…'))
        self.pb_project_open.setCursor(Qt.PointingHandCursor)
        self.pb_project_open.clicked.connect(self.open_project_dialog)
        lay_proj_btns.addWidget(self.pb_project_open)
        w_proj_btns = QWidget()
        w_proj_btns.setLayout(lay_proj_btns)
        gl_tool.addWidget(w_proj_btns, r_, 3, Qt.AlignRight | Qt.AlignTop)

        r_ += 1
        sep_line = QFrame(self)
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 4)
        
        for key_ in self.dic_prj['dems']:

            r_ += 1
            lb_title_ = QLabel(
                self.tr('MDE de referência:') if key_ == 0 else self.tr('MDE de teste:'))
            if key_ == 0:
                self.lb_dem_ref = lb_title_
            else:
                self.lb_dem_test = lb_title_
            label_row = r_
            gl_tool.addWidget(lb_title_, label_row, 0)
            r_ += 1
            obj_cbx = QgsMapLayerComboBox(self)
            obj_cbx.setFilters(QgsMapLayerProxyModel.RasterLayer)
            obj_cbx.setAllowEmptyLayer(True)
            gl_tool.addWidget(obj_cbx, r_, 0, 1, 3)
            self.dic_prj['dems'][key_]['obj_cbx'] = obj_cbx
            obj_pb = _make_dem_info_button(self)
            obj_pb.setToolTip(self.tr('Informações do MDE selecionado'))
            gl_tool.addWidget(obj_pb, label_row, 3, 2, 1, Qt.AlignRight | Qt.AlignVCenter)
            self.dic_prj['dems'][key_]['obj_pb'] = obj_pb
            r_ += 1
            obj_prog_bar = QProgressBar(self)
            gl_tool.addWidget(obj_prog_bar, r_, 0, 1, 4)
            self.dic_prj['dems'][key_]['obj_prog_bar'] = obj_prog_bar

        r_ += 1
        sep_line = QFrame(self)
        sep_line.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line, r_, 0, 1, 4)  

        
        r_ += 1        
        r_start_run = r_
        self.lb_wf_study = QLabel(self.tr('Definição da área de estudos:'))
        gl_tool.addWidget(self.lb_wf_study, r_, 0)
        r_ += 1
        self.cbx_workflow_study = QComboBox(self)
        self.cbx_workflow_study.addItems([
            self.tr('Calcular pela interseção dos MDEs'),
            self.tr('Editar após interseção'),
            self.tr('Selecionar de uma camada'),
        ])
        gl_tool.addWidget(self.cbx_workflow_study, r_, 0, 1, 2)
        r_ += 1
        self.lb_area = QLabel(self.dic_lb_texts['area'].format('—'))
        gl_tool.addWidget(self.lb_area, r_, 0)
        r_ += 1
        self.lb_ext_min = QLabel(self.dic_lb_texts['ext_min'].format('—'))
        gl_tool.addWidget(self.lb_ext_min, r_, 0)

        r_ += 1
        self.lb_study_layer = QLabel(self.tr('Camada polígono (área de estudo):'))
        gl_tool.addWidget(self.lb_study_layer, r_, 0)
        r_ += 1
        self.cbx_study_area_layer = QgsMapLayerComboBox(self)
        self.cbx_study_area_layer.setFilters(QgsMapLayerProxyModel.PolygonLayer)
        self.cbx_study_area_layer.setAllowEmptyLayer(True)
        self.lb_study_layer.setVisible(False)
        self.cbx_study_area_layer.setVisible(False)
        gl_tool.addWidget(self.cbx_study_area_layer, r_, 0, 1, 2)

        r_ += 1
        sep_line_wf = QFrame(self)
        sep_line_wf.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line_wf, r_, 0, 1, 2)

        r_ += 1
        self.lb_wf_pairs = QLabel(self.tr('Seleção de pares homólogos:'))
        gl_tool.addWidget(self.lb_wf_pairs, r_, 0)
        r_ += 1
        self.cbx_workflow_pairs = QComboBox(self)
        self.cbx_workflow_pairs.addItems([
            self.tr('Automática'),
            self.tr('Revisar'),
        ])
        gl_tool.addWidget(self.cbx_workflow_pairs, r_, 0, 1, 2)

        r_ += 1
        self.lb_ext_match = QLabel()
        gl_tool.addWidget(self.lb_ext_match, r_, 0)
        r_ += 1
        self.lb_pair_nr = QLabel()
        gl_tool.addWidget(self.lb_pair_nr, r_, 0)
        self._refresh_extent_and_pairs_labels()

        r_ += 1
        sep_line_wf = QFrame(self)
        sep_line_wf.setFrameShape(QFrame.HLine)
        gl_tool.addWidget(sep_line_wf, r_, 0, 1, 2)

        r_ += 1
        self.lb_wf_outliers = QLabel(self.tr('Tratamento de outliers:'))
        gl_tool.addWidget(self.lb_wf_outliers, r_, 0)
        r_ += 1
        outliers_combo_row = r_
        self.cbx_workflow_outliers = QComboBox(self)
        self.cbx_workflow_outliers.addItems([
            self.tr('Remover automaticamente'),
            self.tr('Avaliar individualmente'),
            self.tr('Usar todos'),
        ])
        gl_tool.addWidget(self.cbx_workflow_outliers, outliers_combo_row, 0, 1, 2)

        self.cb_open_report = QCheckBox(self.tr('Abrir o relatório'))
        self.cb_open_report.setChecked(self._load_open_report_pref())
        self._refresh_open_report_checkbox_ui()
        self.cb_open_report.installEventFilter(self)
        gl_tool.addWidget(
            self.cb_open_report, outliers_combo_row, 3, 1, 1, Qt.AlignRight | Qt.AlignVCenter)

        r_ += 1
        # sep_line_wf = QFrame(self)
        # sep_line_wf.setFrameShape(QFrame.VLine)
        # gl_tool.addWidget(sep_line_wf, r_start_run, 2, r_ - r_start_run, 1, Qt.AlignHCenter)
        gl_tool.addItem(QSpacerItem(0, 0, QSizePolicy.Fixed, QSizePolicy.Expanding), r_, 0, 1, 4)


        self.pb_proc = QPushButton(self.tr('Avaliar'))
        gl_tool.addWidget(
            self.pb_proc, r_start_run, 3, outliers_combo_row - r_start_run, 1, Qt.AlignHCenter)
        self._refresh_proc_button()

        r_ += 1
        self.lb_log = QLabel(self.tr('LOG:'))
        gl_tool.addWidget(self.lb_log, r_, 0, 1, 1)
        r_ += 1
        self.pte_log = QPlainTextEdit ()
        self.pte_log.setReadOnly(True)  # Logs are read-only
        self.pte_log.setWordWrapMode(QTextOption.WordWrap)  # Prevents long lines from wrapping
        self.pte_log.setBackgroundVisible(False)  # Optional, makes it look cleaner
        self.pte_log.setFont(QFont("Monospace", 8))  # Use a monospace font for better alignment
        gl_tool.addWidget(self.pte_log, r_, 0, 1, 4)

        lg_sa = QGridLayout()
        lg_sa.setContentsMargins(0, 0, 0, 0)
        lg_sa.setSpacing(0)
        lg_sa.addLayout(gl_tool, 0, 0)

        self.trigger_actions()
        return lg_sa

    def trigger_actions(self):
        for key_ in self.dic_prj['dems']:
            self.dic_prj['dems'][key_]['obj_pb'].clicked.connect(partial(self.log_dem_layer_info, key_=key_))
            self.dic_prj['dems'][key_]['obj_cbx'].layerChanged.connect(self.persist_dem_layer_selection)
        self.cbx_workflow_study.currentIndexChanged.connect(self._on_workflow_study_changed)
        self.cbx_workflow_study.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_workflow_pairs.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_workflow_outliers.currentIndexChanged.connect(self._persist_workflow_ui_if_project)
        self.cbx_study_area_layer.layerChanged.connect(self._persist_workflow_ui_if_project)
        self.pb_proc.clicked.connect(self.exec_analyze)
        self.pb_config.clicked.connect(self.open_settings)
        self.cb_open_report.toggled.connect(self._save_open_report_pref)

    def _refresh_open_report_checkbox_ui(self) -> None:
        cb = getattr(self, 'cb_open_report', None)
        if cb is None:
            return
        cb.setToolTip(self.tr(
            'Caixa: abrir automaticamente após a avaliação.\n'
            'Clique no texto para abrir o último relatório PDF.'))

    @staticmethod
    def _checkbox_click_on_text(cb: QCheckBox, pos) -> bool:
        opt = QStyleOptionButton()
        cb.initStyleOption(opt)
        style = cb.style()
        indicator = style.subElementRect(QStyle.SE_CheckBoxIndicator, opt, cb)
        if indicator.contains(pos):
            return False
        contents = style.subElementRect(QStyle.SE_CheckBoxContents, opt, cb)
        return contents.contains(pos)

    def eventFilter(self, obj, event):
        cb = getattr(self, 'cb_open_report', None)
        if obj is cb and cb is not None:
            et = event.type()
            if et == QEvent.MouseMove:
                if self._checkbox_click_on_text(cb, event.pos()):
                    cb.setCursor(QCursor(Qt.PointingHandCursor))
                else:
                    cb.unsetCursor()
            elif et == QEvent.MouseButtonRelease and event.button() == Qt.LeftButton:
                if self._checkbox_click_on_text(cb, event.pos()):
                    self._open_last_report()
                    return True
        return super().eventFilter(obj, event)

    def _resolve_last_report_pdf_path(self) -> str:
        path = getattr(self, '_last_report_pdf_path', '') or ''
        if path and os.path.isfile(path):
            return path
        pf = self.dic_prj.get('project_file')
        if not pf:
            return ''
        data_dir = project_data_dir(pf)
        if not os.path.isdir(data_dir):
            return ''
        candidates = glob.glob(os.path.join(data_dir, 'Relatorio_MDE_AP_*.pdf'))
        if not candidates:
            return ''
        return max(candidates, key=os.path.getmtime)

    def _open_last_report(self) -> None:
        path = self._resolve_last_report_pdf_path()
        if not path:
            self.log_message(self.tr('Nenhum relatório disponível para abrir.'), 'WARNING')
            return
        self._open_report_file(path)

    def _load_open_report_pref(self) -> bool:
        return bool(QSettings(SETTINGS_ORG, SETTINGS_APP).value(
            SETTINGS_KEY_OPEN_REPORT, True, type=bool))

    def _save_open_report_pref(self, _checked: bool = False) -> None:
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        settings.setValue(SETTINGS_KEY_OPEN_REPORT, self.cb_open_report.isChecked())
        settings.sync()

    def _open_report_file(self, path: str) -> None:
        if not path or not os.path.isfile(path):
            return
        url = QUrl.fromLocalFile(os.path.normpath(os.path.abspath(path)))
        if not QDesktopServices.openUrl(url):
            self.log_message(
                self.tr('Não foi possível abrir o relatório: {0}').format(path), 'WARNING')

    def set_project_paths(self, project_file: str):
        """Define arquivo .pa.gpkg e pasta de dados (logs etc.) com o mesmo nome base."""
        project_file = os.path.normpath(os.path.abspath(project_file))
        pl = project_file.lower()
        if pl.endswith(PROJECT_EXT.lower()):
            pass
        else:
            root, ext = os.path.splitext(project_file)
            if ext.lower() == '.gpkg':
                if root.lower().endswith('.pa'):
                    project_file = project_file
                else:
                    project_file = root + PROJECT_EXT
            else:
                project_file = (root + PROJECT_EXT) if ext else project_file + PROJECT_EXT
        self.dic_prj['project_file'] = project_file
        self.gpkg_path = project_file
        data_dir = project_data_dir(project_file)
        self.dic_prj['path'] = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.lb_path_proj.setText(project_file)
        self.node_group = None

    def _active_gpkg_path(self) -> str:
        return _norm_gpkg_path(self.gpkg_path or self.dic_prj.get('project_file') or '')

    def _layer_display_name(self, logical_name: str) -> str:
        return project_layer_display_name(
            logical_name,
            self.gpkg_path or self.dic_prj.get('project_file') or '',
        )

    def _find_map_layer_for_project(self, logical_name: str, gpkg_path: str = ''):
        """Camada no mapa cujo source aponta para o .pa.gpkg indicado."""
        gpkg_n = _norm_gpkg_path(gpkg_path or self._active_gpkg_path())
        if not gpkg_n or not logical_name:
            return None
        proj = QgsProject.instance()
        names = [self._layer_display_name(logical_name)]
        if names[0] != logical_name:
            names.append(logical_name)
        for nm in names:
            for lyr in proj.mapLayersByName(nm):
                if lyr and lyr.isValid() and map_layer_gpkg_path(lyr) == gpkg_n:
                    return lyr
        return None

    def _dialog_start_dir(self) -> str:
        d = self.aux_tools.get_(key_=AUX_LAST_PROJECT_DIR_KEY)
        if d and isinstance(d, str) and os.path.isdir(d):
            return d
        legacy = self.aux_tools.get_(key_='project_file')
        if legacy and isinstance(legacy, str):
            parent = os.path.dirname(legacy)
            if parent and os.path.isdir(parent):
                return parent
        return ''

    def _persist_panel_stats_to_mdepa(self):
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        c = self._panel_stats_cache
        save_panel_stats_to_mdepa_path(
            pf,
            c.get('area', ''),
            c.get('ext_min', ''),
            c.get('ext_match', ''),
            c.get('pair_nr', ''),
        )

    def _reset_panel_stats_ui(self):
        self._panel_stats_cache = {'area': '', 'ext_min': '', 'ext_match': '', 'pair_nr': ''}
        self.lb_area.setText(self.dic_lb_texts['area'].format('—'))
        self.lb_ext_min.setText(self.dic_lb_texts['ext_min'].format('—'))
        self._refresh_extent_and_pairs_labels()

    def _refresh_extent_and_pairs_labels(self):
        em_raw = (self._panel_stats_cache.get('ext_match') or '').strip()
        em_m = self._float_from_panel_str(em_raw)
        if em_m is not None and em_m > 0:
            em_km = round(em_m / 1000.0, 1)
            em_disp = str(em_km)
        else:
            em_disp = '—'
        pr = (self._panel_stats_cache.get('pair_nr') or '').strip()
        self.lb_ext_match.setText(self.dic_lb_texts['ext_match'].format(em_disp))
        self.lb_pair_nr.setText(self.dic_lb_texts['pair_nr'].format(pr or '—'))

    def restore_panel_stats_from_project(self):
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        data = load_panel_stats_from_mdepa_path(pf)
        self._panel_stats_cache = {k: (data.get(k) or '') for k in self._panel_stats_cache}
        self.lb_area.setText(
            self.dic_lb_texts['area'].format(self._panel_stats_cache['area'] or '—'))
        self.lb_ext_min.setText(
            self.dic_lb_texts['ext_min'].format(self._panel_stats_cache['ext_min'] or '—'))
        self._refresh_extent_and_pairs_labels()

    def open_project_dialog(self):
        start_dir = self._dialog_start_dir()
        path, _ = QFileDialog.getOpenFileName(
            self, self.tr('Abrir projeto'), start_dir, project_file_filter_i18n())
        if not path:
            return
        if not os.path.isfile(path):
            self.log_message(self.tr('Arquivo não encontrado: {0}').format(path), 'ERROR')
            return
        self.set_project_paths(path)
        self.ensure_pipeline_etapas_table()
        self.aux_tools.save_(
            key_=AUX_LAST_PROJECT_DIR_KEY,
            value_=os.path.dirname(self.dic_prj['project_file']),
        )
        self.check_prj_folder(self.dic_prj['project_file'])
        self.reload_settings_from_project_file()
        n_layers = self._load_project_layers_into_map()
        self._log_grass_provider_check()
        self.log_message(
            self.tr('Projeto aberto: {0}').format(self.dic_prj['project_file']), 'INFO')
        if n_layers:
            self.log_message(
                self.tr('Camadas do projeto carregadas no mapa: {0}').format(n_layers),
                'INFO')

    def new_project_dialog(self):
        # Mesma lógica que «Abrir projeto»: last_project_dir (gravado ao abrir/criar), não a chave legacy project_file.
        start_dir = self._dialog_start_dir()
        # Sem PROJECT_EXT no nome sugerido: no Windows o QFileDialog costuma acrescentar a extensão do filtro.
        suggest = os.path.join(start_dir, 'novo_projeto') if start_dir else 'novo_projeto'
        path, _ = QFileDialog.getSaveFileName(
            self, self.tr('Novo projeto'), suggest, project_file_filter_i18n())
        if not path:
            return
        path = os.path.normpath(os.path.abspath(path))
        dup_ext = (PROJECT_EXT + PROJECT_EXT).lower()
        while path.lower().endswith(dup_ext):
            path = path[:-len(PROJECT_EXT)]
        pl = path.lower()
        if not pl.endswith(PROJECT_EXT.lower()):
            root, ext = os.path.splitext(path)
            if ext.lower() == '.gpkg':
                if not root.lower().endswith('.pa'):
                    path = root + PROJECT_EXT
            else:
                path = root + PROJECT_EXT
        if os.path.isfile(path):
            self.log_message(
                self.tr('Já existe um arquivo com esse nome. Escolha outro nome ou use Abrir projeto.'),
                'ERROR')
            return
        parent = os.path.dirname(path)
        if not parent or not os.path.isdir(parent):
            self.log_message(self.tr('Diretório inválido para salvar o projeto.'), 'ERROR')
            return
        self.set_project_paths(path)
        proj_crs = QgsProject.instance().crs()
        crs_auth = proj_crs.authid() if proj_crs.isValid() else 'EPSG:4326'
        try:
            if not self._create_empty_mdepa_shell(path):
                raise RuntimeError('create_empty_mdepa_shell retornou False')
            if not self.ensure_pipeline_etapas_table():
                raise RuntimeError('ensure_pipeline_etapas_table retornou False')
        except Exception as e:
            if os.path.isfile(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
            root = _strip_project_ext(path)
            stray_gpkg = root + '.gpkg'
            if os.path.isfile(stray_gpkg):
                try:
                    os.remove(stray_gpkg)
                except OSError:
                    pass
            self.dic_prj['project_file'] = ''
            self.gpkg_path = ''
            self.dic_prj['path'] = ''
            self.lb_path_proj.setText('~~~')
            self.log_message(self.tr('Não foi possível criar o projeto: {0}').format(e), 'ERROR')
            return
        self.aux_tools.save_(key_=AUX_LAST_PROJECT_DIR_KEY, value_=os.path.dirname(path))
        self.check_prj_folder(path)
        self._reset_panel_stats_ui()
        self.persist_project_config_from_widgets(log_values=False)
        self.reload_settings_from_project_file()
        self._log_grass_provider_check()
        self.log_message(
            self.tr('Novo projeto criado: {0} (CRS inicial: {1})').format(path, crs_auth), 'INFO')

    def reload_settings_from_project_file(self):
        """Restaura dic_param: defaults → QSettings → valores gravados no .pa.gpkg (por projeto)."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        dlg = self.settings_dlg
        dlg.apply_defaults_to_values()
        dlg.get_dic_from_settings()
        load_plugin_settings_from_mdepa_path(pf, dlg.dic_param)
        dlg.sync_widgets_from_dic_param()
        self.restore_dem_layers_from_project()
        self.restore_workflow_ui_from_project()
        self.restore_panel_stats_from_project()

    def persist_dem_layer_selection(self):
        """Grava QgsRasterLayer.source() dos combos no .pa.gpkg (slots 0 e 1)."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        key_to_source = {}
        for key_ in (0, 1):
            cbx = self.dic_prj['dems'][key_]['obj_cbx']
            ly = cbx.currentLayer() if cbx else None
            if isinstance(ly, QgsRasterLayer):
                key_to_source[key_] = ly.source()
            else:
                key_to_source[key_] = ''
        save_dem_sources_to_project_path(pf, key_to_source)

    def restore_dem_layers_from_project(self):
        """Carrega DEMs guardados no .pa.gpkg se ainda não estiverem no projeto QGIS."""
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        sources = load_dem_sources_from_project_path(pf)
        uri0 = (sources.get(0) or '').strip() if sources else ''
        uri1 = (sources.get(1) or '').strip() if sources else ''
        if not uri0 and not uri1:
            self.auto_assign_dems_by_resolution_if_needed()
            return
        for key_ in (0, 1):
            cbx = self.dic_prj['dems'][key_]['obj_cbx']
            uri = (sources.get(key_) or '').strip()
            cbx.blockSignals(True)
            try:
                if not uri:
                    cbx.setLayer(None)
                    continue
                existing = find_dem_layer_in_project(uri)
                if existing is not None:
                    cbx.setLayer(existing)
                    continue
                base = os.path.basename(uri.split('|')[0].strip()) or f'DEM_{key_}'
                label = self.dic_prj['dems'][key_]['type']
                layer_name = f'{label} ({base})'
                rl = QgsRasterLayer(uri, layer_name)
                if not rl.isValid():
                    self.log_message(
                        self.tr('Não foi possível carregar o DEM: {0}').format(uri), 'ERROR')
                    cbx.setLayer(None)
                    continue
                QgsProject.instance().addMapLayer(rl)
                cbx.setLayer(rl)
            finally:
                cbx.blockSignals(False)

    def _candidate_input_dem_layers(self):
        """Rasters válidos do projeto QGIS, excluindo camadas internas do plugin / .pa.gpkg."""
        pf = self.dic_prj.get('project_file')
        pf_norm = ''
        if pf and os.path.isfile(pf):
            pf_norm = os.path.normcase(os.path.normpath(os.path.abspath(pf)))
        out = []
        for layer in QgsProject.instance().mapLayers().values():
            if not isinstance(layer, QgsRasterLayer) or not layer.isValid():
                continue
            name = (layer.name() or '').strip()
            if name.startswith('__'):
                continue
            src = (layer.source() or '').strip()
            path_part = src.split('|')[0].strip() if src else ''
            if pf_norm and path_part and os.path.isfile(path_part):
                if os.path.normcase(os.path.normpath(os.path.abspath(path_part))) == pf_norm:
                    continue
            gsd = _raster_layer_gsd(layer)
            if gsd is None:
                continue
            out.append((gsd, layer))
        return out

    def auto_assign_dems_by_resolution_if_needed(self) -> bool:
        """
        Se ref/teste não estão definidos no .pa.gpkg e há exatamente 2 MDEs no mapa,
        atribui o de melhor resolução (menor GSD) à referência e o outro ao teste.
        """
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return False
        sources = load_dem_sources_from_project_path(pf)
        uri0 = (sources.get(0) or '').strip() if sources else ''
        uri1 = (sources.get(1) or '').strip() if sources else ''
        if uri0 or uri1:
            return False

        candidates = self._candidate_input_dem_layers()
        if len(candidates) != 2:
            return False

        candidates.sort(key=lambda item: (item[0], (item[1].name() or '').lower()))
        gsd_ref, layer_ref = candidates[0]
        gsd_test, layer_test = candidates[1]
        for key_, layer in ((0, layer_ref), (1, layer_test)):
            cbx = self.dic_prj['dems'][key_]['obj_cbx']
            if cbx is None:
                return False
            cbx.blockSignals(True)
            try:
                cbx.setLayer(layer)
            finally:
                cbx.blockSignals(False)

        self.persist_dem_layer_selection()
        self.log_message(
            self.tr(
                'MDEs atribuídos automaticamente pela resolução espacial: '
                'referência={0} (GSD≈{1:.3f}), teste={2} (GSD≈{3:.3f}).'
            ).format(layer_ref.name(), gsd_ref, layer_test.name(), gsd_test),
            'INFO',
        )
        return True

    def save_plugin_settings_to_project(self, dic_param: dict) -> bool:
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return False
        return save_plugin_settings_to_mdepa_path(pf, dic_param)

    def persist_project_config_from_widgets(self, log_values: bool = False):
        """Atualiza dic_param a partir dos widgets, grava QSettings e .pa.gpkg (parâmetros + MDEs)."""
        dlg = self.settings_dlg
        dlg.flush_widgets_to_dic_param(log_values=log_values)
        dic_save = {}
        for item_i in dlg.dic_param:
            if not item_i.startswith('step_'):
                continue
            dic_save[item_i] = {
                item_j: dlg.dic_param[item_i]['fields'][item_j]['value']
                for item_j in dlg.dic_param[item_i]['fields']
            }
        dlg.aux_tools.save_dic(dic_=dic_save, key_='dic_param')
        self.save_plugin_settings_to_project(dlg.dic_param)
        self.persist_dem_layer_selection()
        # Não limpar _workflow_pause aqui: senão um segundo exec_analyze (reentrância/Qt)
        # vê pausa já limpa e faz reprocessamento completo em vez de Continuar → morfologia/buffers.
        self._persist_workflow_ui_if_project(clear_workflow_pause=False)
        self._persist_panel_stats_to_mdepa()

    def _flatten_run_snapshot(self) -> dict:
        """Parâmetros dos steps + fontes DEM (para comparar com a última avaliação concluída)."""
        out = {}
        dlg = self.settings_dlg
        for sk, block in dlg.dic_param.items():
            if not isinstance(sk, str) or not sk.startswith('step_'):
                continue
            if not isinstance(block, dict) or 'fields' not in block:
                continue
            for fk, meta in block['fields'].items():
                if isinstance(meta, dict):
                    out[f'{sk}.{fk}'] = str(meta.get('value', ''))
        for i in (0, 1):
            cbx = self.dic_prj['dems'][i]['obj_cbx']
            ly = cbx.currentLayer() if cbx else None
            out[f'dem_{i}'] = ly.source() if isinstance(ly, QgsRasterLayer) else ''
        out['workflow.study_mode'] = str(self.cbx_workflow_study.currentIndex())
        out['workflow.pairs_mode'] = str(self.cbx_workflow_pairs.currentIndex())
        out['workflow.outliers_mode'] = str(self.cbx_workflow_outliers.currentIndex())
        sly = self.cbx_study_area_layer.currentLayer()
        out['workflow.study_layer_source'] = (
            sly.source() if isinstance(sly, QgsVectorLayer) else '')
        return out

    def _refresh_proc_button(self):
        if self._workflow_pause == 'post_intersection':
            self.pb_proc.setText(self.tr('Continuar'))
            self.pb_proc.setToolTip(
                self.tr('Continuar para morfologia após editar a área de interseção.'))
            ic = _icon_from_icons_file('icon_continuar.png')
        elif self._workflow_pause == 'post_pairs_review':
            self.pb_proc.setText(self.tr('Continuar'))
            self.pb_proc.setToolTip(
                self.tr('Continuar para gerar buffers após rever os pares.'))
            ic = _icon_from_icons_file('icon_continuar.png')
        else:
            self.pb_proc.setText(self.tr('Avaliar'))
            self.pb_proc.setToolTip(self.tr('Executar ou retomar a análise.'))
            ic = _icon_from_icons_file('icon_avaliar.png')
        if not ic.isNull():
            self.pb_proc.setIcon(ic)
            self.pb_proc.setIconSize(QSize(22, 22))
        else:
            self.pb_proc.setIcon(QIcon())

    def _on_workflow_study_changed(self, idx):
        show = idx == 2
        self.lb_study_layer.setVisible(show)
        self.cbx_study_area_layer.setVisible(show)

    def _persist_workflow_ui_if_project(self, clear_workflow_pause: bool = True):
        if clear_workflow_pause and self._workflow_pause is not None:
            self._workflow_pause = None
            self._refresh_proc_button()
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        ly = self.cbx_study_area_layer.currentLayer()
        src = ly.source() if isinstance(ly, QgsVectorLayer) else ''
        save_workflow_ui_to_mdepa_path(
            pf,
            self.cbx_workflow_study.currentIndex(),
            self.cbx_workflow_pairs.currentIndex(),
            self.cbx_workflow_outliers.currentIndex(),
            src,
        )

    def restore_workflow_ui_from_project(self):
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            return
        data = load_workflow_ui_from_mdepa_path(pf)
        widgets = (
            self.cbx_workflow_study,
            self.cbx_workflow_pairs,
            self.cbx_workflow_outliers,
            self.cbx_study_area_layer,
        )
        for w in widgets:
            w.blockSignals(True)
        try:
            self.cbx_workflow_study.setCurrentIndex(
                max(0, min(int(data.get('study_mode', 0)), 2)))
            self.cbx_workflow_pairs.setCurrentIndex(
                max(0, min(int(data.get('pairs_mode', 0)), 1)))
            self.cbx_workflow_outliers.setCurrentIndex(
                max(0, min(int(data.get('outliers_mode', 0)), 2)))
            src = (data.get('study_layer_source') or '').strip()
            if src:
                vl = find_vector_layer_in_project(src)
                self.cbx_study_area_layer.setLayer(vl)
            else:
                self.cbx_study_area_layer.setLayer(None)
        finally:
            for w in widgets:
                w.blockSignals(False)
        self._on_workflow_study_changed(self.cbx_workflow_study.currentIndex())

    def _count_outliers_flagged(self, dic_values):
        n = 0
        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                for count_ in dic_values[scale_][class_]:
                    rec = dic_values[scale_][class_][count_]
                    if self._is_statistical_outlier_rec(rec, 'outlier_h', 'dm_h'):
                        n += 1
                    elif self._is_statistical_outlier_rec(rec, 'outlier_v', 'dm_v'):
                        n += 1
        return n

    def _reset_outlier_flags(self, dic_values):
        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                for count_ in dic_values[scale_][class_]:
                    rec = dic_values[scale_][class_][count_]
                    rec['outlier_h'] = False
                    rec['outlier_v'] = False
                    rec['outlier'] = False

    def _apply_outlier_workflow(self, dic_values):
        """Aplica modo de outliers do fluxo (IQR ou usar todos)."""
        om = self.cbx_workflow_outliers.currentIndex()
        if om in (0, 1):
            self.check_outliers(dic_values)
        else:
            self._reset_outlier_flags(dic_values)

    def apply_study_area_from_map_layer(self) -> bool:
        self.log_message(self.tr('ÁREA DE ESTUDO A PARTIR DA CAMADA'), 'INFO')
        ly = self.cbx_study_area_layer.currentLayer()
        if not isinstance(ly, QgsVectorLayer) or not ly.isValid():
            self.log_message(self.tr('Selecione uma camada de polígonos válida.'), 'ERROR')
            return False
        if ly.geometryType() != QgsWkbTypes.PolygonGeometry:
            self.log_message(self.tr('A camada de área de estudo tem de ser poligonal.'), 'ERROR')
            return False
        tgt_crs = QgsCoordinateReferenceSystem(self.crs_epsg)
        if not tgt_crs.isValid():
            self.log_message(self.tr('CRS do MDE de referência inválido.'), 'ERROR')
            return False
        xform = QgsCoordinateTransform(ly.crs(), tgt_crs, QgsProject.instance())
        geoms = []
        for f in ly.getFeatures():
            g = f.geometry()
            if g is None or g.isEmpty():
                continue
            g2 = QgsGeometry(g)
            try:
                g2.transform(xform)
            except Exception:
                self.log_message(
                    self.tr('Falha ao reprojetar geometrias para o CRS do projeto.'), 'ERROR')
                return False
            geoms.append(g2)
        if not geoms:
            self.log_message(
                self.tr('A camada de área de estudo não tem polígonos válidos.'), 'ERROR')
            return False
        union_g = QgsGeometry.unaryUnion(geoms)
        if union_g.isEmpty():
            self.log_message(self.tr('União da área de estudo está vazia.'), 'ERROR')
            return False
        self._clear_features_from_limit_layers()
        for key_ in (0, 1):
            name = f'__Limit_{self.dic_prj["dems"][key_]["type"]}__'
            layer = self._resolve_limit_layer_for_editing(name)
            if layer is None:
                self.log_message(
                    self.tr('Camada de limite indisponível: {0}').format(name), 'ERROR')
                return False
            feat = QgsFeature(layer.fields())
            feat.setGeometry(QgsGeometry(union_g))
            layer.startEditing()
            layer.addFeature(feat)
            layer.commitChanges()
            layer.updateExtents()
            layer.triggerRepaint()
            self.dic_prj['dems'][key_]['geom_status'] = True
        self.run_polygon_intersection()
        return True

    def _gpkg_layer_valid(self, layer_name: str) -> bool:
        """True se a camada nomeada existe e abre no projeto .pa.gpkg atual."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        uri = f'{self.gpkg_path}|layername={layer_name}'
        vl = QgsVectorLayer(uri, layer_name, 'ogr')
        return vl.isValid()

    def _create_empty_mdepa_shell(self, path: str) -> bool:
        """GPKG vazio (sem camadas de limite); tabelas vetoriais na primeira Avaliar → _ensure_limit_vector_tables_in_mdepa."""
        try:
            drv = ogr.GetDriverByName('GPKG')
            if drv is None:
                return False
            ds = drv.CreateDataSource(path)
            if ds is None:
                return False
            ds = None
            normalize_project_pa_file(path)
            return os.path.isfile(path)
        except Exception:
            return False

    def _plugin_layer_tree_group(self):
        """Garante o grupo raiz do plugin na árvore (__MDE_AP__ + nome do projeto)."""
        root = QgsProject.instance().layerTreeRoot()
        project_file = self.gpkg_path or self.dic_prj.get('project_file') or ''
        target = plugin_layer_tree_group_name(project_file)
        grp = root.findGroup(target)
        if grp:
            return grp
        return root.insertGroup(0, target)

    def _gpkg_has_any_vector_layer(self) -> bool:
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        ds = ogr.Open(self.gpkg_path, 0)
        if ds is None:
            return False
        try:
            return ds.GetLayerCount() > 0
        finally:
            ds = None

    def _ensure_limit_vector_tables_in_mdepa(self, crs_authid: str) -> bool:
        """Garante no ficheiro .pa.gpkg as três camadas de polígono vazias (sem as carregar no mapa)."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        r = f'__Limit_{self.dic_prj["dems"][0]["type"]}__'
        t = f'__Limit_{self.dic_prj["dems"][1]["type"]}__'
        i = self.intersection_name
        if self._gpkg_layer_valid(r) and self._gpkg_layer_valid(t) and self._gpkg_layer_valid(i):
            return True

        data_dir = self.dic_prj.get('path')
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
        crs = QgsCoordinateReferenceSystem(crs_authid)
        if not crs.isValid():
            crs = QgsCoordinateReferenceSystem('EPSG:4326')
        ctx = QgsCoordinateTransformContext()
        crs_s = crs.authid()

        if not self._gpkg_layer_valid(r):
            if not self._gpkg_has_any_vector_layer():
                opt = QgsVectorFileWriter.SaveVectorOptions()
                opt.driverName = 'GPKG'
                opt.layerName = r
                writer_ = QgsVectorFileWriter.create(
                    self.gpkg_path,
                    QgsFields(),
                    QgsWkbTypes.Polygon,
                    crs,
                    ctx,
                    opt)
                if writer_.hasError() != QgsVectorFileWriter.NoError:
                    return False
                del writer_
            else:
                mem = QgsVectorLayer(f'polygon?crs={crs_s}&index=yes', r, 'memory')
                mem.dataProvider().addAttributes(QgsFields())
                mem.updateFields()
                opt = QgsVectorFileWriter.SaveVectorOptions()
                opt.driverName = 'GPKG'
                opt.layerName = r
                opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
                QgsVectorFileWriter.writeAsVectorFormat(
                    layer=mem, fileName=self.gpkg_path, options=opt)
            normalize_project_pa_file(self.gpkg_path)

        if not self._gpkg_layer_valid(t):
            mem = QgsVectorLayer(f'polygon?crs={crs_s}&index=yes', t, 'memory')
            mem.dataProvider().addAttributes(QgsFields())
            mem.updateFields()
            opt = QgsVectorFileWriter.SaveVectorOptions()
            opt.driverName = 'GPKG'
            opt.layerName = t
            opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
            QgsVectorFileWriter.writeAsVectorFormat(
                layer=mem, fileName=self.gpkg_path, options=opt)
            normalize_project_pa_file(self.gpkg_path)

        if not self._gpkg_layer_valid(i):
            mem = QgsVectorLayer(f'polygon?crs={crs_s}&index=yes', i, 'memory')
            pr_ = mem.dataProvider()
            sch = QgsFields()
            sch.append(QgsField('fid', QVariant.Int))
            sch.append(QgsField('AREA', QVariant.Double))
            pr_.addAttributes(sch)
            mem.updateFields()
            opt = QgsVectorFileWriter.SaveVectorOptions()
            opt.driverName = 'GPKG'
            opt.layerName = i
            opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
            QgsVectorFileWriter.writeAsVectorFormat(
                layer=mem, fileName=self.gpkg_path, options=opt)
            normalize_project_pa_file(self.gpkg_path)

        return (
            self._gpkg_layer_valid(r)
            and self._gpkg_layer_valid(t)
            and self._gpkg_layer_valid(i))

    def _ensure_limit_layers_for_analysis(self) -> bool:
        """Só entra no QGIS o que já existe no .pa.gpkg: cria tabelas vazias no ficheiro e depois carrega com OGR."""
        if not self._ensure_limit_vector_tables_in_mdepa(self.crs_epsg):
            return False
        for key_ in (0, 1):
            name = f'__Limit_{self.dic_prj["dems"][key_]["type"]}__'
            if self._find_map_layer_for_project(name) is None:
                self.get_gpkg_layer(prefix_=name, gpkg_path=self.gpkg_path)
        iname = self.intersection_name
        if self._find_map_layer_for_project(iname) is None:
            self.get_gpkg_layer(prefix_=iname, gpkg_path=self.gpkg_path)
        return True

    def _project_vector_layer_names_preferred(self) -> list:
        """Ordem de carregamento (primeira = fundo do grupo; última = topo)."""
        names = [
            f'__Limit_{self.dic_prj["dems"][0]["type"]}__',
            f'__Limit_{self.dic_prj["dems"][1]["type"]}__',
            self.intersection_name,
        ]
        names.extend(self._morphology_gpkg_layer_names())
        names.append(self.match_lines_layer_name)
        names.append(self.buffer_name)
        return names

    def _list_gpkg_geometry_layer_names(self, gpkg_path: str = None) -> list:
        """Nomes das camadas com geometria no .pa.gpkg (exclui tabelas de config)."""
        path = gpkg_path or self.gpkg_path
        if not path or not os.path.isfile(path):
            return []
        skip = {
            PA_SETTINGS_TABLE,
            PIPELINE_ETAPAS_TABLE,
            'gpkg_contents',
            'gpkg_geometry_columns',
            'gpkg_spatial_ref_sys',
            'gpkg_tile_matrix',
            'gpkg_tile_matrix_set',
            'gpkg_metadata',
            'gpkg_metadata_reference',
            'gpkg_extensions',
        }
        names = []
        ds = ogr.Open(path, 0)
        if ds is None:
            return []
        try:
            for i in range(ds.GetLayerCount()):
                lyr = ds.GetLayerByIndex(i)
                if lyr is None:
                    continue
                name = lyr.GetName()
                if not name or name in skip or name.startswith('rtree_'):
                    continue
                # Só camadas com geometria (não tabelas atributo)
                try:
                    if lyr.GetGeomType() == ogr.wkbNone:
                        continue
                except Exception:
                    pass
                names.append(name)
        finally:
            ds = None
        return names

    def _load_project_layers_into_map(self) -> int:
        """Ao abrir .pa.gpkg: carrega no mapa as camadas vetoriais já existentes no ficheiro."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return 0
        preferred = self._project_vector_layer_names_preferred()
        present = set(self._list_gpkg_geometry_layer_names(self.gpkg_path))
        if not present:
            return 0
        # Preferidas na ordem definida; restantes __* no fim
        ordered = [n for n in preferred if n in present]
        extras = sorted(
            n for n in present
            if n not in ordered and (n.startswith('__') or n in preferred)
        )
        ordered.extend(extras)
        loaded = 0
        for name in ordered:
            if self._find_map_layer_for_project(name, self.gpkg_path) is not None:
                loaded += 1
                continue
            if not self._gpkg_layer_valid(name):
                continue
            lyr = self.get_gpkg_layer(
                prefix_=name, gpkg_path=self.gpkg_path, show=True)
            if lyr is not None and lyr.isValid():
                loaded += 1
        return loaded

    def _clear_gpkg_vector_layer_features(self, layer_name: str) -> bool:
        """Esvazia feições da camada nomeada no .pa.gpkg (só do projeto ativo)."""
        gpkg_n = self._active_gpkg_path()
        if not gpkg_n or not os.path.isfile(gpkg_n):
            return False
        lyr = self._find_map_layer_for_project(layer_name, gpkg_n)
        if lyr is None:
            uri = gpkg_layer_uri(gpkg_n, layer_name)
            lyr = QgsVectorLayer(uri, layer_name, 'ogr')
        if lyr is None or not lyr.isValid():
            return False
        lyr.startEditing()
        ids = [f.id() for f in lyr.getFeatures()]
        if ids:
            lyr.deleteFeatures(ids)
        if not lyr.commitChanges():
            lyr.rollBack()
            return False
        lyr.updateExtents()
        lyr.triggerRepaint()
        return True

    def _clear_features_from_limit_layers(self):
        """Nova análise: esvazia limites e interseção no .pa.gpkg antes de gerar polígonos."""
        names = (
            f'__Limit_{self.dic_prj["dems"][0]["type"]}__',
            f'__Limit_{self.dic_prj["dems"][1]["type"]}__',
            self.intersection_name,
        )
        for nm in names:
            self._clear_gpkg_vector_layer_features(nm)

    def _morphology_gpkg_layer_names(self):
        type_0 = self.dic_prj['dems'][0]['type']
        type_1 = self.dic_prj['dems'][1]['type']
        return [f'__{m}_Z_{t}__' for m in self.list_morph for t in (type_0, type_1)]

    def _remove_project_layers_named(self, *layer_names: str) -> None:
        """Remove do mapa apenas camadas do .pa.gpkg ativo."""
        gpkg_n = self._active_gpkg_path()
        proj = QgsProject.instance()
        for nm in layer_names:
            candidates = []
            display = self._layer_display_name(nm)
            candidates.extend(proj.mapLayersByName(display))
            if display != nm:
                candidates.extend(proj.mapLayersByName(nm))
            for lyr in list(candidates):
                if gpkg_n and map_layer_gpkg_path(lyr) != gpkg_n:
                    continue
                try:
                    proj.removeMapLayer(lyr.id())
                except Exception:
                    pass

    def _pipeline_reset_timestamps_from_ordem(self, min_ordem: int) -> None:
        path = self.gpkg_path
        if not path or not os.path.isfile(path):
            return
        try:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    f'''UPDATE {PIPELINE_ETAPAS_TABLE}
                        SET inicio = NULL, fim = NULL
                        WHERE ordem >= ?''',
                    (min_ordem,),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    def _clear_pec_report_cache(self) -> None:
        self._pec_report_pec_intro = ''
        self._pec_report_plan_rows = []
        self._pec_report_alt_rows = []

    def _sanitize_pipeline_for_restart_immediate(self, restart) -> None:
        """Antes de retomar a cadeia: limpa camadas GPKG, rótulos do painel e variáveis obsoletas."""
        self._workflow_pause = None
        self._refresh_proc_button()
        self.layer_buffers = None
        mn = self.match_lines_layer_name
        bn = self.buffer_name
        morph_names = self._morphology_gpkg_layer_names()

        known = (None, 'morfologia_referencia', 'correspondencia_linhas', 'buffers')
        if restart not in known:
            self.log_message(
                self.tr(
                    'Tipo de retomada não reconhecido ({0}); aplicando limpeza completa.'
                ).format(repr(restart)),
                'WARNING',
            )
            restart = None

        if restart == 'buffers':
            self._clear_gpkg_vector_layer_features(bn)
            self._remove_project_layers_named(bn)
            self._clear_pec_report_cache()
            self._pipeline_reset_timestamps_from_ordem(6)
            self.log_message(
                self.tr(
                    'Buffers e PEC serão refeitos: camada de buffers limpa.'
                ),
                'INFO',
            )
            return

        if restart == 'correspondencia_linhas':
            self._clear_gpkg_vector_layer_features(mn)
            self._clear_gpkg_vector_layer_features(bn)
            self._remove_project_layers_named(mn, bn)
            self.dic_match = {}
            self._panel_stats_cache['ext_match'] = ''
            self._panel_stats_cache['pair_nr'] = ''
            self._refresh_extent_and_pairs_labels()
            self._persist_panel_stats_to_mdepa()
            self._clear_pec_report_cache()
            self._pipeline_reset_timestamps_from_ordem(5)
            self.log_message(
                self.tr(
                    'Correspondência e buffers serão refeitos: linhas de correspondência e buffers limpos; '
                    'pares e extensão da amostra repostos.'
                ),
                'INFO',
            )
            return

        if restart == 'morfologia_referencia':
            for nm in morph_names:
                self._clear_gpkg_vector_layer_features(nm)
            self._clear_gpkg_vector_layer_features(mn)
            self._clear_gpkg_vector_layer_features(bn)
            self._remove_project_layers_named(*morph_names, mn, bn)
            self.dic_match = {}
            for key_ in (0, 1):
                self.dic_prj['dems'][key_].pop('model', None)
            self._panel_stats_cache['ext_match'] = ''
            self._panel_stats_cache['pair_nr'] = ''
            self._refresh_extent_and_pairs_labels()
            self._persist_panel_stats_to_mdepa()
            self._clear_pec_report_cache()
            self._pipeline_reset_timestamps_from_ordem(3)
            self.log_message(
                self.tr(
                    'Morfologia e etapas seguintes serão refeitas: camadas de morfologia, '
                    'linhas de correspondência e buffers foram limpos; pares e extensão da amostra repostos.'
                ),
                'INFO',
            )
            return

        # restart is None — reprocessamento completo desde limites/interseção
        self._clear_features_from_limit_layers()
        for nm in morph_names:
            self._clear_gpkg_vector_layer_features(nm)
        self._clear_gpkg_vector_layer_features(mn)
        self._clear_gpkg_vector_layer_features(bn)
        self._remove_project_layers_named(*morph_names, mn, bn)
        self.dic_match = {}
        for key_ in (0, 1):
            self.dic_prj['dems'][key_]['geom_status'] = False
            self.dic_prj['dems'][key_].pop('model', None)
        self._reset_panel_stats_ui()
        self._persist_panel_stats_to_mdepa()
        self._clear_pec_report_cache()
        self._pipeline_reset_timestamps_from_ordem(1)
        self.log_message(
            self.tr(
                'Reprocessamento completo: limites, morfologia, correspondência e buffers '
                'foram limpos; estatísticas do painel repostas.'
            ),
            'INFO',
        )

    def check_prj_folder(self, project_file):
        """Atualiza rótulos conforme o ficheiro de projeto (.pa.gpkg) existe."""
        project_file = project_file or self.dic_prj.get('project_file')
        if not project_file:
            return
        if not self.dic_prj.get('path'):
            self.set_project_paths(project_file)
        data_dir = self.dic_prj['path']
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError as e:
            self.log_message(
                self.tr('Não foi possível criar pasta de dados: {0} ({1})').format(data_dir, e), 'ERROR')
            return

        if os.path.isfile(project_file):
            self.lb_status_proj.setText(self.tr('Projeto OK'))
            self.lb_status_proj.setStyleSheet('color: green;')
            self.dic_prj['status'] = 1
        else:
            self.lb_status_proj.setText(self.tr('Arquivo .pa.gpkg ausente'))
            self.lb_status_proj.setStyleSheet('color: red;')
            self.dic_prj['status'] = 0

    def _log_grass_provider_check(self):
        """Verifica provider GRASS no Processing e regista resultado explícito no log."""
        info = inspect_grass_processing()
        self.log_message(self.tr('=== Verificação GRASS (morfologia do terreno) ==='), 'INFO')

        if not info['provider_id']:
            self.log_message(
                self.tr(
                    'GRASS: provider não encontrado no Processing. '
                    'Instale o GRASS GIS (ex.: OSGeo4W com componente GRASS) ou reinstale o QGIS com suporte GRASS.'
                ),
                'ERROR',
            )
            return False

        self.log_message(
            self.tr('GRASS: provider «{0}» — {1}.').format(
                info['provider_id'],
                info['provider_long_name'] or info['provider_id'],
            ),
            'INFO',
        )

        if not info['provider_can_activate']:
            self.log_message(
                self.tr(
                    'GRASS: provider não pode ser ativado (GRASS não instalado ou dependências em falta no sistema).'
                ),
                'ERROR',
            )
            return False

        if not info['provider_active']:
            self.log_message(
                self.tr(
                    'GRASS: provider DESATIVADO. '
                    'Ative em Configurações → Opções → Processamento → aba Providers → marque «GRASS GIS». '
                    'Sem isso, a morfologia (cumeadas e hidrografia numérica) não executará.'
                ),
                'ERROR',
            )
            return False

        missing = [name for name, algo_id in info['algorithms'].items() if not algo_id]
        if missing:
            self.log_message(
                self.tr('GRASS ativo, mas algoritmos indisponíveis: {0}.').format(', '.join(missing)),
                'ERROR',
            )
            return False

        resolved = ', '.join(
            f'{name} ({algo_id})' for name, algo_id in info['algorithms'].items()
        )
        self.log_message(
            self.tr('GRASS OK — morfologia pode executar. Algoritmos: {0}.').format(resolved),
            'INFO',
        )
        return True

    def log_message(self, message: str, level: str = "INFO"):
        """
        Appends a new log message with a timestamp and color coding.
        """
        timestamp = QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss")
        if message:
            log_entry = f"[{timestamp}] [{level}]\n  {message}\n"
        else:
            log_entry = f""

        # Determine the color based on the log level
        if level == "INFO":
            color = QColor("black")
        elif level == "WARNING":
            color = QColor("darkorange")
        elif level == "ERROR":
            color = QColor("red")
        else:
            color = QColor("gray")

        # Apply color to the text
        cursor = self.pte_log.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        cursor.insertText(log_entry)

        # Set format for the newly inserted text
        format = QTextCharFormat()
        format.setForeground(QBrush(color))
        cursor.movePosition(cursor.MoveOperation.StartOfLine, cursor.MoveMode.KeepAnchor)
        cursor.mergeCharFormat(format)

        # Scroll to the bottom automatically
        self.pte_log.verticalScrollBar().setValue(self.pte_log.verticalScrollBar().maximum())

        data_dir = self.dic_prj.get('path')
        if not data_dir:
            return
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError:
            return
        dic_st = self.dic_prj["standard"]
        log_path = os.path.join(data_dir, f'{dic_st["name"]}{dic_st["files"]["log"]}')
        with open(log_path, "a", encoding='utf-8', errors='replace') as file:
            file.write(log_entry)

    def log_dem_layer_info(self, key_: int):
        cbx = self.dic_prj['dems'][key_].get('obj_cbx')
        if not cbx:
            return
        layer_ = cbx.currentLayer()
        if not layer_ or not layer_.isValid():
            self.log_message(
                self.tr('MDE ({0}) NÃO DEFINIDO').format(self.dic_prj['dems'][key_]['type']),
                'ERROR')
            return

        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('  INFORMAÇÕES DO MDE — {0}\n').format(
            self.dic_prj['dems'][key_]['type'].upper())
        mss_ += self.tr('  Nome da camada: {0}\n').format(layer_.name())
        mss_ += self.tr('  Caminho da fonte: {0}\n').format(layer_.source())
        mss_ += self.tr('  Válida: {0}\n').format(layer_.isValid())
        mss_ += self.tr('  SRC: {0}\n').format(layer_.crs().authid())
        mss_ += self.tr('  Largura (px): {0}\n').format(layer_.width())
        mss_ += self.tr('  Altura (px): {0}\n').format(layer_.height())
        mss_ += self.tr('  Número de bandas: {0}\n').format(layer_.bandCount())
        mss_ += self.tr('  Extensão: {0}\n').format(layer_.extent().snappedToGrid(0.001))
        mss_ += self.tr('  Tamanho do pixel X: {0:.3f}\n').format(layer_.rasterUnitsPerPixelX())
        mss_ += self.tr('  Tamanho do pixel Y: {0:.3f}\n').format(layer_.rasterUnitsPerPixelY())
        mss_ += self.tr('=======================================\n')
        self.log_message(mss_, 'INFO')

    def task_done(self, key_):
        """ Called when a thread finishes processing, allowing another to start """
        self.threads_running -= 1  # Reduce active thread count
        if key_ in self.active_workers:
            del self.active_workers[key_]  # Remove from active workers

        # Start next task if there are pending tasks in the queue
        if not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    def start_task(self, key_, dic_):
        """ Start a worker task and track it """
        worker = Worker(key_, dic_, self)
        worker.finished.connect(self.task_done)  # Connect finished signal
        self.active_workers[key_] = worker
        worker.start()  # Start processing
        self.threads_running += 1

    def unload_cleanup(self):
        """Para workers, esvazia fila e fecha diálogos antes do unload / Plugin Reloader."""
        if getattr(self, 'settings_dlg', None):
            try:
                self.settings_dlg.reject()
            except Exception:
                try:
                    self.settings_dlg.close()
                except Exception:
                    pass
        for key_ in list(self.active_workers.keys()):
            worker = self.active_workers.pop(key_, None)
            if not worker:
                continue
            th = getattr(worker, 'process_thread', None)
            if th is not None:
                try:
                    th.sig_status.disconnect(self.update_bar)
                except TypeError:
                    try:
                        th.sig_status.disconnect()
                    except TypeError:
                        pass
            try:
                worker.finished.disconnect(self.task_done)
            except TypeError:
                try:
                    worker.finished.disconnect()
                except TypeError:
                    pass
            worker.stop()
        while True:
            try:
                self.task_queue.get_nowait()
            except Empty:
                break
        self.threads_running = 0

    def exec_analyze(self):
        if not self.dic_prj.get('project_file'):
            self.log_message(
                self.tr('Defina o projeto (.pa.gpkg): menu ⋯ → Abrir ou Novo.'), 'ERROR')
            return
        pf = self.dic_prj['project_file']
        if not os.path.isfile(pf):
            self.log_message(self.tr('O arquivo .pa.gpkg do projeto não existe.'), 'ERROR')
            return

        layer_ref = self.dic_prj['dems'][0]['obj_cbx'].currentLayer()
        layer_test = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
        if not isinstance(layer_ref, QgsRasterLayer) or not layer_ref.isValid():
            self.log_message(
                self.tr('Selecione o MDE de referência (DEM válido).'), 'ERROR')
            return
        if not isinstance(layer_test, QgsRasterLayer) or not layer_test.isValid():
            self.log_message(
                self.tr('Selecione o MDE de teste (DEM válido).'), 'ERROR')
            return

        self.crs_epsg = layer_ref.crs().authid()

        if self._workflow_pause == 'post_intersection':
            self.persist_project_config_from_widgets(log_values=False)
            self._workflow_pause = None
            self._refresh_proc_button()
            self.define_morphology(0)
            return
        if self._workflow_pause == 'post_pairs_review':
            self.persist_project_config_from_widgets(log_values=False)
            self._workflow_pause = None
            self._refresh_proc_button()
            self.define_buffers()
            return

        if self.threads_running > 0 or len(self.active_workers) > 0:
            self.persist_project_config_from_widgets(log_values=False)
            self.log_message(
                self.tr('Aguarde o fim da análise em curso antes de nova avaliação.'), 'WARNING')
            return

        # Comparar com o estado gravado **antes** de persistir no .pa.gpkg; senão não há baseline
        # e só o snapshot do PEC concluído faria retomada parcial (mudar só match parecia «do zero»).
        dlg = self.settings_dlg
        dlg.flush_widgets_to_dic_param(log_values=False)
        flat_now = self._flatten_run_snapshot()
        flat_was_pec = load_pipeline_last_ok_snapshot(pf)
        if flat_was_pec:
            flat_was = flat_was_pec
        else:
            flat_was = build_flat_snapshot_from_mdepa_stored_settings(pf)
            if not any(k.startswith('step_') for k in flat_was):
                flat_was = {}
        restart, _reason = compute_restart_etapa_from_snapshots(flat_now, flat_was)
        if restart == '__noop__':
            # Projeto novo: defaults gravados no .pa.gpkg coincidem com os widgets → diff vazio, mas
            # não há PEC concluído nem etapa terminada: há que arrancar a cadeia, não mostrar «inalterado».
            if flat_was_pec or pipeline_has_completed_any_etapa(pf):
                self.persist_project_config_from_widgets(log_values=False)
                self.log_message(
                    self.tr(
                        'Parâmetros e MDEs inalterados (última avaliação concluída ou configuração gravada no projeto).'),
                    'INFO',
                )
                return
            restart = None

        self.persist_project_config_from_widgets(log_values=False)

        self.create_gpkg()
        self._sanitize_pipeline_for_restart_immediate(restart)

        if restart is None:
            self.log_message(
                self.tr('Reprocessamento completo desde polígonos de limite e interseção.'), 'INFO')
            if self.cbx_workflow_study.currentIndex() == 2:
                if not self.apply_study_area_from_map_layer():
                    return
                return
            self.define_intersection()
        elif restart == 'morfologia_referencia':
            self.log_message(self.tr('Retomando a partir da morfologia (parâmetros alterados).'), 'INFO')
            self.define_morphology(0)
        elif restart == 'correspondencia_linhas':
            self.log_message(
                self.tr('Retomando a partir da correspondência de linhas (parâmetros alterados).'), 'INFO')
            self.matching_lines()
        elif restart == 'buffers':
            self.log_message(
                self.tr('Retomando a partir dos buffers (parâmetros alterados).'), 'INFO')
            if not self.dic_match:
                self.matching_lines()
            else:
                self.define_buffers()
        else:
            self.define_intersection()

    def run_polygon_intersection(self):
        status_0 = self.dic_prj['dems'][0]['geom_status']
        status_1 = self.dic_prj['dems'][1]['geom_status']
        if status_0 and status_1:
            mss_ = self.tr('CALCULANDO ÁREA DE INTERSEÇÃO DOS MDEs')
            self.log_message(mss_, 'INFO')

            layer_0 = self.get_gpkg_layer(prefix_= f'__Limit_{self.dic_prj["dems"][0]["type"]}__')
            layer_1 = self.get_gpkg_layer(prefix_= f'__Limit_{self.dic_prj["dems"][1]["type"]}__')
            layer_i = self.get_gpkg_layer(prefix_= self.intersection_name)

            layer_i.startEditing()
            ids_i = [f.id() for f in layer_i.getFeatures()]
            if ids_i:
                layer_i.deleteFeatures(ids_i)
            layer_i.commitChanges()

            sum_area_m2 = 0.0
            crs_i = layer_i.crs()

            for feat_0 in layer_0.getFeatures():
                geom_0 = feat_0.geometry()
                for feat_1 in layer_1.getFeatures():
                    geom_1 = feat_1.geometry()
                    intersec_ = geom_0.intersection(geom_1)
                    area_m2 = geometry_area_square_meters(intersec_, crs_i)
                    sum_area_m2 += area_m2
                    feat_i = QgsFeature()
                    feat_i.setGeometry(intersec_)
                    count = layer_i.featureCount()
                    # Campo AREA: sempre metros quadrados (etiquetas no mapa: ver estilo __Limit_Intersecao__)
                    feat_i.setAttributes([count + 1, area_m2])
                    layer_i.startEditing()
                    layer_i.addFeature(feat_i)
                    layer_i.commitChanges()
                    layer_i.updateExtents()
                    layer_i.triggerRepaint()
            sum_area_km2 = sum_area_m2 / 1_000_000.0
            area_disp = round(sum_area_km2, 4)
            self.lb_area.setText(self.dic_lb_texts['area'].format(area_disp))
            # ext_m = 2.0176 * (sum_area_m2 ** 0.5478)
            ext_km = round((2.0176 * (sum_area_km2 ** 0.5478)), 1)
            self.lb_ext_min.setText(self.dic_lb_texts['ext_min'].format(ext_km))
            self._panel_stats_cache['area'] = str(area_disp)
            self._panel_stats_cache['ext_min'] = str(ext_km)
            self._persist_panel_stats_to_mdepa()
            mss_ = self.tr('ÁREA DE INTERSEÇÃO DOS MDEs DEFINIDA\n')
            mss_ += self.tr('=======================================\n')
            self.log_message(mss_, 'INFO')
            if self.cbx_workflow_study.currentIndex() == 1:
                self._workflow_pause = 'post_intersection'
                self._refresh_proc_button()
                self.log_message(
                    self.tr(
                        'Edite a camada de interseção se necessário e prima Continuar para morfologia.'),
                    'INFO')
            else:
                self.define_morphology(0)

    def ensure_pipeline_etapas_table(self, gpkg_path=None):
        """Cria tabela de etapas e insere linhas previstas (idempotente). Sem SpatiaLite — só SQLite."""
        path = gpkg_path or self.gpkg_path
        if not path or not os.path.isfile(path):
            return False
        try:
            conn = sqlite3.connect(path)
            try:
                # inicio/fim: data e hora local armazenada como TEXT no formato PIPELINE_DATETIME_FMT
                conn.execute(
                    f'''CREATE TABLE IF NOT EXISTS {PIPELINE_ETAPAS_TABLE} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ordem INTEGER NOT NULL UNIQUE,
                        etapa TEXT NOT NULL UNIQUE,
                        inicio TEXT DEFAULT NULL,
                        fim TEXT DEFAULT NULL
                    )'''
                )
                _ensure_pa_settings_table_conn(conn)
                for old_n, new_n in (
                    ('raster_intersecao', 'dem_intersecao'),
                    ('matching_linhas', 'correspondencia_linhas'),
                ):
                    row_new = conn.execute(
                        f'SELECT 1 FROM {PIPELINE_ETAPAS_TABLE} WHERE etapa = ?', (new_n,)
                    ).fetchone()
                    if row_new:
                        conn.execute(
                            f'DELETE FROM {PIPELINE_ETAPAS_TABLE} WHERE etapa = ?', (old_n,))
                    else:
                        conn.execute(
                            f'UPDATE {PIPELINE_ETAPAS_TABLE} SET etapa = ? WHERE etapa = ?',
                            (new_n, old_n),
                        )
                for ordem, etapa in PIPELINE_ETAPAS_DEF:
                    conn.execute(
                        f'''INSERT OR IGNORE INTO {PIPELINE_ETAPAS_TABLE}
                            (ordem, etapa, inicio, fim) VALUES (?, ?, NULL, NULL)''',
                        (ordem, etapa),
                    )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            return False
        return True

    def pipeline_set_etapa_inicio(self, etapa: str, gpkg_path=None, quando=None):
        """Grava data/hora de início da etapa (padrão: agora local)."""
        path = gpkg_path or self.gpkg_path
        if not path or not os.path.isfile(path):
            return False
        ts = quando if quando is not None else pipeline_datetime_now_local()
        try:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    f'UPDATE {PIPELINE_ETAPAS_TABLE} SET inicio = ? WHERE etapa = ?',
                    (ts, etapa),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            return False
        return True

    def pipeline_set_etapa_fim(self, etapa: str, gpkg_path=None, quando=None):
        """Grava data/hora de fim da etapa (padrão: agora local)."""
        path = gpkg_path or self.gpkg_path
        if not path or not os.path.isfile(path):
            return False
        ts = quando if quando is not None else pipeline_datetime_now_local()
        try:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    f'UPDATE {PIPELINE_ETAPAS_TABLE} SET fim = ? WHERE etapa = ?',
                    (ts, etapa),
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            return False
        return True

    def create_gpkg(self):
        """Garante ficheiro de projeto, tabelas auxiliares, tabelas de limite e carrega-as no mapa a partir do GPKG."""
        if not self.gpkg_path:
            self.log_message(self.tr('Caminho do projeto (.pa.gpkg) indefinido.'), 'ERROR')
            return
        data_dir = self.dic_prj.get('path')
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
        if not os.path.isfile(self.gpkg_path):
            self.log_message(self.tr('O arquivo .pa.gpkg do projeto não existe.'), 'ERROR')
            return
        if not self.ensure_pipeline_etapas_table():
            self.log_message(self.tr('Falha ao garantir tabelas auxiliares no .pa.gpkg.'), 'ERROR')
            return
        if not self._ensure_limit_layers_for_analysis():
            self.log_message(self.tr('Falha ao preparar camadas de limite no projeto.'), 'ERROR')

    def gpkg_conn(self, gpkg_path_=''):
        if not gpkg_path_:
            gpkg_path_ = self.gpkg_path
        conn_ = sqlite3.connect(gpkg_path_)  # , isolation_level=None)
        conn_.row_factory = sqlite3.Row
        conn_.enable_load_extension(True)
        conn_.load_extension('mod_spatialite')
        conn_.execute('SELECT load_extension("mod_spatialite")')
        conn_.execute('pragma journal_mode=wal')
        # cur_ = conn_.cursor()
        return conn_

    def gpkg_close_conn(self, conn_=None, cur_=None):
        if conn_:
            conn_.close()
        if cur_:
            cur_.close()

    def get_gpkg_layer(self, prefix_='', gpkg_path='', show=True):
        if not gpkg_path:
            gpkg_path = self.gpkg_path
        gpkg_path = os.path.normpath(gpkg_path)
        self.node_group = self._plugin_layer_tree_group()

        conn = None
        layer_ = None
        if prefix_:
            layer_ = self._find_map_layer_for_project(prefix_, gpkg_path)
            if layer_:
                return layer_

            conn = self.gpkg_conn(gpkg_path)
            uri_ = gpkg_layer_uri(gpkg_path, prefix_)
            display_name = self._layer_display_name(prefix_)

            layer_ = QgsVectorLayer(uri_, display_name, 'ogr')
            conn.commit()
            style_path = os.path.join(plugin_path, r'styles', f'{prefix_}.qml')
            layer_.loadNamedStyle(style_path)
            layer_.triggerRepaint()
            if show:
                QgsProject.instance().addMapLayer(layer_, False)
                layer_node = QgsLayerTreeLayer(layer_)
                self.node_group.insertChildNode(0, layer_node)
        self.gpkg_close_conn(conn)

        return layer_

    def define_intersection(self):
        for key_ in self.dic_prj['dems']:
            self.dic_prj['dems'][key_]['geom_status'] = False
        self._clear_features_from_limit_layers()
        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('DEFININDO POLÍGONOS')
        self.log_message(mss_, 'INFO')
        for key_ in self.dic_prj['dems']:
            layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()
            dic_ = {
                'file_path': layer_.source(),
                'step': 'polygon',
                'srid_ref': self.crs_epsg,
                'srid': layer_.crs().authid(),
                'gpkg':self.gpkg_path,
                'layer':  f'__Limit_{self.dic_prj["dems"][0]["type"]}__',
                'parent': self,
                'main': self.main
            }

            # Add tasks to queue
            self.task_queue.put((key_, dic_))

        # Start up to max_threads tasks
        while self.threads_running < self.max_threads and not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    def define_morphology(self, key_=0):
        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('DEFININDO ELEMENTOS DE MORFOLOGIA DO TERRENO - {0}').format(
            self.dic_prj['dems'][key_]['type'])
        self.log_message(mss_, 'INFO')
        if not self._log_grass_provider_check():
            self.log_message(
                self.tr(
                    'Morfologia cancelada: GRASS indisponível ou desativado. '
                    'Corrija antes de continuar (ver mensagens acima).'
                ),
                'ERROR',
            )
            return
        dic_param_morphology = self.settings_dlg.dic_param['step_morfologia']['fields']
        mem_status = _windows_memory_status()
        configured_grass_gb = float(dic_param_morphology['max_memo_grass']['value'])
        if mem_status:
            self.log_message(
                self.tr(
                    'RAM do sistema: {0}% em uso, {1} MB livres de {2} MB.'
                ).format(
                    mem_status['load_pct'],
                    mem_status['avail_mb'],
                    mem_status['total_mb'],
                ),
                'WARNING' if mem_status['load_pct'] >= 85 else 'INFO',
            )
            advice = _grass_memory_advice(mem_status, configured_grass_gb)
            if advice:
                level = 'WARNING' if (
                    mem_status['load_pct'] >= 85 or configured_grass_gb > _recommend_grass_memory_gb(mem_status)
                ) else 'INFO'
                self.log_message(advice, level)
            if mem_status['load_pct'] >= 85:
                self.log_message(
                    self.tr(
                        'RAM elevada antes da morfologia — o r.watershed (GRASS) pode falhar. '
                        'Feche outras aplicações, reinicie o QGIS se necessário, '
                        'ou reduza «Limite de Memória para Grass GIS» nas definições do plugin.'
                    ),
                    'WARNING',
                )
        layer_ = self.dic_prj['dems'][key_]['obj_cbx'].currentLayer()
        src_path = (layer_.source() or '') + ' ' + (self.dic_prj.get('project_file') or '')
        if 'onedrive' in src_path.lower() or 'área de trabalho' in src_path.lower() or 'area de trabalho' in src_path.lower():
            self.log_message(
                self.tr(
                    'AVISO: projeto/MDE sob OneDrive ou «Área de Trabalho» (caminho com acentos). '
                    'Isto causa falhas intermitentes no GRASS no Windows. '
                    'Copie o projeto e os rasters para um caminho local sem acentos '
                    '(ex.: C:\\dados\\mdepa\\) e pause a sincronização do OneDrive durante o processamento.'
                ),
                'WARNING',
            )
        gsd_ = layer_.rasterUnitsPerPixelX()
        max_px = int(float(dic_param_morphology['max_basin_area']['value']) / (gsd_ ** 2))
        dic_ = {
            'file_path': layer_.source(),
            'step': 'morphology',
            'srid_ref': self.crs_epsg,
            'srid': layer_.crs().authid(),
            'gpkg':self.gpkg_path,
            'layer':  self.get_gpkg_layer(prefix_= self.intersection_name).source(),
            'max_px': max_px,
            'max_memo': float(dic_param_morphology['max_memo_grass']['value']),
            'morph_names':self.list_morph,
            'gsd': gsd_,
            'parent': self,
            'main': self.main
        }

        # Add tasks to queue
        self.task_queue.put((key_, dic_))

        # Start up to max_threads tasks
        while self.threads_running < self.max_threads and not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    @staticmethod
    def _line_midpoint_xy(geom: QgsGeometry):
        """Ponto ao longo da linha a meio do comprimento (2D)."""
        if geom is None or geom.isEmpty() or geom.type() != QgsWkbTypes.LineGeometry:
            return None
        L = geom.length()
        if L <= 0:
            return None
        g2 = QgsGeometry(geom)
        mid = g2.interpolate(L / 2.0)
        if mid is None or mid.isEmpty():
            return None
        return mid.asPoint()

    def _sync_match_lines_layer_from_dic_match(self) -> bool:
        """Grava no GPKG linhas teste→referência (meio a meio) e metadados dos pares."""
        if not self.gpkg_path or not os.path.isfile(self.gpkg_path):
            return False
        if not self.dic_match:
            return False
        crs = QgsCoordinateReferenceSystem(self.crs_epsg or 'EPSG:4326')
        if not crs.isValid():
            crs = QgsCoordinateReferenceSystem('EPSG:4326')
        crs_s = crs.authid()
        type_r = self.dic_prj['dems'][0]['type']
        type_t = self.dic_prj['dems'][1]['type']
        mem = QgsVectorLayer(
            f'LineString?crs={crs_s}&index=yes', self.match_lines_layer_name, 'memory')
        sch = QgsFields()
        sch.append(QgsField('tipo', QVariant.String, len=64))
        sch.append(QgsField('fid_r', QVariant.Int))
        sch.append(QgsField('fid_t', QVariant.Int))
        sch.append(QgsField('dist_m', QVariant.Double))
        sch.append(QgsField('per_r', QVariant.Double))
        sch.append(QgsField('len_r', QVariant.Double))
        pr_ = mem.dataProvider()
        pr_.addAttributes(sch)
        mem.updateFields()
        mem.startEditing()
        n_ok = 0
        for tag_ in self.dic_match:
            layer_r = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_r}__')
            layer_t = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_t}__')
            if layer_r is None or layer_t is None or not layer_r.isValid() or not layer_t.isValid():
                self.log_message(
                    self.tr('[__Linhas_de_Correspondencia__] Camadas de morfologia indisponíveis para tipo {0}.').format(
                        tag_), 'WARNING')
                continue
            for vet_ in self.dic_match[tag_]:
                if len(vet_) < 5:
                    continue
                try:
                    fid_r = int(vet_[0])
                    fid_t = int(vet_[1])
                except (TypeError, ValueError):
                    continue
                fr = layer_r.getFeature(fid_r)
                ft = layer_t.getFeature(fid_t)
                if not fr.hasGeometry() or not ft.hasGeometry():
                    continue
                pt_t = self._line_midpoint_xy(QgsGeometry(ft.geometry()))
                pt_r = self._line_midpoint_xy(QgsGeometry(fr.geometry()))
                if pt_t is None or pt_r is None:
                    continue
                line_g = QgsGeometry.fromPolylineXY([pt_t, pt_r])
                if line_g.isEmpty():
                    continue
                feat = QgsFeature(mem.fields())
                feat.setGeometry(line_g)
                try:
                    dist_m = float(vet_[2])
                    per_r = float(vet_[3])
                    len_r = float(vet_[4])
                except (TypeError, ValueError, IndexError):
                    dist_m, per_r, len_r = 0.0, 0.0, fr.geometry().length()
                feat.setAttributes([str(tag_), fid_r, fid_t, dist_m, per_r, len_r])
                mem.addFeature(feat)
                n_ok += 1
        mem.commitChanges()
        if n_ok == 0:
            self.log_message(
                self.tr('[__Linhas_de_Correspondencia__] Nenhuma linha de ligação foi criada.'), 'WARNING')
            return False
        opt = QgsVectorFileWriter.SaveVectorOptions()
        opt.driverName = 'GPKG'
        opt.layerName = self.match_lines_layer_name
        opt.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        try:
            QgsVectorFileWriter.writeAsVectorFormat(
                layer=mem,
                fileName=self.gpkg_path,
                options=opt,
            )
        except Exception as e:
            self.log_message(
                self.tr('[__Linhas_de_Correspondencia__] Falha ao gravar no GPKG: {0}').format(e), 'ERROR')
            return False
        normalize_project_pa_file(self.gpkg_path)
        proj = QgsProject.instance()
        gpkg_n = self._active_gpkg_path()
        for lyr in list(proj.mapLayersByName(self.match_lines_layer_name)):
            if map_layer_gpkg_path(lyr) == gpkg_n:
                try:
                    proj.removeMapLayer(lyr.id())
                except Exception:
                    pass
        for lyr in list(proj.mapLayersByName(self._layer_display_name(self.match_lines_layer_name))):
            if map_layer_gpkg_path(lyr) == gpkg_n:
                try:
                    proj.removeMapLayer(lyr.id())
                except Exception:
                    pass
        self.get_gpkg_layer(prefix_=self.match_lines_layer_name, gpkg_path=self.gpkg_path)
        self.log_message(
            self.tr('[__Linhas_de_Correspondencia__] {0} ligações gravadas (edite antes de Continuar se estiver em revisão).').format(
                n_ok), 'INFO')
        return True

    def _dic_match_from_match_lines_layer(self):
        """Reconstrói dic_match a partir da camada editável de correspondência (GPKG)."""
        lyr = self._resolve_limit_layer_for_editing(self.match_lines_layer_name)
        if lyr is None or not lyr.isValid():
            return None
        if lyr.featureCount() == 0:
            return {}
        type_r = self.dic_prj['dems'][0]['type']
        type_t = self.dic_prj['dems'][1]['type']
        out = {}
        n_skip = 0
        for feat in lyr.getFeatures():
            if not feat.hasGeometry():
                n_skip += 1
                continue
            tipo = feat.attribute('tipo')
            if tipo is None or str(tipo).strip() == '':
                n_skip += 1
                continue
            tag_ = str(tipo).strip()
            try:
                fid_r = int(feat.attribute('fid_r'))
                fid_t = int(feat.attribute('fid_t'))
            except (TypeError, ValueError):
                n_skip += 1
                continue
            layer_r = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_r}__')
            layer_t = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_t}__')
            if layer_r is None or layer_t is None or not layer_r.isValid() or not layer_t.isValid():
                n_skip += 1
                continue
            fr = layer_r.getFeature(fid_r)
            ft = layer_t.getFeature(fid_t)
            if not fr.hasGeometry() or not ft.hasGeometry():
                n_skip += 1
                continue
            gr = QgsGeometry(fr.geometry())
            gt = QgsGeometry(ft.geometry())
            len_r = feat.attribute('len_r')
            try:
                len_r = float(len_r)
            except (TypeError, ValueError):
                len_r = gr.length()
            dist_m = feat.attribute('dist_m')
            try:
                dist_m = float(dist_m)
            except (TypeError, ValueError):
                dist_m = round(gr.distance(gt), 2)
            per_r = feat.attribute('per_r')
            try:
                per_r = float(per_r)
            except (TypeError, ValueError):
                per_r = 0.0
            row = [fid_r, fid_t, round(dist_m, 2), round(per_r, 2), float(len_r)]
            out.setdefault(tag_, []).append(row)
        if n_skip:
            self.log_message(
                self.tr('[__Linhas_de_Correspondencia__] {0} feição(ões) ignoradas.').format(n_skip), 'WARNING')
        return out

    @staticmethod
    def _float_from_panel_str(val):
        try:
            if val is None or str(val).strip() == '':
                return None
            return float(val)
        except (TypeError, ValueError):
            return None

    def _total_sample_extent_m_from_dic_match(self, dm=None) -> float:
        """Soma dos comprimentos da linha de referência por par (último valor da linha = LEN em m do CRS)."""
        dm = dm if dm is not None else getattr(self, 'dic_match', None) or {}
        s = 0.0
        for key_ in dm:
            for vet_ in dm[key_]:
                if not vet_:
                    continue
                try:
                    s += float(vet_[-1])
                except (TypeError, ValueError, IndexError):
                    continue
        return s

    @staticmethod
    def _morph_tag_from_layer_name(layer_name) -> str:
        """Extrai morfologia de nomes como __Cumeada_Z_ref__ ou __Cumeada_Z_ref@projeto__."""
        if not layer_name:
            return ''
        s = str(layer_name).strip()
        if s.startswith('__') and s.endswith('__'):
            s = s[2:-2]
        if '@' in s:
            s = s.split('@', 1)[0]
        parts = s.split('_Z_')
        return parts[0] if parts else ''

    def _match_pair_extent_m(self, rec) -> float:
        """Comprimento (m) da linha de referência do par — alinhado a dic_match / extent_ref."""
        ext = _coerce_finite_extent_m(rec.get('extent_ref'))
        if ext is not None:
            return ext
        try:
            fid_r = int(rec.get('fid_r'))
            fid_t = int(rec.get('fid_t'))
        except (TypeError, ValueError):
            return 0.0
        tag_ = (str(rec.get('morph_tag') or '').strip()
                or self._morph_tag_from_layer_name(rec.get('layer_r') or ''))
        dm = getattr(self, 'dic_match', None) or {}
        rows = dm.get(tag_) or []
        for vet_ in rows:
            try:
                if int(vet_[0]) == fid_r and int(vet_[1]) == fid_t:
                    len_m = float(vet_[-1])
                    if math.isfinite(len_m) and len_m > 0:
                        return len_m
            except (TypeError, ValueError, IndexError):
                continue
        for vet_ in rows:
            try:
                if int(vet_[0]) == fid_r:
                    len_m = float(vet_[-1])
                    if math.isfinite(len_m) and len_m > 0:
                        return len_m
            except (TypeError, ValueError, IndexError):
                continue
        return 0.0

    @staticmethod
    def _is_statistical_outlier_rec(rec, outlier_key, dm_key):
        """Outlier IQR com medição válida (dm nulo/indefinido não conta como outlier estatístico)."""
        if not rec.get(outlier_key):
            return False
        return _coerce_finite_measurement_scalar(rec.get(dm_key)) is not None

    def _resolve_ext_min_km_sample_gate(self):
        """Extensão mínima (km) para o gate antes dos buffers.

        Prioriza o valor gravado no .pa.gpkg (`panel_stats.ext_min`), igual ao que o painel
        mostra após abrir o projeto; o cache em memória só entra se o ficheiro ainda não
        tiver valor válido (ex.: interseção acabada de calcular na mesma sessão).
        Devolve None se não houver limite (fluxos antigos, sem interseção, ou ≤ 0).
        """
        pf = self.dic_prj.get('project_file')
        disk_raw = ''
        disk_val = None
        if pf and os.path.isfile(pf):
            data = load_panel_stats_from_mdepa_path(pf)
            disk_raw = (data.get('ext_min') or '').strip()
            disk_val = self._float_from_panel_str(disk_raw)
        cache_raw = (self._panel_stats_cache.get('ext_min') or '').strip()
        cache_val = self._float_from_panel_str(cache_raw)

        if disk_val is not None and disk_val > 0:
            if cache_raw != disk_raw:
                self._panel_stats_cache['ext_min'] = disk_raw
                self.lb_ext_min.setText(
                    self.dic_lb_texts['ext_min'].format(disk_raw or '—'))
            return disk_val
        if cache_val is not None and cache_val > 0:
            return cache_val
        return None

    def _check_sample_extent_vs_minimum(self) -> bool:
        """False interrompe antes dos buffers: extensão acumulada da amostra < extensão mínima recomendada."""
        ext_min_km = self._resolve_ext_min_km_sample_gate()
        if ext_min_km is None:
            return True
        dm = getattr(self, 'dic_match', None) or {}
        n_pairs = sum(len(dm[k]) for k in dm)
        ext_m = self._total_sample_extent_m_from_dic_match(dm)
        ext_km = ext_m / 1000.0
        if n_pairs == 0:
            self.log_message(
                self.tr(
                    'Não há pares homólogos válidos. O processamento foi interrompido antes dos buffers.\n\n'
                    'Sugestões:\n'
                    '• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas.\n'
                    '• Afrouxar a correspondência: aumentar a distância máxima entre centróides (pixels do MDE de teste) '
                    'e o percentual de diferença de área entre os envelopes mínimos.'),
                'ERROR',
            )
            return False
        if ext_km < ext_min_km:
            self.log_message(
                self.tr(
                    'A extensão total da amostra ({0} km) é menor que a extensão mínima recomendada ({1} km). '
                    'O processamento foi interrompido antes dos buffers.\n\n'
                    'Sugestões:\n'
                    '• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas e maior extensão acumulada.\n'
                    '• Afrouxar a correspondência: aumentar a distância máxima entre centróides (pixels do MDE de teste) '
                    'e o percentual de diferença de área entre os envelopes mínimos.'
                ).format(round(ext_km, 4), round(ext_min_km, 4)),
                'ERROR',
            )
            return False
        return True

    def matching_lines(self):
        conn = self.gpkg_conn()
        curs = conn.cursor()
        dic_param_match = self.settings_dlg.dic_param['step_match']['fields']
        dist_max_px = float(dic_param_match['dist_max']['value'])
        layer_test = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
        if not isinstance(layer_test, QgsRasterLayer) or not layer_test.isValid():
            self.log_message(
                self.tr('MDE de teste inválido — não é possível aplicar a distância máxima em pixels.'), 'ERROR')
            return
        gsd_test = layer_test.rasterUnitsPerPixelX()
        if not gsd_test or gsd_test <= 0:
            self.log_message(
                self.tr('GSD do MDE de teste inválido — não é possível converter pixels em distância no mapa.'), 'ERROR')
            return
        dist_max = dist_max_px * gsd_test
        area_percent = float(dic_param_match['percent_area']['value']) / 100
        type_0 = self.dic_prj["dems"][0]["type"]
        type_1 = self.dic_prj["dems"][1]["type"]
        morph_0 = self.list_morph[0]
        morph_1 = self.list_morph[1]
        sql_ = f"""
        WITH
            ct as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)) len from __{morph_0}_Z_{type_1}__),
            cr as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)) len  from __{morph_0}_Z_{type_0}__),
            ht as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)) len  from __{morph_1}_Z_{type_1}__),
            hr as (select  fid, OrientedEnvelope(GeomFromGPB(geom)) as eogeom, ST_Line_Interpolate_Point(GeomFromGPB(geom), 0.5) as centroid, ST_LENGTH(GeomFromGPB(geom)
            ) len  from __{morph_1}_Z_{type_0}__)
        SELECT 
            '{morph_0}' TIPO, 
            cr.fid fidr, 
            ct.fid fidt, 
            ROUND(ST_DISTANCE(ct.centroid, cr.centroid),2) as DIST,  
            ROUND(ABS(ST_AREA(ct.eogeom) - ST_AREA(cr.eogeom))/ ST_AREA(ct.eogeom),2) PER, 
            cr.len LEN
            FROM ct, cr
            WHERE 
                ST_DISTANCE(ct.centroid, cr.centroid) < {dist_max}
                AND (ABS(ST_AREA(ct.eogeom) - ST_AREA(cr.eogeom))/ ST_AREA(ct.eogeom)) < {area_percent}
        UNION
        SELECT 
            '{morph_1}' TIPO, 
            hr.fid fidr, 
            ht.fid fidt, 
            ROUND(ST_DISTANCE(ht.centroid, hr.centroid),2) as DIST, 
            ROUND(ABS(ST_AREA(ht.eogeom) - ST_AREA(hr.eogeom))/ ST_AREA(ht.eogeom),2) PER, 
            hr.len LEN
            FROM ht, hr
            WHERE 
                ST_DISTANCE(ht.centroid, hr.centroid) < {dist_max}
                AND (ABS(ST_AREA(ht.eogeom) - ST_AREA(hr.eogeom))/ ST_AREA(ht.eogeom)) < {area_percent}
            ORDER BY 1,4 ASC;
        """
        result_ = curs.execute(sql_)
        result_fa = result_.fetchall()
        try:
            curs.close()
            conn.close()
        except Exception:
            pass
        self.dic_match = {}
        for row_ in result_fa:
            for j, col_ in enumerate(row_):
                if j == 0:
                    tag_ = col_
                    if tag_ not in self.dic_match:
                        self.dic_match[tag_] = [[]]
                        k = 0
                    else:
                        self.dic_match[tag_].append([])
                        k += 1
                else:
                    self.dic_match[tag_][k].append(col_)

            #     print(col_,end='\t')
            # print()
        ext_sum = self._total_sample_extent_m_from_dic_match(self.dic_match)
        self.log_message(
            self.tr('Extensão total da amostra: {0} m').format(int(round(ext_sum))), 'INFO')
        n_pairs = sum(len(self.dic_match[k]) for k in self.dic_match)
        self._panel_stats_cache['ext_match'] = str(round(ext_sum, 1))
        self._panel_stats_cache['pair_nr'] = str(n_pairs)
        self._refresh_extent_and_pairs_labels()
        self._persist_panel_stats_to_mdepa()
        self._sync_match_lines_layer_from_dic_match()
        # print('dic_match', self.dic_match)
        if self.cbx_workflow_pairs.currentIndex() == 1:
            self._workflow_pause = 'post_pairs_review'
            self._refresh_proc_button()
            self.log_message(
                self.tr(
                    'Camada __Linhas_de_Correspondencia__: {0} pares. Edite, remova ou adicione linhas '
                    '(meio teste → meio referência); atributos: tipo, fid_r, fid_t. Prima Continuar.'
                ).format(n_pairs),
                'INFO')
            return
        self.define_buffers()

    def create_buffers_layer(self, show_on_map: bool = True):

        layer_0 = QgsVectorLayer(f'multipolygon?crs={self.crs_epsg}&index=yes', self.buffer_name, "memory")
        schema_ = QgsFields()
        # Double: escalas nominais (PEC) e raios CE90/LE90 em metros
        schema_.append(QgsField('scale', QVariant.Double))
        schema_.append(QgsField('class', QVariant.String))
        schema_.append(QgsField('id_origem', QVariant.Int))
        schema_.append(QgsField('camada_origem', QVariant.String))
        pr_ = layer_0.dataProvider()
        pr_.addAttributes(schema_)
        layer_0.updateFields()

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        options.layerName = self.buffer_name
        QgsVectorFileWriter.writeAsVectorFormat(
            layer=layer_0,
            fileName=self.gpkg_path,
            options=options)
        normalize_project_pa_file(self.gpkg_path)
        layer_ = self.get_gpkg_layer(
            prefix_=self.buffer_name, gpkg_path=self.gpkg_path, show=show_on_map)
        return layer_

    def _reset_buffers_layer_for_run(self) -> None:
        """Esvazia/recria __Buffers__ antes de uma nova geração (evita conflito de fid)."""
        bn = self.buffer_name
        try:
            self._clear_gpkg_vector_layer_features(bn)
        except Exception:
            pass
        try:
            self._remove_project_layers_named(bn)
        except Exception:
            pass
        self.layer_buffers = None

    def _show_buffers_on_map_setting(self) -> bool:
        """Config step_buffers: 0 = não mostrar no mapa durante processamento."""
        try:
            return int(
                self.settings_dlg.dic_param['step_buffers']['fields']['show_buffers_on_map']['value']
            ) == 1
        except (TypeError, ValueError, KeyError):
            return False

    def _ensure_buffers_layer_for_editing(self):
        if not self.layer_buffers:
            show = self._show_buffers_on_map_setting()
            self._buffers_map_deferred = not show
            self.layer_buffers = self.create_buffers_layer(show_on_map=show)
        return self.layer_buffers

    def _append_buffer_feats_batch(self, feats, *, repaint: bool = False) -> None:
        """Grava lote de geometrias na __Buffers__ (um commit por par)."""
        if not feats:
            return
        lyr = self._ensure_buffers_layer_for_editing()
        if not getattr(self, '_buffers_layer_target_logged', False):
            self._buffers_layer_target_logged = True
            self.log_message(
                self.tr(
                    '[__Buffers__] Tipo de geometria da camada: {0}. '
                    'Gravação em lote por par (sem repaint durante o processamento).'
                ).format(QgsWkbTypes.displayString(lyr.wkbType())),
                'INFO',
            )
        fields = lyr.fields()
        # Índices por nome — nunca gravar 'fid' (PK do GeoPackage)
        idx_scale = fields.indexOf('scale')
        idx_class = fields.indexOf('class')
        idx_id = fields.indexOf('id_origem')
        idx_cam = fields.indexOf('camada_origem')

        def _attrs_from_source(feat_src):
            """Extrai scale/class/id_origem/camada_origem sem reutilizar fid."""
            src = list(feat_src.attributes() or [])
            # Formatos históricos: [fid_sintetico, scale, class, id, camada] ou [scale, class, id, camada]
            if len(src) >= 5:
                scale_v, class_v, id_v, cam_v = src[1], src[2], src[3], src[4]
            elif len(src) >= 4:
                scale_v, class_v, id_v, cam_v = src[0], src[1], src[2], src[3]
            else:
                scale_v = class_v = id_v = cam_v = None
            out = [None] * fields.count()
            if idx_scale >= 0:
                out[idx_scale] = scale_v
            if idx_class >= 0:
                out[idx_class] = class_v
            if idx_id >= 0:
                out[idx_id] = id_v
            if idx_cam >= 0:
                out[idx_cam] = cam_v
            return out

        lyr.startEditing()
        n_ok, n_skip, n_add_fail = 0, 0, 0
        for feat_ in feats:
            g0 = feat_.geometry()
            g_adj = self._geometry_for_buffers_layer(g0, lyr)
            if g_adj is None:
                n_skip += 1
                continue
            feat_adj = QgsFeature(fields)
            feat_adj.setGeometry(g_adj)
            feat_adj.setAttributes(_attrs_from_source(feat_))
            if not lyr.addFeature(feat_adj):
                n_add_fail += 1
            else:
                n_ok += 1
        if n_skip or n_add_fail:
            self.log_message(
                self.tr(
                    '[__Buffers__] Lote: {0} adicionadas, {1} ignoradas (geometria), '
                    '{2} rejeitadas pelo fornecedor.'
                ).format(n_ok, n_skip, n_add_fail),
                'WARNING' if (n_skip + n_add_fail) else 'INFO',
            )
        if not lyr.commitChanges():
            errs = lyr.commitErrors()
            self.log_message(
                self.tr('[__Buffers__] commitChanges falhou:\n{0}').format(
                    '\n'.join(errs) if errs else self.tr('(sem detalhe)')),
                'ERROR',
            )
            lyr.rollBack()
        else:
            lyr.updateExtents()
            if repaint:
                lyr.triggerRepaint()

    def _begin_buffers_map_canvas_freeze(self) -> None:
        try:
            c = self.iface.mapCanvas()
            self._buffers_canvas_render_saved = c.renderFlag()
            c.setRenderFlag(False)
        except Exception:
            self._buffers_canvas_render_saved = True

    def _end_buffers_map_canvas_freeze(self, *, refresh: bool = True) -> None:
        try:
            c = self.iface.mapCanvas()
            c.setRenderFlag(getattr(self, '_buffers_canvas_render_saved', True))
            if refresh:
                c.refresh()
        except Exception:
            pass

    def _finalize_buffers_map_display(self) -> None:
        """Um repaint no fim; adiciona __Buffers__ ao mapa se estava diferido."""
        if getattr(self, '_buffers_map_deferred', False):
            self.layer_buffers = self.get_gpkg_layer(
                prefix_=self.buffer_name, gpkg_path=self.gpkg_path, show=True)
            self._buffers_map_deferred = False
        lyr = self.layer_buffers
        if lyr is not None and lyr.isValid():
            lyr.updateExtents()
        self._end_buffers_map_canvas_freeze(refresh=True)
        if lyr is not None and lyr.isValid():
            lyr.triggerRepaint()

    def _log_buffer_geom_diag_once(self, message: str):
        if not hasattr(self, '_buffer_geom_diag_counts'):
            self._buffer_geom_diag_counts = {}
        n = self._buffer_geom_diag_counts.get(message, 0) + 1
        self._buffer_geom_diag_counts[message] = n
        if n == 1:
            self.log_message(
                self.tr('[__Buffers__] {0}').format(message), 'WARNING')

    def _geometry_for_buffers_layer(self, geom: QgsGeometry, layer: QgsVectorLayer):
        """Alinha geometria ao tipo da camada (2D MultiPolygon típico) antes de addFeature."""
        if geom is None or geom.isNull() or geom.isEmpty():
            self._log_buffer_geom_diag_once(
                self.tr('Geometria vazia ou nula — feição ignorada.'))
            return None
        g = QgsGeometry(geom)
        tgt = layer.wkbType()
        want_z = QgsWkbTypes.hasZ(tgt)
        want_m = QgsWkbTypes.hasM(tgt)
        want_multi = QgsWkbTypes.isMultiType(tgt)
        # Linhas 3D → buffers PolygonZ; camada __Buffers__ é multpolygon 2D no GPKG
        if not want_z and hasattr(g, 'dropZValue'):
            try:
                g.dropZValue()
            except Exception:
                pass
        if not want_m and hasattr(g, 'dropMValue'):
            try:
                g.dropMValue()
            except Exception:
                pass
        if g.isEmpty():
            self._log_buffer_geom_diag_once(
                self.tr('Geometria vazia após remover Z/M — feição ignorada.'))
            return None
        if g.type() != QgsWkbTypes.PolygonGeometry:
            g_mv = g.makeValid()
            if not g_mv.isEmpty() and g_mv.type() == QgsWkbTypes.PolygonGeometry:
                g = g_mv
            else:
                self._log_buffer_geom_diag_once(
                    self.tr(
                        'Geometria não poligonal ({0}); makeValid não produziu polígono — ignorada.'
                    ).format(QgsWkbTypes.displayString(geom.wkbType())))
                return None
        if want_multi and not QgsWkbTypes.isMultiType(g.wkbType()):
            # collectGeometries só em QGIS recente; fromMultiPolygonXY é estável em 3.x
            cg = getattr(QgsGeometry, 'collectGeometries', None)
            if cg is not None:
                g_col = cg([g])
                if g_col is None or g_col.isEmpty():
                    self._log_buffer_geom_diag_once(
                        self.tr('Falha ao promover polígono simples a MultiPolygon — ignorada.'))
                    return None
                g = g_col
            else:
                poly_xy = g.asPolygon()
                if not poly_xy or not poly_xy[0]:
                    self._log_buffer_geom_diag_once(
                        self.tr('Polígono sem anéis — não foi possível formar MultiPolygon.'))
                    return None
                g = QgsGeometry.fromMultiPolygonXY([poly_xy])
                if g.isEmpty():
                    self._log_buffer_geom_diag_once(
                        self.tr('Falha ao promover polígono simples a MultiPolygon — ignorada.'))
                    return None
        if not g.isGeosValid():
            g = g.makeValid()
            if g.isEmpty() or g.type() != QgsWkbTypes.PolygonGeometry:
                self._log_buffer_geom_diag_once(
                    self.tr('Geometria inválida após makeValid — ignorada.'))
                return None
        if not want_z and QgsWkbTypes.hasZ(g.wkbType()) and hasattr(g, 'dropZValue'):
            try:
                g.dropZValue()
            except Exception:
                pass
        if g.isEmpty():
            return None
        return g

    def get_list_scale(self):
        def get_gsd():
            layer_ = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
            if layer_:
                return layer_.rasterUnitsPerPixelX()
            return
        max_scale_from_set = self.settings_dlg.dic_param['step_buffers']['fields']['max_scale']['value']
        min_scale_from_set = self.settings_dlg.dic_param['step_buffers']['fields']['min_scale']['value']

        if  max_scale_from_set < len(self.dic_pec_v):
            max_scale = max_scale_from_set
        else:
            gsd_ = get_gsd()
            for i, scale_ in enumerate(self.dic_pec_v):
                if self.dic_pec_mm['H']['A']['pec'] * scale_ > gsd_/2:
                    max_scale = i - 1
                    break
        if min_scale_from_set < len(self.dic_pec_v):
            min_scale = min_scale_from_set
        else:
            if not gsd_:
                gsd_ = get_gsd()
            for i, scale_ in enumerate(self.dic_pec_v):
                if self.dic_pec_mm['H']['A']['pec'] * scale_ > gsd_ * 2:
                    min_scale = i
                    break
        max_scale_idx = max(min(max_scale, min_scale), 0)
        min_scale_idx = min(max(max_scale, min_scale), len(self.dic_pec_v) -1)
        list_ = []
        for i in range (max_scale_idx, min_scale_idx + 1):
            list_.append(list(self.dic_pec_v)[i])
        return list_

    def define_buffers(self):
        self._buffer_geom_diag_counts = {}
        self._buffers_layer_target_logged = False
        self._reset_buffers_layer_for_run()
        self._buffers_map_deferred = not self._show_buffers_on_map_setting()
        self._begin_buffers_map_canvas_freeze()
        rebuilt = self._dic_match_from_match_lines_layer()
        if rebuilt is not None:
            if not rebuilt:
                self.log_message(
                    self.tr(
                        'Define buffers: a camada __Linhas_de_Correspondencia__ está vazia ou sem pares válidos.'),
                    'ERROR')
                return
            self.dic_match = rebuilt
            ext_sum = self._total_sample_extent_m_from_dic_match(self.dic_match)
            self._panel_stats_cache['ext_match'] = str(round(ext_sum, 1))
            self._panel_stats_cache['pair_nr'] = str(
                sum(len(self.dic_match[k]) for k in self.dic_match))
            self._refresh_extent_and_pairs_labels()
            self._persist_panel_stats_to_mdepa()
        if not self._check_sample_extent_vs_minimum():
            if self.cbx_workflow_pairs.currentIndex() == 1:
                self._workflow_pause = 'post_pairs_review'
                self._refresh_proc_button()
            return
        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('DEFININDO BUFFERS')
        self.log_message(mss_, 'INFO')
        accuracy_standard = self._accuracy_standard_index()
        gsd = self._test_dem_gsd()
        list_scale = [] if accuracy_standard == ACCURACY_STANDARD_CE90 else self.get_list_scale()
        if accuracy_standard == ACCURACY_STANDARD_CE90:
            if not gsd or gsd <= 0:
                self.log_message(
                    self.tr(
                        'CE90/LE90: resolução do MDE de teste indisponível. '
                        'Selecione o raster de teste e tente novamente.'),
                    'ERROR')
                return
            max_h, max_v = self._ce90_max_gsd_multipliers()
            dec = ce90_threshold_decimals(gsd)
            self.log_message(
                self.tr(
                    'Modo CE90/LE90 — pixel MDE teste={0:.3f} m; '
                    'precisão limiar={1} casa(s) decimal(is); '
                    'máx. H={2:g} pixels do MDE de teste ({3} m); '
                    'máx. V={4:g} pixels do MDE de teste ({5} m).'
                ).format(
                    gsd, dec,
                    max_h, f'{max_h * gsd:.{dec}f}',
                    max_v, f'{max_v * gsd:.{dec}f}',
                ),
                'INFO')
        dic_layers_line = {}
        for tag_ in self.dic_match:
            dic_layers_line[tag_] = {}
            for i in [0, 1]:
                type_ = self.dic_prj["dems"][i]["type"]
                layer_name = f'__{tag_}_Z_{type_}__'
                layer_ = self.get_gpkg_layer(layer_name)
                dic_layers_line[tag_].update({i: layer_})
        # list_layers_buffer = self.create_buffers_layer()
        norm_type = self.settings_dlg.dic_param['step_normalize_prog']['fields']['norm_type']['value']
        dm_formula = self._dm_formula_index()
        max_h_mult, max_v_mult = self._ce90_max_gsd_multipliers()
        dic_={
            'step': 'buffers',
            'dic_layers_line': dic_layers_line,
            'list_scale': list_scale,
            'dic_match': self.dic_match,
            'dic_pec_mm': self.dic_pec_mm,
            'dic_pec_v': self.dic_pec_v,
            'norm_type': norm_type,
            'dm_formula': dm_formula,
            'accuracy_standard': accuracy_standard,
            'gsd': gsd,
            'ce90_max_h': max_h_mult,
            'ce90_max_v': max_v_mult,
            'parent': self,
            'main': self.main}
        # Add tasks to queue
        key_ = 3
        self.task_queue.put((key_, dic_))

        # Start up to max_threads tasks
        while self.threads_running < self.max_threads and not self.task_queue.empty():
            key_, dic_ = self.task_queue.get()
            self.start_task(key_, dic_)

    def update_dic_vectors(self, dic_values):
        dic_vectors = {}
        for scale_ in dic_values:
            dic_vectors[scale_] = {}
            for class_ in dic_values[scale_]:
                dic_vectors[scale_][class_] = {'H': [], 'V': []}
                for count_ in dic_values[scale_][class_]:
                    rec = dic_values[scale_][class_][count_]
                    if not rec.get('outlier_h', rec.get('outlier', False)):
                        dh = _coerce_finite_measurement_scalar(rec.get('dm_h'))
                        if dh is not None:
                            dic_vectors[scale_][class_]['H'].append(dh)
                    if not rec.get('outlier_v', False):
                        dv = _coerce_finite_measurement_scalar(rec.get('dm_v'))
                        if dv is not None:
                            dic_vectors[scale_][class_]['V'].append(dv)
        return dic_vectors

    def _mark_outliers_iqr(self, dic_values, dm_key, outlier_key):
        """Marca outliers por IQR para uma dimensão (dm_h ou dm_v)."""
        stats = {}
        for scale_ in dic_values:
            stats[scale_] = {}
            for class_ in dic_values[scale_]:
                stats[scale_][class_] = []
                for count_ in dic_values[scale_][class_]:
                    v_ = _coerce_finite_measurement_scalar(
                        dic_values[scale_][class_][count_].get(dm_key))
                    if v_ is not None:
                        stats[scale_][class_].append(v_)

        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                vals = stats[scale_][class_]
                if len(vals) < 2:
                    for count_ in dic_values[scale_][class_]:
                        dic_values[scale_][class_][count_][outlier_key] = False
                    continue
                quant_ = statistics.quantiles(data=vals)
                iqr_ = quant_[2] - quant_[0]
                ls_ = quant_[2] + 1.5 * iqr_
                li_ = quant_[0] - 1.5 * iqr_
                for count_ in dic_values[scale_][class_]:
                    rec = dic_values[scale_][class_][count_]
                    v_ = _coerce_finite_measurement_scalar(rec.get(dm_key))
                    if v_ is None:
                        rec[outlier_key] = False
                    elif v_ < li_ or v_ > ls_:
                        rec[outlier_key] = True
                    else:
                        rec[outlier_key] = False

    def check_outliers(self, dic_values):
        self._reset_outlier_flags(dic_values)
        self._mark_outliers_iqr(dic_values, 'dm_h', 'outlier_h')
        self._mark_outliers_iqr(dic_values, 'dm_v', 'outlier_v')
        for scale_ in dic_values:
            for class_ in dic_values[scale_]:
                for count_ in dic_values[scale_][class_]:
                    rec = dic_values[scale_][class_][count_]
                    rec['outlier'] = rec.get('outlier_h', False)

    def ensure_crs_from_reference_dem(self):
        """Define crs_epsg a partir do MDE de referência (ou interseção no GPKG)."""
        cbx = self.dic_prj['dems'][0]['obj_cbx']
        ly = cbx.currentLayer() if cbx else None
        if isinstance(ly, QgsRasterLayer) and ly.isValid():
            self.crs_epsg = ly.crs().authid()
            return
        lyr = self._resolve_limit_layer_for_editing(self.intersection_name)
        if lyr is not None and lyr.isValid() and lyr.crs().isValid():
            self.crs_epsg = lyr.crs().authid()

    def recompute_dic_values_from_project(self):
        """Recalcula dic_values a partir das camadas no .pa.gpkg (síncrono, sem thread)."""
        from .mod_worker_threads import BufferThread

        rebuilt = self._dic_match_from_match_lines_layer()
        if not rebuilt:
            raise RuntimeError(
                self.tr('Sem pares válidos em __Linhas_de_Correspondencia__ no projeto.'))
        self.dic_match = rebuilt
        accuracy_standard = self._accuracy_standard_index()
        gsd = self._test_dem_gsd()
        list_scale = [] if accuracy_standard == ACCURACY_STANDARD_CE90 else self.get_list_scale()
        if accuracy_standard == ACCURACY_STANDARD_CE90:
            if not gsd or gsd <= 0:
                raise RuntimeError(
                    self.tr('CE90/LE90: resolução do MDE de teste indisponível.'))
        elif not list_scale:
            raise RuntimeError(self.tr('Lista de escalas vazia - verifique parâmetros de buffers.'))
        dic_layers_line = {}
        for tag_ in self.dic_match:
            dic_layers_line[tag_] = {}
            for i in (0, 1):
                type_ = self.dic_prj['dems'][i]['type']
                layer_name = f'__{tag_}_Z_{type_}__'
                layer_ = self.get_gpkg_layer(layer_name)
                if layer_ is None or not layer_.isValid():
                    raise RuntimeError(
                        self.tr('Camada ausente ou inválida no GPKG: {0}').format(layer_name))
                dic_layers_line[tag_][i] = layer_
        norm_type = self.settings_dlg.dic_param['step_normalize_prog']['fields']['norm_type']['value']
        dm_formula = self._dm_formula_index()
        max_h_mult, max_v_mult = self._ce90_max_gsd_multipliers()
        bt = BufferThread(
            self.main,
            self,
            key_=3,
            dic_={
                'dic_layers_line': dic_layers_line,
                'list_scale': list_scale,
                'dic_match': self.dic_match,
                'dic_pec_mm': self.dic_pec_mm,
                'dic_pec_v': self.dic_pec_v,
                'norm_type': norm_type,
                'dm_formula': dm_formula,
                'accuracy_standard': accuracy_standard,
                'gsd': gsd,
                'ce90_max_h': max_h_mult,
                'ce90_max_v': max_v_mult,
            },
        )
        bt.run()
        self._ce90_meta = getattr(bt, 'ce90_meta', None)
        return bt.dic_values

    def calc_pec(self, dic_values):
        self._pec_report_plan_rows = []
        self._pec_report_alt_rows = []
        om = self.cbx_workflow_outliers.currentIndex()
        om_names = (
            self.tr('Remover automaticamente'),
            self.tr('Avaliar individualmente'),
            self.tr('Usar todos'),
        )
        is_ce90 = self._accuracy_standard_index() == ACCURACY_STANDARD_CE90
        if is_ce90:
            self._pec_report_pec_intro = self.tr(
                'Tratamento de outliers (CE90/LE90): {0}'
            ).format(om_names[om] if 0 <= om < len(om_names) else str(om))
        else:
            self._pec_report_pec_intro = self.tr('Tratamento de outliers (PEC): {0}').format(
                om_names[om] if 0 <= om < len(om_names) else str(om))

        self._log_null_dm_samples(dic_values, 'dm_h')
        self._log_null_dm_samples(dic_values, 'dm_v')

        if is_ce90:
            self._calc_ce90_le90_report(dic_values)
            self._write_pec_results_txt()
            self._sync_panel_extent_after_pec(dic_values)
            return

        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('CALCULANDO PEC PLANIMÉTRICO')
        self.log_message(mss_, 'INFO')

        for scale_ in sorted(dic_values):
            for class_ in dic_values[scale_]:
                pec_h = round(scale_ * self.dic_pec_mm['H'][class_]['pec'], 2)
                ep_h = round(scale_ * self.dic_pec_mm['H'][class_]['ep'], 2)
                row, str_ = self._calc_pec_group_report(
                    dic_values, scale_, class_, 'dm_h', pec_h, ep_h, dimension='H')
                self.log_message(str_, 'INFO')
                self._pec_report_plan_rows.append(row)

        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('CALCULANDO PEC ALTIMÉTRICO')
        self.log_message(mss_, 'INFO')
        for scale_ in sorted(dic_values):
            for class_ in dic_values[scale_]:
                pec_v, ep_v = pec_alt_limits(scale_, class_, self.dic_pec_alt)
                if pec_v is None:
                    self.log_message(
                        self.tr('PEC altimétrico ignorado para escala 1:{0}.000 (sem limites definidos).').format(
                            scale_),
                        'WARNING',
                    )
                    continue
                row, str_ = self._calc_pec_group_report(
                    dic_values, scale_, class_, 'dm_v', pec_v, ep_v,
                    dimension='V', eq=self.dic_pec_v.get(scale_))
                self.log_message(str_, 'INFO')
                self._pec_report_alt_rows.append(row)

        self._write_pec_results_txt()
        self._sync_panel_extent_after_pec(dic_values)

    def _calc_ce90_le90_report(self, dic_values):
        """Relatório CE90/LE90: uma linha por candidato avaliado na busca."""
        gsd = self._test_dem_gsd() or 0.0
        meta = getattr(self, '_ce90_meta', None) or {}
        if gsd <= 0:
            try:
                gsd = float(meta.get('gsd') or 0.0)
            except (TypeError, ValueError):
                gsd = 0.0
        dec = self._ce90_threshold_decimals()
        final_h = meta.get('final_h')
        final_v = meta.get('final_v')
        trials_h = meta.get('trials_h') or {}
        trials_v = meta.get('trials_v') or {}

        def _is_final(scale_, final_):
            if final_ is None:
                return False
            try:
                return abs(float(scale_) - float(final_)) < (10 ** (-dec)) / 2.0
            except (TypeError, ValueError):
                return False

        def _meta_for(scale_, trials_map):
            try:
                key = round(float(scale_), dec)
            except (TypeError, ValueError):
                key = scale_
            info = trials_map.get(key) or trials_map.get(scale_) or {}
            if not info and isinstance(scale_, (int, float)):
                for k, v in trials_map.items():
                    try:
                        if abs(float(k) - float(scale_)) < 1e-9:
                            return v or {}
                    except (TypeError, ValueError):
                        continue
            return info or {}

        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('TABELA CE90 (todos os limiares avaliados)')
        self.log_message(mss_, 'INFO')
        for scale_ in sorted(k for k in dic_values if isinstance(k, (int, float))):
            if CLASS_CE90 not in dic_values[scale_]:
                continue
            pec_h = float(scale_)
            ep_h = round(pec_h * EP_RATIO_H, dec)
            tmeta = _meta_for(scale_, trials_h)
            row, str_ = self._calc_pec_group_report(
                dic_values, scale_, CLASS_CE90, 'dm_h', pec_h, ep_h,
                dimension='H', pec_decimals=dec)
            row['ce90_final'] = _is_final(scale_, final_h)
            row['ce90_ciclo'] = tmeta.get('ciclo')
            if pec_h <= 0:
                str_ = self.tr(
                    'CE90={0} m — RODADA 1 — FALHOU (sem buffer/DM)'
                ).format(self._format_ce90_m(0.0, dec))
            else:
                if row.get('ce90_ciclo') is not None:
                    str_ = self.tr('RODADA {0} — {1}').format(
                        row['ce90_ciclo'], str_)
                if gsd > 0:
                    str_ += self.tr(
                        ' ({0:.1f} pixels do MDE de teste)'
                    ).format(pec_h / gsd)
            self.log_message(str_, 'INFO')
            self._pec_report_plan_rows.append(row)

        mss_ = self.tr('=======================================\n')
        mss_ += self.tr('TABELA LE90 (todos os limiares avaliados)')
        self.log_message(mss_, 'INFO')
        for scale_ in sorted(k for k in dic_values if isinstance(k, (int, float))):
            if CLASS_LE90 not in dic_values[scale_]:
                continue
            pec_v = float(scale_)
            ep_v = round(pec_v * EP_RATIO_V, dec)
            tmeta = _meta_for(scale_, trials_v)
            row, str_ = self._calc_pec_group_report(
                dic_values, scale_, CLASS_LE90, 'dm_v', pec_v, ep_v,
                dimension='V', pec_decimals=dec)
            row['ce90_final'] = _is_final(scale_, final_v)
            row['ce90_ciclo'] = tmeta.get('ciclo')
            if pec_v <= 0:
                str_ = self.tr(
                    'LE90={0} m — RODADA 1 — FALHOU (sem buffer/DM)'
                ).format(self._format_ce90_m(0.0, dec))
            else:
                if row.get('ce90_ciclo') is not None:
                    str_ = self.tr('RODADA {0} — {1}').format(
                        row['ce90_ciclo'], str_)
                if gsd > 0:
                    str_ += self.tr(
                        ' ({0:.1f} pixels do MDE de teste)'
                    ).format(pec_v / gsd)
            self.log_message(str_, 'INFO')
            self._pec_report_alt_rows.append(row)

    @staticmethod
    def _extent_km_from_m(ext_m):
        if not ext_m:
            return 0.0
        try:
            return round(float(ext_m) / 1000.0, 1)
        except (TypeError, ValueError):
            return 0.0

    def _valid_pec_extent_m(self, dic_values, dm_key='dm_h'):
        """Extensão total (m) das amostras válidas PEC numa escala/classe (mesma lógica do relatório)."""
        if not dic_values:
            return 0.0
        scale_ = sorted(dic_values)[0]
        class_ = sorted(dic_values[scale_])[0]
        samples, _ = self._pec_samples_group(dic_values, scale_, class_, dm_key)
        return sum(s['extent'] for s in samples)

    def _sync_panel_extent_after_pec(self, dic_values):
        """Mantém ext_match = correspondência total; regista extensão PEC válida só no log."""
        ext_valid_m = self._valid_pec_extent_m(dic_values, 'dm_h')
        ext_corr_m = self._total_sample_extent_m_from_dic_match()
        if ext_corr_m > 0:
            self._panel_stats_cache['ext_match'] = str(round(ext_corr_m, 1))
            self._refresh_extent_and_pairs_labels()
            self._persist_panel_stats_to_mdepa()
        if ext_valid_m > 0 and ext_corr_m > 0 and abs(ext_corr_m - ext_valid_m) > 0.5:
            self.log_message(
                self.tr(
                    'Extensão amostras válidas PEC: {0} km (correspondência total: {1} km).'
                ).format(
                    self._extent_km_from_m(ext_valid_m),
                    self._extent_km_from_m(ext_corr_m),
                ),
                'INFO',
            )

    def _pec_samples_group(self, dic_values, scale_, class_, dm_key):
        """Amostras válidas, outliers e listas auxiliares por escala/classe."""
        outlier_key = 'outlier_h' if dm_key == 'dm_h' else 'outlier_v'
        samples = []
        outlier_ids = set()
        for count_ in dic_values[scale_][class_]:
            rec = dic_values[scale_][class_][count_]
            fid_r = rec.get('fid_r')
            if self._is_statistical_outlier_rec(rec, outlier_key, dm_key):
                if fid_r is not None:
                    try:
                        outlier_ids.add(int(fid_r))
                    except (TypeError, ValueError):
                        pass
                continue
            dm = _coerce_finite_measurement_scalar(rec.get(dm_key))
            if dm is None:
                continue
            ext = self._match_pair_extent_m(rec)
            samples.append({
                'dm': dm,
                'fid_r': int(fid_r) if fid_r is not None else None,
                'extent': ext if ext > 0 else 0.0,
            })
        return samples, sorted(outlier_ids)

    def _collect_null_dm_ignored(self, dic_values, dm_key):
        """Registos ignorados: cada entrada é nula só na escala/classe correspondente."""
        out = []
        for scale_ in sorted(dic_values):
            for class_ in sorted(dic_values[scale_]):
                for count_ in dic_values[scale_][class_]:
                    rec = dic_values[scale_][class_][count_]
                    if _coerce_finite_measurement_scalar(rec.get(dm_key)) is not None:
                        continue
                    ext_m = self._match_pair_extent_m(rec)
                    out.append((scale_, class_, rec, ext_m))
        return out

    def _log_null_dm_samples(self, dic_values, dm_key):
        """Regista quantidade e extensão das amostras ignoradas por DM nulo (por escala/classe)."""
        dm_label = 'DM_H' if dm_key == 'dm_h' else 'DM_V'
        ignored = self._collect_null_dm_ignored(dic_values, dm_key)
        if not ignored:
            return
        total_ext_m = sum(ext_m for _, _, _, ext_m in ignored)
        n_ign = len(ignored)
        ext_km = self._extent_km_from_m(total_ext_m)
        head = self.tr(
            '{0} nulo/indefinido: {1} amostra(s) ignorada(s) na respetiva escala/classe — '
            'extensão total ignorada: {2} km ({3} m)'
        ).format(dm_label, n_ign, ext_km, int(round(total_ext_m)))
        groups = {}
        for scale_, class_, rec, ext_m in ignored:
            gk = (scale_, class_)
            if gk not in groups:
                groups[gk] = {'n': 0, 'ext_m': 0.0, 'items': []}
            groups[gk]['n'] += 1
            groups[gk]['ext_m'] += ext_m
            groups[gk]['items'].append((rec, ext_m))
        lines = []
        for (scale_, class_) in sorted(groups.keys()):
            g = groups[(scale_, class_)]
            lines.append(
                self.tr('  1:{0}.000 — classe {1}: {2} ignorada(s), {3} m').format(
                    scale_, class_, g['n'], int(round(g['ext_m']))))
            for rec, ext_m in g['items']:
                lines.append(
                    self.tr('    ref {0} fid_r={1} | teste {2} fid_t={3} | ext. {4} m').format(
                        rec.get('layer_r'), rec.get('fid_r'),
                        rec.get('layer_t'), rec.get('fid_t'),
                        int(round(ext_m)) if ext_m > 0 else 0,
                    ))
        extra = ''
        max_lines = 60
        if len(lines) > max_lines:
            lines = lines[:max_lines]
            extra = self.tr('\n  … detalhe truncado ({0} amostras no total).').format(n_ign)
        self.log_message(head + ':\n' + '\n'.join(lines) + extra, 'WARNING')

    def _pec_norm_fail_row(self, scale_, class_, pec_lim, samples, outlier_ids, *, dimension='H', eq=None):
        """Linha de relatório quando normalidade falha ou não há amostras."""
        norm_fail = self.tr('NORMALIDADE - FALHOU')
        failed = self.tr('FALHOU')
        return {
            'scale': scale_,
            'class': class_,
            'eq': eq,
            'n_outliers': len(outlier_ids),
            'n_valid': len(samples),
            'ext_km': self._extent_km_from_m(sum(s['extent'] for s in samples)),
            'perc_q': '',
            'pec_lim': pec_lim,
            'result_q': failed,
            'result_q_ok': False,
            'perc_e': '',
            'result_e': failed,
            'result_e_ok': False,
            'teste_pec_q': norm_fail,
            'teste_pec_e': norm_fail,
            'teste_ep': '',
            'result_ep': failed,
            'result_ep_ok': False,
            'outlier_ids': ', '.join(str(i) for i in outlier_ids),
            'reprovados_ids': '',
        }

    def _calc_pec_group_report(
        self, dic_values, scale_, class_, dm_key, pec_raw, ep_raw, *,
        dimension='H', eq=None, pec_decimals=None,
    ):
        samples, outlier_ids = self._pec_samples_group(
            dic_values, scale_, class_, dm_key)
        values = [s['dm'] for s in samples]
        extents = [s['extent'] for s in samples]
        pec_lim = pec_test_limit(pec_raw, decimals=pec_decimals)
        is_ce = class_ in (CLASS_CE90, CLASS_LE90)
        if is_ce and pec_decimals is not None:
            pec_lim_txt = self._format_ce90_m(pec_lim, pec_decimals)
            ep_txt = self._format_ce90_m(ep_raw, pec_decimals)
            scale_txt = self._format_ce90_m(scale_, pec_decimals)
        else:
            pec_lim_txt = pec_lim
            ep_txt = ep_raw
            scale_txt = scale_
        reprov_ids = sorted({
            s['fid_r'] for s in samples
            if s['fid_r'] is not None and s['dm'] > pec_lim
        })
        norm_fail = self.tr('NORMALIDADE - FALHOU')

        if not values or not check_norm_values(values):
            row = self._pec_norm_fail_row(
                scale_, class_, pec_lim, samples, outlier_ids,
                dimension=dimension, eq=eq)
            if is_ce:
                str_ = self.tr('{0}={1} m — {2}, {3} amostras').format(
                    class_, scale_txt, norm_fail, len(values))
            elif dimension == 'V':
                str_ = self.tr('EQ {0} — 1:{1}.000-{2}= {3}, {4} amostras').format(
                    eq, scale_, class_, norm_fail, len(values))
            else:
                str_ = self.tr('1:{0}.000-{1}= {2}, {3} amostras').format(
                    scale_, class_, norm_fail, len(values))
            return row, str_

        perc_q = perc_pec_quant(values, pec_raw, decimals=pec_decimals)
        perc_e = perc_pec_ext(values, extents, pec_raw, decimals=pec_decimals)
        pec_ok_q = perc_q >= 0.90
        pec_ok_e = perc_e >= 0.90
        rms_ = self.rms(values)
        ep_ok = math.isfinite(rms_) and rms_ <= ep_raw
        rms_show = round(rms_, 2) if math.isfinite(rms_) else float('nan')
        cmp_ep = '<=' if ep_ok else '>'
        perc_q_pct = round(perc_q * 100)
        perc_e_pct = round(perc_e * 100)

        row = {
            'scale': scale_,
            'class': class_,
            'eq': eq,
            'n_outliers': len(outlier_ids),
            'n_valid': len(values),
            'ext_km': self._extent_km_from_m(sum(extents)),
            'perc_q': perc_q_pct,
            'pec_lim': pec_lim,
            'result_q': self.tr('PASSOU') if pec_ok_q else self.tr('FALHOU'),
            'result_q_ok': pec_ok_q,
            'perc_e': perc_e_pct,
            'result_e': self.tr('PASSOU') if pec_ok_e else self.tr('FALHOU'),
            'result_e_ok': pec_ok_e,
            'teste_pec_q': f'{perc_q_pct} % <= {pec_lim_txt}',
            'teste_pec_e': f'{perc_e_pct} % <= {pec_lim_txt}',
            'teste_ep': (
                f'{rms_show} {cmp_ep} {ep_txt} EP'
                if math.isfinite(rms_show) else ''
            ),
            'result_ep': self.tr('PASSOU') if ep_ok else self.tr('FALHOU'),
            'result_ep_ok': ep_ok,
            'outlier_ids': ', '.join(str(i) for i in outlier_ids),
            'reprovados_ids': ', '.join(str(i) for i in reprov_ids),
        }

        if is_ce:
            str_ = self.tr(
                '{0}={1} m — quant {2}% <= {3} - {4}, ext {5}% <= {3} - {6},'
            ).format(
                class_, scale_txt, perc_q_pct, pec_lim_txt, row['result_q'],
                perc_e_pct, row['result_e'])
        elif dimension == 'V':
            str_ = self.tr(
                'EQ {0} — 1:{1}.000-{2}= quant {3}% <= {4} - {5}, ext {6}% <= {4} - {7},'
            ).format(
                eq, scale_, class_, perc_q_pct, pec_lim, row['result_q'],
                perc_e_pct, row['result_e'])
        else:
            str_ = self.tr(
                '1:{0}.000-{1}= quant {2}% <= {3} - {4}, ext {5}% <= {3} - {6},'
            ).format(
                scale_, class_, perc_q_pct, pec_lim, row['result_q'],
                perc_e_pct, row['result_e'])
        if ep_ok:
            str_ += self.tr(' {0} <= {1} EP - PASSOU, {2}').format(
                rms_show if math.isfinite(rms_show) else self.tr('n/d'),
                ep_txt if is_ce else ep_raw, len(values))
        else:
            str_ += self.tr(' {0} > {1} EP - FALHOU, {2}').format(
                rms_show if math.isfinite(rms_show) else self.tr('n/d'),
                ep_txt if is_ce else ep_raw, len(values))
        return row, str_

    def _pec_row_data_cells(self, row, dimension='H'):
        """Células de dados na ordem das colunas do relatório (plan. 11 cols / alt. 12 cols)."""
        is_ce = row.get('class') in (CLASS_CE90, CLASS_LE90)
        if is_ce:
            dec = self._ce90_threshold_decimals()
            try:
                lim_m = float(row['scale'])
            except (TypeError, ValueError):
                lim_m = row['scale']
            if isinstance(lim_m, (int, float)):
                lim_txt = self._format_ce90_m(lim_m, dec)
            else:
                lim_txt = str(lim_m)
            ciclo = row.get('ce90_ciclo')
            if ciclo is None and isinstance(lim_m, (int, float)) and lim_m <= 0:
                ciclo = 1
            ciclo_txt = '' if ciclo is None else str(int(ciclo))
            # CE/LE (m) | RODADA — mesma grelha nas duas tabelas (11 colunas)
            head = [lim_txt, ciclo_txt]
            blank_stats = (
                (isinstance(lim_m, (int, float)) and lim_m <= 0)
                or int(row.get('n_valid') or 0) == 0
            )
            pec_lim_txt = self._format_ce90_m(row.get('pec_lim'), dec)
            if blank_stats:
                n_out = n_val = ext_km = ''
                t_q = t_e = t_ep = ''
                failed = self.tr('FALHOU')
                r_q = r_e = r_ep = failed
            else:
                n_out = row['n_outliers']
                n_val = row['n_valid']
                ext_km = row['ext_km']
                t_q = row.get(
                    'teste_pec_q',
                    f"{row.get('perc_q', '')} % <= {pec_lim_txt}")
                t_e = row.get(
                    'teste_pec_e',
                    f"{row.get('perc_e', '')} % <= {pec_lim_txt}")
                t_ep = row.get('teste_ep', '')
                r_q = row['result_q']
                r_e = row['result_e']
                r_ep = row['result_ep']
            tail = [n_out, n_val, ext_km, t_q, r_q, t_e, r_e, t_ep, r_ep]
            return head + tail
        if dimension == 'V':
            head = [
                self.tr('1:{0}.000').format(row['scale']),
                row.get('eq', ''),
                row['class'],
            ]
        else:
            head = [
                self.tr('1:{0}.000').format(row['scale']),
                row['class'],
            ]
        tail = [
            row['n_outliers'],
            row['n_valid'],
            row['ext_km'],
            row.get('teste_pec_q', f"{row.get('perc_q', '')} % <= {row['pec_lim']}"),
            row['result_q'],
            row.get('teste_pec_e', f"{row.get('perc_e', '')} % <= {row['pec_lim']}"),
            row['result_e'],
            row.get('teste_ep', ''),
            row['result_ep'],
        ]
        return head + tail

    def _pec_results_header_labels(self, altimetric=False):
        """Rótulos do cabeçalho (PEC-PCD ou CE90/LE90)."""
        if self._accuracy_standard_index() == ACCURACY_STANDARD_CE90:
            return {
                'escala': self.tr('LE (m)') if altimetric else self.tr('CE (m)'),
                'eq': '',
                'classe': self.tr('RODADA'),
                'ce90_layout': True,
                'outliers': self.tr('Outliers'),
                'amostras': self.tr('Amostras Válidas'),
                'quant': self.tr('Quant.'),
                'ext_km': self.tr('Ext. (km)'),
                'pec_group': (
                    self.tr('LE90 (90% d_i ≤ limiar)')
                    if altimetric
                    else self.tr('CE90 (90% d_i ≤ limiar)')
                ),
                'pec_quant': self.tr('Quantitativo'),
                'pec_ext': self.tr('Extensão'),
                'teste': self.tr('Teste'),
                'resultado': self.tr('Resultado'),
                'ep_group': self.tr('EP (RMS ≤ EP)'),
            }
        return {
            'escala': self.tr('Escala'),
            'eq': self.tr('EQ (m)'),
            'classe': self.tr('Classe'),
            'outliers': self.tr('Outliers'),
            'amostras': self.tr('Amostras Válidas'),
            'quant': self.tr('Quant.'),
            'ext_km': self.tr('Ext. (km)'),
            'pec_group': self.tr('PEC (90% d_i ≤ PEC-PCD)'),
            'pec_quant': self.tr('Quantitativo'),
            'pec_ext': self.tr('Extensão'),
            'teste': self.tr('Teste'),
            'resultado': self.tr('Resultado'),
            'ep_group': self.tr('EP (RMS ≤ EP)'),
        }

    def _pec_results_table_head_html(self, altimetric=False, widths_pct=None) -> str:
        return _pec_results_table_head_html(
            altimetric=altimetric,
            header_labels=self._pec_results_header_labels(altimetric=altimetric),
            widths_pct=widths_pct,
        )

    def _pec_results_table_head_txt_rows(self, altimetric=False):
        """Três linhas de cabeçalho tabulado (mesma estrutura lógica do LaTeX)."""
        lb = self._pec_results_header_labels(altimetric=altimetric)
        use_alt = bool(altimetric) and not bool(lb.get('ce90_layout'))
        n = 12 if use_alt else 11
        blank = [''] * n

        if use_alt:
            row1 = [
                lb['escala'], lb['eq'], lb['classe'], lb['outliers'],
                lb['amostras'], '',
                lb['pec_group'], '', '', '',
                lb['ep_group'], '',
            ]
            row2 = blank[:4] + ['', ''] + [lb['pec_quant'], '', lb['pec_ext'], ''] + ['', '']
        else:
            row1 = [
                lb['escala'], lb['classe'], lb['outliers'],
                lb['amostras'], '',
                lb['pec_group'], '', '', '',
                lb['ep_group'], '',
            ]
            row2 = blank[:3] + ['', ''] + [lb['pec_quant'], '', lb['pec_ext'], ''] + ['', '']

        row3 = blank[:4] if use_alt else blank[:3]
        row3 += [lb['quant'], lb['ext_km']]
        row3 += [lb['teste'], lb['resultado'], lb['teste'], lb['resultado']]
        row3 += [lb['teste'], lb['resultado']]
        return row1, row2, row3

    def _pec_row_to_cells(self, row, dimension='H'):
        return self._pec_row_data_cells(row, dimension)

    def _pec_row_snapshot_entry(self, row, dimension='H') -> dict:
        """Entrada PEC no snapshot/PDF: texto traduzido + flags pass/fail (idioma-independente)."""
        cells = []
        for c in self._pec_row_to_cells(row, dimension):
            if c is None or c == '':
                cells.append('')
            else:
                cells.append(str(c))
        # CE90/LE90 usam grelha de 11 colunas (como planimétrico)
        is_ce = row.get('class') in (CLASS_CE90, CLASS_LE90)
        return {
            'cells': cells,
            'result_ok': [
                row.get('result_q_ok'),
                row.get('result_e_ok'),
                row.get('result_ep_ok'),
            ],
            'ce90_layout': is_ce,
        }

    def _write_pec_results_txt(self):
        """Grava tabelas PEC planimétrica/altimétrica em texto tabulado (formato pec_from_gpkg)."""
        data_dir = self.dic_prj.get('path')
        if not data_dir:
            return
        plan_rows = getattr(self, '_pec_report_plan_rows', None) or []
        alt_rows = getattr(self, '_pec_report_alt_rows', None) or []
        if not plan_rows and not alt_rows:
            return
        dic_st = self.dic_prj['standard']
        out_path = os.path.join(data_dir, f'{dic_st["name"]}{dic_st["files"]["result_txt"]}')
        try:
            os.makedirs(data_dir, exist_ok=True)
            with open(out_path, 'w', encoding='utf-8') as f:
                intro = getattr(self, '_pec_report_pec_intro', '') or ''
                if intro:
                    f.write(intro + '\n\n')
                f.write(self.tr('ANÁLISE PLANIMÉTRICA') + '\n')
                for hdr in self._pec_results_table_head_txt_rows(altimetric=False):
                    f.write('\t'.join(hdr) + '\n')
                for row in plan_rows:
                    f.write('\t'.join(str(c) for c in self._pec_row_to_cells(row, 'H')) + '\n')
                f.write('\n' + self.tr('ANÁLISE ALTIMÉTRICA') + '\n')
                for hdr in self._pec_results_table_head_txt_rows(altimetric=True):
                    f.write('\t'.join(hdr) + '\n')
                for row in alt_rows:
                    f.write('\t'.join(str(c) for c in self._pec_row_to_cells(row, 'V')) + '\n')
                f.write('\n')
            self.log_message(self.tr('Relatório PEC gravado: {0}').format(out_path), 'INFO')
        except OSError as e:
            self.log_message(
                self.tr('Não foi possível gravar relatório PEC: {0} ({1})').format(out_path, e),
                'ERROR',
            )

    def _format_param_value_for_report(self, meta):
        if not isinstance(meta, dict):
            return ''
        val = meta.get('value')
        if meta.get('type') == 'checkbox':
            try:
                on = bool(int(val))
            except (TypeError, ValueError):
                on = bool(val)
            return self.tr('gerar') if on else self.tr('não gerar')
        if meta.get('type') == 'radio':
            lst = meta.get('list') or []
            try:
                idx = int(val)
            except (TypeError, ValueError):
                idx = 0
            if 0 <= idx < len(lst):
                return str(lst[idx])
            return '' if val is None else str(val)
        if meta.get('type') == 'doublespin':
            try:
                return str(float(val))
            except (TypeError, ValueError):
                return '' if val is None else str(val)
        if 'list' in meta:
            lst = meta['list']
            try:
                idx = int(val)
            except (TypeError, ValueError):
                try:
                    idx = int(float(val))
                except (TypeError, ValueError):
                    idx = 0
            if isinstance(lst, (list, tuple)) and 0 <= idx < len(lst):
                item = lst[idx]
                if 'string' in meta:
                    try:
                        return meta['string'].format(item)
                    except (TypeError, ValueError, IndexError):
                        return str(item)
                return str(item)
            # índice da opção em branco no fim do combo de escalas
            if isinstance(lst, (list, tuple)) and idx == len(lst):
                return ''
            return '' if val is None or val == '' else str(val)
        return '' if val is None or val == '' else str(val)

    def _report_extent_intersection_rows(self) -> list:
        """Linhas da tabela de envelope (colunas Xmin, Ymin, Xmax, Ymax)."""
        lyr = self._resolve_limit_layer_for_editing(self.intersection_name)
        if lyr is None or not lyr.isValid():
            return [{'label': self.tr('Estado'), 'value': self.tr('(camada de interseção indisponível)')}]
        ext = lyr.extent()
        if ext.isEmpty():
            return [{'label': self.tr('Estado'), 'value': self.tr('(extensão vazia — execute a interseção dos MDEs)')}]
        crs = lyr.crs()
        official = QgsCoordinateReferenceSystem(REPORT_ENVELOPE_OFFICIAL_CRS_AUTH)
        official_label = REPORT_ENVELOPE_OFFICIAL_LABEL
        rows = []

        def _env_row(label, rect, *, decimals=3):
            return {
                'label': label,
                'xmin': round(rect.xMinimum(), decimals),
                'ymin': round(rect.yMinimum(), decimals),
                'xmax': round(rect.xMaximum(), decimals),
                'ymax': round(rect.yMaximum(), decimals),
            }

        crs_label = crs.authid() if crs.isValid() else self.tr('(sem CRS)')
        if crs.isValid() and crs.authid().upper() == REPORT_ENVELOPE_OFFICIAL_CRS_AUTH.upper():
            rows.append(_env_row(
                self.tr('Envelope ({0})').format(official_label), ext, decimals=3))
        else:
            rows.append(_env_row(
                self.tr('Envelope ({0})').format(crs_label), ext, decimals=3))
            if crs.isValid():
                try:
                    xform = QgsCoordinateTransform(
                        crs, official, QgsProject.instance(),
                    )
                    rect = xform.transformBoundingBox(ext)
                    rows.append(_env_row(
                        self.tr('Envelope ({0})').format(official_label), rect, decimals=6))
                except Exception:
                    rows.append({
                        'label': self.tr('Envelope ({0})').format(official_label),
                        'value': self.tr('(transformação indisponível)'),
                    })
        return rows

    def _report_extent_intersection_lines(self) -> list:
        """Compatibilidade: texto plano derivado das linhas da tabela."""
        lines = []
        for row in self._report_extent_intersection_rows():
            if 'xmin' in row:
                lines.append(
                    '{0}: Xmin={1}, Ymin={2}, Xmax={3}, Ymax={4}'.format(
                        row.get('label', ''),
                        row.get('xmin', ''),
                        row.get('ymin', ''),
                        row.get('xmax', ''),
                        row.get('ymax', ''),
                    ))
            elif row.get('label'):
                lines.append(f'{row.get("label", "")}: {row.get("value", "")}')
            else:
                lines.append(str(row.get('value', '')))
        return lines

    def _report_extent_intersection_html(self) -> str:
        return '<br/>'.join(html.escape(s) for s in self._report_extent_intersection_lines())

    def _build_pec_results_tables_html(self) -> str:
        """Tabelas planimétrica / altimétrica (mesma função que TXT→PDF)."""
        intro = (getattr(self, '_pec_report_pec_intro', '') or '').strip()
        plan_rows = getattr(self, '_pec_report_plan_rows', None) or []
        alt_rows = getattr(self, '_pec_report_alt_rows', None) or []
        empty_msg = ''
        if not plan_rows and not alt_rows:
            empty_msg = self.tr(
                '(ainda não há resultados de PEC nesta sessão — execute a análise até ao fim.)')
        is_ce90 = self._accuracy_standard_index() == ACCURACY_STANDARD_CE90
        return _build_pec_results_tables_html_blocks(
            intro=intro,
            plan_title=(
                self.tr('7.1 CE90') if is_ce90 else self.tr('7.1 PEC Planimétrico')
            ),
            alt_title=(
                self.tr('7.2 LE90') if is_ce90 else self.tr('7.2 PEC Altimétrico')
            ),
            plan_data_rows=[
                self._pec_row_snapshot_entry(row, 'H') for row in plan_rows],
            alt_data_rows=[
                self._pec_row_snapshot_entry(row, 'V') for row in alt_rows],
            empty_message=empty_msg,
            plan_header_labels=self._pec_results_header_labels(altimetric=False),
            alt_header_labels=self._pec_results_header_labels(altimetric=True),
            plan_page_break=True,
            alt_page_break=True,
        )

    def _normalization_method_index(self) -> int:
        try:
            return int(
                self.settings_dlg.dic_param['step_normalize_prog']['fields']['norm_type']['value'])
        except (TypeError, ValueError, KeyError):
            return 0

    def _dm_formula_index(self) -> int:
        try:
            return int(
                self.settings_dlg.dic_param['step_dm_formula']['fields']['dm_formula']['value'])
        except (TypeError, ValueError, KeyError):
            return 0

    def _accuracy_standard_index(self) -> int:
        try:
            return int(
                self.settings_dlg.dic_param['step_buffers']['fields']['accuracy_standard']['value'])
        except (TypeError, ValueError, KeyError):
            return ACCURACY_STANDARD_BR

    def _ce90_max_gsd_multipliers(self):
        fields = self.settings_dlg.dic_param['step_buffers']['fields']
        try:
            max_h = float(fields.get('ce90_max_h', {}).get('value', 5.0))
        except (TypeError, ValueError):
            max_h = 5.0
        try:
            max_v = float(fields.get('ce90_max_v', {}).get('value', 2.0))
        except (TypeError, ValueError):
            max_v = 2.0
        return max(0.1, max_h), max(0.1, max_v)

    def _test_dem_gsd(self):
        try:
            layer_ = self.dic_prj['dems'][1]['obj_cbx'].currentLayer()
        except (TypeError, KeyError, AttributeError):
            return None
        if layer_ is None:
            return None
        try:
            gsd = float(layer_.rasterUnitsPerPixelX())
        except (TypeError, ValueError, AttributeError):
            return None
        if not math.isfinite(gsd) or gsd <= 0:
            return None
        return gsd

    def _ce90_threshold_decimals(self) -> int:
        """Casas decimais do limiar CE90/LE90 (pixel < 5 m → 2)."""
        meta = getattr(self, '_ce90_meta', None) or {}
        raw = meta.get('threshold_decimals')
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass
        gsd = self._test_dem_gsd()
        if gsd is None:
            try:
                gsd = float(meta.get('gsd') or 0.0)
            except (TypeError, ValueError):
                gsd = 0.0
        return ce90_threshold_decimals(gsd)

    def _format_ce90_m(self, value, decimals=None) -> str:
        """Formata metros CE90/LE90 com as casas do limiar (para relatório/log)."""
        dec = self._ce90_threshold_decimals() if decimals is None else int(decimals)
        try:
            return f'{float(value):.{dec}f}'
        except (TypeError, ValueError):
            return '' if value is None else str(value)

    @staticmethod
    def _geometry_wkt_for_report(geom) -> str:
        if geom is None or geom.isNull() or geom.isEmpty():
            return ''
        try:
            return QgsGeometry(geom).asWkt()
        except Exception:
            return ''

    def _collect_homologous_pairs_profile_data(self) -> dict:
        """Pares homólogos com WKT dos perfis compatibilizados e escalar k (método linear)."""
        norm_idx = self._normalization_method_index()
        norm_label = self.list_norm_type[norm_idx] if 0 <= norm_idx < len(self.list_norm_type) else ''
        dm = getattr(self, 'dic_match', None) or {}
        if not dm:
            rebuilt = self._dic_match_from_match_lines_layer()
            if rebuilt is not None:
                dm = rebuilt
        if not dm:
            return {
                'norm_index': norm_idx,
                'norm_label': norm_label,
                'pairs': [],
                'linear_scalars': [],
            }

        type_r = self.dic_prj['dems'][0]['type']
        type_t = self.dic_prj['dems'][1]['type']
        pairs = []
        linear_scalars = []
        pair_n = 0
        wkt_dec = 2

        for tag_ in sorted(dm.keys()):
            layer_r = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_r}__')
            layer_t = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_t}__')
            if layer_r is None or layer_t is None or not layer_r.isValid() or not layer_t.isValid():
                continue
            layer_ref_name = layer_r.name()
            layer_test_name = layer_t.name()
            for vet_ in dm[tag_]:
                if not vet_ or len(vet_) < 2:
                    continue
                try:
                    fid_r = int(vet_[0])
                    fid_t = int(vet_[1])
                except (TypeError, ValueError):
                    continue
                fr = layer_r.getFeature(fid_r)
                ft = layer_t.getFeature(fid_t)
                if not fr.hasGeometry() or not ft.hasGeometry():
                    continue
                gr = QgsGeometry(fr.geometry())
                gt = QgsGeometry(ft.geometry())
                profiles = build_compatibilized_profile_geometries(gr, gt, norm_idx)
                pair_n += 1
                entry = {
                    'n': pair_n,
                    'morph_tag': tag_,
                    'ref_id': fid_r,
                    'layer_ref': layer_ref_name,
                    'wkt_ref': '',
                    'test_id': fid_t,
                    'layer_test': layer_test_name,
                    'wkt_test': '',
                }
                if profiles:
                    entry['wkt_ref'] = _profile_geometry_wkt_for_report(
                        profiles['geom_prof_r'], decimals=wkt_dec)
                    entry['wkt_test'] = _profile_geometry_wkt_for_report(
                        profiles['geom_prof_t'], decimals=wkt_dec)
                    k_t = profiles.get('k_t')
                    if norm_idx == 0 and k_t is not None and math.isfinite(k_t):
                        entry['scalar_k'] = k_t
                        linear_scalars.append(k_t)
                pairs.append(entry)

        return {
            'norm_index': norm_idx,
            'norm_label': norm_label,
            'pairs': pairs,
            'linear_scalars': linear_scalars,
        }

    def _build_report_pairs_section(self) -> dict:
        data = self._collect_homologous_pairs_profile_data()
        norm_idx = data.get('norm_index', 0)
        norm_label = data.get('norm_label') or ''
        pairs = data.get('pairs') or []
        scalars = data.get('linear_scalars') or []

        rows = []
        if norm_label:
            rows.append({
                'label': self.tr('Método de normalização de progressivas'),
                'value': norm_label,
            })
        if pairs:
            rows.append({
                'label': self.tr('Total de pares'),
                'value': str(len(pairs)),
            })
            rows.append({
                'label': self.tr('Ficheiro WKT dos perfis'),
                'value': '',
            })
        if norm_idx == 0 and scalars:
            rows.append({
                'is_group': True,
                'label': self.tr('Escalar (k)'),
            })
            dec = 2
            rows.extend([
                {
                    'label': self.tr('Média'),
                    'value': _format_report_scalar_k(statistics.mean(scalars), decimals=dec),
                },
                {
                    'label': self.tr('Mínima'),
                    'value': _format_report_scalar_k(min(scalars), decimals=dec),
                },
                {
                    'label': self.tr('Máxima'),
                    'value': _format_report_scalar_k(max(scalars), decimals=dec),
                },
                {
                    'label': self.tr('Desvio Padrão'),
                    'value': _format_report_scalar_k(
                        statistics.stdev(scalars) if len(scalars) >= 2 else 0.0,
                        decimals=dec,
                    ),
                },
            ])

        empty_msg = ''
        if not pairs:
            empty_msg = self.tr('(sem pares homólogos definidos — execute a correspondência de linhas.)')

        return {
            'title': self.tr('6. Pares homólogos — estatísticas'),
            'header': [self.tr('Opção'), self.tr('Valor')],
            'rows': rows,
            'norm_caption': self.tr('Método de normalização de progressivas'),
            'norm_label': norm_label,
            'wkt_file_caption': self.tr('Ficheiro WKT dos perfis'),
            'empty_message': empty_msg,
        }

    def _profiles_wkt_export_labels(self) -> dict:
        return {
            'datetime': self.tr('Data/hora'),
            'project_file': self.tr('Ficheiro de projeto'),
            'norm_caption': self.tr('Método de normalização de progressivas'),
            'pair': self.tr('Par'),
            'ref_id': self.tr('ref_id'),
            'layer_ref': self.tr('camada_ref'),
            'wkt_ref': self.tr('Perfil ref. (WKT compatibilizado)'),
            'test_id': self.tr('test_id'),
            'layer_test': self.tr('camada_test'),
            'wkt_test': self.tr('Perfil teste (WKT compatibilizado)'),
            'scalar_k': self.tr('Escalar k (linear)'),
        }

    def _build_profiles_wkt_txt(self) -> str:
        data = self._collect_homologous_pairs_profile_data()
        when = QDateTime.currentDateTime().toString('yyyy-MM-dd HH:mm:ss')
        return format_profiles_wkt_txt(
            data,
            datetime_str=when,
            project_file=self.dic_prj.get('project_file') or '',
            labels=self._profiles_wkt_export_labels(),
        )

    def _collect_report_snapshot(self) -> dict:
        """Dados completos do relatório (fonte única para TXT v1 e PDF)."""
        when = QDateTime.currentDateTime().toString('yyyy-MM-dd HH:mm:ss')
        proj_path = self.dic_prj.get('project_file') or ''
        crs_ = self.crs_epsg or self.tr('(não definido)')

        sly = self.cbx_study_area_layer.currentLayer()
        study_ly = sly.name() if isinstance(sly, QgsVectorLayer) else self.tr('(nenhuma)')

        dem_rows = []
        for i in (0, 1):
            cbx = self.dic_prj['dems'][i]['obj_cbx']
            ly = cbx.currentLayer() if cbx else None
            label = str(self.dic_prj['dems'][i]['type'])
            if isinstance(ly, QgsRasterLayer) and ly.isValid():
                dem_rows.append({
                    'role': label,
                    'name': ly.name(),
                    'source': ly.source()[:500],
                })
            else:
                dem_rows.append({
                    'role': label,
                    'name': self.tr('(não selecionado)'),
                    'source': '',
                })

        param_groups = []
        dlg = self.settings_dlg
        is_ce90 = self._accuracy_standard_index() == ACCURACY_STANDARD_CE90
        for sk, block in dlg.dic_param.items():
            if not isinstance(sk, str) or not sk.startswith('step_'):
                continue
            if not isinstance(block, dict) or 'fields' not in block:
                continue
            fields = []
            for fk, meta in block['fields'].items():
                if not isinstance(meta, dict):
                    continue
                if sk == 'step_buffers':
                    if is_ce90 and fk in ('max_scale', 'min_scale'):
                        continue
                    if not is_ce90 and fk in ('ce90_max_h', 'ce90_max_v'):
                        continue
                fields.append({
                    'label': meta.get('label', fk),
                    'value': self._format_param_value_for_report(meta),
                })
            if not fields:
                continue
            param_groups.append({
                'label': block.get('label', sk),
                'fields': fields,
            })

        em_raw = (self._panel_stats_cache.get('ext_match') or '').strip()
        em_m = self._float_from_panel_str(em_raw)
        if em_m is not None and em_m > 0:
            ext_match_disp = str(round(em_m / 1000.0, 1))
        else:
            ext_match_disp = '—'
        stats_rows = [
            {
                'label': self.tr('Área de estudo'),
                'value': f'{self._panel_stats_cache.get("area") or "—"} km²',
            },
            {
                'label': self.tr('Extensão mínima da amostra'),
                'value': f'{self._panel_stats_cache.get("ext_min") or "—"} km',
            },
            {
                'label': self.tr('Extensão da amostra'),
                'value': f'{ext_match_disp} km',
            },
            {
                'label': self.tr('Número de pares homólogos'),
                'value': str(self._panel_stats_cache.get('pair_nr') or '—'),
            },
        ]
        if is_ce90:
            dec = self._ce90_threshold_decimals()
            gsd = self._test_dem_gsd()
            if gsd is None:
                meta = getattr(self, '_ce90_meta', None) or {}
                try:
                    gsd = float(meta.get('gsd') or 0.0) or None
                except (TypeError, ValueError):
                    gsd = None
            stats_rows.append({
                'label': self.tr('Precisão do limiar CE90/LE90'),
                'value': self.tr('{0} casa(s) decimal(is)').format(dec),
            })
            if gsd:
                stats_rows.append({
                    'label': self.tr('Pixel do MDE de teste'),
                    'value': f'{gsd:.3f} m',
                })

        plan_rows = getattr(self, '_pec_report_plan_rows', None) or []
        alt_rows = getattr(self, '_pec_report_alt_rows', None) or []
        pec_empty = self.tr(
            '(ainda não há resultados de PEC nesta sessão — execute a análise até ao fim.)')

        return {
            'meta': {
                'title': self.tr('Relatório — MDE AP — Acurácia Posicional'),
                'datetime': when,
                'project_file': proj_path,
                'crs': crs_,
                'labels': {
                    'title': self.tr('Título'),
                    'datetime': self.tr('Data/hora'),
                    'project_file': self.tr('Ficheiro de projeto'),
                    'crs': self.tr('CRS de referência (análise)'),
                    'option': self.tr('Opção'),
                    'value': self.tr('Valor'),
                },
            },
            'sections': {
                'location': {
                    'title': self.tr('1. Localização da área de estudo'),
                    'header': [
                        self.tr('Envelope'),
                        'Xmin',
                        'Ymin',
                        'Xmax',
                        'Ymax',
                    ],
                    'rows': self._report_extent_intersection_rows(),
                },
                'workflow': {
                    'title': self.tr('2. Fluxo de trabalho'),
                    'header': [self.tr('Opção'), self.tr('Valor')],
                    'rows': [
                        {'option': self.tr('Definição da área de estudos'), 'value': self.cbx_workflow_study.currentText()},
                        {'option': self.tr('Pares homólogos'), 'value': self.cbx_workflow_pairs.currentText()},
                        {'option': self.tr('Tratamento de outliers'), 'value': self.cbx_workflow_outliers.currentText()},
                        {'option': self.tr('Camada polígono (se aplicável)'), 'value': study_ly},
                    ],
                },
                'dems': {
                    'title': self.tr('3. Modelos digitais de elevação (MDE)'),
                    'header': [
                        self.tr('Papel'),
                        self.tr('Nome'),
                        self.tr('Fonte (início)'),
                    ],
                    'rows': dem_rows,
                },
                'params': {
                    'title': self.tr('4. Parâmetros de processamento'),
                    'header': [self.tr('Parâmetro'), self.tr('Valor')],
                    'groups': param_groups,
                },
                'stats': {
                    'title': self.tr('5. Estatísticas do painel'),
                    'header': [self.tr('Opção'), self.tr('Valor')],
                    'rows': stats_rows,
                },
                'pairs': self._build_report_pairs_section(),
                'pec': {
                    'title': (
                        self.tr('7. Resultados CE90 / LE90')
                        if self._accuracy_standard_index() == ACCURACY_STANDARD_CE90
                        else self.tr('7. Resultados PEC')
                    ),
                    'intro': (getattr(self, '_pec_report_pec_intro', '') or '').strip(),
                    'header_labels': self._pec_results_header_labels(),
                    'plan': {
                        'title': (
                            self.tr('7.1 CE90')
                            if self._accuracy_standard_index() == ACCURACY_STANDARD_CE90
                            else self.tr('7.1 PEC Planimétrico')
                        ),
                        'header_rows': list(self._pec_results_table_head_txt_rows(altimetric=False)),
                        'data_rows': [
                            self._pec_row_snapshot_entry(row, 'H')
                            for row in plan_rows
                        ],
                    },
                    'alt': {
                        'title': (
                            self.tr('7.2 LE90')
                            if self._accuracy_standard_index() == ACCURACY_STANDARD_CE90
                            else self.tr('7.2 PEC Altimétrico')
                        ),
                        'header_rows': list(self._pec_results_table_head_txt_rows(altimetric=True)),
                        'data_rows': [
                            self._pec_row_snapshot_entry(row, 'V')
                            for row in alt_rows
                        ],
                    },
                    'empty_message': pec_empty if not plan_rows and not alt_rows else '',
                },
            },
        }

    def _build_full_report_txt(self) -> str:
        """Relatório completo em TXT v1 (parseável → PDF sem correr o pipeline)."""
        return format_full_report_txt(self._collect_report_snapshot())

    def _audit_report_flags(self):
        """Devolve (horizontal, vertical) a partir dos parâmetros (0/1)."""
        try:
            fields = self.settings_dlg.dic_param['step_audit_report']['fields']
        except (KeyError, TypeError, AttributeError):
            return False, False
        def _on(key):
            try:
                return bool(int(fields[key].get('value', 0)))
            except (TypeError, ValueError, KeyError):
                return False
        return _on('audit_horizontal'), _on('audit_vertical')

    def _audit_test_model_name(self) -> str:
        """Nome curto do MDE de teste para ficheiros Audit_*.pdf (nunca path TEMP)."""
        dems = (self.dic_prj or {}).get('dems') or {}
        test = dems.get(1) or {}

        def _clean_token(raw: str) -> str:
            s = (raw or '').strip()
            if not s:
                return ''
            # Path completo / URI com |layername=…
            base = s.split('|')[0].replace('\\', '/')
            if '/' in base or base.lower().endswith(('.tif', '.tiff', '.img', '.asc', '.vrt')):
                s = os.path.splitext(os.path.basename(base))[0]
            # Tokens do Processing TEMP (clip intermédio) — inválidos para o nome do audit.
            low = s.lower()
            if 'processing_' in low or low.startswith('output') or 'appdata_local_temp' in low:
                return ''
            if low in ('teste', 'test', 'referencia', 'referência', 'reference'):
                return ''
            return s.strip() or ''

        name = _clean_token(str(test.get('model') or ''))
        if not name:
            layer = None
            obj = test.get('obj_cbx')
            if obj is not None and hasattr(obj, 'currentLayer'):
                layer = obj.currentLayer()
            if layer is not None and layer.isValid():
                name = _clean_token(layer.source()) or _clean_token(layer.name())
        if not name:
            name = _clean_token(str(test.get('type') or '')) or 'TESTE'
        return name

    def _pec_v_tables_for_audit(self, scales):
        """Tabelas PEC-V / EQ no formato esperado pelo gerador de PDF."""
        pec_v_table = {}
        eq_v_table = {}
        if self._accuracy_standard_index() == ACCURACY_STANDARD_CE90:
            for scale in scales:
                try:
                    r = float(scale)
                except (TypeError, ValueError):
                    continue
                eq_v_table[r] = r
                pec_v_table[r] = {
                    CLASS_LE90: {
                        'pec': r,
                        'ep': round(r * EP_RATIO_V, self._ce90_threshold_decimals()),
                    }
                }
            return pec_v_table, eq_v_table
        for scale in scales:
            scale = int(scale)
            eq = self.dic_pec_v.get(scale)
            if eq is None:
                continue
            eq_v_table[scale] = eq
            pec_v_table[scale] = {}
            for class_ in self.dic_pec_mm.get('V', {}):
                pec_v_table[scale][class_] = {
                    'pec': float(eq) * float(self.dic_pec_mm['V'][class_]['pec']),
                    'ep': float(eq) * float(self.dic_pec_mm['V'][class_]['ep']),
                }
        return pec_v_table, eq_v_table

    def _dm_by_scale_index_from_dic_values(self, dic_values):
        """(fid_r, fid_t) → {scale: {class: {DM_V: …}}}."""
        index = {}
        if not dic_values:
            return index
        ce90 = self._accuracy_standard_index() == ACCURACY_STANDARD_CE90
        for scale, by_class in dic_values.items():
            try:
                scale_i = float(scale) if ce90 else int(scale)
            except (TypeError, ValueError):
                continue
            for class_, by_count in (by_class or {}).items():
                for _count, rec in (by_count or {}).items():
                    if not isinstance(rec, dict):
                        continue
                    try:
                        fid_r = int(rec.get('fid_r'))
                        fid_t = int(rec.get('fid_t'))
                    except (TypeError, ValueError):
                        continue
                    dm_v = rec.get('dm_v')
                    try:
                        dm_v = float(dm_v)
                        if not math.isfinite(dm_v):
                            dm_v = None
                    except (TypeError, ValueError):
                        dm_v = None
                    dm_h = rec.get('dm_h')
                    try:
                        dm_h = float(dm_h)
                        if not math.isfinite(dm_h):
                            dm_h = None
                    except (TypeError, ValueError):
                        dm_h = None
                    index.setdefault((fid_r, fid_t), {}).setdefault(scale_i, {})[str(class_)] = {
                        'DM_V': dm_v,
                        'DM_H': dm_h,
                    }
        return index

    def _build_audit_pair_specs(self, dic_values=None):
        """Pares (geometrias + DM_V) para o relatório de auditoria vertical."""
        dm = getattr(self, 'dic_match', None) or {}
        if not dm:
            rebuilt = self._dic_match_from_match_lines_layer()
            if rebuilt is not None:
                dm = rebuilt
        if not dm:
            return []

        dm_index = self._dm_by_scale_index_from_dic_values(dic_values)
        type_r = self.dic_prj['dems'][0]['type']
        type_t = self.dic_prj['dems'][1]['type']
        pairs = []
        for tag_ in sorted(dm.keys()):
            layer_r = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_r}__')
            layer_t = self._resolve_limit_layer_for_editing(f'__{tag_}_Z_{type_t}__')
            if layer_r is None or layer_t is None or not layer_r.isValid() or not layer_t.isValid():
                continue
            for vet_ in dm[tag_]:
                if not vet_ or len(vet_) < 2:
                    continue
                try:
                    fid_r = int(vet_[0])
                    fid_t = int(vet_[1])
                except (TypeError, ValueError):
                    continue
                fr = layer_r.getFeature(fid_r)
                ft = layer_t.getFeature(fid_t)
                if not fr.hasGeometry() or not ft.hasGeometry():
                    continue
                pairs.append({
                    'id_ref': fid_r,
                    'id_test': fid_t,
                    'layer_ref': layer_r.name(),
                    'geom_r': orient_line_high_to_low(QgsGeometry(fr.geometry())),
                    'geom_t': orient_line_high_to_low(QgsGeometry(ft.geometry())),
                    'dm_by_scale': dm_index.get((fid_r, fid_t), {}),
                })
        return pairs

    def _pec_h_tables_for_audit(self, scales):
        """Tabela PEC-H (m) por escala/classe: scale × pec_mm (ou limiar CE90)."""
        pec_h_table = {}
        if self._accuracy_standard_index() == ACCURACY_STANDARD_CE90:
            for scale in scales:
                try:
                    r = float(scale)
                except (TypeError, ValueError):
                    continue
                pec_h_table[r] = {
                    CLASS_CE90: {
                        'pec': r,
                        'ep': round(r * EP_RATIO_H, self._ce90_threshold_decimals()),
                    }
                }
            return pec_h_table
        for scale in scales:
            scale = int(scale)
            pec_h_table[scale] = {}
            for class_ in self.dic_pec_mm.get('H', {}):
                pec_h_table[scale][class_] = {
                    'pec': float(scale) * float(self.dic_pec_mm['H'][class_]['pec']),
                    'ep': float(scale) * float(self.dic_pec_mm['H'][class_]['ep']),
                }
        return pec_h_table

    def _audit_scales_for_dimension(self, dic_values, dimension='H'):
        """Escalas (PEC-PCD) ou limiares (CE90/LE90) para auditoria."""
        if self._accuracy_standard_index() == ACCURACY_STANDARD_CE90:
            want = CLASS_CE90 if dimension == 'H' else CLASS_LE90
            scales = []
            if dic_values:
                for scale_, classes in dic_values.items():
                    if want in classes:
                        try:
                            scales.append(float(scale_))
                        except (TypeError, ValueError):
                            continue
            return sorted(set(scales))
        return self.get_list_scale() or []

    def _audit_progress_begin(self, total, msg=None):
        """Reinicia as duas barras (Referência/Teste) para a geração de auditoria."""
        total = max(int(total or 0), 1)
        label = msg or self.tr('Auditoria')
        for key in (0, 1):
            self.update_bar({'key': key, 'quant': total})
            self.update_bar({
                'key': key,
                'value': 0,
                'msg': label,
                'progress_only': True,
            })
        QApplication.processEvents()

    def _audit_progress_tick(self, value, total, msg):
        """Atualiza as duas barras com o passo atual da auditoria."""
        for key in (0, 1):
            self.update_bar({
                'key': key,
                'value': int(value),
                'msg': str(msg),
                'progress_only': True,
            })
        QApplication.processEvents()

    def _audit_progress_end(self, total, msg=None):
        """Marca as duas barras como concluídas."""
        total = max(int(total or 0), 1)
        label = msg or self.tr('Auditoria concluída')
        for key in (0, 1):
            self.update_bar({'key': key, 'end': total, 'msg': label})
        QApplication.processEvents()

    def export_audit_horizontal_pdfs(self, dic_values=None, report_ts=None, progress=None):
        """Gera Audit_horizontal_{modelo}_{escala}_{ts}.pdf."""
        try:
            self.settings_dlg.flush_widgets_to_dic_param(log_values=False)
        except Exception:
            pass
        audit_h, _v = self._audit_report_flags()
        if not audit_h:
            return []

        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            self.log_message(
                self.tr('Defina um projeto (.pa.gpkg) para exportar a auditoria.'),
                'ERROR',
            )
            return []
        data_dir = project_data_dir(pf)
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError as e:
            self.log_message(
                self.tr('Não foi possível criar a pasta do projeto: {0}').format(e),
                'ERROR',
            )
            return []

        scales = self._audit_scales_for_dimension(dic_values, 'H')
        if not scales:
            self.log_message(
                self.tr('Auditoria horizontal: nenhuma escala definida.'), 'WARNING'
            )
            return []

        pairs = self._build_audit_pair_specs(dic_values=dic_values)
        if not pairs:
            self.log_message(
                self.tr('Auditoria horizontal: sem pares homólogos.'), 'WARNING'
            )
            return []

        try:
            from .mod_gen_audit import (  # noqa: WPS433
                generate_audit_horizontal_pdfs_from_pairs,
            )
        except Exception as e:
            self.log_message(
                self.tr('Falha ao carregar gerador de auditoria: {0}').format(e),
                'ERROR',
            )
            return []

        pec_h_table = self._pec_h_tables_for_audit(scales)
        norm_type = self._normalization_method_index()
        test_name = self._audit_test_model_name()
        ts = report_ts or getattr(self, '_last_report_ts', None)
        if not ts:
            ts = QDateTime.currentDateTime().toString('yyyy-MM-dd_HHmm')
        self.log_message(
            self.tr('A gerar relatório de auditoria horizontal ({0} pares)…').format(
                len(pairs)
            ),
            'INFO',
        )
        try:
            outputs = generate_audit_horizontal_pdfs_from_pairs(
                out_dir=data_dir,
                test_name=test_name,
                pairs=pairs,
                scales=scales,
                norm_type=norm_type,
                pec_h_table=pec_h_table,
                timestamp=ts,
                log=lambda msg: self.log_message(str(msg), 'INFO'),
                progress=progress,
            )
        except Exception as e:
            self.log_message(
                self.tr('Falha na auditoria horizontal: {0}').format(e),
                'ERROR',
            )
            return []

        for out_path, drawn, _skipped in outputs:
            self.log_message(
                self.tr('Auditoria horizontal gravada: {0} ({1} páginas)').format(
                    out_path, drawn
                ),
                'INFO',
            )
        return outputs

    def export_audit_vertical_pdfs(self, dic_values=None, report_ts=None, progress=None):
        """Gera Audit_vertical_{modelo}_{linear|proximidade}_{escala}_{ts}.pdf."""
        try:
            self.settings_dlg.flush_widgets_to_dic_param(log_values=False)
        except Exception:
            pass
        _h, audit_v = self._audit_report_flags()
        if not audit_v:
            return []

        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            self.log_message(
                self.tr('Defina um projeto (.pa.gpkg) para exportar a auditoria.'),
                'ERROR',
            )
            return []
        data_dir = project_data_dir(pf)
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError as e:
            self.log_message(
                self.tr('Não foi possível criar a pasta do projeto: {0}').format(e),
                'ERROR',
            )
            return []

        scales = self._audit_scales_for_dimension(dic_values, 'V')
        if not scales:
            self.log_message(self.tr('Auditoria vertical: nenhuma escala definida.'), 'WARNING')
            return []

        pairs = self._build_audit_pair_specs(dic_values=dic_values)
        if not pairs:
            self.log_message(self.tr('Auditoria vertical: sem pares homólogos.'), 'WARNING')
            return []

        try:
            from .mod_gen_audit import (  # noqa: WPS433
                generate_audit_pdfs_from_pairs,
            )
        except Exception as e:
            self.log_message(
                self.tr('Falha ao carregar gerador de auditoria: {0}').format(e),
                'ERROR',
            )
            return []

        pec_v_table, eq_v_table = self._pec_v_tables_for_audit(scales)
        norm_type = self._normalization_method_index()
        test_name = self._audit_test_model_name()
        ts = report_ts or getattr(self, '_last_report_ts', None)
        if not ts:
            ts = QDateTime.currentDateTime().toString('yyyy-MM-dd_HHmm')
        self.log_message(
            self.tr('A gerar relatório de auditoria vertical ({0} pares)…').format(len(pairs)),
            'INFO',
        )
        try:
            outputs = generate_audit_pdfs_from_pairs(
                out_dir=data_dir,
                test_name=test_name,
                pairs=pairs,
                scales=scales,
                norm_type=norm_type,
                pec_v_table=pec_v_table,
                eq_v_table=eq_v_table,
                timestamp=ts,
                log=lambda msg: self.log_message(str(msg), 'INFO'),
                progress=progress,
            )
        except Exception as e:
            self.log_message(
                self.tr('Falha na auditoria vertical: {0}').format(e),
                'ERROR',
            )
            return []

        for out_path, drawn, _skipped in outputs:
            self.log_message(
                self.tr('Auditoria vertical gravada: {0} ({1} páginas)').format(
                    out_path, drawn
                ),
                'INFO',
            )
        return outputs

    def _run_audit_exports_with_progress(self, dic_values=None, report_ts=None):
        """Gera H e/ou V atualizando as duas barras de progresso do painel."""
        audit_h, audit_v = self._audit_report_flags()
        if not audit_h and not audit_v:
            return
        pairs = self._build_audit_pair_specs(dic_values=dic_values)
        scales_h = self._audit_scales_for_dimension(dic_values, 'H') if audit_h else []
        scales_v = self._audit_scales_for_dimension(dic_values, 'V') if audit_v else []
        n_units = 0
        if pairs:
            if audit_h:
                n_units += len(pairs) * max(len(scales_h), 1)
            if audit_v:
                n_units += len(pairs) * max(len(scales_v), 1)
        total = n_units
        if total <= 0:
            # Ainda assim corre os exports (logs de aviso internos)
            if audit_h:
                self.export_audit_horizontal_pdfs(
                    dic_values=dic_values, report_ts=report_ts,
                )
            if audit_v:
                self.export_audit_vertical_pdfs(
                    dic_values=dic_values, report_ts=report_ts,
                )
            return

        state = {'i': 0}

        def _tick(msg):
            state['i'] += 1
            self._audit_progress_tick(state['i'], total, msg)

        self._audit_progress_begin(total, self.tr('Auditoria'))
        try:
            if audit_h:
                self.export_audit_horizontal_pdfs(
                    dic_values=dic_values,
                    report_ts=report_ts,
                    progress=_tick,
                )
            if audit_v:
                self.export_audit_vertical_pdfs(
                    dic_values=dic_values,
                    report_ts=report_ts,
                    progress=_tick,
                )
        finally:
            self._audit_progress_end(total, self.tr('Auditoria concluída'))

    def _build_pdf_report_html(self) -> str:
        return render_pdf_report_html(self._collect_report_snapshot())

    def export_project_reports_to(
        self,
        out_pdf: str = None,
        out_txt: str = None,
        *,
        margin_mm: float = None,
    ):
        """Gera PDF, TXT e HTML na pasta do projeto (ou caminhos indicados). Devolve (pdf_path, txt_path)."""
        if margin_mm is None:
            margin_mm = REPORT_PDF_MARGIN_MM
        pf = self.dic_prj.get('project_file')
        if not pf or not os.path.isfile(pf):
            self.log_message(
                self.tr('Defina um projeto (.pa.gpkg) para exportar o relatório.'), 'ERROR')
            return None, None
        data_dir = project_data_dir(pf)
        self.dic_prj['path'] = data_dir
        try:
            self.settings_dlg.flush_widgets_to_dic_param(log_values=False)
        except Exception:
            pass
        try:
            os.makedirs(data_dir, exist_ok=True)
        except OSError as e:
            self.log_message(
                self.tr('Não foi possível criar a pasta do projeto: {0}').format(e), 'ERROR')
            return None, None

        stem = os.path.basename(_strip_project_ext(pf)) or 'projeto'
        ts = QDateTime.currentDateTime().toString('yyyy-MM-dd_HHmm')
        self._last_report_ts = ts
        safe_stem = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in stem)[:80]

        if out_pdf:
            pdf_path = os.path.normpath(os.path.abspath(out_pdf))
            os.makedirs(os.path.dirname(pdf_path) or '.', exist_ok=True)
        else:
            fn = f'Relatorio_MDE_AP_{safe_stem}_{ts}.pdf'
            pdf_path = os.path.normpath(os.path.join(data_dir, fn))

        if out_txt:
            txt_path = os.path.normpath(os.path.abspath(out_txt))
            os.makedirs(os.path.dirname(txt_path) or '.', exist_ok=True)
        elif out_pdf:
            root, _ = os.path.splitext(pdf_path)
            txt_path = root + '.txt'
        else:
            fn = f'Relatorio_MDE_AP_{safe_stem}_{ts}.txt'
            txt_path = os.path.normpath(os.path.join(data_dir, fn))

        html_path = _companion_report_path(txt_path, '.html')

        snapshot = self._collect_report_snapshot()
        wkt_body = self._build_profiles_wkt_txt()
        wkt_path = None
        if wkt_body:
            fn_wkt = f'Perfis_WKT_MDE_AP_{safe_stem}_{ts}.txt'
            wkt_path = os.path.normpath(os.path.join(data_dir, fn_wkt))
            try:
                with open(wkt_path, 'w', encoding='utf-8') as f:
                    f.write(wkt_body)
            except OSError as e:
                self.log_message(
                    self.tr('Falha ao gerar ficheiro WKT dos perfis: {0} ({1})').format(wkt_path, e),
                    'ERROR',
                )
                wkt_path = None
            else:
                pairs_sec = snapshot.get('sections', {}).get('pairs') or {}
                wkt_name = os.path.basename(wkt_path)
                pairs_sec['wkt_file'] = wkt_name
                wkt_caption = pairs_sec.get('wkt_file_caption') or self.tr('Ficheiro WKT dos perfis')
                pair_rows = list(pairs_sec.get('rows') or [])
                updated = False
                for row in pair_rows:
                    if not row.get('is_group') and row.get('label') == wkt_caption:
                        row['value'] = wkt_name
                        updated = True
                        break
                if not updated:
                    insert_at = min(2, len(pair_rows))
                    pair_rows.insert(insert_at, {'label': wkt_caption, 'value': wkt_name})
                pairs_sec['rows'] = pair_rows
                snapshot.setdefault('sections', {})['pairs'] = pairs_sec

        html_doc = render_pdf_report_html(snapshot)
        txt_body = format_full_report_txt(snapshot)

        try:
            write_pdf_from_html_doc(html_doc, pdf_path, margin_mm=margin_mm)
        except Exception as e:
            self.log_message(self.tr('Falha ao gerar PDF: {0}').format(e), 'ERROR')
            pdf_path = None

        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(txt_body)
        except OSError as e:
            self.log_message(
                self.tr('Falha ao gerar relatório TXT: {0} ({1})').format(txt_path, e), 'ERROR')
            txt_path = None

        try:
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_doc)
        except OSError as e:
            self.log_message(
                self.tr('Falha ao gravar HTML do relatório: {0} ({1})').format(html_path, e),
                'WARNING',
            )

        if pdf_path:
            self.log_message(self.tr('Relatório PDF exportado: {0}').format(pdf_path), 'INFO')
        if txt_path:
            self.log_message(
                self.tr('Relatório TXT v1 exportado (parseável → PDF): {0}').format(txt_path),
                'INFO',
            )
        if wkt_path:
            self.log_message(
                self.tr('Ficheiro WKT dos perfis exportado: {0}').format(wkt_path), 'INFO')
        if os.path.isfile(html_path):
            self.log_message(self.tr('Relatório HTML exportado: {0}').format(html_path), 'INFO')
        if pdf_path and txt_path:
            self.log_message(
                self.tr('Relatórios na pasta do projeto: PDF + TXT (+ HTML se aplicável).'),
                'INFO',
            )
        if pdf_path and os.path.isfile(pdf_path):
            self._last_report_pdf_path = pdf_path
        return pdf_path, txt_path

    def export_project_pdf_report_to(self, out_path: str = None, *, margin_mm: float = None) -> str:
        """Gera PDF; `out_path` opcional (senão pasta de dados do projeto + timestamp)."""
        pdf_path, _ = self.export_project_reports_to(out_pdf=out_path, margin_mm=margin_mm)
        return pdf_path

    def export_project_pdf_report(self):
        """Gera PDF, TXT e HTML na pasta de dados do projeto."""
        pdf_path, _ = self.export_project_reports_to()
        return pdf_path

    def rms(self, vet_):
        vals = []
        for v in vet_:
            x = _coerce_finite_measurement_scalar(v)
            if x is not None:
                vals.append(x)
        n = len(vals)
        if n < 2:
            return float('nan')
        try:
            sun_ = math.fsum(x * x for x in vals)
            if not math.isfinite(sun_) or sun_ < 0:
                return float('nan')
            return math.sqrt(sun_ / (n - 1))
        except (OverflowError, OSError, ValueError, ArithmeticError):
            return float('nan')

    def perc_pec(self, vet_, pec_):
        vals = []
        for v in vet_:
            x = _coerce_finite_measurement_scalar(v)
            if x is not None:
                vals.append(x)
        return perc_pec_quant(vals, pec_)

    def _resolve_limit_layer_for_editing(self, layer_name: str):
        """Camada de limite no projeto (válida) ou carregada do .pa.gpkg; remove stubs inválidos."""
        proj = QgsProject.instance()
        gpkg_n = self._active_gpkg_path()
        display = self._layer_display_name(layer_name)
        for nm in (display, layer_name):
            for lyr in list(proj.mapLayersByName(nm)):
                if not lyr.isValid():
                    if gpkg_n and map_layer_gpkg_path(lyr) == gpkg_n:
                        try:
                            proj.removeMapLayer(lyr.id())
                        except Exception:
                            pass
                    continue
                if gpkg_n and map_layer_gpkg_path(lyr) != gpkg_n:
                    continue
                return lyr
        lyr = self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)
        if lyr is not None and lyr.isValid():
            return lyr
        if self._ensure_limit_vector_tables_in_mdepa(self.crs_epsg):
            lyr = self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)
            if lyr is not None and lyr.isValid():
                return lyr
        return None

    def update_bar(self, dic_):
        if dic_.get('logonly', None):
            self.log_message(dic_['logonly'])
            return
        key_ = dic_['key']
        prog_bar = self.dic_prj['dems'][key_]['obj_prog_bar']
        palette = QPalette()
        palette.setColor(QPalette.Highlight, QColor(Qt.cyan))
        prog_bar.setPalette(palette)
        if 'error' in dic_:
            prog_bar.setFormat(str(dic_['error']))
            palette.setColor(QPalette.Highlight, QColor(Qt.red))
            prog_bar.setPalette(palette)
            if key_ in (0, 1):
                self._end_buffers_map_canvas_freeze(refresh=False)
            if key_ == 3:
                self.log_message(
                    self.tr('Buffer - {0}').format(dic_['error']), level='ERROR')
            else:
                self.log_message(
                    self.tr('{0} {1} - {2}').format(
                        self.dic_prj['dems'][dic_['key']]['type'],
                        dic_['value'],
                        dic_['error']),
                    level='ERROR')
        elif 'warn' in dic_:
            prog_bar.setFormat(str(dic_['warn']))
            palette.setColor(QPalette.Highlight, QColor(Qt.lightGray))
            prog_bar.setPalette(palette)
            if dic_.get('log_warning'):
                self.log_message(str(dic_['log_warning']), 'WARNING')
        elif 'quant' in dic_:
            prog_bar.setRange(0, dic_['quant'])
            prog_bar.setValue(0)
            # self.log.info(True, f"set range {key_} 0 - {dic_['quant']}", pretty=True)
            palette.setColor(QPalette.Highlight, QColor(Qt.yellow))
            prog_bar.setPalette(palette)
        elif 'value' in dic_:
            prog_bar.setValue(dic_['value'])
            if dic_.get('progress_only'):
                prog_bar.setFormat(f"{dic_['value']} - {dic_['msg']}")
                return
            prog_bar.setFormat(f"{dic_['value']} - {dic_['msg']}")
            if 'feats_batch' in dic_:
                self._append_buffer_feats_batch(dic_['feats_batch'], repaint=False)
                return
            type_ = self.dic_prj['dems'][dic_['key']]['type']
            self.log_message(
                self.tr('{0} {1} - {2}').format(type_, dic_['value'], dic_['msg']))
            # print('dic_:', dic_)
            if 'feat' in dic_:
                if dic_['value'] == 6:
                    layer_name = f'__Limit_{self.dic_prj["dems"][key_]["type"]}__'
                    layer = self._resolve_limit_layer_for_editing(layer_name)
                    if layer is None:
                        self.log_message(
                            tr_ui('Camada de limite indisponível: {0}').format(layer_name),
                            'ERROR')
                        return
                    count = layer.featureCount()
                    feat_ = dic_['feat']
                    feat_.setAttributes([count + 1])
                    # print(feat_, feat_.geometry())

                    layer.startEditing()
                    layer.addFeature(feat_)
                    try:
                        ok = layer.commitChanges()
                    except Exception as e:
                        layer.rollBack()
                        self.log_message(
                            tr_ui('Falha ao gravar limite ({0}): {1}').format(layer_name, e),
                            'ERROR')
                        return
                    if not ok:
                        errs = layer.commitErrors() if hasattr(layer, 'commitErrors') else []
                        self.log_message(
                            tr_ui('commitChanges falhou em {0}: {1}').format(
                                layer_name, '; '.join(errs) if errs else '?'),
                            'ERROR')
                        return
                    layer.updateExtents()
                    layer.triggerRepaint()

                    self.dic_prj['dems'][dic_['key']]['geom_status'] = True
                    self.run_polygon_intersection()
            elif 'feats' in dic_:
                # Legado: dicionário feat_br/feat_bt/feat_i num único passo
                self._append_buffer_feats_batch(list(dic_['feats'].values()), repaint=False)
            elif 'layer' in dic_:
                if isinstance(dic_['layer']['gpkg'], str):
                    datasource = ogr.Open(dic_['layer']['gpkg'])
                    for i in range(datasource.GetLayerCount()):
                        layer = datasource.GetLayerByIndex(i)
                    layer_prefix = datasource.GetLayerByIndex(0).GetName()
                    layer = self.get_gpkg_layer(prefix_=layer_prefix, gpkg_path=dic_['layer']['gpkg'], show=False)
                else:
                    layer = dic_['layer']['gpkg']
                layer_name = f'__{dic_["layer"]["type"]}_{self.dic_prj["dems"][key_]["type"]}__'
                options = QgsVectorFileWriter.SaveVectorOptions()
                options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
                output_fields = QgsFields()
                for field in layer.fields():
                    # Check if the field name is 'fid' (case-insensitive check for robustness)
                    if field.name().lower() != 'fid':
                        output_fields.append(QgsField(field.name(), field.type()))  # Append
                options.fields = output_fields
                options.layerName = layer_name
                QgsVectorFileWriter.writeAsVectorFormat(
                    layer=layer,
                    fileName=self.gpkg_path,
                    options=options)
                normalize_project_pa_file(self.gpkg_path)
                self.get_gpkg_layer(prefix_=layer_name, gpkg_path=self.gpkg_path)
            if 'start_task' in dic_:
                if key_ == 0:
                    self.define_morphology(1)
                elif key_ == 1:
                    self.matching_lines()
            if 'model' in dic_:
                self.dic_prj["dems"][key_]['model'] = dic_['model']
        elif 'dic_values' in dic_:
            self._finalize_buffers_map_display()
            dv = dic_['dic_values']
            if 'ce90_meta' in dic_:
                self._ce90_meta = dic_.get('ce90_meta')
            om = self.cbx_workflow_outliers.currentIndex()
            self._apply_outlier_workflow(dv)
            if om == 1:
                n_out = self._count_outliers_flagged(dv)
                QMessageBox.information(
                    self,
                    self.tr('Tratamento de outliers'),
                    self.tr(
                        'Foram identificados {0} valores atípicos (excluídos do cálculo PEC). '
                        'Prima OK para continuar.').format(n_out),
                )
            self.calc_pec(dv)
            if self.gpkg_path:
                save_pipeline_last_ok_snapshot(self.gpkg_path, self._flatten_run_snapshot())
            pdf_path, txt_path = self.export_project_reports_to()
            if not pdf_path and not txt_path:
                self.log_message(self.tr('Falha ao exportar relatórios.'), 'ERROR')
            elif self.cb_open_report.isChecked() and pdf_path:
                self._open_report_file(pdf_path)
            try:
                report_ts = getattr(self, '_last_report_ts', None)
                self._run_audit_exports_with_progress(
                    dic_values=dv,
                    report_ts=report_ts,
                )
            except Exception as e:
                self.log_message(
                    self.tr('Falha na auditoria: {0}').format(e),
                    'ERROR',
                )
            # print(dic_['dic_values'])
        elif 'end' in dic_:
            palette.setColor(QPalette.Highlight, QColor(Qt.darkGreen))
            prog_bar.setValue(dic_['end'])
            prog_bar.setFormat(dic_['msg'])
            prog_bar.setPalette(palette)
            # self.db.commit_()

    def apply_ui_language(
        self,
        rebuild_settings: bool = False,
        refresh_open_settings: bool = False,
        refresh_open_language: bool = False,
    ):
        """Recarrega traduções e actualiza textos visíveis após mudança de idioma."""
        plugin = self.main
        locale_code = saved_ui_locale() or LOCALE_AUTO
        if plugin:
            plugin.translator = install_plugin_translator(locale_code)
            plugin.name_ = tr_ui(PLUGIN_DISPLAY_NAME)
            if getattr(plugin, 'dock1', None):
                plugin.dock1.setWindowTitle(f'{plugin.name_}.')

        self._refresh_ui_language_button()

        self.dic_lb_texts = {
            'area': tr_ui('Área de estudo: {} km²'),
            'ext_min': tr_ui('Extensão mínima da amostra: {} km'),
            'ext_match': tr_ui('Extensão da Amostra: {} km'),
            'pair_nr': tr_ui('Número de pares homólogos: {}'),
        }
        self.list_norm_type = [
            tr_ui('Linear'), tr_ui('Por Proximidade'), tr_ui('Sem Normalização')]
        self.list_accuracy_standard = [
            tr_ui('Padrão Brasileiro - PEC PCD'),
            tr_ui('CE90 e LE90'),
        ]
        self.list_dm_formula = [
            tr_ui('Equação original (eq:dm-buffer-duplo)'),
            tr_ui('Nova equação (eq:dm-buffer-duplo-media)'),
        ]
        self.list_dm_formula_tooltips = [
            tr_ui(
                'dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ\n\n'
                'dmᵢ — discrepância média do par i\n'
                'π — constante pi\n'
                'x — PEC (raio do buffer) da escala/classe\n'
                'A₁ — área do buffer da feição de teste\n'
                'A₂ — área do buffer da feição de referência\n'
                'A₃ — área da interseção dos buffers'
            ),
            tr_ui(
                'dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)\n\n'
                'A média (A₁ + A₂)/2 entra no numerador (no lugar de A₂) e no '
                'denominador (no lugar de A₁), tratando os dois erros de extensão '
                'com o mesmo peso.\n\n'
                'dmᵢ — discrepância média do par i\n'
                'π — constante pi\n'
                'x — PEC (raio do buffer) da escala/classe\n'
                'A₁ — área do buffer da feição de teste\n'
                'A₂ — área do buffer da feição de referência\n'
                'A₃ — área da interseção dos buffers'
            ),
        ]

        self.lb_title_proj.setText(self.tr('Projeto (.pa.gpkg):'))
        self.pb_config.setToolTip(self.tr('Config'))
        self.pb_lang.setToolTip(self.tr('Alterar idioma da interface'))
        self.pb_project_new.setToolTip(self.tr('Novo projeto…'))
        self.pb_project_open.setToolTip(self.tr('Abrir projeto…'))
        self.lb_study_layer.setText(self.tr('Camada polígono (área de estudo):'))
        self.pb_proc.setText(self.tr('Avaliar'))
        self.lb_log.setText(self.tr('LOG:'))
        if getattr(self, 'cb_open_report', None):
            self.cb_open_report.setText(self.tr('Abrir o relatório'))
            self._refresh_open_report_checkbox_ui()
        if getattr(self, 'lb_dem_ref', None):
            self.lb_dem_ref.setText(self.tr('MDE de referência:'))
        if getattr(self, 'lb_dem_test', None):
            self.lb_dem_test.setText(self.tr('MDE de teste:'))
        if getattr(self, 'lb_wf_study', None):
            self.lb_wf_study.setText(self.tr('Definição da área de estudos:'))
        if getattr(self, 'lb_wf_pairs', None):
            self.lb_wf_pairs.setText(self.tr('Seleção de pares homólogos:'))
        if getattr(self, 'lb_wf_outliers', None):
            self.lb_wf_outliers.setText(self.tr('Tratamento de outliers:'))

        def _reload_combo(combo, texts):
            idx = combo.currentIndex()
            combo.blockSignals(True)
            combo.clear()
            combo.addItems(texts)
            if 0 <= idx < combo.count():
                combo.setCurrentIndex(idx)
            combo.blockSignals(False)

        _reload_combo(self.cbx_workflow_study, [
            self.tr('Calcular pela interseção dos MDEs'),
            self.tr('Editar após interseção'),
            self.tr('Selecionar de uma camada'),
        ])
        _reload_combo(self.cbx_workflow_pairs, [
            self.tr('Automática'),
            self.tr('Revisar'),
        ])
        _reload_combo(self.cbx_workflow_outliers, [
            self.tr('Remover automaticamente'),
            self.tr('Avaliar individualmente'),
            self.tr('Usar todos'),
        ])

        for key_ in self.dic_prj['dems']:
            obj_pb = self.dic_prj['dems'][key_].get('obj_pb')
            if obj_pb is not None:
                obj_pb.setToolTip(self.tr('Informações do MDE selecionado'))

        self._refresh_extent_and_pairs_labels()
        self._refresh_proc_button()

        pf = self.dic_prj.get('project_file')
        if pf and os.path.isfile(pf):
            self.lb_status_proj.setText(self.tr('Projeto OK'))
        elif pf:
            self.lb_status_proj.setText(self.tr('Arquivo .pa.gpkg ausente'))
        else:
            self.lb_status_proj.setText(self.tr('Não definido'))

        if refresh_open_language and self.language_dlg and self.language_dlg.isVisible():
            self.language_dlg.apply_language_live()
        # Sempre retraduzir a janela de parâmetros se existir (aberta ou fechada).
        # Antes só atualizava quando estava visível — ao reabrir ficava no idioma antigo.
        if self.settings_dlg is not None and (refresh_open_settings or rebuild_settings):
            if rebuild_settings and not self.settings_dlg.isVisible():
                old_dlg = self.settings_dlg
                self.settings_dlg = None
                if old_dlg is not None:
                    try:
                        old_dlg._save_window_geometry()
                    except Exception:
                        pass
                    old_dlg.deleteLater()
                self.settings_dlg = SettingsDlg(main=self.parent, parent=self)
                self.reload_settings_from_project_file()
            else:
                self.settings_dlg.apply_language_live()
        elif rebuild_settings:
            old_dlg = self.settings_dlg
            self.settings_dlg = None
            if old_dlg is not None:
                try:
                    old_dlg._save_window_geometry()
                except Exception:
                    pass
                old_dlg.deleteLater()
            self.settings_dlg = SettingsDlg(main=self.parent, parent=self)
            self.reload_settings_from_project_file()

    def _refresh_ui_language_button(self):
        if not getattr(self, 'pb_lang', None):
            return
        self.pb_lang.setText(locale_button_label())

    def open_language_dialog(self):
        if not self.language_dlg:
            self.language_dlg = LanguageDlg(parent=self)
        self.language_dlg.show()
        self.language_dlg.raise_()
        self.language_dlg.activateWindow()

    def open_settings(self):
        if not self.settings_dlg:
            self.settings_dlg = SettingsDlg(main=self.parent, parent=self)
            self.reload_settings_from_project_file()
        else:
            # Garante idioma atual (ex.: mudou idioma com a janela fechada).
            self.settings_dlg.apply_language_live()
        self.settings_dlg.show()
        self.settings_dlg.raise_()
        self.settings_dlg.activateWindow()

