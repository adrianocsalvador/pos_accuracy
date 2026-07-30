# -*- coding: utf-8 -*-
"""Sincroniza pos_accuracy_en.ts a partir do código Python (contexto único PositionalAccuracyPlugin).

pylupdate5 atribui contexto Wd1 a self.tr() do painel, mas em runtime Wd1.tr() usa
tr_ui() -> PositionalAccuracyPlugin. Este script evita essa divergência.
"""
from __future__ import annotations

import ast
import os
import re
import xml.etree.ElementTree as ET

PLUGIN_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
I18N_DIR = os.path.dirname(__file__)
TS_PATH = os.path.join(I18N_DIR, 'pos_accuracy_en.ts')
TS_PATH_ES = os.path.join(I18N_DIR, 'pos_accuracy_es_ES.ts')
SOURCES = (
    os.path.join(PLUGIN_ROOT, 'mods', 'mod_positional_accuracy.py'),
    os.path.join(PLUGIN_ROOT, 'mods', 'mod_settings.py'),
    os.path.join(PLUGIN_ROOT, 'mods', 'mod_language_dlg.py'),
    os.path.join(PLUGIN_ROOT, 'mods', 'plugin_i18n.py'),
)
CONTEXT = 'PositionalAccuracyPlugin'
MENU_SOURCE = '&T MDE AP - Acurácia Posicional'

# Traduções novas ou corrigidas (fonte PT -> EN)
EXTRA_EN: dict[str, str] = {
    MENU_SOURCE: '&T MDE AP - Positional Accuracy',
    'MDE AP - Acurácia Posicional': 'MDE AP - Positional Accuracy',
    'Revisar': 'Review',
    'Retomando a partir da correspondência de linhas (parâmetros alterados).': (
        'Resuming from line matching (parameters changed).'),
    'Relatório — MDE AP — Acurácia Posicional': 'Report — MDE AP — Positional Accuracy',
    '1. Localização da área de estudo': '1. Study area location',
    '2. Fluxo de trabalho': '2. Workflow',
    '3. Modelos digitais de elevação (MDE)': '3. Digital elevation models (DEM)',
    '4. Parâmetros de processamento': '4. Processing parameters',
    '5. Estatísticas do painel': '5. Panel statistics',
    '6. Pares homólogos — perfis (WKT)': '6. Homologous pairs — profiles (WKT)',
    '6. Pares homólogos — estatísticas': '6. Homologous pairs — statistics',
    '7. Resultados PEC': '7. PEC results',
    '7.1 PEC Planimétrico': '7.1 Horizontal PEC',
    '7.2 PEC Altimétrico': '7.2 Vertical PEC',
    'Método de normalização de progressivas': 'Chainage normalization method',
    'Escalar k (linear)': 'Scalar k (linear)',
    'Total de pares': 'Total pairs',
    'Total de pares: {0}': 'Total pairs: {0}',
    'Escalar (k)': 'Scalar (k)',
    'Média': 'Mean',
    'Mínima': 'Minimum',
    'Máxima': 'Maximum',
    'Desvio Padrão': 'Standard deviation',
    'Escalar k (L_ref / L_teste) — média: {0}': 'Scalar k (L_ref / L_test) — mean: {0}',
    'Escalar k — mínima: {0}': 'Scalar k — minimum: {0}',
    'Escalar k — máxima: {0}': 'Scalar k — maximum: {0}',
    'Escalar k — desvio padrão: {0}': 'Scalar k — standard deviation: {0}',
    '(sem pares homólogos definidos — execute a correspondência de linhas.)': (
        '(no homologous pairs defined — run line matching.)'),
    'Ficheiro WKT dos perfis': 'Profiles WKT file',
    'Ficheiro WKT dos perfis exportado: {0}': 'Profiles WKT file exported: {0}',
    'Falha ao gerar ficheiro WKT dos perfis: {0} ({1})': (
        'Could not generate profiles WKT file: {0} ({1})'),
    'Título': 'Title',
    'Data/hora': 'Date/time',
    'Ficheiro de projeto': 'Project file',
    'CRS de referência (análise)': 'Reference CRS (analysis)',
    'Par': 'Pair',
    'ref_id': 'ref_id',
    'camada_ref': 'ref_layer',
    'wkt_ref': 'wkt_ref',
    'Perfil ref. (WKT compatibilizado)': 'Ref. profile (compatibilized WKT)',
    'Perfil teste (WKT compatibilizado)': 'Test profile (compatibilized WKT)',
    'test_id': 'test_id',
    'camada_test': 'test_layer',
    'wkt_test': 'wkt_test',
    'Idioma da interface': 'Interface language',
    'Idioma do QGIS ({0})': 'QGIS language ({0})',
    'Tradução: {0}': 'Translation: {0}',
    'Idioma de desenvolvimento ({0})': 'Development language ({0})',
    'Alterar idioma da interface': 'Change interface language',
    '{0} não encontrado': '{0} not found',
    'Fechar': 'Close',
    'Para criar tradução num idioma ainda sem ficheiro .qm:\n'
    '1. Copie i18n/pos_accuracy_en.ts para pos_accuracy_<locale>.ts '
    '(ex.: pos_accuracy_es_ES.ts).\n'
    '2. Traduza no Qt Linguist ou edite o .ts (contexto PositionalAccuracyPlugin).\n'
    '3. Compile: execute i18n/build_translations.bat qm-only '
    '(requer pyside6-lrelease ou lrelease do OSGeo4W).\n'
    '4. Confirme que pos_accuracy_<locale>.qm ficou na pasta i18n/ '
    'e recarregue o plugin.\n'
    'Use pos_accuracy_en.ts como modelo — é a tradução completa de referência.\n'
    'Idioma de desenvolvimento (textos fonte): pt_BR.': (
        'To create a translation for a locale that has no .qm file yet:\n'
        '1. Copy i18n/pos_accuracy_en.ts to pos_accuracy_<locale>.ts '
        '(e.g. pos_accuracy_es_ES.ts).\n'
        '2. Translate in Qt Linguist or edit the .ts (context PositionalAccuracyPlugin).\n'
        '3. Compile: run i18n/build_translations.bat qm-only '
        '(requires pyside6-lrelease or OSGeo4W lrelease).\n'
        '4. Ensure pos_accuracy_<locale>.qm is in the i18n/ folder '
        'and reload the plugin.\n'
        'Use pos_accuracy_en.ts as the reference template — it is the complete English translation.\n'
        'Development language (source strings): pt_BR.'),
    'Opção': 'Option',
    'Valor': 'Value',
    'Papel': 'Role',
    'Nome': 'Name',
    'Fonte (início)': 'Source (start)',
    'Parâmetro': 'Parameter',
    'Definição da área de estudos:': 'Study area definition:',
    'Definição da área de estudos': 'Study area definition',
    'Español': 'Español',
    'Pares homólogos': 'Homologous pairs',
    'Tratamento de outliers:': 'Outlier handling:',
    'Tratamento de outliers': 'Outlier handling',
    'Camada polígono (se aplicável)': 'Polygon layer (if applicable)',
    'Referência': 'Reference',
    'Teste': 'Test',
    '(não definido)': '(not set)',
    '(não selecionado)': '(not selected)',
    '(nenhuma)': '(none)',
    '(camada de interseção indisponível)': '(intersection layer unavailable)',
    '(extensão vazia — execute a interseção dos MDEs)': (
        '(empty extent — run DEM intersection)'),
    '(ainda não há resultados de PEC nesta sessão — execute a análise até ao fim.)': (
        '(no PEC results in this session yet — run the analysis to completion.)'),
    'Envelope ({0}): Xmin={1}, Ymin={2}, Xmax={3}, Ymax={4}': (
        'Envelope ({0}): Xmin={1}, Ymin={2}, Xmax={3}, Ymax={4}'),
    'Envelope: Xmin={0}, Ymin={1}, Xmax={2}, Ymax={3}': (
        'Envelope: Xmin={0}, Ymin={1}, Xmax={2}, Ymax={3}'),
    '(transformação para {0} indisponível)': '(reprojection to {0} unavailable)',
    'Não foi possível criar a pasta do projeto: {0}': (
        'Could not create the project folder: {0}'),
    'Relatório PEC gravado: {0}': 'PEC report saved: {0}',
    'Não foi possível gravar relatório PEC: {0} ({1})': (
        'Could not save PEC report: {0} ({1})'),
    'Relatório TXT gravado: {0}': 'TXT report saved: {0}',
    'Relatório PDF gravado: {0}': 'PDF report saved: {0}',
    'Não foi possível gravar relatório TXT: {0} ({1})': (
        'Could not save TXT report: {0} ({1})'),
    'Não foi possível gravar relatório PDF: {0} ({1})': (
        'Could not save PDF report: {0} ({1})'),
    '=== Verificação GRASS (morfologia do terreno) ===': (
        '=== GRASS check (terrain morphology) ==='),
    'GRASS: provider «{0}» — {1}.': 'GRASS: provider «{0}» — {1}.',
    'GRASS ativo, mas algoritmos indisponíveis: {0}.': (
        'GRASS active, but algorithms unavailable: {0}.'),
    'GRASS OK — morfologia pode executar. Algoritmos: {0}.': (
        'GRASS OK — morphology can run. Algorithms: {0}.'),
    'Mostrar buffers no mapa durante o processamento': (
        'Show buffers on the map during processing'),
    'Não': 'No',
    'Sim': 'Yes',
    'Todos (*.*)': 'All (*.*)',
    'Correspondência e buffers serão refeitos: linhas de correspondência e buffers limpos; pares e extensão da amostra repostos.': (
        'Matching and buffers will be rebuilt: match lines and buffers cleared; pairs and sample extent reset.'),
    'Morfologia e etapas seguintes serão refeitas: camadas de morfologia, linhas de correspondência e buffers foram limpos; pares e extensão da amostra repostos.': (
        'Morphology and following steps will be redone: morphology layers, match lines and buffers were cleared; pairs and sample extent reset.'),
    'Reprocessamento completo: limites, morfologia, correspondência e buffers foram limpos; estatísticas do painel repostas.': (
        'Full reprocessing: limits, morphology, matching and buffers were cleared; panel statistics reset.'),
    'Extensão amostras válidas PEC: {0} km (correspondência total: {1} km).': (
        'Valid PEC sample extent: {0} km (total matching: {1} km).'),
    'Tratamento de outliers (PEC): {0}': 'Outlier handling (PEC): {0}',
    'Escala': 'Scale',
    'EQ (m)': 'EQ (m)',
    'Classe': 'Class',
    'Outliers': 'Outliers',
    'Amostras Válidas': 'Valid samples',
    'Quant.': 'Qty.',
    'Ext. (km)': 'Ext. (km)',
    'PEC (90% d_i ≤ PEC-PCD)': 'PEC (90% d_i ≤ PEC-PCD)',
    'Quantitativo': 'Quantitative',
    'Extensão': 'Extent',
    'Teste': 'Test',
    'Resultado': 'Result',
    'EP (RMS ≤ EP)': 'EP (RMS ≤ EP)',
    'ANÁLISE PLANIMÉTRICA': 'HORIZONTAL ANALYSIS',
    'ANÁLISE ALTIMÉTRICA': 'VERTICAL ANALYSIS',
    'PASSOU': 'PASS',
    'FALHOU': 'FAIL',
    'NORMALIDADE - FALHOU': 'NORMALITY - FAIL',
    'n/d': 'n/a',
    'DEFININDO POLÍGONOS': 'DEFINING POLYGON LIMITS',
    'DEFININDO ELEMENTOS DE MORFOLOGIA DO TERRENO - {0}': (
        'DEFINING TERRAIN MORPHOLOGY FEATURES - {0}'),
    'MDE de teste inválido — não é possível aplicar a distância máxima em pixels.': (
        'Invalid test DEM — cannot apply maximum distance in pixels.'),
    'GSD do MDE de teste inválido — não é possível converter pixels em distância no mapa.': (
        'Invalid test DEM GSD — cannot convert pixels to map distance.'),
    'Sem pares válidos em __Linhas_de_Correspondencia__ no projeto.': (
        'No valid pairs in __Linhas_de_Correspondencia__ in the project.'),
    'Lista de escalas vazia - verifique parâmetros de buffers.': (
        'Empty scale list — check buffer parameters.'),
    'Camada ausente ou inválida no GPKG: {0}': (
        'Layer missing or invalid in GPKG: {0}'),
    'PEC altimétrico ignorado para escala 1:{0}.000 (sem limites definidos).': (
        'Vertical PEC skipped for scale 1:{0},000 (no limits defined).'),
    '(sem detalhe)': '(no detail)',
    'Geometria vazia ou nula — feição ignorada.': (
        'Empty or null geometry — feature ignored.'),
    'Geometria vazia após remover Z/M — feição ignorada.': (
        'Empty geometry after removing Z/M — feature ignored.'),
    'Tipo de retomada não reconhecido ({0}); aplicando limpeza completa.': (
        'Unrecognized resume type ({0}); applying full cleanup.'),
    'Buffers e PEC serão refeitos: camada de buffers limpa.': (
        'Buffers and PEC will be rebuilt: buffer layer cleared.'),
    'GRASS: provider não encontrado no Processing. Instale o GRASS GIS (ex.: OSGeo4W com componente GRASS) ou reinstale o QGIS com suporte GRASS.': (
        'GRASS: provider not found in Processing. Install GRASS GIS (e.g. OSGeo4W with GRASS component) or reinstall QGIS with GRASS support.'),
    'GRASS: provider não pode ser ativado (GRASS não instalado ou dependências em falta no sistema).': (
        'GRASS: provider cannot be activated (GRASS not installed or missing system dependencies).'),
    'GRASS: provider DESATIVADO. Ative em Configurações → Opções → Processamento → aba Providers → marque «GRASS GIS». Sem isso, a morfologia (cumeadas e hidrografia numérica) não executará.': (
        'GRASS: provider DISABLED. Enable in Settings → Options → Processing → Providers tab → check «GRASS GIS». Otherwise morphology (ridges and stream networks) will not run.'),
    '=======================================\n': '=======================================\n',
    '  Tamanho do pixel X: {0:.3f}\n': '  Pixel size X: {0:.3f}\n',
    '  Tamanho do pixel Y: {0:.3f}\n': '  Pixel size Y: {0:.3f}\n',
    'Morfologia cancelada: GRASS indisponível ou desativado. Corrija antes de continuar (ver mensagens acima).': (
        'Morphology cancelled: GRASS unavailable or disabled. Fix before continuing (see messages above).'),
    'RAM do sistema: {0}% em uso, {1} MB livres de {2} MB.': (
        'System RAM: {0}% in use, {1} MB free of {2} MB.'),
    'RAM elevada antes da morfologia — o r.watershed (GRASS) pode falhar. Feche outras aplicações, reinicie o QGIS se necessário, ou reduza «Limite de Memória para Grass GIS» nas definições do plugin.': (
        'High RAM before morphology — r.watershed (GRASS) may fail. Close other applications, restart QGIS if needed, or reduce «Grass GIS memory limit» in plugin settings.'),
    '[__Linhas_de_Correspondencia__] Camadas de morfologia indisponíveis para tipo {0}.': (
        '[__Linhas_de_Correspondencia__] Morphology layers unavailable for type {0}.'),
    '[__Linhas_de_Correspondencia__] Nenhuma linha de ligação foi criada.': (
        '[__Linhas_de_Correspondencia__] No link line was created.'),
    '[__Linhas_de_Correspondencia__] Falha ao gravar no GPKG: {0}': (
        '[__Linhas_de_Correspondencia__] Failed to write to GPKG: {0}'),
    '[__Linhas_de_Correspondencia__] {0} ligações gravadas (edite antes de Continuar se estiver em revisão).': (
        '[__Linhas_de_Correspondencia__] {0} links saved (edit before Continue if under review).'),
    '[__Linhas_de_Correspondencia__] {0} feição(ões) ignoradas.': (
        '[__Linhas_de_Correspondencia__] {0} feature(s) ignored.'),
    'Camada __Linhas_de_Correspondencia__: {0} pares. Edite, remova ou adicione linhas (meio teste → meio referência); atributos: tipo, fid_r, fid_t. Prima Continuar.': (
        '__Linhas_de_Correspondencia__ layer: {0} pairs. Edit, remove or add lines (test midpoint → reference midpoint); attributes: tipo, fid_r, fid_t. Click Continue.'),
    '[__Buffers__] Tipo de geometria da camada: {0}. Gravação em lote por par (sem repaint durante o processamento).': (
        '[__Buffers__] Layer geometry type: {0}. Batch write per pair (no repaint during processing).'),
    '[__Buffers__] Lote: {0} adicionadas, {1} ignoradas (geometria), {2} rejeitadas pelo fornecedor.': (
        '[__Buffers__] Batch: {0} added, {1} ignored (geometry), {2} rejected by provider.'),
    '[__Buffers__] commitChanges falhou:\n{0}': (
        '[__Buffers__] commitChanges failed:\n{0}'),
    'Geometria não poligonal ({0}); makeValid não produziu polígono — ignorada.': (
        'Non-polygon geometry ({0}); makeValid did not produce a polygon — ignored.'),
    'Define buffers: a camada __Linhas_de_Correspondencia__ está vazia ou sem pares válidos.': (
        'Define buffers: __Linhas_de_Correspondencia__ layer is empty or has no valid pairs.'),
    '{0} nulo/indefinido: {1} amostra(s) ignorada(s) na respetiva escala/classe — extensão total ignorada: {2} km ({3} m)': (
        '{0} null/undefined: {1} sample(s) ignored at the respective scale/class — total extent ignored: {2} km ({3} m)'),
    '  1:{0}.000 — classe {1}: {2} ignorada(s), {3} m': (
        '  1:{0}.000 — class {1}: {2} ignored, {3} m'),
    '    ref {0} fid_r={1} | teste {2} fid_t={3} | ext. {4} m': (
        '    ref {0} fid_r={1} | test {2} fid_t={3} | ext. {4} m'),
    '\n  … detalhe truncado ({0} amostras no total).': (
        '\n  … detail truncated ({0} samples in total).'),
    'EQ {0} — 1:{1}.000-{2}= {3}, {4} amostras': (
        'EQ {0} — 1:{1}.000-{2}= {3}, {4} samples'),
    '1:{0}.000-{1}= {2}, {3} amostras': (
        '1:{0}.000-{1}= {2}, {3} samples'),
    'EQ {0} — 1:{1}.000-{2}= quant {3}% <= {4} - {5}, ext {6}% <= {4} - {7},': (
        'EQ {0} — 1:{1}.000-{2}= qty {3}% <= {4} - {5}, ext {6}% <= {4} - {7},'),
    '1:{0}.000-{1}= quant {2}% <= {3} - {4}, ext {5}% <= {3} - {6},': (
        '1:{0}.000-{1}= qty {2}% <= {3} - {4}, ext {5}% <= {3} - {6},'),
    ' {0} <= {1} EP - PASSOU, {2}': ' {0} <= {1} EP - PASS, {2}',
    ' {0} > {1} EP - FALHOU, {2}': ' {0} > {1} EP - FAIL, {2}',
    '1:{0}.000': '1:{0}.000',
    'Defina um projeto (.pa.gpkg) para exportar o relatório.': (
        'Define a project (.pa.gpkg) to export the report.'),
    'Falha ao gerar PDF: {0}': 'Failed to generate PDF: {0}',
    'Falha ao gerar relatório TXT: {0} ({1})': (
        'Failed to generate TXT report: {0} ({1})'),
    'Falha ao gravar HTML do relatório: {0} ({1})': (
        'Failed to save report HTML: {0} ({1})'),
    'Relatório PDF exportado: {0}': 'PDF report exported: {0}',
    'Relatório TXT v1 exportado (parseável → PDF): {0}': (
        'TXT report v1 exported (parseable → PDF): {0}'),
    'Relatório HTML exportado: {0}': 'HTML report exported: {0}',
    'Relatórios na pasta do projeto: PDF + TXT (+ HTML se aplicável).': (
        'Reports in the project folder: PDF + TXT (+ HTML if applicable).'),
    'Falha ao exportar relatórios.': 'Failed to export reports.',
    'Relatório de Auditoria': 'Audit Report',
    'Horizontal': 'Horizontal',
    'Fórmula para cálculo da Discrepância Média': (
        'Formula for Mean Discrepancy calculation'),
    'Equação original (eq:dm-buffer-duplo)': (
        'Original equation (eq:dm-buffer-duplo)'),
    'Nova equação (eq:dm-buffer-duplo-media)': (
        'New equation (eq:dm-buffer-duplo-media)'),
    'dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ\n\n'
    'dmᵢ — discrepância média do par i\n'
    'π — constante pi\n'
    'x — PEC (raio do buffer) da escala/classe\n'
    'A₁ — área do buffer da feição de teste\n'
    'A₂ — área do buffer da feição de referência\n'
    'A₃ — área da interseção dos buffers': (
        'dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ\n\n'
        'dmᵢ — mean discrepancy of pair i\n'
        'π — pi constant\n'
        'x — PEC (buffer radius) for the scale/class\n'
        'A₁ — test feature buffer area\n'
        'A₂ — reference feature buffer area\n'
        'A₃ — buffer intersection area'),
    'dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)\n\n'
    'A média (A₁ + A₂)/2 entra no numerador (no lugar de A₂) e no '
    'denominador (no lugar de A₁), tratando os dois erros de extensão '
    'com o mesmo peso.\n\n'
    'dmᵢ — discrepância média do par i\n'
    'π — constante pi\n'
    'x — PEC (raio do buffer) da escala/classe\n'
    'A₁ — área do buffer da feição de teste\n'
    'A₂ — área do buffer da feição de referência\n'
    'A₃ — área da interseção dos buffers': (
        'dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)\n\n'
        'The average (A₁ + A₂)/2 is used in the numerator (instead of A₂) and '
        'in the denominator (instead of A₁), treating both length errors with '
        'equal weight.\n\n'
        'dmᵢ — mean discrepancy of pair i\n'
        'π — pi constant\n'
        'x — PEC (buffer radius) for the scale/class\n'
        'A₁ — test feature buffer area\n'
        'A₂ — reference feature buffer area\n'
        'A₃ — buffer intersection area'),
    'Vertical': 'Vertical',
    'Vertical': 'Vertical',
    'Defina um projeto (.pa.gpkg) para exportar a auditoria.': (
        'Define a project (.pa.gpkg) to export the audit.'),
    'Auditoria vertical: nenhuma escala definida.': (
        'Vertical audit: no scale defined.'),
    'Auditoria vertical: sem pares homólogos.': (
        'Vertical audit: no homologous pairs.'),
    'Falha ao carregar gerador de auditoria: {0}': (
        'Failed to load audit generator: {0}'),
    'A gerar relatório de auditoria vertical ({0} pares)…': (
        'Generating vertical audit report ({0} pairs)…'),
    'Falha na auditoria vertical: {0}': 'Vertical audit failed: {0}',
    'Auditoria vertical gravada: {0} ({1} páginas)': (
        'Vertical audit saved: {0} ({1} pages)'),
    'Auditoria horizontal: nenhuma escala definida.': (
        'Horizontal audit: no scale defined.'),
    'Auditoria horizontal: sem pares homólogos.': (
        'Horizontal audit: no homologous pairs.'),
    'A gerar relatório de auditoria horizontal ({0} pares)…': (
        'Generating horizontal audit report ({0} pairs)…'),
    'Falha na auditoria horizontal: {0}': 'Horizontal audit failed: {0}',
    'Auditoria horizontal gravada: {0} ({1} páginas)': (
        'Horizontal audit saved: {0} ({1} pages)'),
    'Falha na auditoria: {0}': 'Audit failed: {0}',
    'Pares homólogos: {0} grupos; rever no mapa e prima Continuar.': (
        'Homologous pairs: {0} groups; review on the map and click Continue.'),
    'Edite a camada de interseção se necessário e prima Continuar para morfologia.': (
        'Edit the intersection layer if needed, then click Continue for morphology.'),
    'Área da Interseção dos MDEs: {}': 'DEM intersection area: {}',
    'Abrir projeto existente ou criar novo (.pa.gpkg)': (
        'Open an existing project or create a new one (.pa.gpkg)'),
    'Parâmetros e MDEs inalterados (última avaliação concluída ou configuração gravada no projeto).': (
        'Parameters and elevation models (MDE) are unchanged (last completed evaluation or configuration saved in the project).'),
    'Projeto MDE-AP (*.pa.gpkg)': 'MDE-AP project (*.pa.gpkg)',
    'Falha ao promover polígono simples a MultiPolygon — ignorada.': (
        'Failed to promote simple polygon to MultiPolygon — ignored.'),
    'Polígono sem anéis — não foi possível formar MultiPolygon.': (
        'Polygon without rings — could not form MultiPolygon.'),
    'Geometria inválida após makeValid — ignorada.': (
        'Invalid geometry after makeValid — ignored.'),
}

# Renomeações de fonte (antigo -> novo PT alinhado ao código)
SOURCE_RENAMES: dict[str, str] = {
    '&T AP Acurácia Posicional': MENU_SOURCE,
    'Parâmetros e APs inalterados desde a última avaliação concluída.': (
        'Parâmetros e MDEs inalterados (última avaliação concluída ou configuração gravada no projeto).'),
    'Área da Interseção dos APs: {}': 'Área da Interseção dos MDEs: {}',
    'ÁREA DE INTERSEÇÃO DOS APs DEFINIDA\n': 'ÁREA DE INTERSEÇÃO DOS MDEs DEFINIDA\n',
    'CALCULANDO ÁREA DE INTERSEÇÃO DOS APs': 'CALCULANDO ÁREA DE INTERSEÇÃO DOS MDEs',
    '  INFORMAÇÕES DO AP — {0}\n': '  INFORMAÇÕES DO MDE — {0}\n',
    'AP ({0}) NÃO DEFINIDO': 'MDE ({0}) NÃO DEFINIDO',
    'AP de referência:': 'MDE de referência:',
    'AP de teste:': 'MDE de teste:',
    'Não foi possível carregar o raster: {0}': 'Não foi possível carregar o DEM: {0}',
    'Projeto AP-PA (*.pa.gpkg)': 'Projeto MDE-AP (*.pa.gpkg)',
    'Selecione o AP de referência (raster válido).': 'Selecione o MDE de referência (DEM válido).',
    'Selecione o AP de teste (raster válido).': 'Selecione o MDE de teste (DEM válido).',
    'Calcular pela interseção dos APs': 'Calcular pela interseção dos MDEs',
    'CRS do AP de referência inválido.': 'CRS do MDE de referência inválido.',
    'Revisar automática': 'Revisar',
    'Retomando a partir do emparelhamento de linhas (parâmetros alterados).': (
        'Retomando a partir da correspondência de linhas (parâmetros alterados).'),
}


_PT_MARKERS = re.compile(r'[ãõáéíóúàâêôçÃÕÁÉÍÓÚÀÂÊÔÇ]|(?:ão|ções|não|será|está|grava|defina|falha|morfologia)', re.I)


def _looks_portuguese(text: str) -> bool:
    return bool(_PT_MARKERS.search(text))


def _resolve_en(src: str, existing: dict[str, str]) -> str:
    if src in EXTRA_EN:
        return EXTRA_EN[src]
    tr = existing.get(src, '')
    if tr and tr != src:
        return tr
    if tr == src and _looks_portuguese(src):
        return ''
    return tr


def _resolve_es(src: str, existing: dict[str, str], en_tr: str) -> str:
    if src in EXTRA_ES:
        return EXTRA_ES[src]
    tr = existing.get(src, '')
    if tr and tr != src:
        return tr
    if en_tr and en_tr != src:
        return en_tr
    return src


def _load_existing_ts(path: str = TS_PATH) -> dict[str, str]:
    if not os.path.isfile(path):
        return {}
    tree = ET.parse(path)
    root = tree.getroot()
    out: dict[str, str] = {}
    for ctx in root.findall('context'):
        for msg in ctx.findall('message'):
            src_el = msg.find('source')
            tr_el = msg.find('translation')
            if src_el is None or tr_el is None or not (src_el.text or '').strip():
                continue
            src = src_el.text or ''
            if tr_el.get('type') in ('obsolete', 'vanished'):
                continue
            tr = tr_el.text or ''
            if tr_el.get('type') == 'unfinished' and not tr.strip():
                continue
            out[src] = tr
    if path == TS_PATH:
        migrated: dict[str, str] = {}
        for src, tr in out.items():
            new_src = SOURCE_RENAMES.get(src, src)
            migrated[new_src] = EXTRA_EN.get(new_src, tr)
        for old, new in SOURCE_RENAMES.items():
            if old in out and new not in migrated:
                migrated[new] = EXTRA_EN.get(new, out[old])
        return migrated
    return out


# Traduções PT -> ES (prioridade sobre tradução automática)
EXTRA_ES: dict[str, str] = {
    'Español': 'Español',
    MENU_SOURCE: '&T MDE AP - Precisión Posicional',
    'MDE AP - Acurácia Posicional': 'MDE AP - Precisión Posicional',
    'Idioma da interface': 'Idioma de la interfaz',
    'Idioma do QGIS ({0})': 'Idioma de QGIS ({0})',
    'Alterar idioma da interface': 'Cambiar idioma de la interfaz',
    'Fechar': 'Cerrar',
    'Tradução: {0}': 'Traducción: {0}',
    'Idioma de desenvolvimento ({0})': 'Idioma de desarrollo ({0})',
    '{0} não encontrado': '{0} no encontrado',
    'Parâmetros': 'Parámetros',
    'Relatório de Auditoria': 'Informe de Auditoría',
    'Horizontal': 'Horizontal',
    'Fórmula para cálculo da Discrepância Média': (
        'Fórmula para el cálculo de la Discrepancia Media'),
    'Equação original (eq:dm-buffer-duplo)': (
        'Ecuación original (eq:dm-buffer-duplo)'),
    'Nova equação (eq:dm-buffer-duplo-media)': (
        'Nueva ecuación (eq:dm-buffer-duplo-media)'),
    'dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ\n\n'
    'dmᵢ — discrepância média do par i\n'
    'π — constante pi\n'
    'x — PEC (raio do buffer) da escala/classe\n'
    'A₁ — área do buffer da feição de teste\n'
    'A₂ — área do buffer da feição de referência\n'
    'A₃ — área da interseção dos buffers': (
        'dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ\n\n'
        'dmᵢ — discrepancia media del par i\n'
        'π — constante pi\n'
        'x — PEC (radio del buffer) de la escala/clase\n'
        'A₁ — área del buffer de la entidad de prueba\n'
        'A₂ — área del buffer de la entidad de referencia\n'
        'A₃ — área de la intersección de los buffers'),
    'dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)\n\n'
    'A média (A₁ + A₂)/2 entra no numerador (no lugar de A₂) e no '
    'denominador (no lugar de A₁), tratando os dois erros de extensão '
    'com o mesmo peso.\n\n'
    'dmᵢ — discrepância média do par i\n'
    'π — constante pi\n'
    'x — PEC (raio do buffer) da escala/classe\n'
    'A₁ — área do buffer da feição de teste\n'
    'A₂ — área do buffer da feição de referência\n'
    'A₃ — área da interseção dos buffers': (
        'dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)\n\n'
        'La media (A₁ + A₂)/2 entra en el numerador (en lugar de A₂) y en el '
        'denominador (en lugar de A₁), tratando ambos errores de extensión '
        'con el mismo peso.\n\n'
        'dmᵢ — discrepancia media del par i\n'
        'π — constante pi\n'
        'x — PEC (radio del buffer) de la escala/clase\n'
        'A₁ — área del buffer de la entidad de prueba\n'
        'A₂ — área del buffer de la entidad de referencia\n'
        'A₃ — área de la intersección de los buffers'),
    'Vertical': 'Vertical',
    'Vertical': 'Vertical',
    'Defina um projeto (.pa.gpkg) para exportar a auditoria.': (
        'Defina un proyecto (.pa.gpkg) para exportar la auditoría.'),
    'Auditoria vertical: nenhuma escala definida.': (
        'Auditoría vertical: ninguna escala definida.'),
    'Auditoria vertical: sem pares homólogos.': (
        'Auditoría vertical: sin pares homólogos.'),
    'Falha ao carregar gerador de auditoria: {0}': (
        'Error al cargar el generador de auditoría: {0}'),
    'A gerar relatório de auditoria vertical ({0} pares)…': (
        'Generando informe de auditoría vertical ({0} pares)…'),
    'Falha na auditoria vertical: {0}': 'Error en la auditoría vertical: {0}',
    'Auditoria vertical gravada: {0} ({1} páginas)': (
        'Auditoría vertical guardada: {0} ({1} páginas)'),
    'Auditoria horizontal: nenhuma escala definida.': (
        'Auditoría horizontal: ninguna escala definida.'),
    'Auditoria horizontal: sem pares homólogos.': (
        'Auditoría horizontal: sin pares homólogos.'),
    'A gerar relatório de auditoria horizontal ({0} pares)…': (
        'Generando informe de auditoría horizontal ({0} pares)…'),
    'Falha na auditoria horizontal: {0}': 'Error en la auditoría horizontal: {0}',
    'Auditoria horizontal gravada: {0} ({1} páginas)': (
        'Auditoría horizontal guardada: {0} ({1} páginas)'),
    'Falha na auditoria: {0}': 'Error en la auditoría: {0}',
    'Salvar': 'Guardar',
    'Restaurar': 'Restaurar',
    'Config': 'Configuración',
    'Avaliar': 'Evaluar',
    'Tratamento de outliers:': 'Tratamiento de valores atípicos:',
    'Tratamento de outliers': 'Tratamiento de valores atípicos',
    'English': 'English',
    'Português (Brasil)': 'Portugués (Brasil)',
    'Número de pares homólogos: {}': 'Número de pares homólogos: {}',
    'Revisar': 'Revisar',
    'Continuar': 'Continuar',
    'Escala': 'Escala',
    'Resultado': 'Resultado',
    'Par': 'Par',
    'Título': 'Título',
    'Valor': 'Valor',
    'Pares homólogos': 'Pares homólogos',
    'Papel': 'Papel',
    'Não': 'No',
    'Total de pares': 'Total de pares',
    'Escalar (k)': 'Escalar (k)',
    'Média': 'Media',
    'Mínima': 'Mínima',
    'Máxima': 'Máxima',
    'Desvio Padrão': 'Desviación estándar',
    'Ficheiro WKT dos perfis': 'Archivo WKT de perfiles',
    'Perfil ref. (WKT compatibilizado)': 'Perfil ref. (WKT compatibilizado)',
    'Perfil teste (WKT compatibilizado)': 'Perfil prueba (WKT compatibilizado)',
}


def _auto_translate_pt_to_es(text: str, cache: dict[str, str]) -> str:
    if not text or not text.strip():
        return text
    if text in cache:
        return cache[text]
    try:
        from deep_translator import GoogleTranslator
        translated = GoogleTranslator(source='pt', target='es').translate(text)
        cache[text] = translated or text
    except Exception:
        cache[text] = text
    return cache[text]


def _build_es_merged(sources: list[str], en_merged: dict[str, str]) -> dict[str, str]:
    existing_es = _load_existing_ts(TS_PATH_ES)
    cache: dict[str, str] = {}
    merged: dict[str, str] = {}
    for src in sources:
        if src in EXTRA_ES and EXTRA_ES[src]:
            merged[src] = EXTRA_ES[src]
            continue
        if src in existing_es and existing_es[src] and existing_es[src] != src:
            en_tr = en_merged.get(src, '')
            if en_tr and existing_es[src] == en_tr:
                pass  # fallback inglês — tentar traduzir de novo
            else:
                merged[src] = existing_es[src]
                continue
        es = _auto_translate_pt_to_es(src, cache)
        if es and es != src:
            merged[src] = es
            continue
        en_tr = en_merged.get(src, '')
        if en_tr and en_tr != src:
            merged[src] = en_tr
        else:
            merged[src] = src
    for src, es in EXTRA_ES.items():
        if es:
            merged[src] = es
    return merged


def _extract_from_python(path: str) -> list[str]:
    with open(path, encoding='utf-8') as f:
        src = f.read()
    strings: list[str] = []

    class V(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:
            func = node.func
            name = None
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            if name not in ('tr', 'tr_ui', 'translate'):
                return self.generic_visit(node)
            if not node.args:
                return self.generic_visit(node)
            arg0 = node.args[0]
            if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
                s = arg0.value
                if s and s != '&T {self.name_}':
                    strings.append(s)
            self.generic_visit(node)

    tree = ast.parse(src)
    V().visit(tree)

    # f-strings simples com uma parte estática (ex. tooltips com PLUGIN_DISPLAY_NAME)
    for m in re.finditer(r"(?:self\.tr|tr_ui)\(\s*f'([^'{]+)\{", src):
        pass  # ignorar templates dinâmicos

    return strings


def _collect_sources() -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for path in SOURCES:
        for s in _extract_from_python(path):
            if s not in seen:
                seen.add(s)
                ordered.append(s)
    if MENU_SOURCE not in seen:
        ordered.insert(0, MENU_SOURCE)
    return ordered


def _escape_cdata(text: str) -> str:
    return text.replace(']]>', ']]]]><![CDATA[>')


def _write_ts(
    sources: list[str],
    translations: dict[str, str],
    *,
    path: str,
    language_tag: str,
    resolve=None,
) -> None:
    lines = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<!DOCTYPE TS>',
        f'<TS version="2.0" language="{language_tag}">',
        '<context>',
        f'    <name>{CONTEXT}</name>',
        '    <message>',
        '        <source><![CDATA[]]></source>',
        '        <translation><![CDATA[]]></translation>',
        '    </message>',
    ]
    missing = []
    for src in sources:
        if resolve is not None:
            tr = resolve(src, translations)
            if not tr.strip():
                missing.append(src)
                tr = src
        else:
            tr = translations.get(src, src)
            if (not tr.strip() or tr == src) and _looks_portuguese(src):
                missing.append(src)
        lines.append('    <message>')
        lines.append(f'        <source><![CDATA[{_escape_cdata(src)}]]></source>')
        lines.append(f'        <translation><![CDATA[{_escape_cdata(tr)}]]></translation>')
        lines.append('    </message>')
    lines.extend(['</context>', '</TS>', ''])
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        f.write('\n'.join(lines))
    print(f'Wrote {len(sources)} messages to {path}')
    if missing:
        print(f'Warning: {len(missing)} without translation in {os.path.basename(path)}:')
        for s in missing[:20]:
            line = f'  - {s[:80]}'
            try:
                print(line)
            except UnicodeEncodeError:
                print(line.encode('ascii', 'replace').decode('ascii'))
        if len(missing) > 20:
            print(f'  ... and {len(missing) - 20} more')


def main() -> int:
    existing = _load_existing_ts()
    sources = _collect_sources()
    en_merged = {src: _resolve_en(src, existing) for src in sources}
    for src, en in EXTRA_EN.items():
        if en:
            en_merged[src] = en
    _write_ts(
        sources,
        en_merged,
        path=TS_PATH,
        language_tag='en_US',
        resolve=_resolve_en,
    )

    es_merged = _build_es_merged(sources, en_merged)
    _write_ts(
        sources,
        es_merged,
        path=TS_PATH_ES,
        language_tag='es_ES',
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
