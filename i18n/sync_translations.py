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
    'Método de normalização de progressivas': 'Chainage compatibilization method',
    'Método de compatibilização de progressivas': 'Chainage compatibilization method',
    'Sem Normalização': 'No compatibilization',
    'Sem Compatibilização': 'No compatibilization',
    'Definições para Normalização de Progressivas': 'Chainage compatibilization settings',
    'Definições para Compatibilização de Progressivas': 'Chainage compatibilization settings',
    'Método para Normalização': 'Compatibilization method',
    'Método para Compatibilização': 'Compatibilization method',
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
    'Parâmetros da metodologia: morfologia, pares, buffers, normalização, fórmula da DM e auditoria.': (
        'Methodology parameters: morphology, pairs, buffers, chainage compatibilization, MD formula and audit.'),
    'Parâmetros da metodologia: morfologia, pares, buffers, compatibilização, fórmula da DM e auditoria.': (
        'Methodology parameters: morphology, pairs, buffers, chainage compatibilization, MD formula and audit.'),
    'Informações do MDE selecionado': 'Information about the selected DEM',
    'Abrir o relatório': 'Open the report',
    'Ficheiro GeoPackage do projeto (.pa.gpkg): camadas, parâmetros e resultados.': (
        'Project GeoPackage file (.pa.gpkg): layers, parameters and results.'),
    'Estado do projeto: definido (ficheiro .pa.gpkg encontrado) ou não definido.': (
        'Project status: defined (.pa.gpkg file found) or not defined.'),
    'Versão instalada do complemento.': 'Installed plugin version.',
    'MDE de referência (maior rigor posicional), usado como verdade de campo.': (
        'Reference DEM (higher positional accuracy), used as ground truth.'),
    'MDE a avaliar. A resolução (GSD) deste raster define as distâncias em pixels.': (
        'DEM to evaluate. This raster’s GSD defines distances in pixels.'),
    'Seleccione o raster de referência.': 'Select the reference raster.',
    'Delimita a área da análise: interseção automática dos MDEs, edição após gerar o polígono, ou polígono de uma camada existente.': (
        'Defines the analysis area: automatic DEM intersection, edit after generating the polygon, or a polygon from an existing layer.'),
    'Como obter a área de estudo: (i) interseção dos MDEs; (ii) editar após a interseção; (iii) seleccionar polígono de uma camada.': (
        'How to obtain the study area: (i) DEM intersection; (ii) edit after intersection; (iii) select a polygon from a layer.'),
    'Calcula automaticamente o polígono pela interseção dos dois MDEs.': (
        'Automatically computes the polygon from the intersection of both DEMs.'),
    'Gera a interseção e permite editar o polígono antes de continuar.': (
        'Generates the intersection and lets you edit the polygon before continuing.'),
    'Usa um polígono já existente numa camada do projeto.': (
        'Uses an existing polygon from a project layer.'),
    'Área do polígono de estudo (km²).': 'Study-area polygon area (km²).',
    'Extensão linear mínima recomendada da amostra, proporcional à área de estudo.': (
        'Recommended minimum linear sample extent, proportional to the study area.'),
    'Camada de polígono usada quando a área de estudo vem de uma camada existente.': (
        'Polygon layer used when the study area comes from an existing layer.'),
    'Seleccione a camada polígono que delimita a área de estudo.': (
        'Select the polygon layer that bounds the study area.'),
    'Definição dos pares homólogos: seleção automática ou revisão após a seleção.': (
        'Homologous-pair definition: automatic selection or review after selection.'),
    '(i) Automática — usa só os filtros de Config. (ii) Revisar — permite editar os pares no mapa antes dos buffers.': (
        '(i) Automatic — uses Config filters only. (ii) Review — lets you edit pairs on the map before buffers.'),
    'Selecciona os pares só com os filtros de distância e envelopes.': (
        'Selects pairs using distance and envelope filters only.'),
    'Pausa após a seleção para rever, remover ou acrescentar pares no mapa.': (
        'Pauses after selection so you can review, remove or add pairs on the map.'),
    'Soma dos comprimentos das linhas de referência nos pares aceites.': (
        'Sum of reference-line lengths in accepted pairs.'),
    'Número de pares homólogos válidos.': 'Number of valid homologous pairs.',
    'Outliers pelo método do boxplot (IQR). Pode remover todos, avaliar os indicados, ou usar todas as amostras.': (
        'Outliers by the boxplot (IQR) method. You may remove all, inspect those flagged, or keep all samples.'),
    '(i) Remover automaticamente os outliers; (ii) avaliar os indicados; (iii) usar todos, ignorando a indicação do boxplot.': (
        '(i) Automatically remove outliers; (ii) inspect those flagged; (iii) keep all, ignoring the boxplot flags.'),
    'Exclui automaticamente as amostras fora do critério IQR (boxplot).': (
        'Automatically excludes samples outside the IQR (boxplot) criterion.'),
    'Mostra os outliers para decisão caso a caso antes do PEC.': (
        'Shows outliers for a case-by-case decision before PEC.'),
    'Mantém todas as amostras, sem excluir outliers.': (
        'Keeps all samples, without excluding outliers.'),
    'Mensagens do processamento e avisos da análise.': (
        'Processing messages and analysis warnings.'),
    'Registo detalhado da execução (só leitura).': (
        'Detailed run log (read-only).'),
    'Repositório GitHub do plugin\n(clique para abrir o site)': (
        'Plugin GitHub repository\n(click to open the site)'),
    'Universidade Federal de Viçosa\n(clique para abrir o site)': (
        'Federal University of Viçosa\n(click to open the site)'),
    'Programa de Pós-Graduação em Engenharia Civil\n(clique para abrir o site)': (
        'Postgraduate Program in Civil Engineering\n(click to open the site)'),
    'Caixa: abrir automaticamente após a avaliação.\nClique no texto para abrir o último relatório PDF.': (
        'Check: open automatically after evaluation.\nClick the text to open the last PDF report.'),
    'Nenhum relatório disponível para abrir.': 'No report available to open.',
    'Não foi possível abrir o relatório: {0}': 'Could not open the report: {0}',
    'MDEs atribuídos automaticamente pela resolução espacial: referência={0} (GSD≈{1:.3f}), teste={2} (GSD≈{3:.3f}).': (
        'DEMs assigned automatically by spatial resolution: reference={0} (GSD≈{1:.3f}), test={2} (GSD≈{3:.3f}).'),
    'Normalização alterada: recalculando discrepâncias (DM), sem rematch e sem limpar a camada de buffers.': (
        'Compatibilization changed: recalculating discrepancies (MD) without rematching or clearing the buffer layer.'),
    'Normalização alterada: recalculando discrepâncias (DM), sem reemparelhar e sem limpar a camada de buffers.': (
        'Compatibilization changed: recalculating discrepancies (MD) without rematching or clearing the buffer layer.'),
    'Compatibilização alterada: recalculando discrepâncias (DM), sem rematch e sem limpar a camada de buffers.': (
        'Compatibilization changed: recalculating discrepancies (MD) without rematching or clearing the buffer layer.'),
    'Compatibilização alterada: recalculando discrepâncias (DM), sem reemparelhar e sem limpar a camada de buffers.': (
        'Compatibilization changed: recalculating discrepancies (MD) without rematching or clearing the buffer layer.'),
    'Fórmula de DM alterada: recalculando discrepâncias a partir dos pares existentes, sem rematch e sem limpar a camada de buffers.': (
        'MD formula changed: recalculating discrepancies from existing pairs without rematching or clearing the buffer layer.'),
    'Morfologia e etapas seguintes serão refeitas: camadas de morfologia, linhas de correspondência e buffers foram limpos; área de interseção mantida; pares e extensão da amostra repostos.': (
        'Morphology and later steps will be redone: morphology layers, matching lines and buffers were cleared; intersection area kept; pairs and sample extent reset.'),
    'Parâmetros e MDEs inalterados (última avaliação concluída).': (
        'Parameters and DEMs unchanged (last evaluation completed).'),
    'Parâmetros alterados: {0}. Retomada a partir de: {1}.': (
        'Parameters changed: {0}. Resuming from: {1}.'),
    'limites/interseção': 'limits/intersection',
    'Retomando a partir da correspondência de linhas (morfologia mantida; parâmetros de pares/configuração).': (
        'Resuming from line matching (morphology kept; pair/config parameters).'),
    'Retomando recalculo de DM (normalização / altimetria); pares e camada de buffers mantidos.': (
        'Resuming MD recalculation (compatibilization / altimetry); pairs and buffer layer kept.'),
    'Retomando recalculo de DM (compatibilização / altimetria); pares e camada de buffers mantidos.': (
        'Resuming MD recalculation (compatibilization / altimetry); pairs and buffer layer kept.'),
    'Retomando recalculo de DM (fórmula); pares e camada de buffers mantidos.': (
        'Resuming MD recalculation (formula); pairs and buffer layer kept.'),
    'AVISO: projeto/MDE sob OneDrive ou «Área de Trabalho» (caminho com acentos). Isto causa falhas intermitentes no GRASS no Windows. Copie o projeto e os rasters para um caminho local sem acentos (ex.: C:\\dados\\mdepa\\) e pause a sincronização do OneDrive durante o processamento.': (
        'WARNING: project/DEM under OneDrive or Desktop (path with accents). This causes intermittent GRASS failures on Windows. Copy the project and rasters to a local path without accents (e.g. C:\\data\\mdepa\\) and pause OneDrive sync during processing.'),
    'Não há pares homólogos válidos. O processamento foi interrompido antes dos buffers.\n\n'
    'Sugestões:\n'
    '• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas.\n'
    '• Afrouxar a correspondência: aumentar a distância máxima entre centróides, '
    'os percentuais de diferença de área/comprimento dos envelopes mínimos, '
    'ou reduzir a extensão mínima da feição de teste.': (
        'There are no valid homologous pairs. Processing stopped before buffers.\n\n'
        'Suggestions:\n'
        '• Decrease maximum basin area (morphology) to generate more lines.\n'
        '• Relax matching: increase maximum centroid distance, envelope area/length '
        'difference percentages, or reduce the minimum test-feature extent.'),
    'A extensão total da amostra ({0} km) é menor que a extensão mínima recomendada ({1} km). '
    'O processamento foi interrompido antes dos buffers.\n\n'
    'Sugestões:\n'
    '• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas e maior extensão acumulada.\n'
    '• Afrouxar a correspondência: aumentar a distância máxima entre centróides, '
    'os percentuais de diferença de área/comprimento dos envelopes mínimos, '
    'ou reduzir a extensão mínima da feição de teste.': (
        'Total sample extent ({0} km) is smaller than the recommended minimum ({1} km). '
        'Processing stopped before buffers.\n\n'
        'Suggestions:\n'
        '• Decrease maximum basin area (morphology) to generate more lines and greater accumulated extent.\n'
        '• Relax matching: increase maximum centroid distance, envelope area/length '
        'difference percentages, or reduce the minimum test-feature extent.'),
    'Seleção de pares: dist. máx.={0:g} px ({1:.2f} m); Δárea envelope<{2:g} %; Δcomprimento envelope<{3:g} %; extensão mín. teste={4:g} px ({5:.2f} m).': (
        'Pair selection: max dist.={0:g} px ({1:.2f} m); envelope Δarea<{2:g} %; envelope Δlength<{3:g} %; min. test extent={4:g} px ({5:.2f} m).'),
    'Extensão total da amostra: {0} m': 'Total sample extent: {0} m',
    'RECALCULANDO DM (NORMALIZAÇÃO / ALTIMETRIA)': 'RECALCULATING MD (COMPATIBILIZATION / ALTIMETRY)',
    'RECALCULANDO DM (COMPATIBILIZAÇÃO / ALTIMETRIA)': 'RECALCULATING MD (COMPATIBILIZATION / ALTIMETRY)',
    'RECALCULANDO DM (FÓRMULA)': 'RECALCULATING MD (FORMULA)',
    'RECALCULANDO DM (SEM REGRAVAR BUFFERS)': 'RECALCULATING MD (WITHOUT REWRITING BUFFERS)',
    'CE90/LE90: resolução do MDE de teste indisponível. Selecione o raster de teste e tente novamente.': (
        'CE90/LE90: test DEM resolution unavailable. Select the test raster and try again.'),
    'Modo CE90/LE90 — pixel MDE teste={0:.3f} m; precisão limiar={1} casa(s) decimal(is); máx. H={2:g} pixels do MDE de teste ({3} m); máx. V={4:g} pixels do MDE de teste ({5} m).': (
        'CE90/LE90 mode — test DEM pixel={0:.3f} m; threshold precision={1} decimal place(s); max H={2:g} test-DEM pixels ({3} m); max V={4:g} test-DEM pixels ({5} m).'),
    'CE90/LE90: resolução do MDE de teste indisponível.': (
        'CE90/LE90: test DEM resolution unavailable.'),
    'não gerar': 'do not generate',
    '(transformação indisponível)': '(transform unavailable)',
    'Área de estudo': 'Study area',
    'Extensão mínima da amostra': 'Minimum sample extent',
    'Extensão da amostra': 'Sample extent',
    'Número de pares homólogos': 'Number of homologous pairs',
    'Precisão do limiar CE90/LE90': 'CE90/LE90 threshold precision',
    'Auditoria concluída': 'Audit finished',
    'relatório': 'report',
    'CSV de auditoria horizontal gravado: {0}': 'Horizontal audit CSV saved: {0}',
    'CSV de auditoria vertical gravado: {0}': 'Vertical audit CSV saved: {0}',
    'Marque Horizontal e/ou Vertical em Relatório de Auditoria antes de gerar o CSV.': (
        'Check Horizontal and/or Vertical under Audit Report before generating the CSV.'),
    'Falha ao recalcular DM: {0}': 'Failed to recalculate MD: {0}',
    'CSV de auditoria concluído': 'Audit CSV finished',
    'Falha ao gravar limite ({0}): {1}': 'Failed to save limit ({0}): {1}',
    'Extração das feições lineares (cumeadas e hidrografia) por watershed (GRASS).': (
        'Extraction of linear features (ridges and hydrography) by GRASS watershed.'),
    'Controla a densidade das linhas: diminuir a área gera mais feições; aumentar gera menos. Padrão: 675000 m².': (
        'Controls line density: decreasing the area yields more features; increasing yields fewer. Default: 675000 m².'),
    'Memória máxima do GRASS no r.watershed. Aumente se o processo falhar por RAM. Padrão: 4 GB.': (
        'Maximum GRASS memory for r.watershed. Decrease if it fails. Default: 4 GB.'),
    'Memória máxima do GRASS no r.watershed. Diminua se falhar. Padrão: 4 GB.': (
        'Maximum GRASS memory for r.watershed. Decrease if it fails. Default: 4 GB.'),
    'Filtros para formar pares homólogos entre linhas de referência e de teste.': (
        'Filters to form homologous pairs between reference and test lines.'),
    'Filtro inicial: distância máxima entre centróides, em pixels do MDE de teste. Aumentar tende a aumentar candidatos; diminuir pode enviesar a amostra ou não atingir o mínimo. Padrão: 3 px.': (
        'Initial filter: maximum centroid distance, in test-DEM pixels. Increasing tends to add candidates; decreasing may bias the sample or miss the minimum. Default: 3 px.'),
    'Segundo filtro: geometrias semelhantes têm envelopes com áreas semelhantes, reduzindo a influência de erros posicionais. Aumentar ou diminuir tem o mesmo efeito que na distância entre centróides. Padrão: 10 %.': (
        'Second filter: similar geometries have envelopes with similar areas, reducing the effect of positional errors. Raising or lowering has the same effect as centroid distance. Default: 10 %.'),
    'Diferença % entre os comprimentos dos mínimos envelopes': (
        'Maximum % difference between minimum-envelope lengths'),
    'Filtro pelo lado maior dos envelopes orientados. Pares homólogos devem ter comprimentos de envelope semelhantes. Padrão: 5 %.': (
        'Filter on the longest side of oriented envelopes. Homologous pairs should have similar envelope lengths. Default: 5 %.'),
    'Extensão mínima da feição de teste (Pixels do MDE de teste)': (
        'Minimum test-feature extent (test DEM pixels)'),
    'Extensão mínima da feição de teste (pixels do MDE de teste)': (
        'Minimum test-feature extent (test DEM pixels)'),
    'Descarta linhas de teste mais curtas que este comprimento (pixels × GSD do teste), evitando amostras pouco representativas. Padrão: 10 px.': (
        'Drops test lines shorter than this length (pixels × test GSD), avoiding poorly representative samples. Default: 10 px.'),
    'Raios de buffer e padrão de acurácia (PEC-PCD ou CE90/LE90).': (
        'Buffer radii and accuracy standard (PEC-PCD or CE90/LE90).'),
    'Maior escala (maior detalhe) da avaliação PEC-PCD, p.ex. 1:10.000.': (
        'Largest scale (most detail) of the PEC-PCD evaluation, e.g. 1:10,000.'),
    'Menor escala (menor detalhe) da avaliação PEC-PCD. A análise percorre da máxima à mínima.': (
        'Smallest scale (least detail) of the PEC-PCD evaluation. Analysis runs from maximum to minimum.'),
    'Teto da busca do CE90, em pixels do MDE de teste (× GSD = metros). Padrão: 5 px.': (
        'Upper bound of the CE90 search, in test-DEM pixels (× GSD = metres). Default: 5 px.'),
    'Teto da busca do LE90, em pixels do MDE de teste (× GSD = metros). Padrão: 2 px.': (
        'Upper bound of the LE90 search, in test-DEM pixels (× GSD = metres). Default: 2 px.'),
    'Se Sim, a camada de buffers aparece no mapa enquanto é gerada (pode tornar o processamento mais lento). Padrão: Não.': (
        'If checked, the buffer layer appears on the map while it is generated (may slow processing). Default: unchecked.'),
    'Se marcado, a camada de buffers aparece no mapa enquanto é gerada (pode tornar o processamento mais lento). Padrão: desmarcado.': (
        'If checked, the buffer layer appears on the map while it is generated (may slow processing). Default: unchecked.'),
    'Compatibilização das progressivas dos perfis altimétricos (referência vs teste).': (
        'Chainage compatibilization of altimetric profiles (reference vs test).'),
    'Linear: reescala o comprimento do perfil de teste. Por proximidade: associa pontos pela menor distância. Sem normalização: usa as progressivas originais.': (
        'Linear: rescales the test-profile length. By proximity: pairs points by shortest distance. No compatibilization: uses original chainages.'),
    'Linear: reescala o comprimento do perfil de teste. Por proximidade: associa pontos pela menor distância. Sem compatibilização: usa as progressivas originais.': (
        'Linear: rescales the test-profile length. By proximity: pairs points by shortest distance. No compatibilization: uses original chainages.'),
    'Equação da discrepância média (DM) a partir das áreas dos buffers duplos.': (
        'Mean-discrepancy (MD) equation from double-buffer areas.'),
    'PDFs/CSV par a par para conferir buffers e DM. Pode gerar só o planimétrico, só o altimétrico, ou ambos.': (
        'Pair-by-pair PDFs/CSV to check buffers and MD. You may generate planimetric, altimetric, or both.'),
    'Gera PDF de auditoria (e sempre o CSV correspondente) para conferir buffers e DM. Pode activar só o planimétrico, só o altimétrico, ou ambos.': (
        'Generates an audit PDF (and always the matching CSV) to check buffers and MD. '
        'You may enable planimetric only, altimetric only, or both.'),
    'Horizontal (PDF)': 'Horizontal (PDF)',
    'Vertical (PDF)': 'Vertical (PDF)',
    'Gera o relatório de auditoria planimétrica (buffers no plano XY).': (
        'Generates the planimetric audit report (buffers in the XY plane).'),
    'Gera o relatório de auditoria altimétrica (perfis cota × progressiva).': (
        'Generates the altimetric audit report (elevation × chainage profiles).'),
    'Gera o PDF de auditoria planimétrica (buffers no plano XY). O CSV correspondente é sempre gravado.': (
        'Generates the planimetric audit PDF (buffers in the XY plane). The matching CSV is always written.'),
    'Gera o PDF de auditoria altimétrica (perfis cota × progressiva). O CSV correspondente é sempre gravado.': (
        'Generates the altimetric audit PDF (elevation × chainage profiles). The matching CSV is always written.'),
    'Se marcado, grava só o CSV (uma linha por feição, colunas por raio/escala), sem gerar o PDF de auditoria.': (
        'If checked, writes only the CSV (one row per feature, columns per radius/scale), without the audit PDF.'),
    'Segundo filtro: geometrias semelhantes têm envelopes com áreas semelhantes, reduzindo a influência de erros posicionais. Padrão: 10 %.': (
        'Second filter: similar geometries have envelopes with similar areas, reducing the effect of positional errors. Default: 10 %.'),
    'Descarta linhas de teste mais curtas que este comprimento (pixels × GSD do teste). Padrão: 10 px.': (
        'Drops test lines shorter than this length (pixels × test GSD). Default: 10 px.'),
    'Menor escala (menor detalhe) da avaliação PEC-PCD.': (
        'Smallest scale (least detail) of the PEC-PCD evaluation.'),
    'Teto da busca do CE90, em pixels do MDE de teste. Padrão: 5 px.': (
        'Upper bound of the CE90 search, in test-DEM pixels. Default: 5 px.'),
    'Teto da busca do LE90, em pixels do MDE de teste. Padrão: 2 px.': (
        'Upper bound of the LE90 search, in test-DEM pixels. Default: 2 px.'),
    'Se Sim, a camada de buffers aparece no mapa enquanto é gerada. Padrão: Não.': (
        'If checked, the buffer layer appears on the map while it is generated. Default: unchecked.'),
    'Se marcado, a camada de buffers aparece no mapa enquanto é gerada. Padrão: desmarcado.': (
        'If checked, the buffer layer appears on the map while it is generated. Default: unchecked.'),
    'PDFs/CSV par a par para conferir buffers e DM (planimétrico e/ou altimétrico).': (
        'Pair-by-pair PDFs/CSV to check buffers and MD (planimetric and/or altimetric).'),
    'Se marcado, grava só o CSV, sem gerar o PDF de auditoria.': (
        'If checked, writes only the CSV, without the audit PDF.'),
    'Apenas CSV (sem PDF)': 'CSV only (no PDF)',
    'Repõe todos os parâmetros desta janela nos valores padrão.': (
        'Restores all parameters in this window to their default values.'),
    'Grava os parâmetros no projeto (.pa.gpkg) e fecha a janela.': (
        'Saves parameters to the project (.pa.gpkg) and closes the window.'),
    'Recalcula DM a partir do projeto e grava só os CSV (Horizontal/Vertical conforme as opções acima). Sem PDF.': (
        'Recalculates MD from the project and writes only the CSVs (Horizontal/Vertical as checked above). No PDF.'),
    'Falha ao gerar CSV de auditoria: {0}': 'Failed to generate audit CSV: {0}',
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
    'Método de normalização de progressivas': 'Método de compatibilização de progressivas',
    'Sem Normalização': 'Sem Compatibilização',
    'Definições para Normalização de Progressivas': (
        'Definições para Compatibilização de Progressivas'),
    'Método para Normalização': 'Método para Compatibilização',
    'Parâmetros da metodologia: morfologia, pares, buffers, normalização, fórmula da DM e auditoria.': (
        'Parâmetros da metodologia: morfologia, pares, buffers, compatibilização, fórmula da DM e auditoria.'),
    'Normalização alterada: recalculando discrepâncias (DM), sem rematch e sem limpar a camada de buffers.': (
        'Compatibilização alterada: recalculando discrepâncias (DM), sem rematch e sem limpar a camada de buffers.'),
    'Normalização alterada: recalculando discrepâncias (DM), sem reemparelhar e sem limpar a camada de buffers.': (
        'Compatibilização alterada: recalculando discrepâncias (DM), sem rematch e sem limpar a camada de buffers.'),
    'Retomando recalculo de DM (normalização / altimetria); pares e camada de buffers mantidos.': (
        'Retomando recalculo de DM (compatibilização / altimetria); pares e camada de buffers mantidos.'),
    'RECALCULANDO DM (NORMALIZAÇÃO / ALTIMETRIA)': (
        'RECALCULANDO DM (COMPATIBILIZAÇÃO / ALTIMETRIA)'),
    'Linear: reescala o comprimento do perfil de teste. Por proximidade: associa pontos pela menor distância. Sem normalização: usa as progressivas originais.': (
        'Linear: reescala o comprimento do perfil de teste. Por proximidade: associa pontos pela menor distância. Sem compatibilização: usa as progressivas originais.'),
    'Memória máxima do GRASS no r.watershed. Aumente se o processo falhar por RAM. Padrão: 4 GB.': (
        'Memória máxima do GRASS no r.watershed. Diminua se falhar. Padrão: 4 GB.'),
    'Extensão mínima da feição de teste (Pixels do MDE de teste)': (
        'Extensão mínima da feição de teste (pixels do MDE de teste)'),
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
    'Parâmetros da metodologia: morfologia, pares, buffers, normalização, fórmula da DM e auditoria.': (
        'Parámetros de la metodología: morfología, pares, buffers, compatibilización de progresivas, fórmula de la DM y auditoría.'),
    'Parâmetros da metodologia: morfologia, pares, buffers, compatibilização, fórmula da DM e auditoria.': (
        'Parámetros de la metodología: morfología, pares, buffers, compatibilización de progresivas, fórmula de la DM y auditoría.'),
    'Método de normalização de progressivas': 'Método de compatibilización de progresivas',
    'Método de compatibilização de progressivas': 'Método de compatibilización de progresivas',
    'Sem Normalização': 'Sin compatibilización',
    'Sem Compatibilização': 'Sin compatibilización',
    'Definições para Normalização de Progressivas': (
        'Definiciones para la compatibilización de progresivas'),
    'Definições para Compatibilização de Progressivas': (
        'Definiciones para la compatibilización de progresivas'),
    'Método para Normalização': 'Método de compatibilización',
    'Método para Compatibilização': 'Método de compatibilización',
    'Compatibilização das progressivas dos perfis altimétricos (referência vs teste).': (
        'Compatibilización de las progresivas de los perfiles altimétricos (referencia vs prueba).'),
    'Linear: reescala o comprimento do perfil de teste. Por proximidade: associa pontos pela menor distância. Sem normalização: usa as progressivas originais.': (
        'Lineal: reescala la longitud del perfil de prueba. Por proximidad: asocia puntos por la distancia más corta. Sin compatibilización: usa las progresivas originales.'),
    'Linear: reescala o comprimento do perfil de teste. Por proximidade: associa pontos pela menor distância. Sem compatibilização: usa as progressivas originais.': (
        'Lineal: reescala la longitud del perfil de prueba. Por proximidad: asocia puntos por la distancia más corta. Sin compatibilización: usa las progresivas originales.'),
    'Normalização alterada: recalculando discrepâncias (DM), sem rematch e sem limpar a camada de buffers.': (
        'Compatibilización modificada: recalculando discrepancias (DM), sin reemparejar y sin vaciar la capa de buffers.'),
    'Normalização alterada: recalculando discrepâncias (DM), sem reemparelhar e sem limpar a camada de buffers.': (
        'Compatibilización modificada: recalculando discrepancias (DM), sin reemparejar y sin vaciar la capa de buffers.'),
    'Compatibilização alterada: recalculando discrepâncias (DM), sem rematch e sem limpar a camada de buffers.': (
        'Compatibilización modificada: recalculando discrepancias (DM), sin reemparejar y sin vaciar la capa de buffers.'),
    'Compatibilização alterada: recalculando discrepâncias (DM), sem reemparelhar e sem limpar a camada de buffers.': (
        'Compatibilización modificada: recalculando discrepancias (DM), sin reemparejar y sin vaciar la capa de buffers.'),
    'Retomando recalculo de DM (normalização / altimetria); pares e camada de buffers mantidos.': (
        'Reanudando recálculo de DM (compatibilización / altimetría); pares y capa de buffers conservados.'),
    'Retomando recalculo de DM (compatibilização / altimetria); pares e camada de buffers mantidos.': (
        'Reanudando recálculo de DM (compatibilización / altimetría); pares y capa de buffers conservados.'),
    'RECALCULANDO DM (NORMALIZAÇÃO / ALTIMETRIA)': (
        'RECALCULANDO DM (COMPATIBILIZACIÓN / ALTIMETRÍA)'),
    'RECALCULANDO DM (COMPATIBILIZAÇÃO / ALTIMETRIA)': (
        'RECALCULANDO DM (COMPATIBILIZACIÓN / ALTIMETRÍA)'),
    'Informações do MDE selecionado': 'Información del MDE seleccionado',
    'Abrir o relatório': 'Abrir el informe',
    'Ficheiro GeoPackage do projeto (.pa.gpkg): camadas, parâmetros e resultados.': (
        'Archivo GeoPackage del proyecto (.pa.gpkg): capas, parámetros y resultados.'),
    'Estado do projeto: definido (ficheiro .pa.gpkg encontrado) ou não definido.': (
        'Estado del proyecto: definido (archivo .pa.gpkg encontrado) o no definido.'),
    'Versão instalada do complemento.': 'Versión instalada del complemento.',
    'MDE de referência (maior rigor posicional), usado como verdade de campo.': (
        'MDE de referencia (mayor rigor posicional), usado como verdad de campo.'),
    'MDE a avaliar. A resolução (GSD) deste raster define as distâncias em pixels.': (
        'MDE a evaluar. La resolución (GSD) de este ráster define las distancias en píxeles.'),
    'Seleccione o raster de referência.': 'Seleccione el ráster de referencia.',
    'Delimita a área da análise: interseção automática dos MDEs, edição após gerar o polígono, ou polígono de uma camada existente.': (
        'Delimita el área de análisis: intersección automática de los MDE, edición tras generar el polígono, o polígono de una capa existente.'),
    'Como obter a área de estudo: (i) interseção dos MDEs; (ii) editar após a interseção; (iii) seleccionar polígono de uma camada.': (
        'Cómo obtener el área de estudio: (i) intersección de los MDE; (ii) editar tras la intersección; (iii) seleccionar polígono de una capa.'),
    'Calcula automaticamente o polígono pela interseção dos dois MDEs.': (
        'Calcula automáticamente el polígono por la intersección de ambos MDE.'),
    'Gera a interseção e permite editar o polígono antes de continuar.': (
        'Genera la intersección y permite editar el polígono antes de continuar.'),
    'Usa um polígono já existente numa camada do projeto.': (
        'Usa un polígono ya existente en una capa del proyecto.'),
    'Área do polígono de estudo (km²).': 'Área del polígono de estudio (km²).',
    'Extensão linear mínima recomendada da amostra, proporcional à área de estudo.': (
        'Extensión lineal mínima recomendada de la muestra, proporcional al área de estudio.'),
    'Camada de polígono usada quando a área de estudo vem de uma camada existente.': (
        'Capa de polígono usada cuando el área de estudio proviene de una capa existente.'),
    'Seleccione a camada polígono que delimita a área de estudo.': (
        'Seleccione la capa polígono que delimita el área de estudio.'),
    'Definição dos pares homólogos: seleção automática ou revisão após a seleção.': (
        'Definición de los pares homólogos: selección automática o revisión tras la selección.'),
    '(i) Automática — usa só os filtros de Config. (ii) Revisar — permite editar os pares no mapa antes dos buffers.': (
        '(i) Automática — usa solo los filtros de Config. (ii) Revisar — permite editar los pares en el mapa antes de los buffers.'),
    'Selecciona os pares só com os filtros de distância e envelopes.': (
        'Selecciona los pares solo con los filtros de distancia y envolventes.'),
    'Pausa após a seleção para rever, remover ou acrescentar pares no mapa.': (
        'Pausa tras la selección para revisar, quitar o añadir pares en el mapa.'),
    'Soma dos comprimentos das linhas de referência nos pares aceites.': (
        'Suma de las longitudes de las líneas de referencia en los pares aceptados.'),
    'Número de pares homólogos válidos.': 'Número de pares homólogos válidos.',
    'Outliers pelo método do boxplot (IQR). Pode remover todos, avaliar os indicados, ou usar todas as amostras.': (
        'Valores atípicos por el método boxplot (RIC). Puede quitar todos, evaluar los indicados o usar todas las muestras.'),
    '(i) Remover automaticamente os outliers; (ii) avaliar os indicados; (iii) usar todos, ignorando a indicação do boxplot.': (
        '(i) Quitar automáticamente los valores atípicos; (ii) evaluar los indicados; (iii) usar todos, ignorando la indicación del boxplot.'),
    'Exclui automaticamente as amostras fora do critério IQR (boxplot).': (
        'Excluye automáticamente las muestras fuera del criterio RIC (boxplot).'),
    'Mostra os outliers para decisão caso a caso antes do PEC.': (
        'Muestra los valores atípicos para decidir caso por caso antes del PEC.'),
    'Mantém todas as amostras, sem excluir outliers.': (
        'Conserva todas las muestras, sin excluir valores atípicos.'),
    'Mensagens do processamento e avisos da análise.': (
        'Mensajes del procesamiento y avisos del análisis.'),
    'Registo detalhado da execução (só leitura).': (
        'Registro detallado de la ejecución (solo lectura).'),
    'Repositório GitHub do plugin\n(clique para abrir o site)': (
        'Repositorio GitHub del complemento\n(clic para abrir el sitio)'),
    'Universidade Federal de Viçosa\n(clique para abrir o site)': (
        'Universidad Federal de Viçosa\n(clic para abrir el sitio)'),
    'Programa de Pós-Graduação em Engenharia Civil\n(clique para abrir o site)': (
        'Programa de Posgrado en Ingeniería Civil\n(clic para abrir el sitio)'),
    'Caixa: abrir automaticamente após a avaliação.\nClique no texto para abrir o último relatório PDF.': (
        'Casilla: abrir automáticamente tras la evaluación.\nClic en el texto para abrir el último informe PDF.'),
    'Nenhum relatório disponível para abrir.': 'Ningún informe disponible para abrir.',
    'Não foi possível abrir o relatório: {0}': 'No se pudo abrir el informe: {0}',
    'Diferença % entre os comprimentos dos mínimos envelopes': (
        'Diferencia % entre las longitudes de las envolventes mínimas'),
    'Extensão mínima da feição de teste (Pixels do MDE de teste)': (
        'Extensión mínima de la entidad de prueba (píxeles del MDE de prueba)'),
    'Extensão mínima da feição de teste (pixels do MDE de teste)': (
        'Extensión mínima de la entidad de prueba (píxeles del MDE de prueba)'),
    'Memória máxima do GRASS no r.watershed. Aumente se o processo falhar por RAM. Padrão: 4 GB.': (
        'Memoria máxima de GRASS en r.watershed. Disminuya si falla. Predeterminado: 4 GB.'),
    'Memória máxima do GRASS no r.watershed. Diminua se falhar. Padrão: 4 GB.': (
        'Memoria máxima de GRASS en r.watershed. Disminuya si falla. Predeterminado: 4 GB.'),
    'Se marcado, a camada de buffers aparece no mapa enquanto é gerada (pode tornar o processamento mais lento). Padrão: desmarcado.': (
        'Si está marcado, la capa de buffers aparece en el mapa mientras se genera '
        '(puede ralentizar el procesamiento). Predeterminado: desmarcado.'),
    'Se marcado, a camada de buffers aparece no mapa enquanto é gerada. Padrão: desmarcado.': (
        'Si está marcado, la capa de buffers aparece en el mapa mientras se genera. Predeterminado: desmarcado.'),
    'Horizontal': 'Horizontal',
    'Horizontal (PDF)': 'Horizontal (PDF)',
    'Vertical': 'Vertical',
    'Vertical (PDF)': 'Vertical (PDF)',
    'Gera PDF de auditoria (e sempre o CSV correspondente) para conferir buffers e DM. Pode activar só o planimétrico, só o altimétrico, ou ambos.': (
        'Genera PDF de auditoría (y siempre el CSV correspondiente) para comprobar buffers y DM. '
        'Puede activar solo el planimétrico, solo el altimétrico, o ambos.'),
    'Gera o PDF de auditoria planimétrica (buffers no plano XY). O CSV correspondente é sempre gravado.': (
        'Genera el PDF de auditoría planimétrica (buffers en el plano XY). El CSV correspondiente siempre se guarda.'),
    'Gera o PDF de auditoria altimétrica (perfis cota × progressiva). O CSV correspondente é sempre gravado.': (
        'Genera el PDF de auditoría altimétrica (perfiles cota × progresiva). El CSV correspondiente siempre se guarda.'),
    'limites/interseção': 'límites/intersección',
    'Área de estudo': 'Área de estudio',
    'Extensão mínima da amostra': 'Extensión mínima de la muestra',
    'Extensão da amostra': 'Extensión de la muestra',
    'Número de pares homólogos': 'Número de pares homólogos',
    'Precisão do limiar CE90/LE90': 'Precisión del umbral CE90/LE90',
    'Auditoria concluída': 'Auditoría concluida',
    'relatório': 'informe',
    'não gerar': 'no generar',
    '(transformação indisponível)': '(transformación no disponible)',
    'CSV de auditoria concluído': 'CSV de auditoría concluido',
    'Repõe todos os parâmetros desta janela nos valores padrão.': (
        'Restablece todos los parámetros de esta ventana a los valores predeterminados.'),
    'Grava os parâmetros no projeto (.pa.gpkg) e fecha a janela.': (
        'Guarda los parámetros en el proyecto (.pa.gpkg) y cierra la ventana.'),
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
