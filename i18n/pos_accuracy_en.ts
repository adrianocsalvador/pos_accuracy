<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE TS>
<TS version="2.0" language="en_US">
<context>
    <name>PositionalAccuracyPlugin</name>
    <message>
        <source><![CDATA[]]></source>
        <translation><![CDATA[]]></translation>
    </message>
    <message>
        <source><![CDATA[Projeto MDE-AP (*.pa.gpkg)]]></source>
        <translation><![CDATA[MDE-AP project (*.pa.gpkg)]]></translation>
    </message>
    <message>
        <source><![CDATA[Todos (*.*)]]></source>
        <translation><![CDATA[All (*.*)]]></translation>
    </message>
    <message>
        <source><![CDATA[&T MDE AP - Acurácia Posicional]]></source>
        <translation><![CDATA[&T MDE AP - Positional Accuracy]]></translation>
    </message>
    <message>
        <source><![CDATA[Área de estudo: {} km²]]></source>
        <translation><![CDATA[Study area: {} km²]]></translation>
    </message>
    <message>
        <source><![CDATA[Extensão mínima da amostra: {} km]]></source>
        <translation><![CDATA[Minimum sample extent: {} km]]></translation>
    </message>
    <message>
        <source><![CDATA[Extensão da Amostra: {} km]]></source>
        <translation><![CDATA[Sample extent: {} km]]></translation>
    </message>
    <message>
        <source><![CDATA[Número de pares homólogos: {}]]></source>
        <translation><![CDATA[Number of homologous pairs: {}]]></translation>
    </message>
    <message>
        <source><![CDATA[Linear]]></source>
        <translation><![CDATA[Linear]]></translation>
    </message>
    <message>
        <source><![CDATA[Por Proximidade]]></source>
        <translation><![CDATA[By proximity]]></translation>
    </message>
    <message>
        <source><![CDATA[Sem Normalização]]></source>
        <translation><![CDATA[No normalization]]></translation>
    </message>
    <message>
        <source><![CDATA[Equação original (eq:dm-buffer-duplo)]]></source>
        <translation><![CDATA[Original equation (eq:dm-buffer-duplo)]]></translation>
    </message>
    <message>
        <source><![CDATA[Nova equação (eq:dm-buffer-duplo-media)]]></source>
        <translation><![CDATA[New equation (eq:dm-buffer-duplo-media)]]></translation>
    </message>
    <message>
        <source><![CDATA[dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ

dmᵢ — discrepância média do par i
π — constante pi
x — PEC (raio do buffer) da escala/classe
A₁ — área do buffer da feição de teste
A₂ — área do buffer da feição de referência
A₃ — área da interseção dos buffers]]></source>
        <translation><![CDATA[dmᵢ = π · x · (A₂ᵢ − A₃ᵢ) / A₁ᵢ

dmᵢ — mean discrepancy of pair i
π — pi constant
x — PEC (buffer radius) for the scale/class
A₁ — test feature buffer area
A₂ — reference feature buffer area
A₃ — buffer intersection area]]></translation>
    </message>
    <message>
        <source><![CDATA[dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)

A média (A₁ + A₂)/2 entra no numerador (no lugar de A₂) e no denominador (no lugar de A₁), tratando os dois erros de extensão com o mesmo peso.

dmᵢ — discrepância média do par i
π — constante pi
x — PEC (raio do buffer) da escala/classe
A₁ — área do buffer da feição de teste
A₂ — área do buffer da feição de referência
A₃ — área da interseção dos buffers]]></source>
        <translation><![CDATA[dmᵢ = π · x · ((A₁ᵢ + A₂ᵢ)/2 − A₃ᵢ) / ((A₁ᵢ + A₂ᵢ)/2)

The average (A₁ + A₂)/2 is used in the numerator (instead of A₂) and in the denominator (instead of A₁), treating both length errors with equal weight.

dmᵢ — mean discrepancy of pair i
π — pi constant
x — PEC (buffer radius) for the scale/class
A₁ — test feature buffer area
A₂ — reference feature buffer area
A₃ — buffer intersection area]]></translation>
    </message>
    <message>
        <source><![CDATA[Config]]></source>
        <translation><![CDATA[Settings]]></translation>
    </message>
    <message>
        <source><![CDATA[Alterar idioma da interface]]></source>
        <translation><![CDATA[Change interface language]]></translation>
    </message>
    <message>
        <source><![CDATA[Projeto (.pa.gpkg):]]></source>
        <translation><![CDATA[Project (.pa.gpkg):]]></translation>
    </message>
    <message>
        <source><![CDATA[Não definido]]></source>
        <translation><![CDATA[Not set]]></translation>
    </message>
    <message>
        <source><![CDATA[Novo projeto…]]></source>
        <translation><![CDATA[New project…]]></translation>
    </message>
    <message>
        <source><![CDATA[Abrir projeto…]]></source>
        <translation><![CDATA[Open project…]]></translation>
    </message>
    <message>
        <source><![CDATA[MDE de referência:]]></source>
        <translation><![CDATA[Reference DEM:]]></translation>
    </message>
    <message>
        <source><![CDATA[MDE de teste:]]></source>
        <translation><![CDATA[Test DEM:]]></translation>
    </message>
    <message>
        <source><![CDATA[Informações do MDE selecionado]]></source>
        <translation><![CDATA[Informações do MDE selecionado]]></translation>
    </message>
    <message>
        <source><![CDATA[Definição da área de estudos:]]></source>
        <translation><![CDATA[Study area definition:]]></translation>
    </message>
    <message>
        <source><![CDATA[Calcular pela interseção dos MDEs]]></source>
        <translation><![CDATA[Compute from DEMs intersection]]></translation>
    </message>
    <message>
        <source><![CDATA[Editar após interseção]]></source>
        <translation><![CDATA[Edit after intersection]]></translation>
    </message>
    <message>
        <source><![CDATA[Selecionar de uma camada]]></source>
        <translation><![CDATA[Select from a layer]]></translation>
    </message>
    <message>
        <source><![CDATA[Camada polígono (área de estudo):]]></source>
        <translation><![CDATA[Polygon layer (study area):]]></translation>
    </message>
    <message>
        <source><![CDATA[Seleção de pares homólogos:]]></source>
        <translation><![CDATA[Homologous pair selection:]]></translation>
    </message>
    <message>
        <source><![CDATA[Automática]]></source>
        <translation><![CDATA[Automatic]]></translation>
    </message>
    <message>
        <source><![CDATA[Revisar]]></source>
        <translation><![CDATA[Review]]></translation>
    </message>
    <message>
        <source><![CDATA[Tratamento de outliers:]]></source>
        <translation><![CDATA[Outlier handling:]]></translation>
    </message>
    <message>
        <source><![CDATA[Remover automaticamente]]></source>
        <translation><![CDATA[Remove automatically]]></translation>
    </message>
    <message>
        <source><![CDATA[Avaliar individualmente]]></source>
        <translation><![CDATA[Review individually]]></translation>
    </message>
    <message>
        <source><![CDATA[Usar todos]]></source>
        <translation><![CDATA[Use all values]]></translation>
    </message>
    <message>
        <source><![CDATA[Abrir o relatório]]></source>
        <translation><![CDATA[Abrir o relatório]]></translation>
    </message>
    <message>
        <source><![CDATA[Avaliar]]></source>
        <translation><![CDATA[Evaluate]]></translation>
    </message>
    <message>
        <source><![CDATA[LOG:]]></source>
        <translation><![CDATA[LOG:]]></translation>
    </message>
    <message>
        <source><![CDATA[Caixa: abrir automaticamente após a avaliação.
Clique no texto para abrir o último relatório PDF.]]></source>
        <translation><![CDATA[Caixa: abrir automaticamente após a avaliação.
Clique no texto para abrir o último relatório PDF.]]></translation>
    </message>
    <message>
        <source><![CDATA[Nenhum relatório disponível para abrir.]]></source>
        <translation><![CDATA[Nenhum relatório disponível para abrir.]]></translation>
    </message>
    <message>
        <source><![CDATA[Não foi possível abrir o relatório: {0}]]></source>
        <translation><![CDATA[Não foi possível abrir o relatório: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Abrir projeto]]></source>
        <translation><![CDATA[Open project]]></translation>
    </message>
    <message>
        <source><![CDATA[Arquivo não encontrado: {0}]]></source>
        <translation><![CDATA[File not found: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Projeto aberto: {0}]]></source>
        <translation><![CDATA[Project opened: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Novo projeto]]></source>
        <translation><![CDATA[New project]]></translation>
    </message>
    <message>
        <source><![CDATA[Já existe um arquivo com esse nome. Escolha outro nome ou use Abrir projeto.]]></source>
        <translation><![CDATA[A file with this name already exists. Choose another name or use Open project.]]></translation>
    </message>
    <message>
        <source><![CDATA[Diretório inválido para salvar o projeto.]]></source>
        <translation><![CDATA[Invalid directory to save the project.]]></translation>
    </message>
    <message>
        <source><![CDATA[Não foi possível criar o projeto: {0}]]></source>
        <translation><![CDATA[Could not create the project: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Novo projeto criado: {0} (CRS inicial: {1})]]></source>
        <translation><![CDATA[New project created: {0} (initial CRS: {1})]]></translation>
    </message>
    <message>
        <source><![CDATA[Não foi possível carregar o DEM: {0}]]></source>
        <translation><![CDATA[Could not load raster: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Continuar]]></source>
        <translation><![CDATA[Continue]]></translation>
    </message>
    <message>
        <source><![CDATA[Continuar para morfologia após editar a área de interseção.]]></source>
        <translation><![CDATA[Continue to morphology after editing the intersection area.]]></translation>
    </message>
    <message>
        <source><![CDATA[Continuar para gerar buffers após rever os pares.]]></source>
        <translation><![CDATA[Continue to build buffers after reviewing pairs.]]></translation>
    </message>
    <message>
        <source><![CDATA[Executar ou retomar a análise.]]></source>
        <translation><![CDATA[Run or resume the analysis.]]></translation>
    </message>
    <message>
        <source><![CDATA[ÁREA DE ESTUDO A PARTIR DA CAMADA]]></source>
        <translation><![CDATA[STUDY AREA FROM LAYER]]></translation>
    </message>
    <message>
        <source><![CDATA[Selecione uma camada de polígonos válida.]]></source>
        <translation><![CDATA[Select a valid polygon layer.]]></translation>
    </message>
    <message>
        <source><![CDATA[A camada de área de estudo tem de ser poligonal.]]></source>
        <translation><![CDATA[The study area layer must be polygonal.]]></translation>
    </message>
    <message>
        <source><![CDATA[CRS do MDE de referência inválido.]]></source>
        <translation><![CDATA[Invalid reference DEM CRS.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao reprojetar geometrias para o CRS do projeto.]]></source>
        <translation><![CDATA[Failed to reproject geometries to the project CRS.]]></translation>
    </message>
    <message>
        <source><![CDATA[A camada de área de estudo não tem polígonos válidos.]]></source>
        <translation><![CDATA[The study area layer has no valid polygons.]]></translation>
    </message>
    <message>
        <source><![CDATA[União da área de estudo está vazia.]]></source>
        <translation><![CDATA[The union of the study area is empty.]]></translation>
    </message>
    <message>
        <source><![CDATA[Camada de limite indisponível: {0}]]></source>
        <translation><![CDATA[Limit layer unavailable: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Tipo de retomada não reconhecido ({0}); aplicando limpeza completa.]]></source>
        <translation><![CDATA[Unrecognized resume type ({0}); applying full cleanup.]]></translation>
    </message>
    <message>
        <source><![CDATA[Buffers e PEC serão refeitos: camada de buffers limpa.]]></source>
        <translation><![CDATA[Buffers and PEC will be rebuilt: buffer layer cleared.]]></translation>
    </message>
    <message>
        <source><![CDATA[Correspondência e buffers serão refeitos: linhas de correspondência e buffers limpos; pares e extensão da amostra repostos.]]></source>
        <translation><![CDATA[Matching and buffers will be rebuilt: match lines and buffers cleared; pairs and sample extent reset.]]></translation>
    </message>
    <message>
        <source><![CDATA[Morfologia e etapas seguintes serão refeitas: camadas de morfologia, linhas de correspondência e buffers foram limpos; pares e extensão da amostra repostos.]]></source>
        <translation><![CDATA[Morphology and following steps will be redone: morphology layers, match lines and buffers were cleared; pairs and sample extent reset.]]></translation>
    </message>
    <message>
        <source><![CDATA[Reprocessamento completo: limites, morfologia, correspondência e buffers foram limpos; estatísticas do painel repostas.]]></source>
        <translation><![CDATA[Full reprocessing: limits, morphology, matching and buffers were cleared; panel statistics reset.]]></translation>
    </message>
    <message>
        <source><![CDATA[Não foi possível criar pasta de dados: {0} ({1})]]></source>
        <translation><![CDATA[Could not create data folder: {0} ({1})]]></translation>
    </message>
    <message>
        <source><![CDATA[Projeto OK]]></source>
        <translation><![CDATA[Project OK]]></translation>
    </message>
    <message>
        <source><![CDATA[Arquivo .pa.gpkg ausente]]></source>
        <translation><![CDATA[Missing .pa.gpkg file]]></translation>
    </message>
    <message>
        <source><![CDATA[=== Verificação GRASS (morfologia do terreno) ===]]></source>
        <translation><![CDATA[=== GRASS check (terrain morphology) ===]]></translation>
    </message>
    <message>
        <source><![CDATA[GRASS: provider não encontrado no Processing. Instale o GRASS GIS (ex.: OSGeo4W com componente GRASS) ou reinstale o QGIS com suporte GRASS.]]></source>
        <translation><![CDATA[GRASS: provider not found in Processing. Install GRASS GIS (e.g. OSGeo4W with GRASS component) or reinstall QGIS with GRASS support.]]></translation>
    </message>
    <message>
        <source><![CDATA[GRASS: provider «{0}» — {1}.]]></source>
        <translation><![CDATA[GRASS: provider «{0}» — {1}.]]></translation>
    </message>
    <message>
        <source><![CDATA[GRASS: provider não pode ser ativado (GRASS não instalado ou dependências em falta no sistema).]]></source>
        <translation><![CDATA[GRASS: provider cannot be activated (GRASS not installed or missing system dependencies).]]></translation>
    </message>
    <message>
        <source><![CDATA[GRASS: provider DESATIVADO. Ative em Configurações → Opções → Processamento → aba Providers → marque «GRASS GIS». Sem isso, a morfologia (cumeadas e hidrografia numérica) não executará.]]></source>
        <translation><![CDATA[GRASS: provider DISABLED. Enable in Settings → Options → Processing → Providers tab → check «GRASS GIS». Otherwise morphology (ridges and stream networks) will not run.]]></translation>
    </message>
    <message>
        <source><![CDATA[GRASS ativo, mas algoritmos indisponíveis: {0}.]]></source>
        <translation><![CDATA[GRASS active, but algorithms unavailable: {0}.]]></translation>
    </message>
    <message>
        <source><![CDATA[GRASS OK — morfologia pode executar. Algoritmos: {0}.]]></source>
        <translation><![CDATA[GRASS OK — morphology can run. Algorithms: {0}.]]></translation>
    </message>
    <message>
        <source><![CDATA[MDE ({0}) NÃO DEFINIDO]]></source>
        <translation><![CDATA[DEM ({0}) IS NOT DEFINED]]></translation>
    </message>
    <message>
        <source><![CDATA[=======================================
]]></source>
        <translation><![CDATA[=======================================
]]></translation>
    </message>
    <message>
        <source><![CDATA[  INFORMAÇÕES DO MDE — {0}
]]></source>
        <translation><![CDATA[  DEM INFORMATION — {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Nome da camada: {0}
]]></source>
        <translation><![CDATA[  Layer name: {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Caminho da fonte: {0}
]]></source>
        <translation><![CDATA[  Source path: {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Válida: {0}
]]></source>
        <translation><![CDATA[  Valid: {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  SRC: {0}
]]></source>
        <translation><![CDATA[  CRS: {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Largura (px): {0}
]]></source>
        <translation><![CDATA[  Width (px): {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Altura (px): {0}
]]></source>
        <translation><![CDATA[  Height (px): {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Número de bandas: {0}
]]></source>
        <translation><![CDATA[  Band count: {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Extensão: {0}
]]></source>
        <translation><![CDATA[  Extent: {0}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Tamanho do pixel X: {0:.3f}
]]></source>
        <translation><![CDATA[  Pixel size X: {0:.3f}
]]></translation>
    </message>
    <message>
        <source><![CDATA[  Tamanho do pixel Y: {0:.3f}
]]></source>
        <translation><![CDATA[  Pixel size Y: {0:.3f}
]]></translation>
    </message>
    <message>
        <source><![CDATA[Defina o projeto (.pa.gpkg): menu ⋯ → Abrir ou Novo.]]></source>
        <translation><![CDATA[Define the project (.pa.gpkg): menu ⋯ → Open or New.]]></translation>
    </message>
    <message>
        <source><![CDATA[O arquivo .pa.gpkg do projeto não existe.]]></source>
        <translation><![CDATA[The project .pa.gpkg file does not exist.]]></translation>
    </message>
    <message>
        <source><![CDATA[Selecione o MDE de referência (DEM válido).]]></source>
        <translation><![CDATA[Select the reference DEM (valid raster).]]></translation>
    </message>
    <message>
        <source><![CDATA[Selecione o MDE de teste (DEM válido).]]></source>
        <translation><![CDATA[Select the test DEM (valid raster).]]></translation>
    </message>
    <message>
        <source><![CDATA[Aguarde o fim da análise em curso antes de nova avaliação.]]></source>
        <translation><![CDATA[Wait for the current analysis to finish before starting a new evaluation.]]></translation>
    </message>
    <message>
        <source><![CDATA[Parâmetros e MDEs inalterados (última avaliação concluída ou configuração gravada no projeto).]]></source>
        <translation><![CDATA[Parameters and elevation models (MDE) are unchanged (last completed evaluation or configuration saved in the project).]]></translation>
    </message>
    <message>
        <source><![CDATA[Reprocessamento completo desde polígonos de limite e interseção.]]></source>
        <translation><![CDATA[Full reprocessing from limit polygons and intersection.]]></translation>
    </message>
    <message>
        <source><![CDATA[Retomando a partir da morfologia (parâmetros alterados).]]></source>
        <translation><![CDATA[Resuming from morphology (parameters changed).]]></translation>
    </message>
    <message>
        <source><![CDATA[Retomando a partir da correspondência de linhas (parâmetros alterados).]]></source>
        <translation><![CDATA[Resuming from line matching (parameters changed).]]></translation>
    </message>
    <message>
        <source><![CDATA[Retomando a partir dos buffers (parâmetros alterados).]]></source>
        <translation><![CDATA[Resuming from buffers (parameters changed).]]></translation>
    </message>
    <message>
        <source><![CDATA[CALCULANDO ÁREA DE INTERSEÇÃO DOS MDEs]]></source>
        <translation><![CDATA[COMPUTING DEMs INTERSECTION AREA]]></translation>
    </message>
    <message>
        <source><![CDATA[ÁREA DE INTERSEÇÃO DOS MDEs DEFINIDA
]]></source>
        <translation><![CDATA[DEMs INTERSECTION AREA DEFINED
]]></translation>
    </message>
    <message>
        <source><![CDATA[Edite a camada de interseção se necessário e prima Continuar para morfologia.]]></source>
        <translation><![CDATA[Edit the intersection layer if needed, then click Continue for morphology.]]></translation>
    </message>
    <message>
        <source><![CDATA[Caminho do projeto (.pa.gpkg) indefinido.]]></source>
        <translation><![CDATA[Project path (.pa.gpkg) is undefined.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao garantir tabelas auxiliares no .pa.gpkg.]]></source>
        <translation><![CDATA[Failed to ensure auxiliary tables in the .pa.gpkg file.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao preparar camadas de limite no projeto.]]></source>
        <translation><![CDATA[Failed to prepare limit layers in the project.]]></translation>
    </message>
    <message>
        <source><![CDATA[DEFININDO POLÍGONOS]]></source>
        <translation><![CDATA[DEFINING POLYGON LIMITS]]></translation>
    </message>
    <message>
        <source><![CDATA[DEFININDO ELEMENTOS DE MORFOLOGIA DO TERRENO - {0}]]></source>
        <translation><![CDATA[DEFINING TERRAIN MORPHOLOGY FEATURES - {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Morfologia cancelada: GRASS indisponível ou desativado. Corrija antes de continuar (ver mensagens acima).]]></source>
        <translation><![CDATA[Morphology cancelled: GRASS unavailable or disabled. Fix before continuing (see messages above).]]></translation>
    </message>
    <message>
        <source><![CDATA[RAM do sistema: {0}% em uso, {1} MB livres de {2} MB.]]></source>
        <translation><![CDATA[System RAM: {0}% in use, {1} MB free of {2} MB.]]></translation>
    </message>
    <message>
        <source><![CDATA[RAM elevada antes da morfologia — o r.watershed (GRASS) pode falhar. Feche outras aplicações, reinicie o QGIS se necessário, ou reduza «Limite de Memória para Grass GIS» nas definições do plugin.]]></source>
        <translation><![CDATA[High RAM before morphology — r.watershed (GRASS) may fail. Close other applications, restart QGIS if needed, or reduce «Grass GIS memory limit» in plugin settings.]]></translation>
    </message>
    <message>
        <source><![CDATA[AVISO: projeto/MDE sob OneDrive ou «Área de Trabalho» (caminho com acentos). Isto causa falhas intermitentes no GRASS no Windows. Copie o projeto e os rasters para um caminho local sem acentos (ex.: C:\dados\mdepa\) e pause a sincronização do OneDrive durante o processamento.]]></source>
        <translation><![CDATA[AVISO: projeto/MDE sob OneDrive ou «Área de Trabalho» (caminho com acentos). Isto causa falhas intermitentes no GRASS no Windows. Copie o projeto e os rasters para um caminho local sem acentos (ex.: C:\dados\mdepa\) e pause a sincronização do OneDrive durante o processamento.]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Linhas_de_Correspondencia__] Camadas de morfologia indisponíveis para tipo {0}.]]></source>
        <translation><![CDATA[[__Linhas_de_Correspondencia__] Morphology layers unavailable for type {0}.]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Linhas_de_Correspondencia__] Nenhuma linha de ligação foi criada.]]></source>
        <translation><![CDATA[[__Linhas_de_Correspondencia__] No link line was created.]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Linhas_de_Correspondencia__] Falha ao gravar no GPKG: {0}]]></source>
        <translation><![CDATA[[__Linhas_de_Correspondencia__] Failed to write to GPKG: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Linhas_de_Correspondencia__] {0} ligações gravadas (edite antes de Continuar se estiver em revisão).]]></source>
        <translation><![CDATA[[__Linhas_de_Correspondencia__] {0} links saved (edit before Continue if under review).]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Linhas_de_Correspondencia__] {0} feição(ões) ignoradas.]]></source>
        <translation><![CDATA[[__Linhas_de_Correspondencia__] {0} feature(s) ignored.]]></translation>
    </message>
    <message>
        <source><![CDATA[Não há pares homólogos válidos. O processamento foi interrompido antes dos buffers.

Sugestões:
• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas.
• Afrouxar a correspondência: aumentar a distância máxima entre centróides (pixels do MDE de teste) e o percentual de diferença de área entre os envelopes mínimos.]]></source>
        <translation><![CDATA[There are no valid homologous pairs. Processing was stopped before buffers.

Suggestions:
• Reduce the maximum basin area (morphology) to generate more lines.
• Loosen matching: increase the maximum distance between centroids (pixels of the test DEM) and the percentage difference in area between minimum bounding rectangles.]]></translation>
    </message>
    <message>
        <source><![CDATA[A extensão total da amostra ({0} km) é menor que a extensão mínima recomendada ({1} km). O processamento foi interrompido antes dos buffers.

Sugestões:
• Diminuir a área máxima das bacias (morfologia) para gerar mais linhas e maior extensão acumulada.
• Afrouxar a correspondência: aumentar a distância máxima entre centróides (pixels do MDE de teste) e o percentual de diferença de área entre os envelopes mínimos.]]></source>
        <translation><![CDATA[Total sample extent ({0} km) is less than the recommended minimum extent ({1} km). Processing was stopped before buffers.

Suggestions:
• Reduce the maximum basin area (morphology) to generate more lines and more accumulated extent.
• Loosen matching: increase the maximum distance between centroids (pixels of the test DEM) and the percentage difference in area between minimum bounding rectangles.]]></translation>
    </message>
    <message>
        <source><![CDATA[MDE de teste inválido — não é possível aplicar a distância máxima em pixels.]]></source>
        <translation><![CDATA[Invalid test DEM — cannot apply maximum distance in pixels.]]></translation>
    </message>
    <message>
        <source><![CDATA[GSD do MDE de teste inválido — não é possível converter pixels em distância no mapa.]]></source>
        <translation><![CDATA[Invalid test DEM GSD — cannot convert pixels to map distance.]]></translation>
    </message>
    <message>
        <source><![CDATA[Extensão total da amostra: {0} m]]></source>
        <translation><![CDATA[Extensão total da amostra: {0} m]]></translation>
    </message>
    <message>
        <source><![CDATA[Camada __Linhas_de_Correspondencia__: {0} pares. Edite, remova ou adicione linhas (meio teste → meio referência); atributos: tipo, fid_r, fid_t. Prima Continuar.]]></source>
        <translation><![CDATA[__Linhas_de_Correspondencia__ layer: {0} pairs. Edit, remove or add lines (test midpoint → reference midpoint); attributes: tipo, fid_r, fid_t. Click Continue.]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Buffers__] Tipo de geometria da camada: {0}. Gravação em lote por par (sem repaint durante o processamento).]]></source>
        <translation><![CDATA[[__Buffers__] Layer geometry type: {0}. Batch write per pair (no repaint during processing).]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Buffers__] Lote: {0} adicionadas, {1} ignoradas (geometria), {2} rejeitadas pelo fornecedor.]]></source>
        <translation><![CDATA[[__Buffers__] Batch: {0} added, {1} ignored (geometry), {2} rejected by provider.]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Buffers__] commitChanges falhou:
{0}]]></source>
        <translation><![CDATA[[__Buffers__] commitChanges failed:
{0}]]></translation>
    </message>
    <message>
        <source><![CDATA[(sem detalhe)]]></source>
        <translation><![CDATA[(no detail)]]></translation>
    </message>
    <message>
        <source><![CDATA[[__Buffers__] {0}]]></source>
        <translation><![CDATA[[__Buffers__] {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Geometria vazia ou nula — feição ignorada.]]></source>
        <translation><![CDATA[Empty or null geometry — feature ignored.]]></translation>
    </message>
    <message>
        <source><![CDATA[Geometria vazia após remover Z/M — feição ignorada.]]></source>
        <translation><![CDATA[Empty geometry after removing Z/M — feature ignored.]]></translation>
    </message>
    <message>
        <source><![CDATA[Geometria não poligonal ({0}); makeValid não produziu polígono — ignorada.]]></source>
        <translation><![CDATA[Non-polygon geometry ({0}); makeValid did not produce a polygon — ignored.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao promover polígono simples a MultiPolygon — ignorada.]]></source>
        <translation><![CDATA[Failed to promote simple polygon to MultiPolygon — ignored.]]></translation>
    </message>
    <message>
        <source><![CDATA[Polígono sem anéis — não foi possível formar MultiPolygon.]]></source>
        <translation><![CDATA[Polygon without rings — could not form MultiPolygon.]]></translation>
    </message>
    <message>
        <source><![CDATA[Geometria inválida após makeValid — ignorada.]]></source>
        <translation><![CDATA[Invalid geometry after makeValid — ignored.]]></translation>
    </message>
    <message>
        <source><![CDATA[Define buffers: a camada __Linhas_de_Correspondencia__ está vazia ou sem pares válidos.]]></source>
        <translation><![CDATA[Define buffers: __Linhas_de_Correspondencia__ layer is empty or has no valid pairs.]]></translation>
    </message>
    <message>
        <source><![CDATA[DEFININDO BUFFERS]]></source>
        <translation><![CDATA[DEFINING BUFFERS]]></translation>
    </message>
    <message>
        <source><![CDATA[Sem pares válidos em __Linhas_de_Correspondencia__ no projeto.]]></source>
        <translation><![CDATA[No valid pairs in __Linhas_de_Correspondencia__ in the project.]]></translation>
    </message>
    <message>
        <source><![CDATA[Lista de escalas vazia - verifique parâmetros de buffers.]]></source>
        <translation><![CDATA[Empty scale list — check buffer parameters.]]></translation>
    </message>
    <message>
        <source><![CDATA[Camada ausente ou inválida no GPKG: {0}]]></source>
        <translation><![CDATA[Layer missing or invalid in GPKG: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Tratamento de outliers (PEC): {0}]]></source>
        <translation><![CDATA[Outlier handling (PEC): {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[CALCULANDO PEC PLANIMÉTRICO]]></source>
        <translation><![CDATA[COMPUTING HORIZONTAL ACCURACY CLASS (PEC)]]></translation>
    </message>
    <message>
        <source><![CDATA[CALCULANDO PEC ALTIMÉTRICO]]></source>
        <translation><![CDATA[COMPUTING VERTICAL ACCURACY CLASS (PEC)]]></translation>
    </message>
    <message>
        <source><![CDATA[PEC altimétrico ignorado para escala 1:{0}.000 (sem limites definidos).]]></source>
        <translation><![CDATA[Vertical PEC skipped for scale 1:{0},000 (no limits defined).]]></translation>
    </message>
    <message>
        <source><![CDATA[Extensão amostras válidas PEC: {0} km (correspondência total: {1} km).]]></source>
        <translation><![CDATA[Valid PEC sample extent: {0} km (total matching: {1} km).]]></translation>
    </message>
    <message>
        <source><![CDATA[{0} nulo/indefinido: {1} amostra(s) ignorada(s) na respetiva escala/classe — extensão total ignorada: {2} km ({3} m)]]></source>
        <translation><![CDATA[{0} null/undefined: {1} sample(s) ignored at the respective scale/class — total extent ignored: {2} km ({3} m)]]></translation>
    </message>
    <message>
        <source><![CDATA[  1:{0}.000 — classe {1}: {2} ignorada(s), {3} m]]></source>
        <translation><![CDATA[  1:{0}.000 — class {1}: {2} ignored, {3} m]]></translation>
    </message>
    <message>
        <source><![CDATA[    ref {0} fid_r={1} | teste {2} fid_t={3} | ext. {4} m]]></source>
        <translation><![CDATA[    ref {0} fid_r={1} | test {2} fid_t={3} | ext. {4} m]]></translation>
    </message>
    <message>
        <source><![CDATA[
  … detalhe truncado ({0} amostras no total).]]></source>
        <translation><![CDATA[
  … detail truncated ({0} samples in total).]]></translation>
    </message>
    <message>
        <source><![CDATA[NORMALIDADE - FALHOU]]></source>
        <translation><![CDATA[NORMALITY - FAIL]]></translation>
    </message>
    <message>
        <source><![CDATA[FALHOU]]></source>
        <translation><![CDATA[FAIL]]></translation>
    </message>
    <message>
        <source><![CDATA[EQ {0} — 1:{1}.000-{2}= {3}, {4} amostras]]></source>
        <translation><![CDATA[EQ {0} — 1:{1}.000-{2}= {3}, {4} samples]]></translation>
    </message>
    <message>
        <source><![CDATA[1:{0}.000-{1}= {2}, {3} amostras]]></source>
        <translation><![CDATA[1:{0}.000-{1}= {2}, {3} samples]]></translation>
    </message>
    <message>
        <source><![CDATA[PASSOU]]></source>
        <translation><![CDATA[PASS]]></translation>
    </message>
    <message>
        <source><![CDATA[EQ {0} — 1:{1}.000-{2}= quant {3}% <= {4} - {5}, ext {6}% <= {4} - {7},]]></source>
        <translation><![CDATA[EQ {0} — 1:{1}.000-{2}= qty {3}% <= {4} - {5}, ext {6}% <= {4} - {7},]]></translation>
    </message>
    <message>
        <source><![CDATA[1:{0}.000-{1}= quant {2}% <= {3} - {4}, ext {5}% <= {3} - {6},]]></source>
        <translation><![CDATA[1:{0}.000-{1}= qty {2}% <= {3} - {4}, ext {5}% <= {3} - {6},]]></translation>
    </message>
    <message>
        <source><![CDATA[ {0} <= {1} EP - PASSOU, {2}]]></source>
        <translation><![CDATA[ {0} <= {1} EP - PASS, {2}]]></translation>
    </message>
    <message>
        <source><![CDATA[n/d]]></source>
        <translation><![CDATA[n/a]]></translation>
    </message>
    <message>
        <source><![CDATA[ {0} > {1} EP - FALHOU, {2}]]></source>
        <translation><![CDATA[ {0} > {1} EP - FAIL, {2}]]></translation>
    </message>
    <message>
        <source><![CDATA[1:{0}.000]]></source>
        <translation><![CDATA[1:{0}.000]]></translation>
    </message>
    <message>
        <source><![CDATA[Escala]]></source>
        <translation><![CDATA[Scale]]></translation>
    </message>
    <message>
        <source><![CDATA[EQ (m)]]></source>
        <translation><![CDATA[EQ (m)]]></translation>
    </message>
    <message>
        <source><![CDATA[Classe]]></source>
        <translation><![CDATA[Class]]></translation>
    </message>
    <message>
        <source><![CDATA[Outliers]]></source>
        <translation><![CDATA[Outliers]]></translation>
    </message>
    <message>
        <source><![CDATA[Amostras Válidas]]></source>
        <translation><![CDATA[Valid samples]]></translation>
    </message>
    <message>
        <source><![CDATA[Quant.]]></source>
        <translation><![CDATA[Qty.]]></translation>
    </message>
    <message>
        <source><![CDATA[Ext. (km)]]></source>
        <translation><![CDATA[Ext. (km)]]></translation>
    </message>
    <message>
        <source><![CDATA[PEC (90% d_i ≤ PEC-PCD)]]></source>
        <translation><![CDATA[PEC (90% d_i ≤ PEC-PCD)]]></translation>
    </message>
    <message>
        <source><![CDATA[Quantitativo]]></source>
        <translation><![CDATA[Quantitative]]></translation>
    </message>
    <message>
        <source><![CDATA[Extensão]]></source>
        <translation><![CDATA[Extent]]></translation>
    </message>
    <message>
        <source><![CDATA[Teste]]></source>
        <translation><![CDATA[Test]]></translation>
    </message>
    <message>
        <source><![CDATA[Resultado]]></source>
        <translation><![CDATA[Result]]></translation>
    </message>
    <message>
        <source><![CDATA[EP (RMS ≤ EP)]]></source>
        <translation><![CDATA[EP (RMS ≤ EP)]]></translation>
    </message>
    <message>
        <source><![CDATA[ANÁLISE PLANIMÉTRICA]]></source>
        <translation><![CDATA[HORIZONTAL ANALYSIS]]></translation>
    </message>
    <message>
        <source><![CDATA[ANÁLISE ALTIMÉTRICA]]></source>
        <translation><![CDATA[VERTICAL ANALYSIS]]></translation>
    </message>
    <message>
        <source><![CDATA[Relatório PEC gravado: {0}]]></source>
        <translation><![CDATA[PEC report saved: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Não foi possível gravar relatório PEC: {0} ({1})]]></source>
        <translation><![CDATA[Could not save PEC report: {0} ({1})]]></translation>
    </message>
    <message>
        <source><![CDATA[Estado]]></source>
        <translation><![CDATA[Estado]]></translation>
    </message>
    <message>
        <source><![CDATA[(camada de interseção indisponível)]]></source>
        <translation><![CDATA[(intersection layer unavailable)]]></translation>
    </message>
    <message>
        <source><![CDATA[(extensão vazia — execute a interseção dos MDEs)]]></source>
        <translation><![CDATA[(empty extent — run DEM intersection)]]></translation>
    </message>
    <message>
        <source><![CDATA[(sem CRS)]]></source>
        <translation><![CDATA[(sem CRS)]]></translation>
    </message>
    <message>
        <source><![CDATA[Envelope ({0})]]></source>
        <translation><![CDATA[Envelope ({0})]]></translation>
    </message>
    <message>
        <source><![CDATA[(transformação indisponível)]]></source>
        <translation><![CDATA[(transformação indisponível)]]></translation>
    </message>
    <message>
        <source><![CDATA[(ainda não há resultados de PEC nesta sessão — execute a análise até ao fim.)]]></source>
        <translation><![CDATA[(no PEC results in this session yet — run the analysis to completion.)]]></translation>
    </message>
    <message>
        <source><![CDATA[7.1 PEC Planimétrico]]></source>
        <translation><![CDATA[7.1 Horizontal PEC]]></translation>
    </message>
    <message>
        <source><![CDATA[7.2 PEC Altimétrico]]></source>
        <translation><![CDATA[7.2 Vertical PEC]]></translation>
    </message>
    <message>
        <source><![CDATA[Método de normalização de progressivas]]></source>
        <translation><![CDATA[Chainage normalization method]]></translation>
    </message>
    <message>
        <source><![CDATA[Total de pares]]></source>
        <translation><![CDATA[Total pairs]]></translation>
    </message>
    <message>
        <source><![CDATA[Ficheiro WKT dos perfis]]></source>
        <translation><![CDATA[Profiles WKT file]]></translation>
    </message>
    <message>
        <source><![CDATA[Escalar (k)]]></source>
        <translation><![CDATA[Scalar (k)]]></translation>
    </message>
    <message>
        <source><![CDATA[Média]]></source>
        <translation><![CDATA[Mean]]></translation>
    </message>
    <message>
        <source><![CDATA[Mínima]]></source>
        <translation><![CDATA[Minimum]]></translation>
    </message>
    <message>
        <source><![CDATA[Máxima]]></source>
        <translation><![CDATA[Maximum]]></translation>
    </message>
    <message>
        <source><![CDATA[Desvio Padrão]]></source>
        <translation><![CDATA[Standard deviation]]></translation>
    </message>
    <message>
        <source><![CDATA[(sem pares homólogos definidos — execute a correspondência de linhas.)]]></source>
        <translation><![CDATA[(no homologous pairs defined — run line matching.)]]></translation>
    </message>
    <message>
        <source><![CDATA[6. Pares homólogos — estatísticas]]></source>
        <translation><![CDATA[6. Homologous pairs — statistics]]></translation>
    </message>
    <message>
        <source><![CDATA[Opção]]></source>
        <translation><![CDATA[Option]]></translation>
    </message>
    <message>
        <source><![CDATA[Valor]]></source>
        <translation><![CDATA[Value]]></translation>
    </message>
    <message>
        <source><![CDATA[Data/hora]]></source>
        <translation><![CDATA[Date/time]]></translation>
    </message>
    <message>
        <source><![CDATA[Ficheiro de projeto]]></source>
        <translation><![CDATA[Project file]]></translation>
    </message>
    <message>
        <source><![CDATA[Par]]></source>
        <translation><![CDATA[Pair]]></translation>
    </message>
    <message>
        <source><![CDATA[ref_id]]></source>
        <translation><![CDATA[ref_id]]></translation>
    </message>
    <message>
        <source><![CDATA[camada_ref]]></source>
        <translation><![CDATA[ref_layer]]></translation>
    </message>
    <message>
        <source><![CDATA[Perfil ref. (WKT compatibilizado)]]></source>
        <translation><![CDATA[Ref. profile (compatibilized WKT)]]></translation>
    </message>
    <message>
        <source><![CDATA[test_id]]></source>
        <translation><![CDATA[test_id]]></translation>
    </message>
    <message>
        <source><![CDATA[camada_test]]></source>
        <translation><![CDATA[test_layer]]></translation>
    </message>
    <message>
        <source><![CDATA[Perfil teste (WKT compatibilizado)]]></source>
        <translation><![CDATA[Test profile (compatibilized WKT)]]></translation>
    </message>
    <message>
        <source><![CDATA[Escalar k (linear)]]></source>
        <translation><![CDATA[Scalar k (linear)]]></translation>
    </message>
    <message>
        <source><![CDATA[(não definido)]]></source>
        <translation><![CDATA[(not set)]]></translation>
    </message>
    <message>
        <source><![CDATA[(nenhuma)]]></source>
        <translation><![CDATA[(none)]]></translation>
    </message>
    <message>
        <source><![CDATA[(não selecionado)]]></source>
        <translation><![CDATA[(not selected)]]></translation>
    </message>
    <message>
        <source><![CDATA[Área de estudo]]></source>
        <translation><![CDATA[Área de estudo]]></translation>
    </message>
    <message>
        <source><![CDATA[Extensão mínima da amostra]]></source>
        <translation><![CDATA[Extensão mínima da amostra]]></translation>
    </message>
    <message>
        <source><![CDATA[Extensão da amostra]]></source>
        <translation><![CDATA[Extensão da amostra]]></translation>
    </message>
    <message>
        <source><![CDATA[Número de pares homólogos]]></source>
        <translation><![CDATA[Número de pares homólogos]]></translation>
    </message>
    <message>
        <source><![CDATA[Relatório — MDE AP — Acurácia Posicional]]></source>
        <translation><![CDATA[Report — MDE AP — Positional Accuracy]]></translation>
    </message>
    <message>
        <source><![CDATA[Título]]></source>
        <translation><![CDATA[Title]]></translation>
    </message>
    <message>
        <source><![CDATA[CRS de referência (análise)]]></source>
        <translation><![CDATA[Reference CRS (analysis)]]></translation>
    </message>
    <message>
        <source><![CDATA[1. Localização da área de estudo]]></source>
        <translation><![CDATA[1. Study area location]]></translation>
    </message>
    <message>
        <source><![CDATA[Envelope]]></source>
        <translation><![CDATA[Envelope]]></translation>
    </message>
    <message>
        <source><![CDATA[2. Fluxo de trabalho]]></source>
        <translation><![CDATA[2. Workflow]]></translation>
    </message>
    <message>
        <source><![CDATA[Definição da área de estudos]]></source>
        <translation><![CDATA[Study area definition]]></translation>
    </message>
    <message>
        <source><![CDATA[Pares homólogos]]></source>
        <translation><![CDATA[Homologous pairs]]></translation>
    </message>
    <message>
        <source><![CDATA[Tratamento de outliers]]></source>
        <translation><![CDATA[Outlier handling]]></translation>
    </message>
    <message>
        <source><![CDATA[Camada polígono (se aplicável)]]></source>
        <translation><![CDATA[Polygon layer (if applicable)]]></translation>
    </message>
    <message>
        <source><![CDATA[3. Modelos digitais de elevação (MDE)]]></source>
        <translation><![CDATA[3. Digital elevation models (DEM)]]></translation>
    </message>
    <message>
        <source><![CDATA[Papel]]></source>
        <translation><![CDATA[Role]]></translation>
    </message>
    <message>
        <source><![CDATA[Nome]]></source>
        <translation><![CDATA[Name]]></translation>
    </message>
    <message>
        <source><![CDATA[Fonte (início)]]></source>
        <translation><![CDATA[Source (start)]]></translation>
    </message>
    <message>
        <source><![CDATA[4. Parâmetros de processamento]]></source>
        <translation><![CDATA[4. Processing parameters]]></translation>
    </message>
    <message>
        <source><![CDATA[Parâmetro]]></source>
        <translation><![CDATA[Parameter]]></translation>
    </message>
    <message>
        <source><![CDATA[5. Estatísticas do painel]]></source>
        <translation><![CDATA[5. Panel statistics]]></translation>
    </message>
    <message>
        <source><![CDATA[7. Resultados PEC]]></source>
        <translation><![CDATA[7. PEC results]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria]]></source>
        <translation><![CDATA[Auditoria]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria concluída]]></source>
        <translation><![CDATA[Auditoria concluída]]></translation>
    </message>
    <message>
        <source><![CDATA[Defina um projeto (.pa.gpkg) para exportar a auditoria.]]></source>
        <translation><![CDATA[Define a project (.pa.gpkg) to export the audit.]]></translation>
    </message>
    <message>
        <source><![CDATA[Não foi possível criar a pasta do projeto: {0}]]></source>
        <translation><![CDATA[Could not create the project folder: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria horizontal: nenhuma escala definida.]]></source>
        <translation><![CDATA[Horizontal audit: no scale defined.]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria horizontal: sem pares homólogos.]]></source>
        <translation><![CDATA[Horizontal audit: no homologous pairs.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao carregar gerador de auditoria: {0}]]></source>
        <translation><![CDATA[Failed to load audit generator: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[A gerar relatório de auditoria horizontal ({0} pares)…]]></source>
        <translation><![CDATA[Generating horizontal audit report ({0} pairs)…]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha na auditoria horizontal: {0}]]></source>
        <translation><![CDATA[Horizontal audit failed: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria horizontal gravada: {0} ({1} páginas)]]></source>
        <translation><![CDATA[Horizontal audit saved: {0} ({1} pages)]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria vertical: nenhuma escala definida.]]></source>
        <translation><![CDATA[Vertical audit: no scale defined.]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria vertical: sem pares homólogos.]]></source>
        <translation><![CDATA[Vertical audit: no homologous pairs.]]></translation>
    </message>
    <message>
        <source><![CDATA[A gerar relatório de auditoria vertical ({0} pares)…]]></source>
        <translation><![CDATA[Generating vertical audit report ({0} pairs)…]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha na auditoria vertical: {0}]]></source>
        <translation><![CDATA[Vertical audit failed: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Auditoria vertical gravada: {0} ({1} páginas)]]></source>
        <translation><![CDATA[Vertical audit saved: {0} ({1} pages)]]></translation>
    </message>
    <message>
        <source><![CDATA[Defina um projeto (.pa.gpkg) para exportar o relatório.]]></source>
        <translation><![CDATA[Define a project (.pa.gpkg) to export the report.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao gerar ficheiro WKT dos perfis: {0} ({1})]]></source>
        <translation><![CDATA[Could not generate profiles WKT file: {0} ({1})]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao gerar PDF: {0}]]></source>
        <translation><![CDATA[Failed to generate PDF: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao gerar relatório TXT: {0} ({1})]]></source>
        <translation><![CDATA[Failed to generate TXT report: {0} ({1})]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao gravar HTML do relatório: {0} ({1})]]></source>
        <translation><![CDATA[Failed to save report HTML: {0} ({1})]]></translation>
    </message>
    <message>
        <source><![CDATA[Relatório PDF exportado: {0}]]></source>
        <translation><![CDATA[PDF report exported: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Relatório TXT v1 exportado (parseável → PDF): {0}]]></source>
        <translation><![CDATA[TXT report v1 exported (parseable → PDF): {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Ficheiro WKT dos perfis exportado: {0}]]></source>
        <translation><![CDATA[Profiles WKT file exported: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Relatório HTML exportado: {0}]]></source>
        <translation><![CDATA[HTML report exported: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Relatórios na pasta do projeto: PDF + TXT (+ HTML se aplicável).]]></source>
        <translation><![CDATA[Reports in the project folder: PDF + TXT (+ HTML if applicable).]]></translation>
    </message>
    <message>
        <source><![CDATA[Buffer - {0}]]></source>
        <translation><![CDATA[Buffer - {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[{0} {1} - {2}]]></source>
        <translation><![CDATA[{0} {1} - {2}]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao gravar limite ({0}): {1}]]></source>
        <translation><![CDATA[Falha ao gravar limite ({0}): {1}]]></translation>
    </message>
    <message>
        <source><![CDATA[commitChanges falhou em {0}: {1}]]></source>
        <translation><![CDATA[commitChanges falhou em {0}: {1}]]></translation>
    </message>
    <message>
        <source><![CDATA[Foram identificados {0} valores atípicos (excluídos do cálculo PEC). Prima OK para continuar.]]></source>
        <translation><![CDATA[{0} outlier values were identified (excluded from ACC calculation). Click OK to continue.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha ao exportar relatórios.]]></source>
        <translation><![CDATA[Failed to export reports.]]></translation>
    </message>
    <message>
        <source><![CDATA[Falha na auditoria: {0}]]></source>
        <translation><![CDATA[Audit failed: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Parâmetros]]></source>
        <translation><![CDATA[Parameters]]></translation>
    </message>
    <message>
        <source><![CDATA[Definições para Geração de Morfologia]]></source>
        <translation><![CDATA[Morphology generation settings]]></translation>
    </message>
    <message>
        <source><![CDATA[Máxima Área das Bacias (m²)]]></source>
        <translation><![CDATA[Maximum basin area (m²)]]></translation>
    </message>
    <message>
        <source><![CDATA[Limite de Memória para Grass GIS (GB)]]></source>
        <translation><![CDATA[Grass GIS memory limit (GB)]]></translation>
    </message>
    <message>
        <source><![CDATA[Definições para Seleção dos Pares]]></source>
        <translation><![CDATA[Feature-pair selection settings]]></translation>
    </message>
    <message>
        <source><![CDATA[Distância máxima entre centróides (pixels do MDE de teste)]]></source>
        <translation><![CDATA[Maximum distance between centroids (pixels of the test DEM)]]></translation>
    </message>
    <message>
        <source><![CDATA[Diferença % entre área dos mínimos envelopes]]></source>
        <translation><![CDATA[Percent difference between minimum bounding rectangle areas]]></translation>
    </message>
    <message>
        <source><![CDATA[Definições para Geração Buffers]]></source>
        <translation><![CDATA[Buffer generation settings]]></translation>
    </message>
    <message>
        <source><![CDATA[Máxima Escala]]></source>
        <translation><![CDATA[Maximum scale]]></translation>
    </message>
    <message>
        <source><![CDATA[Mínima Escala]]></source>
        <translation><![CDATA[Minimum scale]]></translation>
    </message>
    <message>
        <source><![CDATA[Padrão Brasileiro - PEC PCD]]></source>
        <translation><![CDATA[Brazilian Standard - PEC PCD]]></translation>
    </message>
    <message>
        <source><![CDATA[CE90 e LE90]]></source>
        <translation><![CDATA[CE90 and LE90]]></translation>
    </message>
    <message>
        <source><![CDATA[Máximo Horizontal (pixels do MDE de teste)]]></source>
        <translation><![CDATA[Maximum Horizontal (test DEM pixels)]]></translation>
    </message>
    <message>
        <source><![CDATA[Máximo Vertical (pixels do MDE de teste)]]></source>
        <translation><![CDATA[Maximum Vertical (test DEM pixels)]]></translation>
    </message>
    <message>
        <source><![CDATA[Mostrar buffers no mapa durante o processamento]]></source>
        <translation><![CDATA[Show buffers on the map during processing]]></translation>
    </message>
    <message>
        <source><![CDATA[Não]]></source>
        <translation><![CDATA[No]]></translation>
    </message>
    <message>
        <source><![CDATA[Sim]]></source>
        <translation><![CDATA[Yes]]></translation>
    </message>
    <message>
        <source><![CDATA[Definições para Normalização de Progressivas]]></source>
        <translation><![CDATA[Chainage normalization settings]]></translation>
    </message>
    <message>
        <source><![CDATA[Método para Normalização]]></source>
        <translation><![CDATA[Normalization method]]></translation>
    </message>
    <message>
        <source><![CDATA[Fórmula para cálculo da Discrepância Média]]></source>
        <translation><![CDATA[Formula for Mean Discrepancy calculation]]></translation>
    </message>
    <message>
        <source><![CDATA[Relatório de Auditoria]]></source>
        <translation><![CDATA[Audit Report]]></translation>
    </message>
    <message>
        <source><![CDATA[Horizontal]]></source>
        <translation><![CDATA[Horizontal]]></translation>
    </message>
    <message>
        <source><![CDATA[Vertical]]></source>
        <translation><![CDATA[Vertical]]></translation>
    </message>
    <message>
        <source><![CDATA[Restaurar]]></source>
        <translation><![CDATA[Restore defaults]]></translation>
    </message>
    <message>
        <source><![CDATA[Salvar]]></source>
        <translation><![CDATA[Save]]></translation>
    </message>
    <message>
        <source><![CDATA[Idioma da interface]]></source>
        <translation><![CDATA[Interface language]]></translation>
    </message>
    <message>
        <source><![CDATA[Fechar]]></source>
        <translation><![CDATA[Close]]></translation>
    </message>
    <message>
        <source><![CDATA[Idioma do QGIS ({0})]]></source>
        <translation><![CDATA[QGIS language ({0})]]></translation>
    </message>
    <message>
        <source><![CDATA[Para criar tradução num idioma ainda sem ficheiro .qm:
1. Copie i18n/pos_accuracy_en.ts para pos_accuracy_<locale>.ts (ex.: pos_accuracy_es_ES.ts).
2. Traduza no Qt Linguist ou edite o .ts (contexto PositionalAccuracyPlugin).
3. Compile: execute i18n/build_translations.bat qm-only (requer pyside6-lrelease ou lrelease do OSGeo4W).
4. Confirme que pos_accuracy_<locale>.qm ficou na pasta i18n/ e recarregue o plugin.
Use pos_accuracy_en.ts como modelo — é a tradução completa de referência.
Idioma de desenvolvimento (textos fonte): pt_BR.]]></source>
        <translation><![CDATA[To create a translation for a locale that has no .qm file yet:
1. Copy i18n/pos_accuracy_en.ts to pos_accuracy_<locale>.ts (e.g. pos_accuracy_es_ES.ts).
2. Translate in Qt Linguist or edit the .ts (context PositionalAccuracyPlugin).
3. Compile: run i18n/build_translations.bat qm-only (requires pyside6-lrelease or OSGeo4W lrelease).
4. Ensure pos_accuracy_<locale>.qm is in the i18n/ folder and reload the plugin.
Use pos_accuracy_en.ts as the reference template — it is the complete English translation.
Development language (source strings): pt_BR.]]></translation>
    </message>
    <message>
        <source><![CDATA[Tradução: {0}]]></source>
        <translation><![CDATA[Translation: {0}]]></translation>
    </message>
    <message>
        <source><![CDATA[Idioma de desenvolvimento ({0})]]></source>
        <translation><![CDATA[Development language ({0})]]></translation>
    </message>
    <message>
        <source><![CDATA[{0} não encontrado]]></source>
        <translation><![CDATA[{0} not found]]></translation>
    </message>
</context>
</TS>
