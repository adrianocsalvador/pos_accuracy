# MDE AP — Acurácia Posicional

Plugin QGIS para avaliar a **acurácia posicional tridimensional** de Modelos Digitais de Elevação (MDE), comparando um MDE de **referência** com um MDE de **teste** usando feições lineares e perfis altimétricos.

Desenvolvido no [Programa de Pós-Graduação em Engenharia Civil (PPGEC)](https://posengenhariacivil.ufv.br/) da [Universidade Federal de Viçosa (UFV)](https://www.ufv.br).

**Repositório:** [github.com/adrianocsalvador/pos_accuracy](https://github.com/adrianocsalvador/pos_accuracy)

---

## O que o plugin faz

A cadeia automática:

1. **Área de estudo** — interseção dos MDEs, edição manual ou polígono de uma camada.
2. **Morfologia** — extração de linhas (cumeadas e hidrografia) com *watershed* GRASS (`r.watershed`).
3. **Pares homólogos** — correspondência entre linhas de referência e teste.
4. **Buffers e discrepância média (DM)** — buffer duplo planimétrico e perfil altimétrico.
5. **Classificação** — padrão brasileiro **PEC-PCD** (classes A–D) ou busca de limiar **CE90 / LE90**.
6. **Relatórios** — PDF/TXT do resultado e, opcionalmente, PDFs/CSV de auditoria.

Os resultados e as camadas intermediárias ficam no ficheiro de projeto **`.pa.gpkg`**.

---

## Requisitos

| Item | Valor |
|------|--------|
| QGIS | ≥ 3.30 |
| GRASS GIS | fornecedor de processamento activo (morfologia) |
| Licença | GNU GPL v2 (ou posterior) |

---

## Instalação

### Gestor de complementos (recomendado, após publicação)

1. **Complementos → Gerir e instalar complementos → Todos**.
2. Procurar **MDE AP** e instalar.
3. Confirmar que o fornecedor **GRASS** está activo em *Processamento*.

### Cópia manual / ZIP

1. Copiar a pasta `pos_accuracy` para o directório de plugins do perfil QGIS, por exemplo:

   `…/QGIS3/profiles/default/python/plugins/pos_accuracy`

2. Em **Complementos → Gerir e instalar complementos**, activar **MDE AP - Acurácia Posicional**.

3. Confirmar que o fornecedor **GRASS** está activo em *Processamento*.

Para gerar o ZIP do repositório oficial (`plugins.qgis.org`):

```
python package_plugin.py
```

O ficheiro fica em `dist/pos_accuracy-<versão>.zip`. Inclui só o runtime do plugin (`mods`, `i18n/*.qm`, `icons`, `styles`). As pastas `scripts*` **permanecem no GitHub** para desenvolvimento e validação; **não entram** no ZIP enviado ao `plugins.qgis.org`.

---

## Uso rápido

1. **Novo** ou **Abrir** um projeto (`.pa.gpkg`).
2. Seleccionar o **MDE de referência** e o **MDE de teste**.
3. Definir a área de estudo e, se necessário, os parâmetros em **Config**.
4. Premir **Avaliar**.

Se o projecto já estiver calculado e só alguns parâmetros mudarem, o plugin **retoma a cadeia no passo mais básico alterado** (não volta a correr tudo).

---

## Funcionalidades

- Projecto persistente (`.pa.gpkg`) com MDEs, morfologia, pares, buffers e configuração.
- Fluxo de área de estudo: interseção automática, revisão após interseção, ou camada polígono.
- Selecção de pares automática ou com **revisão** no mapa.
- Tratamento de *outliers* (automático / revisão / usar todos).
- Compatibilização de progressivas para o perfil altimétrico (linear, por proximidade, ou nenhuma).
- Duas fórmulas de DM (buffer duplo).
- Relatório PDF com tabelas PEC ou CE90/LE90.
- Auditoria horizontal e/ou vertical (PDF e/ou CSV).
- Interface em português, inglês e espanhol.
- Retoma inteligente: morfologia → pares → buffers → compatibilização (só altimetria) → fórmula DM.

---

## Parâmetros padrão (Config)

### Morfologia

| Parâmetro | Padrão | Notas |
|-----------|--------|--------|
| Máxima área das bacias | **675 000 m²** | Controla a densidade das linhas extraídas |
| Limite de memória GRASS | **4 GB** | Ajustar conforme a RAM disponível |

Alterar estes valores **regenera as linhas** (watershed). A área de interseção dos MDEs **não** é recalculada.

### Selecção dos pares

| Parâmetro | Padrão | Notas |
|-----------|--------|--------|
| Distância máxima entre centróides | **3 px** do MDE de teste | Convertida para metros via GSD do teste |
| Diferença % da **área** dos mínimos envelopes | **10 %** | Relativa à área do envelope de teste |
| Diferença % do **comprimento** dos mínimos envelopes | **5 %** | Lado maior do envelope orientado |
| Extensão mínima da feição de teste | **10 px** do MDE de teste | Comprimento da linha de teste |

Alterar só estes valores **repete a correspondência e os passos seguintes**, sem refazer a morfologia.

### Buffers e padrão de acurácia

| Parâmetro | Padrão |
|-----------|--------|
| Padrão | **PEC-PCD** (Brasil) |
| Escala máxima / mínima (PEC-PCD) | **1:10 000** |
| Máximo horizontal CE90 | **5 px** do MDE de teste |
| Máximo vertical LE90 | **2 px** do MDE de teste |
| Mostrar buffers no mapa durante o processamento | **desmarcado** |

Alterar definições de buffer **regenera buffers e o cálculo de DM/PEC**, sem rematch.

### Compatibilização de progressivas

| Método | Índice | Padrão |
|--------|--------|--------|
| Linear | 0 | **sim** |
| Por proximidade | 1 | |
| Sem compatibilização | 2 | |

Mudar o método **recalcula a DM altimétrica** (perfis); a camada de buffers planimétricos no mapa não é regravada.

### Fórmula da discrepância média (DM)

**Original (padrão)**

\[
dm_i = \pi \cdot x \cdot \frac{A_{2i} - A_{3i}}{A_{1i}}
\]

**Média (A₁+A₂)/2**

\[
dm_i = \pi \cdot x \cdot \frac{(A_{1i}+A_{2i})/2 - A_{3i}}{(A_{1i}+A_{2i})/2}
\]

- \(x\) — raio do buffer (PEC da escala/classe, ou limiar CE90/LE90)
- \(A_1\) — área do buffer da feição de teste
- \(A_2\) — área do buffer da feição de referência
- \(A_3\) — área da interseção dos buffers

Mudar só a fórmula **recalcula a DM** sem voltar a gerar as áreas no GeoPackage.

### Relatório de auditoria

Por omissão **desligado**. Ao activar **Horizontal (PDF)** e/ou **Vertical (PDF)**, gera-se o PDF e **sempre** o CSV correspondente.

---

## Padrões de acurácia

### PEC-PCD (Brasil)

Classes **A, B, C, D**. Limites planimétricos em milímetros na escala; altimétricos = **EQ(escala) × coeficiente**.

EQ por escala nominal (extracto): 1:10 000 → 5; 1:25 000 → 10; 1:50 000 → 20; 1:100 000 → 50.

### CE90 / LE90

Busca o **menor limiar (m)** que cumpre, em conjunto:

- ≥ 90 % das amostras com \(d_i \le\) limiar  
- ≥ 90 % da extensão com \(d_i \le\) limiar  
- RMS \(\le\) EP (razões da classe A do PEC-PCD)  
- teste de normalidade nas amostras (após IQR)

O tecto da busca é `máx. H/V (pixels) × GSD` do MDE de teste.

---

## Retoma da cadeia

Se o projecto já foi calculado e vários parâmetros mudam, usa-se o **passo mais básico** e segue-se até ao fim:

1. Morfologia  
2. Selecção de pares  
3. Buffers  
4. Compatibilização (só altimetria)  
5. Fórmula DM  

Mudar MDEs ou o modo de área de estudo implica **reprocessamento desde a interseção**. Opções só de auditoria **não** disparam recálculo.

---

## Saídas

Na pasta do projecto (junto ao `.pa.gpkg`):

- Relatório PDF / TXT / HTML  
- `Audit_horizontal_…_CE90_….pdf` / `.csv` (se activo)  
- `Audit_vertical_…_LE90_….pdf` / `.csv` (se activo)  

No GeoPackage: limites, interseção, linhas de morfologia, pares, buffers.

---

## Créditos

- **Adriano Caliman Salvador** — [adriano.caliman@ufv.br](mailto:adriano.caliman@ufv.br)  
- [Universidade Federal de Viçosa](https://www.ufv.br)  
- [PPGEC — Programa de Pós-Graduação em Engenharia Civil](https://posengenhariacivil.ufv.br/)

---

## Licença

GNU General Public License v2 (ou posterior). Ver o ficheiro `LICENSE`.
