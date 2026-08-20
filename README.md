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

1. **Complementos → Gerenciar e instalar complementos → Todos**.
2. Procurar **MDE AP** e instalar.
3. Confirmar que o fornecedor **GRASS** está activo em *Processamento*.

### Cópia manual / ZIP

1. Copiar a pasta `pos_accuracy` para o directório de plugins do perfil QGIS, por exemplo:

   `…/QGIS3/profiles/default/python/plugins/pos_accuracy`

2. Em **Complementos → Gerenciar e instalar complementos**, activar **MDE AP - Acurácia Posicional**.

3. Confirmar que o fornecedor **GRASS** está activo em *Processamento*.

Para gerar o ZIP do repositório oficial (`plugins.qgis.org`):

```
python package_plugin.py
```

O ficheiro fica em `dist/pos_accuracy-<versão>.zip`. Inclui só o runtime do plugin (`mods`, `i18n/*.qm`, `icons`, `styles`). As pastas `scripts*` **permanecem no GitHub** para desenvolvimento e validação; **não entram** no ZIP enviado ao `plugins.qgis.org`.

---

## Uso rápido

1. **Novo** ou **Abrir** um projeto (`.pa.gpkg`).
2. Selecionar o **MDE de referência** e o **MDE de teste**.
3. Se necessário Definir a área de estudo e os parâmetros em **Config**.
4. Premir **Avaliar**.

Se o projecto já estiver calculado e só alguns parâmetros mudarem, o plugin **retoma a cadeia no passo mais básico alterado** (não volta a rodar tudo).

---

## Funcionalidades

- Persistência no projeto (`.pa.gpkg`) com MDEs, morfologia, pares, buffers e configuração.
- Determinação da área de estudo:
  - interseção dos MDEs (automática)
  - revisão após interseção
  - camada polígono
- Selecção de pares:
  - automática usando parâmetros definidos
  - com **revisão** no mapa
- Tratamento de *outliers*:
  - remoção automática
  - revisão
  - usar tudo
- Compatibilização de progressivas para o perfil altimétrico:
  - linear
  - por proximidade
  - ou nenhuma
- Fórmulas de DM (buffer duplo):
  - Original
  - Utilizando média $(A_{T} + A_{R}) / 2$
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
| Distância máxima entre centróides | **3 pixels** do MDE de teste | Convertida para metros via GSD do teste |
| Diferença % da **área** dos mínimos envelopes | **5 %** | Deferênça percentual entre às áreas do mínimo envelope orientado de teste e mínimo envelope orientado de referência|
| Diferença % do **comprimento** dos mínimos envelopes | **5 %** | Deferênça percentual entre lados maiores dos mínimos envelopes orientados de teste e de referência|
| Extensão mínima da feição de teste | **10 pixels** do MDE de teste | Comprimento da linha de teste |

Alterar só estes valores **repete a correspondência e os passos seguintes**, sem refazer a morfologia.

### Buffers e padrão de acurácia

| Parâmetro | Padrão |
|-----------|--------|
| Padrão | **PEC-PCD** (Brasil) |
| Escala máxima  (PEC-PCD) | **1:10 000** |
| Escala mínima (PEC-PCD) | **1:25 000** |


| Parâmetro | Padrão |
|-----------|--------|
| Padrão | **CE90 e LE90** (Internacional) |
| Máximo horizontal CE90 | **5 pixels** do MDE de teste |
| Máximo vertical LE90 | **2 pixels** do MDE de teste |
| Critério utilizado para aprovação | |
| Quantitavo | Aprova se 90 %, ou mais, dos pares homólogos foram menores que o limiar (raio)|
| Extensão | Aprova se 90%, ou mais, da extensão dos pares homólogos foram menores que o limiar (raio)|
| RMS (EP) | Aprova se RMS da amostra é menor ou igual ao EP (0,17 x raio) |


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

$$
dm_{i} = \pi \cdot x \cdot \frac{A_{R} - A_{I}}{A_{T}}
$$

**Média $(A_{T} + A_{R})/2$**

$$
dm_{i} = \pi \cdot x \cdot \frac{(A_{T}+A_{R})/2 - A_{I}}{(A_{T}+A_{R})/2}
$$

- $x$ — raio do buffer (PEC da escala/classe, ou limiar CE90/LE90)
- $A_{T}$ — área do buffer da feição de teste
- $A_{R}$ — área do buffer da feição de referência
- $A_{I}$ — área da interseção dos buffers

Mudar só a fórmula **recalcula a DM** sem voltar a gerar as áreas no GeoPackage.

### Relatório de auditoria

Por omissão **desligado**. Ao activar **Horizontal (PDF)** e/ou **Vertical (PDF)**, gera-se o PDF e **sempre** os CSV correspondentes são gerados.

---

## Padrões de acurácia
- ≥ 90 % das amostras com \(d_i \le\) limiar  
- ≥ 90 % da extensão com \(d_i \le\) limiar  
- RMS \(\le\) EP (razões da classe A do PEC-PCD) 

### PEC-PCD (Brasil) 

Classes **A, B, C, D**.
Escalas:

- 1:1.000
- 1:2.000
- 1:5.000
- 1:10.000
- 1:25.000
- 1:50.000
- 1:100.000
- 1:250.000
- 1:500.000
- 1:1.000.000


#### Planimetria

Decreto n.º 89.817/84 e ET-CQDG. $E$ é o denominador da escala, desta forma PEC-PCD para a classe A da escala 1/1.000 é 0,28 metros e EP 0,17 metros.

| Classe | PEC-PCD | EP |
|--------|---------|-----|
| A | 0,28 × $E$ | 0,17 × $E$ |
| B | 0,50 × $E$ | 0,30 × $E$ |
| C | 0,80 × $E$ | 0,50 × $E$ |
| D | 1,00 × $E$ | 0,60 × $E$ |

#### Atimetria

Para a altimetria utiliza-se a Equidistância vertical (EQ) usual para a escala:

| Escala | EQ (m) |
|--------|--------|
| 1:1.000 | 1 |
| 1:2.000 | 1 |
| 1:5.000 | 2 |
| 1:10.000 | 5 |
| 1:25.000 | 10 |
| 1:50.000 | 20 |
| 1:100.000 | 50 |
| 1:250.000 | 100 |
| 1:500.000 | 100 |
| 1:1.000.000 | 100 |

Os limites altimétricos são **EQ(Equidistância) × coeficiente**. Por exemplo, na classe A da escala 1:1.000 (EQ = 1 m) o PEC-PCD é 0,27 m e o EP 0,17 m.

| Classe | PEC-PCD | EP |
|--------|---------|-----|
| A | 0,27 × $EQ$ | 0,17 × $EQ$ |
| B | 0,50 × $EQ$ | 0,33 × $EQ$ |
| C | 0,60 × $EQ$ | 0,40 × $EQ$ |
| D | 0,75 × $EQ$ | 0,50 × $EQ$ |

### CE90 / LE90

Busca o **menor limiar (m)** que cumpre, em conjunto:

O limites mínimos são 0 (zero) e os máximos são definidos em função do `pixel (GSD)` do MDE de teste.

---

## Retoma da processamento

Se o projeto já foi calculado e vários parâmetros mudam, usa-se o **passo mais básico** e segue-se até ao fim:

1. Morfologia  
2. Selecção de pares  
3. Buffers  
4. Compatibilização (só altimetria)  
5. Fórmula DM  

Mudar MDEs ou o modo de área de estudo implica **reprocessamento desde a definição da área**. Opções só de auditoria **não** disparam recálculo.

---

## Saídas

Na pasta do projecto (junto ao `.pa.gpkg`):

- Relatório PDF / TXT / HTML  
- `Audit_horizontal_…_CE90_….csv` / `.pdf` (se activo)  
- `Audit_vertical_…_LE90_….csv` / `.pdf` (se activo) 

No GeoPackage: limites, interseção, linhas de morfologia, pares, buffers.

---

## Créditos

- **Adriano Caliman Salvador** — [adriano.caliman@ufv.br](mailto:adriano.caliman@ufv.br)  
- [Universidade Federal de Viçosa](https://www.ufv.br)  
- [PPGEC — Programa de Pós-Graduação em Engenharia Civil](https://posengenhariacivil.ufv.br/)

---

## Licença

GNU General Public License v2 (ou posterior). Ver o ficheiro `LICENSE`.
