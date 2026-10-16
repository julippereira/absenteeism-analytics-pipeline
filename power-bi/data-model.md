# Power BI Data Model

## Overview

The public **Absenteeism Analytics Dashboard** uses a compact semantic model composed of three dimensions, two fact tables, and one dedicated measure table.

The model preserves the structure of the original Power BI report while replacing employee, organizational, occupational-health, and monitored health-group data with synthetic values.

![Power BI semantic model](../screenshots/13-data-model.png)

---

## Model structure

```text
d_calendario
d_cid
d_colab
f_abs
f_cat
Medidas
```

| Table | Type | Purpose |
|---|---|---|
| `d_calendario` | Dimension | Shared date attributes for time-based analysis. |
| `d_cid` | Dimension | Anonymized hierarchy of health groups, categories, and subcategories. |
| `d_colab` | Dimension | Synthetic employee, demographic, and organizational attributes. |
| `f_abs` | Fact | Absenteeism, certificate, leave-duration, and health-group events. |
| `f_cat` | Fact | Synthetic occupational accident and CAT-related events. |
| `Medidas` | Measure table | Central storage for reusable DAX measures. |

---

## Model diagram interpretation

The public model view shows the following relationships:

```text
d_cid           1 ───── * f_abs

d_calendario    1 ───── * f_abs

d_colab         * ───── * f_abs

d_colab         * ───── 1 f_cat
```

The arrow indicators in the model view show single-direction filtering along the configured relationship paths.

Two aspects are intentionally documented as part of the current PBIX implementation:

1. the relationship between `d_colab` and `f_abs` is many-to-many;
2. the relationship between `d_colab` and `f_cat` places `f_cat` on the `1` side and `d_colab` on the `*` side.

These cardinalities reproduce the current report model shown in `13-data-model.png`. They are not presented as the preferred dimensional-model design.

---

# Dimension tables

## `d_calendario`

The calendar dimension supports year, month, quarter, semester, weekday, and date-based filtering.

### Main fields

| Field | Type | Description |
|---|---|---|
| `Ano` | Integer | Calendar year. |
| `ano_mes` | Integer | Numeric year-month key in `YYYYMM` format. |
| `dia` | Integer | Day of the month. |
| `Dia da Semana` | Text | Display name for the weekday. |
| `dia_semana_num` | Integer | Numeric weekday order. |
| `id_data` | DateTime | Calendar date key. |
| `mes` | Integer | Month number. |
| `Mês` | Text | Display name for the month. |
| `Mês e Ano` | Text | Combined month and year label. |
| `nome_dia_semana` | Text | Technical weekday-name attribute. |
| `nome_mes` | Text | Technical month-name attribute. |
| `semestre` | Integer | Semester number. |
| `semestre_desc` | Text | Semester description. |
| `trimestre` | Integer | Quarter number. |
| `trimestre_desc` | Text | Quarter description. |

### Current relationship

```text
d_calendario[id_data]
    1 ───── * f_abs[dt_inicio_afastamento]
```

This active relationship drives the date filters used by the absenteeism pages, including frequency analysis, time series, monitored-group evolution, and rolling-period measures.

### Model note

The screenshot does not show a relationship between `d_calendario` and `f_cat`. As a result, calendar selections do not automatically filter accident records through the displayed model configuration.

---

## `d_cid`

The health-classification dimension provides an anonymized hierarchy for analytical navigation.

### Main fields

| Field | Type | Description |
|---|---|---|
| `CID Capítulo` | Text | Public synthetic health chapter. |
| `CID Categoria` | Text | Public synthetic category. |
| `CID Grupo` | Text | Public analytical group. |
| `CID Subcategoria` | Text | Public synthetic subcategory. |
| `CID Subgrupo` | Text | Public analytical subgroup. |
| `id_categoria` | Text | Synthetic health-group key. |
| `id_cid` | Text | Synthetic condition key. |
| `subcategoria` | Text | Synthetic subcategory identifier. |

### Public hierarchy example

```text
Health Group A
└── Analytical Group A
    └── Category A.1
        └── Subcategory A.1
```

The public category keys are:

```text
HGA
HGB
HGC
HGD
OTH
NRI
```

The original monitored health classifications are not retained in the mock dataset or public DAX.

### Current relationship

```text
d_cid[id_cid]
    1 ───── * f_abs[id_cid]
```

The dimension filters the absenteeism fact and supports chapter, group, category, subcategory, condition, and monitored-group analysis.

---

## `d_colab`

The employee dimension stores synthetic employee, demographic, and organizational attributes.

### Main fields

| Field | Type | Description |
|---|---|---|
| `Área` | Text | Anonymized organizational area. |
| `Cargo` | Text | Generic job-role label. |
| `Centro de Custos` | Text | Synthetic cost-center identifier. |
| `Colaborador` | Text | Synthetic employee label. |
| `desc_deficiencia` | Text | Generic disability description. |
| `dt_snapshot` | DateTime | Dimension snapshot timestamp. |
| `Faixa Etária` | Text | Employee age group. |
| `id_colaborador_sk` | Text | Stable analytical employee key. |
| `id_matricula_funcionario` | Text | Synthetic employee registration. |
| `id_matricula_rh` | Text | Synthetic HR registration. |
| `nome_perimetro` | Text | Anonymized perimeter. |
| `num_cpf` | Text | Synthetic personal identifier. |
| `num_idade` | Integer | Synthetic employee age. |
| `origem_dado` | Text | Current or historical source classification. |
| `Sexo` | Text | Synthetic gender attribute. |
| `Tipo de Deficiência` | Text | Generic disability classification. |
| `Unidade` | Text | Anonymized organizational unit. |

### Public values

```text
Employee 01
RH0001
EMP0001
SYNTH00000000001
Unit A
Area A
Perimeter A
Role A
CC001
```

### Current relationships

The model screenshot shows:

```text
d_colab * ───── * f_abs
```

and:

```text
d_colab * ───── 1 f_cat
```

The relationships use employee-registration attributes in the current PBIX. The visible cardinalities indicate that the selected relationship columns are not unique on the expected employee-dimension side.

### Design note

The public mock CSV contains unique values in `d_colab[id_matricula_rh]`. Therefore, a conventional star-schema alternative can be created as:

```text
d_colab[id_matricula_rh]
    1 ───── * f_abs[id_matricula]

d_colab[id_matricula_rh]
    1 ───── * f_cat[id_matricula]
```

This alternative is recommended for future refactoring because it provides a single employee dimension filtering both facts. However, the relationship image and this document retain the current PBIX configuration rather than claiming that the refactoring has already been applied.

---

# Fact tables

## `f_abs`

The absenteeism fact is the central table in the current semantic model.

### Grain

One row represents one synthetic absence, certificate, or leave event.

### Main fields

| Field | Type | Description |
|---|---|---|
| `ano_afastamento` | Integer | Year derived from the absence start date. |
| `Doença` | Text | Anonymized condition description. |
| `dt_criacao` | DateTime | Record creation timestamp. |
| `dt_fim_afastamento` | DateTime | Final absence date. |
| `dt_inicio_afastamento` | DateTime | Absence start date. |
| `dt_snapshot` | DateTime | Fact snapshot timestamp. |
| `flg_tipo_duracao` | Text | Indicates day-based or hourly duration. |
| `hora_fim_atestado` | Text | End time for hourly events. |
| `hora_inicio_atestado` | Text | Start time for hourly events. |
| `id_atestado` | Text | Synthetic absence-event identifier. |
| `id_cid` | Text | Foreign key to `d_cid`. |
| `id_matricula` | Text | Synthetic employee registration. |
| `nome_unidade` | Text | Anonymized unit recorded on the fact. |
| `num_cpf` | Text | Synthetic personal identifier. |
| `origem_dado` | Text | Current or historical source classification. |
| `qtd_dias_afastado` | Number | Reported duration in days. |
| `qtd_horas_afastado` | Text | Reported hourly duration. |
| `Status` | Text | Current leave-record status. |
| `Tipo de Afastamento` | Text | Hourly or day-based absence. |
| `tipo_atestado` | Text | Generic certificate classification. |
| `tipo_licenca` | Text | Generic leave classification. |
| `tipo_registro` | Text | Analytical record type. |
| `total_dias_afastamento` | Number | Standardized duration in analytical days. |
| `total_horas_afastamento` | Number | Standardized duration in analytical hours. |

### Relationships

```text
d_calendario  1 ───── * f_abs
d_cid         1 ───── * f_abs
d_colab       * ───── * f_abs
```

### Primary analytical uses

- medical certificate counts;
- absence-hour totals;
- employees currently on leave;
- synthetic health-group monitoring;
- time-series analysis;
- organizational comparisons;
- demographic analysis;
- employee-level analysis;
- Bradford Factor calculation.

---

## `f_cat`

The accident fact stores synthetic occupational accident and CAT-related events.

### Grain

One row represents one synthetic occupational accident record.

### Main fields

| Field | Type | Description |
|---|---|---|
| `ano_acidente` | Integer | Year derived from the accident date. |
| `desc_agente_causador` | Text | Generic causing-agent classification. |
| `desc_arvore_causa` | Text | Generic cause-tree classification. |
| `desc_atividade_momento` | Text | Generic activity at the event time. |
| `desc_causa_acidente` | Text | Generic accident cause. |
| `desc_local_acidente` | Text | Generic event location. |
| `desc_ocorrencia` | Text | Synthetic event description. |
| `desc_parte_corpo` | Text | Generic affected-body-part classification. |
| `desc_potencial_acidente` | Text | Synthetic potential-severity classification. |
| `desc_situacao_geradora` | Text | Generic generating-situation classification. |
| `dt_acidente` | DateTime | Accident date. |
| `dt_registro` | DateTime | Record creation timestamp. |
| `dt_ultimo_dia_trabalho` | DateTime | Last working day before associated leave. |
| `flg_afastamento` | Boolean | Indicates whether the event caused leave. |
| `flg_morte` | Boolean | Synthetic fatality indicator. |
| `id_cat` | Text | Synthetic CAT identifier. |
| `id_matricula` | Text | Synthetic employee registration. |
| `nome_area` | Text | Anonymized area. |
| `nome_emitente` | Text | Generic issuer label. |
| `num_cpf` | Text | Synthetic personal identifier. |
| `qtd_dias_afastado` | Number | Leave days associated with the event. |
| `tipo_acidente` | Text | Generic accident classification. |
| `tipo_cat` | Text | Generic CAT classification. |
| `tipo_registro` | Text | Record type. |

### Current relationship

The screenshot shows:

```text
f_cat 1 ───── * d_colab
```

The relationship direction and cardinality differ from the conventional star-schema expectation. No visible relationship connects `f_cat` to `d_calendario` in the current model view.

### Current analytical scope

The uploaded report pages focus primarily on absenteeism. The accident fact remains available in the semantic model for future occupational-accident analysis and model extension.

---

# Measure table

## `Medidas`

`Medidas` is a disconnected organizational table used only to store DAX measures.

It contains measures such as:

```text
Afastados Hoje
Fator de Bradford
Grupo A | Atestados
Grupo A | Horas
Grupo B | Atestados
Grupo B | Horas
Grupo C | Atestados
Grupo C | Horas
Grupo D | Atestados
Grupo D | Horas
Número de Atestados
Total de Horas de Afastamento
Última Atualização do Pipeline
```

The table does not require a physical relationship with the dimensions or facts.

Detailed measure definitions are available in:

```text
power-bi/measures.md
```

---

# Relationship summary

| From | Cardinality | To | Filter behavior | Status in current model |
|---|---:|---|---|---|
| `d_calendario` | `1:*` | `f_abs` | Single direction | Active and visible |
| `d_cid` | `1:*` | `f_abs` | Single direction | Active and visible |
| `d_colab` | `*:*` | `f_abs` | Single direction shown | Active and visible |
| `f_cat` | `1:*` | `d_colab` | Single direction shown | Active and visible |
| `d_calendario` | Not configured | `f_cat` | None | Not visible |
| `d_cid` | Not applicable | `f_cat` | None | Not configured |

---

# Pipeline-to-Power-BI mapping

The semantic model retains the original PBIX table names to avoid rebuilding visuals and measures.

| Power BI table | Public pipeline equivalent |
|---|---|
| `d_calendario` | Semantic-model calendar dimension |
| `d_cid` | `dim_icd` |
| `d_colab` | `dim_employee` |
| `f_abs` | `fact_absenteeism` |
| `f_cat` | `fact_occupational_accident` |
| `Medidas` | DAX measure container |

The physical names in Power BI are intentionally preserved even though the public data-engineering layer uses English table names.

---

# Public mock-data mapping

The PBIX can be refreshed with:

```text
power-bi/mock-data/
├── d_calendario.csv
├── d_cid.csv
├── d_colab.csv
├── f_abs.csv
└── f_cat.csv
```

The mock files preserve:

- table names;
- column names;
- compatible data types;
- employee-registration links;
- date links;
- synthetic health-group links;
- visual behavior.

The mock files replace:

- employee identities;
- tax identifiers;
- organizational units;
- areas and perimeters;
- job titles;
- health classifications;
- monitored groups;
- medical conditions;
- accident descriptions;
- operational values.

---

# Model limitations and refactoring opportunities

## Current many-to-many employee relationship

The current `d_colab` to `f_abs` relationship is many-to-many. This can produce ambiguous filtering or unexpected totals if duplicate employee-registration values exist on both sides.

The mock dimension was generated with unique `id_matricula_rh` values, enabling a conventional one-to-many relationship if the model is refactored.

## Current accident relationship

The screenshot places `f_cat` on the `1` side and `d_colab` on the `*` side. For a standard star schema, the preferred structure would be:

```text
d_colab[id_matricula_rh]
    1 ───── * f_cat[id_matricula]
```

## Missing calendar relationship for accidents

No visible relationship connects:

```text
d_calendario[id_data]
```

to:

```text
f_cat[dt_acidente]
```

Without this relationship, shared calendar filters do not propagate to accident records.

## Preserved PBIX structure

These points are documented rather than silently changed because the public repository aims to preserve the existing dashboard behavior and avoid unnecessary report reconstruction.

---

# Privacy and publication controls

The public semantic model must not retain:

- real employee names;
- real employee registrations;
- real personal identifiers;
- real units, areas, perimeters, or cost centers;
- organization-specific job titles;
- real ICD codes;
- original monitored ICD chapter labels;
- real diagnosis descriptions;
- free-text medical observations;
- internal data-source paths;
- production catalog or schema names.

The public `d_cid` must use only synthetic keys and labels, including:

```text
HGA
HGB
HGC
HGD
Health Group A
Health Group B
Health Group C
Health Group D
```

The correspondence between these values and the original monitored classifications must not be included in the repository.

---

# Related documentation

```text
power-bi/dashboard-pages.md
power-bi/measures.md
data/schemas/data-dictionary.md
screenshots/README.md
```
