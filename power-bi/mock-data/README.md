# 📈 Power BI Mock Data

## Overview

This directory contains the synthetic CSV datasets used to refresh the public version of the **Absenteeism Analytics Dashboard**.

The files reproduce the table names, column names, relationships, data types, and analytical behavior expected by the Power BI semantic model while removing real employee, organizational, medical, and operational information.

> These datasets are intended for portfolio demonstration and report testing only. They must not be interpreted as production, clinical, HR, or occupational-health data.

---

## Directory structure

```text
power-bi/mock-data/
├── README.md
├── d_calendario.csv
├── d_cid.csv
├── d_colab.csv
├── f_abs.csv
└── f_cat.csv
```

---

## Dataset summary

| File | Semantic role | Rows | Purpose |
|---|---|---:|---|
| `d_calendario.csv` | Dimension | 1,096 | Calendar attributes from 2024 through 2026. |
| `d_cid.csv` | Dimension | 10 | Fully anonymized health-group hierarchy. |
| `d_colab.csv` | Dimension | 36 | Synthetic employee, demographic, and organizational attributes. |
| `f_abs.csv` | Fact | 180 | Synthetic absence, certificate, leave, and duration events. |
| `f_cat.csv` | Fact | 30 | Synthetic occupational accident and CAT-related events. |

---

# Dataset descriptions

## `d_calendario.csv`

Calendar dimension used by the report's temporal filters and analyses.

### Main attributes

```text
Ano
ano_mes
dia
Dia da Semana
dia_semana_num
id_data
mes
Mês
Mês e Ano
nome_dia_semana
nome_mes
semestre
semestre_desc
trimestre
trimestre_desc
```

### Date range

```text
2024-01-01 to 2026-12-31
```

### Main relationship

```text
d_calendario[id_data]
    -> f_abs[dt_inicio_afastamento]
```

The current PBIX model does not show an active calendar relationship to `f_cat`.

---

## `d_cid.csv`

Synthetic health-classification dimension used to replace real ICD chapters, categories, subcategories, codes, and descriptions.

### Main attributes

```text
CID Capítulo
CID Categoria
CID Grupo
CID Subcategoria
CID Subgrupo
id_categoria
id_cid
subcategoria
```

### Public health groups

```text
Health Group A
Health Group B
Health Group C
Health Group D
Other Health Groups
Not Reported
```

### Synthetic category keys

```text
HGA
HGB
HGC
HGD
OTH
NRI
```

### Synthetic condition keys

```text
HGA-01
HGA-02
HGB-01
HGB-02
HGC-01
HGC-02
HGD-01
HGD-02
OTHER-01
NO_ICD
```

### Main relationship

```text
d_cid[id_cid]
    -> f_abs[id_cid]
```

The correspondence between these synthetic groups and the original monitored classifications is intentionally excluded from the repository.

---

## `d_colab.csv`

Synthetic employee dimension containing demographic and organizational attributes required by the report.

### Main attributes

```text
Área
Cargo
Centro de Custos
Colaborador
desc_deficiencia
dt_snapshot
Faixa Etária
id_colaborador_sk
id_matricula_funcionario
id_matricula_rh
nome_perimetro
num_cpf
num_idade
origem_dado
Sexo
Tipo de Deficiência
Unidade
```

### Synthetic conventions

```text
Employee 01 to Employee 36
RH0001 to RH0036
EMP0001 to EMP0036
Unit A to Unit E
Area A to Area D
Perimeter A to Perimeter D
Role A to Role E
CC001 to CC005
```

The `num_cpf` values are synthetic placeholders and are not valid personal identifiers.

### Relationship fields

```text
d_colab[id_matricula_rh]
f_abs[id_matricula]
f_cat[id_matricula]
```

The mock dimension contains unique `id_matricula_rh` values and can support conventional one-to-many relationships if the PBIX is refactored.

---

## `f_abs.csv`

Synthetic absenteeism fact used by the majority of report pages and measures.

### Grain

One row represents one synthetic absence, medical certificate, or leave event.

### Main attributes

```text
ano_afastamento
Doença
dt_criacao
dt_fim_afastamento
dt_inicio_afastamento
dt_snapshot
flg_tipo_duracao
hora_fim_atestado
hora_inicio_atestado
id_atestado
id_cid
id_matricula
nome_unidade
num_cpf
origem_dado
qtd_dias_afastado
qtd_horas_afastado
Status
Tipo de Afastamento
tipo_atestado
tipo_licenca
tipo_registro
total_dias_afastamento
total_horas_afastamento
```

### Synthetic condition labels

```text
Condition A.1
Condition A.2
Condition B.1
Condition B.2
Condition C.1
Condition C.2
Condition D.1
Condition D.2
Other condition
Not reported
```

### Duration scenarios

The file contains:

- day-based absences;
- hourly absences;
- records with synthetic health classifications;
- records classified as not reported;
- current-system and historical-source records;
- standardized analytical duration in days and hours.

### Main measurements

```text
total_dias_afastamento
total_horas_afastamento
```

The analytical conversion follows:

```text
1 workday = 8 hours
```

---

## `f_cat.csv`

Synthetic occupational-accident fact retained in the semantic model for CAT-related analysis and future report extension.

### Grain

One row represents one synthetic occupational-accident record.

### Main attributes

```text
ano_acidente
desc_agente_causador
desc_arvore_causa
desc_atividade_momento
desc_causa_acidente
desc_local_acidente
desc_ocorrencia
desc_parte_corpo
desc_potencial_acidente
desc_situacao_geradora
dt_acidente
dt_registro
dt_ultimo_dia_trabalho
flg_afastamento
flg_morte
id_cat
id_matricula
nome_area
nome_emitente
num_cpf
qtd_dias_afastado
tipo_acidente
tipo_cat
tipo_registro
```

### Synthetic scenarios

The file contains:

- accidents with leave;
- accidents without leave;
- generic accident types;
- generic causing agents;
- generic affected body parts;
- generic locations and areas;
- low, moderate, and high synthetic potential classifications.

---

# Expected relationships

The mock files support these conventional relationships:

```text
d_calendario[id_data]
    1 -> * f_abs[dt_inicio_afastamento]

d_calendario[id_data]
    1 -> * f_cat[dt_acidente]

d_cid[id_cid]
    1 -> * f_abs[id_cid]

d_colab[id_matricula_rh]
    1 -> * f_abs[id_matricula]

d_colab[id_matricula_rh]
    1 -> * f_cat[id_matricula]
```

The current PBIX may retain different cardinalities to preserve the original report model. See:

```text
power-bi/data-model.md
```

---

# Power BI replacement mapping

Each CSV replaces one semantic-model table without requiring table or column renaming:

```text
d_calendario.csv -> d_calendario
d_cid.csv        -> d_cid
d_colab.csv      -> d_colab
f_abs.csv        -> f_abs
f_cat.csv        -> f_cat
```

Preserving the existing names reduces the risk of breaking:

- relationships;
- visuals;
- filters;
- hierarchies;
- DAX measures;
- drill-through pages;
- tooltip pages.

---

# Refreshing the public PBIX

Recommended process:

1. duplicate the original PBIX;
2. remove or disable production-source queries;
3. connect each semantic table to its matching CSV;
4. preserve table and column names;
5. verify date, number, text, and Boolean data types;
6. refresh all tables;
7. review relationship cardinalities and filter directions;
8. replace the original monitored-group measures with the anonymized measures;
9. inspect hidden pages, drill-through pages, tooltips, filters, and query parameters;
10. save the sanitized report as `absenteeism-analytics.pbix`.

Detailed Power BI documentation is available in:

```text
power-bi/README.md
power-bi/measures.md
power-bi/data-model.md
```

---

# Privacy controls

The mock datasets replace:

- employee names;
- employee registrations;
- personal identifiers;
- units and locations;
- areas and perimeters;
- job titles;
- health chapters and codes;
- clinical descriptions;
- monitored health groups;
- accident descriptions;
- operational values.

The files must not contain:

- real names;
- valid personal identifiers;
- internal employee numbers;
- real organizational structures;
- real ICD codes;
- original monitored chapter labels;
- diagnosis descriptions;
- medical notes;
- production catalog or schema names;
- internal file-system paths.

---

# Validation checklist

Before committing an updated mock dataset, confirm that:

- all five CSV files are present;
- table and column names remain compatible with the PBIX;
- `d_colab[id_matricula_rh]` contains the employee keys used in both facts;
- `d_cid[id_cid]` contains every health key used in `f_abs`;
- `d_calendario[id_data]` covers every event date used by the facts;
- no original ICD chapter, category, code, or description remains;
- no real employee or organizational value remains;
- Boolean fields import correctly;
- date and timestamp fields parse correctly;
- total-day and total-hour fields remain numeric;
- the PBIX refresh completes without missing-column errors;
- visible and hidden report pages use synthetic values only.

---

# Difference from `data/sample/`

The repository contains two different synthetic data areas:

```text
data/sample/
└── source-compatible examples for the engineering pipeline

power-bi/mock-data/
└── semantic-model tables ready to refresh the public PBIX
```

The files in this directory should not be presented as raw API responses or direct pipeline-source samples.

---

# Known considerations

- The mock data prioritize structural and analytical coverage over realistic production distributions.
- The perimeter slicer may truncate labels because the public mock contains more perimeter categories than the original layout.
- The `Afastados Hoje` measure may return `0` when no static mock interval includes the current date.
- Bradford Factor values are synthetic and must not be interpreted as operational results.
- The accident fact is included even though the current dashboard pages focus mainly on absenteeism.
- The calendar ends on `2026-12-31` and should be extended if later event dates are added.

---

# Related documentation

```text
power-bi/README.md
power-bi/dashboard-pages.md
power-bi/measures.md
power-bi/data-model.md
screenshots/README.md
```

