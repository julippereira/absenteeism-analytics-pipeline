# Power BI Measures

## Overview

This document describes the DAX measures used by the public, anonymized version of the **Absenteeism Analytics Dashboard**.

The measures are stored in the dedicated `Medidas` table and use the existing Power BI semantic-model names:

```text
d_calendario
d_cid
d_colab
f_abs
f_cat
Medidas
```

The public PBIX replaces the original monitored health classifications with synthetic groups:

```text
Health Group A -> HGA
Health Group B -> HGB
Health Group C -> HGC
Health Group D -> HGD
```

The mapping between these public groups and the original monitored classifications is intentionally excluded from the repository, PBIX, screenshots, DAX names, and documentation.

---

## Measure catalog

```text
Medidas
├── Afastados Hoje
├── Fator de Bradford
├── Grupo A | Atestados
├── Grupo A | Horas
├── Grupo B | Atestados
├── Grupo B | Horas
├── Grupo C | Atestados
├── Grupo C | Horas
├── Grupo D | Atestados
├── Grupo D | Horas
├── Número de Atestados
├── Total de Horas de Afastamento
└── Última Atualização do Pipeline
```

All count and total measures use `COALESCE` so cards and charts display `0` rather than a blank value when the current filter context contains no records.

---

# Core measures

## Número de Atestados

Returns the distinct number of absence records in the current filter context.

```DAX
Número de Atestados =
COALESCE(
    DISTINCTCOUNT('f_abs'[id_atestado]),
    0
)
```

### Used in

- executive overview;
- ICD analysis;
- condition and subgroup analysis;
- location analysis;
- frequency analysis;
- time series;
- employee analysis;
- demographic analysis.

---

## Total de Horas de Afastamento

Returns the standardized total absence hours in the current filter context.

```DAX
Total de Horas de Afastamento =
COALESCE(
    SUM('f_abs'[total_horas_afastamento]),
    0
)
```

The source field already contains the standardized analytical duration:

```text
day-based absence  -> reported days x 8 hours
hourly absence     -> reported or converted duration in hours
```

### Used in

- executive overview;
- ICD analysis;
- condition and subgroup analysis;
- location analysis;
- frequency analysis;
- time series;
- employee analysis;
- demographic analysis.

---

## Afastados Hoje

Returns the distinct number of employees whose leave interval includes the current date.

```DAX
Afastados Hoje =
COALESCE(
    CALCULATE(
        DISTINCTCOUNT('f_abs'[id_matricula]),
        FILTER(
            'f_abs',
            'f_abs'[dt_inicio_afastamento] <= TODAY()
                && 'f_abs'[dt_fim_afastamento] >= TODAY()
        )
    ),
    0
)
```

### Behavior

- counts distinct employee registrations rather than absence records;
- includes records starting today;
- includes records ending today;
- returns `0` when no active leave exists;
- respects the remaining report-filter context.

> This measure is dynamic because `TODAY()` is evaluated according to the Power BI refresh and query environment.

---

## Última Atualização do Pipeline

Returns the latest available snapshot timestamp from the absenteeism fact.

```DAX
Última Atualização do Pipeline =
MAX('f_abs'[dt_snapshot])
```

### Recommended formatting

```text
dd/MM/yyyy HH:mm
```

If the report must also consider the accident fact, use the combined version below:

```DAX
Última Atualização do Pipeline =
VAR AbsenteeismSnapshot =
    MAX('f_abs'[dt_snapshot])
VAR AccidentSnapshot =
    MAX('f_cat'[dt_registro])
RETURN
    MAX(AbsenteeismSnapshot, AccidentSnapshot)
```

The first version is preferable when the card is intended to represent the absenteeism pipeline only.

---

# Anonymized health-group measures

## Design rule

The public group measures filter by the synthetic key `d_cid[id_categoria]`, not by the visible chapter description.

Use:

```DAX
'd_cid'[id_categoria] = "HGA"
```

Do not use a clinical description or original chapter code in public DAX.

`KEEPFILTERS` preserves the existing report context, including year, unit, gender, perimeter, duration type, leave type, and other active selections.

---

## Grupo A | Atestados

```DAX
Grupo A | Atestados =
COALESCE(
    CALCULATE(
        [Número de Atestados],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGA"
        )
    ),
    0
)
```

---

## Grupo A | Horas

```DAX
Grupo A | Horas =
COALESCE(
    CALCULATE(
        [Total de Horas de Afastamento],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGA"
        )
    ),
    0
)
```

---

## Grupo B | Atestados

```DAX
Grupo B | Atestados =
COALESCE(
    CALCULATE(
        [Número de Atestados],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGB"
        )
    ),
    0
)
```

---

## Grupo B | Horas

```DAX
Grupo B | Horas =
COALESCE(
    CALCULATE(
        [Total de Horas de Afastamento],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGB"
        )
    ),
    0
)
```

---

## Grupo C | Atestados

```DAX
Grupo C | Atestados =
COALESCE(
    CALCULATE(
        [Número de Atestados],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGC"
        )
    ),
    0
)
```

---

## Grupo C | Horas

```DAX
Grupo C | Horas =
COALESCE(
    CALCULATE(
        [Total de Horas de Afastamento],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGC"
        )
    ),
    0
)
```

---

## Grupo D | Atestados

```DAX
Grupo D | Atestados =
COALESCE(
    CALCULATE(
        [Número de Atestados],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGD"
        )
    ),
    0
)
```

---

## Grupo D | Horas

```DAX
Grupo D | Horas =
COALESCE(
    CALCULATE(
        [Total de Horas de Afastamento],
        KEEPFILTERS(
            'd_cid'[id_categoria] = "HGD"
        )
    ),
    0
)
```

---

# Bradford Factor

## Purpose

The Bradford Factor highlights repeated short-term absence patterns. In the public dashboard, the measure is calculated for each employee over a rolling 12-month period.

The analytical expression is:

```text
Bradford Factor = absence episodes² x total absence days
```

Where:

- `absence episodes` is the distinct number of certificates;
- `total absence days` is the standardized duration in days;
- the period is limited to the latest 12 months in the current date context.

## Fator de Bradford

```DAX
Fator de Bradford =
VAR EndDate =
    MAX('d_calendario'[id_data])
VAR StartDate =
    EDATE(EndDate, -12) + 1
VAR AbsenceEpisodes =
    CALCULATE(
        DISTINCTCOUNT('f_abs'[id_atestado]),
        DATESBETWEEN(
            'd_calendario'[id_data],
            StartDate,
            EndDate
        )
    )
VAR AbsenceDays =
    CALCULATE(
        SUM('f_abs'[total_dias_afastamento]),
        DATESBETWEEN(
            'd_calendario'[id_data],
            StartDate,
            EndDate
        )
    )
RETURN
    COALESCE(
        POWER(AbsenceEpisodes, 2) * AbsenceDays,
        0
    )
```

## Evaluation context

When the measure is displayed by employee, the employee filter from `d_colab` is propagated to `f_abs`. The measure then calculates the score for each synthetic employee within the rolling period.

## Interpretation note

The public values are generated from synthetic data and must not be interpreted as real occupational-health results, thresholds, diagnoses, or recommendations.

The repository does not define risk bands because Bradford Factor interpretation depends on the organization's policy and analytical context.

---

# Optional support measures

These measures are not required by the current report but can simplify validation and future documentation.

## Total de Dias de Afastamento

```DAX
Total de Dias de Afastamento =
COALESCE(
    SUM('f_abs'[total_dias_afastamento]),
    0
)
```

---

## Número de Colaboradores Afastados

Returns the number of distinct employees with at least one absence record in the selected context.

```DAX
Número de Colaboradores Afastados =
COALESCE(
    DISTINCTCOUNT('f_abs'[id_matricula]),
    0
)
```

This is not equivalent to `Afastados Hoje` because it does not require the absence interval to include the current date.

---

## Média de Horas por Atestado

```DAX
Média de Horas por Atestado =
DIVIDE(
    [Total de Horas de Afastamento],
    [Número de Atestados],
    0
)
```

---

# Measure dependencies

```text
Número de Atestados
    ├── Grupo A | Atestados
    ├── Grupo B | Atestados
    ├── Grupo C | Atestados
    ├── Grupo D | Atestados
    └── Média de Horas por Atestado

Total de Horas de Afastamento
    ├── Grupo A | Horas
    ├── Grupo B | Horas
    ├── Grupo C | Horas
    ├── Grupo D | Horas
    └── Média de Horas por Atestado

d_calendario[id_data]
    └── Fator de Bradford
```

---

# Formatting recommendations

| Measure | Recommended format |
|---|---|
| `Número de Atestados` | Whole number |
| `Total de Horas de Afastamento` | Whole number or `#,##0.0` |
| `Afastados Hoje` | Whole number |
| `Grupo A-D / Atestados` | Whole number |
| `Grupo A-D / Horas` | Whole number or `#,##0.0` |
| `Fator de Bradford` | Whole number with thousands separator |
| `Última Atualização do Pipeline` | `dd/MM/yyyy HH:mm` |
| `Média de Horas por Atestado` | `#,##0.0` |

---

# Required model relationships

The measures assume the following active relationships:

```text
d_calendario[id_data]
    1 -> * f_abs[dt_inicio_afastamento]

d_cid[id_cid]
    1 -> * f_abs[id_cid]

d_colab[id_matricula_rh]
    1 -> * f_abs[id_matricula]
```

Recommended filter direction:

```text
Dimension -> Fact
```

The group measures require `d_cid[id_categoria]` to filter `f_abs` through the active `id_cid` relationship.

The Bradford Factor requires `d_calendario[id_data]` to filter `f_abs[dt_inicio_afastamento]` through the active date relationship.

---

# Privacy requirements

Public measures do not contain:

- original ICD chapter letters;
- real ICD codes;
- clinical chapter descriptions;
- organization-specific monitored-group names;
- internal thresholds or confidential risk rules;
- real employee identifiers;
- production table, catalog, or schema names.

Use only the public synthetic category keys:

```text
HGA
HGB
HGC
HGD
```

The private correspondence between the synthetic keys and the original monitored groups will not be committed to the repository.

