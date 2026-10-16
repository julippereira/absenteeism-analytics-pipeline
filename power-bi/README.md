# Power BI

## Overview

This directory contains the public, anonymized Power BI assets for the **Absenteeism Analytics Dashboard**.

The report connects the curated analytical model to an interactive semantic layer covering absenteeism, medical certificates, monitored health groups, temporal patterns, organizational comparisons, employee-level analysis, demographic attributes, and occupational accident data.

The public version preserves the original report structure while replacing all sensitive, proprietary, and organization-specific information with synthetic values.

> The PBIX, screenshots, DAX measures, and mock datasets in this repository are intended for portfolio demonstration only. They must not be used for clinical, HR, legal, or operational decision-making.

---

## Directory structure

```text
power-bi/
├── README.md
├── dashboard-pages.md
├── measures.md
├── data-model.md
├── absenteeism-analytics.pbix
├── mock-data/
│   ├── d_calendario.csv
│   ├── d_cid.csv
│   ├── d_colab.csv
│   ├── f_abs.csv
│   └── f_cat.csv
└── theme/
    └── absenteeism-theme.json
```

Related screenshots are stored outside this directory:

```text
screenshots/
├── 01-overview.png
├── 02-icd-analysis.png
├── 03-disease-subgroup.png
├── 04-location-analysis.png
├── 05-frequency-analysis.png
├── 06-time-series.png
├── 07-health-group-evolution.png
├── 08-hours-evolution.png
├── 09-certificates-evolution.png
├── 10-employee-analysis.png
├── 11-demographics.png
├── 12-detail-table.png
└── 13-data-model.png
```

---

## Report pages

The dashboard contains 12 analytical pages:

```text
1. Resumo 2026
2. CID-10
3. Doença e Subgrupo
4. Local
5. Frequência
6. Série Temporal
7. Evolução CID
8. Evolução Horas
9. Evolução Atestados
10. Colaborador
11. Demográfico
12. Tabela
```

The pages support:

- executive KPI monitoring;
- hierarchical health-group analysis;
- condition and subgroup comparisons;
- organizational and location analysis;
- weekday, monthly, and yearly frequency analysis;
- time-series investigation;
- monitored-group evolution;
- employee-level analysis;
- Bradford Factor analysis;
- demographic slicing;
- record-level validation.

Detailed page documentation is available in:

```text
power-bi/dashboard-pages.md
```

---

## Semantic model

The public Power BI model contains:

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
| `d_calendario` | Dimension | Calendar attributes and time filtering. |
| `d_cid` | Dimension | Anonymized health hierarchy. |
| `d_colab` | Dimension | Synthetic employee, demographic, and organizational attributes. |
| `f_abs` | Fact | Absenteeism, certificate, leave, and duration events. |
| `f_cat` | Fact | Synthetic occupational accident records. |
| `Medidas` | Measure table | Centralized DAX measures. |

The model retains the original PBIX table names to avoid rebuilding report visuals and calculations.

### Pipeline mapping

| Power BI table | Public pipeline equivalent |
|---|---|
| `d_calendario` | Semantic-model calendar dimension |
| `d_cid` | `dim_icd` |
| `d_colab` | `dim_employee` |
| `f_abs` | `fact_absenteeism` |
| `f_cat` | `fact_occupational_accident` |
| `Medidas` | DAX measure container |

Detailed model documentation is available in:

```text
power-bi/data-model.md
```

---

## Measures

The report uses a dedicated `Medidas` table.

Main measures include:

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

The public health-group measures filter synthetic category keys:

```text
HGA
HGB
HGC
HGD
```

No original monitored chapter code or clinical description is retained in the public DAX.

Detailed formulas and measure behavior are documented in:

```text
power-bi/measures.md
```

---

## Mock datasets

The public PBIX is supplied by five synthetic CSV files:

```text
power-bi/mock-data/
├── d_calendario.csv
├── d_cid.csv
├── d_colab.csv
├── f_abs.csv
└── f_cat.csv
```

These files were designed to preserve:

- existing table names;
- existing column names;
- compatible data types;
- date relationships;
- employee-registration relationships;
- health-classification relationships;
- DAX behavior;
- report-page behavior.

The mock datasets contain:

```text
d_calendario   1,096 rows
d_cid             10 rows
d_colab           36 rows
f_abs            180 rows
f_cat             30 rows
```

### Synthetic conventions

```text
Employee 01 to Employee 36
RH0001 and EMP0001
Unit A to Unit E
Area A to Area D
Perimeter A to Perimeter D
Role A to Role E
CC001 to CC005
```

These values do not correspond to real people, locations, organizational structures, or records.

---

## Health-group anonymization

The public report does not retain real monitored ICD chapters, categories, subcategories, codes, or descriptions.

The public hierarchy uses:

```text
Health Group A
Health Group B
Health Group C
Health Group D
Other Health Groups
Not Reported
```

Synthetic condition labels include:

```text
Condition A.1
Condition A.2
Condition B.1
Condition B.2
Other condition
Not reported
```

The private correspondence between these public groups and the original monitored classifications must not be committed to:

- the PBIX;
- DAX formulas;
- CSV files;
- screenshots;
- Markdown documentation;
- image metadata;
- hidden pages;
- tooltips;
- drill-through pages.

---

## PBIX publication

The repository may include:

```text
power-bi/absenteeism-analytics.pbix
```

Only the fully mock-data version may be published.

Before committing the file, confirm that the PBIX does not contain:

- imported production data;
- real employee names or registrations;
- real personal identifiers;
- real units, areas, departments, or perimeters;
- organization-specific job titles;
- real ICD codes or monitored chapter descriptions;
- diagnosis descriptions or medical notes;
- production database names;
- internal catalog or schema names;
- server addresses;
- local file-system paths;
- workspace or tenant information;
- credentials or authentication details;
- hidden report pages containing sensitive values;
- sensitive tooltip or drill-through content.

If a fully sanitized PBIX cannot be produced, omit the file and publish only the screenshots, mock CSVs, and documentation.

---

## Data-source replacement

The public PBIX should point to the CSV files under:

```text
power-bi/mock-data/
```

When replacing production queries with mock files:

1. duplicate the original PBIX;
2. remove or disable production data-source queries;
3. connect each model table to its corresponding mock CSV;
4. preserve table and column names;
5. verify data types;
6. refresh the model;
7. review relationship cardinalities;
8. confirm that all measures still return expected results;
9. review hidden pages, filters, tooltips, and drill-through pages;
10. save the result as the public PBIX.

The mock tables map directly to the semantic model:

```text
d_calendario.csv -> d_calendario
d_cid.csv        -> d_cid
d_colab.csv      -> d_colab
f_abs.csv        -> f_abs
f_cat.csv        -> f_cat
```

---

## Current model considerations

The current model screenshot documents these relationships:

```text
d_cid           1 ───── * f_abs

d_calendario    1 ───── * f_abs

d_colab         * ───── * f_abs

d_colab         * ───── 1 f_cat
```

The current PBIX therefore contains:

- a many-to-many relationship between `d_colab` and `f_abs`;
- an inverted cardinality between `d_colab` and `f_cat` relative to a conventional star schema;
- no visible calendar relationship to `f_cat`.

The mock `d_colab.csv` contains unique `id_matricula_rh` values. A future model refactoring may therefore use:

```text
d_colab[id_matricula_rh]
    1 ───── * f_abs[id_matricula]

d_colab[id_matricula_rh]
    1 ───── * f_cat[id_matricula]

d_calendario[id_data]
    1 ───── * f_cat[dt_acidente]
```

The repository documents the current PBIX structure rather than claiming that this refactoring has already been implemented.

---

## Theme

The optional theme file is stored in:

```text
power-bi/theme/absenteeism-theme.json
```

The theme should define reusable visual settings such as:

- color palette;
- typography;
- chart colors;
- card formatting;
- slicer styling;
- background colors;
- title styles.

The theme must not include company names, logos, proprietary color references, or internal metadata.

If no standalone theme file was exported from the PBIX, the `theme/` folder may be omitted.

---

## Screenshots

The public report screenshots are stored in:

```text
screenshots/
```

The screenshot catalog and publication checklist are documented in:

```text
screenshots/README.md
```

The model image used by `data-model.md` is:

```text
screenshots/13-data-model.png
```

Some perimeter labels may appear truncated because the public mock dataset contains more synthetic categories than the original report layout was designed to display. This does not affect filter behavior or analytical results.

---

## Refresh and date behavior

The public mock calendar covers:

```text
2024-01-01 to 2026-12-31
```

The model uses `d_calendario[id_data]` as the date dimension key and `f_abs[dt_inicio_afastamento]` as the active absenteeism date.

The `Afastados Hoje` measure uses `TODAY()`. As a result, the card can return `0` when the static mock dataset contains no leave interval covering the date on which the report is viewed.

The latest pipeline update card is based on the synthetic snapshot timestamp stored in `f_abs[dt_snapshot]`.

---

## Known visual considerations

- The perimeter slicer can display truncated labels because the public mock includes four perimeters while the original layout was designed for fewer values.
- Blank categories may appear when optional relationships or source attributes are empty.
- The mock dataset prioritizes structural coverage over realistic production distribution.
- Bradford Factor values are synthetic and must not be interpreted as real health or workforce indicators.
- The accident fact is present in the semantic model even though the current report pages focus mainly on absenteeism.

---

## Privacy checklist

Before publishing any Power BI asset, verify that:

- all report data come from the mock CSV files;
- all employee labels are synthetic;
- all registrations and identifiers are synthetic;
- all units, areas, roles, departments, and perimeters are generic;
- health groups use only the public synthetic labels;
- DAX formulas do not expose original monitored classifications;
- the original `CID F`, `CID M`, `CID S`, and `CID Z` measure names have been removed;
- disease labels are synthetic;
- hidden pages and tooltips contain no production data;
- query parameters contain no internal paths;
- credentials and data-source permissions are removed;
- screenshots show only the mock report;
- PBIX metadata does not expose internal project or workspace identifiers.

---

## Related documentation

```text
power-bi/dashboard-pages.md
power-bi/measures.md
power-bi/data-model.md
data/schemas/data-dictionary.md
screenshots/README.md
```

---

## Scope limitations

- The mock report demonstrates analytical structure rather than production-scale volume.
- Synthetic health groups do not represent clinical classifications.
- Synthetic demographic and organizational distributions do not represent a real workforce.
- The PBIX is a portfolio artifact and not a production deployment package.
- Gateway configuration, refresh schedules, workspace deployment, and access-control settings are intentionally excluded.
