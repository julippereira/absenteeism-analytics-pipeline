# 💾 Data

## Overview

This directory contains the public data assets used to document and demonstrate the **Absenteeism Analytics Pipeline**.

All datasets included in this repository are fully synthetic. They reproduce the expected structure and relationships of the pipeline without exposing real employee information, medical records, organizational names, internal identifiers, or proprietary source data.

> The sample files are intended for portfolio demonstration, schema inspection, and local testing only. They must not be treated as production data.

---

## Directory structure

```text
data/
├── README.md
├── sample/
│   ├── employee_master_sample.csv
│   ├── employee_disability_sample.csv
│   ├── absenteeism_sample.csv
│   ├── medical_leave_details_sample.csv
│   ├── historical_absenteeism_sample.csv
│   ├── occupational_accident_sample.csv
│   └── organizational_perimeter_sample.csv
└── schemas/
    └── data-dictionary.md
```

---

## Sample datasets

### `employee_master_sample.csv`

Synthetic employee master data used to build the employee dimension.

It includes:

- generic employee identifiers;
- labels such as `Employee A`, `Employee B`, and `Employee C`;
- anonymized units, departments, and cost centers;
- job titles;
- gender and age attributes;
- ingestion timestamps.

**Primary transformation:** `dim_employee`

---

### `employee_disability_sample.csv`

Synthetic disability and rehabilitation attributes associated with selected employees.

It includes:

- generic employee identifiers;
- disability type and classification;
- rehabilitation status;
- start and rehabilitation dates;
- ingestion timestamps.

**Primary transformation:** `dim_employee`

---

### `absenteeism_sample.csv`

Synthetic current absenteeism and medical certificate records.

It includes:

- absence identifiers;
- employee registrations;
- ICD codes and generic descriptions;
- start and end dates;
- durations in days or hours;
- anonymized units;
- certificate and leave classifications.

The dataset contains examples of:

- day-based absences;
- hourly absences;
- records with an ICD code;
- records without an ICD code;
- different absence durations and organizational units.

**Primary transformations:** `dim_icd` and `fact_absenteeism`

---

### `medical_leave_details_sample.csv`

Synthetic detailed medical leave records representing the batched API source.

It includes:

- leave identifiers;
- employee registrations;
- ICD codes;
- leave dates;
- anonymized units;
- source-system boolean flags;
- leave types and generic reasons.

**Primary transformations:** `dim_icd` and `fact_absenteeism`

---

### `historical_absenteeism_sample.csv`

Synthetic legacy absenteeism history used to demonstrate integration between current and historical sources.

The file intentionally preserves the legacy column format, including spaces, punctuation, and accented characters:

```text
Matrícula
Situação
Doença
Observação
Inicio Afast.
Fim Afast.
D.TOTAL
H.TOTAL
```

During transformation, these names are normalized safely:

```text
Matrícula       -> matricula
Situação        -> situacao
Doença          -> doenca
Observação      -> observacao
Inicio Afast.   -> inicio_afast
Fim Afast.      -> fim_afast
D.TOTAL         -> d_total
H.TOTAL         -> h_total
```

The dataset contains both:

- employees already present in the current employee source;
- employees available only in the historical source.

This allows the project to demonstrate:

- historical employee recovery;
- historical ICD integration;
- historical absence integration with `unionByName(..., allowMissingColumns=True)`.

**Primary transformations:** `dim_employee`, `dim_icd`, and `fact_absenteeism`

---

### `occupational_accident_sample.csv`

Synthetic occupational accident and CAT-related records.

It includes:

- generic accident identifiers;
- synthetic employee references;
- accident and registration dates;
- leave and fatality flags;
- affected body part;
- causing agent;
- accident cause and location;
- anonymized organizational areas.

The dataset includes examples of accidents:

- with leave;
- without leave;
- with different potential-severity classifications;
- associated with different generic locations and activities.

**Primary transformation:** `fact_occupational_accident`

---

### `organizational_perimeter_sample.csv`

Synthetic mapping between cost centers and organizational perimeters.

Example values include:

```text
CC001 -> Perimeter A
CC002 -> Perimeter A
CC003 -> Perimeter B
```

This dataset enriches employee records without exposing real organizational structures.

**Primary transformation:** `dim_employee`

---

## Synthetic data conventions

The sample files use consistent generic labels across datasets.

### Employees

```text
Employee A
Employee B
Employee C
...
Employee R
```

Employees available only in the historical source use labels such as:

```text
Legacy Employee 10
Legacy Employee 11
Legacy Employee 12
```

### Organizational structure

```text
Unit A
Unit B
Unit C
Unit D
Unit E

Department A
Department B
Department C
Department D
Department E

Perimeter A
Perimeter B
Perimeter C
Perimeter D
```

### Identifiers

Identifiers are artificial and exist only to preserve relationships between sample files.

Examples:

```text
RH0001
EMP0001
ABS00001
LIC00001
CAT00001
SYNTH00000000001
```

They do not represent real employee registrations, tax identifiers, certificates, leave records, or accident reports.

---

## Relationships between sample files

```text
employee_master_sample.csv
        |
        +---- employee_disability_sample.csv
        |
        +---- absenteeism_sample.csv
        |
        +---- medical_leave_details_sample.csv
        |
        +---- occupational_accident_sample.csv
        |
        +---- historical_absenteeism_sample.csv

organizational_perimeter_sample.csv
        |
        +---- employee_master_sample.csv
        +---- historical_absenteeism_sample.csv
```

The relationships are preserved through synthetic employee registrations, cost center identifiers, and analytical business keys.

---

## Data flow

```text
Synthetic source datasets
        |
        v
Ingestion-compatible schemas
        |
        v
PySpark transformations
        |
        +---- dim_employee
        +---- dim_icd
        +---- fact_absenteeism
        +---- fact_occupational_accident
        |
        v
Incremental Delta MERGE
        |
        v
Power BI analytical model
```

---

## Schema documentation

Detailed definitions for source and curated tables are available in:

```text
schemas/data-dictionary.md
```

The data dictionary documents:

- field names and types;
- nullable fields;
- primary and foreign keys;
- historical column normalization;
- dimension and fact relationships;
- partition columns;
- incremental load behavior;
- privacy and publication constraints.

---

## Privacy and security

The following rules apply to this directory:

- only synthetic or fully anonymized datasets may be committed;
- real employee names must not be included;
- real units, departments, perimeters, and cost centers must not be included;
- real tax identifiers and employee registrations must not be included;
- medical notes and free-text observations must remain empty or generic;
- API credentials, report codes, company identifiers, and endpoints must never be stored in CSV files;
- exported notebook results must be reviewed before publication;
- Power BI files and screenshots must be checked for hidden data and metadata.

---

## Usage

The CSV files can be uploaded to a development workspace or read directly with Spark for demonstration purposes.

Example:

```python
sample_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/sample/absenteeism_sample.csv")
)
```

When reproducing the complete pipeline, map the sample datasets to the corresponding generic source-table names defined in:

```text
config/config.example.json
```

---

## Limitations

- The sample volume is intentionally small and does not represent production scale.
- The datasets cover representative scenarios rather than every possible API response.
- Clinical descriptions are generic and included only to demonstrate analytical grouping.
- The historical schema is intentionally preserved to demonstrate normalization and integration.
- The sample files do not contain raw API payloads, proprietary mappings, or company-specific business rules.
- The files are designed for documentation and portfolio demonstration, not clinical, HR, or operational decision-making.

