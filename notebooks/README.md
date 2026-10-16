# 📑 Notebooks

## Overview

This directory contains the Databricks Source notebooks used to implement the **Absenteeism Analytics Pipeline**.

The notebooks are organized by responsibility:

1. source ingestion;
2. analytical transformation;
3. curated Delta loading.

The public version uses generic source names, synthetic datasets, environment-based configuration, and external secret references. It does not contain production credentials, endpoints, company identifiers, internal catalog names, or real employee and health data.

> The notebooks document the pipeline architecture and transformation logic. Production execution requires private environment configuration, governed source access, and a properly configured Databricks workspace.

---

## Directory structure

```text
notebooks/
├── README.md
├── 01_ingestion/
│   ├── 01_employee_master_ingestion.py
│   ├── 02_disability_ingestion.py
│   ├── 03_absenteeism_ingestion.py
│   ├── 04_medical_leave_ingestion.py
│   └── 05_leave_details_ingestion.py
├── 02_transformation/
│   ├── 01_employee_dimension.py
│   ├── 02_icd_dimension.py
│   ├── 03_absenteeism_fact.py
│   └── 04_accident_fact.py
└── 03_load/
    └── 01_curated_delta_load.py
```

---

# Processing layers

## `01_ingestion/`

The ingestion layer extracts source data, applies only the minimum required type handling, validates source contracts, and persists generic Delta source tables.

Main responsibilities:

- retrieve credentials from a secret manager;
- send controlled API requests;
- validate TLS certificates;
- apply timeouts and retry policies;
- divide historical requests into bounded date windows;
- validate required source fields;
- preserve source-oriented schemas;
- write governed Delta source tables;
- avoid logging sensitive request or response content.

### Ingestion notebooks

#### `01_employee_master_ingestion.py`

Extracts current employee master data.

Primary output:

```text
employee_master_source
```

The source supports employee identifiers, synthetic employee labels, job attributes, organizational structures, gender, age, and employee-dimension construction.

---

#### `02_disability_ingestion.py`

Extracts disability and rehabilitation attributes associated with employees.

Primary output:

```text
employee_disability_source
```

The source supports disability descriptions, classifications, rehabilitation status, and employee-dimension enrichment.

---

#### `03_absenteeism_ingestion.py`

Extracts current absenteeism and medical-certificate records.

Primary output:

```text
absenteeism_source
```

The notebook supports bounded historical windows and fields such as:

- absence identifiers;
- employee registrations;
- diagnosis codes;
- start and end dates;
- durations in days or hours;
- certificate and leave classifications.

---

#### `04_medical_leave_ingestion.py`

Extracts the medical-leave source used by the analytical pipeline.

Primary output:

```text
medical_leave_source
```

This source is configured separately because its API contract and reporting behavior may differ from the general absenteeism endpoint.

---

#### `05_leave_details_ingestion.py`

Extracts detailed medical-leave records in controlled historical batches.

Primary output:

```text
medical_leave_details_source
```

This source complements the current absenteeism history with detailed leave dates, classifications, source flags, and one or more diagnosis codes.

---

## Governed source dependencies

Some sources are represented as governed dependencies rather than public extraction notebooks:

```text
historical_absenteeism_source
occupational_accident_source
organizational_perimeter_source
```

These sources are still documented in the configuration and data dictionary because they participate in the analytical model.

The public repository does not expose proprietary extraction logic, internal storage locations, or production source contracts for these dependencies.

---

## `02_transformation/`

The transformation layer standardizes source schemas and builds the dimensions and facts used by the curated model.

Main responsibilities:

- normalize column names to lowercase `snake_case`;
- transliterate accented characters before normalization;
- integrate current and historical records;
- harmonize dates, timestamps, flags, and durations;
- generate deterministic SHA-256 analytical keys;
- resolve dimension relationships;
- deduplicate records;
- validate primary keys and required dates;
- create temporary Spark views for the load layer.

### Column-name normalization

Historical fields may contain accents, spaces, and punctuation.

Examples:

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

The transformation notebooks use Unicode normalization before applying the naming convention so accented fields remain discoverable.

---

### Transformation notebooks

#### `01_employee_dimension.py`

Builds the conformed employee dimension from:

```text
employee_master_source
employee_disability_source
historical_absenteeism_source
organizational_perimeter_source
```

Main operations:

- standardize current employee attributes;
- identify employees available only in the historical source;
- enrich records with disability attributes;
- map organizational perimeter information;
- standardize age groups and gender values;
- create a stable employee key;
- deduplicate employee records.

Historical-only employees are added through a name-aligned union:

```python
current_employee_df.unionByName(
    historical_only_df,
    allowMissingColumns=True
)
```

Output view:

```text
source_dim_employee
```

Curated target:

```text
dim_employee
```

---

#### `02_icd_dimension.py`

Builds the analytical health-classification dimension from current and historical records.

Main inputs:

```text
absenteeism_source
medical_leave_details_source
historical_absenteeism_source
```

Main operations:

- standardize category and subcategory codes;
- consolidate codes from multiple sources;
- include health classifications found only in the historical source;
- create analytical chapters, groups, and subgroups;
- add `NO_ICD` and `UNKNOWN` members;
- generate a stable ICD key;
- deduplicate the final hierarchy.

Historical classifications are integrated with:

```python
.unionByName(
    historical_icd_df,
    allowMissingColumns=True
)
```

Output view:

```text
source_dim_icd
```

Curated target:

```text
dim_icd
```

> The public Power BI mock further replaces real health classifications with synthetic groups. The transformation notebook documents the pipeline pattern and must be executed only with governed or synthetic data.

---

#### `03_absenteeism_fact.py`

Builds the unified absenteeism fact from:

```text
absenteeism_source
medical_leave_details_source
historical_absenteeism_source
source_dim_employee
source_dim_icd
```

Main operations:

- standardize each source independently;
- integrate current, detailed leave, and historical records;
- preserve the historical schema through `unionByName`;
- standardize diagnosis codes;
- parse flexible dates and timestamps;
- convert `HH:mm` durations to decimal hours;
- calculate analytical days and hours;
- derive final end dates when necessary;
- classify leave status;
- resolve employee and ICD keys;
- generate stable absence keys;
- prepare `absence_year` for partitioning;
- deduplicate overlapping events.

The central historical integration is:

```python
absenteeism_union_df = (
    current_absenteeism_df
    .unionByName(
        medical_leave_df,
        allowMissingColumns=True
    )
    .unionByName(
        historical_absenteeism_df,
        allowMissingColumns=True
    )
)
```

Output view:

```text
source_fact_absenteeism
```

Curated target:

```text
fact_absenteeism
```

---

#### `04_accident_fact.py`

Builds the occupational-accident fact from:

```text
occupational_accident_source
source_dim_employee
```

Main operations:

- standardize accident and CAT-related fields;
- normalize dates and boolean flags;
- standardize non-negative leave duration;
- classify events as `FATAL`, `WITH_LEAVE`, or `WITHOUT_LEAVE`;
- resolve employee relationships;
- remove the governed personal identifier from the curated output;
- create a stable accident key;
- prepare `accident_year` for partitioning;
- deduplicate records.

Output view:

```text
source_fact_occupational_accident
```

Curated target:

```text
fact_occupational_accident
```

The historical absenteeism dataset is not unioned into this fact because the legacy dataset represents absence records rather than structured CAT events.

---

## `03_load/`

The load layer validates and persists all curated dimensions and facts.

### `01_curated_delta_load.py`

Inputs:

```text
source_dim_employee
source_dim_icd
source_fact_absenteeism
source_fact_occupational_accident
```

Targets:

```text
dim_employee
dim_icd
fact_absenteeism
fact_occupational_accident
```

Main operations:

- validate required temporary views;
- validate required columns;
- block empty required datasets;
- validate non-null and unique primary keys;
- validate partition columns;
- compare existing target schemas;
- add the deterministic unknown employee member;
- validate referential integrity;
- create missing Delta tables;
- incrementally merge into existing Delta tables;
- verify that current-batch keys were persisted;
- perform post-load quality checks.

### Incremental behavior

```text
Target does not exist -> CREATE
Target already exists -> MERGE
```

The Delta merge performs:

```text
matching key      -> update all columns
new key           -> insert all columns
missing in source -> preserve target record
```

No automatic hard delete is performed.

### Partitioning

```text
fact_absenteeism
└── absence_year

fact_occupational_accident
└── accident_year
```

---

# Execution order

## Source preparation

Run or make available the required source datasets:

```text
1. 01_employee_master_ingestion.py
2. 02_disability_ingestion.py
3. 03_absenteeism_ingestion.py
4. 04_medical_leave_ingestion.py
5. 05_leave_details_ingestion.py
6. historical_absenteeism_source
7. occupational_accident_source
8. organizational_perimeter_source
```

Independent ingestion tasks may run in parallel when the environment supports it.

## Transformation and load

The current transformation notebooks exchange temporary Spark views. Therefore, the following notebooks must execute sequentially in the same Spark session:

```text
1. 01_employee_dimension.py
2. 02_icd_dimension.py
3. 03_absenteeism_fact.py
4. 04_accident_fact.py
5. 01_curated_delta_load.py
```

If these notebooks are configured as independent tasks in separate Spark sessions, temporary views will not be shared. In that architecture, replace the temporary views with persisted intermediate Delta tables or another shared mechanism.

---

# Configuration

All notebooks use:

```text
config/config.example.json
```

The relative path used by the notebooks is:

```python
CONFIG_PATH = "../../config/config.example.json"
```

The configuration defines:

- secret references;
- API behavior;
- extraction enablement;
- historical date windows;
- generic source and curated table names;
- catalog and schema placeholders;
- schema-evolution behavior;
- partition columns;
- data-quality controls;
- privacy controls.

Detailed configuration guidance is available in:

```text
config/README.md
```

---

# Databricks Source format

The Python files use Databricks Source notebook markers:

```python
# Databricks notebook source
```

Markdown cells use:

```python
# MAGIC %md
```

Cell boundaries use:

```python
# COMMAND ----------
```

This format allows the `.py` files to remain readable in GitHub while preserving notebook-cell structure when imported into Databricks.

---

# Data-quality controls

The notebooks apply controls at different stages.

## Ingestion controls

- required source fields;
- empty responses;
- request status;
- timeouts and retries;
- bounded historical windows;
- safe logging.

## Transformation controls

- normalized column-name collisions;
- required business fields;
- empty required inputs;
- invalid dates;
- negative durations;
- duplicate analytical keys;
- deterministic unknown members.

## Load controls

- required views;
- required curated columns;
- non-empty datasets;
- non-null primary keys;
- unique primary keys;
- valid partition values;
- schema compatibility;
- employee referential integrity;
- ICD referential integrity;
- post-merge source-key persistence.

---

# Security and privacy

The notebooks must not expose:

- API keys;
- report codes;
- company identifiers;
- production endpoints;
- internal catalogs or schemas;
- workspace or cluster identifiers;
- real employee names;
- real employee registrations;
- real personal identifiers;
- medical notes;
- raw diagnosis descriptions;
- production organizational structures.

The public repository uses:

- external secret references;
- generic table names;
- synthetic datasets;
- generic organizational labels;
- anonymized Power BI health groups;
- redacted logs;
- notebook files without production outputs.

Before committing notebook changes:

- remove rendered outputs;
- remove debug payloads;
- remove copied API responses;
- search for credentials and internal endpoints;
- verify that real source values were not embedded in comments;
- confirm that configuration references remain generic.

---

# Public sample data

Synthetic source-compatible datasets are available in:

```text
data/sample/
```

These files demonstrate source schemas and relationships for:

```text
employee master
disability
absenteeism
medical leave details
historical absenteeism
occupational accidents
organizational perimeter
```

The Power BI-ready mock model is stored separately under:

```text
power-bi/mock-data/
```

The two locations have different purposes:

```text
data/sample/
└── source-compatible pipeline samples

power-bi/mock-data/
└── semantic-model tables ready for the PBIX
```

---

# Notebook-to-table mapping

| Notebook | Main input | Output |
|---|---|---|
| `01_employee_master_ingestion.py` | Employee API | `employee_master_source` |
| `02_disability_ingestion.py` | Disability API | `employee_disability_source` |
| `03_absenteeism_ingestion.py` | Absenteeism API | `absenteeism_source` |
| `04_medical_leave_ingestion.py` | Medical-leave API | `medical_leave_source` |
| `05_leave_details_ingestion.py` | Leave-details API | `medical_leave_details_source` |
| `01_employee_dimension.py` | Employee, disability, history, perimeter | `source_dim_employee` |
| `02_icd_dimension.py` | Absenteeism, leave details, history | `source_dim_icd` |
| `03_absenteeism_fact.py` | Current, detailed, and historical absences | `source_fact_absenteeism` |
| `04_accident_fact.py` | Accident source and employee dimension | `source_fact_occupational_accident` |
| `01_curated_delta_load.py` | Four temporary curated views | Four curated Delta tables |

---

# Local and portfolio usage

The notebook files are designed primarily for Databricks execution. The public CSV files can be loaded into development tables to demonstrate the transformations, provided the configured source table names and expected schemas are preserved.

The repository does not simulate network API calls inside the notebooks. API execution requires private credentials and network access in the target environment.

The synthetic samples must always be identified as synthetic when used for demonstrations, screenshots, tests, or portfolio materials.

---

# Related documentation

```text
config/README.md
data/README.md
data/schemas/data-dictionary.md
orchestration/README.md
power-bi/README.md
```

---

# Scope limitations

- The public repository does not provision Databricks infrastructure.
- The public repository does not create secret scopes or credentials.
- Some governed sources are represented as dependencies rather than public ingestion notebooks.
- The current transformation and load design relies on a shared Spark session for temporary views.
- Quarantine settings are documented in configuration but may require additional persistence logic.
- The notebooks demonstrate analytical engineering patterns and are not a drop-in production deployment without private environment setup.
