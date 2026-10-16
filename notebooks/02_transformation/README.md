# 🧪 Transformation Notebooks

## Overview

This directory contains the analytical transformation layer of the **Absenteeism Analytics Pipeline**.

The notebooks integrate current and historical sources, normalize heterogeneous schemas, generate stable analytical keys, resolve dimensional relationships, and prepare the temporary Spark views consumed by the curated Delta load.

The public implementation uses generic source names and synthetic examples. Real employee identities, organizational structures, medical notes, source credentials, and proprietary storage locations are not included.

> The transformation notebooks currently exchange temporary Spark views and must execute sequentially in the same Spark session before the curated-load notebook runs.

---

## Directory structure

```text
notebooks/02_transformation/
├── README.md
├── 01_employee_dimension.py
├── 02_icd_dimension.py
├── 03_absenteeism_fact.py
└── 04_accident_fact.py
```

---

## Layer responsibilities

The transformation layer is responsible for:

- reading governed Delta source tables;
- normalizing field names and types;
- transliterating accented column names;
- integrating current and historical datasets;
- resolving employees across different identifiers;
- standardizing dates, timestamps, flags, codes, and durations;
- generating deterministic SHA-256 analytical keys;
- constructing conformed dimensions and facts;
- deduplicating records;
- validating required fields and business keys;
- producing temporary Spark views for the load layer.

The transformation layer does not physically write the final curated tables. Persistence is handled by:

```text
notebooks/03_load/01_curated_delta_load.py
```

---

## Shared configuration

All transformation notebooks load:

```python
CONFIG_PATH = "../../config/config.example.json"
```

The configuration provides:

- generic catalog and schema names;
- source-table names;
- curated-table names;
- privacy controls;
- data-quality behavior;
- schema-evolution settings;
- fact partition columns.

See:

```text
config/README.md
```

---

## Column-name normalization

Historical source columns may contain accents, spaces, punctuation, and inconsistent capitalization.

The notebooks normalize names by:

1. trimming whitespace;
2. applying Unicode `NFKD` normalization;
3. removing diacritical marks safely;
4. replacing punctuation and whitespace with underscores;
5. collapsing repeated underscores;
6. converting names to lowercase.

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

The normalization function also rejects collisions when two original columns would produce the same normalized name.

---

# Notebook catalog

## `01_employee_dimension.py`

### Purpose

Builds the conformed employee dimension by integrating current employee data, historical-only employees, disability attributes, and organizational-perimeter mappings.

### Inputs

```text
employee_master_source
employee_disability_source
historical_absenteeism_source
organizational_perimeter_source
```

### Main operations

- normalize current and historical fields;
- standardize employee identifiers;
- identify historical employees absent from the current source;
- enrich employees with disability attributes;
- enrich cost centers with perimeter information;
- standardize gender values;
- derive age groups;
- assign data origin;
- generate a stable SHA-256 employee key;
- deduplicate employee records.

### Historical integration

Current employees are prioritized. Employees found only in history are appended with:

```python
employee_base_df = current_employee_df.unionByName(
    historical_only_df,
    allowMissingColumns=True
)
```

### Output view

```text
source_dim_employee
```

### Curated target

```text
dim_employee
```

---

## `02_icd_dimension.py`

### Purpose

Builds the analytical health-classification dimension from current absenteeism, detailed medical leave, and historical records.

### Inputs

```text
absenteeism_source
medical_leave_details_source
historical_absenteeism_source
```

### Main operations

- normalize diagnosis-code formats;
- extract the first valid code from multi-code fields;
- consolidate current and historical descriptions;
- derive analytical chapter, group, subgroup, category, and subcategory fields;
- add deterministic `NO_ICD` and `UNKNOWN` members;
- generate a stable SHA-256 ICD key;
- deduplicate the final hierarchy.

### Historical integration

```python
icd_source_df = (
    absenteeism_icd_df
    .unionByName(
        medical_leave_icd_df,
        allowMissingColumns=True
    )
    .unionByName(
        historical_icd_df,
        allowMissingColumns=True
    )
)
```

### Output view

```text
source_dim_icd
```

### Curated target

```text
dim_icd
```

### Publication note

The engineering layer documents the health-classification transformation pattern. The public Power BI model uses separate synthetic groups so the original monitored classifications are not exposed.

---

## `03_absenteeism_fact.py`

### Purpose

Builds the unified absenteeism fact from current certificates, detailed medical leaves, and legacy absence history.

### Inputs

```text
absenteeism_source
medical_leave_details_source
historical_absenteeism_source
source_dim_employee
source_dim_icd
```

### Main operations

- transform each source into a common fact schema;
- integrate the three source families with `unionByName`;
- parse flexible dates and timestamps;
- standardize diagnosis codes;
- convert `HH:mm` values into decimal hours;
- calculate standardized analytical days and hours;
- derive a final end date when necessary;
- classify records as returned or currently on leave;
- resolve employee and ICD foreign keys;
- generate deterministic absence keys;
- derive `absence_year` for partitioning;
- deduplicate overlapping or repeated events.

### Historical integration

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

### Duration convention

```text
1 analytical workday = 8 hours
```

When only hours are available:

```text
total_absence_days = total_absence_hours / 8
```

When only days are available:

```text
total_absence_hours = total_absence_days x 8
```

### Output view

```text
source_fact_absenteeism
```

### Curated target

```text
fact_absenteeism
```

### Partition column

```text
absence_year
```

---

## `04_accident_fact.py`

### Purpose

Builds the occupational-accident fact from governed CAT-related records and the employee dimension.

### Inputs

```text
occupational_accident_source
source_dim_employee
```

### Main operations

- normalize source fields;
- parse accident and registration dates;
- convert string flags to booleans;
- prevent negative leave-day values;
- resolve employee relationships;
- classify accident status;
- remove the governed personal identifier from the curated output;
- generate a deterministic accident key;
- derive `accident_year` for partitioning;
- deduplicate accident records.

### Accident-status values

```text
FATAL
WITH_LEAVE
WITHOUT_LEAVE
```

### Output view

```text
source_fact_occupational_accident
```

### Curated target

```text
fact_occupational_accident
```

### Partition column

```text
accident_year
```

The historical absenteeism dataset is not unioned into this fact because legacy absence records do not represent structured occupational-accident events.

---

# Execution order

Run the transformation notebooks in this order:

```text
1. 01_employee_dimension.py
2. 02_icd_dimension.py
3. 03_absenteeism_fact.py
4. 04_accident_fact.py
```

Then run:

```text
notebooks/03_load/01_curated_delta_load.py
```

The employee and ICD views must exist before the relevant fact transformations run.

---

# Shared-session requirement

The four transformation outputs are temporary Spark views:

```text
source_dim_employee
source_dim_icd
source_fact_absenteeism
source_fact_occupational_accident
```

Temporary views exist only in the Spark session that created them. Therefore, transformation and load must run sequentially in the same session.

For independent Databricks job tasks, replace temporary views with persisted intermediate Delta tables or another shared storage mechanism.

---

# Analytical keys

The notebooks use deterministic SHA-256 hashes to create stable keys.

```text
employee_key
icd_key
absence_key
accident_key
```

Deterministic keys support:

- repeatable processing;
- idempotent Delta `MERGE` operations;
- duplicate detection;
- stable dimension relationships;
- reprocessing without random surrogate-key changes.

The notebooks do not use the hash as a claim of irreversible anonymization. Governed business identifiers must still remain protected.

---

# Unknown members

The model uses controlled special members for incomplete relationships.

## Employee

```text
EMPLOYEE|UNKNOWN
```

The corresponding dimension member is created by the load notebook.

## Health classification

```text
NO_ICD
UNKNOWN
```

These distinguish missing classifications from invalid or unrecognized values.

---

# Data-quality controls

The transformation layer validates:

- source-table availability;
- non-empty required inputs;
- required columns;
- normalized-name collisions;
- valid employee identifiers;
- valid and parseable event dates;
- non-negative durations;
- unique analytical keys;
- valid partition years;
- deduplicated output records.

Referential integrity and persisted target keys are validated again in the load layer.

---

# Source-to-output mapping

| Output view | Main source dependencies | Curated table |
|---|---|---|
| `source_dim_employee` | Employee master, disability, historical absences, perimeter mapping | `dim_employee` |
| `source_dim_icd` | Current absences, leave details, historical absences | `dim_icd` |
| `source_fact_absenteeism` | Current absences, leave details, historical absences, employee and ICD views | `fact_absenteeism` |
| `source_fact_occupational_accident` | Accident source and employee view | `fact_occupational_accident` |

---

# Privacy and publication checklist

Before committing a transformation notebook, confirm that:

- no real employee or personal identifier is hardcoded;
- no real employee name is included in examples;
- no production organizational unit or perimeter is included;
- no credentials, endpoints, or internal storage names are included;
- sensitive source fields are removed from curated outputs where required;
- comments do not reveal original monitored health classifications;
- debug displays and notebook outputs have been removed;
- public examples use synthetic values;
- the public Power BI health-group mapping is not embedded in transformation code;
- configuration references remain generic.

---

# Related documentation

```text
notebooks/README.md
notebooks/01_ingestion/README.md
notebooks/03_load/README.md
data/schemas/data-dictionary.md
orchestration/README.md
```

---

# Scope limitations

- The transformation notebooks require governed source tables or schema-compatible synthetic tables.
- The current implementation depends on a shared Spark session for temporary views.
- The public notebooks do not provide a complete clinical terminology service.
- Health hierarchies are analytical groupings and should not be interpreted as clinical guidance.
- Production performance tuning, cluster sizing, and deployment automation remain environment-specific.
