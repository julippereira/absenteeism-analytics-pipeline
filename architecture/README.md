# 🏗️ Solution Architecture

## Overview

This directory documents the end-to-end architecture of the **Absenteeism Analytics Pipeline**, from governed source systems to the public Power BI analytical experience.

The solution combines controlled API ingestion, historical-source integration, PySpark transformations, Delta Lake persistence, Databricks orchestration, and Power BI semantic modeling.

The public architecture intentionally uses generic component names. Production endpoints, workspace identifiers, catalogs, schemas, credentials, organizational names, and real employee or health data are not represented.

---

## Directory structure

```text
architecture/
├── README.md
├── solution-architecture.png
└── data-flow.png
```

| File | Purpose |
|---|---|
| `solution-architecture.png` | High-level view of platforms, layers, and system boundaries. |
| `data-flow.png` | Detailed movement of data from extraction through analytical consumption. |

The Power BI semantic model is documented separately in:

```text
power-bi/data-model.md
screenshots/13-data-model.png
```

---

# Architecture principles

The solution follows these principles:

- separate source ingestion from analytical transformation;
- keep credentials outside notebook code;
- preserve source-oriented schemas in the ingestion layer;
- integrate current and historical data in the transformation layer;
- generate deterministic analytical keys;
- validate data before curated persistence;
- use Delta `MERGE` for repeatable incremental loads;
- model dimensions and facts for analytical consumption;
- maintain separate pipeline samples and Power BI mock datasets;
- publish only synthetic or fully anonymized portfolio assets;
- document current implementation constraints rather than hiding them.

---

# High-level architecture

```text
Governed APIs and historical sources
                |
                v
Databricks ingestion notebooks
                |
                v
Delta source tables
                |
                v
PySpark transformation notebooks
                |
                v
Temporary curated views
                |
                v
Curated Delta MERGE
                |
                v
Dimensions and fact tables
                |
                v
Power BI semantic model
                |
                v
Interactive analytical dashboard
```

---

# Architecture layers

## 1. Source layer

The solution receives data from controlled operational sources.

### API-based sources

```text
Employee Master API
Employee Disability API
Absenteeism API
Medical Leave API
Medical Leave Details API
```

### Governed supporting sources

```text
Historical Absenteeism Source
Occupational Accident Source
Organizational Perimeter Source
```

The public repository does not expose production endpoints, authentication methods beyond generic secret references, proprietary report codes, or internal storage paths.

---

## 2. Configuration and security layer

Runtime behavior is controlled through:

```text
config/config.example.json
```

The configuration defines:

- API request behavior;
- extraction enablement;
- historical date windows;
- generic source and curated table names;
- Delta storage settings;
- partition columns;
- data-quality controls;
- privacy controls;
- secret-key references.

Protected values are resolved through a Databricks secret scope.

```text
Notebook code
    -> secret-key reference
    -> Databricks Secret Scope
    -> protected runtime value
```

Required public safeguards include:

```text
allow_plaintext_secrets = false
publish_real_data = false
use_synthetic_data_in_repository = true
verify_ssl = true
redact_sensitive_logs = true
```

---

## 3. Ingestion layer

The ingestion layer is implemented in:

```text
notebooks/01_ingestion/
```

Primary responsibilities:

- retrieve secrets at runtime;
- call controlled APIs;
- validate TLS certificates;
- enforce timeout and retry policies;
- split historical requests into bounded windows;
- validate mandatory source fields;
- perform minimal source-level casting;
- add ingestion timestamps;
- persist generic Delta source tables.

### Source outputs

```text
employee_master_source
employee_disability_source
absenteeism_source
medical_leave_source
medical_leave_details_source
historical_absenteeism_source
occupational_accident_source
organizational_perimeter_source
```

The source layer preserves source contracts and avoids premature dimensional modeling.

---

## 4. Transformation layer

The transformation layer is implemented in:

```text
notebooks/02_transformation/
```

Primary responsibilities:

- normalize heterogeneous field names;
- transliterate accented historical columns;
- standardize data types;
- integrate current and historical records;
- harmonize dates, flags, codes, and durations;
- create deterministic SHA-256 analytical keys;
- resolve dimensional relationships;
- deduplicate records;
- produce conformed dimensions and fact tables.

### Outputs

```text
source_dim_employee
source_dim_icd
source_fact_absenteeism
source_fact_occupational_accident
```

These outputs are temporary Spark views consumed by the curated-load notebook.

---

## 5. Curated Delta layer

The curated persistence layer is implemented in:

```text
notebooks/03_load/01_curated_delta_load.py
```

### Curated tables

```text
dim_employee
dim_icd
fact_absenteeism
fact_occupational_accident
```

### Load behavior

```text
Target does not exist -> CREATE
Target already exists -> MERGE
```

The Delta merge:

- updates existing analytical keys;
- inserts new analytical keys;
- preserves target records absent from the current batch;
- performs no automatic hard delete.

### Partitioning

```text
fact_absenteeism             -> absence_year
fact_occupational_accident   -> accident_year
```

---

## 6. Orchestration layer

The workflow is represented as a multi-stage Databricks job and documented in:

```text
orchestration/README.md
orchestration/job-workflow.png
```

The logical sequence is:

```text
1. Source ingestion and source availability
2. Employee dimension
3. ICD dimension
4. Absenteeism fact
5. Occupational accident fact
6. Curated Delta MERGE
7. Power BI refresh or analytical consumption
```

Independent ingestion tasks may execute in parallel when source limits and runtime resources allow it.

### Shared-session constraint

The transformation notebooks currently exchange temporary Spark views. Therefore, transformation and load must run sequentially in the same Spark session.

```text
01_employee_dimension.py
02_icd_dimension.py
03_absenteeism_fact.py
04_accident_fact.py
01_curated_delta_load.py
```

If these steps are separated into independent job tasks, the temporary views must be replaced by persisted intermediate Delta tables or another shared storage mechanism.

---

## 7. Semantic and reporting layer

The curated analytical model is consumed by Power BI.

### Pipeline tables

```text
dim_employee
dim_icd
fact_absenteeism
fact_occupational_accident
```

### Power BI tables

```text
d_colab
d_cid
f_abs
f_cat
d_calendario
Medidas
```

### Mapping

| Pipeline table | Power BI table |
|---|---|
| `dim_employee` | `d_colab` |
| `dim_icd` | `d_cid` |
| `fact_absenteeism` | `f_abs` |
| `fact_occupational_accident` | `f_cat` |
| Semantic calendar | `d_calendario` |
| DAX container | `Medidas` |

The PBIX retains its original physical table names to avoid rebuilding visuals, relationships, and measures.

---

# End-to-end data flow

```text
API and governed sources
        |
        v
Secure extraction
        |
        v
Source Delta tables
        |
        +-- current employee records
        +-- disability attributes
        +-- current absenteeism
        +-- detailed medical leaves
        +-- historical absenteeism
        +-- occupational accidents
        +-- organizational mapping
        |
        v
PySpark standardization and integration
        |
        +-- employee dimension
        +-- health-classification dimension
        +-- absenteeism fact
        +-- occupational-accident fact
        |
        v
Data-quality and referential-integrity checks
        |
        v
Incremental Delta MERGE
        |
        v
Curated dimensions and facts
        |
        v
Power BI semantic model and DAX measures
        |
        v
Dashboard pages, filters, drill-through, and detail analysis
```

---

# Historical integration

The legacy absenteeism source participates in three transformations.

```text
historical_absenteeism_source
        |
        +-- historical-only employees -> dim_employee
        |
        +-- historical health codes -> dim_icd
        |
        +-- historical absence events -> fact_absenteeism
```

Historical records are aligned with current schemas through:

```python
unionByName(..., allowMissingColumns=True)
```

The occupational-accident fact does not receive historical absenteeism records because those records do not represent structured CAT events.

---

# Analytical keys and relationships

Deterministic SHA-256 keys are generated for:

```text
employee_key
icd_key
absence_key
accident_key
```

Primary curated relationships:

```text
fact_absenteeism[employee_key]
    -> dim_employee[employee_key]

fact_absenteeism[icd_key]
    -> dim_icd[icd_key]

fact_occupational_accident[employee_key]
    -> dim_employee[employee_key]
```

A deterministic unknown employee member prevents unresolved fact records from referencing a missing dimension row.

---

# Data-quality architecture

Quality controls operate at multiple layers.

## Ingestion

- configuration validation;
- secret availability;
- accepted HTTP status;
- retries and timeouts;
- mandatory source fields;
- empty-response handling;
- protected logging.

## Transformation

- required source tables;
- normalized-name collisions;
- valid identifiers and dates;
- non-negative durations;
- deduplicated output records;
- deterministic keys;
- valid partition years.

## Curated load

- required temporary views;
- required columns;
- non-empty datasets;
- non-null and unique primary keys;
- schema compatibility;
- partition integrity;
- employee referential integrity;
- ICD referential integrity;
- current-batch key persistence after `MERGE`.

---

# Synthetic data architecture

The repository separates two synthetic data purposes.

## Pipeline samples

```text
data/sample/
```

These files reproduce schemas expected by the engineering pipeline.

## Power BI mock data

```text
power-bi/mock-data/
```

These files reproduce the existing PBIX semantic-model tables and column names.

```text
data/sample/
    -> engineering demonstration

power-bi/mock-data/
    -> public PBIX refresh
```

The separation prevents semantic-model mock tables from being misrepresented as raw API-source samples.

---

# Health-data anonymization

The public Power BI model replaces monitored clinical classifications with synthetic groups.

```text
Health Group A
Health Group B
Health Group C
Health Group D
Other Health Groups
Not Reported
```

The public model also replaces:

- employee names;
- registrations;
- personal identifiers;
- organizational units;
- areas and perimeters;
- job titles;
- medical conditions;
- accident descriptions;
- operational values.

The correspondence between synthetic health groups and original monitored classifications must not be included in the repository, PBIX, DAX, screenshots, metadata, hidden pages, tooltips, or documentation.

---

# Architecture diagrams

## Solution architecture

![Solution architecture](solution-architecture.png)

The solution architecture diagram presents the main platforms, processing layers, security controls, orchestration components, and analytical consumption flow.

The diagram should also represent:

- configuration and secret management;
- orchestration;
- data-quality controls;
- synthetic publication boundary.

## Data flow

![End-to-end data flow](data-flow.png)


The data-flow diagram details the lineage between governed sources, Delta source tables, PySpark transformations, curated dimensions and facts, and the Power BI semantic model.

---

# Current design constraints

## Temporary views

The current transformation-to-load handoff uses Spark temporary views, requiring a shared session.

## Power BI model

The public PBIX preserves the existing semantic-model structure, including documented non-ideal cardinalities, to avoid unnecessary dashboard reconstruction.

## Governed sources

Some source dependencies are represented without public extraction notebooks because their production extraction logic is proprietary or environment-specific.

## Deployment

The repository does not provision:

- Databricks workspaces;
- clusters or serverless compute;
- secret scopes;
- catalogs and permissions;
- Power BI workspaces;
- gateways;
- refresh schedules;
- deployment pipelines.

---

# Security and privacy boundary

The public repository may contain:

- generic notebook logic;
- secret-key references;
- synthetic CSV files;
- anonymized screenshots;
- public configuration templates;
- architecture diagrams;
- documentation.

The public repository must not contain:

- real credentials;
- internal endpoints;
- production catalog or schema names;
- workspace, cluster, tenant, or gateway identifiers;
- real employee or medical data;
- original monitored health-group mappings;
- production PBIX data;
- exported protected API responses;
- internal operational logs.

---

# Related documentation

```text
config/README.md
notebooks/README.md
data/README.md
data/schemas/data-dictionary.md
orchestration/README.md
power-bi/README.md
power-bi/data-model.md
screenshots/README.md
```

---

# Scope limitations

- The diagrams represent logical architecture rather than deployable infrastructure definitions.
- Production performance, scaling, access control, and networking are environment-specific.
- The public project demonstrates architecture and engineering patterns using synthetic data.
- The Power BI report is a portfolio artifact and not a production occupational-health decision system.
