# Pipeline Orchestration

## Overview

The **Absenteeism Analytics Pipeline** is orchestrated as a multi-stage Databricks job. The solution was originally implemented with **Databricks Workflows**, currently represented by **Lakeflow Jobs** terminology.

The workflow coordinates:

- source ingestion;
- historical data integration;
- dimensional transformations;
- fact-table construction;
- incremental Delta loading;
- analytical availability for Power BI.

The public repository documents the orchestration logic without exposing production schedules, workspace identifiers, cluster configuration, notification recipients, internal notebook paths, or credentials.

---

## Workflow diagram

![Pipeline job workflow](job-workflow.png)

The diagram is an illustrative representation of the task dependencies. It does not reproduce the production Databricks workspace or disclose internal configuration.

---

## Workflow stages

### 1. Source ingestion

The ingestion stage extracts and prepares the source datasets used by the analytical model.

```text
Employee Master Ingestion
Disability Data Ingestion
Absenteeism Ingestion
Medical Leave Details Ingestion
Occupational Accident Source
Historical Absenteeism Source
Organizational Perimeter Source
```

The API-based ingestion tasks apply:

- secure credential retrieval;
- TLS verification;
- timeout and retry policies;
- controlled request intervals;
- historical extraction windows;
- source-schema validation;
- minimum source-layer type conversion;
- Delta source-table persistence.

Independent ingestion tasks may run in parallel when their dependencies and runtime resources allow it.

---

### 2. Employee dimension

The employee dimension depends on:

```text
Employee Master Source
Employee Disability Source
Historical Absenteeism Source
Organizational Perimeter Source
```

This stage:

- standardizes current employee records;
- recovers employees available only in the historical source;
- enriches employees with organizational perimeter information;
- incorporates disability attributes;
- standardizes gender and age groups;
- creates stable analytical employee keys;
- produces the temporary view `source_dim_employee`.

---

### 3. ICD dimension

The ICD dimension depends on:

```text
Absenteeism Source
Medical Leave Details Source
Historical Absenteeism Source
```

This stage:

- consolidates diagnosis codes from multiple sources;
- normalizes ICD category and subcategory formats;
- creates analytical chapters, groups, and subgroups;
- adds `NO_ICD` and `UNKNOWN` members;
- creates stable analytical ICD keys;
- produces the temporary view `source_dim_icd`.

---

### 4. Absenteeism fact

The absenteeism fact depends on:

```text
Absenteeism Source
Medical Leave Details Source
Historical Absenteeism Source
Employee Dimension
ICD Dimension
```

This stage:

- integrates current absenteeism, detailed medical leave, and legacy records;
- preserves historical integration with `unionByName(..., allowMissingColumns=True)`;
- standardizes dates, durations, identifiers, and ICD codes;
- converts durations between analytical days and hours;
- resolves employee and ICD relationships;
- calculates the final absence date and current status;
- prepares the partition year;
- produces the temporary view `source_fact_absenteeism`.

---

### 5. Occupational accident fact

The occupational accident fact depends on:

```text
Occupational Accident Source
Employee Dimension
```

This stage:

- standardizes accident and CAT-related records;
- resolves employee relationships;
- converts dates and boolean indicators;
- classifies accident status;
- removes the governed personal identifier from the curated output;
- prepares the partition year;
- produces the temporary view `source_fact_occupational_accident`.

---

### 6. Curated Delta load

The curated load starts only after all required dimensions and facts have been generated successfully.

The load stage persists:

```text
dim_employee
dim_icd
fact_absenteeism
fact_occupational_accident
```

It uses incremental Delta `MERGE` to:

- update records whose analytical keys already exist;
- insert records with new analytical keys;
- preserve records absent from the current batch;
- avoid duplicate primary keys;
- validate source keys after persistence;
- maintain relationships between facts and dimensions.

The fact tables are partitioned by:

```text
fact_absenteeism             -> absence_year
fact_occupational_accident   -> accident_year
```

---

## Task dependencies

```text
Employee Master Ingestion ---------+
                                    |
Disability Data Ingestion ----------+---> Employee Dimension --------+
                                    |                                |
Historical Absenteeism Source ------+                                |
                                    |                                |
Organizational Perimeter Source ----+                                |
                                                                     |
Absenteeism Ingestion --------------+---> ICD Dimension -------------+---> Absenteeism Fact ---+
                                    |                                |                         |
Medical Leave Details Ingestion ----+--------------------------------+                         |
                                    |                                                          |
Historical Absenteeism Source ------+----------------------------------------------------------+
                                                                                               |
Occupational Accident Source -------+---> Occupational Accident Fact --------------------------+
                                    |                                                          |
Employee Dimension -----------------+                                                          |
                                                                                               v
                                                                                  Curated Delta MERGE
                                                                                               |
                                                                                               v
                                                                                     Power BI Analytics
```

---

## Logical execution order

```text
1. Source ingestion and source availability
2. Employee dimension
3. ICD dimension
4. Absenteeism fact
5. Occupational accident fact
6. Curated Delta MERGE
7. Power BI refresh or analytical consumption
```

The employee and ICD dimensions can be processed independently after their respective source dependencies are satisfied.

The absenteeism and occupational accident facts start only after their required dimensions are available.

---

## Current implementation constraint

The transformation notebooks currently create temporary Spark views:

```text
source_dim_employee
source_dim_icd
source_fact_absenteeism
source_fact_occupational_accident
```

Temporary views are available only within the Spark session in which they were created. Therefore, the public implementation should execute the transformation and load notebooks sequentially in the same session.

A technically consistent job design is:

```text
Source Ingestion Tasks
        |
        v
Dimensional Transformation and Curated Load
        |
        +-- 01_employee_dimension.py
        +-- 02_icd_dimension.py
        +-- 03_absenteeism_fact.py
        +-- 04_accident_fact.py
        +-- 01_curated_delta_load.py
        |
        v
Power BI Refresh
```

If each transformation is configured as an independent job task using separate Spark sessions, the temporary views must be replaced by persisted intermediate Delta tables or another shared storage mechanism.

---

## Recommended task configuration

### Ingestion tasks

Each ingestion notebook can be configured as an independent task.

```text
Task type: Notebook or Python script
Run condition: All upstream tasks succeeded
Retries: Configurable for transient failures
Timeout: Configurable by source
```

Suggested task names:

```text
ingest_employee_master
ingest_employee_disability
ingest_absenteeism
ingest_medical_leave_details
prepare_historical_source
prepare_accident_source
prepare_organizational_perimeter
```

### Transformation and load task

Because the current notebooks exchange temporary views, they should execute as one sequential task or through a controller notebook.

Suggested task name:

```text
transform_and_load_curated_model
```

Suggested internal sequence:

```text
01_employee_dimension.py
02_icd_dimension.py
03_absenteeism_fact.py
04_accident_fact.py
01_curated_delta_load.py
```

### Analytical refresh task

A downstream refresh may be triggered only after the curated load succeeds.

Suggested task name:

```text
refresh_analytical_model
```

The production refresh mechanism is intentionally omitted from the public repository.

---

## Failure behavior

Downstream tasks should use an all-required-upstream-tasks-succeeded condition.

If an ingestion or transformation task fails:

- dependent tasks do not run;
- the curated load is not executed;
- existing curated Delta tables remain available;
- the analytical model is not refreshed with incomplete data;
- the failed task can be retried after correction.

The load notebook also blocks persistence when it detects:

- an empty required source;
- missing required columns;
- null primary keys;
- duplicate primary keys;
- invalid partition values;
- incompatible schema changes;
- orphan employee keys;
- orphan ICD keys;
- unsupported non-Delta target tables.

---

## Scheduling

The production schedule is intentionally not included in the public repository.

The workflow may be triggered by:

- a scheduled execution;
- a manual run;
- an external orchestration process;
- a programmatic job trigger.

The extraction period is controlled through configuration rather than credentials embedded in notebook code.

---

## Runtime configuration

Public runtime behavior is defined in:

```text
config/config.example.json
```

Examples of configurable values include:

```text
historical_start_date
end_date_rule
max_window_days
timeout_seconds
max_retries
request_interval_seconds
target catalog
target schema
source table names
curated table names
partition columns
schema-evolution behavior
```

Credentials, company identifiers, report codes, and API endpoints are resolved from a secret manager and are not stored as visible notebook parameters.

---

## Monitoring

Recommended monitoring points include:

- overall job status;
- individual task status;
- task duration;
- number of extraction windows;
- records received by API batch;
- API retry events;
- empty source responses;
- missing source columns;
- duplicate business keys;
- invalid dates and durations;
- Delta `MERGE` execution status;
- source and target record counts;
- referential-integrity failures;
- downstream analytical refresh status.

Logs must not expose:

- credentials;
- request payloads containing secrets;
- API URLs with parameters;
- personal identifiers;
- employee names;
- diagnosis descriptions;
- medical notes.

---

## Security and privacy

The public orchestration representation does not include:

- workspace identifiers;
- cluster identifiers;
- production schedules;
- internal notebook paths;
- user or service-principal names;
- notification recipients;
- secret-scope values used in production;
- production catalog or schema names;
- internal API endpoints;
- company identifiers;
- real employee or medical data.

The `job-workflow.png` diagram must use only the generic task and dataset names documented in this repository.

---

## Repository representation

The orchestration artifacts are documentation assets, not deployable production configuration.

```text
orchestration/
├── README.md
└── job-workflow.png
```

- `README.md` explains stages, dependencies, execution behavior, monitoring, and security.
- `job-workflow.png` provides a clean DAG-style representation of the workflow.

No exported workspace configuration or production job JSON is included.

---

## Terminology

The solution was originally implemented using **Databricks Workflows**. Current Databricks documentation refers to the orchestration capability as **Lakeflow Jobs**.
