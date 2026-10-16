# 📤 Curated Load Notebook

## Overview

This directory contains the curated persistence layer of the **Absenteeism Analytics Pipeline**.

The load notebook validates the transformed dimensions and facts, creates missing Delta tables, and performs incremental Delta `MERGE` operations for existing targets.

The load process is intentionally defensive: all required transformed datasets are validated before any curated table is modified.

> The notebook expects transformation views created in the same Spark session. It cannot consume session-scoped temporary views produced by unrelated tasks running in different sessions.

---

## Directory structure

```text
notebooks/03_load/
├── README.md
└── 01_curated_delta_load.py
```

---

## Notebook

## `01_curated_delta_load.py`

### Purpose

Persists the public analytical model as curated Delta tables.

### Input views

```text
source_dim_employee
source_dim_icd
source_fact_absenteeism
source_fact_occupational_accident
```

### Target tables

```text
dim_employee
dim_icd
fact_absenteeism
fact_occupational_accident
```

### Primary keys

```text
dim_employee                 -> employee_key
dim_icd                      -> icd_key
fact_absenteeism             -> absence_key
fact_occupational_accident   -> accident_key
```

### Partition columns

```text
fact_absenteeism             -> absence_year
fact_occupational_accident   -> accident_year
```

The dimensions are not partitioned.

---

# Configuration

The notebook loads:

```python
CONFIG_PATH = "../../config/config.example.json"
```

Relevant sections include:

```text
security
privacy
storage
tables.curated
data_quality
```

The notebook requires:

```text
storage.format = delta
allow_plaintext_secrets = false
publish_real_data = false
```

Generic catalog, schema, table, partition, and schema-evolution settings are resolved from configuration rather than hardcoded production values.

See:

```text
config/README.md
```

---

# Load strategy

The notebook supports two target states.

## Initial load

When the target table does not exist:

```text
CREATE
```

The validated DataFrame is written as a new Delta table. Fact partitioning is established during this initial creation.

## Incremental load

When the target already exists:

```text
MERGE
```

The merge condition matches the configured analytical primary key.

Conceptual behavior:

```text
matching key      -> update all columns
new key           -> insert all columns
missing in source -> preserve target record
```

No automatic hard delete is performed.

---

# Delta MERGE pattern

The notebook uses the Delta table API conceptually as follows:

```python
(
    delta_target.alias("target")
    .merge(
        source_dataframe.alias("source"),
        "target.primary_key = source.primary_key"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

The actual primary key is selected from the table specification.

This pattern supports:

- repeatable batch processing;
- updates to previously loaded records;
- insertion of new records;
- preservation of target history absent from the current batch;
- avoidance of repeated full-table replacement.

---

# Load order

The notebook persists tables in this order:

```text
1. dim_employee
2. dim_icd
3. fact_absenteeism
4. fact_occupational_accident
```

Dimensions are loaded before facts so post-load dimensional relationships can be evaluated consistently.

---

# Unknown employee member

Fact transformations may assign a deterministic unknown employee key when an employee cannot be resolved.

The load notebook ensures that the corresponding dimension member exists:

```text
employee_name = UNKNOWN EMPLOYEE
data_origin   = SYSTEM_MEMBER
```

The key is derived deterministically from:

```text
EMPLOYEE|UNKNOWN
```

This prevents facts from referencing an absent dimension member.

---

# Pre-load validation

Before writing any target, the notebook validates all transformed datasets.

## Required views

Each expected temporary view must exist in the current Spark session.

## Required columns

Every dataset must contain the columns defined by its load specification.

## Non-empty datasets

Required dimensions and facts cannot be empty. This prevents an invalid batch from replacing or creating an unusable curated table.

## Primary keys

Each configured primary key must be:

```text
non-null
unique within the source batch
```

## Partition columns

Configured partition columns must:

```text
exist in the source schema
contain no null values
```

## Existing schema compatibility

If a target exists and schema overwrite is disabled, the notebook compares:

- missing source columns;
- unexpected source columns;
- incompatible data types.

Incompatible changes block the load.

---

# Referential-integrity validation

The notebook validates transformed relationships before persistence.

## Absenteeism to employee

```text
source_fact_absenteeism[employee_key]
    -> source_dim_employee[employee_key]
```

## Occupational accident to employee

```text
source_fact_occupational_accident[employee_key]
    -> source_dim_employee[employee_key]
```

## Absenteeism to ICD

```text
source_fact_absenteeism[icd_key]
    -> source_dim_icd[icd_key]
```

Orphan keys block the load.

---

# Post-load validation

After each `CREATE` or `MERGE`, the notebook checks that all keys from the current source batch exist in the target table.

It then repeats essential curated checks:

- target is not empty;
- primary key is not null;
- primary key remains unique;
- partition columns contain no null values.

The total target row count is not expected to equal the current source-batch count because incremental targets may contain preserved records from earlier batches.

---

# Schema evolution

When configuration allows compatible schema evolution:

```text
allow_schema_evolution = true
```

The notebook enables Delta automatic schema merge for incremental operations.

When schema overwrite remains disabled:

```text
allow_schema_overwrite = false
```

incompatible replacement of the existing target schema is blocked.

Schema evolution is not a substitute for reviewing semantic-model and downstream compatibility.

---

# Execution requirements

Run the transformation notebooks first, in the same Spark session:

```text
1. notebooks/02_transformation/01_employee_dimension.py
2. notebooks/02_transformation/02_icd_dimension.py
3. notebooks/02_transformation/03_absenteeism_fact.py
4. notebooks/02_transformation/04_accident_fact.py
5. notebooks/03_load/01_curated_delta_load.py
```

The target catalog must already exist. The notebook may create the configured schema if permissions allow, but it intentionally does not attempt privileged catalog creation.

---

# Shared-session constraint

The notebook reads session-scoped views with:

```text
spark.table(<temporary_view>)
```

If each transformation is configured as an independent task with a separate Spark session, the views will not be available.

For independently scheduled tasks, use persisted intermediate Delta tables or another shared persistence layer.

---

# Failure behavior

The notebook stops before or during persistence when it detects:

- a missing required view;
- an empty required dataset;
- missing required columns;
- null primary keys;
- duplicate primary keys;
- null partition values;
- an incompatible existing schema;
- an existing non-Delta target;
- orphan employee keys;
- orphan ICD keys;
- failure to persist a current-batch key.

Existing successfully loaded Delta tables remain available when validation fails before target modification.

---

# Load summary

A successful run reports, for each dataset:

```text
action
source record count
target record count
target table
partition columns
```

The action is:

```text
CREATE
```

or:

```text
MERGE
```

Logs must remain free of personal data, credentials, medical descriptions, and production connection values.

---

# Table specification summary

| Dataset | Source view | Target table | Primary key | Partition |
|---|---|---|---|---|
| Employee dimension | `source_dim_employee` | `dim_employee` | `employee_key` | None |
| ICD dimension | `source_dim_icd` | `dim_icd` | `icd_key` | None |
| Absenteeism fact | `source_fact_absenteeism` | `fact_absenteeism` | `absence_key` | `absence_year` |
| Accident fact | `source_fact_occupational_accident` | `fact_occupational_accident` | `accident_key` | `accident_year` |

---

# Privacy and publication checklist

Before committing changes, confirm that:

- no internal catalog or schema name is hardcoded;
- no production table name is exposed when proprietary;
- no credential or secret value is present;
- no real employee or medical data appears in examples or logs;
- plaintext secrets remain disabled;
- real-data publication remains disabled;
- only generic target names are documented;
- no original monitored health-group mapping is embedded;
- notebook outputs and operational logs have been removed;
- merge conditions use stable analytical keys;
- no hard-delete behavior was added unintentionally.

---

# Related documentation

```text
config/README.md
notebooks/README.md
notebooks/02_transformation/README.md
data/schemas/data-dictionary.md
orchestration/README.md
```

---

# Scope limitations

- The notebook requires Delta Lake support.
- The target catalog and required permissions must be provisioned separately.
- The current architecture assumes transformation and load share one Spark session.
- No automatic target deletion is implemented for records absent from the source batch.
- Quarantine-table persistence is outside the current load notebook scope.
- Deployment automation, cluster configuration, and operational alerting remain environment-specific.
