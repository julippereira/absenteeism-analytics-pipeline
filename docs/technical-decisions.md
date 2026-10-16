# ⚖️ Technical Decisions

## Overview

This document records the main design choices used in the **Absenteeism Analytics Pipeline**, including the motivation, trade-offs, and current implementation constraints.

---

## 1. Separate ingestion, transformation, and load layers

### Decision

Organize notebooks into:

```text
01_ingestion
02_transformation
03_load
```

### Rationale

This separates source connectivity from analytical rules and persistence behavior. Each layer has a clear responsibility and can be documented, tested, and reviewed independently.

### Trade-off

The repository contains more files, but the structure is easier to maintain than one large notebook containing extraction, modeling, and loading logic.

---

## 2. External configuration

### Decision

Store public runtime settings in:

```text
config/config.example.json
```

### Rationale

Configuration changes do not require notebook-code changes. The file centralizes generic table names, extraction behavior, storage settings, quality rules, and privacy controls.

### Trade-off

Private environments require an additional configuration-management process.

---

## 3. Secret references instead of plaintext credentials

### Decision

Resolve protected values from a Databricks secret scope.

### Rationale

API keys, endpoints, identifiers, and report codes must not be embedded in notebooks or committed to version control.

### Required settings

```text
allow_plaintext_secrets = false
redact_sensitive_logs = true
log_response_body = false
verify_ssl = true
```

---

## 4. Delta Lake for source and curated persistence

### Decision

Use Delta tables for governed persistence.

### Rationale

Delta supports schema enforcement, ACID transactions, partitioning, reliable updates, and incremental `MERGE` operations.

### Trade-off

Execution requires an environment with Delta Lake support.

---

## 5. Bounded historical extraction windows

### Decision

Split date-based API extractions into windows of no more than 30 days.

### Rationale

Smaller windows reduce response size, isolate failures, support targeted retries, and reduce pressure on source APIs.

### Trade-off

Historical runs generate more requests and require orchestration and monitoring.

---

## 6. Minimal transformation in ingestion

### Decision

Keep ingestion schemas source-oriented and defer analytical rules to the transformation layer.

### Rationale

This preserves traceability and avoids mixing extraction logic with business modeling.

### Allowed ingestion work

```text
JSON parsing
Expected-field selection
Essential type conversion
Ingestion metadata
Source-contract validation
```

---

## 7. Unicode-aware column normalization

### Decision

Normalize historical field names using Unicode `NFKD` handling before applying lowercase `snake_case` conventions.

### Rationale

Legacy fields contain accents, spaces, and punctuation. Simple character replacement could cause missing fields or inconsistent names.

Example:

```text
Matrícula -> matricula
Doença    -> doenca
D.TOTAL   -> d_total
```

---

## 8. Historical integration with `unionByName`

### Decision

Integrate current and historical records using:

```python
unionByName(..., allowMissingColumns=True)
```

### Rationale

Current, detailed, and legacy sources do not always contain identical columns. Name-aligned unions preserve meaning and support gradual schema harmonization.

### Trade-off

Missing fields require controlled defaults and downstream validation.

---

## 9. Deterministic SHA-256 analytical keys

### Decision

Generate stable keys for employees, classifications, absences, and accidents.

```text
employee_key
icd_key
absence_key
accident_key
```

### Rationale

Deterministic keys support repeatable processing, duplicate detection, stable relationships, and idempotent Delta merges.

### Important limitation

Hashing is not treated as sufficient anonymization. Protected business identifiers remain governed even when used to derive a hash.

---

## 10. Standardized duration convention

### Decision

Use an analytical conversion of:

```text
1 workday = 8 hours
```

### Rationale

The sources contain both day-based and hourly events. Standardized measures allow consistent comparison.

### Trade-off

The conversion is an analytical convention, not a universal payroll or legal rule.

---

## 11. Controlled unknown members

### Decision

Use deterministic special members for unresolved relationships.

```text
EMPLOYEE|UNKNOWN
NO_ICD
UNKNOWN
```

### Rationale

Facts should not reference absent dimension rows. Special members preserve referential integrity while distinguishing missing and unrecognized classifications.

---

## 12. Temporary views between transformation and load

### Decision

Transformation notebooks currently publish temporary Spark views.

### Rationale

This keeps the public implementation concise and avoids unnecessary intermediate persistence.

### Constraint

Transformation and load must run in the same Spark session.

### Future option

Persist intermediate Delta tables if each step must run as an independent task or session.

---

## 13. Incremental curated load with Delta `MERGE`

### Decision

Use:

```text
CREATE when a target does not exist
MERGE when a target already exists
```

### Merge behavior

```text
Matched key      -> update
New key          -> insert
Absent from batch -> preserve target record
```

### Rationale

The pipeline can be rerun without replacing full curated tables or generating duplicate records.

### Trade-off

No automatic hard delete is performed. A separate governed deletion strategy would be required if source deletions must propagate.

---

## 14. Validate all transformed datasets before persistence

### Decision

Run pre-load checks across all required views before modifying curated targets.

### Checks

```text
Required views and columns
Non-empty datasets
Non-null primary keys
Unique primary keys
Valid partition values
Schema compatibility
Referential integrity
```

### Rationale

This reduces the risk of partially updating the analytical model with an invalid batch.

---

## 15. Separate engineering samples from Power BI mock data

### Decision

Maintain two synthetic data areas:

```text
data/sample/
power-bi/mock-data/
```

### Rationale

`data/sample/` represents source-compatible engineering examples. `power-bi/mock-data/` reproduces the existing PBIX semantic tables.

This prevents semantic-model exports from being misrepresented as raw source data.

---

## 16. Preserve original PBIX table names

### Decision

Keep:

```text
d_calendario
d_cid
d_colab
f_abs
f_cat
Medidas
```

### Rationale

Renaming tables would require rebuilding relationships, measures, visuals, filters, drill-through pages, and tooltips.

### Documentation approach

Use a public mapping between pipeline and PBIX names instead of modifying the report structure.

---

## 17. Anonymize monitored health groups

### Decision

Replace original monitored classifications with:

```text
Health Group A
Health Group B
Health Group C
Health Group D
```

Use synthetic category keys:

```text
HGA
HGB
HGC
HGD
```

### Rationale

Renaming visible measures alone would not protect the underlying DAX or dimension data. The public PBIX therefore uses synthetic health records and synthetic filters.

---

## 18. Preserve current PBIX cardinalities in documentation

### Decision

Document the current model as shown, including non-ideal cardinalities, rather than presenting a future star schema as already implemented.

### Rationale

Documentation should reflect the actual artifact.

### Future opportunity

The mock employee dimension contains unique registrations and can support one-to-many employee relationships if the model is later refactored.

---

## 19. Keep architecture diagrams logical, not environment-specific

### Decision

Use generic platform and layer diagrams instead of workspace screenshots.

### Rationale

This communicates the technical design without exposing workspace names, clusters, users, schedules, endpoints, or internal configuration.

---

## 20. Publish the PBIX only if fully sanitized

### Decision

Include `absenteeism-analytics.pbix` only when production data, hidden content, credentials, paths, and metadata have been reviewed and removed.

### Fallback

If a fully sanitized PBIX cannot be guaranteed, publish only documentation, mock CSVs, diagrams, and screenshots.
