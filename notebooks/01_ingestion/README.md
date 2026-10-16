# 📥 Ingestion Notebooks

## Overview

This directory contains the source-ingestion layer of the **Absenteeism Analytics Pipeline**.

The notebooks retrieve data from controlled APIs, validate source contracts, apply only the minimum transformations required for safe persistence, and write generic Delta source tables for downstream processing.

The public version contains no production credentials, internal endpoints, company identifiers, proprietary report codes, or real employee and health data. Sensitive values are resolved through secret references defined in `config/config.example.json`.

> Production execution requires private environment configuration, governed source access, and a configured Databricks secret scope.

---

## Directory structure

```text
notebooks/01_ingestion/
├── README.md
├── 01_employee_master_ingestion.py
├── 02_disability_ingestion.py
├── 03_absenteeism_ingestion.py
├── 04_medical_leave_ingestion.py
└── 05_leave_details_ingestion.py
```

---

## Layer responsibilities

The ingestion layer is responsible for:

- retrieving credentials from a secret manager;
- validating required configuration;
- sending controlled HTTP requests;
- enforcing TLS and SSL verification;
- applying request timeouts and retry policies;
- respecting intervals between API calls;
- splitting historical extractions into bounded date windows;
- validating mandatory source fields;
- casting only essential source data types;
- adding ingestion metadata;
- preserving source-oriented schemas;
- persisting Delta source tables;
- preventing sensitive request and response logging.

The ingestion layer does not perform dimensional joins, analytical grouping, historical unions, surrogate-key generation, or Power BI-specific anonymization. Those responsibilities belong to `02_transformation/`, `03_load/`, and `power-bi/mock-data/`.

---

## Shared configuration

All ingestion notebooks load:

```python
CONFIG_PATH = "../../config/config.example.json"
```

The configuration defines:

- secret-scope references;
- API timeout and retry rules;
- SSL verification;
- extraction enablement;
- request methods;
- secret-key names for API keys and report codes;
- historical date placeholders;
- generic source-table names;
- catalog and schema placeholders;
- privacy and quality controls.

See `config/README.md` for the complete configuration reference.

---

## Security model

The notebooks retrieve protected values through a Databricks secret scope. The repository stores only conceptual secret-key names, such as:

```text
api-base-url
company-id
work-company-id
<source>-api-key
<source>-report-code
```

Required safeguards:

```text
allow_plaintext_secrets = false
redact_sensitive_logs = true
log_response_body = false
log_request_parameters = false
tls_verification = true
verify_ssl = true
```

The notebooks must not print API keys, report codes, authorization headers, employee identifiers, personal identifiers, diagnosis data, medical notes, or complete protected response bodies.

---

## Shared request behavior

The public configuration uses these defaults:

```text
timeout_seconds           = 60
max_retries               = 3
retry_backoff_seconds     = 2
request_interval_seconds  = 2
max_window_days           = 30
accepted_status_codes     = [200]
```

Retries are intended for transient network, timeout, and service-availability failures. Permanent authentication, authorization, schema, or configuration errors should fail visibly.

### Historical windows

Date-based extractions are divided into windows of no more than 30 days.

```text
2024-01-01 to 2024-01-30
2024-01-31 to 2024-02-29
2024-03-01 to 2024-03-30
```

This limits response size, isolates failed periods, reduces API pressure, and supports controlled retries. Public start dates remain placeholders such as `YYYY-MM-DD`.

---

# Notebook catalog

## `01_employee_master_ingestion.py`

Extracts current employee master data used to construct the employee dimension.

```text
Configuration: extractions.employee_master
Request method: POST
Load strategy: full_refresh
Output: employee_master_source
Downstream: 01_employee_dimension.py
Sample: data/sample/employee_master_sample.csv
```

The source includes employee identifiers, names, job attributes, cost centers, organizational units, departments, gender, age, and ingestion metadata.

---

## `02_disability_ingestion.py`

Extracts disability and rehabilitation attributes used to enrich the employee dimension.

```text
Configuration: extractions.employee_disability
Request method: POST
Load strategy: full_refresh
Output: employee_disability_source
Downstream: 01_employee_dimension.py
Sample: data/sample/employee_disability_sample.csv
```

The source includes employee references, disability type, description, classification, rehabilitation status, relevant dates, and ingestion metadata.

---

## `03_absenteeism_ingestion.py`

Extracts current absenteeism and medical-certificate records through bounded historical date windows.

```text
Configuration: extractions.absenteeism
Request method: GET
Load strategy: full_refresh
Maximum window: 30 days
Output: absenteeism_source
Downstream: 02_icd_dimension.py, 03_absenteeism_fact.py
Sample: data/sample/absenteeism_sample.csv
```

The source includes absence identifiers, employee registrations, diagnosis information, dates, times, durations, certificate types, leave types, leave reasons, units, and ingestion metadata.

---

## `04_medical_leave_ingestion.py`

Extracts the medical-leave source represented separately from the general absenteeism endpoint.

```text
Configuration: extractions.medical_leave
Output: medical_leave_source
```

The request method, enablement, historical period, and source persistence behavior are controlled by configuration. The source remains separate because its API contract and operational purpose may differ from the detailed leave endpoint.

---

## `05_leave_details_ingestion.py`

Extracts detailed medical-leave records in controlled historical batches.

```text
Configuration: extractions.medical_leave_details
Maximum window: 30 days
Output: medical_leave_details_source
Downstream: 02_icd_dimension.py, 03_absenteeism_fact.py
Sample: data/sample/medical_leave_details_sample.csv
```

The source includes leave identifiers, employee registrations, diagnosis codes, generic reasons, leave dates, request and update dates, duration attributes, source flags, and ingestion metadata.

---

# Governed dependencies

The analytical model also uses sources that are not produced by public ingestion notebooks:

```text
historical_absenteeism_source
occupational_accident_source
organizational_perimeter_source
```

## Historical absenteeism

Used to recover historical-only employees and health classifications and to integrate legacy absence events.

```text
Sample: data/sample/historical_absenteeism_sample.csv
```

## Occupational accidents

Used by `04_accident_fact.py` to build the occupational-accident fact.

```text
Sample: data/sample/occupational_accident_sample.csv
```

## Organizational perimeter

Maps cost centers to anonymized business perimeters for employee-dimension enrichment.

```text
Sample: data/sample/organizational_perimeter_sample.csv
```

These dependencies are documented without exposing proprietary extraction logic or internal storage locations.

---

# Persistence behavior

The ingestion notebooks persist Delta source tables using generic catalog and schema settings.

```text
<catalog>.<schema>.<source_table>
```

Public defaults:

```text
catalog = portfolio_catalog
schema  = occupational_health
format  = delta
```

Each extraction defines its source-layer load strategy, such as `full_refresh`. This is separate from the curated incremental `MERGE` implemented by `notebooks/03_load/01_curated_delta_load.py`.

---

# Source-layer transformation policy

Allowed ingestion operations include:

- parsing API JSON;
- selecting expected fields;
- casting essential dates and timestamps;
- adding ingestion metadata;
- validating mandatory fields;
- rejecting structurally invalid responses;
- preserving source business identifiers.

The ingestion layer should avoid:

- dimensional joins;
- analytical hierarchy construction;
- cross-source employee deduplication;
- SHA-256 analytical-key generation;
- cross-system duration standardization;
- historical `unionByName` integration;
- Power BI-specific anonymization.

---

# Data-quality controls

The notebooks should fail clearly when:

- required configuration is missing;
- secrets cannot be resolved;
- the API returns a non-accepted response;
- retries are exhausted;
- required fields are missing;
- a mandatory source is empty and empty-source failure is enabled;
- the response cannot be parsed safely;
- the target Delta table cannot be written.

Relevant configuration controls include:

```text
fail_on_empty_required_source
fail_on_schema_mismatch
quarantine_invalid_records
```

Quarantine settings document the intended quality contract. A notebook may require additional logic if physical quarantine persistence is needed.

---

# Execution model

Independent ingestion tasks may run in parallel when source dependencies, Databricks resources, request intervals, and API limits allow it.

```text
Employee Master Ingestion
Disability Data Ingestion
Absenteeism Ingestion
Medical Leave Ingestion
Leave Details Ingestion
```

Transformation starts only after the required source tables are available.

```text
Ingestion
    -> dimensions
    -> facts
    -> curated Delta MERGE
```

See `orchestration/README.md` for the complete workflow.

---

# Databricks Source format

The files use Databricks Source notebook markers:

```python
# Databricks notebook source
```

```python
# COMMAND ----------
```

```python
# MAGIC %md
```

This format preserves notebook cells when imported into Databricks while keeping the `.py` files readable in GitHub.

---

# Notebook-to-source mapping

| Notebook | Configuration block | Source table | Main downstream use |
|---|---|---|---|
| `01_employee_master_ingestion.py` | `employee_master` | `employee_master_source` | Employee dimension |
| `02_disability_ingestion.py` | `employee_disability` | `employee_disability_source` | Employee dimension |
| `03_absenteeism_ingestion.py` | `absenteeism` | `absenteeism_source` | ICD dimension and absenteeism fact |
| `04_medical_leave_ingestion.py` | `medical_leave` | `medical_leave_source` | Governed leave workflow |
| `05_leave_details_ingestion.py` | `medical_leave_details` | `medical_leave_details_source` | ICD dimension and absenteeism fact |

---

# Privacy and publication checklist

Before committing an ingestion notebook, confirm that:

- no API key or report-code value is present;
- no company identifier or internal API URL is present;
- no authorization header or protected payload is included;
- no real employee name, registration, or personal identifier is included;
- no real diagnosis, description, or medical note is included;
- no internal catalog, schema, workspace, or cluster name is included;
- debug statements do not expose sensitive values;
- exported notebook outputs have been removed;
- secret references are used instead of plaintext values;
- TLS and SSL verification remain enabled;
- sample content is clearly identified as synthetic.

---

# Related documentation

```text
config/README.md
notebooks/README.md
data/README.md
data/schemas/data-dictionary.md
orchestration/README.md
```

---

# Scope limitations

- Public notebooks do not include production credentials.
- Public notebooks cannot call private APIs outside the governed target environment.
- Production APIs may return additional fields not required by the analytical model.
- Some sources are documented as governed dependencies instead of public extraction notebooks.
- Source load strategies and schedules may differ in private environments.
- The notebooks are portfolio-ready engineering artifacts, not an infrastructure deployment package.
