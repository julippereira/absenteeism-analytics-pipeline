# ⚙️ Configuration

## Overview

This directory contains the public configuration template used by the **Absenteeism Analytics Pipeline**.

The configuration separates environment-specific behavior from notebook logic. It defines extraction settings, generic table names, Delta Lake storage options, data-quality controls, privacy rules, and references to secrets stored outside the repository.

> `config.example.json` is a public template. It contains placeholders and secret-key references only. Real credentials, API endpoints, company identifiers, report codes, workspace values, and production table locations must never be committed.

---

## Directory structure

```text
config/
├── README.md
└── config.example.json
```

---

## Configuration file

### `config.example.json`

The template is consumed by the ingestion, transformation, and curated-load notebooks.

It contains these top-level sections:

```text
project
security
api
extractions
storage
tables
data_quality
privacy
```

The file is intentionally JSON rather than Python so environment behavior can be updated without modifying notebook code.

---

# Project settings

The `project` section identifies the public solution and its execution context.

```json
{
  "project": {
    "name": "absenteeism-analytics-pipeline",
    "technical_name": "soc_abs",
    "environment": "development",
    "timezone": "America/Sao_Paulo"
  }
}
```

| Setting | Purpose |
|---|---|
| `name` | Public repository and solution name. |
| `technical_name` | Short technical identifier used by the project. |
| `environment` | Execution environment, such as `development`, `test`, or `production`. |
| `timezone` | Timezone used for date and timestamp interpretation. |

---

# Security settings

The `security` section defines how sensitive values and logs must be handled.

```json
{
  "security": {
    "secret_provider": "databricks-secret-scope",
    "secret_scope": "occupational-health-api",
    "fail_on_missing_secret": true,
    "allow_plaintext_secrets": false,
    "redact_sensitive_logs": true,
    "log_response_body": false,
    "log_request_parameters": false,
    "tls_verification": true
  }
}
```

## Secret management

The pipeline expects sensitive values to be resolved from a Databricks secret scope.

The public file stores only secret-key names, for example:

```text
api-base-url
company-id
work-company-id
employee-master-api-key
employee-master-report-code
absenteeism-api-key
absenteeism-report-code
```

The actual secret values must exist in the target environment and must not appear in:

- JSON configuration files;
- notebooks;
- repository history;
- screenshots;
- logs;
- exception messages;
- exported notebook outputs.

## Sensitive logging

The template disables response-body and request-parameter logging because API payloads may contain personal or health-related information.

Fields configured for masking include:

```text
api_key
company_id
employee_id
employee_registration
tax_id
employee_name
diagnosis_code
diagnosis_description
```

## Required production behavior

```text
allow_plaintext_secrets = false
tls_verification = true
verify_ssl = true
redact_sensitive_logs = true
```

The notebooks fail when plaintext secrets or unsafe publication settings are enabled.

---

# API settings

The `api` section defines shared request behavior.

| Setting | Example | Purpose |
|---|---:|---|
| `base_url_secret_key` | `api-base-url` | Secret key containing the API base URL. |
| `company_id_secret_key` | `company-id` | Secret key containing the generic company identifier. |
| `work_company_id_secret_key` | `work-company-id` | Secret key containing the work-company identifier. |
| `request_method_default` | `GET` | Default HTTP method. |
| `output_format` | `json` | Expected response format. |
| `timeout_seconds` | `60` | Maximum wait time per request. |
| `max_retries` | `3` | Maximum retry attempts for transient failures. |
| `retry_backoff_seconds` | `2` | Base wait time between retries. |
| `request_interval_seconds` | `2` | Delay between controlled extraction requests. |
| `max_window_days` | `30` | Maximum date-window size for historical extraction. |
| `verify_ssl` | `true` | Enables SSL certificate validation. |
| `accepted_status_codes` | `[200]` | HTTP status codes treated as successful. |

## Historical windows

Date-based extractions are split into windows of no more than 30 days. This reduces request size, supports retry isolation, and avoids attempting one large historical API call.

Example:

```text
2024-01-01 to 2024-01-30
2024-01-31 to 2024-02-29
2024-03-01 to 2024-03-30
```

No real production start date is included in the public configuration. Date placeholders use:

```text
YYYY-MM-DD
```

---

# Extraction settings

The `extractions` section contains one configuration block per source process.

Configured extraction names include:

```text
employee_master
employee_disability
absenteeism
medical_leave
medical_leave_details
occupational_accident
```

A typical extraction block contains:

```json
{
  "enabled": true,
  "request_method": "GET",
  "report_code_secret_key": "example-report-code",
  "api_key_secret_key": "example-api-key",
  "target_table": "example_source",
  "load_strategy": "full_refresh",
  "historical_start_date": "YYYY-MM-DD",
  "end_date_rule": "yesterday"
}
```

## Extraction attributes

| Setting | Purpose |
|---|---|
| `enabled` | Controls whether the extraction is available for execution. |
| `request_method` | Overrides the shared HTTP method when required. |
| `report_code_secret_key` | Secret key containing the source report code. |
| `api_key_secret_key` | Secret key containing the source API key. |
| `target_table` | Generic Delta source-table name. |
| `load_strategy` | Source persistence strategy. |
| `historical_start_date` | Public placeholder for date-based extraction. |
| `end_date_rule` | Rule used to determine the extraction end date. |

## Enabled and disabled sources

An extraction may remain configured with:

```json
"enabled": false
```

This documents the expected source contract without implying that the public example can execute the production integration.

The occupational-accident source may be represented as a governed dependency when public API extraction is not available.

---

# Storage settings

The `storage` section defines the generic Delta Lake destination.

```json
{
  "storage": {
    "catalog": "portfolio_catalog",
    "schema": "occupational_health",
    "format": "delta",
    "allow_schema_evolution": true,
    "allow_schema_overwrite": false
  }
}
```

## Storage attributes

| Setting | Purpose |
|---|---|
| `catalog` | Generic public catalog name. |
| `schema` | Generic public schema name. |
| `format` | Storage format required by the curated load. |
| `allow_schema_evolution` | Allows compatible new columns during Delta operations. |
| `allow_schema_overwrite` | Prevents uncontrolled replacement of an existing table schema. |

## Partition columns

The fact tables use yearly partition columns:

```text
fact_absenteeism             -> absence_year
fact_occupational_accident   -> accident_year
```

Dimensions are not partitioned in the public configuration.

## Incremental load behavior

The curated-load notebook uses:

```text
CREATE when the target table does not exist
MERGE when the target table already exists
```

Matching analytical keys are updated, new keys are inserted, and target records absent from the current batch are preserved.

---

# Table names

The `tables` section centralizes source and curated table names.

## Source tables

```text
employee_master_source
employee_disability_source
absenteeism_source
medical_leave_source
medical_leave_details_source
occupational_accident_source
historical_absenteeism_source
organizational_perimeter_source
```

## Curated tables

```text
dim_employee
dim_icd
fact_absenteeism
fact_occupational_accident
```

The configuration uses generic public names rather than production catalog objects.

---

# Data-quality settings

The `data_quality` section controls defensive pipeline behavior.

```json
{
  "data_quality": {
    "fail_on_empty_required_source": true,
    "fail_on_schema_mismatch": true,
    "reject_duplicate_business_keys": true,
    "quarantine_invalid_records": true,
    "quarantine_table_suffix": "_quarantine"
  }
}
```

| Setting | Behavior |
|---|---|
| `fail_on_empty_required_source` | Prevents required empty inputs from replacing valid curated data. |
| `fail_on_schema_mismatch` | Stops processing when required source columns are unavailable. |
| `reject_duplicate_business_keys` | Prevents duplicate dimension or fact keys. |
| `quarantine_invalid_records` | Reserves a controlled path for invalid records. |
| `quarantine_table_suffix` | Defines the suffix for quarantine tables. |

Not every public notebook physically writes a quarantine table. The setting documents the intended quality-control contract and can support future implementation.

---

# Privacy settings

The `privacy` section explicitly marks the governed nature of the original data.

```json
{
  "privacy": {
    "contains_personal_data": true,
    "contains_health_data": true,
    "publish_real_data": false,
    "use_synthetic_data_in_repository": true,
    "remove_notebook_outputs_before_commit": true
  }
}
```

## Prohibited public fields

The template identifies fields that must not be published from the production environment:

```text
tax_id
employee_name
employee_registration
medical_notes
raw_diagnosis_description
```

The public repository uses synthetic names, registrations, identifiers, organizational values, conditions, and health groups.

---

# Notebook usage

The notebooks load the configuration using a relative path:

```python
CONFIG_PATH = "../../config/config.example.json"
```

This path assumes a notebook structure such as:

```text
notebooks/
├── 01_ingestion/
├── 02_transformation/
└── 03_load/
```

Each notebook reads only the sections required for its responsibility.

## Ingestion notebooks

Primarily use:

```text
security
api
extractions
storage
tables.sources
data_quality
privacy
```

## Transformation notebooks

Primarily use:

```text
storage
tables.sources
data_quality
privacy
```

## Curated-load notebook

Primarily uses:

```text
storage
tables.curated
data_quality
privacy
```

---

# Environment setup

## 1. Create a private configuration copy if required

The public example can be copied to an ignored environment-specific file:

```text
config/config.development.json
config/config.test.json
config/config.production.json
```

Do not commit environment-specific files if they contain internal names or operational values.

## 2. Create required secrets

Create the secret scope and the keys referenced by the selected extraction blocks.

A conceptual key inventory includes:

```text
api-base-url
company-id
work-company-id
<source>-api-key
<source>-report-code
```

The exact production values and environment-specific scope names must remain private.

## 3. Configure generic destinations

Update only through a safe private copy when different target locations are required:

```json
{
  "storage": {
    "catalog": "your_catalog",
    "schema": "your_schema"
  }
}
```

Do not replace the public example with internal catalog or schema names before committing.

## 4. Configure historical dates

Replace `YYYY-MM-DD` only in the private runtime configuration.

## 5. Run the pipeline

Execute the ingestion, transformation, and load stages according to the orchestration documentation:

```text
orchestration/README.md
```

---

# Public repository rules

The committed `config.example.json` must:

- remain executable as a structural template;
- use generic catalog and schema names;
- use placeholder historical dates;
- reference secrets rather than contain secret values;
- keep plaintext secrets disabled;
- keep real-data publication disabled;
- preserve TLS verification;
- avoid internal URLs, company identifiers, report codes, and workspace names;
- avoid real table or database names when those names are proprietary;
- remain consistent with the public notebooks and data dictionary.

---

# Files that should be ignored

The repository `.gitignore` should exclude private variants and local artifacts, for example:

```gitignore
config/config.development.json
config/config.test.json
config/config.production.json
config/*.local.json
config/*.private.json
.env
.env.*
```

Do not ignore `config.example.json`, because the public notebooks and documentation reference it.

---

# Validation checklist

Before committing configuration changes, confirm that:

- `allow_plaintext_secrets` remains `false`;
- `publish_real_data` remains `false`;
- `use_synthetic_data_in_repository` remains `true`;
- SSL and TLS verification remain enabled;
- no actual API key is present;
- no actual report code is present;
- no internal API endpoint is present;
- no production company identifier is present;
- no internal catalog, schema, workspace, or cluster name is present;
- historical dates remain placeholders unless explicitly safe for publication;
- source and curated table names match the notebooks;
- partition columns match the transformed fact schemas;
- referenced secret keys are documented without revealing their values;
- JSON syntax remains valid.

---

# Related documentation

```text
notebooks/README.md
orchestration/README.md
data/schemas/data-dictionary.md
power-bi/README.md
```

---

## Scope limitations

- The example file does not provision secret scopes or create credentials.
- The example file does not deploy Databricks jobs or workspace resources.
- The public configuration cannot execute production APIs without private environment setup.
- Quarantine behavior is represented as a configuration contract and may require additional implementation.
- Environment-specific deployment tooling is intentionally outside the public repository scope.
