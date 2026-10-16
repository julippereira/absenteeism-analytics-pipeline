# 🔒 Data Privacy and Public-Repository Controls

## Overview

The original solution domain includes personal, employment, demographic, disability, medical-leave, diagnosis, and occupational-accident information. These categories require strict governance regardless of whether a specific field appears harmless in isolation.

The public repository therefore uses synthetic data, generic configuration, anonymized screenshots, and non-production architecture representations.

> No public artifact should be interpreted as proof that production data are anonymous. Public datasets were generated specifically for portfolio use.

---

## Sensitive data categories

The governed solution may process:

- employee names;
- employee registrations;
- personal identifiers;
- age and gender attributes;
- disability information;
- absence and leave dates;
- medical certificate information;
- diagnosis codes and descriptions;
- medical notes or free-text observations;
- occupational-accident details;
- organizational units, roles, and cost centers.

Combining multiple fields can increase re-identification risk even when direct names are removed.

---

## Public-data strategy

The repository does not publish transformed production extracts. Instead, it uses independently generated synthetic datasets.

### Pipeline samples

```text
data/sample/
```

Purpose:

- demonstrate source schemas;
- illustrate current and historical integration;
- support engineering documentation.

### Power BI mock data

```text
power-bi/mock-data/
```

Purpose:

- preserve existing PBIX table and column names;
- refresh the public semantic model;
- support anonymized screenshots and DAX behavior.

---

## Synthetic conventions

Public assets use values such as:

```text
Employee 01
RH0001
EMP0001
SYNTH00000000001
Unit A
Area A
Perimeter A
Role A
CC001
Health Group A
Condition A.1
```

These labels do not represent real people, identifiers, locations, organizational structures, or medical classifications.

---

## Health-classification protection

The public report does not retain the original monitored health chapters, categories, codes, or descriptions.

Public groups are:

```text
Health Group A
Health Group B
Health Group C
Health Group D
Other Health Groups
Not Reported
```

Public DAX filters use:

```text
HGA
HGB
HGC
HGD
```

The private correspondence between public groups and original monitored classifications must not appear in:

- CSV files;
- PBIX data;
- DAX formulas;
- measure names;
- screenshots;
- image metadata;
- documentation;
- hidden pages;
- tooltips;
- drill-through pages;
- repository history.

---

## Configuration controls

The public configuration requires:

```text
allow_plaintext_secrets = false
publish_real_data = false
use_synthetic_data_in_repository = true
remove_notebook_outputs_before_commit = true
redact_sensitive_logs = true
log_response_body = false
log_request_parameters = false
verify_ssl = true
```

Protected values are referenced through a secret manager rather than stored in code.

---

## Prohibited public content

The repository do not contain:

- API keys or tokens;
- report-code values;
- production endpoints;
- company identifiers;
- authentication headers;
- internal workspace, tenant, cluster, gateway, catalog, or schema names;
- real employee names;
- valid personal identifiers;
- real employee registrations;
- real units, areas, roles, perimeters, or cost centers;
- real diagnosis codes or descriptions;
- medical notes;
- protected API payloads;
- production PBIX data;
- operational logs containing protected values.

---

## Notebook controls applied

The public notebooks were reviewed and sanitized before publication.

The following controls were applied:

- rendered notebook outputs were removed;
- copied API responses were removed;
- debugging displays and temporary inspection commands were removed;
- notebooks were reviewed for credentials, tokens, endpoints, and internal paths;
- employee and organization-specific values were replaced or removed;
- logs were reviewed to ensure that request parameters and protected response content are not exposed;
- all published sample values were verified as synthetic;
- plaintext credentials were replaced with secret-key references;
- production catalog, schema, workspace, and cluster identifiers were removed.

Hash-derived analytical keys are used for stable processing and relationships. However, hashing is not treated as sufficient anonymization, and the original business identifiers remain excluded from the public repository.

---

## PBIX controls applied

The public Power BI file was created from a separate copy of the original report and sanitized for portfolio publication.

The following controls were applied:

1. the original report was duplicated before modification;
2. production data sources were replaced with synthetic CSV files;
3. all semantic-model tables were refreshed with mock data;
4. imported data and calculated objects were reviewed;
5. monitored health-group measures were renamed and refactored;
6. health-group filters were replaced with synthetic keys;
7. Power Query parameters and source paths were reviewed;
8. hidden report pages were inspected;
9. tooltips and drill-through pages were inspected;
10. visual-level, page-level, and report-level filters were reviewed;
11. bookmarks and selection states were reviewed;
12. saved credentials and data-source permissions were removed;
13. report metadata and file properties were reviewed;
14. all public screenshots were regenerated from the sanitized mock report.

The public PBIX contains synthetic employee, organizational, health, absence, and occupational-accident data only. The original monitored health classifications are not retained in the public data, DAX formulas, measure names, filters, screenshots, or documentation.

## Screenshot controls

All screenshots exclude:

- account and workspace information;
- browser or desktop notifications;
- internal URLs and paths;
- production filter values;
- real employee records;
- original monitored health groups;
- sensitive tooltips or drill-through results;
- query and credential dialogs.

The screenshot checklist is maintained in:

```text
screenshots/README.md
```

---

## Logs and observability

Useful operational logs include:

```text
job status
task duration
record counts
retry events
schema failures
duplicate-key failures
MERGE status
referential-integrity failures
```

Logs do not include:

```text
credentials
protected request parameters
response bodies
employee names
personal identifiers
diagnosis descriptions
medical notes
```

---

## Repository-history protection

The repository was reviewed to reduce the risk of sensitive information remaining in version history.

The review covered:

- committed configuration files;
- notebook source files and exported outputs;
- synthetic CSV datasets;
- Power BI assets;
- screenshots and diagrams;
- generated archives and downloadable artifacts;
- internal paths, endpoints, credentials, and identifiers.

No production credentials, protected API responses, real employee records, original monitored health-group mappings, or private configuration files are intentionally included in the public repository.

Deleting a sensitive file in a later commit does not guarantee its removal from repository history. If protected content is committed accidentally, the response process should include:

1. stopping further publication;
2. revoking exposed credentials immediately;
3. removing the protected content from repository history through an appropriate governed process;
4. reviewing forks, releases, caches, and generated artifacts;
5. documenting the incident through the applicable internal process;
6. regenerating and republishing sanitized assets.

---

## `.gitignore` controls applied

The repository excludes private configuration files, local environment files, credentials, generated caches, and other non-public artifacts.

The following patterns are included in the `.gitignore` policy:

```gitignore
config/config.development.json
config/config.test.json
config/config.production.json
config/*.local.json
config/*.private.json

.env
.env.*

__pycache__/
*.py[cod]

.ipynb_checkpoints/
*databricks/

*.log
*.tmp
*.bak
```

The public configuration template remains tracked:
```
config/c*nfig.example.json
```

The `.gitignore` is a preventive control and does not replace the manual review of files, generated archives, notebook outputs, or repository history.

---

## Public artifacts reviewed

Status: completed

The public repository artifacts were reviewed before publication.

The following controls were confirmed:

 - All published CSV datasets contain synthetic data.
 - Power BI screenshots were generated from the mock report.
 - The public PBIX was refreshed with synthetic datasets.
 - Hidden pages, tooltips, drill-through pages, filters, bookmarks, and selections were reviewed.
 - Original monitored health-group mappings were removed from public data, DAX, measures, screenshots, and documentation.
 - Notebook outputs, copied API responses, and debugging displays were removed.
 - Public configuration files contain secret references rather than secret values.
 - Architecture diagrams use generic platform, table, and component names.
 - Private configuration variants are excluded from version control.
 - Generated archives were reviewed to ensure that private files were not included.
 - Repository content was reviewed for credentials, endpoints, internal identifiers, and protected values.
 - Public documentation consistently identifies the datasets and dashboard values as synthetic.

The public repository contains only sanitized technical artifacts intended for portfolio demonstration.

---

## Scope

This document describes the repository's privacy-by-design controls. It does not replace organizational privacy, security, legal, retention, access-control, or incident-response policies.
