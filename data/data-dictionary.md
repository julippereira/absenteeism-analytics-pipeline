# Data Dictionary

## Overview

This document describes the public, anonymized data model used by the **Absenteeism Analytics Pipeline**. The repository contains only generic schemas and synthetic examples. Real employee identifiers, medical records, API credentials, internal endpoints, and proprietary organizational mappings are not included.

The pipeline is organized into two logical layers:

- **Source layer:** datasets ingested from APIs or controlled historical sources.
- **Curated layer:** dimensions and facts prepared for analytics and Power BI consumption.

> **Privacy note:** some source datasets may contain personal and health-related information in the governed production environment. All public samples use synthetic values only.

---

## Modeling conventions

| Convention | Description |
|---|---|
| Naming | Curated columns use lowercase `snake_case`. |
| Analytical keys | Stable SHA-256 hashes generated from controlled business identifiers. |
| Dates | Stored as `date` whenever time-of-day is not required. |
| Timestamps | Stored as `timestamp`. |
| Durations | Standardized into days and hours using an analytical 8-hour workday. |
| Unknown members | Unmatched relationships use deterministic `UNKNOWN` dimension members. |
| Load strategy | Curated tables use incremental Delta `MERGE`. Matching keys are updated and new keys are inserted. |
| Partitioning | Fact tables are partitioned by the year of the business event. |

---

# Source layer

## `employee_master_source`

Current employee master data extracted from the source API. It supports employee identification, organizational enrichment, and construction of `dim_employee`.

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `MATRICULARH` | string | No | Employee identifier used by the HR source. |
| `MATRICULAFUNCIONARIO` | string | Yes | Secondary employee registration identifier. |
| `CPF` | string | No | Governed personal identifier used only for controlled matching. Not included in public samples. |
| `NOME` | string | Yes | Employee name. Synthetic values only in the repository. |
| `NOMECARGO` | string | Yes | Employee job title. |
| `RHCENTROCUSTOUNIDADE` | string | Yes | Organizational cost center identifier. |
| `NOMEUNIDADE` | string | Yes | Organizational unit name. |
| `NOMESETOR` | string | Yes | Department or sector name. |
| `SEXO` | string | Yes | Gender value supplied by the source system. |
| `IDADE` | integer | Yes | Calculated employee age at ingestion time. |
| `ingestion_timestamp` | timestamp | No | Timestamp when the source dataset was ingested. |

> The physical API may return additional attributes. Only fields relevant to the analytical model are documented here.

---

## `employee_disability_source`

Employee disability and rehabilitation attributes extracted from the source API.

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `ID_FUNCIONARIO` | string | No | Source-system employee identifier. |
| `MATRICULA` | string | Yes | Employee registration used to relate the record to the employee dimension. |
| `NOME_FUNCIONARIO` | string | Yes | Employee name from the source. Synthetic values only in public samples. |
| `ID_TIPO_DEFICIENCIA` | string | Yes | Source identifier for the disability type. |
| `DEFICIENCIA` | string | Yes | Standardized disability description. |
| `CLASSIFICACAO` | string | Yes | Disability classification supplied by the source. |
| `REABILITADO` | string | Yes | Rehabilitation status. |
| `DATA_INICIO_DEFICIENCIA` | date | Yes | Start date associated with the disability record. |
| `DATA_REABILITACAO` | date | Yes | Rehabilitation date, when available. |
| `ingestion_timestamp` | timestamp | No | Timestamp when the source dataset was ingested. |

---

## `absenteeism_source`

Current absenteeism and medical certificate records extracted in controlled date windows.

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `CODIGO_SEQUENCIAL_ATESTADO` | string | No | Business identifier of the medical certificate or absence event. |
| `MATRICULA_FUNC` | string | No | Employee registration associated with the event. |
| `CID_PRINCIPAL` | string | Yes | Primary ICD code reported for the event. |
| `DESCRICAO_CID` | string | Yes | Diagnosis description returned by the source. Sensitive in production. |
| `UNIDADE` | string | Yes | Organizational unit associated with the employee or event. |
| `DT_INICIO_ATESTADO` | date | No | Start date of the medical certificate. |
| `DT_FIM_ATESTADO` | date | Yes | Reported end date of the medical certificate. |
| `DT_FICHA_ATESTADO` | date | Yes | Date of the source medical record. |
| `DT_CRIACAO` | date/timestamp | Yes | Creation date or timestamp of the source record. |
| `HORA_INICIO_ATESTADO` | string | Yes | Start time for hourly absences. |
| `HORA_FIM_ATESTADO` | string | Yes | End time for hourly absences. |
| `DIAS_AFASTADOS` | integer | Yes | Reported absence duration in days. |
| `HORAS_AFASTADO` | string | Yes | Reported absence duration in hours or `HH:mm` format. |
| `TIPO_ATESTADO` | string | Yes | Medical certificate classification. |
| `TIPO_LICENCA` | string | Yes | Leave category, when returned by the source. |
| `MOTIVO_LICENCA` | string | Yes | Leave reason. Sensitive in production. |
| `ingestion_timestamp` | timestamp | No | Timestamp when the source dataset was ingested. |

---

## `medical_leave_details_source`

Detailed medical leave records extracted in historical batches.

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `codigoSequencialLicenca` | string | No | Business identifier of the leave record. |
| `matricula` | string | Yes | Employee registration associated with the leave. |
| `cids` | string | Yes | One or more ICD codes returned by the API. The analytical model uses the first valid code. |
| `descricaoMotivo` | string | Yes | Leave or diagnosis description. Sensitive in production. |
| `dataFicha` | date | Yes | Date of the medical record. |
| `dataInicioAfastamento` | date | No | Start date of the leave. |
| `dataFimAfastamento` | date | Yes | Reported end date of the leave. |
| `dataSolicitacao` | date | Yes | Date when the leave was requested. |
| `dataCriacao` | date | Yes | Creation date of the source record. |
| `dataUltimaAlteracao` | date | Yes | Date of the latest source update. |
| `abonado` | boolean | Yes | Indicates whether the absence was excused. |
| `acidenteTrajeto` | boolean | Yes | Indicates whether the record is related to a commuting accident. |
| `imprimirCid` | boolean | Yes | Source-system display flag for the ICD code. |
| `imprimirMotivo` | boolean | Yes | Source-system display flag for the leave reason. |
| `imprimirSolicitante` | boolean | Yes | Source-system display flag for the requester. |
| `existeLicencaMesmoMotivo60Dias` | boolean | Yes | Indicates another leave with the same reason within 60 days. |
| `ingestion_timestamp` | timestamp | No | Timestamp when the source dataset was ingested. |

---

## `occupational_accident_source`

Occupational accident and CAT-related records supplied by a governed source table.

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `NUMEROCAT` | string | No | Source business identifier for the CAT record. |
| `CPF` | string | Yes | Governed personal identifier used only for controlled employee matching. |
| `DATADOACIDENTE` | date/string | No | Date of the accident. |
| `DATADOREGISTRO` | timestamp/string | Yes | Date or timestamp when the accident was registered. |
| `ULTIMODIATRABALHO` | date/string | Yes | Last working day before the leave. |
| `DIASAFASTADO` | integer/string | Yes | Number of leave days associated with the accident. |
| `AFASTAMENTO` | boolean/string | Yes | Indicates whether the accident caused leave. |
| `MORTE` | boolean/string | Yes | Indicates whether the event was fatal. |
| `TIPODECAT` | string | Yes | CAT classification. |
| `TIPODEACIDENTE` | string | Yes | Accident classification. |
| `TIPO` | string | Yes | Source record type. |
| `PARTEDOCORPOATINGIDO` | string | Yes | Affected body part. |
| `AGENTECAUSADOR` | string | Yes | Agent associated with the accident. |
| `ARVORESDECAUSA` | string | Yes | Cause-tree description. |
| `CAUSASDOACIDENTE` | string | Yes | Accident cause description. |
| `SITUACAOGERADORA` | string | Yes | Situation that generated the accident. |
| `ATIVIDADENOATODOACIDENTE` | string | Yes | Activity performed at the time of the accident. |
| `LOCALDOACIDENTE` | string | Yes | Accident location. |
| `POTENCIALDEACIDENTE` | string | Yes | Accident-potential classification. |
| `AREA` | string | Yes | Organizational area associated with the event. |
| `EMITENTE` | string | Yes | Source issuer or responsible party. |
| `OCORRENCIADOREGISTRO` | string | Yes | Source occurrence description. |

---

## `historical_absenteeism_source`

Legacy absenteeism history integrated with current API data. Original names contain spaces, punctuation, and accented characters. Transformation notebooks normalize them before modeling.

| Original field | Normalized name | Type | Nullable | Description |
|---|---|---|---:|---|
| `PERIMETRO` | `perimetro` | string | Yes | Organizational perimeter description. |
| `UNIDADE` | `unidade` | string | Yes | Organizational unit. |
| `D.TOTAL` | `d_total` | string | Yes | Historical absence duration in days. |
| `H.TOTAL` | `h_total` | string | Yes | Historical absence duration in hours. |
| `D.ESTESA` | `d_estesa` | string | Yes | Legacy source field retained only at source level. |
| `H.ESTESA` | `h_estesa` | string | Yes | Legacy source field retained only at source level. |
| `DATA ATESTADO` | `data_atestado` | string | Yes | Date of the historical medical certificate. |
| `Inicio Afast.` | `inicio_afast` | string | No | Start date of the historical absence. |
| `Fim Afast.` | `fim_afast` | string | Yes | End date of the historical absence. |
| `HORAS` | `horas` | string | Yes | Additional historical hour field. |
| `DIAS` | `dias` | string | Yes | Additional historical day field. |
| `STATUS` | `status` | string | Yes | Historical record status. |
| `Tipo Atestado` | `tipo_atestado` | string | Yes | Historical certificate type. |
| `COLABORADOR` | `colaborador` | string | Yes | Employee name. Synthetic values only in public samples. |
| `Matrícula` | `matricula` | string | No | Historical employee registration. |
| `Situação` | `situacao` | string | Yes | Historical leave situation or reason. |
| `CID` | `cid` | string | Yes | Historical ICD code. |
| `SubCID` | `subcid` | string | Yes | Historical ICD subcategory. |
| `CAPITULO` | `capitulo` | string | Yes | Historical ICD chapter. |
| `Doença` | `doenca` | string | Yes | Historical diagnosis description. Sensitive in production. |
| `Subnormalidades` | `subnormalidades` | string | Yes | Additional historical clinical classification. |
| `PCD` | `pcd` | string | Yes | Historical disability indicator. |
| `Observação` | `observacao` | string | Yes | Free-text observation. Never included in public samples. |
| `CCUSTO` | `ccusto` | string | Yes | Historical cost center identifier. |
| `Sexo` | `sexo` | string | Yes | Historical gender value. |
| `Setor` | `setor` | string | Yes | Historical department or sector. |
| `Cargo` | `cargo` | string | Yes | Historical job title. |
| `Idade` | `idade` | string | Yes | Historical employee age. |

### Historical integration

The historical dataset supports three transformations:

1. **Employee dimension:** adds employees found only in the legacy history.
2. **ICD dimension:** adds ICD codes and descriptions found only in historical records.
3. **Absenteeism fact:** appends historical absence events with `unionByName(..., allowMissingColumns=True)`.

---

## `organizational_perimeter_source`

Auxiliary mapping used to enrich employee records with an organizational perimeter.

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `cost_center_id` | string | No | Cost center identifier. |
| `perimeter_name` | string | Yes | Business perimeter associated with the cost center. |

---

# Curated layer

## `dim_employee`

Conformed employee dimension integrating current employees, historical-only employees, organizational perimeter, and disability attributes.

**Primary key:** `employee_key`

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `employee_key` | string | No | Stable SHA-256 analytical key for the employee. |
| `employee_hr_id` | string | Yes | Employee identifier from the HR-oriented source. |
| `employee_registration_id` | string | Yes | Secondary employee registration identifier. |
| `employee_name` | string | Yes | Standardized employee name. Synthetic values only in public samples. |
| `job_title` | string | Yes | Standardized job title. |
| `cost_center_id` | string | Yes | Cost center identifier. |
| `perimeter_name` | string | Yes | Organizational perimeter. |
| `unit_name` | string | Yes | Organizational unit. |
| `department_name` | string | Yes | Department or sector. |
| `gender` | string | No | Standardized value: `M`, `F`, or `NOT_INFORMED`. |
| `age` | integer | Yes | Employee age available at processing time. |
| `age_group` | string | No | Analytical age range. |
| `disability_description` | string | No | Standardized disability description or default member. |
| `disability_classification` | string | No | Standardized disability classification or default member. |
| `data_origin` | string | No | Origin of the employee record, such as `CURRENT_SYSTEM`, `LEGACY_SYSTEM`, or `SYSTEM_MEMBER`. |
| `snapshot_timestamp` | timestamp | No | Timestamp when the dimension version was generated. |

### Age-group values

| Value | Meaning |
|---|---|
| `UP_TO_19` | Up to 19 years old. |
| `20_TO_29` | From 20 to 29 years old. |
| `30_TO_39` | From 30 to 39 years old. |
| `40_TO_49` | From 40 to 49 years old. |
| `50_TO_59` | From 50 to 59 years old. |
| `60_OR_MORE` | 60 years old or older. |
| `NOT_INFORMED` | Age missing or invalid. |

---

## `dim_icd`

Analytical ICD dimension consolidating categories and subcategories from current and historical records.

**Primary key:** `icd_key`  
**Business key:** `icd_code`

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `icd_key` | string | No | Stable SHA-256 analytical key generated from `icd_code`. |
| `icd_code` | string | No | Standardized ICD category or subcategory code. |
| `icd_chapter` | string | No | High-level ICD analytical chapter. |
| `icd_group` | string | No | Consolidated analytical disease group. |
| `icd_subgroup` | string | No | More detailed analytical subgroup. |
| `icd_category_code` | string | No | Three-character ICD category code. |
| `icd_category_description` | string | No | Category code and selected description. |
| `icd_subcategory_code` | string | No | Standardized ICD subcategory code. |
| `icd_subcategory_description` | string | No | Subcategory code and selected description. |
| `snapshot_timestamp` | timestamp | No | Timestamp when the dimension was generated. |

### Special members

| `icd_code` | Meaning |
|---|---|
| `NO_ICD` | Medical record without an ICD code. |
| `UNKNOWN` | Invalid or unrecognized ICD code. |

> The hierarchy is designed for analytical grouping and does not claim to reproduce a complete official terminology database.

---

## `fact_absenteeism`

Unified fact table for current absenteeism, medical leave details, and historical absence records.

**Primary key:** `absence_key`  
**Partition column:** `absence_year`

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `absence_key` | string | No | Stable SHA-256 analytical key for the absence event. |
| `absence_id` | string | No | Source business identifier or deterministic identifier generated for legacy events. |
| `employee_key` | string | No | Foreign key to `dim_employee`. |
| `employee_source_id` | string | Yes | Source employee registration used during matching. |
| `icd_key` | string | No | Foreign key to `dim_icd`. |
| `icd_code` | string | No | Standardized ICD code, `NO_ICD`, or `UNKNOWN`. |
| `diagnosis_description` | string | Yes | Selected source diagnosis description. Sensitive in production. |
| `unit_name` | string | Yes | Organizational unit associated with the record. |
| `absence_start_date` | date | No | Start date of the absence. |
| `absence_end_date` | date | No | Reported or calculated end date. |
| `record_date` | date | Yes | Date of the source medical record. |
| `creation_timestamp` | timestamp | Yes | Creation timestamp or fallback from `record_date`. |
| `absence_start_time` | string | Yes | Start time for hourly absences. |
| `absence_end_time` | string | Yes | End time for hourly absences. |
| `absence_days_reported` | double | Yes | Duration in days as reported by the source. |
| `absence_hours_reported` | double | Yes | Duration in hours as reported or converted from `HH:mm`. |
| `total_absence_days` | double | No | Standardized duration in analytical days. |
| `total_absence_hours` | double | No | Standardized duration in analytical hours. |
| `duration_type` | string | No | `DAYS`, `HOURS`, or `NOT_INFORMED`. |
| `certificate_type` | string | Yes | Medical certificate classification. |
| `leave_type` | string | Yes | Leave classification. |
| `leave_reason` | string | Yes | Leave reason. Sensitive in production. |
| `record_type` | string | No | Constant analytical value `ABSENTEEISM`. |
| `data_origin` | string | No | `CURRENT_SYSTEM`, `MEDICAL_LEAVE_API`, or `LEGACY_SYSTEM`. |
| `absence_year` | integer | No | Year derived from `absence_start_date`; used for partitioning. |
| `absence_status` | string | No | `RETURNED` or `ON_LEAVE`, based on the final end date. |
| `snapshot_timestamp` | timestamp | No | Timestamp when the fact record was transformed. |

### Duration rule

```text
1 analytical workday = 8 hours
```

When only hours are reported:

```text
total_absence_days = absence_hours_reported / 8
```

When only days are reported:

```text
total_absence_hours = absence_days_reported * 8
```

---

## `fact_occupational_accident`

Curated occupational accident fact prepared from CAT-related records.

**Primary key:** `accident_key`  
**Partition column:** `accident_year`

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `accident_key` | string | No | Stable SHA-256 analytical key for the accident record. |
| `accident_id` | string | No | Source business identifier or deterministic fallback identifier. |
| `employee_key` | string | No | Foreign key to `dim_employee`. |
| `employee_source_id` | string | Yes | Source employee registration used during matching. |
| `accident_date` | date | No | Date of the accident. |
| `record_timestamp` | timestamp | Yes | Timestamp when the event was registered. |
| `last_workday_date` | date | Yes | Last working day before associated leave. |
| `absence_days` | integer | Yes | Non-negative number of leave days associated with the event. |
| `leave_flag` | boolean | Yes | Indicates whether the event caused leave. |
| `fatality_flag` | boolean | Yes | Indicates whether the accident was fatal. |
| `accident_status` | string | No | `FATAL`, `WITH_LEAVE`, or `WITHOUT_LEAVE`. |
| `cat_type` | string | Yes | CAT classification. |
| `accident_type` | string | Yes | Accident classification. |
| `record_type` | string | No | Source record type or `OCCUPATIONAL_ACCIDENT`. |
| `affected_body_part` | string | Yes | Affected body part. |
| `causing_agent` | string | Yes | Agent associated with the accident. |
| `cause_tree` | string | Yes | Cause-tree description. |
| `accident_cause` | string | Yes | Accident cause description. |
| `generating_situation` | string | Yes | Situation that generated the accident. |
| `activity_at_accident` | string | Yes | Activity performed at the time of the accident. |
| `accident_location` | string | Yes | Accident location. |
| `accident_potential` | string | Yes | Potential-severity classification. |
| `area_name` | string | Yes | Organizational area. |
| `issuer_name` | string | Yes | Source issuer or responsible party. |
| `record_occurrence` | string | Yes | Source occurrence description. |
| `data_origin` | string | No | Constant value `OCCUPATIONAL_ACCIDENT_SOURCE`. |
| `accident_year` | integer | No | Year derived from `accident_date`; used for partitioning. |
| `snapshot_timestamp` | timestamp | No | Timestamp when the fact record was transformed. |

---

# Relationships

| From | Field | To | Field | Cardinality |
|---|---|---|---|---|
| `fact_absenteeism` | `employee_key` | `dim_employee` | `employee_key` | Many-to-one |
| `fact_absenteeism` | `icd_key` | `dim_icd` | `icd_key` | Many-to-one |
| `fact_occupational_accident` | `employee_key` | `dim_employee` | `employee_key` | Many-to-one |

The load layer validates these relationships before executing the Delta `MERGE`.

---

# Incremental load behavior

Curated tables are loaded using Delta `MERGE`:

- matching primary keys are updated;
- new primary keys are inserted;
- records absent from the current batch are preserved;
- no automatic hard delete is performed;
- the first successful load creates the Delta table;
- subsequent loads retain the original partition structure.

| Target table | Merge key | Partition |
|---|---|---|
| `dim_employee` | `employee_key` | None |
| `dim_icd` | `icd_key` | None |
| `fact_absenteeism` | `absence_key` | `absence_year` |
| `fact_occupational_accident` | `accident_key` | `accident_year` |

---

# Privacy and publication rules

The following controls apply to this repository:

- only synthetic or fully anonymized datasets may be published;
- real names, tax identifiers, employee registrations, medical notes, and raw diagnosis descriptions must not be committed;
- notebook outputs must be removed before publication;
- API credentials and company identifiers must be read from a secret manager;
- internal catalog, schema, endpoint, and report-code values must be replaced with placeholders;
- screenshots and Power BI files must be reviewed for hidden personal data and metadata;
- free-text medical observations must never be included in public samples.

---

# Scope limitations

- The public schema is simplified and may not include every field returned by the production APIs.
- The ICD hierarchy is an analytical grouping layer, not a complete clinical terminology service.
- Employee age is snapshot-based and may change between executions.
- The duration conversion uses an analytical assumption of eight hours per workday.
- Historical fields not required by the analytical model remain documented at source level but are not propagated to curated tables.
- Occupational accident ingestion is represented as a governed source dependency when a public extraction notebook is not available.
