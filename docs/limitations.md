# 🚧 Limitations

## Overview

The public **Absenteeism Analytics Pipeline** is a portfolio-oriented representation of a governed analytical solution. It demonstrates architecture, engineering logic, semantic modeling, and dashboard behavior using synthetic data.

The project should not be interpreted as a deployable production package or a clinical, HR, legal, payroll, or occupational-health decision system.

---

## Synthetic data

- All public records are synthetic.
- Distributions do not represent a real workforce.
- Values were designed for structural and analytical coverage rather than statistical realism.
- Employee, demographic, organizational, medical, and accident patterns must not be interpreted operationally.
- Synthetic identifiers are placeholders and are not valid personal identifiers.

---

## Health classifications

- Public health groups are synthetic.
- `Health Group A–D` do not represent documented clinical categories.
- The original monitored classifications are intentionally excluded.
- Health hierarchies in the public project demonstrate analytical navigation, not clinical terminology management.
- The project does not provide diagnosis validation or medical interpretation.

---

## API execution

- Public notebooks do not include valid credentials or production endpoints.
- Private network access may be required.
- Production APIs may return fields not represented in the public samples.
- Rate limits, authentication mechanisms, response contracts, and schedules may vary by environment.
- Public examples cannot be executed against private systems without governed setup.

---

## Infrastructure and deployment

The repository does not provision:

- Databricks workspaces;
- compute resources;
- secret scopes;
- catalogs and access permissions;
- network controls;
- Power BI workspaces;
- gateways;
- refresh schedules;
- deployment pipelines;
- monitoring infrastructure;
- alerting channels.

Cluster sizing, scaling, cost controls, and service-level objectives remain environment-specific.

---

## Temporary Spark views

The current transformation and load sequence uses temporary Spark views.

```text
source_dim_employee
source_dim_icd
source_fact_absenteeism
source_fact_occupational_accident
```

Transformation and load must therefore run in the same Spark session.

Independent task execution requires persisted intermediate tables or another shared storage mechanism.

---

## Curated load behavior

- The load performs `CREATE` or incremental `MERGE`.
- Matched keys are updated.
- New keys are inserted.
- Existing target records absent from the current batch are preserved.
- No automatic hard delete is implemented.
- Source deletions require a separate governed strategy.
- Quarantine settings are documented but physical quarantine persistence may require additional implementation.

---

## Schema evolution

- Compatible schema evolution may be enabled.
- Incompatible changes are blocked when schema overwrite is disabled.
- Automatic schema merge does not guarantee downstream Power BI compatibility.
- New fields and changed types still require semantic-model review.

---

## Duration convention

The analytical model uses:

```text
1 workday = 8 hours
```

This supports consistent comparison between day-based and hourly events but may not match payroll, legal, collective-agreement, or local scheduling rules.

---

## Bradford Factor

- Bradford Factor values are based on synthetic events.
- Values may be unusually high because the dataset prioritizes scenario coverage.
- No organizational risk thresholds are published.
- The measure must not be used for medical diagnosis, disciplinary action, employment decisions, or health recommendations.
- Interpretation depends on policy, context, and appropriate human review.

---

## Power BI model

The current PBIX preserves existing model characteristics, including documented non-ideal relationships.

Current considerations include:

- a many-to-many relationship between `d_colab` and `f_abs`;
- an inverted visible cardinality between `d_colab` and `f_cat`;
- no active calendar relationship to `f_cat` in the documented model;
- a disconnected `Medidas` table, which is intentional.

The mock employee dimension supports future one-to-many refactoring, but documentation does not claim that this change has been implemented.

---

## Power BI visuals

- The perimeter slicer may truncate labels because the mock contains more categories than the original layout.
- Blank values may appear where source attributes or relationships are optional.
- `Afastados Hoje` may return `0` when no static mock interval includes the viewing date.
- The calendar currently ends on `2026-12-31`.
- Later event dates require calendar extension.
- The accident fact is present even though the current pages focus mainly on absenteeism.

---

## PBIX publication

- The PBIX may still contain hidden metadata, query paths, cached content, or hidden-page values if sanitization is incomplete.
- The file should be omitted if a full review cannot be completed.
- Documentation and screenshots are safer fallbacks than publishing an uncertain binary artifact.
- The public PBIX is not a deployment package.

---

## Data quality

The project validates schemas, keys, partitions, and relationships, but cannot guarantee:

- correctness of unavailable production source data;
- completeness of source-system history;
- semantic accuracy of every external field;
- absence of upstream business-process errors;
- clinical correctness of free-text descriptions;
- automatic resolution of every duplicate or conflicting record.

Data-quality controls reduce risk but do not replace source ownership and governance.

---

## Performance and scale

- Public datasets are intentionally small.
- Performance at production scale has not been demonstrated by the public samples.
- Partitioning only by year may not suit every data volume or access pattern.
- Join strategies, caching, clustering, file sizing, and optimization require environment-specific testing.
- Power BI import size and refresh performance depend on production volume and capacity.

---

## Security and privacy

- The repository describes public safeguards but does not implement enterprise identity, authorization, retention, or incident management.
- Secret references require properly governed runtime configuration.
- Hash keys do not by themselves make production data anonymous.
- Re-identification risk must be assessed across combinations of fields.
- Organizational policies and applicable privacy requirements remain authoritative.

---

## Testing

- Public samples support structural validation, not comprehensive production testing.
- Automated unit, integration, regression, and deployment tests are not fully represented.
- API failures, schema drifts, late-arriving records, deletions, and partial outages require additional controlled test scenarios.
- Power BI visual regression testing is outside the current repository scope.

---

## Intended use

Appropriate uses include:

- portfolio review;
- architecture discussion;
- data-engineering demonstration;
- Power BI documentation;
- synthetic testing;
- repository-structure reference.

Inappropriate uses include:

- clinical assessment;
- employee evaluation;
- legal or compliance decisions;
- payroll calculations;
- safety incident adjudication;
- production deployment without review;
- interpretation of synthetic values as real organizational evidence.
