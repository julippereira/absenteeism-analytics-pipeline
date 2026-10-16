# 🏢 Business Context

## Overview

Organizations often receive occupational-health and absenteeism information from multiple operational sources, each with different schemas, identifiers, time formats, update cycles, and levels of detail. Without a unified analytical layer, recurring absences, duration patterns, organizational concentration, and historical trends are difficult to monitor consistently.

The **Absenteeism Analytics Pipeline** was designed to consolidate these sources into a governed analytical model and an interactive Power BI dashboard.

> The public repository uses synthetic data and generic organizational structures. It demonstrates the solution design without representing a real workforce or exposing production information.

---

## Business problem

The original analytical need involved several challenges:

- employee information distributed across different source systems;
- current and historical absence records stored in different formats;
- day-based and hourly durations requiring standardization;
- health classifications represented at different hierarchy levels;
- employees appearing only in legacy records;
- organizational attributes requiring auxiliary mappings;
- repeated manual effort to consolidate data for reporting;
- limited traceability between source records and dashboard indicators;
- sensitive employee and health-related information requiring strict governance.

A reliable solution therefore needed to address both the analytical problem and the confidentiality of the underlying data.

---

## Project objective

The project creates an end-to-end pipeline that:

1. extracts controlled source data;
2. preserves source-oriented records in Delta tables;
3. integrates current and historical information;
4. builds conformed employee and health-classification dimensions;
5. builds absenteeism and occupational-accident facts;
6. validates keys, schemas, partitions, and relationships;
7. performs incremental curated loads;
8. exposes a reusable semantic model in Power BI;
9. supports executive, temporal, organizational, demographic, and record-level analysis.

---

## Analytical questions

The dashboard was designed to support questions such as:

- How many certificates and absence hours occurred in the selected period?
- Which units, areas, cost centers, or roles concentrate the highest values?
- Are high totals caused by many short records or fewer long-duration events?
- How do patterns change by weekday, month, and year?
- Which anonymized health groups show recurring or increasing behavior?
- Which employees have repeated absence episodes in the selected period?
- How do absence patterns vary across demographic attributes?
- Which employees are currently classified as on leave?
- Can aggregated indicators be traced back to synthetic detail records?

---

## Main business indicators

The public report includes indicators such as:

```text
Number of medical certificates
Total absence hours
Employees currently on leave
Bradford Factor
Monthly evolution
Health-group evolution
Organizational concentration
Employee-level distribution
Demographic distribution
```

The monitored health classifications are represented publicly as:

```text
Health Group A
Health Group B
Health Group C
Health Group D
```

The original correspondence is intentionally excluded from the repository.

---

## Stakeholder perspectives

### Executive and management view

Supports a concise overview of volume, duration, organizational concentration, and current status.

### Occupational-health and EHS view

Supports investigation of recurring health-group patterns, employee history, duration, and organizational distribution while respecting governed access requirements.

### Data and analytics view

Provides a traceable pipeline, reusable dimensions and facts, documented measures, quality controls, and a maintainable semantic model.

### Audit and governance view

Documents how credentials, personal information, health data, historical integration, and public portfolio assets are handled.

---

## Solution scope

The public solution covers:

- employee master data;
- disability and rehabilitation attributes;
- current absenteeism and medical certificates;
- detailed medical-leave records;
- legacy absenteeism history;
- organizational perimeter mapping;
- occupational-accident records;
- employee and health dimensions;
- absence and accident facts;
- Power BI analysis and DAX measures.

---

## Value delivered

The solution demonstrates how fragmented operational records can become a consistent analytical product through:

- automated extraction patterns;
- schema normalization;
- historical integration;
- standardized duration calculations;
- deterministic analytical keys;
- reusable dimensions and facts;
- incremental Delta loading;
- centralized DAX measures;
- interactive Power BI analysis;
- privacy-aware public documentation.

---

## Public portfolio adaptation

The repository is not a copy of a production environment. It is a sanitized technical representation.

Public assets use:

- synthetic employee names and identifiers;
- generic units, areas, roles, and perimeters;
- synthetic health classifications;
- mock operational values;
- generic catalog and schema names;
- secret references rather than credentials;
- anonymized screenshots and diagrams.

This separation preserves the technical narrative without exposing confidential information.

---

## Success criteria

The public project is considered complete when:

- all source and curated layers are documented;
- sample datasets preserve relationships without real data;
- the mock PBIX refreshes successfully;
- original monitored health classifications are absent from public assets;
- measures return valid results in synthetic contexts;
- architecture and orchestration are understandable without production access;
- privacy and limitations are clearly disclosed.
