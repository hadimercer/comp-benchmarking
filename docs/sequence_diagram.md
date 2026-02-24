# UML Sequence Diagram — End-to-End Data Flow

**Document ID:** FRD-COMP-001 | FR-18  
**Project:** TechNova Compensation & Market Benchmarking Dashboard  
**Prepared by:** Business Analyst — BA Portfolio Project 2  
**Date:** February 2026  
**Traces to:** OBJ-02, OBJ-05

---

## Overview

This diagram illustrates the complete end-to-end data flow of TechNova's
Compensation & Market Benchmarking Dashboard — from a Total Rewards
administrator triggering a pipeline refresh, through BLS API data acquisition,
PostgreSQL storage, and final data consumption by the Streamlit dashboard.

Two distinct flows are shown:
- **Flow A — BLS Pipeline:** External market data ingestion (quarterly)
- **Flow B — CSV Ingestion:** Internal employee and job grade data upload
- **Flow C — Dashboard Query:** Read path from dashboard to database

---

## Participants

| Participant | Type | Description |
|---|---|---|
| Total Rewards Admin | Human Actor | Initiates pipeline runs and CSV uploads |
| Streamlit Dashboard | Frontend | 8-page compensation intelligence UI |
| BLS Pipeline | Python Module | `pipeline/bls_pipeline.py` — API client + transformer + loader |
| CSV Ingestion Module | Python Module | `pipeline/csv_ingestion.py` — validator + loader |
| BLS Public API | External System | `https://api.bls.gov/publicAPI/v2` |
| PostgreSQL (Supabase) | Database | Single source of truth — all market + internal data |
| End User (Recruiter / HRBP) | Human Actor | Consumes dashboard for offer decisions |

---

## Flow A — BLS Market Data Pipeline

```mermaid
sequenceDiagram
    autonumber

    actor Admin as Total Rewards Admin
    participant UI as Streamlit Dashboard
    participant PL as BLS Pipeline<br/>(bls_pipeline.py)
    participant BLS as BLS Public API<br/>(api.bls.gov/v2)
    participant DB as PostgreSQL<br/>(Supabase)

    Admin->>UI: Clicks "Refresh BLS Data Now"<br/>on Data Management page

    UI->>PL: Calls pipeline.bls_pipeline.main()

    PL->>DB: Reads job_soc_crosswalk<br/>to get target SOC codes
    DB-->>PL: Returns SOC codes + pipeline_query_flag = YES

    PL->>DB: Reads employees table<br/>to get MSA codes for office locations
    DB-->>PL: Returns distinct MSA codes

    note over PL: Constructs BLS series IDs<br/>from SOC × MSA × data_type combinations<br/>Batches into groups of 50 (registered key limit)

    loop For each batch of ≤ 50 series IDs
        PL->>BLS: POST /timeseries/data/<br/>{ seriesid[], startyear, endyear, registrationkey }
        BLS-->>PL: HTTP 200 — { status, Results: { series[] } }

        alt status = REQUEST_SUCCEEDED and data present
            note over PL: Parses wage values per series<br/>Maps data_type codes to columns<br/>(annual_mean, pct_25, pct_50, pct_75…)<br/>Converts string values to numeric<br/>Handles suppressed values ("-") as NULL
        else status = REQUEST_FAILED or empty data
            note over PL: Logs warning (FR-04)<br/>Skips batch — no partial write (NFR-01a)
        else HTTP 429 — Rate limit exceeded
            PL->>DB: Writes FAILED entry to pipeline_run_log
            note over PL: Preserves last-known-good dataset<br/>Does not overwrite existing data (NFR-03c)
        end
    end

    PL->>DB: Upserts transformed rows<br/>INTO bls_wage_data<br/>ON CONFLICT (soc_code, msa_code, reference_year, data_type)<br/>DO UPDATE SET wage_value, loaded_at

    PL->>DB: Reconciles records_received vs records_written<br/>Flags discrepancy > 0 (NFR-01b)

    PL->>DB: Writes run summary to pipeline_run_log<br/>{ pipeline_type, status, records_written,<br/>run_duration_seconds, run_timestamp }

    DB-->>UI: pipeline_run_log updated
    UI-->>Admin: Displays SUCCESS / FAILED status<br/>with record count and duration
```

---

## Flow B — CSV Internal Data Ingestion

```mermaid
sequenceDiagram
    autonumber

    actor Admin as Total Rewards Admin
    participant UI as Streamlit Dashboard
    participant CSV as CSV Ingestion Module<br/>(csv_ingestion.py)
    participant DB as PostgreSQL<br/>(Supabase)

    Admin->>UI: Uploads CSV file<br/>(Employee data or Job Grades)

    UI->>CSV: Passes DataFrame to validate_employees()<br/>or validate_job_grades()

    note over CSV: Schema validation (FR-06)<br/>Checks required columns present<br/>Checks data types (numeric salaries, valid dates)<br/>Checks non-null constraints

    alt Validation FAILS
        CSV-->>UI: Returns list of validation errors
        UI-->>Admin: Displays errors — no records written<br/>(NFR-01a: fail loudly, no partial load)
    else Validation PASSES
        UI-->>Admin: Displays "Validation passed — confirm load?"
        Admin->>UI: Clicks "Confirm and Load"

        UI->>CSV: Calls upsert_df(conn, df, UPSERT_SQL)

        loop For each row in validated DataFrame
            CSV->>DB: Executes UPSERT SQL<br/>ON CONFLICT (employee_id / grade_code)<br/>DO UPDATE SET all fields
        end

        CSV->>DB: Writes run summary to pipeline_run_log<br/>{ pipeline_type=CSV_EMPLOYEES or CSV_GRADES,<br/>status, records_written, run_duration_seconds }

        DB-->>UI: Commit confirmed
        UI-->>Admin: Displays records written + duration
    end
```

---

## Flow C — Dashboard Query (Read Path)

```mermaid
sequenceDiagram
    autonumber

    actor User as End User<br/>(Recruiter / HRBP / Total Rewards)
    participant UI as Streamlit Dashboard
    participant DB as PostgreSQL<br/>(Supabase)

    User->>UI: Selects Job Family + Level + Office Location<br/>on Benchmarking View

    UI->>DB: query_df() — SELECT from internal_job_grades<br/>WHERE job_family = ? AND job_level = ?
    DB-->>UI: Returns band_minimum, band_midpoint, band_maximum

    UI->>DB: query_df() — SELECT from job_soc_crosswalk<br/>WHERE job_family = ? AND pipeline_query_flag = YES
    DB-->>UI: Returns soc_code, match_quality, soc_title

    UI->>DB: query_df() — SELECT from employees<br/>WHERE office_location = ? → MSA code
    DB-->>UI: Returns msa_code, msa_name

    UI->>DB: query_df() — SELECT from bls_wage_data<br/>WHERE soc_code = ? AND msa_code = ?<br/>ORDER BY reference_year DESC LIMIT 1
    DB-->>UI: Returns pct_25, pct_50, pct_75, annual_mean

    note over UI: Computes gap $ and gap %<br/>between internal midpoint and BLS P50<br/>Applies below-market flag if midpoint < P25<br/>Renders grouped bar chart + KPI metrics

    UI-->>User: Displays benchmarking view<br/>Internal vs Market — target < 2 minutes (OBJ-01)

    note over UI: All queries cached via @st.cache_data(ttl=300)<br/>Subsequent loads served from cache
```

---

## Error Handling Summary

| Scenario | Pipeline Behaviour | Traces To |
|---|---|---|
| BLS API returns `REQUEST_FAILED` | Log warning, skip batch, no write | FR-04 |
| HTTP 429 — rate limit | Log FAILED, preserve existing data | NFR-03c |
| HTTP 500 — server error | Log FAILED, preserve existing data | NFR-03c |
| CSV schema validation fails | Return errors to UI, zero records written | NFR-01a, FR-06 |
| Records received ≠ records written | Flag discrepancy in run log | NFR-01b |
| DB connection failure | Exception raised, logged, UI shows error | NFR-03a |

---

## Data Store Reference

| Table | Written By | Read By | Purpose |
|---|---|---|---|
| `bls_wage_data` | BLS Pipeline | Dashboard, BLS Pipeline | BLS market wage data by SOC + MSA + year |
| `internal_job_grades` | CSV Ingestion | Dashboard | TechNova salary bands by job family + level |
| `employees` | CSV Ingestion | Dashboard, BLS Pipeline | Employee records with office location + salary |
| `job_soc_crosswalk` | Manual / Admin | BLS Pipeline, Dashboard | SOC code mapping — controlled artifact (NFR-01c) |
| `pipeline_run_log` | BLS Pipeline, CSV Ingestion | Dashboard (sidebar freshness) | Audit log of all pipeline runs |
