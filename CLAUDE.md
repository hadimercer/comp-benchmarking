# CLAUDE.md — TechNova Compensation & Market Benchmarking Dashboard
# Project memory for Claude Code. Read this at the start of every session.
# Last updated: 2026-02-23

---

## WHO I AM BUILDING THIS FOR

**Hadi Mercer** — BA professional building a 6-project portfolio.
- GitHub: `hadimercer` → `github.com/hadimercer`
- Portfolio hub: `hadimercer.github.io`
- This is **Smaller Project 2 (S2)** of the portfolio
- Local project path: `C:\Users\belgh\projects\hadimercer-portfolio\comp-benchmarking`

---

## WHAT THIS PROJECT IS

A compensation benchmarking dashboard for **TechNova** — a fictional healthcare technology company (~800 employees, 5 US office locations). The system:

1. Pulls BLS OEWS wage data via API (Python pipeline)
2. Stores it alongside internal job grade data in PostgreSQL (Supabase)
3. Serves a public-facing Streamlit dashboard showing market benchmarking gaps
4. Includes a pay equity analysis module flagging gender pay gaps

**Portfolio purpose**: Demonstrates end-to-end BA + technical capability — BABOK-aligned FRD, OpenAPI/Swagger spec, data pipeline, PostgreSQL schema, Streamlit dashboard, deployed publicly on Streamlit Cloud.

---

## TECH STACK

| Layer | Technology |
|---|---|
| Language | Python 3.14 |
| Database | PostgreSQL via Supabase (free tier) |
| Dashboard | Streamlit (deployed to Streamlit Community Cloud) |
| API integration | BLS OEWS public API (registered key) |
| Data validation | pandas |
| DB driver | psycopg2-binary |
| Config | python-dotenv (.env file) |
| Version control | Git → GitHub (`hadimercer`) |

**Python command**: Always use `python` (not `python3`) and `python -m pip` for package installs.

---

## DATABASE — SUPABASE

**Project name**: `hadimercer-comp-dashboard`
**Host**: `aws-1-us-east-1.pooler.supabase.com` (Session pooler — NOT the direct db. host)
**Port**: `5432`
**DB name**: `postgres`
**User**: `postgres.jemgurbyjqlegumcqvrf` ← pooler requires project ref appended to username
**SSL**: required

**All credentials are in `.env` — never hardcode them.**

---

## DATABASE SCHEMA — 8 TABLES

### 1. `employees`
Holds 800 synthetic TechNova employee records.
```
employee_id (PK), first_name, last_name, gender, hire_date,
job_family, role_title, job_level, department, office_location,
msa_name, msa_code, annual_base_salary, salary_currency,
data_as_of_date, created_at, updated_at
```
**Status**: ✅ 800 rows loaded

### 2. `internal_job_grades`
Holds 150 job grade band definitions (min/mid/max salary by role+level).
```
grade_id (PK), grade_code (UNIQUE), job_family, role_title,
job_level, band_minimum, band_midpoint, band_maximum,
salary_currency, geo_scope, below_market_flag, effective_date,
last_reviewed_date, created_at, updated_at
```
**Status**: ✅ 150 rows loaded
**Note**: `below_market_flag = 'YES'` on 48 rows (Corporate, IT/Systems Admin, UX/Design) — intentional for FR-14 flagging demo

### 3. `bls_wage_data`
Holds BLS OEWS wage data pulled from the API.
```
wage_id (PK, SERIAL), soc_code (FK), soc_title, msa_code, msa_name,
reference_year, annual_mean, pct_10, pct_25, pct_50, pct_75, pct_90,
total_employment, data_source, pipeline_run_id, created_at
```
**Unique constraint**: `(soc_code, msa_code, reference_year)` — required for ON CONFLICT
**Status**: ✅ 100 rows loaded (2024 BLS OEWS data, 5 MSAs × 20 SOC codes)

### 4. `soc_code_reference`
20 BLS Standard Occupational Classification codes used by TechNova.
```
soc_code (PK, CHAR(7)), soc_title, soc_major_group,
used_by_families, bls_oews_url, created_at
```
**Status**: ✅ 20 rows seeded

### 5. `job_soc_crosswalk`
38 mappings from TechNova role titles to BLS SOC codes.
```
crosswalk_id (PK), job_family, technova_role_title,
job_level_applicability, soc_code (FK), soc_title,
match_quality, match_notes, pipeline_query_flag,
naics_filter_recommended, last_reviewed_date,
created_at, updated_at, updated_by
```
**match_quality values**: EXACT / CLOSE / BEST_AVAILABLE / KNOWN_GAP
**Status**: ✅ 38 rows seeded

### 6. `pipeline_run_log`
Audit log for every pipeline execution.
```
run_id (PK, SERIAL), run_timestamp, pipeline_type, status,
records_requested, records_received, records_written,
discrepancy_flag, error_message, run_duration_seconds, triggered_by
```
**Status**: ✅ Active — every pipeline run writes here

### 7. `crosswalk_change_log`
Controlled artifact change history per NFR-01c.
```
change_id (PK, SERIAL), change_date, crosswalk_id_affected,
change_type, field_changed, previous_value, new_value_reason,
author, created_at
```
**Status**: ✅ Seeded with initial creation entry

### 8. `job_families`
Job family lookup table (currently unpopulated — used as reference).
```
job_family_id (PK, SERIAL), family_name (UNIQUE), family_group, created_at
```
**Status**: Empty — populated by future migration if needed

---

## THE 9 JOB FAMILIES

1. Software Engineering
2. Data & Analytics
3. Product Management
4. Clinical Informatics / Health IT
5. UX / Design
6. DevOps / Platform / SRE
7. IT / Systems Administration
8. Sales & Account Management
9. Corporate

**Levels**: L1 (Junior) through L6 (Principal/Staff)

---

## THE 5 OFFICE LOCATIONS (MSAs)

| City | MSA Code | BLS Area Code (7-digit) |
|---|---|---|
| Austin TX | 12420 | 0012420 |
| New York NY | 35620 | 0035620 |
| San Francisco CA | 41860 | 0041860 |
| Washington DC | 47900 | 0047900 |
| Denver CO | 19740 | 0019740 |

---

## PIPELINE FILES

### `pipeline/__init__.py`
Makes pipeline/ a Python package. Run scripts with `python -m pipeline.script_name`.

### `pipeline/csv_ingestion.py`
Loads both CSVs into Supabase.
- Validates before writing (rejects entire file on failure — no silent partial loads)
- Idempotent: ON CONFLICT DO UPDATE
- Run: `python -m pipeline.csv_ingestion`

### `pipeline/seed_reference_data.py`
Seeds `soc_code_reference` (20 rows) and `job_soc_crosswalk` (38 rows).
- Idempotent: ON CONFLICT DO NOTHING
- Run: `python -m pipeline.seed_reference_data`

### `pipeline/bls_pipeline.py`
Pulls BLS OEWS wage data and loads into `bls_wage_data`.
- Reads SOC codes from DB (not hardcoded)
- Builds 600 series IDs: 5 MSAs × 20 SOC codes × 6 data types
- **Series ID format**: `OEUM` + area(7) + `000000` + soc(6) + datatype(2) = 25 chars
  - Example: `OEUM001974000000015125203` = Denver + Software Developers + mean
- Batches 50 series/request (registered key) with 0.5s delay
- Survey year controlled by `BLS_SURVEY_YEAR` env var (default: 2024)
- Run: `python -m pipeline.bls_pipeline`

---

## .ENV VARIABLES

```
DB_HOST=aws-1-us-east-1.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.jemgurbyjqlegumcqvrf
DB_PASSWORD=[secret]
BLS_API_BASE_URL=https://api.bls.gov/publicAPI/v2
BLS_REGISTRATION_KEY=[registered key — raises limit to 500 req/day]
BLS_SURVEY_YEAR=2024
DATA_DIR=./data
LOG_DIR=./logs
```

---

## DATA FILES (in data/)

| File | Rows | Description |
|---|---|---|
| `technova_employees.csv` | 800 | Synthetic employee records |
| `technova_job_grades.csv` | 150 | Job grade band definitions |

---

## INTENTIONAL DATA SIGNALS (for dashboard demo)

### Below-market bands (FR-14 flagging):
- Corporate: bands ~10% below geo-adjusted market
- IT / Systems Administration: bands ~10% below market
- UX / Design: bands ~10% below market

### Gender pay gap (FR-15 equity module):
- Software Engineering + Data & Analytics
- At levels L3, L4, L5
- Female salaries ~6.5% below male equivalent
- Detectable as statistically significant with 309 female employees

---

## BUILD STATUS

| Step | Status |
|---|---|
| FRD (BABOK-aligned requirements doc) | ✅ Complete — `/docs/TechNova_FRD_COMP001.docx` |
| SOC Crosswalk Excel (XWALK-001) | ✅ Complete — `/docs/TechNova_SOC_Crosswalk_XWALK001.xlsx` |
| Supabase schema (8 tables) | ✅ Complete |
| CSV ingestion pipeline | ✅ Complete |
| Reference data seed | ✅ Complete |
| BLS API pipeline | ✅ Complete |
| GitHub repo setup | ⬜ Next |
| Streamlit dashboard | ⬜ Next |
| Deploy to Streamlit Cloud | ⬜ Next |
| OpenAPI/Swagger spec | ⬜ Queued |
| UML sequence diagram | ⬜ Queued |
| CLAUDE.md + README.md | ✅ This file |

---

## NEXT STEPS IN ORDER

1. Push code to GitHub (`hadimercer/comp-benchmarking`)
2. Build Streamlit dashboard (`app.py`)
   - Page 1: Benchmarking view (filter by family/level/location → market range)
   - Page 2: Below-market flags (FR-14)
   - Page 3: Pay equity gaps (FR-15, gender)
   - Data freshness indicator on every page (FR-16)
3. Deploy to Streamlit Community Cloud (public URL)
4. Generate OpenAPI/Swagger spec for BLS API integration
5. Generate UML sequence diagram
6. Build portfolio hub (`hadimercer.github.io`)

---

## KEY DESIGN DECISIONS

- **Session pooler over direct connection**: Direct `db.` host failed DNS on Windows. Pooler at `aws-1-us-east-1.pooler.supabase.com` works reliably.
- **CSV ingestion over HRIS integration**: Internal data via flat file upload — HRIS integration is Phase 2.
- **BLS OEWS only**: No commercial survey data (Radford, Mercer). SOC-level aggregation documented in crosswalk.
- **Streamlit over Power BI**: Power BI Service requires Pro license for public sharing. Streamlit is free, public, and Python-native.
- **Gender-only for pay equity v1**: Race/ethnicity deferred to Phase 2 per CON-07.
- **Fail loudly**: Validation rejects entire file on error — no silent partial loads (NFR-01a).
- **SRE mapped to 15-1252**: Intentionally split from DevOps (15-1244) because SRE work is software-heavy by task definition.

---

## FUNCTIONAL REQUIREMENTS REFERENCE

| ID | Description | Status |
|---|---|---|
| FR-01 | BLS API pull: wage percentiles + MSA filter | ✅ |
| FR-02 | Quarterly manual trigger, no-dev run procedure | ✅ |
| FR-03 | Run log: timestamp + record count + status | ✅ |
| FR-04 | Schema change + HTTP error detection | ✅ |
| FR-05 | CSV flat file load to PostgreSQL | ✅ |
| FR-06 | Schema validation before write | ✅ |
| FR-07 | Documented CSV template with examples | ✅ |
| FR-08 | Job-to-SOC crosswalk table | ✅ |
| FR-09 | MSA-level geographic wage storage | ✅ |
| FR-10 | Full data dictionary | ✅ (Excel artifact) |
| FR-11 | Dashboard filter: family + level + location → range | ⬜ |
| FR-12 | Internal vs. market side-by-side view | ⬜ |
| FR-13 | Percentile toggle (P25/P50/P75) | ⬜ |
| FR-14 | Below-market flags view | ⬜ |
| FR-15 | Pay equity gap flags view (gender, v1) | ⬜ |
| FR-16 | Data freshness indicator | ⬜ |
| FR-17 | OpenAPI/Swagger spec for BLS integration | ⬜ |
| FR-18 | UML sequence diagram: end-to-end data flow | ⬜ |
