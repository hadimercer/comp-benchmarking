# TechNova Compensation & Market Benchmarking Dashboard

**Portfolio Project S2 | Hadi Mercer | BA Portfolio 2026**

A full-stack data pipeline and interactive dashboard that pulls real wage data from the Bureau of Labor Statistics, stores it alongside internal job grade data in PostgreSQL, and surfaces compensation benchmarking gaps and pay equity flags through a live Streamlit web application.

> TechNova is a realistic fictional healthcare technology company created for portfolio demonstration. All employee data, salary figures, and organizational details are synthetic.

---

## Live Demo

🔗 **Dashboard**: [Coming — deploying to Streamlit Community Cloud]
📁 **Portfolio Hub**: [hadimercer.github.io](https://hadimercer.github.io)

---

## What This Project Demonstrates

| Capability | Evidence |
|---|---|
| Business Analysis | BABOK v3-aligned FRD with MoSCoW prioritization, stakeholder map, traceability matrix |
| Data Engineering | Python pipeline: API ingestion, CSV validation, PostgreSQL loading, audit logging |
| API Integration | BLS OEWS REST API — 600 series/run, batched requests, error handling |
| Database Design | PostgreSQL schema with 8 tables, FK constraints, indexes, unique constraints |
| Data Modeling | Job-to-SOC crosswalk with match quality metadata across 38 role mappings |
| Visualization | Streamlit dashboard: benchmarking view, below-market flags, pay equity module |
| Documentation | OpenAPI/Swagger spec, UML sequence diagram, data dictionary |
| Security | Environment variable credential management, no hardcoded secrets |

---

## Project Architecture

```
BLS Public API (OEWS)
        │
        ▼
pipeline/bls_pipeline.py
  ├── Reads SOC codes from DB
  ├── Builds 600 series IDs (5 MSAs × 20 SOC codes × 6 data types)
  ├── Batches API calls (50/request, 0.5s delay)
  └── Loads → bls_wage_data table
        │
data/ CSVs (employees + job grades)
        │
        ▼
pipeline/csv_ingestion.py
  ├── Schema validation (rejects entire file on failure)
  ├── Type coercion + null handling
  └── Upserts → employees + internal_job_grades tables
        │
pipeline/seed_reference_data.py
  └── Seeds → soc_code_reference + job_soc_crosswalk tables
        │
        ▼
PostgreSQL (Supabase)
  ├── employees (800 rows)
  ├── internal_job_grades (150 rows)
  ├── bls_wage_data (100 rows, 2024 OEWS)
  ├── soc_code_reference (20 SOC codes)
  ├── job_soc_crosswalk (38 role mappings)
  ├── pipeline_run_log (audit trail)
  ├── crosswalk_change_log (controlled artifact history)
  └── job_families (lookup)
        │
        ▼
Streamlit Dashboard (app.py)
  ├── Page 1: Benchmarking View (filter → market range)
  ├── Page 2: Below-Market Flags
  └── Page 3: Pay Equity Module (gender gaps)
```

---

## Repository Structure

```
comp-benchmarking/
├── data/
│   ├── technova_employees.csv      # 800 synthetic employees
│   └── technova_job_grades.csv     # 150 job grade band definitions
├── docs/
│   ├── TechNova_FRD_COMP001.docx   # BABOK-aligned FRD
│   ├── TechNova_SOC_Crosswalk_XWALK001.xlsx  # SOC mapping artifact
│   ├── openapi_bls_integration.yaml  # OpenAPI/Swagger spec (coming)
│   └── uml_sequence_diagram.png      # Data flow diagram (coming)
├── logs/                           # Pipeline run logs (git-ignored)
├── pipeline/
│   ├── __init__.py
│   ├── csv_ingestion.py            # CSV → PostgreSQL loader
│   ├── seed_reference_data.py      # SOC codes + crosswalk seeder
│   └── bls_pipeline.py             # BLS API → PostgreSQL pipeline
├── app.py                          # Streamlit dashboard (coming)
├── .env                            # Credentials (git-ignored)
├── .gitignore
├── CLAUDE.md                       # Project memory for AI coding sessions
├── README.md                       # This file
└── requirements.txt                # Python dependencies (coming)
```

---

## Setup Instructions

### Prerequisites
- Python 3.9+
- A free [Supabase](https://supabase.com) account
- A free [BLS API registration key](https://data.bls.gov/registrationEngine)

### 1. Clone the repository
```bash
git clone https://github.com/hadimercer/comp-benchmarking.git
cd comp-benchmarking
```

### 2. Install dependencies
```bash
python -m pip install streamlit psycopg2-binary python-dotenv pandas requests plotly
```

### 3. Set up the database
Create a new Supabase project, then paste the contents of `docs/schema.sql` into the Supabase SQL Editor and run it. This creates all 8 tables.

### 4. Configure environment variables
Copy `.env.example` to `.env` and fill in your values:
```
DB_HOST=aws-1-us-east-1.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.[your-project-ref]
DB_PASSWORD=[your-supabase-password]
BLS_API_BASE_URL=https://api.bls.gov/publicAPI/v2
BLS_REGISTRATION_KEY=[your-bls-key]
BLS_SURVEY_YEAR=2024
DATA_DIR=./data
LOG_DIR=./logs
```

### 5. Run the pipelines in order
```bash
# Load internal data
python -m pipeline.csv_ingestion

# Seed SOC reference data and crosswalk
python -m pipeline.seed_reference_data

# Pull BLS wage data
python -m pipeline.bls_pipeline
```

### 6. Launch the dashboard
```bash
python -m streamlit run app.py
```

---

## Data Pipeline Detail

### BLS OEWS Series ID Format
The BLS OEWS API uses a 25-character series identifier:

```
O  E  U  M  0  0  1  9  7  4  0  0  0  0  0  0  0  1  5  1  2  5  2  0  3
1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25
|prefix| adj|type|----area code----|----industry----|---occupation---|dtype|
  OE    U    M    0019740            000000            151252          03
```

| Position | Value | Meaning |
|---|---|---|
| 1-2 | `OE` | OEWS prefix |
| 3 | `U` | Unadjusted (seasonal) |
| 4 | `M` | MSA area type |
| 5-11 | `0019740` | Denver MSA code, zero-padded to 7 digits |
| 12-17 | `000000` | All industries |
| 18-23 | `151252` | SOC code digits, no dash, zero-padded to 6 |
| 24-25 | `03` | Annual mean wage |

**Data type codes**: `03`=mean, `11`=P10, `12`=P25, `13`=P50, `14`=P75, `15`=P90

### The 5 TechNova MSAs
Austin TX (12420) · New York NY (35620) · San Francisco CA (41860) · Washington DC (47900) · Denver CO (19740)

### The 20 SOC Codes
Covers all TechNova job families: Software Engineering, Data & Analytics, Product Management, Clinical Informatics/Health IT, UX/Design, DevOps/Platform/SRE, IT/Systems Administration, Sales & Account Management, Corporate.

---

## Functional Requirements Coverage

| ID | Requirement | Status |
|---|---|---|
| FR-01 | BLS API pull — wage percentiles + MSA filter | ✅ Done |
| FR-02 | Quarterly manual trigger, documented run procedure | ✅ Done |
| FR-03 | Pipeline run log — timestamp + record count + status | ✅ Done |
| FR-04 | Schema change + HTTP error detection | ✅ Done |
| FR-05 | CSV flat file load to PostgreSQL | ✅ Done |
| FR-06 | Schema validation before write | ✅ Done |
| FR-07 | Documented CSV template | ✅ Done |
| FR-08 | Job-to-SOC crosswalk table | ✅ Done |
| FR-09 | MSA-level geographic wage storage | ✅ Done |
| FR-10 | Full data dictionary | ✅ Done (Excel artifact) |
| FR-11 | Dashboard: filter → market range | 🔲 In progress |
| FR-12 | Dashboard: internal vs market side-by-side | 🔲 In progress |
| FR-13 | Dashboard: percentile toggle P25/P50/P75 | 🔲 In progress |
| FR-14 | Dashboard: below-market flags view | 🔲 In progress |
| FR-15 | Dashboard: pay equity gap flags (gender) | 🔲 In progress |
| FR-16 | Dashboard: data freshness indicator | 🔲 In progress |
| FR-17 | OpenAPI/Swagger spec for BLS integration | 🔲 Queued |
| FR-18 | UML sequence diagram — end-to-end data flow | 🔲 Queued |

---

## Key Design Decisions

**Streamlit over Power BI**: Power BI Service requires a Pro license ($10/mo) for public report sharing — which creates an access wall for portfolio reviewers. Streamlit Community Cloud is free, produces a public URL instantly, and is Python-native.

**Session pooler over direct Supabase connection**: The direct `db.[ref].supabase.co` host failed DNS resolution on Windows. Supabase's session pooler (`aws-1-us-east-1.pooler.supabase.com`) works reliably and supports full transaction semantics needed for our rollback logic.

**BLS OEWS only (no commercial surveys)**: Radford, Mercer, and WTW data requires paid licenses. BLS OEWS is public, annually refreshed, and sufficient for portfolio demonstration. SOC-level aggregation limitations are documented in the crosswalk and surfaced in the dashboard.

**Fail loudly on validation**: The pipeline rejects the entire CSV file if any validation rule fails — no silent partial loads. This is a non-negotiable data quality standard for a compensation system where decisions are made against the data.

**SRE mapped to 15-1252 (Software Developers), not 15-1244 (Network/SysAdmin)**: SRE roles are intentionally split from DevOps because SRE work is software-heavy by task definition. This is a documented crosswalk decision (XW-021).

---

## Portfolio Context

This is **Smaller Project 2 (S2)** of a 6-project BA portfolio:

| # | Project | Focus |
|---|---|---|
| F1 | Operational Process Intelligence | What-if simulation |
| F2 | BA Co-Pilot | AI-powered artifact generation |
| S1 | HR Process Automation Hub | Workflow automation |
| **S2** | **Comp & Benchmarking Dashboard** | **← This project** |
| S3 | Program Portfolio Dashboard | RAG status health view |
| S4 | Sentiment & Text Analytics | NLP analysis |

---

## Contact

**Hadi Mercer**
LinkedIn: [linkedin.com/in/hadimercer](https://linkedin.com/in/hadimercer)
GitHub: [github.com/hadimercer](https://github.com/hadimercer)
