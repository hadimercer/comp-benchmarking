"""
pipeline/csv_ingestion.py

CSV ingestion pipeline for the TechNova Compensation Benchmarking Dashboard.

Reads technova_employees.csv and technova_job_grades.csv, validates both,
upserts each into Supabase PostgreSQL, and writes a run record to
pipeline_run_log for auditing.

Usage:
    python -m pipeline.csv_ingestion
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# ENVIRONMENT & LOGGING SETUP
# ---------------------------------------------------------------------------

# Load all variables from .env into os.environ before anything else reads them.
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
LOG_DIR  = Path(os.getenv("LOG_DIR",  "./logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"csv_ingestion_{_ts}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    """
    Open a psycopg2 connection using credentials from environment variables.
    Supabase requires SSL, so sslmode='require' is set explicitly.
    Credentials are never hardcoded here.
    """
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode="require",
        connect_timeout=15,
    )


# ---------------------------------------------------------------------------
# VALIDATION — EMPLOYEES
# ---------------------------------------------------------------------------

# All columns expected in technova_employees.csv.
EMPLOYEE_REQUIRED_COLS = [
    "employee_id", "first_name", "last_name", "gender", "hire_date",
    "job_family", "role_title", "job_level", "department", "office_location",
    "msa_name", "msa_code", "annual_base_salary", "salary_currency",
    "data_as_of_date",
]

# These columns must not contain any null / blank values.
EMPLOYEE_NOT_NULL_COLS = [
    "employee_id", "job_family", "role_title", "job_level",
    "office_location", "annual_base_salary",
]


def validate_employees(df: pd.DataFrame) -> list[str]:
    """
    Run all data-quality checks on the employees DataFrame.

    Returns a list of human-readable error strings.
    An empty list means the file passed all checks.
    """
    errors: list[str] = []

    # 1. Confirm every expected column is present.
    missing = [c for c in EMPLOYEE_REQUIRED_COLS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors  # Cannot safely run further checks without the right schema.

    # 2. Check for nulls in critical columns.
    for col in EMPLOYEE_NOT_NULL_COLS:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            errors.append(f"'{col}': {null_count} null value(s)")

    # 3. annual_base_salary must be numeric and strictly positive.
    salary = pd.to_numeric(df["annual_base_salary"], errors="coerce")
    non_numeric = salary.isnull().sum()
    if non_numeric > 0:
        errors.append(f"'annual_base_salary': {non_numeric} non-numeric value(s)")
    # Only count non-positive among rows that were successfully parsed.
    non_positive = (salary[salary.notna()] <= 0).sum()
    if non_positive > 0:
        errors.append(f"'annual_base_salary': {non_positive} non-positive value(s)")

    return errors


def print_employee_validation_summary(df: pd.DataFrame, errors: list[str]) -> None:
    """Log a concise validation report before deciding to accept or reject."""
    log.info("  Validation summary:")
    log.info(f"    Total records     : {len(df):,}")
    log.info(f"    Null employee_id  : {df['employee_id'].isnull().sum()}")
    log.info(f"    Null salary       : {df['annual_base_salary'].isnull().sum()}")
    if errors:
        log.error("  VALIDATION FAILED — issues found:")
        for e in errors:
            log.error(f"    • {e}")
    else:
        log.info("  All checks passed.")


# ---------------------------------------------------------------------------
# VALIDATION — JOB GRADES
# ---------------------------------------------------------------------------

GRADES_REQUIRED_COLS = [
    "grade_code", "job_family", "role_title", "job_level",
    "band_minimum", "band_midpoint", "band_maximum",
    "salary_currency", "geo_scope", "below_market_flag",
    "effective_date", "last_reviewed_date",
]

GRADES_NOT_NULL_COLS = [
    "grade_code", "job_family", "role_title", "job_level",
    "band_minimum", "band_midpoint", "band_maximum",
]


def validate_job_grades(df: pd.DataFrame) -> list[str]:
    """
    Run all data-quality checks on the job grades DataFrame.

    Returns a list of human-readable error strings.
    An empty list means the file passed all checks.
    """
    errors: list[str] = []

    # 1. Confirm every expected column is present.
    missing = [c for c in GRADES_REQUIRED_COLS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors

    # 2. Check for nulls in critical columns.
    for col in GRADES_NOT_NULL_COLS:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            errors.append(f"'{col}': {null_count} null value(s)")

    # 3. Coerce band columns to numeric and check for non-numeric values.
    band_min = pd.to_numeric(df["band_minimum"],  errors="coerce")
    band_mid = pd.to_numeric(df["band_midpoint"], errors="coerce")
    band_max = pd.to_numeric(df["band_maximum"],  errors="coerce")

    for col, series in [("band_minimum", band_min), ("band_midpoint", band_mid),
                        ("band_maximum", band_max)]:
        bad = series.isnull().sum()
        if bad > 0:
            errors.append(f"'{col}': {bad} non-numeric value(s)")

    # 4. Check band ordering: minimum < midpoint < maximum.
    #    Only evaluate rows where all three values are valid numbers so that
    #    null-related errors above don't cascade into spurious ordering failures.
    valid = band_min.notna() & band_mid.notna() & band_max.notna()
    if valid.any():
        bad_min_mid = (band_min[valid] >= band_mid[valid]).sum()
        if bad_min_mid > 0:
            errors.append(
                f"{bad_min_mid} row(s) have band_minimum >= band_midpoint"
            )
        bad_mid_max = (band_mid[valid] >= band_max[valid]).sum()
        if bad_mid_max > 0:
            errors.append(
                f"{bad_mid_max} row(s) have band_midpoint >= band_maximum"
            )

    return errors


def print_grades_validation_summary(df: pd.DataFrame, errors: list[str]) -> None:
    """Log a concise validation report for job grades."""
    log.info("  Validation summary:")
    log.info(f"    Total records : {len(df):,}")
    log.info(f"    Null grade_code: {df['grade_code'].isnull().sum()}")
    if errors:
        log.error("  VALIDATION FAILED — issues found:")
        for e in errors:
            log.error(f"    • {e}")
    else:
        log.info("  All checks passed.")


# ---------------------------------------------------------------------------
# PIPELINE RUN LOGGER
# ---------------------------------------------------------------------------

def log_pipeline_run(
    conn: psycopg2.extensions.connection,
    pipeline_type: str,
    status: str,
    records_requested: int,
    records_received: int,
    records_written: int,
    run_duration_seconds: float,
    error_message: str | None = None,
) -> None:
    """
    Insert one audit row into pipeline_run_log.

    discrepancy_flag is set automatically when records_received != records_written.
    This function commits its own transaction so a prior rollback does not
    prevent the failure record from being saved.
    """
    discrepancy_flag = records_received != records_written

    sql = """
        INSERT INTO pipeline_run_log (
            pipeline_type,
            status,
            records_requested,
            records_received,
            records_written,
            discrepancy_flag,
            error_message,
            run_duration_seconds,
            run_timestamp
        ) VALUES (
            %(pipeline_type)s,
            %(status)s,
            %(records_requested)s,
            %(records_received)s,
            %(records_written)s,
            %(discrepancy_flag)s,
            %(error_message)s,
            %(run_duration_seconds)s,
            %(run_timestamp)s
        )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {
                "pipeline_type":        pipeline_type,
                "status":               status,
                "records_requested":    records_requested,
                "records_received":     records_received,
                "records_written":      records_written,
                "discrepancy_flag":     discrepancy_flag,
                "error_message":        error_message,
                "run_duration_seconds": round(run_duration_seconds, 3),
                "run_timestamp":        datetime.now(timezone.utc),
            })
        conn.commit()
    except Exception as log_exc:
        # Logging the run should never crash the script — warn and move on.
        log.warning(f"Could not write to pipeline_run_log: {log_exc}")
        conn.rollback()


# ---------------------------------------------------------------------------
# INGEST — EMPLOYEES
# ---------------------------------------------------------------------------

# SQL upsert: insert or update all fields if employee_id already exists.
# This makes the script safely re-runnable without creating duplicate rows.
EMPLOYEE_UPSERT_SQL = """
    INSERT INTO employees (
        employee_id, first_name, last_name, gender, hire_date,
        job_family, role_title, job_level, department, office_location,
        msa_name, msa_code, annual_base_salary, salary_currency,
        data_as_of_date
    ) VALUES (
        %(employee_id)s, %(first_name)s, %(last_name)s, %(gender)s,
        %(hire_date)s, %(job_family)s, %(role_title)s, %(job_level)s,
        %(department)s, %(office_location)s, %(msa_name)s, %(msa_code)s,
        %(annual_base_salary)s, %(salary_currency)s, %(data_as_of_date)s
    )
    ON CONFLICT (employee_id) DO UPDATE SET
        first_name         = EXCLUDED.first_name,
        last_name          = EXCLUDED.last_name,
        gender             = EXCLUDED.gender,
        hire_date          = EXCLUDED.hire_date,
        job_family         = EXCLUDED.job_family,
        role_title         = EXCLUDED.role_title,
        job_level          = EXCLUDED.job_level,
        department         = EXCLUDED.department,
        office_location    = EXCLUDED.office_location,
        msa_name           = EXCLUDED.msa_name,
        msa_code           = EXCLUDED.msa_code,
        annual_base_salary = EXCLUDED.annual_base_salary,
        salary_currency    = EXCLUDED.salary_currency,
        data_as_of_date    = EXCLUDED.data_as_of_date
"""


def ingest_employees(conn: psycopg2.extensions.connection) -> dict:
    """
    Validate technova_employees.csv and upsert all rows into the employees table.

    Returns a result dict consumed by main() for the final summary.
    Keys: status ("SUCCESS" | "FAILED"), written (int), requested (int),
          duration (float), error (str, only on failure).
    """
    pipeline_type = "CSV_EMPLOYEES"
    start = time.perf_counter()
    csv_path = DATA_DIR / "technova_employees.csv"

    log.info("=" * 60)
    log.info(f"Pipeline : {pipeline_type}")
    log.info(f"Source   : {csv_path}")

    # --- Read CSV ---
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        msg = f"File not found: {csv_path}"
        log.error(msg)
        duration = time.perf_counter() - start
        log_pipeline_run(conn, pipeline_type, "FAILED", 0, 0, 0, duration, msg)
        return {"status": "FAILED", "error": msg, "written": 0,
                "requested": 0, "duration": duration}

    records_requested = len(df)
    log.info(f"Records read from CSV: {records_requested:,}")

    # --- Validate (reject entire file if any check fails) ---
    errors = validate_employees(df)
    print_employee_validation_summary(df, errors)
    if errors:
        msg = "Validation failed: " + " | ".join(errors)
        duration = time.perf_counter() - start
        log_pipeline_run(conn, pipeline_type, "FAILED",
                         records_requested, records_requested, 0, duration, msg)
        return {"status": "FAILED", "error": msg, "written": 0,
                "requested": records_requested, "duration": duration}

    # --- Type coercions before insert ---
    df["annual_base_salary"] = pd.to_numeric(df["annual_base_salary"])
    # msa_code is numeric in the data; use Int64 (nullable) to handle any blanks.
    df["msa_code"] = pd.to_numeric(df["msa_code"], errors="coerce")

    # --- Upsert rows ---
    records_written = 0
    try:
        with conn.cursor() as cur:
            for record in df.to_dict(orient="records"):
                # Replace pandas NaN / NaT with None so psycopg2 sends SQL NULL.
                clean = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                cur.execute(EMPLOYEE_UPSERT_SQL, clean)
                records_written += 1
        conn.commit()
        log.info(f"Records written to DB: {records_written:,}")

    except Exception as exc:
        conn.rollback()
        msg = f"DB insert failed after {records_written} rows: {exc}"
        log.error(msg)
        duration = time.perf_counter() - start
        log_pipeline_run(conn, pipeline_type, "FAILED",
                         records_requested, records_requested,
                         records_written, duration, msg)
        return {"status": "FAILED", "error": msg, "written": records_written,
                "requested": records_requested, "duration": duration}

    duration = time.perf_counter() - start
    log.info(f"Duration : {duration:.2f}s")
    log_pipeline_run(conn, pipeline_type, "SUCCESS",
                     records_requested, records_requested,
                     records_written, duration)
    return {"status": "SUCCESS", "written": records_written,
            "requested": records_requested, "duration": duration}


# ---------------------------------------------------------------------------
# INGEST — JOB GRADES
# ---------------------------------------------------------------------------

GRADES_UPSERT_SQL = """
    INSERT INTO internal_job_grades (
        grade_code, job_family, role_title, job_level,
        band_minimum, band_midpoint, band_maximum,
        salary_currency, geo_scope, below_market_flag,
        effective_date, last_reviewed_date
    ) VALUES (
        %(grade_code)s, %(job_family)s, %(role_title)s, %(job_level)s,
        %(band_minimum)s, %(band_midpoint)s, %(band_maximum)s,
        %(salary_currency)s, %(geo_scope)s, %(below_market_flag)s,
        %(effective_date)s, %(last_reviewed_date)s
    )
    ON CONFLICT (grade_code) DO UPDATE SET
        job_family         = EXCLUDED.job_family,
        role_title         = EXCLUDED.role_title,
        job_level          = EXCLUDED.job_level,
        band_minimum       = EXCLUDED.band_minimum,
        band_midpoint      = EXCLUDED.band_midpoint,
        band_maximum       = EXCLUDED.band_maximum,
        salary_currency    = EXCLUDED.salary_currency,
        geo_scope          = EXCLUDED.geo_scope,
        below_market_flag  = EXCLUDED.below_market_flag,
        effective_date     = EXCLUDED.effective_date,
        last_reviewed_date = EXCLUDED.last_reviewed_date
"""


def ingest_job_grades(conn: psycopg2.extensions.connection) -> dict:
    """
    Validate technova_job_grades.csv and upsert all rows into internal_job_grades.

    Returns a result dict consumed by main() for the final summary.
    """
    pipeline_type = "CSV_GRADES"
    start = time.perf_counter()
    csv_path = DATA_DIR / "technova_job_grades.csv"

    log.info("=" * 60)
    log.info(f"Pipeline : {pipeline_type}")
    log.info(f"Source   : {csv_path}")

    # --- Read CSV ---
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        msg = f"File not found: {csv_path}"
        log.error(msg)
        duration = time.perf_counter() - start
        log_pipeline_run(conn, pipeline_type, "FAILED", 0, 0, 0, duration, msg)
        return {"status": "FAILED", "error": msg, "written": 0,
                "requested": 0, "duration": duration}

    records_requested = len(df)
    log.info(f"Records read from CSV: {records_requested:,}")

    # --- Validate (reject entire file if any check fails) ---
    errors = validate_job_grades(df)
    print_grades_validation_summary(df, errors)
    if errors:
        msg = "Validation failed: " + " | ".join(errors)
        duration = time.perf_counter() - start
        log_pipeline_run(conn, pipeline_type, "FAILED",
                         records_requested, records_requested, 0, duration, msg)
        return {"status": "FAILED", "error": msg, "written": 0,
                "requested": records_requested, "duration": duration}

    # --- Type coercions before insert ---
    for col in ["band_minimum", "band_midpoint", "band_maximum"]:
        df[col] = pd.to_numeric(df[col])

    # --- Upsert rows ---
    records_written = 0
    try:
        with conn.cursor() as cur:
            for record in df.to_dict(orient="records"):
                clean = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                cur.execute(GRADES_UPSERT_SQL, clean)
                records_written += 1
        conn.commit()
        log.info(f"Records written to DB: {records_written:,}")

    except Exception as exc:
        conn.rollback()
        msg = f"DB insert failed after {records_written} rows: {exc}"
        log.error(msg)
        duration = time.perf_counter() - start
        log_pipeline_run(conn, pipeline_type, "FAILED",
                         records_requested, records_requested,
                         records_written, duration, msg)
        return {"status": "FAILED", "error": msg, "written": records_written,
                "requested": records_requested, "duration": duration}

    duration = time.perf_counter() - start
    log.info(f"Duration : {duration:.2f}s")
    log_pipeline_run(conn, pipeline_type, "SUCCESS",
                     records_requested, records_requested,
                     records_written, duration)
    return {"status": "SUCCESS", "written": records_written,
            "requested": records_requested, "duration": duration}


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    overall_start = time.perf_counter()

    log.info("=" * 60)
    log.info("TechNova — CSV Ingestion Pipeline")
    log.info(f"Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # Open a single connection shared across both ingestion jobs.
    try:
        conn = get_connection()
        log.info("Database connection established.")
    except Exception as exc:
        log.error(f"Cannot connect to database: {exc}")
        sys.exit(1)

    results: dict[str, dict] = {}

    try:
        results["employees"] = ingest_employees(conn)
        results["grades"]    = ingest_job_grades(conn)
    finally:
        conn.close()
        log.info("Database connection closed.")

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    total_duration = time.perf_counter() - overall_start

    log.info("")
    log.info("=" * 60)
    log.info("INGESTION SUMMARY")
    log.info("=" * 60)

    labels = [
        ("Employees  (technova_employees.csv)",  "employees"),
        ("Job Grades (technova_job_grades.csv)",  "grades"),
    ]

    any_failed = False
    for label, key in labels:
        r = results.get(key, {})
        status    = r.get("status",    "UNKNOWN")
        written   = r.get("written",   0)
        requested = r.get("requested", 0)
        dur       = r.get("duration",  0.0)

        log.info(f"  {label}")
        log.info(f"    Status  : {status}")

        if status == "SUCCESS":
            log.info(f"    Loaded  : {written:,} / {requested:,} records")
            if written != requested:
                log.warning(f"    *** DISCREPANCY: {requested - written} record(s) not written ***")
                any_failed = True
        else:
            log.error(f"    Error   : {r.get('error', 'Unknown error')}")
            any_failed = True

        log.info(f"    Runtime : {dur:.2f}s")

    log.info("")
    log.info(f"  Total runtime : {total_duration:.2f}s")
    log.info("=" * 60)

    # Non-zero exit code signals failure to any orchestrator or CI system.
    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
