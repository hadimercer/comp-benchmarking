"""
pipeline/bls_pipeline.py

Pulls Occupational Employment and Wage Statistics (OEWS) data from the
BLS public API v2 and loads wage benchmarks into the bls_wage_data table.

Scope:
  - 5 TechNova MSAs × 20 SOC codes × 6 wage percentiles = 600 series
  - Series are batched (max 50 per request for registered tier; the public
    tier supports 25 — set BATCH_SIZE accordingly if no key is provided)
  - One row is written to bls_wage_data per (soc_code, msa_code, survey_year),
    with all available wage columns populated in a single upsert.

Assumes bls_wage_data schema:
    soc_code            TEXT NOT NULL
    soc_title           TEXT
    msa_code            TEXT NOT NULL
    msa_name            TEXT
    reference_year      INTEGER NOT NULL
    annual_mean         NUMERIC          -- BLS data type 03
    pct_10              NUMERIC          -- BLS data type 11
    pct_25              NUMERIC          -- BLS data type 12
    pct_50              NUMERIC          -- BLS data type 13  (median)
    pct_75              NUMERIC          -- BLS data type 14
    pct_90              NUMERIC          -- BLS data type 15
    total_employment    NUMERIC
    data_source         TEXT
    pipeline_run_id     INTEGER
    created_at          TIMESTAMPTZ
    UNIQUE (soc_code, msa_code, reference_year)

Usage:
    python -m pipeline.bls_pipeline
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# ENVIRONMENT & LOGGING SETUP
# ---------------------------------------------------------------------------

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "./logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"bls_pipeline_{_ts}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

BLS_API_URL       = os.getenv("BLS_API_BASE_URL", "https://api.bls.gov/publicAPI/v2")
BLS_SERIES_URL    = f"{BLS_API_URL}/timeseries/data/"
BLS_REGKEY        = os.getenv("BLS_REGISTRATION_KEY", "").strip()

# Public tier: 25 series/request.  Registered tier: 50 series/request.
BATCH_SIZE        = 50 if BLS_REGKEY else 25

# Survey year: read from BLS_SURVEY_YEAR env var, or fall back to 2024.
# (BLS OEWS releases in April each year; 2025 data won't be available until April 2026.)
SURVEY_YEAR       = int(os.getenv("BLS_SURVEY_YEAR", "2024"))

# Courtesy delay between API requests (public tier rate limit).
INTER_BATCH_DELAY = 0.5  # seconds

# TechNova MSA codes (5-digit, stored as strings to preserve leading zeros
# in any display context; padded to 7 digits only when building series IDs).
MSA_CODES: dict[str, str] = {
    "Austin TX":        "12420",
    "New York NY":      "35620",
    "San Francisco CA": "41860",
    "Washington DC":    "47900",
    "Denver CO":        "19740",
}

# BLS OEWS data-type codes and their column names in bls_wage_data.
DATA_TYPES: dict[str, str] = {
    "03": "annual_mean",
    "11": "pct_10",
    "12": "pct_25",
    "13": "pct_50",
    "14": "pct_75",
    "15": "pct_90",
}

# OEWS industry code for "all industries, cross-industry".
INDUSTRY_CODE = "000000"


# ---------------------------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection using credentials from environment variables."""
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
    """Insert one audit row into pipeline_run_log."""
    discrepancy_flag = records_received != records_written
    sql = """
        INSERT INTO pipeline_run_log (
            pipeline_type, status,
            records_requested, records_received, records_written,
            discrepancy_flag, error_message,
            run_duration_seconds, run_timestamp
        ) VALUES (
            %(pipeline_type)s, %(status)s,
            %(records_requested)s, %(records_received)s, %(records_written)s,
            %(discrepancy_flag)s, %(error_message)s,
            %(run_duration_seconds)s, %(run_timestamp)s
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
    except Exception as exc:
        log.warning(f"Could not write to pipeline_run_log: {exc}")
        conn.rollback()


# ---------------------------------------------------------------------------
# STEP 1: READ SOC CODES FROM DATABASE
# ---------------------------------------------------------------------------

def fetch_soc_codes(conn: psycopg2.extensions.connection) -> dict[str, str]:
    """
    Return a mapping of {soc_code: soc_title} from soc_code_reference.
    Reading from the DB (not hardcoded) so that future updates to the
    reference table are automatically picked up by this pipeline.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT soc_code, soc_title FROM soc_code_reference ORDER BY soc_code")
        rows = cur.fetchall()
    soc_map = {row[0]: row[1] for row in rows}
    log.info(f"Fetched {len(soc_map)} SOC codes from soc_code_reference.")
    return soc_map


# ---------------------------------------------------------------------------
# STEP 2: BUILD SERIES IDS
# ---------------------------------------------------------------------------

def soc_to_digits(soc_code: str) -> str:
    """
    Convert a SOC code like '15-1252' to its 6-digit numeric form '151252'.
    Standard SOC codes (XX-XXXX) always produce exactly 6 digits.
    """
    return soc_code.replace("-", "").zfill(6)


def msa_to_area_code(msa_code: str) -> str:
    """Zero-pad a 5-digit MSA code to the 7-digit BLS area code."""
    return msa_code.zfill(7)


def build_series_id(msa_code: str, soc_code: str, data_type: str) -> str:
    """
    Construct a BLS OEWS series ID for MSA-level data.

    Positions 1-2  : OE  (prefix)
    Position  3    : U   (seasonal adjustment = unadjusted)
    Position  4    : M   (area type = MSA)
    Positions 5-11 : area code, zero-padded to 7 digits
    Positions 12-17: industry code = 000000 (all industries)
    Positions 18-23: SOC code digits only, no dash, zero-padded to 6 digits
    Positions 24-25: data type code

    Example: OEUM001974000000015125203
             OE + U + M + 0019740 + 000000 + 151252 + 03
             (Denver, Software Developers, mean annual wage)
    """
    return (
        "OEUM"
        + msa_to_area_code(msa_code)
        + INDUSTRY_CODE
        + soc_to_digits(soc_code)
        + data_type
    )


def build_all_series(soc_map: dict[str, str]) -> list[dict]:
    """
    Build a flat list of series descriptor dicts for every combination of
    MSA × SOC code × data type.

    Each dict carries enough metadata to parse the response back into a
    structured row without re-parsing the series ID string.
    """
    series_list = []
    for msa_name, msa_code in MSA_CODES.items():
        for soc_code, soc_title in soc_map.items():
            for dt_code, col_name in DATA_TYPES.items():
                series_list.append({
                    "series_id":  build_series_id(msa_code, soc_code, dt_code),
                    "msa_name":   msa_name,
                    "msa_code":   msa_code,
                    "soc_code":   soc_code,
                    "soc_title":  soc_title,
                    "data_type":  dt_code,
                    "col_name":   col_name,
                })
    return series_list


# ---------------------------------------------------------------------------
# STEP 3: FETCH DATA FROM BLS API
# ---------------------------------------------------------------------------

def fetch_bls_batch(
    series_ids: list[str],
    year: int,
    session: requests.Session,
) -> dict | None:
    """
    POST a single batch of series IDs to the BLS API.

    Returns the parsed JSON response dict, or None on HTTP / network error.
    Unexpected response schemas are logged as warnings but do not crash the
    pipeline; the caller handles missing data gracefully.
    """
    payload: dict = {
        "seriesid":  series_ids,
        "startyear": str(year),
        "endyear":   str(year),
    }
    if BLS_REGKEY:
        payload["registrationkey"] = BLS_REGKEY

    try:
        resp = session.post(
            BLS_SERIES_URL,
            json=payload,
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        log.error(f"HTTP error on batch request: {exc}")
        return None
    except requests.exceptions.RequestException as exc:
        log.error(f"Network error on batch request: {exc}")
        return None

    data = resp.json()

    # Validate top-level response structure.
    if "status" not in data or "Results" not in data:
        log.warning(
            f"Unexpected BLS response schema — missing 'status' or 'Results' key. "
            f"Keys found: {list(data.keys())}"
        )
        return None

    if data["status"] != "REQUEST_SUCCEEDED":
        messages = data.get("message", [])
        log.warning(f"BLS API status: {data['status']}  |  messages: {messages}")
        # Return the payload anyway — partial data may still be present.

    return data


# ---------------------------------------------------------------------------
# STEP 4: PARSE API RESPONSE
# ---------------------------------------------------------------------------

def safe_wage(value_str: str) -> float | None:
    """
    Convert a BLS wage string to a float.
    Returns None for suppressed values ('-', '**') or non-numeric strings.
    """
    v = value_str.strip()
    if v in ("-", "**", "N/A", "NA", ""):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def parse_response(
    response_data: dict,
    series_meta: dict[str, dict],
) -> dict[tuple, dict]:
    """
    Extract wage values from a BLS API response and aggregate them into a
    dict keyed by (soc_code, msa_code, survey_year).

    series_meta maps series_id → descriptor dict (from build_all_series).
    Returns a partial results dict that the caller merges into the master.
    """
    partial: dict[tuple, dict] = {}

    series_list = response_data.get("Results", {}).get("series", [])
    if not isinstance(series_list, list):
        log.warning("Unexpected 'Results.series' structure in BLS response.")
        return partial

    for series in series_list:
        sid = series.get("seriesID", "")
        meta = series_meta.get(sid)
        if meta is None:
            log.warning(f"Received unknown series ID in response: {sid}")
            continue

        data_points = series.get("data", [])
        if not data_points:
            # BLS returns an empty list for MSA/SOC combos with no published data.
            log.debug(f"No data for series {sid} — suppressed or not published.")
            continue

        # We requested a single year; take the first (most recent) data point.
        point = data_points[0]
        year  = int(point.get("year", SURVEY_YEAR))
        wage  = safe_wage(point.get("value", ""))

        if wage is None:
            log.debug(f"Suppressed wage value for series {sid}.")
            continue

        key = (meta["soc_code"], meta["msa_code"], year)
        if key not in partial:
            partial[key] = {
                "soc_code":    meta["soc_code"],
                "soc_title":   meta["soc_title"],
                "msa_code":    meta["msa_code"],
                "msa_name":    meta["msa_name"],
                "survey_year": year,
            }
        partial[key][meta["col_name"]] = wage

    return partial


# ---------------------------------------------------------------------------
# STEP 5: LOAD RESULTS INTO DATABASE
# ---------------------------------------------------------------------------

BLS_UPSERT_SQL = """
    INSERT INTO bls_wage_data (
        soc_code, soc_title, msa_code, msa_name, reference_year,
        annual_mean, pct_10, pct_25, pct_50, pct_75, pct_90,
        data_source, pipeline_run_id
    ) VALUES (
        %(soc_code)s, %(soc_title)s, %(msa_code)s, %(msa_name)s, %(survey_year)s,
        %(annual_mean)s, %(pct_10)s, %(pct_25)s,
        %(pct_50)s, %(pct_75)s, %(pct_90)s,
        %(data_source)s, %(pipeline_run_id)s
    )
    ON CONFLICT (soc_code, msa_code, reference_year) DO NOTHING
"""


def load_wage_rows(
    conn: psycopg2.extensions.connection,
    aggregated: dict[tuple, dict],
) -> int:
    """
    Upsert all aggregated wage rows into bls_wage_data.
    Returns the number of rows actually written (conflicts skipped).
    """
    wage_cols = list(DATA_TYPES.values())  # [annual_mean, pct_10, ..., pct_90]
    written = 0

    try:
        with conn.cursor() as cur:
            for row_data in aggregated.values():
                record = {
                    "soc_code":        row_data["soc_code"],
                    "soc_title":       row_data["soc_title"],
                    "msa_code":        row_data["msa_code"],
                    "msa_name":        row_data["msa_name"],
                    "survey_year":     row_data["survey_year"],
                    "data_source":     "BLS_OEWS",
                    "pipeline_run_id": None,
                }
                # Fill wage columns; use None (SQL NULL) for any that were absent
                # (suppressed or not returned by the API).
                for col in wage_cols:
                    record[col] = row_data.get(col, None)

                cur.execute(BLS_UPSERT_SQL, record)
                if cur.rowcount == 1:
                    written += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return written


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    pipeline_type = "BLS_OEWS"
    overall_start = time.perf_counter()

    log.info("=" * 60)
    log.info("TechNova — BLS OEWS Wage Pipeline")
    log.info(f"Started     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Survey year : {SURVEY_YEAR}")
    log.info(f"Batch size  : {BATCH_SIZE} series/request")
    log.info(f"Reg key     : {'PROVIDED' if BLS_REGKEY else 'NOT SET (public tier)'}")
    log.info("=" * 60)

    # --- Connect to database ---
    try:
        conn = get_connection()
        log.info("Database connection established.")
    except Exception as exc:
        log.error(f"Cannot connect to database: {exc}")
        sys.exit(1)

    status    = "SUCCESS"
    error_msg: str | None = None
    records_written   = 0
    records_received  = 0   # unique (soc, msa, year) combos with at least one wage value
    series_with_data  = 0   # individual series that returned a non-null value
    series_gaps: list[str] = []  # series IDs with no published data

    try:
        # ------------------------------------------------------------------
        # 1. Read SOC codes from the reference table
        # ------------------------------------------------------------------
        soc_map = fetch_soc_codes(conn)
        if not soc_map:
            raise RuntimeError("soc_code_reference is empty — run seed_reference_data first.")

        # ------------------------------------------------------------------
        # 2. Build all 600 series descriptors
        # ------------------------------------------------------------------
        all_series   = build_all_series(soc_map)
        total_series = len(all_series)
        log.info(
            f"Series to request: {total_series} "
            f"({len(MSA_CODES)} MSAs × {len(soc_map)} SOC codes "
            f"× {len(DATA_TYPES)} data types)"
        )

        # Build a lookup map: series_id → descriptor (for response parsing).
        series_meta = {s["series_id"]: s for s in all_series}

        # ------------------------------------------------------------------
        # 3. Batch the requests (max BATCH_SIZE per call)
        # ------------------------------------------------------------------
        batches = [
            all_series[i : i + BATCH_SIZE]
            for i in range(0, total_series, BATCH_SIZE)
        ]
        log.info(f"Batches to send : {len(batches)}")

        # Master aggregation: (soc_code, msa_code, survey_year) → wage dict
        master: dict[tuple, dict] = {}

        session = requests.Session()

        for batch_num, batch in enumerate(batches, start=1):
            batch_ids = [s["series_id"] for s in batch]
            log.info(
                f"  Batch {batch_num:>3}/{len(batches)} — "
                f"requesting {len(batch_ids)} series ..."
            )

            response = fetch_bls_batch(batch_ids, SURVEY_YEAR, session)

            if response is None:
                log.warning(f"  Batch {batch_num} returned no response — skipping.")
                series_gaps.extend(batch_ids)
            else:
                partial = parse_response(response, series_meta)

                # Track which series came back empty.
                returned_ids = {
                    sid
                    for series in response.get("Results", {}).get("series", [])
                    for sid in [series.get("seriesID", "")]
                    if series.get("data")
                }
                for sid in batch_ids:
                    if sid not in returned_ids:
                        series_gaps.append(sid)

                series_with_data += len(returned_ids)
                master.update(partial)
                log.info(
                    f"  Batch {batch_num:>3}/{len(batches)} — "
                    f"{len(returned_ids)} series with data, "
                    f"{len(batch_ids) - len(returned_ids)} empty/suppressed."
                )

            # Respectful delay between batches.
            if batch_num < len(batches):
                time.sleep(INTER_BATCH_DELAY)

        # ------------------------------------------------------------------
        # 4. Load aggregated rows into the database
        # ------------------------------------------------------------------
        records_received = len(master)
        log.info(f"Unique (SOC, MSA, year) rows to write: {records_received}")

        if records_received > 0:
            records_written = load_wage_rows(conn, master)
            log.info(f"Rows written to bls_wage_data: {records_written}")
        else:
            log.warning("No wage data was returned by the API — nothing written to DB.")

    except Exception as exc:
        status    = "FAILED"
        error_msg = str(exc)
        log.error(f"Pipeline failed: {exc}")

    finally:
        duration = time.perf_counter() - overall_start
        log_pipeline_run(
            conn,
            pipeline_type=pipeline_type,
            status=status,
            records_requested=total_series if "total_series" in dir() else 0,
            records_received=records_received,
            records_written=records_written,
            run_duration_seconds=duration,
            error_message=error_msg,
        )
        conn.close()
        log.info("Database connection closed.")

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    log.info("")
    log.info("=" * 60)
    log.info("BLS PIPELINE SUMMARY")
    log.info("=" * 60)
    log.info(f"  Survey year          : {SURVEY_YEAR}")
    log.info(f"  Batches sent         : {len(batches) if 'batches' in dir() else 0}")
    log.info(f"  Series requested     : {total_series if 'total_series' in dir() else 0}")
    log.info(f"  Series with data     : {series_with_data}")
    log.info(f"  Series empty/gaps    : {len(series_gaps)}")
    log.info(f"  Unique rows received : {records_received}")
    log.info(f"  Rows written to DB   : {records_written}")
    log.info(f"  Duration             : {duration:.2f}s")
    log.info(f"  Status               : {status}")

    if series_gaps:
        log.info(f"  Gaps ({len(series_gaps)} series with no published data):")
        # Group gaps by (msa, soc) for readability — log up to 30 then summarise.
        for sid in series_gaps[:30]:
            log.info(f"    {sid}")
        if len(series_gaps) > 30:
            log.info(f"    ... and {len(series_gaps) - 30} more (see log file).")

    if error_msg:
        log.error(f"  Error: {error_msg}")

    log.info("=" * 60)

    if status != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
