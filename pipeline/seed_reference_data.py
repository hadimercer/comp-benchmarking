"""
pipeline/seed_reference_data.py

Seeds two static reference tables that never change unless manually updated:
  - soc_code_reference   (20 SOC codes used across TechNova job families)
  - job_soc_crosswalk    (38 role-to-SOC mappings with match quality metadata)

INSERT ... ON CONFLICT DO NOTHING makes this script safely re-runnable.
After seeding, prints a row-count summary and writes one audit record to
pipeline_run_log.

Usage:
    python -m pipeline.seed_reference_data
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
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
        logging.FileHandler(LOG_DIR / f"seed_reference_data_{_ts}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


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
    """
    Insert one audit row into pipeline_run_log.
    Commits its own transaction so a prior rollback does not swallow it.
    """
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
# REFERENCE DATA — soc_code_reference (20 rows)
# ---------------------------------------------------------------------------

SOC_CODES: list[dict] = [
    {
        "soc_code":       "11-1021",
        "soc_title":      "General and Operations Managers",
        "soc_major_group": "Management Occupations",
        "used_by_families": "Corporate",
    },
    {
        "soc_code":       "11-3021",
        "soc_title":      "Computer and Information Systems Managers",
        "soc_major_group": "Management Occupations",
        "used_by_families": "Product Management (L5-L6)",
    },
    {
        "soc_code":       "11-3121",
        "soc_title":      "Human Resources Managers",
        "soc_major_group": "Management Occupations",
        "used_by_families": "Corporate",
    },
    {
        "soc_code":       "13-1071",
        "soc_title":      "Human Resources Specialists",
        "soc_major_group": "Business & Financial Operations",
        "used_by_families": "Corporate",
    },
    {
        "soc_code":       "13-2011",
        "soc_title":      "Accountants and Auditors",
        "soc_major_group": "Business & Financial Operations",
        "used_by_families": "Corporate",
    },
    {
        "soc_code":       "13-2051",
        "soc_title":      "Financial Analysts",
        "soc_major_group": "Business & Financial Operations",
        "used_by_families": "Corporate",
    },
    {
        "soc_code":       "15-1211",
        "soc_title":      "Computer Systems Analysts",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "Clinical Informatics, Health IT, UX Research, IT Analyst",
    },
    {
        "soc_code":       "15-1232",
        "soc_title":      "Computer User Support Specialists",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "IT / Systems Administration",
    },
    {
        "soc_code":       "15-1243",
        "soc_title":      "Database Architects",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "Data Engineering, Analytics Engineering",
    },
    {
        "soc_code":       "15-1244",
        "soc_title":      "Network and Computer Systems Administrators",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "DevOps, Platform, SysAdmin",
    },
    {
        "soc_code":       "15-1252",
        "soc_title":      "Software Developers",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "Software Engineering, SRE, Mobile",
    },
    {
        "soc_code":       "15-1253",
        "soc_title":      "Software Quality Assurance Analysts and Testers",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "Software Engineering (QA/SDET)",
    },
    {
        "soc_code":       "15-1254",
        "soc_title":      "Web Developers",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "Software Engineering (Frontend)",
    },
    {
        "soc_code":       "15-1255",
        "soc_title":      "Web and Digital Interface Designers",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "UX / Design",
    },
    {
        "soc_code":       "15-1299",
        "soc_title":      "Computer Occupations, All Other",
        "soc_major_group": "Computer & Mathematical",
        "used_by_families": "Product Management (IC L1-L4)",
    },
    {
        "soc_code":       "15-2051",
        "soc_title":      "Data Scientists",
        "soc_major_group": "Mathematical Science",
        "used_by_families": "Data Science, Data Analysis",
    },
    {
        "soc_code":       "23-1011",
        "soc_title":      "Lawyers",
        "soc_major_group": "Legal Occupations",
        "used_by_families": "Corporate Legal",
    },
    {
        "soc_code":       "23-2011",
        "soc_title":      "Paralegals and Legal Assistants",
        "soc_major_group": "Legal Occupations",
        "used_by_families": "Corporate Legal",
    },
    {
        "soc_code":       "41-3091",
        "soc_title":      (
            "Sales Representatives of Services, Except Advertising, "
            "Insurance, Financial Services, and Travel"
        ),
        "soc_major_group": "Sales & Related",
        "used_by_families": "Sales, Account Management",
    },
    {
        "soc_code":       "41-9031",
        "soc_title":      "Sales Engineers",
        "soc_major_group": "Sales & Related",
        "used_by_families": "Solutions Engineering",
    },
]

SOC_CODE_INSERT_SQL = """
    INSERT INTO soc_code_reference (
        soc_code, soc_title, soc_major_group, used_by_families
    ) VALUES (
        %(soc_code)s, %(soc_title)s, %(soc_major_group)s, %(used_by_families)s
    )
    ON CONFLICT (soc_code) DO NOTHING
"""


# ---------------------------------------------------------------------------
# REFERENCE DATA — job_soc_crosswalk (38 rows)
# ---------------------------------------------------------------------------

# naics_filter_recommended is None (NULL) where not applicable.
CROSSWALK: list[dict] = [
    {
        "crosswalk_id": "XW-001", "job_family": "Software Engineering",
        "technova_role_title": "Software Engineer - Backend / Full-Stack",
        "job_level_applicability": "L1-L6", "soc_code": "15-1252",
        "soc_title": "Software Developers", "match_quality": "EXACT",
        "match_notes": "Direct SOC match. Primary code for all backend and full-stack SWE roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-002", "job_family": "Software Engineering",
        "technova_role_title": "Software Engineer - Frontend / Web",
        "job_level_applicability": "L1-L4", "soc_code": "15-1254",
        "soc_title": "Web Developers", "match_quality": "CLOSE",
        "match_notes": "Use for frontend-focused roles. Upgrade to 15-1252 at L5+ where scope broadens.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-003", "job_family": "Software Engineering",
        "technova_role_title": "Software Engineer - Mobile",
        "job_level_applicability": "L1-L6", "soc_code": "15-1252",
        "soc_title": "Software Developers", "match_quality": "CLOSE",
        "match_notes": "No mobile-specific SOC exists. 15-1252 is the standard industry mapping.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-004", "job_family": "Software Engineering",
        "technova_role_title": "QA Engineer / SDET",
        "job_level_applicability": "L1-L5", "soc_code": "15-1253",
        "soc_title": "Software Quality Assurance Analysts and Testers",
        "match_quality": "EXACT",
        "match_notes": "Dedicated SOC code for all quality engineering and SDET roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-005", "job_family": "Data & Analytics",
        "technova_role_title": "Data Scientist",
        "job_level_applicability": "L1-L6", "soc_code": "15-2051",
        "soc_title": "Data Scientists", "match_quality": "EXACT",
        "match_notes": "Dedicated code added in 2018 SOC revision. Direct match for all DS roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-006", "job_family": "Data & Analytics",
        "technova_role_title": "Data Engineer",
        "job_level_applicability": "L1-L6", "soc_code": "15-1243",
        "soc_title": "Database Architects", "match_quality": "CLOSE",
        "match_notes": "No Data Engineer SOC exists. 15-1243 is the closest match by task definition.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-007", "job_family": "Data & Analytics",
        "technova_role_title": "Data Analyst",
        "job_level_applicability": "L1-L4", "soc_code": "15-2051",
        "soc_title": "Data Scientists", "match_quality": "BEST_AVAILABLE",
        "match_notes": "CAUTION: No Data Analyst SOC exists. 15-2051 skews high vs analyst-level comp.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-008", "job_family": "Data & Analytics",
        "technova_role_title": "Analytics Engineer",
        "job_level_applicability": "L2-L5", "soc_code": "15-1243",
        "soc_title": "Database Architects", "match_quality": "BEST_AVAILABLE",
        "match_notes": "Hybrid role with no SOC match. 15-1243 used for pipeline/data architecture alignment.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-009", "job_family": "Data & Analytics",
        "technova_role_title": "Business Intelligence Analyst",
        "job_level_applicability": "L1-L4", "soc_code": "15-1211",
        "soc_title": "Computer Systems Analysts", "match_quality": "CLOSE",
        "match_notes": "BI roles commonly mapped to 15-1211 by compensation practitioners.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-010", "job_family": "Product Management",
        "technova_role_title": "Product Manager - IC",
        "job_level_applicability": "L1-L4", "soc_code": "15-1299",
        "soc_title": "Computer Occupations, All Other", "match_quality": "KNOWN_GAP",
        "match_notes": "CRITICAL: No PM SOC exists. 15-1299 is catch-all. Dashboard disclaimer required.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-011", "job_family": "Product Management",
        "technova_role_title": "Senior / Principal Product Manager",
        "job_level_applicability": "L5-L6", "soc_code": "11-3021",
        "soc_title": "Computer and Information Systems Managers",
        "match_quality": "BEST_AVAILABLE",
        "match_notes": "L5+ PM scope approaches management. 11-3021 gives better signal at senior levels.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-012", "job_family": "Clinical Informatics / Health IT",
        "technova_role_title": "Clinical Informatics Analyst",
        "job_level_applicability": "L1-L4", "soc_code": "15-1211",
        "soc_title": "Computer Systems Analysts", "match_quality": "CLOSE",
        "match_notes": "O*NET sub-code 15-1211.01 rolls into 15-1211 for OEWS wage publication.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": "YES - NAICS 62 (Health Care)",
    },
    {
        "crosswalk_id": "XW-013", "job_family": "Clinical Informatics / Health IT",
        "technova_role_title": "Health IT Specialist",
        "job_level_applicability": "L1-L4", "soc_code": "15-1211",
        "soc_title": "Computer Systems Analysts", "match_quality": "CLOSE",
        "match_notes": "NAICS 62 filter critical to get healthcare-sector wage signal.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": "YES - NAICS 62 (Health Care)",
    },
    {
        "crosswalk_id": "XW-014", "job_family": "Clinical Informatics / Health IT",
        "technova_role_title": "EHR Implementation Analyst",
        "job_level_applicability": "L1-L3", "soc_code": "15-1211",
        "soc_title": "Computer Systems Analysts", "match_quality": "CLOSE",
        "match_notes": "Standard mapping for EHR/systems-focused implementation roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": "YES - NAICS 62 (Health Care)",
    },
    {
        "crosswalk_id": "XW-015", "job_family": "Clinical Informatics / Health IT",
        "technova_role_title": "Interoperability / Integration Analyst",
        "job_level_applicability": "L2-L5", "soc_code": "15-1211",
        "soc_title": "Computer Systems Analysts", "match_quality": "CLOSE",
        "match_notes": "HL7/FHIR-focused roles align to systems analyst task definition.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": "YES - NAICS 62 (Health Care)",
    },
    {
        "crosswalk_id": "XW-016", "job_family": "UX / Design",
        "technova_role_title": "UX Designer / Product Designer",
        "job_level_applicability": "L1-L5", "soc_code": "15-1255",
        "soc_title": "Web and Digital Interface Designers", "match_quality": "CLOSE",
        "match_notes": "Added in 2018 SOC revision specifically for digital product design roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-017", "job_family": "UX / Design",
        "technova_role_title": "UX Researcher",
        "job_level_applicability": "L2-L5", "soc_code": "15-1211",
        "soc_title": "Computer Systems Analysts", "match_quality": "BEST_AVAILABLE",
        "match_notes": "No UX Research SOC exists. 15-1211 is the standard practitioner workaround.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-018", "job_family": "UX / Design",
        "technova_role_title": "UI Designer",
        "job_level_applicability": "L1-L4", "soc_code": "15-1255",
        "soc_title": "Web and Digital Interface Designers", "match_quality": "CLOSE",
        "match_notes": "Direct match for UI-focused roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-019", "job_family": "DevOps / Platform / SRE",
        "technova_role_title": "DevOps Engineer",
        "job_level_applicability": "L2-L5", "soc_code": "15-1244",
        "soc_title": "Network and Computer Systems Administrators",
        "match_quality": "CLOSE",
        "match_notes": "Standard mapping. May skew slightly low for senior DevOps.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-020", "job_family": "DevOps / Platform / SRE",
        "technova_role_title": "Platform Engineer",
        "job_level_applicability": "L2-L5", "soc_code": "15-1244",
        "soc_title": "Network and Computer Systems Administrators",
        "match_quality": "CLOSE",
        "match_notes": "Infrastructure-heavy scope aligns to 15-1244.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-021", "job_family": "DevOps / Platform / SRE",
        "technova_role_title": "Site Reliability Engineer (SRE)",
        "job_level_applicability": "L2-L6", "soc_code": "15-1252",
        "soc_title": "Software Developers", "match_quality": "CLOSE",
        "match_notes": (
            "SRE intentionally split from DevOps. "
            "Software-heavy task definition makes 15-1252 more accurate."
        ),
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-022", "job_family": "DevOps / Platform / SRE",
        "technova_role_title": "Cloud Infrastructure Engineer",
        "job_level_applicability": "L2-L5", "soc_code": "15-1244",
        "soc_title": "Network and Computer Systems Administrators",
        "match_quality": "CLOSE",
        "match_notes": "Infrastructure-heavy scope aligns to 15-1244.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-023", "job_family": "IT / Systems Administration",
        "technova_role_title": "IT Support Specialist / Help Desk",
        "job_level_applicability": "L1-L3", "soc_code": "15-1232",
        "soc_title": "Computer User Support Specialists", "match_quality": "EXACT",
        "match_notes": "Direct match for all Tier 1/2 support and help desk roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-024", "job_family": "IT / Systems Administration",
        "technova_role_title": "Systems Administrator",
        "job_level_applicability": "L2-L4", "soc_code": "15-1244",
        "soc_title": "Network and Computer Systems Administrators",
        "match_quality": "EXACT",
        "match_notes": "Direct match for SysAdmin roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-025", "job_family": "IT / Systems Administration",
        "technova_role_title": "Network Administrator",
        "job_level_applicability": "L2-L4", "soc_code": "15-1244",
        "soc_title": "Network and Computer Systems Administrators",
        "match_quality": "EXACT",
        "match_notes": "Direct match for network-focused admin roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-026", "job_family": "IT / Systems Administration",
        "technova_role_title": "IT Analyst",
        "job_level_applicability": "L1-L4", "soc_code": "15-1211",
        "soc_title": "Computer Systems Analysts", "match_quality": "CLOSE",
        "match_notes": "Standard mapping for analyst-level IT roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-027", "job_family": "Sales & Account Management",
        "technova_role_title": "Account Executive (AE)",
        "job_level_applicability": "L1-L5", "soc_code": "41-3091",
        "soc_title": (
            "Sales Representatives of Services, Except Advertising, "
            "Insurance, Financial Services, and Travel"
        ),
        "match_quality": "CLOSE",
        "match_notes": "Standard mapping for SaaS/tech AE roles. Note: BLS wages reflect base only.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-028", "job_family": "Sales & Account Management",
        "technova_role_title": "Sales Engineer / Solutions Engineer",
        "job_level_applicability": "L2-L5", "soc_code": "41-9031",
        "soc_title": "Sales Engineers", "match_quality": "EXACT",
        "match_notes": "Dedicated SOC code for technical pre-sales roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-029", "job_family": "Sales & Account Management",
        "technova_role_title": "Account Manager / Customer Success Manager",
        "job_level_applicability": "L1-L4", "soc_code": "41-3091",
        "soc_title": (
            "Sales Representatives of Services, Except Advertising, "
            "Insurance, Financial Services, and Travel"
        ),
        "match_quality": "CLOSE",
        "match_notes": "No CSM-specific SOC. 41-3091 is the standard workaround.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-030", "job_family": "Sales & Account Management",
        "technova_role_title": "Sales Development Rep (SDR)",
        "job_level_applicability": "L1-L2", "soc_code": "41-3091",
        "soc_title": (
            "Sales Representatives of Services, Except Advertising, "
            "Insurance, Financial Services, and Travel"
        ),
        "match_quality": "CLOSE",
        "match_notes": "SDR comp is structurally different. Treat benchmarks as base-only directional.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-031", "job_family": "Corporate",
        "technova_role_title": "HR Specialist / HRBP",
        "job_level_applicability": "L2-L5", "soc_code": "13-1071",
        "soc_title": "Human Resources Specialists", "match_quality": "EXACT",
        "match_notes": "Direct match.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-032", "job_family": "Corporate",
        "technova_role_title": "HR Manager",
        "job_level_applicability": "L4-L6", "soc_code": "11-3121",
        "soc_title": "Human Resources Managers", "match_quality": "EXACT",
        "match_notes": "Direct match.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-033", "job_family": "Corporate",
        "technova_role_title": "Financial Analyst",
        "job_level_applicability": "L1-L4", "soc_code": "13-2051",
        "soc_title": "Financial Analysts", "match_quality": "EXACT",
        "match_notes": "Direct match.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-034", "job_family": "Corporate",
        "technova_role_title": "FP&A Analyst",
        "job_level_applicability": "L2-L5", "soc_code": "13-2051",
        "soc_title": "Financial Analysts", "match_quality": "EXACT",
        "match_notes": "Same code as Financial Analyst.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-035", "job_family": "Corporate",
        "technova_role_title": "Accountant",
        "job_level_applicability": "L1-L4", "soc_code": "13-2011",
        "soc_title": "Accountants and Auditors", "match_quality": "EXACT",
        "match_notes": "Direct match.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-036", "job_family": "Corporate",
        "technova_role_title": "General Operations Manager",
        "job_level_applicability": "L4-L6", "soc_code": "11-1021",
        "soc_title": "General and Operations Managers", "match_quality": "CLOSE",
        "match_notes": "Broad code appropriate for operations generalist roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-037", "job_family": "Corporate",
        "technova_role_title": "Paralegal",
        "job_level_applicability": "L1-L4", "soc_code": "23-2011",
        "soc_title": "Paralegals and Legal Assistants", "match_quality": "EXACT",
        "match_notes": "Direct match.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
    {
        "crosswalk_id": "XW-038", "job_family": "Corporate",
        "technova_role_title": "Corporate Counsel / Attorney",
        "job_level_applicability": "L3-L6", "soc_code": "23-1011",
        "soc_title": "Lawyers", "match_quality": "EXACT",
        "match_notes": "Direct match for all in-house legal counsel roles.",
        "pipeline_query_flag": "YES", "naics_filter_recommended": None,
    },
]

CROSSWALK_INSERT_SQL = """
    INSERT INTO job_soc_crosswalk (
        crosswalk_id, job_family, technova_role_title,
        job_level_applicability, soc_code, soc_title,
        match_quality, match_notes,
        pipeline_query_flag, naics_filter_recommended
    ) VALUES (
        %(crosswalk_id)s, %(job_family)s, %(technova_role_title)s,
        %(job_level_applicability)s, %(soc_code)s, %(soc_title)s,
        %(match_quality)s, %(match_notes)s,
        %(pipeline_query_flag)s, %(naics_filter_recommended)s
    )
    ON CONFLICT (crosswalk_id) DO NOTHING
"""


# ---------------------------------------------------------------------------
# SEED FUNCTIONS
# ---------------------------------------------------------------------------

def seed_soc_codes(conn: psycopg2.extensions.connection) -> tuple[int, int]:
    """
    Insert all 20 SOC code records. Returns (inserted, skipped).
    rowcount == 1 means the row was new; 0 means ON CONFLICT skipped it.
    """
    inserted = 0
    skipped  = 0
    try:
        with conn.cursor() as cur:
            for row in SOC_CODES:
                cur.execute(SOC_CODE_INSERT_SQL, row)
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted, skipped


def seed_crosswalk(conn: psycopg2.extensions.connection) -> tuple[int, int]:
    """
    Insert all 38 crosswalk records. Returns (inserted, skipped).
    """
    inserted = 0
    skipped  = 0
    try:
        with conn.cursor() as cur:
            for row in CROSSWALK:
                cur.execute(CROSSWALK_INSERT_SQL, row)
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted, skipped


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    pipeline_type = "SEED_REFERENCE"
    overall_start = time.perf_counter()

    log.info("=" * 60)
    log.info("TechNova — Seed Reference Data Pipeline")
    log.info(f"Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        conn = get_connection()
        log.info("Database connection established.")
    except Exception as exc:
        log.error(f"Cannot connect to database: {exc}")
        sys.exit(1)

    total_inserted = 0
    total_skipped  = 0
    error_msg: str | None = None
    status = "SUCCESS"

    try:
        # --- Seed soc_code_reference ---
        log.info("-" * 40)
        log.info("Seeding soc_code_reference ...")
        soc_inserted, soc_skipped = seed_soc_codes(conn)
        total_inserted += soc_inserted
        total_skipped  += soc_skipped
        log.info(f"  Inserted : {soc_inserted}  |  Already present (skipped) : {soc_skipped}")

        # --- Seed job_soc_crosswalk ---
        log.info("-" * 40)
        log.info("Seeding job_soc_crosswalk ...")
        xw_inserted, xw_skipped = seed_crosswalk(conn)
        total_inserted += xw_inserted
        total_skipped  += xw_skipped
        log.info(f"  Inserted : {xw_inserted}  |  Already present (skipped) : {xw_skipped}")

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        log.error(f"Seeding failed: {exc}")

    finally:
        duration = time.perf_counter() - overall_start
        total_requested = len(SOC_CODES) + len(CROSSWALK)

        log_pipeline_run(
            conn,
            pipeline_type=pipeline_type,
            status=status,
            records_requested=total_requested,
            records_received=total_requested,
            records_written=total_inserted,
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
    log.info("SEED SUMMARY")
    log.info("=" * 60)
    log.info(f"  soc_code_reference  : {soc_inserted if status == 'SUCCESS' else 0:>3} inserted"
             f"  |  {soc_skipped if status == 'SUCCESS' else 0:>3} skipped"
             f"  |  {len(SOC_CODES):>3} total defined")
    log.info(f"  job_soc_crosswalk   : {xw_inserted if status == 'SUCCESS' else 0:>3} inserted"
             f"  |  {xw_skipped if status == 'SUCCESS' else 0:>3} skipped"
             f"  |  {len(CROSSWALK):>3} total defined")
    log.info(f"  Total rows inserted : {total_inserted}")
    log.info(f"  Duration            : {duration:.2f}s")
    log.info(f"  Status              : {status}")
    if error_msg:
        log.error(f"  Error               : {error_msg}")
    log.info("=" * 60)

    if status != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
