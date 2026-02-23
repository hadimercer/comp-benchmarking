"""
TechNova Compensation Intelligence Dashboard
"""

import io
import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from pipeline import bls_pipeline
from pipeline.csv_ingestion import (
    EMPLOYEE_UPSERT_SQL,
    GRADES_UPSERT_SQL,
    log_pipeline_run as log_csv_pipeline_run,
    validate_employees,
    validate_job_grades,
)

load_dotenv()

# ── Palette ───────────────────────────────────────────────────────────────────
# Streamlit dark theme: bg=#0E1117, secondary=#262730, text=#FAFAFA
ST_BG        = "#0E1117"   # Streamlit dark background (for reference)
ST_TEXT      = "#FAFAFA"   # Streamlit default text

# Chart bar colours — Option A: Teal + Coral
C_TECHNOVA   = "#4DB6AC"   # TechNova Internal bars — soft teal
C_MARKET     = "#FF7043"   # BLS Market bars — coral orange
C_SINGLE     = "#4DB6AC"   # Single-series charts — same teal
C_BAND       = "rgba(77,182,172,0.18)"  # shaded band fill

# Brand / UI
P_DARK_BLUE  = "#1B4F72"   # page header gradient start
P_MID_BLUE   = "#2E86C1"   # page header gradient end
P_AMBER      = "#F39C12"   # BLS P50 line / warning
P_GREEN      = "#1E8449"   # success
P_RED        = "#C0392B"   # error
P_ORANGE     = "#E67E22"   # mild warning

# Table cell colours — saturated, always dark text
P_LIGHT_RED  = "#E74C3C"   # bad  — saturated red
P_LIGHT_GRN  = "#27AE60"   # good — saturated green
P_LIGHT_AMB  = "#E67E22"   # warn — saturated orange
P_LIGHT_YLW  = "#F39C12"   # caution — amber
P_TEXT_DARK  = "#FFFFFF"   # white text on saturated backgrounds
P_TEXT_MED   = "#A0A8B8"   # muted label text on dark bg

LEVEL_ORDER = ["L1", "L2", "L3", "L4", "L5", "L6"]

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TechNova | Compensation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  /* Page header band */
  .page-header {{
    background: linear-gradient(90deg, {P_DARK_BLUE} 0%, {P_MID_BLUE} 100%);
    border-radius: 0.6rem;
    padding: 1rem 1.4rem 0.9rem;
    margin-bottom: 1.2rem;
  }}
  .page-header h1 {{
    color: #FFFFFF !important;
    font-size: 1.8rem !important;
    font-weight: 700;
    margin: 0 0 0.2rem 0;
    line-height: 1.2;
  }}
  .page-header p {{
    color: rgba(255,255,255,0.82);
    font-size: 0.88rem;
    margin: 0;
  }}

  /* Sidebar logo */
  .sidebar-logo {{
    background: linear-gradient(135deg, {P_DARK_BLUE} 0%, {P_MID_BLUE} 100%);
    border-radius: 0.6rem;
    padding: 0.85rem 1rem;
    margin-bottom: 0.8rem;
  }}
  .sidebar-logo h2 {{ color:#fff; margin:0; font-size:1.25rem; }}
  .sidebar-logo p  {{ color:rgba(255,255,255,0.85); margin:0; font-size:0.78rem; }}

  /* Info pill */
  .info-pill {{
    background: rgba(46,134,193,0.15);
    border-left: 4px solid {P_MID_BLUE};
    border-radius: 0.4rem;
    padding: 0.55rem 0.85rem;
    font-size: 0.86rem;
    color: {P_TEXT_DARK};
    margin-bottom: 0.6rem;
  }}

  /* Section divider */
  .divider {{ border-top: 1.5px solid #D5D8DC; margin: 1.4rem 0; }}

  /* Force st.metric label color */
  [data-testid="stMetricLabel"] {{ color: {P_TEXT_MED} !important; font-size:0.8rem !important; }}
  [data-testid="stMetricValue"] {{ font-size: 1.4rem !important; font-weight: 700 !important; }}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"], port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode="require", connect_timeout=15,
    )


@st.cache_data(ttl=300, show_spinner=False)
def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return pd.DataFrame(cur.fetchall())
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def to_num(v):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None

def fmt_c(v): n = to_num(v); return f"${n:,.0f}" if n is not None else "N/A"
def fmt_p(v): n = to_num(v); return f"{n:.1f}%" if n is not None else "N/A"

def parse_lnum(lvl):
    if not lvl or not isinstance(lvl, str): return None
    s = lvl.strip().upper()
    return int(s[1]) if len(s) == 2 and s[0] == "L" and s[1].isdigit() else None

def level_in(level, applicability):
    n = parse_lnum(level)
    if n is None or not applicability: return False
    for chunk in str(applicability).upper().replace(" ", "").split(","):
        if "-" in chunk:
            pts = chunk.split("-"); lo, hi = parse_lnum(pts[0]), parse_lnum(pts[1])
            if lo and hi and lo <= n <= hi: return True
        elif parse_lnum(chunk) == n: return True
    return False

def sort_levels(vals):
    ordered = [l for l in LEVEL_ORDER if l in vals]
    return ordered + sorted([l for l in vals if l not in ordered])

def distinct(table, col, where="", params=()):
    df = query_df(f"SELECT DISTINCT {col} AS v FROM {table} {where} ORDER BY {col}", params)
    return [] if df.empty else [str(x) for x in df["v"].dropna()]


# ── Chart helper: consistent, readable Plotly layout ─────────────────────────

def clayout(fig, title="", xtitle="", ytitle="", h=440, barmode=None):
    """
    Apply TechNova chart styling optimised for Streamlit dark theme (#0E1117).
    - Fully transparent background so charts float on the dark page
    - All text in #FAFAFA (Streamlit default text colour) for maximum contrast
    - Axis lines and titles clearly visible at full opacity
    - Grid lines whisper-quiet at 10% white
    - Legend and title both centred over the plot area
    """
    updates = dict(
        title=dict(
            text=f"<b>{title}</b>" if title else "",
            font=dict(size=14, color=ST_TEXT, family="Arial"),
            x=0.5, xanchor="center",
            pad=dict(b=10),
        ),
        xaxis=dict(
            title=dict(text=xtitle, font=dict(color=ST_TEXT, size=12)),
            tickfont=dict(color=ST_TEXT, size=11),
            showgrid=False,
            linecolor="rgba(255,255,255,0.35)",
            linewidth=1,
        ),
        yaxis=dict(
            title=dict(text=ytitle, font=dict(color=ST_TEXT, size=12)),
            tickfont=dict(color=ST_TEXT, size=11),
            gridcolor="rgba(255,255,255,0.08)",
            gridwidth=1,
            linecolor="rgba(255,255,255,0.35)",
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.04,
            xanchor="center", x=0.5,
            font=dict(color=ST_TEXT, size=11),
            bgcolor="rgba(255,255,255,0.06)",
            bordercolor="rgba(255,255,255,0.15)",
            borderwidth=1,
        ),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=h,
        margin=dict(t=95, b=55, l=65, r=30),
        font=dict(family="Arial, sans-serif", color=ST_TEXT),
        hoverlabel=dict(
            bgcolor="#1E2530",
            font_color=ST_TEXT,
            bordercolor="rgba(255,255,255,0.2)",
        ),
    )
    if barmode:
        updates["barmode"] = barmode
    fig.update_layout(**updates)
    return fig


# ── Table colour helpers — ALWAYS dark text ────────────────────────────────

def tcolor_gap_below(val):
    v = to_num(val)
    if v is None: return ""
    if v > 15:  return f"background-color:{P_LIGHT_RED}; color:{P_TEXT_DARK}; font-weight:700;"
    if v >= 10: return f"background-color:{P_LIGHT_AMB}; color:{P_TEXT_DARK}; font-weight:700;"
    if v > 0:   return f"background-color:{P_LIGHT_YLW}; color:{P_TEXT_DARK}; font-weight:600;"
    return ""

def tcolor_pos_neg(val):
    v = to_num(val)
    if v is None: return ""
    if v > 0:  return f"background-color:{P_LIGHT_GRN}; color:{P_TEXT_DARK}; font-weight:600;"
    if v < 0:  return f"background-color:{P_LIGHT_RED}; color:{P_TEXT_DARK}; font-weight:600;"
    return ""

def tcolor_compa(val):
    v = to_num(val)
    if v is None: return ""
    return (f"background-color:{P_LIGHT_GRN}; color:{P_TEXT_DARK}; font-weight:600;" if 0.85 <= v <= 1.15
            else f"background-color:{P_LIGHT_RED}; color:{P_TEXT_DARK}; font-weight:700;")

def tcolor_pct_below(val):
    v = to_num(val)
    if v is None or v == 0: return ""
    return (f"background-color:{P_LIGHT_RED}; color:{P_TEXT_DARK}; font-weight:700;" if v > 20
            else f"background-color:{P_LIGHT_AMB}; color:{P_TEXT_DARK}; font-weight:600;")

def tcolor_pct_above(val):
    v = to_num(val)
    if v is None or v == 0: return ""
    return f"background-color:{P_LIGHT_AMB}; color:{P_TEXT_DARK}; font-weight:600;"

def tcolor_gap_7(val):
    v = to_num(val)
    if v is None: return ""
    if v > 5:  return f"background-color:{P_LIGHT_RED}; color:{P_TEXT_DARK}; font-weight:700;"
    if v > 0:  return f"background-color:{P_LIGHT_AMB}; color:{P_TEXT_DARK}; font-weight:600;"
    return f"background-color:{P_LIGHT_GRN}; color:{P_TEXT_DARK}; font-weight:600;"

def tcolor_gap_pct_equity(val):
    v = to_num(val)
    if v is None: return ""
    return (f"background-color:{P_LIGHT_RED}; color:{P_TEXT_DARK}; font-weight:700;" if v > 5
            else f"background-color:{P_LIGHT_GRN}; color:{P_TEXT_DARK}; font-weight:600;")


# ── Page header ───────────────────────────────────────────────────────────────

def header(title, subtitle=""):
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(f"""
    <div class="page-header">
      <h1>{title}</h1>
      {sub}
    </div>""", unsafe_allow_html=True)


def nodata(msg="No data available for the selected filters."):
    st.info(f"ℹ️ {msg}")


# ══════════════════════════════════════════════════════════════════════════════
# CROSSWALK / BLS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def crosswalk_candidates(job_family):
    return query_df("""
        SELECT crosswalk_id, job_family, technova_role_title,
               job_level_applicability, soc_code, soc_title,
               match_quality, match_notes
        FROM job_soc_crosswalk
        WHERE job_family=%s AND pipeline_query_flag='YES'
        ORDER BY crosswalk_id""", (job_family,))


def pick_crosswalk(job_family, job_level=None):
    df = crosswalk_candidates(job_family)
    if df.empty: return None
    cdf = df.copy()
    if job_level:
        lvl = cdf[cdf["job_level_applicability"].apply(lambda x: level_in(job_level, x))]
        if not lvl.empty: cdf = lvl
    rank = {"EXACT":1,"CLOSE":2,"BEST_AVAILABLE":3,"KNOWN_GAP":4}
    cdf = cdf.copy()
    cdf["_r"] = cdf["match_quality"].map(rank).fillna(99)
    return cdf.sort_values(["_r","crosswalk_id"]).iloc[0].to_dict()


def latest_bls(soc_code, msa_code):
    df = query_df("""
        SELECT soc_code,msa_code,msa_name,reference_year,
               annual_mean,pct_25,pct_50,pct_75
        FROM bls_wage_data
        WHERE soc_code=%s AND msa_code=%s
        ORDER BY reference_year DESC LIMIT 1""", (soc_code, str(msa_code)))
    return None if df.empty else df.iloc[0].to_dict()


def msa_for_office(office):
    df = query_df("""
        SELECT msa_code,msa_name,COUNT(*) AS cnt
        FROM employees WHERE office_location=%s
        GROUP BY msa_code,msa_name ORDER BY cnt DESC LIMIT 1""", (office,))
    return None if df.empty else df.iloc[0].to_dict()


def last_success(pipeline_type):
    df = query_df("""
        SELECT run_timestamp FROM pipeline_run_log
        WHERE pipeline_type=%s AND status='SUCCESS'
        ORDER BY run_timestamp DESC LIMIT 1""", (pipeline_type,))
    if df.empty: return None
    ts = df.iloc[0]["run_timestamp"]
    return ts if isinstance(ts, datetime) else None


def bls_avg_soc(soc_codes):
    if not soc_codes: return None
    ph = ",".join(["%s"]*len(soc_codes))
    df = query_df(f"""
        SELECT AVG(pct_50) AS v FROM bls_wage_data
        WHERE reference_year=(SELECT MAX(reference_year) FROM bls_wage_data)
          AND soc_code IN ({ph}) AND pct_50 IS NOT NULL""", tuple(soc_codes))
    return None if df.empty else to_num(df.iloc[0]["v"])


def short_msa(name):
    return str(name).split(",")[0].strip() if name else ""


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — BENCHMARKING VIEW
# ══════════════════════════════════════════════════════════════════════════════

def page1():
    header("Benchmarking View",
           "Internal salary bands vs BLS market data — get a defensible range in under 2 minutes.")

    families = distinct("internal_job_grades", "job_family")
    levels   = sort_levels(distinct("internal_job_grades", "job_level"))
    offices  = distinct("employees", "office_location")
    if not families or not levels or not offices:
        nodata("Filter data unavailable — check that employees and internal_job_grades are loaded.")
        return

    c1, c2, c3 = st.columns(3)
    fam = c1.selectbox("Job Family", families, key="p1_fam")
    lvl = c2.selectbox("Job Level",  levels,   key="p1_lvl")
    off = c3.selectbox("Office",     offices,  key="p1_off")

    gdf = query_df("""
        SELECT AVG(band_minimum) m, AVG(band_midpoint) mid, AVG(band_maximum) mx
        FROM internal_job_grades WHERE job_family=%s AND job_level=%s""", (fam, lvl))
    if gdf.empty or gdf.iloc[0]["mid"] is None:
        nodata("No internal grade found."); return

    xr = pick_crosswalk(fam, lvl)
    if not xr: nodata("No SOC crosswalk mapping found."); return

    msa = msa_for_office(off)
    if not msa: nodata("No MSA mapping found for this office."); return

    bls = latest_bls(xr["soc_code"], str(msa["msa_code"]))
    if not bls: nodata("No BLS wage data found for this SOC + location."); return

    bmin = to_num(gdf.iloc[0]["m"])
    bmid = to_num(gdf.iloc[0]["mid"])
    bmax = to_num(gdf.iloc[0]["mx"])
    p25  = to_num(bls.get("pct_25"))
    p50  = to_num(bls.get("pct_50"))
    p75  = to_num(bls.get("pct_75"))

    # ── Grouped bar chart ─────────────────────────────────────────────────────
    cats = ["Lower Quartile", "Median", "Upper Quartile"]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="TechNova Internal",
        x=cats, y=[bmin, bmid, bmax],
        marker_color=C_TECHNOVA,
        text=[fmt_c(v) for v in [bmin,bmid,bmax]],
        textposition="outside", textfont=dict(color=ST_TEXT, size=11),
    ))
    fig.add_trace(go.Bar(
        name=f"BLS Market — {short_msa(bls.get('msa_name',''))}",
        x=cats, y=[p25, p50, p75],
        marker_color=C_MARKET,
        text=[fmt_c(v) for v in [p25,p50,p75]],
        textposition="outside", textfont=dict(color=ST_TEXT, size=11),
    ))
    clayout(fig, title=f"{fam}  ·  {lvl}  ·  {off}  |  SOC {xr['soc_code']}",
            xtitle="Salary Band", ytitle="Annual Salary (USD)", barmode="group")
    st.plotly_chart(fig, use_container_width=True)

    # ── KPI metrics ───────────────────────────────────────────────────────────
    gap_d = (bmid - p50) if (bmid and p50) else None
    gap_p = (gap_d / p50 * 100) if (gap_d is not None and p50) else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Internal Midpoint", fmt_c(bmid))
    c2.metric("BLS P50 (Market Median)", fmt_c(p50))
    delta_label = f"{'↑' if (gap_d or 0)>=0 else '↓'} {'Above' if (gap_d or 0)>=0 else 'Below'} market"
    c3.metric("Gap $ (vs P50)", fmt_c(gap_d), delta=delta_label,
              delta_color="normal" if (gap_d or 0)>=0 else "inverse")
    c4.metric("Gap %", fmt_p(gap_p),
              delta_color="normal" if (gap_p or 0)>=0 else "inverse")

    if bmid and p25 and bmid < p25:
        st.error("⚠️ **Below Market** — Internal midpoint falls below BLS P25. Flagged for Total Rewards review.")

    # ── Match quality info ────────────────────────────────────────────────────
    icons = {"EXACT":"✅","CLOSE":"🟡","BEST_AVAILABLE":"🟠","KNOWN_GAP":"🔴"}
    desc  = {"EXACT":"Direct SOC match — highest confidence",
             "CLOSE":"Strong match — minor occupational overlap",
             "BEST_AVAILABLE":"Closest available SOC — some aggregation",
             "KNOWN_GAP":"No precise SOC match — treat as indicative only"}
    mq  = str(xr.get("match_quality","")).upper()
    ic  = icons.get(mq,"ℹ️")
    dsc = desc.get(mq,"")
    st.markdown(f"""
    <div class="info-pill">
      {ic} <strong>BLS Match Quality: {mq}</strong> — {dsc}<br>
      <span style="color:{P_TEXT_MED}; font-size:0.82rem;">
        SOC {xr['soc_code']} · {xr.get('soc_title','')}
        {'  |  ' + str(xr.get('match_notes','')) if xr.get('match_notes') else ''}
      </span>
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — MARKET POSITION FLAGS
# ══════════════════════════════════════════════════════════════════════════════

def page2():
    header("Market Position Flags",
           "Grades outside the BLS P25–P75 range — below market and above market, side by side.")

    df = query_df("""
        WITH ly AS (SELECT MAX(reference_year) AS yr FROM bls_wage_data),
        cp AS (
            SELECT DISTINCT ON (job_family) job_family, soc_code
            FROM job_soc_crosswalk WHERE pipeline_query_flag='YES'
            ORDER BY job_family,
              CASE match_quality WHEN 'EXACT' THEN 1 WHEN 'CLOSE' THEN 2
                WHEN 'BEST_AVAILABLE' THEN 3 ELSE 4 END, crosswalk_id
        ),
        fb AS (
            SELECT cp.job_family, AVG(b.pct_25) p25, AVG(b.pct_50) p50, AVG(b.pct_75) p75
            FROM cp JOIN ly ON TRUE
            JOIN bls_wage_data b ON b.soc_code=cp.soc_code AND b.reference_year=ly.yr
            GROUP BY cp.job_family
        )
        SELECT g.job_family, g.role_title, g.job_level,
               g.band_minimum, g.band_midpoint, g.band_maximum,
               fb.p25 bls_p25, fb.p50 bls_p50, fb.p75 bls_p75
        FROM internal_job_grades g
        LEFT JOIN fb ON fb.job_family=g.job_family
        ORDER BY g.job_family, g.job_level, g.role_title""")

    if df.empty: nodata("No grade or BLS data available."); return

    def classify(r):
        mid,p25,p75 = to_num(r.band_midpoint), to_num(r.bls_p25), to_num(r.bls_p75)
        if mid is None: return "Unclassified"
        if p25 and mid < p25: return "Below Market"
        if p75 and mid > p75: return "Above Market"
        return "At Market"

    df["position"] = df.apply(classify, axis=1)

    # ── Family filter — buttons ABOVE multiselect, share same key ─────────────
    fam_opts = sorted(df["job_family"].dropna().unique().tolist())
    if "p2_fams" not in st.session_state:
        st.session_state["p2_fams"] = fam_opts[:]

    b1, b2, _ = st.columns([1.2, 1.2, 8])
    if b1.button("Select All", key="p2_all"):
        st.session_state["p2_fams"] = fam_opts[:]
        st.rerun()
    if b2.button("Clear All", key="p2_clr"):
        st.session_state["p2_fams"] = []
        st.rerun()

    sel = st.multiselect("Job Families", fam_opts, key="p2_fams")
    if not sel: nodata("No families selected."); return

    filt = df[df["job_family"].isin(sel)].copy()
    n_below = (filt["position"]=="Below Market").sum()
    n_above = (filt["position"]=="Above Market").sum()
    n_at    = (filt["position"]=="At Market").sum()

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Grades", f"{len(filt):,}")
    c2.metric("🔴 Below Market (< P25)", str(n_below))
    c3.metric("🟡 Above Market (> P75)", str(n_above))
    c4.metric("✅ At Market", str(n_at))

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    # ── Section A: Below Market ───────────────────────────────────────────────
    st.subheader("🔴 Below Market — Internal Midpoint Below BLS P25")
    below = filt[filt["position"]=="Below Market"].copy()
    if below.empty:
        st.success("✅ No below-market grades in the selected families.")
    else:
        below["Gap $"] = below.apply(
            lambda r: (to_num(r.bls_p25)-to_num(r.band_midpoint)) if to_num(r.bls_p25) and to_num(r.band_midpoint) else None, axis=1)
        below["Gap %"] = below.apply(
            lambda r: (r["Gap $"]/to_num(r.bls_p25)*100) if r["Gap $"] is not None and to_num(r.bls_p25) else None, axis=1)

        show = below.rename(columns={
            "job_family":"Job Family","role_title":"Role Title","job_level":"Level",
            "band_midpoint":"Internal Midpoint","bls_p25":"BLS P25",
        })[["Job Family","Role Title","Level","Internal Midpoint","BLS P25","Gap $","Gap %"]]

        st.dataframe(show.style.format({
            "Internal Midpoint":fmt_c,"BLS P25":fmt_c,"Gap $":fmt_c,"Gap %":fmt_p,
        }).map(tcolor_gap_below, subset=["Gap %"]),
        use_container_width=True, hide_index=True)

        # Severity bar chart
        bins   = ["< 10% gap","10–15% gap","> 15% gap"]
        counts = [
            int((below["Gap %"]<10).sum()),
            int(((below["Gap %"]>=10)&(below["Gap %"]<=15)).sum()),
            int((below["Gap %"]>15).sum()),
        ]
        fig = go.Figure(go.Bar(
            x=bins, y=counts,
            marker_color=[P_LIGHT_YLW, P_LIGHT_AMB, P_LIGHT_RED],
            text=counts, textposition="outside",
            textfont=dict(color=ST_TEXT, size=12),
        ))
        clayout(fig, title="Below-Market Severity Distribution",
                xtitle="Gap Category", ytitle="Number of Grades", h=320)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    # ── Section B: Above Market ───────────────────────────────────────────────
    st.subheader("🟡 Above Market — Internal Midpoint Above BLS P75")
    above = filt[filt["position"]=="Above Market"].copy()
    if above.empty:
        st.success("✅ No above-market grades in the selected families.")
    else:
        above["Premium $"] = above.apply(
            lambda r: (to_num(r.band_midpoint)-to_num(r.bls_p75)) if to_num(r.bls_p75) and to_num(r.band_midpoint) else None, axis=1)
        above["Premium %"] = above.apply(
            lambda r: (r["Premium $"]/to_num(r.bls_p75)*100) if r["Premium $"] is not None and to_num(r.bls_p75) else None, axis=1)

        show2 = above.rename(columns={
            "job_family":"Job Family","role_title":"Role Title","job_level":"Level",
            "band_midpoint":"Internal Midpoint","bls_p75":"BLS P75",
        })[["Job Family","Role Title","Level","Internal Midpoint","BLS P75","Premium $","Premium %"]]

        def tcolor_prem(val):
            v = to_num(val)
            if v is None or v < 20: return ""
            if v >= 50: return f"background-color:{P_LIGHT_RED}; color:{P_TEXT_DARK}; font-weight:700;"
            return f"background-color:{P_LIGHT_AMB}; color:{P_TEXT_DARK}; font-weight:600;"

        st.dataframe(show2.style.format({
            "Internal Midpoint":fmt_c,"BLS P75":fmt_c,"Premium $":fmt_c,"Premium %":fmt_p,
        }).map(tcolor_prem, subset=["Premium %"]),
        use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — PAY EQUITY MODULE
# ══════════════════════════════════════════════════════════════════════════════

def build_equity_df():
    g = query_df("""
        SELECT job_family,job_level,gender,
               AVG(annual_base_salary) avg_sal, COUNT(*) n
        FROM employees WHERE gender IN ('Male','Female')
        GROUP BY job_family,job_level,gender""")
    if g.empty: return pd.DataFrame()
    m = g[g.gender=="Male"].rename(columns={"avg_sal":"m_mean","n":"m_n"})
    f = g[g.gender=="Female"].rename(columns={"avg_sal":"f_mean","n":"f_n"})
    mg = m.merge(f, on=["job_family","job_level"], how="inner")
    mg = mg[(mg.m_n>=5)&(mg.f_n>=5)].copy()
    if mg.empty: return pd.DataFrame()
    mg["gap_d"] = mg.m_mean - mg.f_mean
    mg["gap_p"] = mg.apply(lambda r: r.gap_d/r.m_mean*100 if r.m_mean else None, axis=1)
    mg["flag"]  = mg.gap_p.apply(lambda x: x is not None and x>5)
    mg["job_level"] = pd.Categorical(mg.job_level, categories=LEVEL_ORDER, ordered=True)
    return mg.sort_values(["job_family","job_level"])


def page3():
    header("Pay Equity Module",
           "Gender pay gap patterns — for CHRO and Total Rewards review only.")
    st.warning("📋 **Monitoring Tool Only** — Statistical patterns for internal review. "
               "Not a legal compliance determination.")

    with st.expander("ℹ️ How are flags determined?"):
        st.markdown("""
**Methodology:**
- Groups employees by job family + job level + gender.
- Groups with fewer than **5 employees of either gender** are excluded.
- A **gap > 5%** between male and female average salaries at the same family+level is flagged 🚩.
- The 5% threshold is a monitoring signal, not a formal statistical significance test.
- Gaps may reflect tenure, performance, or location differences — investigation required before conclusions.
        """)

    eq = build_equity_df()
    if eq.empty: nodata("No valid male/female groups with ≥ 5 employees each."); return

    fam_opts = sorted(eq["job_family"].unique().tolist())
    fam = st.selectbox("Job Family", fam_opts, key="p3_fam")
    filt = eq[eq.job_family==fam].copy()
    if filt.empty: nodata("No rows for this family."); return

    # Bar chart
    cdf = query_df("""
        SELECT job_level, gender, AVG(annual_base_salary) avg_sal
        FROM employees WHERE gender IN ('Male','Female') AND job_family=%s
        GROUP BY job_level, gender""", (fam,))
    if not cdf.empty:
        cdf["job_level"] = pd.Categorical(cdf.job_level, categories=LEVEL_ORDER, ordered=True)
        cdf = cdf.sort_values("job_level")
        fig = px.bar(
            cdf, x="job_level", y="avg_sal", color="gender",
            barmode="group",
            color_discrete_map={"Male":C_TECHNOVA,"Female":C_MARKET},
            text_auto=".3s",
        )
        fig.update_traces(textfont=dict(color=ST_TEXT, size=11))
        clayout(fig, title=f"Average Salary by Gender — {fam}",
                xtitle="Job Level", ytitle="Average Salary (USD)")
        st.plotly_chart(fig, use_container_width=True)

    flagged = int(filt["flag"].sum()); total = len(filt)
    c1, c2 = st.columns(2)
    c1.metric("Flagged Combinations (gap > 5%)", f"{flagged} of {total}",
              delta="Requires review" if flagged else "No significant gaps")
    c2.metric("Max Gap %", fmt_p(filt.gap_p.max()))

    tbl = filt.rename(columns={
        "job_family":"Job Family","job_level":"Level",
        "m_n":"Male N","f_n":"Female N",
        "m_mean":"Male Avg","f_mean":"Female Avg",
        "gap_d":"Gap $","gap_p":"Gap %",
    }).copy()
    tbl["🚩"] = tbl["flag"].map(lambda x: "🚩" if x else "✅")
    cols = ["🚩","Job Family","Level","Male N","Female N","Male Avg","Female Avg","Gap $","Gap %"]
    st.dataframe(
        tbl[cols].style.format({"Male Avg":fmt_c,"Female Avg":fmt_c,"Gap $":fmt_c,"Gap %":fmt_p})
        .map(tcolor_gap_pct_equity, subset=["Gap %"]),
        use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — SALARY DISTRIBUTION
# ══════════════════════════════════════════════════════════════════════════════

def page4():
    header("Salary Distribution",
           "Actual employee salary spread by level, with BLS market benchmarks overlaid.")

    fams = distinct("employees","job_family")
    if not fams: nodata("No employee records."); return

    c1, c2 = st.columns([3,1])
    fam      = c1.selectbox("Job Family", sorted(fams), key="p4_fam")
    split_g  = c2.radio("Split by Gender", ["No","Yes"], horizontal=True, key="p4_g")

    edf = query_df(
        "SELECT job_level,gender,annual_base_salary FROM employees WHERE job_family=%s", (fam,))
    if edf.empty: nodata("No employees for this family."); return

    xr = pick_crosswalk(fam)
    bls_row = pd.DataFrame()
    if xr:
        bls_row = query_df("""
            SELECT AVG(pct_25) p25, AVG(pct_50) p50, AVG(pct_75) p75
            FROM bls_wage_data
            WHERE soc_code=%s
              AND reference_year=(SELECT MAX(reference_year) FROM bls_wage_data WHERE soc_code=%s)
            """, (xr["soc_code"], xr["soc_code"]))

    p25 = to_num(bls_row.iloc[0]["p25"]) if not bls_row.empty else None
    p50 = to_num(bls_row.iloc[0]["p50"]) if not bls_row.empty else None
    p75 = to_num(bls_row.iloc[0]["p75"]) if not bls_row.empty else None

    cnt     = edf.groupby("job_level",as_index=False).size().rename(columns={"size":"n"})
    lmap    = {r.job_level:f"{r.job_level} (n={int(r.n)})" for _,r in cnt.iterrows()}
    edf["lbl"] = edf["job_level"].astype(str).map(lmap).fillna(edf["job_level"])
    ordered = [l for l in LEVEL_ORDER if l in edf["job_level"].unique()]
    ord_lbl = [lmap[l] for l in ordered if l in lmap]

    bkw = dict(x="lbl", y="annual_base_salary",
               category_orders={"lbl":ord_lbl}, points="outliers")

    if split_g == "Yes":
        fig = px.box(edf, color="gender",
                     color_discrete_map={"Male":C_TECHNOVA,"Female":C_MARKET}, **bkw)
    else:
        fig = px.box(edf, color_discrete_sequence=[C_TECHNOVA], **bkw)

    for val, dash, clr, lbl in [
        (p25,"dash", P_LIGHT_AMB, "BLS P25 (national)"),
        (p50,"dot",  C_MARKET,    "BLS P50 (national)"),
        (p75,"dash", P_LIGHT_GRN, "BLS P75 (national)"),
    ]:
        if val:
            fig.add_hline(y=val, line_dash=dash, line_color=clr, line_width=2,
                          annotation_text=f"  {lbl}: {fmt_c(val)}",
                          annotation_font_color=clr,
                          annotation_font_size=11,
                          annotation_position="top right")

    clayout(fig, title=f"Salary Distribution by Level — {fam}",
            xtitle="Job Level (n = headcount)", ytitle="Annual Salary (USD)")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Summary Statistics by Level**")
    stats = (edf.groupby("job_level")["annual_base_salary"]
             .agg(Headcount="count",Min="min",Median="median",Mean="mean",Max="max",Std="std")
             .reset_index())
    stats["job_level"] = pd.Categorical(stats.job_level, categories=LEVEL_ORDER, ordered=True)
    stats = stats.sort_values("job_level").rename(columns={"job_level":"Level"})
    st.dataframe(stats.style.format(
        {c:fmt_c for c in ["Min","Median","Mean","Max","Std"]}),
        use_container_width=True, hide_index=True)

    if split_g == "Yes":
        st.markdown("**Average Salary by Gender and Level**")
        piv = (edf.groupby(["job_level","gender"])["annual_base_salary"]
               .mean().unstack(fill_value=0).reset_index())
        piv["job_level"] = pd.Categorical(piv.job_level, categories=LEVEL_ORDER, ordered=True)
        piv = piv.sort_values("job_level").rename(columns={"job_level":"Level"})
        if "Male" in piv.columns and "Female" in piv.columns:
            piv["Gap $"] = piv.Male - piv.Female
            piv["Gap %"] = (piv["Gap $"]/piv.Male*100).round(1)
        fmt_map = {c:fmt_c for c in ["Male","Female","Gap $"] if c in piv.columns}
        if "Gap %" in piv.columns: fmt_map["Gap %"] = fmt_p
        st.dataframe(piv.style.format(fmt_map), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — GEOGRAPHIC PAY DIFFERENTIALS
# ══════════════════════════════════════════════════════════════════════════════

def page5():
    header("Geographic Pay Differentials",
           "BLS P50 across TechNova's 5 cities — for geo-differential offer planning and location strategy.")

    fams   = distinct("internal_job_grades","job_family")
    levels = sort_levels(distinct("internal_job_grades","job_level"))
    if not fams or not levels: nodata("Required grade data is missing."); return

    c1,c2 = st.columns(2)
    fam = c1.selectbox("Job Family", fams, key="p5_fam")
    lvl = c2.selectbox("Job Level",  levels, key="p5_lvl")

    xr = pick_crosswalk(fam, lvl)
    if not xr: nodata("No SOC mapping found."); return

    gdf = query_df("SELECT AVG(band_midpoint) mid FROM internal_job_grades WHERE job_family=%s AND job_level=%s", (fam,lvl))
    imid = to_num(gdf.iloc[0]["mid"]) if not gdf.empty else None

    bdf = query_df("""
        SELECT msa_name,msa_code,pct_25,pct_50,pct_75
        FROM bls_wage_data
        WHERE soc_code=%s
          AND reference_year=(SELECT MAX(reference_year) FROM bls_wage_data WHERE soc_code=%s)
          AND pct_50 IS NOT NULL
        ORDER BY pct_50 DESC""", (xr["soc_code"],xr["soc_code"]))
    if bdf.empty: nodata("No BLS P50 data across MSAs."); return

    top = bdf.iloc[0]; bot = bdf.iloc[-1]
    natl_avg = to_num(bdf["pct_50"].mean())

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Highest Market", fmt_c(top["pct_50"]), delta=short_msa(top["msa_name"]))
    c2.metric("Lowest Market",  fmt_c(bot["pct_50"]), delta=short_msa(bot["msa_name"]))
    c3.metric("5-City Avg (BLS P50)", fmt_c(natl_avg))
    c4.metric("TechNova Midpoint",    fmt_c(imid))

    # Horizontal bar chart
    sdf = bdf.sort_values("pct_50", ascending=True).copy()
    sdf["city"] = sdf["msa_name"].apply(short_msa)
    fig = go.Figure(go.Bar(
        x=sdf["pct_50"], y=sdf["city"], orientation="h",
        marker_color=C_SINGLE,
        text=[fmt_c(v) for v in sdf["pct_50"]],
        textposition="outside", textfont=dict(color=ST_TEXT, size=11),
        name="BLS P50",
    ))
    if imid:
        fig.add_vline(x=imid, line_color=C_MARKET, line_width=2.5, line_dash="dash",
                      annotation_text=f"TechNova Midpoint<br>{fmt_c(imid)}",
                      annotation_font_color=C_MARKET,
                      annotation_bgcolor="rgba(30,30,40,0.85)",
                      annotation_bordercolor=C_MARKET,
                      annotation_font_size=11,
                      annotation_position="top right")
    clayout(fig, title=f"BLS P50 by City — {fam} {lvl}  |  SOC {xr['soc_code']}",
            xtitle="Annual Salary (USD)", ytitle="", h=360)
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

    # Comparison table — P25/P50/P75 + vs 5-city avg + vs TechNova
    st.markdown("**City-by-City Breakdown**")
    comp = sdf.sort_values("pct_50", ascending=False).copy()
    comp["vs Avg $"]       = comp["pct_50"].apply(lambda x: (to_num(x)-natl_avg) if natl_avg and to_num(x) else None)
    comp["vs Avg %"]       = comp["vs Avg $"].apply(lambda x: (x/natl_avg*100) if natl_avg and x is not None else None)
    comp["vs TechNova $"]  = comp["pct_50"].apply(lambda x: (to_num(x)-imid) if imid and to_num(x) else None)
    comp["vs TechNova %"]  = comp["vs TechNova $"].apply(lambda x: (x/imid*100) if imid and x is not None else None)

    show = comp[["city","pct_25","pct_50","pct_75","vs Avg $","vs Avg %","vs TechNova $","vs TechNova %"]]
    show = show.rename(columns={"city":"City","pct_25":"BLS P25","pct_50":"BLS P50","pct_75":"BLS P75"})
    st.dataframe(
        show.style.format({
            "BLS P25":fmt_c,"BLS P50":fmt_c,"BLS P75":fmt_c,
            "vs Avg $":fmt_c,"vs Avg %":fmt_p,
            "vs TechNova $":fmt_c,"vs TechNova %":fmt_p,
        })
        .map(tcolor_pos_neg, subset=["vs Avg %","vs TechNova %"]),
        use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — COMPA-RATIO ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

def page6():
    header("Compa-Ratio Analysis",
           "Actual Salary ÷ Grade Midpoint. Target range: 0.85–1.15.")

    base = query_df("""
        WITH gm AS (
            SELECT job_family,job_level,AVG(band_midpoint) mid
            FROM internal_job_grades GROUP BY job_family,job_level
        )
        SELECT e.employee_id, e.job_family, e.job_level, e.office_location,
               e.gender, e.annual_base_salary, gm.mid band_midpoint,
               CASE WHEN gm.mid>0 THEN e.annual_base_salary/gm.mid ELSE NULL END compa_ratio
        FROM employees e
        JOIN gm ON e.job_family=gm.job_family AND e.job_level=gm.job_level""")
    if base.empty: nodata("No compa-ratio data available."); return

    fams = sorted(base.job_family.dropna().unique().tolist())
    lvls = sort_levels(sorted(base.job_level.dropna().unique().tolist()))
    offs = sorted(base.office_location.dropna().unique().tolist())

    # ── Filters: 3 side-by-side multiselects, each with All/None buttons ──────
    for k, opts in [("p6_fams",fams),("p6_lvls",lvls),("p6_offs",offs)]:
        if k not in st.session_state:
            st.session_state[k] = opts[:]

    c1, c2, c3 = st.columns(3)

    with c1:
        ba, bn = st.columns(2)
        if ba.button("All",  key="p6_fa"): st.session_state["p6_fams"] = fams[:]; st.rerun()
        if bn.button("None", key="p6_fn"): st.session_state["p6_fams"] = [];     st.rerun()
        sel_fams = st.multiselect("Job Family", fams, key="p6_fams")

    with c2:
        ba2, bn2 = st.columns(2)
        if ba2.button("All",  key="p6_la"): st.session_state["p6_lvls"] = lvls[:]; st.rerun()
        if bn2.button("None", key="p6_ln"): st.session_state["p6_lvls"] = [];      st.rerun()
        sel_lvls = st.multiselect("Job Level", lvls, key="p6_lvls")

    with c3:
        ba3, bn3 = st.columns(2)
        if ba3.button("All",  key="p6_oa"): st.session_state["p6_offs"] = offs[:]; st.rerun()
        if bn3.button("None", key="p6_on"): st.session_state["p6_offs"] = [];      st.rerun()
        sel_offs = st.multiselect("Office Location", offs, key="p6_offs")

    # Active exclusions hint
    excl = []
    if len(sel_fams)<len(fams): excl.append(f"Excl. families: {', '.join(f for f in fams if f not in sel_fams)}")
    if len(sel_lvls)<len(lvls): excl.append(f"Excl. levels: {', '.join(l for l in lvls if l not in sel_lvls)}")
    if len(sel_offs)<len(offs): excl.append(f"Excl. offices: {', '.join(o for o in offs if o not in sel_offs)}")
    if excl: st.caption("🔕 " + "  |  ".join(excl))

    df = base[base.job_family.isin(sel_fams) & base.job_level.isin(sel_lvls) & base.office_location.isin(sel_offs)].copy()
    df = df[df.compa_ratio.notna()]
    if df.empty: nodata("No employees match the selected filters."); return

    und_p  = (df.compa_ratio < 0.85).mean()*100
    ovr_p  = (df.compa_ratio > 1.15).mean()*100
    rng_p  = 100-und_p-ovr_p
    avg_cr = df.compa_ratio.mean()

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("% Below 0.85 (Underpaid)", fmt_p(und_p),
              delta=f"{int(df.compa_ratio.lt(0.85).sum())} employees")
    c2.metric("% Above 1.15 (Overpaid)",  fmt_p(ovr_p),
              delta=f"{int(df.compa_ratio.gt(1.15).sum())} employees")
    c3.metric("% In Target Range", fmt_p(rng_p))
    c4.metric("Avg Compa-Ratio", f"{avg_cr:.2f}", delta="Target: 1.00")

    # Histogram
    fig = px.histogram(df, x="compa_ratio", nbins=35, color_discrete_sequence=[C_SINGLE])
    fig.add_vline(x=0.85, line_dash="dash", line_color=P_LIGHT_RED, line_width=2,
                  annotation_text="  0.85 (underpaid threshold)",
                  annotation_font_color=P_LIGHT_RED, annotation_font_size=11,
                  annotation_position="top right")
    fig.add_vline(x=1.15, line_dash="dash", line_color=P_LIGHT_AMB, line_width=2,
                  annotation_text="  1.15 (overpaid threshold)",
                  annotation_font_color=P_LIGHT_AMB, annotation_font_size=11,
                  annotation_position="top right")
    fig.add_vrect(x0=0.85, x1=1.15,
                  fillcolor="rgba(77,182,172,0.08)", layer="below", line_width=0,
                  annotation_text="Target range", annotation_position="top left",
                  annotation_font_color=C_TECHNOVA, annotation_font_size=11)
    clayout(fig, title="Compa-Ratio Distribution",
            xtitle="Compa-Ratio", ytitle="Employee Count")
    st.plotly_chart(fig, use_container_width=True)

    # Summary table
    summary = (df.groupby(["job_family","job_level"], as_index=False)
               .agg(avg_cr=("compa_ratio","mean"), n=("employee_id","count")))
    bl = (df.assign(b=df.compa_ratio<0.85)
          .groupby(["job_family","job_level"],as_index=False)["b"].mean()
          .rename(columns={"b":"pct_b"}))
    ab = (df.assign(a=df.compa_ratio>1.15)
          .groupby(["job_family","job_level"],as_index=False)["a"].mean()
          .rename(columns={"a":"pct_a"}))
    summary = summary.merge(bl, on=["job_family","job_level"]).merge(ab, on=["job_family","job_level"])
    summary["pct_b"] *= 100; summary["pct_a"] *= 100
    summary["job_level"] = pd.Categorical(summary.job_level, categories=LEVEL_ORDER, ordered=True)
    summary = summary.sort_values(["job_family","job_level"]).rename(columns={
        "job_family":"Job Family","job_level":"Level",
        "avg_cr":"Avg Compa-Ratio","pct_b":"% Below 0.85","pct_a":"% Above 1.15","n":"Headcount"})

    st.dataframe(summary.style.format({
        "Avg Compa-Ratio": lambda x: f"{to_num(x):.2f}" if to_num(x) else "N/A",
        "% Below 0.85":fmt_p, "% Above 1.15":fmt_p,
    })
    .map(tcolor_compa,    subset=["Avg Compa-Ratio"])
    .map(tcolor_pct_below, subset=["% Below 0.85"])
    .map(tcolor_pct_above, subset=["% Above 1.15"]),
    use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — LEVEL PROGRESSION VIEW
# ══════════════════════════════════════════════════════════════════════════════

def page7():
    header("Level Progression View",
           "Salary band staircase L1→L6 vs BLS market medians — for compensation strategy and band design.")

    fams = distinct("internal_job_grades","job_family")
    if not fams: nodata("No job family data."); return
    fam = st.selectbox("Job Family", sorted(fams), key="p7_fam")

    bdf = query_df("""
        SELECT job_level,
               AVG(band_minimum) lo, AVG(band_midpoint) mid, AVG(band_maximum) hi
        FROM internal_job_grades WHERE job_family=%s GROUP BY job_level""", (fam,))
    if bdf.empty: nodata("No grade rows found."); return
    bdf["job_level"] = pd.Categorical(bdf.job_level, categories=LEVEL_ORDER, ordered=True)
    bdf = bdf.sort_values("job_level")

    cdf = crosswalk_candidates(fam)
    all_socs = sorted(cdf.soc_code.dropna().unique().tolist()) if not cdf.empty else []

    pts = []
    for lvl in LEVEL_ORDER:
        socs = all_socs
        if not cdf.empty:
            lr = cdf[cdf.job_level_applicability.apply(lambda x: level_in(lvl,x))]
            if not lr.empty: socs = sorted(lr.soc_code.dropna().unique().tolist())
        pts.append({"job_level":lvl, "bls_p50": bls_avg_soc(socs)})

    plot = pd.merge(bdf, pd.DataFrame(pts), on="job_level", how="left").sort_values("job_level")
    x = plot["job_level"].astype(str).tolist()

    fig = go.Figure()
    # Shaded band
    fig.add_trace(go.Scatter(x=x, y=plot["hi"], mode="lines", line=dict(width=0),
                             showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=x, y=plot["lo"], mode="lines", fill="tonexty",
                             line=dict(width=0), fillcolor=C_BAND,
                             name="Band Range (Min–Max)"))
    # Midpoint
    fig.add_trace(go.Scatter(x=x, y=plot["mid"], mode="lines+markers",
                             line=dict(color=C_TECHNOVA, width=3),
                             marker=dict(size=9, color=C_TECHNOVA, symbol="circle"),
                             name="TechNova Midpoint",
                             text=[fmt_c(v) for v in plot["mid"]],
                             hovertemplate="%{text}<extra>TechNova Midpoint</extra>"))
    # BLS P50
    fig.add_trace(go.Scatter(x=x, y=plot["bls_p50"], mode="lines+markers",
                             line=dict(color=C_MARKET, width=3, dash="dot"),
                             marker=dict(size=9, color=C_MARKET, symbol="diamond"),
                             name="BLS P50 (National Avg)",
                             text=[fmt_c(v) for v in plot["bls_p50"]],
                             hovertemplate="%{text}<extra>BLS P50</extra>"))
    clayout(fig, title=f"Level Progression — {fam}",
            xtitle="Job Level", ytitle="Annual Salary (USD)", h=480)
    st.plotly_chart(fig, use_container_width=True)

    # Commentary
    outpaced = plot[plot.bls_p50.notna() & plot.hi.notna() & (plot.bls_p50>plot.hi)].job_level.astype(str).tolist()
    lagging  = plot[plot.bls_p50.notna() & plot.mid.notna() & (plot.bls_p50>plot.mid) & ~plot.job_level.astype(str).isin(outpaced)].job_level.astype(str).tolist()
    leading  = plot[plot.bls_p50.notna() & plot.mid.notna() & (plot.mid>plot.bls_p50)].job_level.astype(str).tolist()

    if outpaced:
        st.error(f"⚠️ **Market has outpaced the band entirely at {', '.join(outpaced)}.** "
                 "BLS P50 exceeds the band maximum — new hires may need above-band offers "
                 "and existing employees may be at flight risk. Immediate band review recommended.")
    if lagging:
        st.warning(f"🟡 **Midpoint lags market median at {', '.join(lagging)}.** "
                   "TechNova's midpoint is below BLS P50 — consider a band refresh in the next compensation cycle.")
    if leading:
        st.success(f"✅ **Above market median at {', '.join(leading)}.** "
                   "TechNova's midpoint exceeds BLS P50 — strong competitive positioning at these levels.")
    if not outpaced and not lagging:
        st.success("✅ All levels are at or above BLS market median. No immediate band adjustments required.")

    # Detail table
    st.markdown("**Level-by-Level Detail**")
    det = plot[["job_level","lo","mid","hi","bls_p50"]].copy()
    det["BLS vs Mid $"] = det.apply(lambda r: (to_num(r.bls_p50)-to_num(r["mid"])) if to_num(r.bls_p50) and to_num(r["mid"]) else None, axis=1)
    det["BLS vs Mid %"] = det.apply(lambda r: (r["BLS vs Mid $"]/to_num(r["mid"])*100) if r["BLS vs Mid $"] is not None and to_num(r["mid"]) else None, axis=1)
    det = det.rename(columns={"job_level":"Level","lo":"Band Min","mid":"Band Midpoint","hi":"Band Max","bls_p50":"BLS P50"})
    st.dataframe(det.style.format({
        "Band Min":fmt_c,"Band Midpoint":fmt_c,"Band Max":fmt_c,
        "BLS P50":fmt_c,"BLS vs Mid $":fmt_c,"BLS vs Mid %":fmt_p,
    }).map(tcolor_gap_7, subset=["BLS vs Mid %"]),
    use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 8 — DATA MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

EMP_COLS    = ["employee_id","first_name","last_name","gender","hire_date","job_family",
               "role_title","job_level","department","office_location","msa_name","msa_code",
               "annual_base_salary","salary_currency","data_as_of_date"]
EMP_SAMPLE  = ["EMP9999","Jane","Doe","Female","2024-01-15","Software Engineering",
               "Software Engineer - Backend","L3","Engineering","Austin TX",
               "Austin-Round Rock-Georgetown TX",12420,130000,"USD","2024-12-31"]

GRD_COLS    = ["grade_code","job_family","role_title","job_level","band_minimum",
               "band_midpoint","band_maximum","salary_currency","geo_scope",
               "below_market_flag","effective_date","last_reviewed_date"]
GRD_SAMPLE  = ["SWE-L3-NATIONAL","Software Engineering","Software Engineer - Backend","L3",
               110000,130000,155000,"USD","NATIONAL","NO","2024-01-01","2024-12-31"]

def make_csv(cols, sample): return pd.DataFrame([dict(zip(cols,sample))]).to_csv(index=False).encode()

def upsert_df(conn, df, sql):
    written = 0
    with conn.cursor() as cur:
        for rec in df.to_dict(orient="records"):
            clean = {k:(None if isinstance(v,float) and pd.isna(v) else v) for k,v in rec.items()}
            cur.execute(sql, clean); written += 1
    conn.commit(); return written


def page8():
    header("Data Management",
           "Upload source files, trigger BLS refreshes, and inspect pipeline run history.")

    # Section 1: Templates
    st.subheader("📥 Section 1 — Download CSV Templates")
    st.markdown("Download the template for your data type, fill it in, then upload in Section 2.")
    tc1, tc2 = st.columns(2)
    tc1.download_button("⬇️ Employee Template", data=make_csv(EMP_COLS,EMP_SAMPLE),
                        file_name="technova_employees_template.csv", mime="text/csv")
    tc1.caption("Key columns: " + ", ".join(EMP_COLS[:7]) + "…")
    tc2.download_button("⬇️ Job Grades Template", data=make_csv(GRD_COLS,GRD_SAMPLE),
                        file_name="technova_job_grades_template.csv", mime="text/csv")
    tc2.caption("Key columns: " + ", ".join(GRD_COLS[:7]) + "…")

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    # Section 2: Upload
    st.subheader("📤 Section 2 — Upload Employee / Grade Data")
    st.warning("🔒 Files containing real salary data are confidential. "
               "Do not store completed CSV files in shared or unsecured locations.")

    tbl = st.selectbox("Target Table", ["Employees","Job Grades"], key="p8_tbl")
    up  = st.file_uploader("Upload CSV", type=["csv"], key="p8_up")

    if up:
        try: dfu = pd.read_csv(io.BytesIO(up.getvalue()))
        except Exception as e: st.error(f"Could not read CSV: {e}"); dfu=None
        if dfu is not None:
            st.write(f"Rows detected: **{len(dfu):,}**")
            if tbl == "Employees":
                errs, ptype, usql = validate_employees(dfu), "CSV_EMPLOYEES", EMPLOYEE_UPSERT_SQL
            else:
                errs, ptype, usql = validate_job_grades(dfu), "CSV_GRADES", GRADES_UPSERT_SQL

            if errs:
                st.error("Validation failed:")
                for e in errs: st.write(f"- {e}")
            else:
                st.success("✅ Validation passed.")
                if st.button("Confirm and Load", key="p8_load"):
                    t0=time.perf_counter(); conn=None; wr=0
                    try:
                        dl = dfu.copy()
                        if tbl=="Employees":
                            dl["annual_base_salary"] = pd.to_numeric(dl["annual_base_salary"],errors="coerce")
                            dl["msa_code"] = pd.to_numeric(dl["msa_code"],errors="coerce")
                        else:
                            for c in ["band_minimum","band_midpoint","band_maximum"]:
                                dl[c] = pd.to_numeric(dl[c],errors="coerce")
                        conn=get_connection(); wr=upsert_df(conn,dl,usql)
                        dur=time.perf_counter()-t0
                        log_csv_pipeline_run(conn,pipeline_type=ptype,status="SUCCESS",
                            records_requested=len(dl),records_received=len(dl),
                            records_written=wr,run_duration_seconds=dur,error_message=None)
                        st.success(f"✅ {wr:,} records written in {dur:.2f}s")
                        st.cache_data.clear()
                    except Exception as e:
                        dur=time.perf_counter()-t0
                        if conn:
                            try: conn.rollback(); log_csv_pipeline_run(conn,pipeline_type=ptype,
                                status="FAILED",records_requested=len(dfu),records_received=len(dfu),
                                records_written=wr,run_duration_seconds=dur,error_message=str(e))
                            except: pass
                        st.error(f"Load failed after {wr:,} rows: {e}")
                    finally:
                        if conn: conn.close()

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    # Section 3: BLS Refresh
    st.subheader("🔄 Section 3 — Refresh BLS Market Data")
    st.info("ℹ️ Triggers a live BLS API call (up to 600 series). "
            "Registered key allows 500 requests/day. Do not run more than once daily.")
    if st.button("🔄 Refresh BLS Data Now", key="p8_bls"):
        with st.spinner("Running BLS pipeline — typically 30–60 seconds…"):
            t0=time.perf_counter(); err=None
            try: bls_pipeline.main()
            except SystemExit as e:
                if e.code not in (0,None): err=f"Exit code {e.code}"
            except Exception as e: err=str(e)
            dur=time.perf_counter()-t0
        st.cache_data.clear()
        lr = query_df("""
            SELECT run_timestamp,status,records_written,run_duration_seconds,error_message
            FROM pipeline_run_log WHERE pipeline_type='BLS_OEWS'
            ORDER BY run_timestamp DESC LIMIT 1""")
        if lr.empty:
            st.error(f"Refresh failed: {err or 'No run log entry found'}")
        else:
            r=lr.iloc[0]; s=r["status"]
            msg=(f"Status: {s}  |  Records: {int(r.records_written or 0):,}  |  "
                 f"Duration: {to_num(r.run_duration_seconds) or dur:.2f}s")
            st.success(msg) if s=="SUCCESS" else st.error(f"{msg}  |  {r.error_message or err}")

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    # Section 4: Run History
    st.subheader("📋 Section 4 — Pipeline Run History")
    hist = query_df("""
        SELECT run_timestamp,pipeline_type,status,records_written,
               run_duration_seconds,error_message
        FROM pipeline_run_log ORDER BY run_timestamp DESC LIMIT 10""")
    if hist.empty: nodata("No pipeline run history."); return
    hist=hist.rename(columns={
        "run_timestamp":"Timestamp","pipeline_type":"Type","status":"Status",
        "records_written":"Records","run_duration_seconds":"Duration (s)","error_message":"Error"})

    def sstatus(v):
        s=str(v).upper()
        if s=="SUCCESS": return f"background-color:{P_GREEN}; color:white; font-weight:600;"
        if s=="FAILED":  return f"background-color:{P_RED};   color:white; font-weight:600;"
        return ""

    st.dataframe(hist.style
        .format({"Duration (s)": lambda x: f"{to_num(x):.2f}" if to_num(x) else "N/A"})
        .map(sstatus, subset=["Status"]),
        use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def sidebar():
    with st.sidebar:
        st.markdown("""
        <div class="sidebar-logo">
          <h2>🏥 TechNova</h2>
          <p>Compensation Intelligence Dashboard</p>
        </div>""", unsafe_allow_html=True)

        pages = [
            "1. Benchmarking View",
            "2. Market Position Flags",
            "3. Pay Equity Module",
            "4. Salary Distribution",
            "5. Geographic Pay Differentials",
            "6. Compa-Ratio Analysis",
            "7. Level Progression View",
            "8. Data Management",
        ]
        choice = st.radio("Navigate", pages, key="nav")

        st.markdown("---")
        st.markdown("**Data Freshness**")
        bls_ts = last_success("BLS_OEWS")
        csv_ts = last_success("CSV_EMPLOYEES")
        st.caption("🌐 BLS:  " + (bls_ts.strftime("%b %d, %Y %H:%M") if bls_ts else "—"))
        st.caption("📄 CSV: " + (csv_ts.strftime("%b %d, %Y %H:%M") if csv_ts else "—"))

    return choice


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    pg = sidebar()
    {
        "1.": page1, "2.": page2, "3.": page3, "4.": page4,
        "5.": page5, "6.": page6, "7.": page7, "8.": page8,
    }.get(pg[:2], page1)()


if __name__ == "__main__":
    main()