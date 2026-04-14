#!/usr/bin/env python3
"""
Build GSS Intelligence Dashboard
NAM Intelligence Pipeline

Generates a self-contained interactive HTML dashboard at
data/exports/GSS_Intelligence_Dashboard.html
"""

import json
import sys
from collections import Counter
from pathlib import Path

# Resolve the project root: walk up from this file to find the directory
# that contains 'data/exports/companies_all.csv'. This handles both the
# main project tree and git worktrees.
def _find_project_root() -> Path:
    candidate = Path(__file__).resolve().parent.parent
    # Check current candidate first
    if (candidate / "data" / "exports" / "companies_all.csv").exists():
        return candidate
    # Check main project if we are in a worktree
    main = candidate
    while main != main.parent:
        if (main / "data" / "exports" / "companies_all.csv").exists():
            return main
        # Also check sibling trees (worktrees live in .claude/worktrees/<name>)
        if main.name == "worktrees":
            break
        main = main.parent
    # Final fallback: the canonical main project path
    fallback = Path(__file__).resolve().parents[4]  # up from scripts/
    if (fallback / "data" / "exports" / "companies_all.csv").exists():
        return fallback
    return candidate

BASE_DIR = _find_project_root()
# Insert the main project scripts dir so abm_shared can be imported
sys.path.insert(0, str(BASE_DIR / "scripts"))
sys.path.insert(0, str(BASE_DIR))

from abm_shared import (
    assign_tier,
    compute_icp_score,
    detect_competitor,
    get_associations_list,
    get_contacts,
    get_email_provider,
    get_employee_count,
    get_spf_list,
    get_tech_stack,
    load_and_merge_data,
    load_competitors,
    load_events,
)

OUTPUT_PATH = BASE_DIR / "data" / "exports" / "GSS_Intelligence_Dashboard.html"

# ── Segment filter functions ───────────────────────────────────────────


MARTECH_KEYWORDS = {"hubspot", "marketo", "mailchimp", "pardot", "activecampaign", "constant contact"}


def filter_salesforce(rec):
    spf = [s.lower() for s in get_spf_list(rec)]
    return any(s in ("salesforce", "pardot") for s in spf)


def filter_legacy_email(rec):
    ep = get_email_provider(rec).lower()
    return ep in ("self-hosted", "self-hosted (on-premise)", "other", "") or not ep


def filter_microsoft_365(rec):
    return get_email_provider(rec) == "Microsoft 365"


def filter_marketing_automation(rec):
    spf = [s.strip().lower() for s in get_spf_list(rec)]
    return any(s in MARTECH_KEYWORDS for s in spf)


def filter_small_mfg(rec):
    ec_max = rec.get("employee_count_max", 0) or 0
    try:
        ec_max = int(ec_max)
    except (ValueError, TypeError):
        ec_max = 0
    website = (rec.get("website") or "").strip()
    return 0 < ec_max <= 100 and bool(website)


def filter_large_mfg(rec):
    ec_min = rec.get("employee_count_min", 0) or 0
    try:
        ec_min = int(ec_min)
    except (ValueError, TypeError):
        ec_min = 0
    return ec_min >= 500


def filter_pma_premium(rec):
    tier = (rec.get("membership_tier") or "").upper()
    return tier in ("PLATINUM", "GOLD")


SEGMENTS = [
    ("Salesforce Users", filter_salesforce),
    ("Legacy Email", filter_legacy_email),
    ("Microsoft 365", filter_microsoft_365),
    ("Marketing Automation", filter_marketing_automation),
    ("Small Mfg", filter_small_mfg),
    ("Large Mfg", filter_large_mfg),
    ("PMA Premium", filter_pma_premium),
]

# ── Stats computation ──────────────────────────────────────────────────


def compute_stats(records):
    """Pre-compute all dashboard statistics."""

    total = len(records)

    # Quality scores
    scores = [r.get("quality_score", 0) or 0 for r in records]
    scores = [int(s) for s in scores]
    avg_score = round(sum(scores) / total, 1) if total else 0
    high_quality = sum(1 for s in scores if s >= 70)

    # Enrichment coverage
    enriched = sum(1 for r in records if r.get("enrichment_status") == "complete")
    enrichment_pct = round(enriched / total * 100, 1) if total else 0

    # Contact coverage
    has_contacts = sum(1 for r in records if get_contacts(r))
    contact_pct = round(has_contacts / total * 100, 1) if total else 0

    # Tech stack detected
    has_tech = sum(1 for r in records if get_tech_stack(r))
    tech_pct = round(has_tech / total * 100, 1) if total else 0

    # By association
    assoc_counter = Counter()
    for r in records:
        for a in get_associations_list(r):
            if a.strip():
                assoc_counter[a.strip()] += 1

    by_association = [{"label": k, "value": v}
                      for k, v in sorted(assoc_counter.items(), key=lambda x: -x[1])]

    # Quality distribution (10 buckets: 0-9, 10-19, ... 90-99, 100)
    buckets = [0] * 10
    for s in scores:
        idx = min(int(s) // 10, 9)
        buckets[idx] += 1
    quality_distribution = [{"label": f"{i*10}-{i*10+9}", "value": buckets[i]} for i in range(10)]

    # Email providers
    ep_counter = Counter()
    for r in records:
        ep = get_email_provider(r)
        if not ep:
            ep = "Unknown"
        ep_counter[ep] += 1

    top_eps = ep_counter.most_common(8)
    other_count = sum(v for k, v in ep_counter.items() if k not in dict(top_eps))
    email_providers = [{"label": k, "value": v} for k, v in top_eps]
    if other_count > 0:
        email_providers.append({"label": "Other", "value": other_count})

    # By state
    state_counter = Counter()
    for r in records:
        state = (r.get("state") or "").strip()
        if state and len(state) <= 3:
            state_counter[state] += 1

    by_state = [{"label": k, "value": v}
                for k, v in state_counter.most_common(15)]

    # Top tech stack items
    tech_counter = Counter()
    for r in records:
        for t in get_tech_stack(r):
            if t.strip():
                tech_counter[t.strip()] += 1

    top_tech = [{"label": k, "value": v}
                for k, v in tech_counter.most_common(10)]

    # SPF services
    spf_counter = Counter()
    for r in records:
        for s in get_spf_list(r):
            if s.strip():
                spf_counter[s.strip()] += 1

    top_spf = [{"label": k, "value": v}
               for k, v in spf_counter.most_common(10)]

    # Segments
    segment_counts = []
    for seg_name, seg_fn in SEGMENTS:
        count = sum(1 for r in records if seg_fn(r))
        segment_counts.append({
            "name": seg_name,
            "count": count,
            "pct": round(count / total * 100, 1) if total else 0,
        })

    return {
        "total": total,
        "avg_score": avg_score,
        "high_quality": high_quality,
        "enrichment_pct": enrichment_pct,
        "contact_pct": contact_pct,
        "tech_pct": tech_pct,
        "by_association": by_association,
        "quality_distribution": quality_distribution,
        "email_providers": email_providers,
        "by_state": by_state,
        "top_tech": top_tech,
        "top_spf": top_spf,
        "segments": segment_counts,
    }


def compress_company(rec, icp_data):
    """Compress company record to abbreviated keys for reduced payload."""
    contacts = get_contacts(rec)
    primary_contact = ""
    primary_email = ""
    if contacts:
        c = contacts[0]
        if isinstance(c, dict):
            primary_contact = c.get("name", "")
            primary_email = c.get("email", "")

    ec_min, ec_max = get_employee_count(rec)
    emp_label = ""
    if ec_max and ec_max > 0:
        emp_label = f"{ec_min}-{ec_max}" if ec_min else f"<{ec_max}"
    elif ec_min and ec_min > 0:
        emp_label = f"{ec_min}+"

    return {
        "n": rec.get("company_name", ""),           # name
        "d": (rec.get("domain") or "").strip(),      # domain
        "w": (rec.get("website") or "").strip(),     # website
        "ci": (rec.get("city") or "").strip(),       # city
        "st": (rec.get("state") or "").strip(),      # state
        "str": (rec.get("street") or "").strip(),    # street
        "z": (rec.get("zip_code") or "").strip(),    # zip
        "ph": (rec.get("phone") or "").strip(),      # phone
        "as": "; ".join(get_associations_list(rec)), # associations
        "ep": get_email_provider(rec),               # email_provider
        "ts": get_tech_stack(rec)[:8],               # tech_stack (cap at 8)
        "sp": get_spf_list(rec)[:6],                 # spf_services (cap at 6)
        "cm": (rec.get("cms") or "").strip(),        # cms
        "qs": rec.get("quality_score", 0) or 0,     # quality_score
        "qg": (rec.get("quality_grade") or "").strip(), # quality_grade
        "ic": icp_data["icp_score"],                 # icp_score
        "tr": assign_tier(
            icp_data["icp_score"],
            bool(contacts),
            rec.get("quality_score", 0) or 0
        ),
        "pc": primary_contact,                       # primary_contact_name
        "pe": primary_email,                         # primary_contact_email
        "em": emp_label,                             # employee_range
        "es": (rec.get("enrichment_status") or "").strip(), # enrichment_status
        "co": detect_competitor(rec),                # competitor_detected
    }


def build_dashboard_data(records, events, competitors):
    """Build the full DASHBOARD_DATA payload."""
    print(f"  Computing stats for {len(records)} records...")
    stats = compute_stats(records)

    print("  Computing ICP scores and compressing records...")
    companies = []
    for rec in records:
        icp_data = compute_icp_score(rec)
        companies.append(compress_company(rec, icp_data))

    # Sort by ICP score desc, then quality desc
    companies.sort(key=lambda c: (-c["ic"], -c["qs"]))

    # Compress events
    compressed_events = []
    for e in events:
        compressed_events.append({
            "n": e.get("event_name", ""),
            "dt": e.get("dates", ""),
            "ci": e.get("city", ""),
            "v": e.get("venue", ""),
            "at": e.get("attendance", ""),
            "in": e.get("industry", ""),
            "p": e.get("priority", ""),
            "no": e.get("notes", ""),
        })

    # Compress competitors
    compressed_competitors = []
    for c in competitors:
        compressed_competitors.append({
            "n": c.get("competitor", ""),
            "pr": c.get("presence", ""),
            "sn": c.get("strategy_notes", ""),
            "tl": c.get("threat_level", ""),
        })

    return {
        "stats": stats,
        "companies": companies,
        "events": compressed_events,
        "competitors": compressed_competitors,
        "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ── HTML Template ──────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GSS Intelligence Dashboard — NAM Pipeline</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --navy: #1F4E79;
    --navy-light: #2563a8;
    --navy-dark: #163b5c;
    --accent: #E8A415;
    --accent-light: #f5c842;
    --bg: #f0f4f8;
    --card-bg: #ffffff;
    --text: #1a2332;
    --text-muted: #64748b;
    --border: #e2e8f0;
    --success: #16a34a;
    --warning: #d97706;
    --danger: #dc2626;
    --info: #0891b2;
    --tier1: #15803d;
    --tier2: #b45309;
    --tier3: #c2410c;
    --shadow: 0 2px 8px rgba(31,78,121,0.10);
    --shadow-lg: 0 4px 20px rgba(31,78,121,0.15);
    --radius: 8px;
    --header-h: 64px;
  }
  [data-theme="dark"] {
    --bg: #0f1929;
    --card-bg: #1a2840;
    --text: #e2e8f0;
    --text-muted: #94a3b8;
    --border: #2d4060;
    --shadow: 0 2px 8px rgba(0,0,0,0.3);
    --shadow-lg: 0 4px 20px rgba(0,0,0,0.4);
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    font-size: 14px;
    line-height: 1.5;
    transition: background 0.2s, color 0.2s;
  }

  /* ── Header ── */
  .header {
    background: linear-gradient(135deg, var(--navy-dark) 0%, var(--navy) 60%, var(--navy-light) 100%);
    color: #fff;
    padding: 0 24px;
    height: var(--header-h);
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 2px 12px rgba(0,0,0,0.3);
  }
  .header-left { display: flex; align-items: center; gap: 12px; }
  .header-logo {
    width: 36px; height: 36px;
    background: var(--accent);
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-weight: 900; font-size: 16px; color: var(--navy-dark);
  }
  .header-title { font-size: 18px; font-weight: 700; letter-spacing: -0.3px; }
  .header-subtitle { font-size: 11px; opacity: 0.75; margin-top: 1px; }
  .header-right { display: flex; align-items: center; gap: 12px; }
  .gen-time { font-size: 11px; opacity: 0.6; }
  .theme-toggle {
    background: rgba(255,255,255,0.15);
    border: 1px solid rgba(255,255,255,0.25);
    color: #fff;
    border-radius: 6px;
    padding: 6px 12px;
    cursor: pointer;
    font-size: 12px;
    transition: background 0.15s;
  }
  .theme-toggle:hover { background: rgba(255,255,255,0.25); }

  /* ── Tabs ── */
  .tabs-bar {
    background: var(--card-bg);
    border-bottom: 2px solid var(--border);
    display: flex;
    padding: 0 24px;
    position: sticky;
    top: var(--header-h);
    z-index: 99;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }
  .tab-btn {
    padding: 14px 20px;
    border: none;
    background: none;
    color: var(--text-muted);
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    border-bottom: 3px solid transparent;
    margin-bottom: -2px;
    transition: color 0.15s, border-color 0.15s;
    white-space: nowrap;
  }
  .tab-btn:hover { color: var(--navy); }
  .tab-btn.active { color: var(--navy); border-bottom-color: var(--navy); font-weight: 600; }
  .tab-content { display: none; padding: 24px; }
  .tab-content.active { display: block; }

  /* ── Cards ── */
  .card {
    background: var(--card-bg);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    box-shadow: var(--shadow);
    padding: 20px;
  }
  .card-title {
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-muted);
    margin-bottom: 8px;
  }

  /* ── KPI Grid ── */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .kpi-card {
    background: var(--card-bg);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    box-shadow: var(--shadow);
    padding: 20px 16px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    position: relative;
    overflow: hidden;
  }
  .kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: var(--navy);
  }
  .kpi-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-muted); }
  .kpi-value { font-size: 28px; font-weight: 800; color: var(--navy); line-height: 1.1; }
  .kpi-sub { font-size: 11px; color: var(--text-muted); }
  .kpi-accent::before { background: var(--accent); }
  .kpi-success::before { background: var(--success); }

  /* ── Circular progress ── */
  .kpi-circle {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 4px;
  }
  .circle-wrap {
    position: relative;
    width: 52px; height: 52px;
    flex-shrink: 0;
  }
  .circle-wrap canvas { position: absolute; top: 0; left: 0; }
  .circle-center {
    position: absolute;
    top: 50%; left: 50%;
    transform: translate(-50%, -50%);
    font-size: 11px;
    font-weight: 700;
    color: var(--navy);
    text-align: center;
    line-height: 1.1;
  }

  /* ── Charts Grid ── */
  .charts-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 16px;
  }
  .chart-card {
    background: var(--card-bg);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    box-shadow: var(--shadow);
    padding: 16px;
  }
  .chart-card .chart-title {
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 12px;
  }
  .chart-wrapper { position: relative; height: 220px; }
  .chart-wrapper.tall { height: 280px; }

  /* ── Filter Bar ── */
  .filter-bar {
    background: var(--card-bg);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    box-shadow: var(--shadow);
    padding: 12px 16px;
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    align-items: center;
    margin-bottom: 16px;
    position: sticky;
    top: calc(var(--header-h) + 51px);
    z-index: 50;
  }
  .filter-bar input, .filter-bar select {
    padding: 7px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg);
    color: var(--text);
    font-size: 13px;
    outline: none;
    transition: border-color 0.15s, box-shadow 0.15s;
  }
  .filter-bar input:focus, .filter-bar select:focus {
    border-color: var(--navy);
    box-shadow: 0 0 0 2px rgba(31,78,121,0.1);
  }
  .filter-bar input { width: 220px; }
  .results-badge {
    margin-left: auto;
    background: var(--navy);
    color: #fff;
    border-radius: 20px;
    padding: 4px 12px;
    font-size: 12px;
    font-weight: 600;
    white-space: nowrap;
  }
  .btn {
    padding: 7px 14px;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    border: none;
    transition: opacity 0.15s, transform 0.1s;
  }
  .btn:hover { opacity: 0.88; transform: translateY(-1px); }
  .btn:active { transform: translateY(0); }
  .btn-primary { background: var(--navy); color: #fff; }
  .btn-outline { background: transparent; color: var(--navy); border: 1px solid var(--navy); }

  /* ── Companies Table ── */
  .table-wrap { overflow-x: auto; border-radius: var(--radius); border: 1px solid var(--border); }
  table { width: 100%; border-collapse: collapse; background: var(--card-bg); }
  thead th {
    background: var(--navy);
    color: #fff;
    padding: 10px 12px;
    text-align: left;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    white-space: nowrap;
    cursor: pointer;
    user-select: none;
    position: sticky;
    top: 0;
  }
  thead th:hover { background: var(--navy-light); }
  thead th .sort-arrow { margin-left: 4px; opacity: 0.5; }
  thead th.sort-asc .sort-arrow::after { content: "▲"; }
  thead th.sort-desc .sort-arrow::after { content: "▼"; }
  thead th:not(.sort-asc):not(.sort-desc) .sort-arrow::after { content: "⇅"; }
  tbody tr {
    border-bottom: 1px solid var(--border);
    cursor: pointer;
    transition: background 0.1s;
  }
  tbody tr:hover { background: rgba(31,78,121,0.05); }
  tbody tr.selected { background: rgba(31,78,121,0.08); }
  tbody td { padding: 9px 12px; font-size: 13px; white-space: nowrap; max-width: 180px; overflow: hidden; text-overflow: ellipsis; }

  /* ── Badges ── */
  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.3px;
  }
  .badge-A { background: #dcfce7; color: #166534; }
  .badge-B { background: #bbf7d0; color: #15803d; }
  .badge-C { background: #fef3c7; color: #92400e; }
  .badge-D { background: #fed7aa; color: #9a3412; }
  .badge-F { background: #fecaca; color: #991b1b; }
  .badge-tier1 { background: #dcfce7; color: #15803d; }
  .badge-tier2 { background: #fef3c7; color: #92400e; }
  .badge-tier3 { background: #fed7aa; color: #9a3412; }
  .badge-unq { background: #f1f5f9; color: #64748b; }
  .badge-high { background: #fecaca; color: #991b1b; }
  .badge-medium { background: #fed7aa; color: #92400e; }
  .badge-low { background: #bbf7d0; color: #15803d; }
  .badge-emerging { background: #bfdbfe; color: #1e40af; }

  /* ── Score bar ── */
  .score-bar-wrap { display: flex; align-items: center; gap: 6px; }
  .score-bar {
    height: 6px; border-radius: 3px; background: var(--border); flex: 1;
    max-width: 60px; overflow: hidden;
  }
  .score-bar-fill { height: 100%; border-radius: 3px; background: var(--navy); }

  /* ── Pagination ── */
  .pagination {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: 16px 0;
    flex-wrap: wrap;
  }
  .page-btn {
    padding: 5px 10px;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: var(--card-bg);
    color: var(--text);
    cursor: pointer;
    font-size: 12px;
    transition: background 0.1s, border-color 0.1s;
  }
  .page-btn:hover { background: var(--navy); color: #fff; border-color: var(--navy); }
  .page-btn.active { background: var(--navy); color: #fff; border-color: var(--navy); font-weight: 700; }
  .page-btn:disabled { opacity: 0.4; cursor: default; }

  /* ── Detail Panel ── */
  .detail-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.4);
    z-index: 200;
    opacity: 0;
    pointer-events: none;
    transition: opacity 0.2s;
  }
  .detail-overlay.open { opacity: 1; pointer-events: auto; }
  .detail-panel {
    position: fixed;
    top: 0; right: 0; bottom: 0;
    width: 480px;
    max-width: 95vw;
    background: var(--card-bg);
    box-shadow: -4px 0 24px rgba(0,0,0,0.15);
    z-index: 201;
    transform: translateX(100%);
    transition: transform 0.25s cubic-bezier(0.4,0,0.2,1);
    display: flex;
    flex-direction: column;
  }
  .detail-panel.open { transform: translateX(0); }
  .detail-header {
    background: linear-gradient(135deg, var(--navy-dark), var(--navy));
    color: #fff;
    padding: 20px 24px;
    flex-shrink: 0;
  }
  .detail-header h2 { font-size: 16px; font-weight: 700; margin-bottom: 4px; }
  .detail-header .detail-domain { font-size: 12px; opacity: 0.75; }
  .detail-close {
    position: absolute;
    top: 16px; right: 16px;
    background: rgba(255,255,255,0.2);
    border: none;
    color: #fff;
    border-radius: 50%;
    width: 28px; height: 28px;
    cursor: pointer;
    font-size: 16px;
    display: flex; align-items: center; justify-content: center;
    transition: background 0.15s;
  }
  .detail-close:hover { background: rgba(255,255,255,0.35); }
  .detail-body {
    flex: 1;
    overflow-y: auto;
    padding: 20px 24px;
  }
  .detail-section { margin-bottom: 20px; }
  .detail-section-title {
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
    padding-bottom: 6px;
    margin-bottom: 10px;
  }
  .detail-row {
    display: flex;
    gap: 8px;
    margin-bottom: 6px;
    align-items: flex-start;
  }
  .detail-key { font-size: 12px; color: var(--text-muted); min-width: 110px; flex-shrink: 0; }
  .detail-val { font-size: 13px; color: var(--text); word-break: break-word; }
  .pills { display: flex; flex-wrap: wrap; gap: 4px; }
  .pill {
    display: inline-block;
    background: rgba(31,78,121,0.1);
    color: var(--navy);
    border-radius: 12px;
    padding: 2px 9px;
    font-size: 11px;
    font-weight: 500;
  }
  [data-theme="dark"] .pill { background: rgba(255,255,255,0.1); color: #93c5fd; }
  .icp-bars { display: grid; gap: 6px; }
  .icp-bar-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
  .icp-bar-label { min-width: 120px; color: var(--text-muted); }
  .icp-bar-track { flex: 1; height: 8px; background: var(--border); border-radius: 4px; overflow: hidden; }
  .icp-bar-fill { height: 100%; border-radius: 4px; background: var(--navy); transition: width 0.3s; }
  .icp-bar-num { min-width: 28px; text-align: right; font-weight: 600; color: var(--navy); }

  /* ── Segments Tab ── */
  .segments-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
    gap: 16px;
  }
  .segment-card {
    background: var(--card-bg);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    box-shadow: var(--shadow);
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    position: relative;
    overflow: hidden;
  }
  .segment-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 4px;
    background: var(--navy);
  }
  .seg-name { font-size: 15px; font-weight: 700; color: var(--text); }
  .seg-count { font-size: 26px; font-weight: 800; color: var(--navy); }
  .seg-pct { font-size: 12px; color: var(--text-muted); }
  .seg-bar-track { height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; }
  .seg-bar-fill { height: 100%; border-radius: 3px; background: var(--navy); }

  /* ── Competitive Tab ── */
  .comp-table-wrap, .events-list-wrap {
    background: var(--card-bg);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    box-shadow: var(--shadow);
    overflow: hidden;
    margin-bottom: 24px;
  }
  .section-heading {
    background: var(--navy);
    color: #fff;
    padding: 12px 20px;
    font-size: 14px;
    font-weight: 700;
    letter-spacing: 0.3px;
  }
  .comp-table { width: 100%; border-collapse: collapse; }
  .comp-table th {
    background: rgba(31,78,121,0.06);
    padding: 9px 14px;
    text-align: left;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
  }
  .comp-table td {
    padding: 10px 14px;
    font-size: 13px;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  .comp-table tr:last-child td { border-bottom: none; }
  .comp-table tr:hover td { background: rgba(31,78,121,0.03); }
  .gss-response { font-size: 11px; font-weight: 600; padding: 3px 8px; border-radius: 4px; }
  .resp-must-counter { background: #fecaca; color: #991b1b; }
  .resp-monitor { background: #fed7aa; color: #92400e; }
  .resp-track { background: #dcfce7; color: #15803d; }
  .resp-watch { background: #bfdbfe; color: #1e40af; }
  .event-item {
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 8px 16px;
    padding: 12px 20px;
    border-bottom: 1px solid var(--border);
    align-items: start;
  }
  .event-item:last-child { border-bottom: none; }
  .event-item:hover { background: rgba(31,78,121,0.03); }
  .event-name { font-size: 14px; font-weight: 600; color: var(--text); }
  .event-meta { font-size: 12px; color: var(--text-muted); margin-top: 2px; }
  .event-note { font-size: 11px; color: var(--text-muted); font-style: italic; margin-top: 2px; }

  /* ── Print ── */
  @media print {
    .header { position: relative; }
    .tabs-bar { display: none; }
    .tab-content { display: block !important; page-break-before: always; }
    .tab-content:first-of-type { page-break-before: avoid; }
    .detail-overlay, .detail-panel { display: none; }
    .filter-bar { position: relative; top: 0; }
    .pagination { display: none; }
    body { background: #fff; color: #000; }
    .card, .chart-card, .kpi-card, .segment-card { box-shadow: none; border: 1px solid #ccc; }
  }

  /* ── Responsive ── */
  @media (max-width: 768px) {
    .header-subtitle { display: none; }
    .tab-btn { padding: 10px 12px; font-size: 12px; }
    .tab-content { padding: 12px; }
    .charts-grid { grid-template-columns: 1fr; }
    .kpi-grid { grid-template-columns: repeat(2, 1fr); }
    .filter-bar { position: relative; top: 0; }
    .detail-panel { width: 100vw; }
  }
</style>
</head>
<body>

<!-- Header -->
<header class="header">
  <div class="header-left">
    <div class="header-logo">G</div>
    <div>
      <div class="header-title">GSS Intelligence Dashboard</div>
      <div class="header-subtitle">NAM Manufacturing Pipeline — 2026</div>
    </div>
  </div>
  <div class="header-right">
    <span class="gen-time" id="genTime"></span>
    <button class="theme-toggle" onclick="toggleTheme()">Dark Mode</button>
  </div>
</header>

<!-- Tabs Bar -->
<nav class="tabs-bar">
  <button class="tab-btn active" onclick="switchTab('overview', this)">Overview</button>
  <button class="tab-btn" onclick="switchTab('companies', this)">Companies</button>
  <button class="tab-btn" onclick="switchTab('segments', this)">Segments</button>
  <button class="tab-btn" onclick="switchTab('intel', this)">Competitive Intel</button>
</nav>

<!-- TAB: Overview -->
<div id="tab-overview" class="tab-content active">
  <div class="kpi-grid" id="kpiGrid"></div>
  <div class="charts-grid" id="chartsGrid"></div>
</div>

<!-- TAB: Companies -->
<div id="tab-companies" class="tab-content">
  <div class="filter-bar" id="filterBar">
    <input type="text" id="searchInput" placeholder="Search company or domain..." oninput="debounceFilter()">
    <select id="filterAssoc" onchange="applyFilters()"><option value="">All Associations</option></select>
    <select id="filterState" onchange="applyFilters()"><option value="">All States</option></select>
    <select id="filterGrade" onchange="applyFilters()">
      <option value="">All Grades</option>
      <option value="A">A</option><option value="B">B</option>
      <option value="C">C</option><option value="D">D</option><option value="F">F</option>
    </select>
    <select id="filterEP" onchange="applyFilters()"><option value="">All Email Providers</option></select>
    <select id="filterTier" onchange="applyFilters()">
      <option value="">All Tiers</option>
      <option value="Tier 1">Tier 1</option>
      <option value="Tier 2">Tier 2</option>
      <option value="Tier 3">Tier 3</option>
      <option value="Unqualified">Unqualified</option>
    </select>
    <button class="btn btn-outline" onclick="clearFilters()">Clear</button>
    <button class="btn btn-primary" onclick="exportCSV()">Export CSV</button>
    <span class="results-badge" id="resultsBadge">0 of 0</span>
  </div>
  <div class="table-wrap">
    <table id="companiesTable">
      <thead>
        <tr>
          <th onclick="sortTable('n')" data-col="n">Company <span class="sort-arrow"></span></th>
          <th onclick="sortTable('d')" data-col="d">Domain <span class="sort-arrow"></span></th>
          <th onclick="sortTable('st')" data-col="st">State <span class="sort-arrow"></span></th>
          <th onclick="sortTable('as')" data-col="as">Association <span class="sort-arrow"></span></th>
          <th onclick="sortTable('ep')" data-col="ep">Email Provider <span class="sort-arrow"></span></th>
          <th onclick="sortTable('qs')" data-col="qs">Q Score <span class="sort-arrow"></span></th>
          <th onclick="sortTable('qg')" data-col="qg">Grade <span class="sort-arrow"></span></th>
          <th onclick="sortTable('ic')" data-col="ic">ICP <span class="sort-arrow"></span></th>
          <th onclick="sortTable('tr')" data-col="tr">Tier <span class="sort-arrow"></span></th>
        </tr>
      </thead>
      <tbody id="tableBody"></tbody>
    </table>
  </div>
  <div class="pagination" id="pagination"></div>
</div>

<!-- TAB: Segments -->
<div id="tab-segments" class="tab-content">
  <div class="segments-grid" id="segmentsGrid"></div>
</div>

<!-- TAB: Competitive Intel -->
<div id="tab-intel" class="tab-content">
  <div class="comp-table-wrap">
    <div class="section-heading">Competitor Landscape</div>
    <table class="comp-table">
      <thead>
        <tr>
          <th>Competitor</th>
          <th>Show Presence</th>
          <th>Strategy Notes</th>
          <th>Threat</th>
          <th>GSS Response</th>
        </tr>
      </thead>
      <tbody id="competitorTableBody"></tbody>
    </table>
  </div>
  <div class="events-list-wrap">
    <div class="section-heading">2026 Industry Events</div>
    <div id="eventsList"></div>
  </div>
</div>

<!-- Detail Panel Overlay -->
<div class="detail-overlay" id="detailOverlay" onclick="closeDetail()"></div>
<div class="detail-panel" id="detailPanel">
  <button class="detail-close" onclick="closeDetail()">&#x2715;</button>
  <div class="detail-header">
    <h2 id="detailName"></h2>
    <div class="detail-domain" id="detailDomain"></div>
  </div>
  <div class="detail-body" id="detailBody"></div>
</div>

<script>
// ── Data Injection ───────────────────────────────────────────────────
const DATA = /*__DASHBOARD_DATA__*/;

// ── State ────────────────────────────────────────────────────────────
let allCompanies = DATA.companies;
let filteredCompanies = [...allCompanies];
let currentPage = 1;
const PAGE_SIZE = 50;
let sortCol = 'ic';
let sortDir = -1; // -1 = desc
let debounceTimer = null;
let charts = {};
let activeAssocFilter = '';

// ── Theme ────────────────────────────────────────────────────────────
function toggleTheme() {
  const curr = document.documentElement.getAttribute('data-theme');
  const next = curr === 'dark' ? '' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('gss-theme', next);
  document.querySelector('.theme-toggle').textContent = next === 'dark' ? 'Light Mode' : 'Dark Mode';
  // Rebuild charts with new colors
  setTimeout(() => initCharts(), 50);
}
(function() {
  const saved = localStorage.getItem('gss-theme');
  if (saved === 'dark') {
    document.documentElement.setAttribute('data-theme', 'dark');
    document.querySelector('.theme-toggle').textContent = 'Light Mode';
  }
})();

// ── Tabs ─────────────────────────────────────────────────────────────
function switchTab(name, btn) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  btn.classList.add('active');
}

// ── KPI Cards ────────────────────────────────────────────────────────
function initKPIs() {
  const s = DATA.stats;
  document.getElementById('genTime').textContent = 'Generated ' + DATA.generated_at;

  const kpis = [
    { label: 'Total Companies', value: s.total.toLocaleString(), sub: 'Across all associations', accent: '' },
    { label: 'Avg Quality Score', value: s.avg_score, sub: 'Out of 100', accent: 'kpi-accent', circle: true },
    { label: 'High Quality (B+)', value: s.high_quality.toLocaleString(), sub: 'Score ≥ 70', accent: 'kpi-success' },
    { label: 'Enrichment Coverage', value: s.enrichment_pct + '%', sub: 'Fully enriched records', accent: '' },
    { label: 'Contact Coverage', value: s.contact_pct + '%', sub: 'Has ≥1 contact', accent: '' },
    { label: 'Tech Stack Detected', value: s.tech_pct + '%', sub: 'Has tech stack data', accent: '' },
  ];

  const grid = document.getElementById('kpiGrid');
  kpis.forEach(k => {
    const card = document.createElement('div');
    card.className = 'kpi-card ' + (k.accent || '');
    if (k.circle) {
      card.innerHTML = `
        <div class="kpi-label">${k.label}</div>
        <div class="kpi-circle">
          <div class="circle-wrap">
            <canvas id="circleCanvas" width="52" height="52"></canvas>
            <div class="circle-center">${k.value}</div>
          </div>
          <div>
            <div class="kpi-value" style="font-size:20px">${k.value}</div>
            <div class="kpi-sub">${k.sub}</div>
          </div>
        </div>`;
    } else {
      card.innerHTML = `
        <div class="kpi-label">${k.label}</div>
        <div class="kpi-value">${k.value}</div>
        <div class="kpi-sub">${k.sub}</div>`;
    }
    grid.appendChild(card);
  });

  // Draw circular progress
  const canvas = document.getElementById('circleCanvas');
  if (canvas) {
    const ctx = canvas.getContext('2d');
    const pct = s.avg_score / 100;
    const cx = 26, cy = 26, r = 22;
    ctx.clearRect(0,0,52,52);
    ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#e2e8f0';
    ctx.lineWidth = 5;
    ctx.beginPath(); ctx.arc(cx,cy,r,0,Math.PI*2); ctx.stroke();
    ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#E8A415';
    ctx.lineWidth = 5;
    ctx.lineCap = 'round';
    ctx.beginPath();
    ctx.arc(cx,cy,r, -Math.PI/2, -Math.PI/2 + Math.PI*2*pct);
    ctx.stroke();
  }
}

// ── Charts ───────────────────────────────────────────────────────────
function getChartDefaults() {
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  return {
    textColor: isDark ? '#94a3b8' : '#64748b',
    gridColor: isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)',
    tooltipBg: isDark ? '#1a2840' : '#fff',
    tooltipText: isDark ? '#e2e8f0' : '#1a2332',
  };
}

const PALETTE_ASSOC = ['#1F4E79','#2563a8','#3b82f6','#60a5fa','#93c5fd','#bfdbfe','#dbeafe','#1e40af','#1d4ed8','#2563a8','#3b82f6','#0284c7','#0369a1','#0c4a6e'];
const PALETTE_EP = ['#1F4E79','#E8A415','#16a34a','#dc2626','#7c3aed','#0891b2','#9a3412','#64748b','#94a3b8'];
const QUALITY_COLORS = [
  '#dc2626','#ef4444','#f97316','#fb923c','#fbbf24','#facc15','#84cc16','#22c55e','#16a34a','#15803d'
];

function destroyChart(id) {
  if (charts[id]) { charts[id].destroy(); delete charts[id]; }
}

function initCharts() {
  const s = DATA.stats;
  const d = getChartDefaults();

  const pluginDefaults = {
    legend: { labels: { color: d.textColor, font: { size: 11 } } },
    tooltip: {
      backgroundColor: d.tooltipBg,
      titleColor: d.tooltipText,
      bodyColor: d.tooltipText,
      borderColor: 'rgba(31,78,121,0.2)',
      borderWidth: 1,
    }
  };
  const scaleDefaults = {
    ticks: { color: d.textColor, font: { size: 11 } },
    grid: { color: d.gridColor }
  };

  const chartsGrid = document.getElementById('chartsGrid');
  chartsGrid.innerHTML = '';

  function makeCard(id, title, tall) {
    const card = document.createElement('div');
    card.className = 'chart-card';
    card.innerHTML = `<div class="chart-title">${title}</div><div class="chart-wrapper${tall?' tall':''}"><canvas id="${id}"></canvas></div>`;
    chartsGrid.appendChild(card);
    return document.getElementById(id);
  }

  // 1. Company Count by Association
  destroyChart('c1');
  const c1 = makeCard('c1', 'Companies by Association');
  charts['c1'] = new Chart(c1, {
    type: 'bar',
    data: {
      labels: s.by_association.map(x=>x.label),
      datasets: [{
        data: s.by_association.map(x=>x.value),
        backgroundColor: PALETTE_ASSOC.slice(0, s.by_association.length),
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { ...pluginDefaults, legend: { display: false } },
      scales: { x: scaleDefaults, y: { ...scaleDefaults, beginAtZero: true } },
      onClick: (evt, els) => {
        if (els.length) {
          const label = s.by_association[els[0].index].label;
          activeAssocFilter = label;
          switchTab('companies', document.querySelectorAll('.tab-btn')[1]);
          document.getElementById('filterAssoc').value = label;
          applyFilters();
        }
      }
    }
  });

  // 2. Quality Score Distribution
  destroyChart('c2');
  const c2 = makeCard('c2', 'Quality Score Distribution');
  charts['c2'] = new Chart(c2, {
    type: 'bar',
    data: {
      labels: s.quality_distribution.map(x=>x.label),
      datasets: [{
        data: s.quality_distribution.map(x=>x.value),
        backgroundColor: QUALITY_COLORS,
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { ...pluginDefaults, legend: { display: false } },
      scales: { x: scaleDefaults, y: { ...scaleDefaults, beginAtZero: true } },
    }
  });

  // 3. Email Provider Donut
  destroyChart('c3');
  const c3 = makeCard('c3', 'Email Provider Breakdown');
  charts['c3'] = new Chart(c3, {
    type: 'doughnut',
    data: {
      labels: s.email_providers.map(x=>x.label),
      datasets: [{
        data: s.email_providers.map(x=>x.value),
        backgroundColor: PALETTE_EP,
        borderWidth: 2,
        borderColor: document.documentElement.getAttribute('data-theme')==='dark'?'#1a2840':'#fff',
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      cutout: '60%',
      plugins: {
        ...pluginDefaults,
        legend: { position: 'right', labels: { color: d.textColor, font: { size: 11 }, boxWidth: 12, padding: 8 } },
      }
    },
    plugins: [{
      id: 'centerText',
      beforeDraw(chart) {
        const { ctx, chartArea } = chart;
        if (!chartArea) return;
        const total = chart.data.datasets[0].data.reduce((a,b)=>a+b,0);
        const cx = (chartArea.left+chartArea.right)/2;
        const cy = (chartArea.top+chartArea.bottom)/2;
        ctx.save();
        ctx.font = 'bold 16px sans-serif';
        ctx.fillStyle = d.tooltipText;
        ctx.textAlign = 'center';
        ctx.fillText(total.toLocaleString(), cx, cy-4);
        ctx.font = '10px sans-serif';
        ctx.fillStyle = d.textColor;
        ctx.fillText('companies', cx, cy+12);
        ctx.restore();
      }
    }]
  });

  // 4. Geographic Distribution (top 15 states)
  destroyChart('c4');
  const c4 = makeCard('c4', 'Geographic Distribution (Top 15 States)', true);
  charts['c4'] = new Chart(c4, {
    type: 'bar',
    data: {
      labels: s.by_state.map(x=>x.label),
      datasets: [{
        data: s.by_state.map(x=>x.value),
        backgroundColor: '#1F4E79',
        borderRadius: 3,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: { ...pluginDefaults, legend: { display: false } },
      scales: {
        x: { ...scaleDefaults, beginAtZero: true },
        y: { ...scaleDefaults, ticks: { color: d.textColor, font: { size: 11 } } }
      },
    }
  });

  // 5. Top Tech Stack
  destroyChart('c5');
  const c5 = makeCard('c5', 'Top 10 Tech Stack Items', true);
  charts['c5'] = new Chart(c5, {
    type: 'bar',
    data: {
      labels: s.top_tech.map(x=>x.label),
      datasets: [{
        data: s.top_tech.map(x=>x.value),
        backgroundColor: '#2563a8',
        borderRadius: 3,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: { ...pluginDefaults, legend: { display: false } },
      scales: {
        x: { ...scaleDefaults, beginAtZero: true },
        y: { ...scaleDefaults }
      },
    }
  });

  // 6. SPF / CRM Services
  destroyChart('c6');
  const c6 = makeCard('c6', 'SPF Services / CRM Signals', true);
  charts['c6'] = new Chart(c6, {
    type: 'bar',
    data: {
      labels: s.top_spf.map(x=>x.label),
      datasets: [{
        data: s.top_spf.map(x=>x.value),
        backgroundColor: '#E8A415',
        borderRadius: 3,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: { ...pluginDefaults, legend: { display: false } },
      scales: {
        x: { ...scaleDefaults, beginAtZero: true },
        y: { ...scaleDefaults }
      },
    }
  });
}

// ── Filter Dropdown Population ────────────────────────────────────────
function populateFilterDropdowns() {
  const assocs = new Set();
  const states = new Set();
  const eps = new Set();
  allCompanies.forEach(c => {
    c.as.split(';').forEach(a => { const t = a.trim(); if (t) assocs.add(t); });
    if (c.st) states.add(c.st);
    if (c.ep) eps.add(c.ep);
  });
  const assocSel = document.getElementById('filterAssoc');
  [...assocs].sort().forEach(a => { const o = new Option(a, a); assocSel.appendChild(o); });
  const stateSel = document.getElementById('filterState');
  [...states].sort().forEach(s => { const o = new Option(s, s); stateSel.appendChild(o); });
  const epSel = document.getElementById('filterEP');
  [...eps].sort().forEach(e => { const o = new Option(e, e); epSel.appendChild(o); });
}

// ── Filtering ─────────────────────────────────────────────────────────
function debounceFilter() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(applyFilters, 200);
}

function applyFilters() {
  const q = document.getElementById('searchInput').value.toLowerCase().trim();
  const assoc = document.getElementById('filterAssoc').value;
  const state = document.getElementById('filterState').value;
  const grade = document.getElementById('filterGrade').value;
  const ep = document.getElementById('filterEP').value;
  const tier = document.getElementById('filterTier').value;

  filteredCompanies = allCompanies.filter(c => {
    if (q && !c.n.toLowerCase().includes(q) && !c.d.toLowerCase().includes(q)) return false;
    if (assoc && !c.as.includes(assoc)) return false;
    if (state && c.st !== state) return false;
    if (grade && c.qg !== grade) return false;
    if (ep && c.ep !== ep) return false;
    if (tier && c.tr !== tier) return false;
    return true;
  });

  currentPage = 1;
  renderTable();
}

function clearFilters() {
  document.getElementById('searchInput').value = '';
  document.getElementById('filterAssoc').value = '';
  document.getElementById('filterState').value = '';
  document.getElementById('filterGrade').value = '';
  document.getElementById('filterEP').value = '';
  document.getElementById('filterTier').value = '';
  filteredCompanies = [...allCompanies];
  currentPage = 1;
  renderTable();
}

// ── Table Rendering ───────────────────────────────────────────────────
function sortTable(col) {
  if (sortCol === col) {
    sortDir *= -1;
  } else {
    sortCol = col;
    sortDir = -1;
  }
  document.querySelectorAll('thead th').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    if (th.dataset.col === col) {
      th.classList.add(sortDir === 1 ? 'sort-asc' : 'sort-desc');
    }
  });
  filteredCompanies.sort((a, b) => {
    const av = a[col] ?? '';
    const bv = b[col] ?? '';
    if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * sortDir;
    return String(av).localeCompare(String(bv)) * sortDir;
  });
  currentPage = 1;
  renderTable();
}

function tierBadgeClass(tier) {
  if (tier === 'Tier 1') return 'badge-tier1';
  if (tier === 'Tier 2') return 'badge-tier2';
  if (tier === 'Tier 3') return 'badge-tier3';
  return 'badge-unq';
}

function renderTable() {
  const total = filteredCompanies.length;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const start = (currentPage - 1) * PAGE_SIZE;
  const page = filteredCompanies.slice(start, start + PAGE_SIZE);

  document.getElementById('resultsBadge').textContent = `Showing ${total.toLocaleString()} of ${allCompanies.length.toLocaleString()}`;

  const tbody = document.getElementById('tableBody');
  tbody.innerHTML = '';
  page.forEach((c, idx) => {
    const tr = document.createElement('tr');
    const scoreW = Math.round((c.qs / 100) * 100);
    tr.innerHTML = `
      <td title="${esc(c.n)}">${esc(c.n)}</td>
      <td title="${esc(c.d)}">${c.d ? `<a href="http://${esc(c.d)}" target="_blank" style="color:var(--navy)">${esc(c.d)}</a>` : '—'}</td>
      <td>${esc(c.st) || '—'}</td>
      <td title="${esc(c.as)}">${esc(c.as.split(';')[0].trim()) || '—'}</td>
      <td>${esc(c.ep) || '—'}</td>
      <td>
        <div class="score-bar-wrap">
          <span>${c.qs}</span>
          <div class="score-bar"><div class="score-bar-fill" style="width:${scoreW}%"></div></div>
        </div>
      </td>
      <td><span class="badge badge-${esc(c.qg)}">${esc(c.qg) || '—'}</span></td>
      <td><strong>${c.ic}</strong></td>
      <td><span class="badge ${tierBadgeClass(c.tr)}">${esc(c.tr)}</span></td>`;
    tr.addEventListener('click', () => openDetail(start + idx));
    tbody.appendChild(tr);
  });

  renderPagination(totalPages);
}

function renderPagination(totalPages) {
  const pag = document.getElementById('pagination');
  pag.innerHTML = '';
  if (totalPages <= 1) return;

  function addBtn(label, page, disabled, active) {
    const btn = document.createElement('button');
    btn.className = 'page-btn' + (active ? ' active' : '');
    btn.textContent = label;
    btn.disabled = disabled;
    btn.addEventListener('click', () => { currentPage = page; renderTable(); });
    pag.appendChild(btn);
  }

  addBtn('«', 1, currentPage === 1, false);
  addBtn('‹', currentPage - 1, currentPage === 1, false);

  const window = 2;
  const pages = [];
  for (let p = Math.max(1, currentPage - window); p <= Math.min(totalPages, currentPage + window); p++) pages.push(p);
  if (pages[0] > 1) { addBtn('1', 1, false, false); if (pages[0] > 2) { const e = document.createElement('span'); e.textContent = '…'; e.style.padding = '5px 4px'; pag.appendChild(e); } }
  pages.forEach(p => addBtn(p, p, false, p === currentPage));
  if (pages[pages.length-1] < totalPages) { if (pages[pages.length-1] < totalPages-1) { const e = document.createElement('span'); e.textContent = '…'; e.style.padding = '5px 4px'; pag.appendChild(e); } addBtn(totalPages, totalPages, false, false); }

  addBtn('›', currentPage + 1, currentPage === totalPages, false);
  addBtn('»', totalPages, currentPage === totalPages, false);
}

// ── Detail Panel ──────────────────────────────────────────────────────
function openDetail(idx) {
  const c = filteredCompanies[idx];
  if (!c) return;

  document.getElementById('detailName').textContent = c.n;
  document.getElementById('detailDomain').textContent = c.d || c.w || 'No website';

  const body = document.getElementById('detailBody');

  function row(key, val) {
    if (!val && val !== 0) return '';
    return `<div class="detail-row"><span class="detail-key">${key}</span><span class="detail-val">${esc(String(val))}</span></div>`;
  }
  function pillsHtml(arr) {
    if (!arr || !arr.length) return '<span style="color:var(--text-muted);font-size:12px">None detected</span>';
    return `<div class="pills">${arr.map(t=>`<span class="pill">${esc(t)}</span>`).join('')}</div>`;
  }

  // ICP breakdown
  const icp_labels = {
    tech_maturity: ['Tech Maturity', 25],
    size_fit: ['Size Fit', 20],
    geo_fit: ['Geo Fit', 15],
    assoc_engagement: ['Assoc Engagement', 15],
    tech_gap: ['Tech Gap', 15],
    data_quality: ['Data Quality', 10],
  };
  // We only have the total icp_score in compressed form; show score bar for each proportional estimate
  // Actually we only store ic (total). Let's show what we have.

  body.innerHTML = `
    <div class="detail-section">
      <div class="detail-section-title">Location & Contact</div>
      ${row('Address', [c.str, c.ci, c.st, c.z].filter(Boolean).join(', '))}
      ${row('Phone', c.ph)}
      ${row('Primary Contact', c.pc)}
      ${row('Primary Email', c.pe)}
    </div>
    <div class="detail-section">
      <div class="detail-section-title">Firmographic</div>
      ${row('Employees', c.em)}
      ${row('Association(s)', c.as)}
      ${row('Enrichment', c.es)}
    </div>
    <div class="detail-section">
      <div class="detail-section-title">Scoring</div>
      ${row('Quality Score', c.qs + ' / 100 (Grade: ' + c.qg + ')')}
      ${row('ICP Score', c.ic + ' / 100')}
      ${row('ABM Tier', c.tr)}
      ${row('Competitor Detected', c.co || 'None')}
    </div>
    <div class="detail-section">
      <div class="detail-section-title">Email & CRM</div>
      ${row('Email Provider', c.ep)}
      ${row('CMS', c.cm)}
      <div class="detail-row"><span class="detail-key">SPF Services</span><span class="detail-val">${pillsHtml(c.sp)}</span></div>
    </div>
    <div class="detail-section">
      <div class="detail-section-title">Tech Stack</div>
      ${pillsHtml(c.ts)}
    </div>
    ${c.w ? `<div class="detail-section"><a href="${esc(c.w.startsWith('http')?c.w:'http://'+c.w)}" target="_blank" class="btn btn-primary" style="display:inline-block;text-decoration:none">Visit Website</a></div>` : ''}`;

  document.getElementById('detailOverlay').classList.add('open');
  document.getElementById('detailPanel').classList.add('open');
}

function closeDetail() {
  document.getElementById('detailOverlay').classList.remove('open');
  document.getElementById('detailPanel').classList.remove('open');
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDetail(); });

// ── Segments Tab ──────────────────────────────────────────────────────
function initSegments() {
  const grid = document.getElementById('segmentsGrid');
  const total = DATA.stats.total;
  DATA.stats.segments.forEach(seg => {
    const pct = seg.pct;
    const card = document.createElement('div');
    card.className = 'segment-card';
    card.innerHTML = `
      <div class="seg-name">${esc(seg.name)}</div>
      <div class="seg-count">${seg.count.toLocaleString()}</div>
      <div class="seg-pct">${pct}% of all companies</div>
      <div class="seg-bar-track"><div class="seg-bar-fill" style="width:${pct}%"></div></div>
      <button class="btn btn-outline" style="margin-top:8px;font-size:12px" onclick="filterToSegment('${esc(seg.name)}')">View in Companies</button>`;
    grid.appendChild(card);
  });
}

const SEGMENT_FILTER_MAP = {
  'Salesforce Users':       c => c.sp.some(s => ['salesforce','pardot'].includes(s.toLowerCase())),
  'Legacy Email':           c => { const ep = c.ep.toLowerCase(); return ['self-hosted','self-hosted (on-premise)','other',''].includes(ep) || !ep; },
  'Microsoft 365':          c => c.ep === 'Microsoft 365',
  'Marketing Automation':   c => c.sp.some(s => ['hubspot','marketo','mailchimp','pardot','activecampaign','constant contact'].includes(s.toLowerCase())),
  'Small Mfg':              c => { const max = parseInt(c.em.replace(/[^0-9]/g,'')) || 0; return max > 0 && max <= 100 && !!c.w; },
  'Large Mfg':              c => { const min = parseInt((c.em.match(/^([0-9]+)/)||[])[1]||0); return min >= 500; },
  'PMA Premium':            c => false, // membership_tier not in compressed record
};

function filterToSegment(segName) {
  switchTab('companies', document.querySelectorAll('.tab-btn')[1]);
  clearFilters();
  const fn = SEGMENT_FILTER_MAP[segName];
  if (fn) {
    filteredCompanies = allCompanies.filter(fn);
    currentPage = 1;
    renderTable();
    document.getElementById('resultsBadge').textContent = `Segment: ${segName} — ${filteredCompanies.length.toLocaleString()} of ${allCompanies.length.toLocaleString()}`;
  }
}

// ── Competitive Intel ─────────────────────────────────────────────────
function initIntel() {
  const THREAT_RESP = {
    'HIGH': ['Must Counter', 'resp-must-counter'],
    'MEDIUM': ['Monitor Closely', 'resp-monitor'],
    'LOW': ['Track Only', 'resp-track'],
    'EMERGING': ['Watch & Prepare', 'resp-watch'],
  };

  const tbody = document.getElementById('competitorTableBody');
  DATA.competitors.forEach(c => {
    const [respText, respClass] = THREAT_RESP[c.tl] || ['Track Only', 'resp-track'];
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><strong>${esc(c.n)}</strong></td>
      <td style="max-width:200px;white-space:normal;font-size:12px">${esc(c.pr)}</td>
      <td style="max-width:240px;white-space:normal;font-size:12px">${esc(c.sn)}</td>
      <td><span class="badge badge-${c.tl.toLowerCase()}">${esc(c.tl)}</span></td>
      <td><span class="gss-response ${respClass}">${respText}</span></td>`;
    tbody.appendChild(tr);
  });

  const PRIORITY_BADGE = {
    'HIGH': 'badge-high',
    'MEDIUM': 'badge-medium',
    'LOW': 'badge-low',
  };
  const eventsList = document.getElementById('eventsList');
  DATA.events.forEach(e => {
    const badgeClass = PRIORITY_BADGE[e.p] || 'badge-low';
    const div = document.createElement('div');
    div.className = 'event-item';
    div.innerHTML = `
      <div>
        <div class="event-name">${esc(e.n)}</div>
        <div class="event-meta">${esc(e.dt)} &bull; ${esc(e.ci)} &bull; ${esc(e.v)} &bull; Attendance: ${esc(e.at)} &bull; ${esc(e.in)}</div>
        ${e.no ? `<div class="event-note">${esc(e.no)}</div>` : ''}
      </div>
      <div style="text-align:right;white-space:nowrap">
        <span class="badge ${badgeClass}">${esc(e.p)}</span>
      </div>`;
    eventsList.appendChild(div);
  });
}

// ── CSV Export ────────────────────────────────────────────────────────
function exportCSV() {
  const headers = ['Company','Domain','Website','City','State','Street','Zip','Phone','Associations','Email Provider','Tech Stack','SPF Services','CMS','Quality Score','Quality Grade','ICP Score','Tier','Primary Contact','Primary Email','Employees','Enrichment'];
  const rows = filteredCompanies.map(c => [
    c.n, c.d, c.w, c.ci, c.st, c.str, c.z, c.ph, c.as,
    c.ep, c.ts.join('; '), c.sp.join('; '), c.cm,
    c.qs, c.qg, c.ic, c.tr, c.pc, c.pe, c.em, c.es
  ].map(v => `"${String(v||'').replace(/"/g,'""')}"`).join(','));

  const csv = [headers.join(','), ...rows].join('\\n');
  const blob = new Blob([csv], {type: 'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'GSS_Intelligence_Export.csv';
  a.click();
}

// ── Utilities ─────────────────────────────────────────────────────────
function esc(str) {
  return String(str||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Init ──────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initKPIs();
  initCharts();
  populateFilterDropdowns();
  filteredCompanies = [...allCompanies];
  renderTable();
  initSegments();
  initIntel();
});
</script>
</body>
</html>"""

# ── Main ───────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("GSS Intelligence Dashboard Builder")
    print("=" * 60)

    print("\n[1/4] Loading and merging data...")
    records = load_and_merge_data()
    print(f"  {len(records)} total records")

    events = load_events()
    print(f"  {len(events)} events")

    competitors = load_competitors()
    print(f"  {len(competitors)} competitors")

    print("\n[2/4] Building dashboard data payload...")
    dashboard_data = build_dashboard_data(records, events, competitors)
    print(f"  {len(dashboard_data['companies'])} companies in payload")
    print(f"  Stats: avg_score={dashboard_data['stats']['avg_score']}, "
          f"high_quality={dashboard_data['stats']['high_quality']}")

    print("\n[3/4] Serializing JSON payload...")
    data_json = json.dumps(dashboard_data, separators=(",", ":"), ensure_ascii=False)
    print(f"  Payload size: {len(data_json):,} bytes ({len(data_json)/1024:.1f} KB)")

    print("\n[4/4] Generating HTML...")
    html = HTML_TEMPLATE.replace("/*__DASHBOARD_DATA__*/", data_json)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    file_size = OUTPUT_PATH.stat().st_size
    print(f"\nDashboard written to: {OUTPUT_PATH}")
    print(f"File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
    print("\nOpen in browser: file://" + str(OUTPUT_PATH).replace("\\", "/"))
    print("Done.")


if __name__ == "__main__":
    main()
