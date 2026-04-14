#!/usr/bin/env python3
"""
Master Deliverable Generator
NAM Intelligence Pipeline

Runs all export scripts in sequence and generates a manifest + zip file.
Usage: python scripts/generate_all_deliverables.py
"""

import os
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
EXPORTS_DIR = BASE_DIR / "data" / "exports"

# Scripts to run in order (dependencies first)
SCRIPTS = [
    # Phase 1: Refresh base data
    ("quality_pipeline.py", "Refreshing quality pipeline (CSV data)"),
    # Phase 2: Core exports
    ("export_excel.py", "Generating Master Excel + Salesforce CSV"),
    ("export_segments.py", "Generating Segmented Lists (15 segments)"),
    ("export_intelligence.py", "Generating Tech Intelligence + Event Strategy"),
    ("generate_exec_summary.py", "Generating Executive Summary (Markdown)"),
    # Phase 3: ICP Scoring (must run before ABM scripts)
    ("abm_icp_scoring.py", "Generating ICP Scored Accounts"),
    # Phase 4: ABM deliverables (independent, can run in any order)
    ("abm_campaign_playbooks.py", "Generating Campaign Playbooks"),
    ("abm_target_lists.py", "Generating ABM Target Lists"),
    ("abm_battlecards.py", "Generating Competitive Battlecards"),
    ("abm_market_intelligence.py", "Generating Market Intelligence Brief"),
    ("abm_executive_briefing.py", "Generating Executive Briefing"),
    # Phase 5: Dashboard (reads from all data sources)
    ("build_dashboard.py", "Generating Interactive HTML Dashboard"),
]

# Files to include in the zip
DELIVERABLES = [
    "GSS_NAM_Intelligence_Master.xlsx",
    "GSS_Salesforce_Import.csv",
    "GSS_Segmented_Lists.xlsx",
    "GSS_Tech_Intelligence.xlsx",
    "GSS_Event_Strategy.xlsx",
    "GSS_Executive_Summary.md",
    "GSS_ICP_Scored_Accounts.xlsx",
    "GSS_Campaign_Playbooks.xlsx",
    "GSS_ABM_Target_Lists.xlsx",
    "GSS_Competitive_Battlecards.xlsx",
    "GSS_Market_Intelligence_Brief.xlsx",
    "GSS_Executive_Briefing.xlsx",
    "GSS_Intelligence_Dashboard.html",
    "companies_all.csv",
    "companies_high_quality.csv",
    "companies_salesforce.csv",
    "competitor_analysis.csv",
    "events_2026.csv",
    "association_contacts.csv",
]


def run_script(script_name: str, description: str) -> bool:
    """Run a Python script and return True if successful."""
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        print(f"  SKIP: {script_name} (not found)")
        return False

    print(f"\n{'-' * 60}")
    print(f"  {description}")
    print(f"  Running: {script_name}")
    print(f"{'-' * 60}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        capture_output=False,
        timeout=300,
    )

    if result.returncode != 0:
        print(f"  ERROR: {script_name} exited with code {result.returncode}")
        return False

    return True


def generate_manifest():
    """Generate MANIFEST.md with file list, sizes, and timestamps."""
    manifest_path = EXPORTS_DIR / "MANIFEST.md"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# GSS Marketing Intelligence Deliverables",
        f"",
        f"Generated: {now}",
        f"",
        "## Files",
        "",
        "| File | Size | Description |",
        "|------|------|-------------|",
    ]

    descriptions = {
        "GSS_NAM_Intelligence_Master.xlsx": "7-sheet master workbook (all companies, high-quality, SFDC, by association/state, contacts, quality report)",
        "GSS_Salesforce_Import.csv": "CRM-ready CSV with custom __c fields, deduped by account name",
        "GSS_Segmented_Lists.xlsx": "15 marketing segments (Salesforce, M365, Legacy, WordPress, Security-Conscious, etc.)",
        "GSS_Tech_Intelligence.xlsx": "Tech stack analysis (4 sheets: email providers, CMS, SPF/marketing, technologies)",
        "GSS_Event_Strategy.xlsx": "Event calendar + competitive landscape (3 sheets)",
        "GSS_Executive_Summary.md": "264-line stakeholder-ready summary with all pipeline stats",
        "GSS_ICP_Scored_Accounts.xlsx": "ICP-scored accounts (4 sheets: all scored, Tier 1 strategic, distribution, model weights)",
        "GSS_Campaign_Playbooks.xlsx": "6 ready-to-execute campaign playbooks with email templates and call scripts",
        "GSS_ABM_Target_Lists.xlsx": "Tiered ABM lists (5 sheets: Tier 1/2/3, assignment matrix, statistics)",
        "GSS_Competitive_Battlecards.xlsx": "Per-competitor battlecards (3 sheets: summary, detailed, account matrix)",
        "GSS_Market_Intelligence_Brief.xlsx": "Market analysis (6 sheets: geographic, overlap, tech maturity, email, competitive, opportunity)",
        "GSS_Executive_Briefing.xlsx": "Leadership presentation (6 sheets: overview, market, quality, top 25, threats, investments)",
        "GSS_Intelligence_Dashboard.html": "Interactive HTML dashboard (open in browser, no server needed)",
        "companies_all.csv": "All companies with enrichment fields",
        "companies_high_quality.csv": "B-grade filtered companies (quality_score >= 70)",
        "companies_salesforce.csv": "Salesforce lead import format",
        "competitor_analysis.csv": "15 ERP competitor tracking",
        "events_2026.csv": "20 industry events with dates and priorities",
        "association_contacts.csv": "21 association leadership contacts",
    }

    total_size = 0
    file_count = 0
    for fname in DELIVERABLES:
        fpath = EXPORTS_DIR / fname
        if fpath.exists():
            size = fpath.stat().st_size
            total_size += size
            file_count += 1
            size_str = f"{size / 1024:.1f} KB" if size < 1024 * 1024 else f"{size / (1024 * 1024):.1f} MB"
            desc = descriptions.get(fname, "")
            lines.append(f"| {fname} | {size_str} | {desc} |")
        else:
            lines.append(f"| {fname} | MISSING | {descriptions.get(fname, '')} |")

    lines.extend([
        "",
        f"## Summary",
        f"",
        f"- **Total files:** {file_count}",
        f"- **Total size:** {total_size / (1024 * 1024):.1f} MB",
        f"- **Generated:** {now}",
    ])

    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\nManifest: {manifest_path}")
    return file_count


def generate_zip():
    """Create zip file with all deliverables."""
    zip_path = EXPORTS_DIR / "GSS_Marketing_Intelligence_v2.zip"
    count = 0

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in DELIVERABLES:
            fpath = EXPORTS_DIR / fname
            if fpath.exists():
                zf.write(fpath, fname)
                count += 1

        # Include manifest
        manifest = EXPORTS_DIR / "MANIFEST.md"
        if manifest.exists():
            zf.write(manifest, "MANIFEST.md")

    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"Zip: {zip_path} ({size_mb:.1f} MB, {count} files)")
    return zip_path


def main():
    print("=" * 60)
    print("NAM Intelligence Pipeline - Master Deliverable Generator")
    print("=" * 60)
    print(f"Base directory: {BASE_DIR}")
    print(f"Output directory: {EXPORTS_DIR}")

    # Run all scripts
    results = {}
    for script_name, description in SCRIPTS:
        success = run_script(script_name, description)
        results[script_name] = success

    # Summary
    print("\n" + "=" * 60)
    print("Script Results")
    print("=" * 60)
    for script_name, success in results.items():
        status = "OK" if success else "FAILED/SKIPPED"
        print(f"  {'[OK]' if success else '[!!]'} {script_name}: {status}")

    # Generate manifest and zip
    print("\n" + "=" * 60)
    print("Packaging Deliverables")
    print("=" * 60)
    file_count = generate_manifest()
    zip_path = generate_zip()

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\n{'=' * 60}")
    print(f"Complete: {passed}/{total} scripts succeeded, {file_count} deliverables packaged")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
