"""
Master Extraction Script
NAM Intelligence Pipeline

Provides a simplified interface to run the complete pipeline for all associations.
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.orchestrator import OrchestratorAgent


def print_banner():
    """Print startup banner."""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║         NAM Manufacturing Intelligence Pipeline               ║
║              Data Extraction & Enrichment Tool                ║
╚═══════════════════════════════════════════════════════════════╝
    """)


def print_summary(result: dict):
    """Print pipeline summary."""
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)

    if result.get("success"):
        print(f"✓ Status: SUCCESS")
    else:
        print(f"✗ Status: FAILED")
        if result.get("error"):
            print(f"  Error: {result['error']}")

    if "phases" in result:
        print(f"\nPhase Results:")
        for phase, phase_result in result["phases"].items():
            status = "✓" if phase_result.get("success") else "✗"
            records = phase_result.get("records_processed", phase_result.get("total_records", "N/A"))
            print(f"  {status} {phase.upper()}: {records} records")

    if "final_record_count" in result:
        print(f"\nFinal Output: {result['final_record_count']} records")

    if result.get("output_path"):
        print(f"Output File: {result['output_path']}")

    print("=" * 60)


async def run_pipeline(
    mode: str,
    associations: list[str],
    enrichments: list[str],
    dry_run: bool,
    job_id: str = None
):
    """Run the pipeline."""
    print(f"\nStarting pipeline in '{mode}' mode...")

    if associations:
        print(f"Associations: {', '.join(associations)}")
    else:
        print("Associations: ALL")

    if dry_run:
        print("DRY RUN - No data will be saved")

    # Create orchestrator
    orchestrator = OrchestratorAgent(
        agent_type="orchestrator",
        job_id=job_id,
        mode=mode,
        associations=associations,
        dry_run=dry_run
    )

    # Build task
    task = {
        "mode": mode,
        "associations": associations,
    }

    if enrichments:
        task["enrichment"] = enrichments[0] if len(enrichments) == 1 else "all"

    # Execute
    print("\n" + "-" * 60)
    result = await orchestrator.execute(task)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="NAM Manufacturing Intelligence Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract from all high-priority associations
  python extract_all.py --mode extract

  # Extract from specific associations
  python extract_all.py --mode extract -a PMA -a NEMA -a AGMA

  # Run full pipeline for one association
  python extract_all.py --mode full -a PMA

  # Run with all enrichment (requires API keys)
  python extract_all.py --mode full -a PMA --enrich all

  # Dry run (no data saved)
  python extract_all.py --mode full -a PMA --dry-run

  # Export to Excel after pipeline completes
  python scripts/export_excel.py
        """
    )

    parser.add_argument(
        "--mode",
        choices=["extract", "extract-all", "enrich", "validate", "full"],
        default="full",
        help="Pipeline mode (default: full)"
    )

    parser.add_argument(
        "-a", "--association",
        action="append",
        dest="associations",
        metavar="CODE",
        help="Association code(s) to process (e.g., PMA, NEMA). Can be specified multiple times."
    )

    parser.add_argument(
        "--enrich",
        action="append",
        dest="enrichments",
        choices=["firmographic", "techstack", "contacts", "all"],
        help="Enrichment type(s). Requires API keys."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without saving results"
    )

    parser.add_argument(
        "--job-id",
        help="Specific job ID (for resume)"
    )

    parser.add_argument(
        "--export-excel",
        action="store_true",
        help="Export results to Excel after pipeline completes"
    )

    parser.add_argument(
        "--output",
        help="Output Excel file path (used with --export-excel)"
    )

    args = parser.parse_args()

    print_banner()

    # Check for required packages
    try:
        import httpx
        import yaml
        from bs4 import BeautifulSoup
    except ImportError as e:
        print(f"Error: Missing required package: {e}")
        print("Run: pip install -r requirements.txt")
        sys.exit(1)

    # Run pipeline
    try:
        result = asyncio.run(run_pipeline(
            mode=args.mode,
            associations=args.associations or [],
            enrichments=args.enrichments or [],
            dry_run=args.dry_run,
            job_id=args.job_id
        ))

        print_summary(result)

        # Export to Excel if requested
        if args.export_excel and result.get("success"):
            print("\nExporting to Excel...")
            from scripts.export_excel import ExcelExporter, load_records

            # Find output file
            output_path = result.get("output_path")
            if output_path:
                records = load_records(output_path)
                excel_path = args.output or output_path.replace(".jsonl", ".xlsx")
                exporter = ExcelExporter(records)
                exporter.export(excel_path)

        # Exit with appropriate code
        sys.exit(0 if result.get("success") else 1)

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\nPipeline error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
