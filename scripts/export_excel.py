"""
Excel Export Script
NAM Intelligence Pipeline

Generates a multi-sheet Excel workbook from validated company data.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

try:
    import openpyxl
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side  # noqa: F401
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.hyperlink import Hyperlink  # noqa: F401
except ImportError:
    print("Error: openpyxl not installed. Run: pip install openpyxl")
    sys.exit(1)


class ExcelExporter:
    """Export pipeline data to Excel workbook."""

    # Column definitions for main sheet
    MAIN_COLUMNS = [
        ("Company Name", "company_name", 35),
        ("Website", "website", 30),
        ("Domain", "domain", 20),
        ("City", "city", 18),
        ("State", "state", 8),
        ("Country", "country", 15),
        ("Association(s)", "associations", 15),
        ("Membership Tier", "membership_tier", 15),
        ("Member Since", "member_since", 12),
        ("Employee Count", "employee_count_display", 15),
        ("Revenue Range", "revenue_display", 18),
        ("Year Founded", "year_founded", 12),
        ("Industry", "industry", 25),
        ("NAICS Code", "naics_code", 12),
        ("ERP System", "erp_system", 18),
        ("CRM System", "crm_system", 15),
        ("Tech Stack", "tech_stack_display", 40),
        ("Contact Name", "primary_contact_name", 25),
        ("Contact Title", "primary_contact_title", 25),
        ("Contact Email", "primary_contact_email", 30),
        ("Contact Phone", "primary_contact_phone", 18),
        ("Quality Score", "quality_score", 12),
        ("Quality Grade", "quality_grade", 12),
        ("Source URL", "source_url", 50),
    ]

    # Colors
    HEADER_FILL = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    HEADER_FONT = Font(color="FFFFFF", bold=True, size=11)
    GRADE_COLORS = {
        "A": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
        "B": PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
        "C": PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid"),
        "D": PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
        "F": PatternFill(start_color="FF9999", end_color="FF9999", fill_type="solid"),
    }

    def __init__(self, records: list[dict]):
        self.records = records
        self.wb = openpyxl.Workbook()
        # Remove default sheet
        self.wb.remove(self.wb.active)

    def export(self, output_path: str):
        """Export all sheets and save workbook."""
        print(f"Exporting {len(self.records)} records to Excel...")

        # Prepare record data
        self._prepare_records()

        # Create sheets
        self._create_main_sheet()
        self._create_by_association_sheet()
        self._create_erp_opportunities_sheet()
        self._create_contacts_sheet()
        self._create_quality_report_sheet()

        # Save workbook
        self.wb.save(output_path)
        print(f"Exported to: {output_path}")

    def _prepare_records(self):
        """Prepare records with display fields."""
        for record in self.records:
            # Employee count display
            emp_min = record.get("employee_count_min")
            emp_max = record.get("employee_count_max")
            if emp_min and emp_max:
                if emp_min == emp_max:
                    record["employee_count_display"] = str(emp_min)
                else:
                    record["employee_count_display"] = f"{emp_min}-{emp_max}"
            elif emp_min:
                record["employee_count_display"] = f"{emp_min}+"

            # Revenue display
            rev_min = record.get("revenue_min_usd")
            rev_max = record.get("revenue_max_usd")
            if rev_min:
                record["revenue_display"] = self._format_currency(rev_min, rev_max)

            # Tech stack display
            tech_stack = record.get("tech_stack", [])
            if tech_stack:
                record["tech_stack_display"] = ", ".join(tech_stack[:10])

            # Associations display
            associations = record.get("associations") or [record.get("association")]
            if associations:
                record["associations"] = ", ".join([a for a in associations if a])

            # Primary contact
            contacts = record.get("contacts", [])
            if contacts:
                primary = contacts[0]
                record["primary_contact_name"] = primary.get("name")
                record["primary_contact_title"] = primary.get("title")
                record["primary_contact_email"] = primary.get("email")
                record["primary_contact_phone"] = primary.get("phone")

            # Domain from website
            website = record.get("website", "")
            if website and "://" in website:
                domain = website.split("://")[1].split("/")[0]
                if domain.startswith("www."):
                    domain = domain[4:]
                record["domain"] = domain

    def _format_currency(self, min_val: int, max_val: int = None) -> str:
        """Format currency value for display."""
        def format_num(n):
            if n >= 1_000_000_000:
                return f"${n / 1_000_000_000:.1f}B"
            elif n >= 1_000_000:
                return f"${n / 1_000_000:.0f}M"
            elif n >= 1_000:
                return f"${n / 1_000:.0f}K"
            else:
                return f"${n:,}"

        if max_val and max_val != min_val:
            return f"{format_num(min_val)} - {format_num(max_val)}"
        return format_num(min_val)

    def _create_main_sheet(self):
        """Create main 'All Companies' sheet."""
        ws = self.wb.create_sheet("All Companies")

        # Write headers
        for col_idx, (header, _, width) in enumerate(self.MAIN_COLUMNS, 1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.fill = self.HEADER_FILL
            cell.font = self.HEADER_FONT
            cell.alignment = Alignment(horizontal="center", vertical="center")
            ws.column_dimensions[get_column_letter(col_idx)].width = width

        # Freeze header row
        ws.freeze_panes = "A2"

        # Write data
        for row_idx, record in enumerate(self.records, 2):
            for col_idx, (_, field, _) in enumerate(self.MAIN_COLUMNS, 1):
                value = record.get(field, "")

                # Handle list values
                if isinstance(value, list):
                    value = ", ".join(str(v) for v in value)

                cell = ws.cell(row=row_idx, column=col_idx, value=value)

                # Add hyperlinks for URLs
                if field in ["website", "source_url"] and value and value.startswith("http"):
                    cell.hyperlink = value
                    cell.font = Font(color="0563C1", underline="single")

                # Color quality grade
                if field == "quality_grade" and value in self.GRADE_COLORS:
                    cell.fill = self.GRADE_COLORS[value]

        # Add filters
        ws.auto_filter.ref = ws.dimensions

        print(f"  - All Companies: {len(self.records)} rows")

    def _create_by_association_sheet(self):
        """Create 'By Association' summary sheet."""
        ws = self.wb.create_sheet("By Association")

        # Aggregate by association
        assoc_stats = {}
        for record in self.records:
            associations = record.get("associations", "").split(", ") if record.get("associations") else ["Unknown"]
            for assoc in associations:
                if not assoc:
                    continue
                if assoc not in assoc_stats:
                    assoc_stats[assoc] = {
                        "count": 0,
                        "with_website": 0,
                        "with_erp": 0,
                        "with_contacts": 0,
                        "total_score": 0
                    }
                stats = assoc_stats[assoc]
                stats["count"] += 1
                if record.get("website"):
                    stats["with_website"] += 1
                if record.get("erp_system"):
                    stats["with_erp"] += 1
                if record.get("contacts"):
                    stats["with_contacts"] += 1
                stats["total_score"] += record.get("quality_score", 0)

        # Headers
        headers = ["Association", "Companies", "With Website", "With ERP", "With Contacts", "Avg Quality Score"]
        for col_idx, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.fill = self.HEADER_FILL
            cell.font = self.HEADER_FONT

        # Data
        for row_idx, (assoc, stats) in enumerate(sorted(assoc_stats.items()), 2):
            ws.cell(row=row_idx, column=1, value=assoc)
            ws.cell(row=row_idx, column=2, value=stats["count"])
            ws.cell(row=row_idx, column=3, value=stats["with_website"])
            ws.cell(row=row_idx, column=4, value=stats["with_erp"])
            ws.cell(row=row_idx, column=5, value=stats["with_contacts"])
            avg_score = stats["total_score"] / stats["count"] if stats["count"] > 0 else 0
            ws.cell(row=row_idx, column=6, value=round(avg_score, 1))

        # Adjust column widths
        ws.column_dimensions["A"].width = 30
        for col in "BCDEF":
            ws.column_dimensions[col].width = 15

        print(f"  - By Association: {len(assoc_stats)} associations")

    def _create_erp_opportunities_sheet(self):
        """Create 'ERP Opportunities' sheet."""
        ws = self.wb.create_sheet("ERP Opportunities")

        # Filter records with ERP info
        erp_records = [r for r in self.records if r.get("erp_system")]

        # Columns for this sheet
        columns = [
            ("Company Name", "company_name", 35),
            ("ERP System", "erp_system", 20),
            ("Tech Source", "tech_source", 15),
            ("Employee Count", "employee_count_display", 15),
            ("Revenue Range", "revenue_display", 18),
            ("City", "city", 18),
            ("State", "state", 8),
            ("Contact Name", "primary_contact_name", 25),
            ("Contact Title", "primary_contact_title", 25),
            ("Contact Email", "primary_contact_email", 30),
            ("Website", "website", 35),
        ]

        # Headers
        for col_idx, (header, _, width) in enumerate(columns, 1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.fill = self.HEADER_FILL
            cell.font = self.HEADER_FONT
            ws.column_dimensions[get_column_letter(col_idx)].width = width

        # Data
        for row_idx, record in enumerate(erp_records, 2):
            for col_idx, (_, field, _) in enumerate(columns, 1):
                value = record.get(field, "")
                cell = ws.cell(row=row_idx, column=col_idx, value=value)

                if field == "website" and value and value.startswith("http"):
                    cell.hyperlink = value
                    cell.font = Font(color="0563C1", underline="single")

        # Freeze and filter
        ws.freeze_panes = "A2"
        if erp_records:
            ws.auto_filter.ref = ws.dimensions

        print(f"  - ERP Opportunities: {len(erp_records)} companies")

    def _create_contacts_sheet(self):
        """Create 'Decision-Maker Contacts' sheet."""
        ws = self.wb.create_sheet("Decision-Maker Contacts")

        # Flatten contacts
        contact_rows = []
        for record in self.records:
            contacts = record.get("contacts", [])
            for contact in contacts:
                contact_rows.append({
                    "company_name": record.get("company_name"),
                    "website": record.get("website"),
                    "name": contact.get("name"),
                    "title": contact.get("title"),
                    "email": contact.get("email"),
                    "phone": contact.get("phone"),
                    "linkedin_url": contact.get("linkedin_url"),
                    "source": contact.get("source"),
                })

        # Columns
        columns = [
            ("Company", "company_name", 35),
            ("Contact Name", "name", 25),
            ("Title", "title", 30),
            ("Email", "email", 35),
            ("Phone", "phone", 18),
            ("LinkedIn", "linkedin_url", 40),
            ("Source", "source", 12),
            ("Website", "website", 35),
        ]

        # Headers
        for col_idx, (header, _, width) in enumerate(columns, 1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.fill = self.HEADER_FILL
            cell.font = self.HEADER_FONT
            ws.column_dimensions[get_column_letter(col_idx)].width = width

        # Data
        for row_idx, contact in enumerate(contact_rows, 2):
            for col_idx, (_, field, _) in enumerate(columns, 1):
                value = contact.get(field, "")
                cell = ws.cell(row=row_idx, column=col_idx, value=value)

                if field in ["website", "linkedin_url"] and value and value.startswith("http"):
                    cell.hyperlink = value
                    cell.font = Font(color="0563C1", underline="single")

        # Freeze and filter
        ws.freeze_panes = "A2"
        if contact_rows:
            ws.auto_filter.ref = ws.dimensions

        print(f"  - Decision-Maker Contacts: {len(contact_rows)} contacts")

    def _create_quality_report_sheet(self):
        """Create 'Data Quality Report' sheet."""
        ws = self.wb.create_sheet("Data Quality Report")

        # Quality distribution
        quality_dist = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
        total_score = 0
        missing_fields = {}

        for record in self.records:
            grade = record.get("quality_grade", "F")
            quality_dist[grade] = quality_dist.get(grade, 0) + 1
            total_score += record.get("quality_score", 0)

            # Track missing fields
            for field in ["website", "city", "state", "employee_count_min", "erp_system", "contacts"]:
                if not record.get(field):
                    missing_fields[field] = missing_fields.get(field, 0) + 1

        # Write summary section
        ws["A1"] = "Data Quality Summary"
        ws["A1"].font = Font(bold=True, size=14)

        ws["A3"] = "Total Records:"
        ws["B3"] = len(self.records)

        ws["A4"] = "Average Quality Score:"
        ws["B4"] = round(total_score / len(self.records), 1) if self.records else 0

        # Quality distribution
        ws["A6"] = "Quality Grade Distribution"
        ws["A6"].font = Font(bold=True)

        ws["A7"] = "Grade"
        ws["B7"] = "Count"
        ws["C7"] = "Percentage"

        for row_idx, (grade, count) in enumerate(quality_dist.items(), 8):
            ws.cell(row=row_idx, column=1, value=grade)
            ws.cell(row=row_idx, column=2, value=count)
            pct = count / len(self.records) * 100 if self.records else 0
            ws.cell(row=row_idx, column=3, value=f"{pct:.1f}%")

        # Missing fields
        ws["A15"] = "Missing Data by Field"
        ws["A15"].font = Font(bold=True)

        ws["A16"] = "Field"
        ws["B16"] = "Missing Count"
        ws["C16"] = "Percentage"

        for row_idx, (field, count) in enumerate(sorted(missing_fields.items(), key=lambda x: -x[1]), 17):
            ws.cell(row=row_idx, column=1, value=field)
            ws.cell(row=row_idx, column=2, value=count)
            pct = count / len(self.records) * 100 if self.records else 0
            ws.cell(row=row_idx, column=3, value=f"{pct:.1f}%")

        # Adjust column widths
        ws.column_dimensions["A"].width = 25
        ws.column_dimensions["B"].width = 15
        ws.column_dimensions["C"].width = 15

        print("  - Data Quality Report: complete")


def load_records(input_path: str) -> list[dict]:
    """Load records from JSONL file."""
    records = []
    path = Path(input_path)

    if not path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Export NAM Intelligence data to Excel"
    )
    parser.add_argument(
        "-i", "--input",
        help="Input JSONL file (default: most recent validated file)",
        default=None
    )
    parser.add_argument(
        "-o", "--output",
        help="Output Excel file path",
        default=None
    )

    args = parser.parse_args()

    # Find input file
    if args.input:
        input_path = args.input
    else:
        # Find most recent validated file
        validated_path = Path("data/validated")
        if not validated_path.exists():
            print("Error: No validated data found. Run the pipeline first.")
            sys.exit(1)

        jsonl_files = list(validated_path.glob("**/companies.jsonl"))
        if not jsonl_files:
            print("Error: No companies.jsonl files found.")
            sys.exit(1)

        input_path = str(max(jsonl_files, key=lambda p: p.stat().st_mtime))
        print(f"Using most recent file: {input_path}")

    # Set output path
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/validated/GSS_NAM_Intelligence_{timestamp}.xlsx"

    # Load records
    records = load_records(input_path)
    print(f"Loaded {len(records)} records")

    if not records:
        print("Error: No records to export")
        sys.exit(1)

    # Export
    exporter = ExcelExporter(records)
    exporter.export(output_path)

    print(f"\nExport complete: {output_path}")


if __name__ == "__main__":
    main()
