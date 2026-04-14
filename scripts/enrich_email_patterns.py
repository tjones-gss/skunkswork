#!/usr/bin/env python3
"""
Email Pattern Enrichment Script
NAM Intelligence Pipeline

For each company with a domain but no email contacts:
1. Confirm MX records exist (email is configured on domain)
2. Try common generic addresses: info@, sales@, contact@
3. If a contact name is known, derive personal patterns:
   firstname@, firstname.lastname@, flastname@, lastname@
4. Verify each candidate via SMTP RCPT TO (250 = exists, 550 = no, 452/timeout = skip)
5. Record the working pattern for the domain

Limits:
- Max 3 SMTP checks per domain (anti-enumeration)
- 2–3 second delay between checks
- Skip domains whose MX server soft-rejects (452) or times out
- --limit flag caps total companies processed per run (default 200)

Output: data/processed/email_patterns.jsonl
"""

import argparse
import csv
import json
import logging
import random
import re
import smtplib
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

import dns.resolver

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = PROJECT_ROOT / "data" / "exports" / "companies_all.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "email_patterns.jsonl"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EHLO_HOSTNAME = "mail.gssmail.com"          # real-looking EHLO hostname
MAIL_FROM = "verify@gssmail.com"            # envelope sender for RCPT TO probe
SMTP_TIMEOUT = 10                           # seconds per connection
MAX_PER_DOMAIN = 3                          # hard cap on RCPT TO probes per domain
INTER_CHECK_DELAY = (2.0, 3.0)             # (min, max) seconds between SMTP checks
GENERIC_PATTERNS = ["info", "sales", "contact"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("email_patterns")


# ---------------------------------------------------------------------------
# Name parsing
# ---------------------------------------------------------------------------
_HONORIFICS = {"mr", "mrs", "ms", "miss", "dr", "prof", "mr.", "mrs.", "ms.", "dr."}

def _clean_name(raw: str) -> str:
    """Strip angle-bracket email portion and honorifics, return bare name."""
    # Drop   Name <email@domain.com>  →  Name
    raw = re.sub(r"<[^>]+>", "", raw).strip()
    # Drop trailing credentials like ", CPA" or " CPA"
    raw = re.sub(r",.*$", "", raw).strip()
    parts = raw.split()
    # Drop leading honorific
    if parts and parts[0].lower() in _HONORIFICS:
        parts = parts[1:]
    return " ".join(parts)


def parse_contact_names(contacts_field: str) -> list[str]:
    """
    Parse the semicolon-separated contacts field and return a list of clean
    full names.  Entries look like:
        Ryan Myers <rmyers@sigma-engineered.com>
        Mr. Chris Blaylock, CPA <cblaylock@wipfli.com>
        Chuck Karch          (no email)
    Returns names that have at least a first and last name token.
    """
    names = []
    for entry in contacts_field.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        name = _clean_name(entry)
        tokens = name.split()
        if len(tokens) >= 2:
            names.append(name)
    return names


def derive_personal_patterns(full_name: str, domain: str) -> list[str]:
    """
    Given 'First Last' (or 'First Middle Last'), return candidate email addresses
    ordered by how commonly manufacturing companies use them.
    """
    parts = full_name.lower().split()
    if len(parts) < 2:
        return []
    first = re.sub(r"[^a-z]", "", parts[0])
    last = re.sub(r"[^a-z]", "", parts[-1])
    if not first or not last:
        return []
    return [
        f"{first}.{last}@{domain}",          # firstname.lastname  (most common)
        f"{first[0]}{last}@{domain}",         # flastname
        f"{first}@{domain}",                  # firstname
        f"{last}@{domain}",                   # lastname
    ]


# ---------------------------------------------------------------------------
# DNS helpers
# ---------------------------------------------------------------------------
def get_mx_hosts(domain: str) -> list[str]:
    """
    Return MX hostnames for domain, sorted by preference (lowest first).
    Returns empty list if no MX records or DNS error.
    """
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=8)
        sorted_ans = sorted(answers, key=lambda r: r.preference)
        return [str(r.exchange).rstrip(".") for r in sorted_ans]
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers,
            dns.exception.Timeout, Exception):
        return []


# ---------------------------------------------------------------------------
# SMTP verification
# ---------------------------------------------------------------------------
class SmtpResult:
    EXISTS = "exists"       # 250
    NOT_FOUND = "not_found" # 550
    INCONCLUSIVE = "skip"   # 452, timeout, error, etc.


def smtp_verify(address: str, mx_host: str) -> str:
    """
    Connect to mx_host:25 and probe address with RCPT TO.
    Returns SmtpResult constant.

    Does NOT send any mail — closes connection after RCPT TO response.
    """
    try:
        with smtplib.SMTP(timeout=SMTP_TIMEOUT) as conn:
            conn.connect(mx_host, 25)
            conn.ehlo(EHLO_HOSTNAME)
            conn.mail(MAIL_FROM)
            code, _ = conn.rcpt(address)
            if code == 250:
                return SmtpResult.EXISTS
            elif code in (550, 551, 553):
                return SmtpResult.NOT_FOUND
            else:
                # 452 (greylisting), 421, etc. — soft failure
                return SmtpResult.INCONCLUSIVE
    except smtplib.SMTPConnectError:
        return SmtpResult.INCONCLUSIVE
    except smtplib.SMTPServerDisconnected:
        return SmtpResult.INCONCLUSIVE
    except socket.timeout:
        return SmtpResult.INCONCLUSIVE
    except OSError:
        # Port 25 blocked by ISP / firewall — very common on residential connections
        return SmtpResult.INCONCLUSIVE
    except Exception:
        return SmtpResult.INCONCLUSIVE


def _sleep_between_checks() -> None:
    lo, hi = INTER_CHECK_DELAY
    time.sleep(random.uniform(lo, hi))


def check_domain(domain: str, contact_names: list[str]) -> dict:
    """
    Full enrichment pass for one domain.
    Returns a result dict ready to write as a JSONL record.
    """
    result = {
        "domain": domain,
        "mx_configured": False,
        "mx_host": None,
        "email_pattern": None,
        "verified_emails": [],
        "general_email": None,
        "checked_addresses": [],
        "smtp_blocked": False,
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }

    mx_hosts = get_mx_hosts(domain)
    if not mx_hosts:
        log.debug("  no MX for %s", domain)
        return result

    result["mx_configured"] = True
    mx_host = mx_hosts[0]
    result["mx_host"] = mx_host

    # Build candidate list — generics first, then personal derivations
    candidates: list[tuple[str, str]] = []  # (address, pattern_label)
    for prefix in GENERIC_PATTERNS:
        candidates.append((f"{prefix}@{domain}", prefix))

    for name in contact_names[:2]:  # at most 2 names to keep candidate count sane
        for addr in derive_personal_patterns(name, domain):
            label = _addr_to_pattern_label(addr, domain)
            candidates.append((addr, label))

    probes_done = 0
    for address, label in candidates:
        if probes_done >= MAX_PER_DOMAIN:
            break

        result["checked_addresses"].append(address)
        verdict = smtp_verify(address, mx_host)
        probes_done += 1

        if verdict == SmtpResult.EXISTS:
            result["verified_emails"].append(address)
            # Record pattern: generic addresses get their prefix as the pattern;
            # personal addresses get the structural pattern name
            if result["email_pattern"] is None:
                result["email_pattern"] = label
            if label in GENERIC_PATTERNS and result["general_email"] is None:
                result["general_email"] = address
            log.debug("  VERIFIED %s (%s)", address, label)

        elif verdict == SmtpResult.INCONCLUSIVE:
            # Likely port-25 block or greylisting; no point continuing
            result["smtp_blocked"] = True
            log.debug("  SMTP inconclusive for %s — stopping domain", address)
            break

        if probes_done < MAX_PER_DOMAIN:
            _sleep_between_checks()

    return result


def _addr_to_pattern_label(address: str, domain: str) -> str:
    """Convert a derived address back to a human-readable pattern name."""
    local = address.split("@")[0]
    if "." in local:
        return "firstname.lastname"
    if len(local) > 1 and local[1:].islower() and len(local) >= 4:
        # heuristic: single char prefix is flastname
        return "flastname"
    if local in GENERIC_PATTERNS:
        return local
    # Check length: very short locals are likely initials+last
    return "firstname"


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------
def load_companies(csv_path: Path) -> list[dict]:
    """
    Load companies_all.csv and return list of dicts with keys:
    company_name, domain, contacts.
    Only rows that have a domain are included.
    """
    companies = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = (row.get("domain") or "").strip()
            if not domain:
                continue
            companies.append({
                "company_name": row.get("company_name", "").strip(),
                "domain": domain,
                "contacts": row.get("contacts", "").strip(),
            })
    return companies


def already_done(output_path: Path) -> set[str]:
    """Return set of domains already written to the output file."""
    done: set[str] = set()
    if not output_path.exists():
        return done
    with open(output_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("domain"):
                    done.add(rec["domain"])
            except json.JSONDecodeError:
                pass
    return done


def needs_email(company: dict) -> bool:
    """
    Return True if the company needs email discovery:
    - Has a domain
    - No existing email address in the contacts field
    """
    return "@" not in company.get("contacts", "")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover email patterns for companies via SMTP verification"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of companies to process this run (default: 200)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all eligible companies (overrides --limit)",
    )
    parser.add_argument(
        "--domain",
        help="Process a single specific domain (for testing)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load data
    log.info("Loading %s ...", CSV_PATH)
    all_companies = load_companies(CSV_PATH)
    log.info("Loaded %d companies with domains", len(all_companies))

    done_domains = already_done(OUTPUT_PATH)
    log.info("%d domains already in output — will skip", len(done_domains))

    # Filter
    if args.domain:
        queue = [c for c in all_companies if c["domain"] == args.domain]
    else:
        queue = [
            c for c in all_companies
            if needs_email(c) and c["domain"] not in done_domains
        ]

    if not args.all and not args.domain:
        queue = queue[: args.limit]

    log.info("Processing %d companies", len(queue))
    if not queue:
        log.info("Nothing to do.")
        return

    # Deduplicate by domain — one pass per domain even if multiple companies share it
    seen_domains: set[str] = set()
    deduped: list[dict] = []
    for c in queue:
        if c["domain"] not in seen_domains:
            seen_domains.add(c["domain"])
            deduped.append(c)

    log.info("%d unique domains to check", len(deduped))

    stats = {"verified": 0, "no_mx": 0, "smtp_blocked": 0, "no_match": 0}

    with open(OUTPUT_PATH, "a", encoding="utf-8") as out:
        for idx, company in enumerate(deduped, 1):
            domain = company["domain"]
            contacts_field = company["contacts"]
            company_name = company["company_name"]

            # Progress print every 25
            if idx % 25 == 1 or idx == 1:
                log.info(
                    "[%d/%d] Processing: %s (%s)",
                    idx, len(deduped), company_name[:45], domain,
                )

            # Parse any known names
            contact_names = parse_contact_names(contacts_field)

            result = check_domain(domain, contact_names)
            result["company_name"] = company_name
            result["contact_names"] = contact_names

            # Tally
            if not result["mx_configured"]:
                stats["no_mx"] += 1
            elif result["smtp_blocked"]:
                stats["smtp_blocked"] += 1
            elif result["verified_emails"]:
                stats["verified"] += 1
            else:
                stats["no_match"] += 1

            out.write(json.dumps(result) + "\n")
            out.flush()

            # Brief delay between domains (not the same as inter-check delay)
            if idx < len(deduped):
                time.sleep(random.uniform(0.5, 1.0))

    # Summary
    log.info("=" * 55)
    log.info("Run complete — %d domains processed", len(deduped))
    log.info("  Verified addresses : %d", stats["verified"])
    log.info("  No MX records      : %d", stats["no_mx"])
    log.info("  SMTP blocked/skip  : %d", stats["smtp_blocked"])
    log.info("  No match found     : %d", stats["no_match"])
    log.info("Output: %s", OUTPUT_PATH)


if __name__ == "__main__":
    main()
