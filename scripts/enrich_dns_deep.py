#!/usr/bin/env python3
"""
Deep DNS Enrichment
NAM Intelligence Pipeline

Enriches company records with deep DNS analysis:
- MX records → email provider detection
- SPF/TXT records → CRM, marketing automation, security tools
- DMARC records → email security maturity
- NS records → hosting provider hints
- Full tech stack inference from DNS

No API keys needed. No browser needed. Pure DNS lookups.
"""

import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import dns.resolver

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "exports" / "companies_all.csv"
ENRICHED_PATH = BASE_DIR / "data" / "processed" / "enriched_all.jsonl"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "dns_enriched.jsonl"

# ── Email Provider Detection (MX records) ──────────────────────────
MX_PROVIDERS = {
    "google": "Google Workspace",
    "googlemail": "Google Workspace",
    "outlook": "Microsoft 365",
    "microsoft": "Microsoft 365",
    "pphosted": "Proofpoint",
    "mimecast": "Mimecast",
    "barracuda": "Barracuda",
    "messagelabs": "Broadcom (Symantec)",
    "postmarkapp": "Postmark",
    "secureserver": "GoDaddy",
    "emailsrvr": "Rackspace",
    "zoho": "Zoho Mail",
    "forcepoint": "Forcepoint",
    "fireeyecloud": "Trellix (FireEye)",
    "sophos": "Sophos",
}

# ── SPF/TXT Service Detection ──────────────────────────────────────
SPF_SERVICES = {
    "spf.protection.outlook.com": "Microsoft 365",
    "_spf.google.com": "Google Workspace",
    "sendgrid.net": "SendGrid",
    "amazonses.com": "Amazon SES",
    "mailchimp": "Mailchimp",
    "servers.mcsv.net": "Mailchimp",
    "hubspot": "HubSpot",
    "salesforce": "Salesforce",
    "pardot": "Pardot (Salesforce)",
    "zendesk": "Zendesk",
    "freshdesk": "Freshdesk",
    "constantcontact": "Constant Contact",
    "mandrillapp": "Mandrill (Mailchimp)",
    "postmarkapp": "Postmark",
    "sparkpostmail": "SparkPost",
    "mailgun": "Mailgun",
    "mcsignup": "Mailchimp",
    "brevo": "Brevo (Sendinblue)",
    "sendinblue": "Brevo (Sendinblue)",
    "activecampaign": "ActiveCampaign",
    "intercom": "Intercom",
    "drift": "Drift",
    "marketo": "Marketo (Adobe)",
    "eloqua": "Eloqua (Oracle)",
    "exacttarget": "Salesforce Marketing Cloud",
    "cust-spf.exacttarget": "Salesforce Marketing Cloud",
    "shopify": "Shopify",
    "squarespace": "Squarespace",
    "wix": "Wix",
    "zoho": "Zoho",
    "netcore": "Netcore Cloud",
    "returnpath": "Validity (Return Path)",
    "proofpoint": "Proofpoint",
    "mimecast": "Mimecast",
    "barracuda": "Barracuda",
    "knowbe4": "KnowBe4",
    "cisco": "Cisco IronPort",
}

# ── DMARC Policy Detection ─────────────────────────────────────────
DMARC_POLICIES = {
    "p=reject": "Strict (reject)",
    "p=quarantine": "Moderate (quarantine)",
    "p=none": "Monitoring (none)",
}


def extract_bare_domain(domain_str: str) -> str:
    """Extract bare domain from URL or domain string."""
    if not domain_str:
        return ""
    d = domain_str.strip().lower()
    # Handle full URLs
    if "://" in d:
        try:
            d = urlparse(d).netloc
        except Exception:
            pass
    # Remove www. and paths
    d = d.split("/")[0]
    if d.startswith("www."):
        d = d[4:]
    # Handle subdomains — try to get registerable domain
    parts = d.split(".")
    if len(parts) > 2:
        # Keep last 2 parts for most TLDs
        if parts[-1] in ("uk", "au", "nz", "za", "br", "mx", "jp", "cn", "in"):
            d = ".".join(parts[-3:])  # co.uk, com.au etc.
        else:
            d = ".".join(parts[-2:])
    return d


def lookup_mx(domain: str) -> dict:
    """Lookup MX records and detect email provider."""
    result = {"mx_records": [], "email_provider": ""}
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=5)
        mx_hosts = sorted([(r.preference, str(r.exchange).lower().rstrip(".")) for r in answers])
        result["mx_records"] = [h for _, h in mx_hosts]

        for _, mx_host in mx_hosts:
            for key, provider in MX_PROVIDERS.items():
                if key in mx_host:
                    result["email_provider"] = provider
                    return result

        # If no known provider, check if self-hosted
        if any(domain in mx for mx in result["mx_records"]):
            result["email_provider"] = "Self-hosted"
        else:
            result["email_provider"] = "Other"
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
        pass
    except dns.exception.Timeout:
        pass
    except Exception:
        pass
    return result


def lookup_txt_spf(domain: str) -> dict:
    """Lookup TXT/SPF records and detect services."""
    result = {"spf_services": [], "txt_services": [], "has_spf": False, "has_dkim_selector": False}
    try:
        answers = dns.resolver.resolve(domain, "TXT", lifetime=5)
        for rdata in answers:
            txt = str(rdata).strip('"').lower()

            # SPF record
            if txt.startswith("v=spf1"):
                result["has_spf"] = True
                for key, service in SPF_SERVICES.items():
                    if key in txt:
                        if service not in result["spf_services"]:
                            result["spf_services"].append(service)

            # Google site verification = uses Google tools
            if "google-site-verification" in txt:
                if "Google Search Console" not in result["txt_services"]:
                    result["txt_services"].append("Google Search Console")

            # Facebook domain verification
            if "facebook-domain-verification" in txt:
                if "Facebook Business" not in result["txt_services"]:
                    result["txt_services"].append("Facebook Business")

            # Microsoft domain verification
            if "ms=" in txt:
                if "Microsoft 365" not in result["txt_services"]:
                    result["txt_services"].append("Microsoft 365 (verified)")

            # Atlassian domain verification
            if "atlassian-domain-verification" in txt:
                if "Atlassian" not in result["txt_services"]:
                    result["txt_services"].append("Atlassian")

            # DocuSign
            if "docusign" in txt:
                if "DocuSign" not in result["txt_services"]:
                    result["txt_services"].append("DocuSign")

            # Globalsign / SSL verification
            if "globalsign" in txt:
                if "GlobalSign SSL" not in result["txt_services"]:
                    result["txt_services"].append("GlobalSign SSL")

            # HubSpot verification
            if "hubspot" in txt:
                if "HubSpot" not in result["txt_services"]:
                    result["txt_services"].append("HubSpot")

            # Zoom verification
            if "zoom" in txt and "verification" in txt:
                if "Zoom" not in result["txt_services"]:
                    result["txt_services"].append("Zoom")

    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
        pass
    except dns.exception.Timeout:
        pass
    except Exception:
        pass
    return result


def lookup_dmarc(domain: str) -> dict:
    """Lookup DMARC record for email security posture."""
    result = {"has_dmarc": False, "dmarc_policy": "", "dmarc_rua": ""}
    try:
        answers = dns.resolver.resolve(f"_dmarc.{domain}", "TXT", lifetime=5)
        for rdata in answers:
            txt = str(rdata).strip('"').lower()
            if "v=dmarc1" in txt:
                result["has_dmarc"] = True
                for policy_key, policy_name in DMARC_POLICIES.items():
                    if policy_key in txt:
                        result["dmarc_policy"] = policy_name
                        break
                # Extract reporting URI
                rua_match = re.search(r"rua=mailto:([^\s;]+)", txt)
                if rua_match:
                    result["dmarc_rua"] = rua_match.group(1)
    except Exception:
        pass
    return result


def compute_security_score(mx_data: dict, spf_data: dict, dmarc_data: dict) -> int:
    """Compute email security maturity score (0-100)."""
    score = 0
    if mx_data.get("mx_records"):
        score += 20  # Has MX
    if mx_data.get("email_provider") and mx_data["email_provider"] not in ("Self-hosted", "Other"):
        score += 15  # Professional email provider
    if spf_data.get("has_spf"):
        score += 25  # Has SPF
    if dmarc_data.get("has_dmarc"):
        score += 25  # Has DMARC
        if dmarc_data.get("dmarc_policy") == "Strict (reject)":
            score += 15  # Strict DMARC
        elif dmarc_data.get("dmarc_policy") == "Moderate (quarantine)":
            score += 10
    return min(score, 100)


def load_domains() -> list[dict]:
    """Load company records that have domains."""
    import csv
    records = []
    if not CSV_PATH.exists():
        print(f"Error: {CSV_PATH} not found")
        sys.exit(1)

    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = (row.get("domain") or "").strip()
            if domain:
                row["_bare_domain"] = extract_bare_domain(domain)
                if row["_bare_domain"]:
                    records.append(row)
    return records


def main():
    print("=" * 60)
    print("NAM Intelligence Pipeline - Deep DNS Enrichment")
    print("=" * 60)

    records = load_domains()
    print(f"\nLoaded {len(records)} companies with domains")

    # Dedupe by bare domain to avoid scanning same domain twice
    domain_map = {}
    for rec in records:
        bd = rec["_bare_domain"]
        if bd not in domain_map:
            domain_map[bd] = rec

    unique_domains = list(domain_map.keys())
    print(f"Unique bare domains: {len(unique_domains)}")

    # DNS enrichment
    results = {}
    errors = 0
    now = datetime.now(timezone.utc).isoformat()

    for i, domain in enumerate(unique_domains):
        mx = lookup_mx(domain)
        spf = lookup_txt_spf(domain)
        dmarc = lookup_dmarc(domain)
        sec_score = compute_security_score(mx, spf, dmarc)

        results[domain] = {
            **mx, **spf, **dmarc,
            "email_security_score": sec_score,
            "dns_enriched_at": now,
        }

        if (i + 1) % 100 == 0:
            mx_count = sum(1 for r in results.values() if r.get("mx_records"))
            spf_count = sum(1 for r in results.values() if r.get("has_spf"))
            dmarc_count = sum(1 for r in results.values() if r.get("has_dmarc"))
            print(f"  [{i+1}/{len(unique_domains)}] MX: {mx_count}, SPF: {spf_count}, DMARC: {dmarc_count}")

        # Rate limit — be gentle on DNS
        if (i + 1) % 50 == 0:
            time.sleep(0.5)

    # Stats
    mx_count = sum(1 for r in results.values() if r.get("mx_records"))
    spf_count = sum(1 for r in results.values() if r.get("has_spf"))
    dmarc_count = sum(1 for r in results.values() if r.get("has_dmarc"))
    ep_counts = Counter(r.get("email_provider", "Unknown") for r in results.values())
    all_spf = []
    for r in results.values():
        all_spf.extend(r.get("spf_services", []))
    spf_svc_counts = Counter(all_spf)
    all_txt = []
    for r in results.values():
        all_txt.extend(r.get("txt_services", []))
    txt_svc_counts = Counter(all_txt)

    print(f"\n{'=' * 60}")
    print(f"DNS Enrichment Results")
    print(f"{'=' * 60}")
    print(f"  Domains scanned: {len(unique_domains)}")
    print(f"  MX resolved: {mx_count} ({mx_count/len(unique_domains)*100:.1f}%)")
    print(f"  SPF found: {spf_count} ({spf_count/len(unique_domains)*100:.1f}%)")
    print(f"  DMARC found: {dmarc_count} ({dmarc_count/len(unique_domains)*100:.1f}%)")
    print(f"\n  Email Providers:")
    for provider, count in ep_counts.most_common(15):
        print(f"    {provider}: {count}")
    print(f"\n  SPF Services (CRM/Marketing/Email tools):")
    for svc, count in spf_svc_counts.most_common(20):
        print(f"    {svc}: {count}")
    print(f"\n  TXT Verified Services:")
    for svc, count in txt_svc_counts.most_common(15):
        print(f"    {svc}: {count}")

    # Write enriched records
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Merge DNS data back into full records
    enriched_records = []
    for rec in records:
        bd = rec["_bare_domain"]
        dns_data = results.get(bd, {})
        merged = {**rec, **dns_data}
        del merged["_bare_domain"]
        enriched_records.append(merged)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for rec in enriched_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"\n  Output: {OUTPUT_PATH} ({len(enriched_records)} records)")
    print("Done!")


if __name__ == "__main__":
    main()
