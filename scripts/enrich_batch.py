"""
Batch Enrichment Script - Enrich ALL records with domains.

Sources:
- data/processed/NEMA/enriched.jsonl (300 records, 285 pending)
- data/raw/AGMA/records_live.jsonl (432 records with domains)
- data/raw/PMA/records_enriched.jsonl (PMA profiles with domains from scraper)

For each record with a domain:
1. Skip already-enriched records (enrichment_status == "complete")
2. Fetch homepage via httpx GET with Chrome User-Agent
3. Tech stack detection from HTML (168 fingerprints)
4. Tech stack from HTTP headers (X-Powered-By, Server)
5. CMS detection from <meta name="generator">
6. Schema.org JSON-LD extraction
7. Contact page detection (/contact, /contact-us, /about, /about-us)
8. Team page detection (/team, /leadership, /our-team)
9. MX record lookup (email provider detection)
10. SPF/TXT record analysis (marketing/CRM service detection)
11. Email pattern guessing for contacts without emails
"""

import asyncio
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import dns.resolver
import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input files
NEMA_ENRICHED = PROJECT_ROOT / "data" / "processed" / "NEMA" / "enriched.jsonl"
AGMA_RECORDS = PROJECT_ROOT / "data" / "raw" / "AGMA" / "records_live.jsonl"
PMA_ENRICHED = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_enriched.jsonl"

# Output
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "enriched_all.jsonl"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

RATE_LIMIT_DELAY = 1  # seconds between domains

TECH_FINGERPRINTS = {
    # Analytics
    "google-analytics.com": "Google Analytics",
    "googletagmanager.com": "Google Tag Manager",
    "analytics.js": "Google Analytics",
    "gtag/js": "Google Analytics",
    "hotjar.com": "Hotjar",
    "segment.com": "Segment",
    "amplitude.com": "Amplitude",
    "mixpanel.com": "Mixpanel",
    "newrelic.com": "New Relic",
    "nr-data.net": "New Relic",
    "optimizely.com": "Optimizely",
    "crazyegg.com": "Crazy Egg",
    "mouseflow.com": "Mouseflow",
    "fullstory.com": "FullStory",
    "heap-analytics": "Heap Analytics",
    "matomo": "Matomo",
    "piwik": "Matomo",
    # Marketing / CRM
    "hubspot.com": "HubSpot",
    "hs-scripts.com": "HubSpot",
    "hs-analytics.net": "HubSpot",
    "marketo.net": "Marketo",
    "marketo.com": "Marketo",
    "mktoresp.com": "Marketo",
    "pardot.com": "Pardot",
    "salesforce.com": "Salesforce",
    "force.com": "Salesforce",
    "eloqua.com": "Oracle Eloqua",
    "demandbase.com": "Demandbase",
    "6sense.com": "6sense",
    "drift.com": "Drift",
    "intercom.io": "Intercom",
    "zendesk.com": "Zendesk",
    "zdassets.com": "Zendesk",
    "freshdesk.com": "Freshdesk",
    "livechatinc.com": "LiveChat",
    "tawk.to": "Tawk.to",
    "crisp.chat": "Crisp",
    "olark.com": "Olark",
    "mailchimp.com": "Mailchimp",
    "cookie-script.com": "Cookie Script",
    "cookiebot.com": "Cookiebot",
    "onetrust.com": "OneTrust",
    "trustarc.com": "TrustArc",
    "osano.com": "Osano",
    "recaptcha": "Google reCAPTCHA",
    # CDN / Hosting
    "cloudflare": "Cloudflare",
    "akamai": "Akamai",
    "fastly": "Fastly",
    "cdn.jsdelivr.net": "jsDelivr CDN",
    "unpkg.com": "unpkg CDN",
    "cdnjs.cloudflare.com": "cdnjs",
    "amazonaws.com": "AWS",
    "azurewebsites.net": "Microsoft Azure",
    "azure.com": "Microsoft Azure",
    # CMS
    "wp-content": "WordPress",
    "wp-includes": "WordPress",
    "wordpress": "WordPress",
    "drupal": "Drupal",
    "joomla": "Joomla",
    "sitecore": "Sitecore",
    "kentico": "Kentico",
    "contentful.com": "Contentful",
    "prismic.io": "Prismic",
    "sanity.io": "Sanity",
    "adobe.com/experience-manager": "Adobe Experience Manager",
    "adobedtm.com": "Adobe DTM",
    "adobeaemcloud": "Adobe Experience Manager",
    "launch-": "Adobe Launch",
    # JavaScript frameworks
    "react": "React",
    "__next": "Next.js",
    "_next/": "Next.js",
    "nuxt": "Nuxt.js",
    "angular": "Angular",
    "vue.js": "Vue.js",
    "jquery": "jQuery",
    "bootstrap": "Bootstrap",
    "tailwind": "Tailwind CSS",
    # E-commerce
    "shopify.com": "Shopify",
    "bigcommerce.com": "BigCommerce",
    "magento": "Magento",
    "woocommerce": "WooCommerce",
    # Video
    "youtube.com/embed": "YouTube Embed",
    "vimeo.com": "Vimeo",
    "wistia.com": "Wistia",
    "vidyard.com": "Vidyard",
    # Social
    "facebook.com/tr": "Facebook Pixel",
    "connect.facebook.net": "Facebook SDK",
    "linkedin.com/insight": "LinkedIn Insight",
    "snap.licdn.com": "LinkedIn Insight",
    "twitter.com/widgets": "Twitter Widget",
    "platform.twitter.com": "Twitter Widget",
    # Search / Ads
    "googlesyndication.com": "Google AdSense",
    "googleadservices.com": "Google Ads",
    "doubleclick.net": "Google DoubleClick",
    "bing.com/bat": "Microsoft Ads",
    "bat.bing.com": "Microsoft Ads",
    # Fonts
    "fonts.googleapis.com": "Google Fonts",
    "use.typekit.net": "Adobe Fonts",
    "fast.fonts.net": "Fonts.com",
}

HEADER_TECHS = {
    "x-powered-by": {
        "express": "Express.js",
        "asp.net": "ASP.NET",
        "php": "PHP",
        "next.js": "Next.js",
        "nuxt": "Nuxt.js",
    },
    "server": {
        "nginx": "Nginx",
        "apache": "Apache",
        "cloudflare": "Cloudflare",
        "iis": "Microsoft IIS",
        "akamaighost": "Akamai",
        "amazons3": "Amazon S3",
        "gws": "Google Web Server",
        "envoy": "Envoy",
    },
}

GENERATOR_MAP = {
    "wordpress": "WordPress",
    "drupal": "Drupal",
    "joomla": "Joomla",
    "sitecore": "Sitecore",
    "hubspot": "HubSpot CMS",
    "wix.com": "Wix",
    "squarespace": "Squarespace",
    "ghost": "Ghost",
    "contentful": "Contentful",
    "adobe experience manager": "Adobe Experience Manager",
}

CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us"]
TEAM_PATHS = ["/team", "/leadership", "/our-team", "/about/leadership", "/about/team"]

# MX record → email provider mapping
MX_PROVIDERS = {
    "google.com": "Google Workspace",
    "googlemail.com": "Google Workspace",
    "aspmx.l.google.com": "Google Workspace",
    "outlook.com": "Microsoft 365",
    "protection.outlook.com": "Microsoft 365",
    "mail.protection.outlook.com": "Microsoft 365",
    "pphosted.com": "Proofpoint",
    "mimecast.com": "Mimecast",
    "barracudanetworks.com": "Barracuda",
    "secureserver.net": "GoDaddy",
    "emailsrvr.com": "Rackspace",
    "zoho.com": "Zoho Mail",
    "zoho.eu": "Zoho Mail",
    "messagelabs.com": "Broadcom/Symantec",
    "fireeyecloud.com": "Trellix",
    "iphmx.com": "Cisco IronPort",
    "ess.barracuda.com": "Barracuda",
    "forcepoint.com": "Forcepoint",
}

# SPF include → service mapping
SPF_SERVICES = {
    "spf.protection.outlook.com": "Microsoft 365",
    "_spf.google.com": "Google Workspace",
    "servers.mcsv.net": "Mailchimp",
    "sendgrid.net": "SendGrid",
    "amazonses.com": "Amazon SES",
    "mailgun.org": "Mailgun",
    "mandrillapp.com": "Mandrill",
    "hubspot.com": "HubSpot",
    "mktomail.com": "Marketo",
    "salesforce.com": "Salesforce",
    "pardot.com": "Pardot",
    "zendesk.com": "Zendesk",
    "freshdesk.com": "Freshdesk",
    "brevo.com": "Brevo",
    "sendinblue.com": "Brevo",
    "constantcontact.com": "Constant Contact",
    "ccsend.com": "Constant Contact",
    "postmarkapp.com": "Postmark",
    "sparkpostmail.com": "SparkPost",
    "createsend.com": "Campaign Monitor",
    "zoho.com": "Zoho",
    "exacttarget.com": "Salesforce Marketing Cloud",
    "netcore.co.in": "Netcore",
}

# Common email patterns (ordered by frequency in B2B companies)
EMAIL_PATTERNS = [
    "{first}.{last}",       # john.doe@domain.com (most common in US B2B)
    "{first_initial}{last}", # jdoe@domain.com
    "{first}",              # john@domain.com (small companies)
    "{first}{last}",        # johndoe@domain.com
    "{first}_{last}",       # john_doe@domain.com
    "{last}.{first}",       # doe.john@domain.com
]

DNS_TIMEOUT = 5  # seconds for DNS queries


def _bare_domain(domain: str) -> str:
    """Strip subdomains for DNS lookups, keeping the registrable domain.

    Examples: www.3m.com -> 3m.com, buildings.honeywell.com -> honeywell.com,
    new.abb.com -> abb.com, medimg.agfa.com -> agfa.com
    """
    parts = domain.lower().split(".")
    # Keep at least 2 parts (e.g. example.com)
    while len(parts) > 2:
        parts = parts[1:]
    return ".".join(parts)


def lookup_mx_records(domain: str) -> tuple[list[str], str | None]:
    """Look up MX records for a domain and identify the email provider.

    Returns (mx_records_list, detected_provider_or_None).
    """
    mx_records = []
    provider = None
    domain = _bare_domain(domain)

    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = DNS_TIMEOUT
        resolver.lifetime = DNS_TIMEOUT
        answers = resolver.resolve(domain, "MX")
        for rdata in answers:
            mx_host = str(rdata.exchange).rstrip(".").lower()
            mx_records.append(mx_host)
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer,
            dns.resolver.NoNameservers, dns.resolver.Timeout,
            dns.exception.DNSException):
        return [], None

    # Match MX hostnames against known providers
    for mx in mx_records:
        for pattern, prov_name in MX_PROVIDERS.items():
            if mx.endswith(pattern) or pattern in mx:
                provider = prov_name
                break
        if provider:
            break

    # Heuristic: if no known provider matched, check if it's self-hosted
    if not provider and mx_records:
        # If MX points to the same domain, it's likely on-premise
        for mx in mx_records:
            if domain in mx:
                provider = "Self-hosted (on-premise)"
                break
        if not provider:
            provider = "Other"

    return mx_records, provider


def lookup_spf_services(domain: str) -> list[str]:
    """Parse SPF/TXT records to detect marketing and CRM services.

    Returns list of detected service names.
    """
    services = []
    txt_records = []
    domain = _bare_domain(domain)

    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = DNS_TIMEOUT
        resolver.lifetime = DNS_TIMEOUT
        answers = resolver.resolve(domain, "TXT")
        for rdata in answers:
            txt = str(rdata).strip('"')
            txt_records.append(txt)
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer,
            dns.resolver.NoNameservers, dns.resolver.Timeout,
            dns.exception.DNSException):
        return []

    for txt in txt_records:
        if not txt.startswith("v=spf1"):
            continue
        # Extract include: directives
        includes = re.findall(r"include:(\S+)", txt)
        for inc in includes:
            inc_lower = inc.lower()
            for pattern, svc_name in SPF_SERVICES.items():
                if pattern in inc_lower:
                    if svc_name not in services:
                        services.append(svc_name)
                    break

    return services


def guess_email_patterns(contacts: list[dict], domain: str) -> list[dict]:
    """Generate email candidates for contacts that have names but no emails.

    Modifies contacts in-place, adding 'email_candidates' field.
    Returns the updated contacts list.
    """
    updated = []
    for contact in contacts:
        c = dict(contact)
        # Skip if already has an email
        if c.get("email"):
            updated.append(c)
            continue

        name = c.get("name", "").strip()
        if not name or not domain:
            updated.append(c)
            continue

        # Parse name into parts
        parts = name.split()
        if len(parts) < 2:
            updated.append(c)
            continue

        first = parts[0].lower()
        last = parts[-1].lower()
        first_initial = first[0] if first else ""

        # Strip non-alpha characters from name parts
        first = re.sub(r"[^a-z]", "", first)
        last = re.sub(r"[^a-z]", "", last)
        first_initial = re.sub(r"[^a-z]", "", first_initial)

        if not first or not last:
            updated.append(c)
            continue

        candidates = []
        for pattern in EMAIL_PATTERNS:
            email = pattern.format(
                first=first, last=last, first_initial=first_initial
            ) + f"@{domain}"
            candidates.append(email)

        c["email_candidates"] = candidates
        updated.append(c)

    return updated


def extract_domain_from_url(url: str) -> str:
    """Extract bare domain from a URL."""
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        d = urlparse(url).netloc.lower()
        return d.removeprefix("www.")
    except Exception:
        return ""


def detect_tech_from_html(html: str) -> list[str]:
    """Detect technologies from HTML content."""
    found = set()
    html_lower = html.lower()
    for fingerprint, tech_name in TECH_FINGERPRINTS.items():
        if fingerprint.lower() in html_lower:
            found.add(tech_name)
    return sorted(found)


def detect_cms_from_generator(html: str) -> str | None:
    """Extract CMS from <meta name='generator'> tag."""
    match = re.search(
        r'<meta\s+name=["\']generator["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if not match:
        match = re.search(
            r'<meta\s+content=["\']([^"\']+)["\']\s+name=["\']generator["\']',
            html,
            re.IGNORECASE,
        )
    if match:
        gen_value = match.group(1).lower()
        for key, cms_name in GENERATOR_MAP.items():
            if key in gen_value:
                return cms_name
        return match.group(1)
    return None


def detect_tech_from_headers(headers: dict) -> list[str]:
    """Detect technologies from HTTP response headers."""
    found = set()
    for header_name, tech_map in HEADER_TECHS.items():
        header_val = headers.get(header_name, "").lower()
        if header_val:
            for pattern, tech_name in tech_map.items():
                if pattern in header_val:
                    found.add(tech_name)
    if "x-drupal" in {k.lower() for k in headers}:
        found.add("Drupal")
    if "x-generator" in {k.lower() for k in headers}:
        gen = headers.get("x-generator", headers.get("X-Generator", ""))
        if gen:
            for key, cms_name in GENERATOR_MAP.items():
                if key in gen.lower():
                    found.add(cms_name)
    return sorted(found)


def extract_schema_org(html: str) -> dict | None:
    """Extract schema.org JSON-LD from HTML."""
    pattern = r'<script\s+type=["\']application/ld\+json["\']\s*>(.*?)</script>'
    matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
    for match in matches:
        try:
            data = json.loads(match.strip())
            if isinstance(data, dict) and "@graph" in data:
                for item in data["@graph"]:
                    if isinstance(item, dict) and item.get("@type") in (
                        "Organization", "Corporation", "LocalBusiness",
                        "MedicalBusiness", "ManufacturingBusiness",
                        ["Organization", "Corporation"],
                    ):
                        return _clean_schema(item)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") in (
                        "Organization", "Corporation", "LocalBusiness",
                        "MedicalBusiness", "ManufacturingBusiness",
                    ):
                        return _clean_schema(item)
            if isinstance(data, dict):
                dtype = data.get("@type", "")
                if dtype in ("Organization", "Corporation", "LocalBusiness",
                             "MedicalBusiness", "ManufacturingBusiness") or (
                    isinstance(dtype, list) and any(
                        t in ("Organization", "Corporation") for t in dtype
                    )
                ):
                    return _clean_schema(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _clean_schema(data: dict) -> dict:
    """Extract useful fields from schema.org data."""
    result = {}
    for key in ("@type", "name", "description", "url", "logo",
                "numberOfEmployees", "address", "telephone",
                "email", "foundingDate", "sameAs", "industry"):
        if key in data:
            val = data[key]
            if key == "address" and isinstance(val, dict):
                result["address"] = {
                    k: v for k, v in val.items()
                    if k in ("streetAddress", "addressLocality",
                             "addressRegion", "postalCode", "addressCountry")
                }
            elif key == "numberOfEmployees" and isinstance(val, dict):
                result["numberOfEmployees"] = val.get("value", val)
            elif key == "logo" and isinstance(val, dict):
                result["logo"] = val.get("url", str(val))
            else:
                result[key] = val
    return result


async def check_page_exists(client: httpx.AsyncClient, base_url: str, path: str) -> bool:
    """Check if a page exists via HEAD request."""
    url = base_url.rstrip("/") + path
    try:
        resp = await client.head(url, follow_redirects=True, timeout=8)
        return resp.status_code == 200
    except (httpx.RequestError, httpx.HTTPStatusError):
        return False


async def enrich_record(client: httpx.AsyncClient, record: dict,
                        skip_http: bool = False) -> dict:
    """Enrich a single record with website data and DNS records.

    If skip_http=True, only performs DNS-based enrichment (MX, SPF, email patterns).
    This avoids 403 errors from Cloudflare-protected sites.
    """
    enriched = dict(record)
    domain = record.get("domain", "")
    website = record.get("website", "")

    if not domain:
        enriched["enrichment_status"] = "skipped_no_domain"
        return enriched

    # Build website URL if not present
    if not website:
        website = f"https://{domain}"

    tech_stack = list(record.get("tech_stack", []))
    cms = record.get("cms")
    schema_org = record.get("schema_org")
    has_contact_page = record.get("has_contact_page", False)
    has_team_page = record.get("has_team_page", False)
    errors = []
    http_ok = False

    # Validate website URL
    if not website.startswith(("http://", "https://")):
        website = f"https://{domain}"
    # Reject clearly malformed URLs
    if ";" in website or " " in website or not domain:
        errors.append("invalid_url")
        website = ""

    # Fetch homepage (unless skip_http or already have tech data)
    if not skip_http and website:
        try:
            resp = await client.get(website, follow_redirects=True, timeout=8)
            html = resp.text
            status = resp.status_code

            if status == 200 and html:
                http_ok = True
                new_tech = detect_tech_from_html(html)
                header_techs = detect_tech_from_headers(dict(resp.headers))
                for t in new_tech + header_techs:
                    if t not in tech_stack:
                        tech_stack.append(t)
                tech_stack.sort()
                cms = cms or detect_cms_from_generator(html)
                schema_org = schema_org or extract_schema_org(html)
            else:
                errors.append(f"homepage_status_{status}")
        except httpx.RequestError as e:
            err_type = type(e).__name__
            errors.append(f"homepage_{err_type}")

        # Check contact and team pages (only if HTTP worked AND we don't
        # already have contact info — avoids 4-10 extra HEAD requests per record)
        if http_ok and not record.get("contacts"):
            base_url = f"https://{domain}"
            for path in CONTACT_PATHS:
                if await check_page_exists(client, base_url, path):
                    has_contact_page = True
                    break
            for path in TEAM_PATHS:
                if await check_page_exists(client, base_url, path):
                    has_team_page = True
                    break

    # MX record lookup (email provider detection) — always runs
    mx_records, email_provider = lookup_mx_records(domain)
    if mx_records:
        enriched["mx_records"] = mx_records
        enriched["has_mx"] = True
        if email_provider:
            enriched["email_provider"] = email_provider
    else:
        enriched["has_mx"] = False

    # SPF/TXT record analysis — always runs
    spf_services = lookup_spf_services(domain)
    if spf_services:
        enriched["spf_services"] = spf_services
        for svc in spf_services:
            if svc not in tech_stack:
                tech_stack.append(svc)
        tech_stack.sort()

    # Email pattern guessing for contacts without emails
    contacts = enriched.get("contacts", [])
    if contacts and domain:
        enriched["contacts"] = guess_email_patterns(contacts, domain)

    # Build enriched record
    enriched["tech_stack"] = tech_stack
    if cms:
        enriched["cms"] = cms
    if schema_org:
        enriched["schema_org"] = schema_org
    enriched["has_contact_page"] = has_contact_page
    enriched["has_team_page"] = has_team_page
    enriched["enriched_at"] = datetime.now(timezone.utc).isoformat()
    enriched["enrichment_source"] = "website_scrape"

    # Status: complete if HTTP worked or if DNS enrichment succeeded
    dns_ok = bool(mx_records or spf_services)
    if http_ok or (not errors):
        enriched["enrichment_status"] = "complete"
    elif dns_ok:
        enriched["enrichment_status"] = "complete"
        enriched["enrichment_errors"] = errors  # keep errors for reference
    else:
        enriched["enrichment_status"] = "error"
        enriched["enrichment_errors"] = errors

    return enriched


def load_records() -> list[dict]:
    """Load all records from all sources."""
    records = []

    # Load NEMA enriched records
    if NEMA_ENRICHED.exists():
        with open(NEMA_ENRICHED, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        print(f"  NEMA: {len(records)} records loaded from {NEMA_ENRICHED.name}")

    nema_count = len(records)

    # Load AGMA records
    if AGMA_RECORDS.exists():
        with open(AGMA_RECORDS, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        agma_count = len(records) - nema_count
        print(f"  AGMA: {agma_count} records loaded from {AGMA_RECORDS.name}")

    pre_pma_count = len(records)

    # Load PMA enriched records (from scrape_pma_batch.py output)
    if PMA_ENRICHED.exists():
        with open(PMA_ENRICHED, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    # Ensure domain is set from website
                    if not rec.get("domain") and rec.get("website"):
                        rec["domain"] = extract_domain_from_url(rec["website"])
                    # Tag as PMA if not already
                    if not rec.get("association"):
                        rec["association"] = "PMA"
                    records.append(rec)
        pma_count = len(records) - pre_pma_count
        print(f"  PMA:  {pma_count} records loaded from {PMA_ENRICHED.name}")

    return records


def _save_checkpoint(already_done: list, enriched_results: list, no_domain: list):
    """Save intermediate results so we don't lose progress on crash."""
    all_output = already_done + enriched_results
    for r in no_domain:
        r.setdefault("enrichment_status", "skipped_no_domain")
        all_output.append(r)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for record in all_output:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch enrichment of records with domains")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint: skip records already in output file")
    args = parser.parse_args()

    start_time = time.time()

    print("=" * 70)
    print("BATCH ENRICHMENT - All Records with Domains")
    print("=" * 70)
    print()

    # Load all records from source files
    print("Loading records...")
    all_records = load_records()
    print(f"  Total: {len(all_records)} records")

    # Resume support: replace source records with enriched versions from output
    if args.resume and OUTPUT_FILE.exists():
        enriched_by_key = {}
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    key = (rec.get("company_name", ""), rec.get("domain", ""))
                    if key != ("", ""):
                        enriched_by_key[key] = rec
        if enriched_by_key:
            merged = []
            replaced = 0
            for r in all_records:
                key = (r.get("company_name", ""), r.get("domain", ""))
                if key in enriched_by_key:
                    merged.append(enriched_by_key[key])
                    replaced += 1
                else:
                    merged.append(r)
            all_records = merged
            print(f"  Resume: replaced {replaced} records from previous checkpoint")
    print()

    # Partition into already-enriched vs needs-enrichment
    # Records marked "complete" that already have MX data are truly done.
    # Records marked "complete" WITHOUT MX data need DNS re-enrichment.
    already_done = []
    to_enrich = []        # need full HTTP + DNS enrichment
    to_enrich_dns = []    # already have tech_stack, just need MX/SPF
    no_domain = []

    for r in all_records:
        if not r.get("domain"):
            no_domain.append(r)
        elif r.get("enrichment_status") == "complete" and r.get("has_mx") is not None:
            already_done.append(r)
        elif r.get("enrichment_status") == "complete" and r.get("has_mx") is None:
            # Previously enriched but missing MX/SPF — needs DNS only
            to_enrich_dns.append(r)
        elif r.get("tech_stack"):
            # Has tech data (maybe from prior partial run) — DNS only
            to_enrich_dns.append(r)
        else:
            to_enrich.append(r)

    print(f"Already enriched (skipping):     {len(already_done)}")
    print(f"No domain (skipping):            {len(no_domain)}")
    print(f"Need full enrichment (HTTP+DNS): {len(to_enrich)}")
    print(f"Need DNS only (MX/SPF/email):    {len(to_enrich_dns)}")
    print()

    if not to_enrich and not to_enrich_dns:
        print("Nothing to enrich. Exiting.")
        return

    # Phase 1: DNS-only enrichment (fast, no HTTP needed)
    enriched_results = []
    error_types = Counter()
    total_to_process = len(to_enrich) + len(to_enrich_dns)
    processed = 0

    if to_enrich_dns:
        print(f"--- Phase 1: DNS-only enrichment ({len(to_enrich_dns)} records) ---")
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            for record in to_enrich_dns:
                company = record.get("company_name", "unknown")
                domain = record.get("domain", "")
                processed += 1

                print(f"[DNS {processed}/{len(to_enrich_dns)}] {company} ({domain})...", end=" ", flush=True)

                enriched = await enrich_record(client, record, skip_http=True)
                enriched_results.append(enriched)

                extras = []
                if enriched.get("email_provider"):
                    extras.append(f"MX:{enriched['email_provider']}")
                if enriched.get("spf_services"):
                    extras.append(f"SPF:{len(enriched['spf_services'])}")
                print(f"OK ({', '.join(extras) if extras else 'no DNS data'})")

                # Small delay to avoid DNS rate limiting
                await asyncio.sleep(0.1)

        print(f"\n  DNS-only phase complete: {len(to_enrich_dns)} records\n")

    # Phase 2: Full HTTP + DNS enrichment
    if to_enrich:
        print(f"--- Phase 2: Full enrichment ({len(to_enrich)} records) ---")
        processed = 0

        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            for i, record in enumerate(to_enrich):
                company = record.get("company_name", "unknown")
                domain = record.get("domain", "")
                processed += 1

                print(f"[{processed}/{len(to_enrich)}] {company} ({domain})...", end=" ", flush=True)

                try:
                    enriched = await enrich_record(client, record)
                except Exception as exc:
                    enriched = dict(record)
                    enriched["enrichment_status"] = "error"
                    enriched["enrichment_errors"] = [f"uncaught_{type(exc).__name__}"]
                    print(f"FATAL_ERR: {exc!s:.80s}")
                enriched_results.append(enriched)

                n_tech = len(enriched.get("tech_stack", []))
                status = enriched.get("enrichment_status", "unknown")
                errs = enriched.get("enrichment_errors", [])

                if status == "complete":
                    extras = []
                    if n_tech > 0:
                        extras.append(f"{n_tech} techs")
                    if enriched.get("schema_org"):
                        extras.append("schema.org")
                    if enriched.get("cms"):
                        extras.append(f"CMS:{enriched['cms']}")
                    if enriched.get("email_provider"):
                        extras.append(f"MX:{enriched['email_provider']}")
                    if enriched.get("spf_services"):
                        extras.append(f"SPF:{len(enriched['spf_services'])}")
                    print(f"OK ({', '.join(extras) if extras else 'no tech detected'})")
                else:
                    for e in errs:
                        error_types[e] += 1
                    print(f"ERROR ({', '.join(errs)})")

                # Rate limit
                if i < len(to_enrich) - 1:
                    await asyncio.sleep(RATE_LIMIT_DELAY)

                # Checkpoint every 100 records to avoid losing progress
                if processed % 100 == 0:
                    _save_checkpoint(already_done, enriched_results, no_domain)
                    print(f"  [Checkpoint saved: {len(already_done) + len(enriched_results)} enriched]")

    # Combine all results: already_done + newly enriched + no_domain
    all_output = already_done + enriched_results
    for r in no_domain:
        r["enrichment_status"] = "skipped_no_domain"
        all_output.append(r)

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for record in all_output:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    elapsed = time.time() - start_time

    # Also update the NEMA enriched file with newly enriched NEMA records
    nema_enriched_records = [r for r in (already_done + enriched_results)
                            if r.get("association") == "NEMA"]
    nema_pending = [r for r in no_domain if r.get("association") == "NEMA"]
    if nema_enriched_records or nema_pending:
        nema_all = nema_enriched_records + nema_pending
        with open(NEMA_ENRICHED, "w", encoding="utf-8") as f:
            for record in nema_all:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(f"\nUpdated {NEMA_ENRICHED.name}: {len(nema_enriched_records)} enriched, {len(nema_pending)} pending")

    # Summary stats
    all_enriched = already_done + enriched_results
    complete = [r for r in all_enriched if r.get("enrichment_status") == "complete"]
    with_tech = [r for r in complete if r.get("tech_stack")]
    with_schema = [r for r in complete if r.get("schema_org")]
    with_contact = [r for r in complete if r.get("has_contact_page")]
    with_team = [r for r in complete if r.get("has_team_page")]
    with_cms = [r for r in complete if r.get("cms")]
    with_mx = [r for r in complete if r.get("has_mx")]
    with_email_prov = [r for r in complete if r.get("email_provider")]
    with_spf = [r for r in complete if r.get("spf_services")]
    with_email_cands = [r for r in complete
                        if any(c.get("email_candidates") for c in r.get("contacts", []))]

    print()
    print("=" * 70)
    print("ENRICHMENT RESULTS SUMMARY")
    print("=" * 70)
    print(f"Total records processed: {total_to_process}")
    print(f"Previously enriched:     {len(already_done)}")
    print(f"DNS-only enriched:       {len(to_enrich_dns)}")
    print(f"Full HTTP+DNS enriched:  {len([r for r in enriched_results if r not in to_enrich_dns and r.get('enrichment_status') == 'complete'])}")
    print(f"Errors:                  {len([r for r in enriched_results if r.get('enrichment_status') == 'error'])}")
    print(f"No domain (skipped):     {len(no_domain)}")
    print(f"Total output records:    {len(all_output)}")
    print()

    total_complete = len(complete)
    if total_complete > 0:
        print(f"Records with tech stack:   {len(with_tech)}/{total_complete} ({len(with_tech)/total_complete*100:.1f}%)")
        print(f"Records with schema.org:   {len(with_schema)}/{total_complete} ({len(with_schema)/total_complete*100:.1f}%)")
        print(f"Records with contact pg:   {len(with_contact)}/{total_complete} ({len(with_contact)/total_complete*100:.1f}%)")
        print(f"Records with team page:    {len(with_team)}/{total_complete} ({len(with_team)/total_complete*100:.1f}%)")
        print(f"Records with CMS:          {len(with_cms)}/{total_complete} ({len(with_cms)/total_complete*100:.1f}%)")
        print(f"Records with MX records:   {len(with_mx)}/{total_complete} ({len(with_mx)/total_complete*100:.1f}%)")
        print(f"Records with email prov:   {len(with_email_prov)}/{total_complete} ({len(with_email_prov)/total_complete*100:.1f}%)")
        print(f"Records with SPF services: {len(with_spf)}/{total_complete} ({len(with_spf)/total_complete*100:.1f}%)")
        print(f"Records with email cands:  {len(with_email_cands)}/{total_complete} ({len(with_email_cands)/total_complete*100:.1f}%)")

    # Top 10 technologies
    tech_counter = Counter()
    for r in complete:
        for t in r.get("tech_stack", []):
            tech_counter[t] += 1
    if tech_counter:
        print()
        print("TOP 10 TECHNOLOGIES:")
        print("-" * 40)
        for tech, count in tech_counter.most_common(10):
            print(f"  {tech:<30} {count:>4} ({count/total_complete*100:.1f}%)")

    # CMS distribution
    cms_counter = Counter()
    for r in complete:
        if r.get("cms"):
            cms_counter[r["cms"]] += 1
    if cms_counter:
        print()
        print("CMS DISTRIBUTION:")
        print("-" * 40)
        for cms, count in cms_counter.most_common(10):
            print(f"  {cms:<30} {count:>4}")

    # Email provider distribution
    provider_counter = Counter()
    for r in complete:
        prov = r.get("email_provider")
        if prov:
            provider_counter[prov] += 1
    if provider_counter:
        print()
        print("EMAIL PROVIDER DISTRIBUTION:")
        print("-" * 40)
        for prov, count in provider_counter.most_common(10):
            print(f"  {prov:<30} {count:>4} ({count/total_complete*100:.1f}%)")

    # SPF services distribution
    spf_counter = Counter()
    for r in complete:
        for svc in r.get("spf_services", []):
            spf_counter[svc] += 1
    if spf_counter:
        print()
        print("SPF-DETECTED SERVICES:")
        print("-" * 40)
        for svc, count in spf_counter.most_common(15):
            print(f"  {svc:<30} {count:>4} ({count/total_complete*100:.1f}%)")

    # Error breakdown
    if error_types:
        print()
        print("ERRORS BY TYPE:")
        print("-" * 40)
        for err, count in error_types.most_common():
            print(f"  {err:<40} {count:>4}")

    print()
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Elapsed time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
