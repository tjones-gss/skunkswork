"""
Batch Enrichment Script - Enrich ALL records with domains.

Sources:
- data/processed/NEMA/enriched.jsonl (300 records, 285 pending)
- data/raw/AGMA/records_live.jsonl (432 records with domains)

For each record with a domain:
1. Skip already-enriched records (enrichment_status == "complete")
2. Fetch homepage via httpx GET with Chrome User-Agent
3. Tech stack detection from HTML (168 fingerprints)
4. Tech stack from HTTP headers (X-Powered-By, Server)
5. CMS detection from <meta name="generator">
6. Schema.org JSON-LD extraction
7. Contact page detection (/contact, /contact-us, /about, /about-us)
8. Team page detection (/team, /leadership, /our-team)
"""

import asyncio
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input files
NEMA_ENRICHED = PROJECT_ROOT / "data" / "processed" / "NEMA" / "enriched.jsonl"
AGMA_RECORDS = PROJECT_ROOT / "data" / "raw" / "AGMA" / "records_live.jsonl"

# Output
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "enriched_all.jsonl"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

RATE_LIMIT_DELAY = 2  # seconds between domains

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


async def enrich_record(client: httpx.AsyncClient, record: dict) -> dict:
    """Enrich a single record with website data."""
    enriched = dict(record)
    domain = record.get("domain", "")
    website = record.get("website", "")
    company = record.get("company_name", "unknown")

    if not domain:
        enriched["enrichment_status"] = "skipped_no_domain"
        return enriched

    # Build website URL if not present
    if not website:
        website = f"https://{domain}"

    tech_stack = []
    cms = None
    schema_org = None
    has_contact_page = False
    has_team_page = False
    errors = []

    # Fetch homepage
    try:
        resp = await client.get(website, follow_redirects=True, timeout=10)
        html = resp.text
        status = resp.status_code

        if status == 200 and html:
            tech_stack = detect_tech_from_html(html)
            header_techs = detect_tech_from_headers(dict(resp.headers))
            for t in header_techs:
                if t not in tech_stack:
                    tech_stack.append(t)
            tech_stack.sort()
            cms = detect_cms_from_generator(html)
            schema_org = extract_schema_org(html)
        else:
            errors.append(f"homepage_status_{status}")
    except httpx.RequestError as e:
        err_type = type(e).__name__
        errors.append(f"homepage_{err_type}")

    # Check contact and team pages
    base_url = f"https://{domain}"

    for path in CONTACT_PATHS:
        if await check_page_exists(client, base_url, path):
            has_contact_page = True
            break

    for path in TEAM_PATHS:
        if await check_page_exists(client, base_url, path):
            has_team_page = True
            break

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
    enriched["enrichment_status"] = "complete" if not errors else "error"
    if errors:
        enriched["enrichment_errors"] = errors

    return enriched


def load_records() -> list[dict]:
    """Load all records from both sources."""
    records = []

    # Load NEMA enriched records
    if NEMA_ENRICHED.exists():
        with open(NEMA_ENRICHED) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        print(f"  NEMA: {len(records)} records loaded from {NEMA_ENRICHED.name}")

    nema_count = len(records)

    # Load AGMA records
    if AGMA_RECORDS.exists():
        with open(AGMA_RECORDS) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        agma_count = len(records) - nema_count
        print(f"  AGMA: {agma_count} records loaded from {AGMA_RECORDS.name}")

    return records


async def main():
    start_time = time.time()

    print("=" * 70)
    print("BATCH ENRICHMENT - All Records with Domains")
    print("=" * 70)
    print()

    # Load all records
    print("Loading records...")
    all_records = load_records()
    print(f"  Total: {len(all_records)} records")
    print()

    # Partition into already-enriched vs needs-enrichment
    already_done = []
    to_enrich = []
    no_domain = []

    for r in all_records:
        if r.get("enrichment_status") == "complete":
            already_done.append(r)
        elif not r.get("domain"):
            no_domain.append(r)
        else:
            to_enrich.append(r)

    print(f"Already enriched (skipping): {len(already_done)}")
    print(f"No domain (skipping): {len(no_domain)}")
    print(f"To enrich: {len(to_enrich)}")
    print()

    if not to_enrich:
        print("Nothing to enrich. Exiting.")
        return

    # Enrich records
    enriched_results = []
    error_types = Counter()
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

            enriched = await enrich_record(client, record)
            enriched_results.append(enriched)

            # Log result
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
                print(f"OK ({', '.join(extras) if extras else 'no tech detected'})")
            else:
                for e in errs:
                    error_types[e] += 1
                print(f"ERROR ({', '.join(errs)})")

            # Rate limit
            if i < len(to_enrich) - 1:
                await asyncio.sleep(RATE_LIMIT_DELAY)

    # Combine all results: already_done + newly enriched + no_domain
    all_output = already_done + enriched_results
    for r in no_domain:
        r["enrichment_status"] = "skipped_no_domain"
        all_output.append(r)

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        for record in all_output:
            f.write(json.dumps(record) + "\n")

    elapsed = time.time() - start_time

    # Also update the NEMA enriched file with newly enriched NEMA records
    nema_enriched_records = [r for r in (already_done + enriched_results)
                            if r.get("association") == "NEMA"]
    nema_pending = [r for r in no_domain if r.get("association") == "NEMA"]
    if nema_enriched_records or nema_pending:
        nema_all = nema_enriched_records + nema_pending
        with open(NEMA_ENRICHED, "w") as f:
            for record in nema_all:
                f.write(json.dumps(record) + "\n")
        print(f"\nUpdated {NEMA_ENRICHED.name}: {len(nema_enriched_records)} enriched, {len(nema_pending)} pending")

    # Summary stats
    all_enriched = already_done + enriched_results
    complete = [r for r in all_enriched if r.get("enrichment_status") == "complete"]
    with_tech = [r for r in complete if r.get("tech_stack")]
    with_schema = [r for r in complete if r.get("schema_org")]
    with_contact = [r for r in complete if r.get("has_contact_page")]
    with_team = [r for r in complete if r.get("has_team_page")]
    with_cms = [r for r in complete if r.get("cms")]

    print()
    print("=" * 70)
    print("ENRICHMENT RESULTS SUMMARY")
    print("=" * 70)
    print(f"Total records processed: {processed}")
    print(f"Previously enriched:     {len(already_done)}")
    print(f"Newly enriched:          {len([r for r in enriched_results if r.get('enrichment_status') == 'complete'])}")
    print(f"Errors:                  {len([r for r in enriched_results if r.get('enrichment_status') == 'error'])}")
    print(f"No domain (skipped):     {len(no_domain)}")
    print(f"Total output records:    {len(all_output)}")
    print()

    total_complete = len(complete)
    if total_complete > 0:
        print(f"Records with tech stack: {len(with_tech)}/{total_complete} ({len(with_tech)/total_complete*100:.1f}%)")
        print(f"Records with schema.org: {len(with_schema)}/{total_complete} ({len(with_schema)/total_complete*100:.1f}%)")
        print(f"Records with contact pg: {len(with_contact)}/{total_complete} ({len(with_contact)/total_complete*100:.1f}%)")
        print(f"Records with team page:  {len(with_team)}/{total_complete} ({len(with_team)/total_complete*100:.1f}%)")
        print(f"Records with CMS:        {len(with_cms)}/{total_complete} ({len(with_cms)/total_complete*100:.1f}%)")

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
