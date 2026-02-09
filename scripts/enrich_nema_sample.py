"""
NEMA Enrichment Script - Sample 15 well-known companies.

Performs website-based enrichment:
- Tech stack detection (Wappalyzer-style fingerprinting)
- Schema.org JSON-LD extraction
- Contact/team page detection
"""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NEMA_RECORDS = PROJECT_ROOT / "data" / "raw" / "NEMA" / "records_00280a42.jsonl"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "NEMA"
OUTPUT_FILE = OUTPUT_DIR / "enriched.jsonl"

SAMPLE_COMPANIES = [
    "3M",
    "ABB Inc.",
    "Siemens Industry, Inc.",
    "Eaton",
    "Honeywell",
    "Schneider Electric",
    "GE Grid Solutions",
    "Emerson",
    "Rockwell Automation",
    "Hubbell Incorporated",
    "Legrand, North America",
    "Leviton Manufacturing Co., Inc.",
    "Lutron Electronics Company, Inc.",
    "Signify North America Corporation",
    "Acuity Brands, Inc.",
]

PUBLIC_COMPANIES = {
    "3M", "ABB Inc.", "Siemens Industry, Inc.", "Eaton", "Honeywell",
    "Schneider Electric", "Emerson", "Rockwell Automation",
    "Hubbell Incorporated", "Acuity Brands, Inc.",
    "Signify North America Corporation", "Legrand, North America",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

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
            # Handle @graph arrays
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


async def enrich_company(client: httpx.AsyncClient, record: dict) -> dict:
    """Enrich a single company record with website data."""
    enriched = dict(record)
    website = record.get("website", "")
    domain = record.get("domain", "")
    company = record.get("company_name", "unknown")

    print(f"  Enriching: {company} ({domain})...")

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
        print(f"    Homepage: {status} ({len(html)} bytes)")

        if status == 200 and html:
            # Tech stack from HTML
            tech_stack = detect_tech_from_html(html)

            # Tech stack from headers
            header_techs = detect_tech_from_headers(dict(resp.headers))
            for t in header_techs:
                if t not in tech_stack:
                    tech_stack.append(t)
            tech_stack.sort()

            # CMS from generator
            cms = detect_cms_from_generator(html)

            # Schema.org
            schema_org = extract_schema_org(html)
        else:
            errors.append(f"homepage_status_{status}")
    except httpx.RequestError as e:
        err_type = type(e).__name__
        print(f"    Homepage error: {err_type}")
        errors.append(f"homepage_{err_type}")

    # Determine base URL for page checks
    base_url = f"https://{domain}"

    # Check contact pages
    for path in CONTACT_PATHS:
        exists = await check_page_exists(client, base_url, path)
        if exists:
            has_contact_page = True
            print(f"    Found: {path}")
            break

    # Check team pages
    for path in TEAM_PATHS:
        exists = await check_page_exists(client, base_url, path)
        if exists:
            has_team_page = True
            print(f"    Found: {path}")
            break

    # Build enriched record
    enriched["tech_stack"] = tech_stack
    if cms:
        enriched["cms"] = cms
    enriched["has_contact_page"] = has_contact_page
    enriched["has_team_page"] = has_team_page
    if schema_org:
        enriched["schema_org"] = schema_org
    if record["company_name"] in PUBLIC_COMPANIES:
        enriched["publicly_traded"] = True
    enriched["enriched_at"] = datetime.now(timezone.utc).isoformat()
    enriched["enrichment_source"] = "website_scrape"
    enriched["enrichment_status"] = "complete"
    if errors:
        enriched["enrichment_errors"] = errors

    print(f"    Tech stack: {len(tech_stack)} technologies found")
    if cms:
        print(f"    CMS: {cms}")
    if schema_org:
        print(f"    Schema.org: {schema_org.get('@type', 'found')}")

    return enriched


async def main():
    print("=" * 70)
    print("NEMA Enrichment Script - 15 Sample Companies")
    print("=" * 70)

    # Load all records
    records = []
    with open(NEMA_RECORDS) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    print(f"Loaded {len(records)} NEMA records")

    # Find sample companies
    sample_records = []
    sample_names = set(SAMPLE_COMPANIES)
    for r in records:
        if r["company_name"] in sample_names:
            sample_records.append(r)

    print(f"Found {len(sample_records)} of {len(SAMPLE_COMPANIES)} target companies")
    print()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Enrich sample companies
    enriched_map = {}
    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    ) as client:
        for i, record in enumerate(sample_records):
            enriched = await enrich_company(client, record)
            enriched_map[record["company_name"]] = enriched
            if i < len(sample_records) - 1:
                print(f"    (waiting 2s rate limit...)")
                await asyncio.sleep(2)
            print()

    # Write all 300 records to output
    enriched_count = 0
    pending_count = 0
    with open(OUTPUT_FILE, "w") as f:
        for record in records:
            if record["company_name"] in enriched_map:
                out = enriched_map[record["company_name"]]
                enriched_count += 1
            else:
                out = dict(record)
                out["enrichment_status"] = "pending"
                pending_count += 1
            f.write(json.dumps(out) + "\n")

    # Summary
    print("=" * 70)
    print("ENRICHMENT RESULTS SUMMARY")
    print("=" * 70)

    tech_count = sum(1 for r in enriched_map.values() if r.get("tech_stack"))
    schema_count = sum(1 for r in enriched_map.values() if r.get("schema_org"))
    contact_count = sum(1 for r in enriched_map.values() if r.get("has_contact_page"))
    team_count = sum(1 for r in enriched_map.values() if r.get("has_team_page"))

    print(f"Sample size: {len(enriched_map)} companies")
    print(f"Tech stack detected: {tech_count}/{len(enriched_map)} ({tech_count/len(enriched_map)*100:.0f}%)")
    print(f"Schema.org found: {schema_count}/{len(enriched_map)} ({schema_count/len(enriched_map)*100:.0f}%)")
    print(f"Contact page found: {contact_count}/{len(enriched_map)} ({contact_count/len(enriched_map)*100:.0f}%)")
    print(f"Team/leadership page: {team_count}/{len(enriched_map)} ({team_count/len(enriched_map)*100:.0f}%)")
    print()
    print(f"Output file: {OUTPUT_FILE}")
    print(f"  Enriched records: {enriched_count}")
    print(f"  Pending records: {pending_count}")
    print(f"  Total records: {enriched_count + pending_count}")
    print()

    # Show tech stack per company
    print("TECH STACK BY COMPANY:")
    print("-" * 70)
    for name in SAMPLE_COMPANIES:
        if name in enriched_map:
            r = enriched_map[name]
            techs = r.get("tech_stack", [])
            cms = r.get("cms", "")
            status = r.get("enrichment_status", "")
            errors = r.get("enrichment_errors", [])
            if errors:
                print(f"  {name}: [{', '.join(errors)}]")
            else:
                cms_str = f" (CMS: {cms})" if cms else ""
                print(f"  {name}: {len(techs)} techs{cms_str}")
                if techs:
                    # Show in rows of 4
                    for j in range(0, len(techs), 4):
                        chunk = techs[j:j+4]
                        print(f"    {', '.join(chunk)}")


if __name__ == "__main__":
    asyncio.run(main())
