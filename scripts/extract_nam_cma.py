"""
Extract NAM CMA member organizations from the NAM website.
Outputs JSONL to data/raw/intelligence/nam_cma_associations.jsonl
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


SOURCE_URL = "https://nam.org/alliances/council-of-manufacturing-associations/member-organizations/"
OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "raw", "intelligence", "nam_cma_associations.jsonl"
)


def get_js_code():
    """Return JS code that extracts associations from the page."""
    return """
() => {
    const mainContent = document.querySelector('.entry-content') ||
                       document.querySelector('main') ||
                       document.body;

    // Step 1: Build map of linked associations (name -> href)
    const linkedMap = {};
    mainContent.querySelectorAll('a').forEach(function(a) {
        const name = a.innerText.trim();
        const href = a.href || '';
        if (!name) return;
        if (href.indexOf('#') !== -1) return;
        if (!href || href === 'https://') return;
        if (href.indexOf('https://nam.org') === 0) return;
        if (href.indexOf('http://nam.org') === 0) return;
        linkedMap[name] = href;
    });

    // Step 2: Extract all names from paragraph text (includes unlinked)
    const allNames = {};
    Object.keys(linkedMap).forEach(function(k) { allNames[k] = linkedMap[k]; });

    mainContent.querySelectorAll('p').forEach(function(p) {
        const text = p.innerText.trim();
        if (!text) return;
        // Split on newline character
        const lines = text.split(String.fromCharCode(10));
        lines.forEach(function(line) {
            const trimmed = line.trim();
            if (!trimmed || trimmed.length < 4) return;
            const lower = trimmed.toLowerCase();
            const skipWords = ['about us', 'issues', 'search', 'member', 'contact',
                               'home', 'skip', 'menu', 'open', 'close',
                               'get involved', 'become', 'business operations'];
            let skip = false;
            skipWords.forEach(function(w) { if (lower === w) skip = true; });
            if (!skip) {
                if (!allNames.hasOwnProperty(trimmed)) {
                    allNames[trimmed] = '';
                }
            }
        });
    });

    // Step 3: Build sorted result array
    const associations = [];
    Object.keys(allNames).forEach(function(name) {
        associations.push({name: name, href: allNames[name]});
    });
    associations.sort(function(a, b) { return a.name.localeCompare(b.name); });

    return {
        total: associations.length,
        linked: Object.keys(linkedMap).length,
        associations: associations
    };
}
"""


def extract_domain(url):
    """Extract bare domain from a URL."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


async def run_extraction():
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        print(f"Navigating to {SOURCE_URL}")
        await page.goto(SOURCE_URL, wait_until="networkidle", timeout=60000)
        print("Page loaded. Scrolling to ensure all content is rendered...")

        # Scroll multiple times to trigger any lazy loading
        for i in range(6):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
        # Scroll back to top then full bottom again
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.5)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)

        print("Extracting associations...")
        results = await page.evaluate(get_js_code())

        await browser.close()
        return results


def main():
    results = asyncio.run(run_extraction())

    associations = results["associations"]
    total = results["total"]
    linked = results["linked"]

    print(f"\nExtracted: {total} total, {linked} with links, {total - linked} unlinked")

    # Build JSONL records
    extracted_at = datetime.now(timezone.utc).isoformat()
    records = []
    for assoc in associations:
        name = assoc["name"]
        website = assoc["href"]
        domain = extract_domain(website)

        record = {
            "association_name": name,
            "website": website,
            "domain": domain,
            "source_url": SOURCE_URL,
            "extracted_at": extracted_at,
        }
        records.append(record)

    # Write to JSONL
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    print(f"\nWrote {len(records)} records to: {OUTPUT_PATH}")

    # Print summary stats
    with_website = sum(1 for r in records if r["website"])
    without_website = sum(1 for r in records if not r["website"])
    print(f"  With website: {with_website}")
    print(f"  Without website: {without_website}")
    print("\nSample records:")
    for rec in records[:5]:
        print(f"  {rec['association_name']} -> {rec['website'] or '(no link)'}")

    return len(records)


if __name__ == "__main__":
    count = main()
    print(f"\nTotal extracted: {count}")
