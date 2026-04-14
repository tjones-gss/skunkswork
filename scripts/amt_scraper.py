"""
AMT Member Directory Scraper
Extracts member companies from https://amtonline.org/membership/member-directory
Uses Playwright to handle Angular client-side rendering and pagination.

Card structure (confirmed from DOM):
  DIV.card  (shadow col-md-6 col-lg-3 ...)
    DIV.view.overlay.image-background
      IMG.member-card-img-top
    DIV.card-body
      H4.card-title.member-card-title   <- company name
      P.card-text.member-card-body      <- "State, Country" or "City, State" or "City, Country"

Location format observed: "Michigan, United States" (state-level, no city)
Member type: applied as a UI filter — each extraction run is for one member type.

Pagination: DIV.pagination > DIV.pages (active = current) + DIV.next
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright


OUTPUT_PATH = Path(__file__).parent.parent / "data" / "raw" / "AMT" / "records.jsonl"
SOURCE_URL = "https://amtonline.org/membership/member-directory"

# US state name -> abbreviation mapping
STATE_ABBREVS = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
}

MEMBER_TYPES = [
    ("Regular", "Regular"),
    ("Commercial Affiliates", "Commercial Affiliates"),
    ("Research and Education", "Research and Education"),
    ("Affiliates", "Affiliates"),
]


def parse_location(location_text: str) -> tuple[str, str, str]:
    """
    Parse AMT location text into (city, state, country).

    Observed formats:
      "Michigan, United States"        -> city='', state='MI', country='United States'
      "Nashville, TN, United States"   -> city='Nashville', state='TN', country='United States'
      "City, ST"                        -> city='City', state='ST', country='United States'
      "City, Country"                   -> city='City', state='', country='Country'
      "Germany"                         -> city='', state='', country='Germany'
    """
    text = location_text.strip()
    if not text:
        return '', '', 'United States'

    # "Part1, Part2, Part3"
    parts = [p.strip() for p in text.split(',')]

    if len(parts) == 3:
        # "City, State/ST, Country"
        city = parts[0]
        state_raw = parts[1].strip()
        country = parts[2].strip()
        # Normalize state to abbreviation
        state = STATE_ABBREVS.get(state_raw, state_raw if len(state_raw) == 2 else '')
        return city, state, country

    if len(parts) == 2:
        p1, p2 = parts[0].strip(), parts[1].strip()

        # "State, United States" (no city)
        if p1 in STATE_ABBREVS:
            return '', STATE_ABBREVS[p1], p2

        # "City, ST" (two-letter state code)
        if len(p2) == 2 and p2.isupper():
            return p1, p2, 'United States'

        # "City, Country" or "State, Country"
        # Check if p2 is a country
        known_countries = {
            'United States', 'Canada', 'Germany', 'Japan', 'China', 'Taiwan',
            'United Kingdom', 'France', 'Italy', 'South Korea', 'Switzerland',
            'Sweden', 'Spain', 'Netherlands', 'Austria', 'Belgium', 'Denmark',
            'Finland', 'Norway', 'Poland', 'Czech Republic', 'Hungary', 'Israel',
            'India', 'Brazil', 'Mexico', 'Australia', 'New Zealand', 'Singapore',
            'Thailand', 'Portugal', 'Slovakia',
        }
        if p2 in known_countries:
            # p1 could be a state name or city
            if p1 in STATE_ABBREVS:
                return '', STATE_ABBREVS[p1], p2
            return p1, '', p2

        return p1, '', p2

    if len(parts) == 1:
        # Could be just a country or just a state
        p = parts[0].strip()
        if p in STATE_ABBREVS:
            return '', STATE_ABBREVS[p], 'United States'
        return '', '', p

    return '', '', 'United States'


async def apply_member_type_filter(page, member_type_label: str) -> bool:
    """Click a member type filter button on the AMT directory page."""
    result = await page.evaluate(f"""
        () => {{
            // Find FILTER BY MEMBER TYPE section
            const filterLabel = '{member_type_label}';

            // Look for filter buttons/links
            const allEls = Array.from(document.querySelectorAll('*'));
            for (const el of allEls) {{
                const text = (el.textContent || '').trim();
                if (text === filterLabel && el.children.length === 0) {{
                    el.click();
                    return true;
                }}
            }}

            // Try spans and divs that contain exactly this text
            const candidates = allEls.filter(el => {{
                return el.innerText && el.innerText.trim() === filterLabel;
            }});
            if (candidates.length > 0) {{
                candidates[0].click();
                return true;
            }}

            return false;
        }}
    """)
    return result


async def clear_member_type_filter(page) -> bool:
    """Click 'All' to reset member type filter."""
    return await apply_member_type_filter(page, 'All')


async def extract_page_records(page, member_type: str = "") -> list[dict]:
    """Extract all member cards from the current Angular-rendered page."""
    now = datetime.now(timezone.utc).isoformat()

    result = await page.evaluate(f"""
        () => {{
            const now = '{now}';
            const records = [];

            const cards = Array.from(document.querySelectorAll('div.card'));

            for (const card of cards) {{
                const titleEl = card.querySelector('h4.card-title, h4.member-card-title, .card-title');
                if (!titleEl) continue;
                const companyName = titleEl.textContent.trim();
                if (!companyName || companyName.length < 2 || companyName.length > 200) continue;

                const bodyEl = card.querySelector('p.card-text, .member-card-body, .card-text');
                const locationText = bodyEl ? bodyEl.textContent.trim() : '';

                records.push({{
                    company_name: companyName,
                    location_raw: locationText,
                    association: 'AMT',
                    source_url: window.location.href,
                    extracted_at: now,
                }});
            }}

            return {{
                records,
                cardCount: cards.length,
            }};
        }}
    """)

    records = result.get('records', [])
    card_count = result.get('cardCount', 0)
    print(f"  {card_count} cards, {len(records)} records")
    return records


async def get_pagination(page) -> dict:
    return await page.evaluate("""
        () => {
            const pagination = document.querySelector('.pagination');
            if (!pagination) return {found: false, totalPages: 0, currentPage: 0, hasNext: false};
            const pageEls = Array.from(pagination.querySelectorAll('.pages'));
            const activeEl = pagination.querySelector('.pages.active');
            const currentPage = activeEl ? parseInt(activeEl.textContent.trim()) || 1 : 1;
            const totalPages = pageEls.length;
            return {
                found: true,
                currentPage,
                totalPages,
                hasNext: currentPage < totalPages,
                nextPageNum: currentPage + 1,
            };
        }
    """)


async def click_page(page, target: int) -> bool:
    return await page.evaluate(f"""
        () => {{
            const pagination = document.querySelector('.pagination');
            if (!pagination) return false;
            const pageEls = Array.from(pagination.querySelectorAll('.pages'));
            for (const el of pageEls) {{
                if (parseInt(el.textContent.trim()) === {target}) {{
                    el.click();
                    return true;
                }}
            }}
            // Try .next element
            const nextEl = pagination.querySelector('.next');
            if (nextEl && !nextEl.classList.contains('disabled')) {{
                nextEl.click();
                return true;
            }}
            return false;
        }}
    """)


async def scrape_all_pages(page, member_type: str = "") -> list[dict]:
    """Scrape all pages for current filter state and return parsed records."""
    collected = []
    seen = set()
    page_num = 1

    while page_num <= 60:
        print(f"  Page {page_num}:")
        await asyncio.sleep(1.5)

        records = await extract_page_records(page, member_type)
        for r in records:
            key = r['company_name'].lower().strip()
            if key not in seen:
                seen.add(key)
                # Parse location
                city, state, country = parse_location(r.get('location_raw', ''))
                collected.append({
                    'company_name': r['company_name'],
                    'city': city,
                    'state': state,
                    'country': country,
                    'member_type': member_type,
                    'association': r['association'],
                    'source_url': r['source_url'],
                    'extracted_at': r['extracted_at'],
                })

        pag = await get_pagination(page)
        print(f"    Pagination: {pag.get('currentPage')}/{pag.get('totalPages')}")

        if not pag.get('hasNext', False):
            break

        await asyncio.sleep(1.5)
        ok = await click_page(page, pag['nextPageNum'])
        if not ok:
            print(f"    Could not advance to page {pag['nextPageNum']}. Stopping.")
            break

        await asyncio.sleep(2.5)
        page_num += 1

    print(f"  Total collected for '{member_type or 'All'}': {len(collected)}")
    return collected


async def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        print("Launching headed browser...")
        browser = await pw.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()

        print(f"Navigating to {SOURCE_URL} ...")
        await page.goto(SOURCE_URL, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(3)
        print(f"Page title: {await page.title()}")

        # Debug: print first card to confirm structure
        first_card = await page.evaluate("""
            () => {
                const card = document.querySelector('div.card');
                if (!card) return 'No card found';
                const title = card.querySelector('.card-title');
                const body = card.querySelector('.card-text');
                return {
                    title: title ? title.textContent.trim() : 'NO TITLE',
                    body: body ? body.textContent.trim() : 'NO BODY',
                    outerHtml: card.outerHTML.substring(0, 600),
                };
            }
        """)
        print(f"\nFirst card sample: {first_card}\n")

        all_records = []

        # Extract each member type separately so we know which type each record is
        for filter_name, member_type_label in MEMBER_TYPES:
            print(f"\n{'='*50}")
            print(f"Filtering by: {filter_name}")

            # Click the filter
            clicked = await apply_member_type_filter(page, filter_name)
            print(f"  Filter click result: {clicked}")
            await asyncio.sleep(2.5)  # Wait for Angular to re-render

            # Verify filter applied
            pag = await get_pagination(page)
            print(f"  After filter - pagination: {pag}")

            # Scrape all pages for this member type
            records = await scrape_all_pages(page, member_type_label)
            all_records.extend(records)

            # Reset to first page before next filter
            if pag.get('totalPages', 0) > 1:
                await click_page(page, 1)
                await asyncio.sleep(1.5)

        await browser.close()

    # Deduplicate across all member types (prefer Regular over others)
    seen_names = {}
    for r in all_records:
        name = r['company_name'].lower().strip()
        if name not in seen_names:
            seen_names[name] = r
        elif r['member_type'] == 'Regular':
            # Prefer Regular member type
            seen_names[name] = r

    final_records = list(seen_names.values())

    # Save
    print(f"\nSaving {len(final_records)} unique records to {OUTPUT_PATH}")
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for r in final_records:
            f.write(json.dumps(r) + "\n")

    # Summary
    member_types = {}
    states = {}
    for r in final_records:
        mt = r.get('member_type') or 'Unknown'
        member_types[mt] = member_types.get(mt, 0) + 1
        st = r.get('state', '') or ''
        if st:
            states[st] = states.get(st, 0) + 1

    print(f"\n=== EXTRACTION COMPLETE ===")
    print(f"Total unique records: {len(final_records)}")
    print(f"\nMember type breakdown:")
    for mt, cnt in sorted(member_types.items(), key=lambda x: -x[1]):
        print(f"  {mt}: {cnt}")

    regular = sum(1 for r in final_records if r.get('member_type') == 'Regular')
    print(f"\nRegular members (ERP targets): {regular}")

    # Sample records
    print(f"\nSample records (first 10):")
    for r in final_records[:10]:
        print(f"  {r['company_name']} | {r['city']}, {r['state']} {r['country']} | {r['member_type']}")

    # Verify output file
    line_count = sum(1 for _ in open(OUTPUT_PATH, encoding='utf-8'))
    print(f"\nOutput file: {OUTPUT_PATH}")
    print(f"Lines written: {line_count}")

    return final_records


if __name__ == "__main__":
    asyncio.run(main())
