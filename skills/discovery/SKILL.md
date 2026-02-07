# Discovery Skill

## Overview

Discovery agents analyze association websites and discover all member listing URLs. This includes mapping site structure and following pagination.

---

## Agents

1. **Site Mapper**: Analyzes website structure to find member directories
2. **Link Crawler**: Follows pagination to discover all member URLs

---

## Site Mapper Agent

### Purpose
Analyze an association website to identify member directory URLs, pagination patterns, and authentication requirements.

### Input
```json
{
  "base_url": "https://pma.org",
  "directory_patterns": ["/members", "/directory", "/member-list"],
  "max_depth": 3
}
```

### Output
```json
{
  "directory_url": "https://pma.org/directory/results.asp?n=2000",
  "pagination": {
    "type": "query_param",
    "param": "n",
    "format": "?n={count}"
  },
  "auth_required": false,
  "estimated_members": 1134
}
```

### Process

```python
async def map_site(base_url: str, patterns: list[str]) -> dict:
    # 1. Check robots.txt FIRST
    robots = await fetch_robots_txt(base_url)
    
    # 2. Try common directory patterns
    directory_url = None
    for pattern in patterns:
        url = urljoin(base_url, pattern)
        if robots.can_fetch("*", url):
            response = await fetch(url, timeout=10)
            if response.status_code == 200:
                directory_url = url
                break
    
    if not directory_url:
        # 3. Check sitemap.xml
        sitemap_urls = await parse_sitemap(f"{base_url}/sitemap.xml")
        for url in sitemap_urls:
            if any(p in url for p in ["member", "directory"]):
                directory_url = url
                break
    
    if not directory_url:
        raise NoDirectoryFoundError(base_url)
    
    # 4. Analyze pagination
    page_html = await fetch(directory_url)
    pagination = detect_pagination(page_html)
    
    # 5. Estimate member count
    member_count = estimate_members(page_html)
    
    return {
        "directory_url": directory_url,
        "pagination": pagination,
        "auth_required": check_auth_required(page_html),
        "estimated_members": member_count
    }
```

### Pagination Detection

```python
def detect_pagination(html: str) -> dict:
    soup = BeautifulSoup(html, 'lxml')
    
    # Check for query parameter pagination: ?page=2, ?n=100
    query_links = soup.find_all('a', href=re.compile(r'\?(page|p|n|offset)=\d+'))
    if query_links:
        href = query_links[0]['href']
        param = re.search(r'\?(page|p|n|offset)=', href).group(1)
        return {"type": "query_param", "param": param}
    
    # Check for path pagination: /page/2
    path_links = soup.find_all('a', href=re.compile(r'/page[-/]?\d+'))
    if path_links:
        return {"type": "path_segment", "pattern": "/page/{n}"}
    
    # Check for infinite scroll
    if soup.find(attrs={"data-infinite-scroll": True}):
        return {"type": "infinite_scroll"}
    
    # Check for "Load More" button
    load_more = soup.find(['button', 'a'], string=re.compile(r'load more|show more', re.I))
    if load_more:
        return {"type": "load_more"}
    
    return {"type": "none"}
```

---

## Link Crawler Agent

### Purpose
Starting from a directory entry point, discover ALL member profile/listing URLs by following pagination.

### Input
```json
{
  "entry_url": "https://pma.org/directory/results.asp?n=2000",
  "pagination": {"type": "query_param", "param": "n"},
  "max_pages": 100
}
```

### Output
```json
{
  "member_urls": [
    "https://pma.org/member/12345",
    "https://pma.org/member/12346"
  ],
  "total_pages": 12,
  "total_urls": 1134
}
```

### Process

```python
async def crawl_directory(entry_url: str, pagination: dict, max_pages: int = 100) -> list[str]:
    member_urls = set()
    page = 1
    
    while page <= max_pages:
        # Build page URL
        if pagination["type"] == "query_param":
            page_url = f"{entry_url}?{pagination['param']}={page * 100}"
        elif pagination["type"] == "path_segment":
            page_url = f"{entry_url}/page/{page}"
        else:
            page_url = entry_url
        
        # Fetch with rate limiting
        await rate_limiter.acquire(urlparse(page_url).netloc)
        response = await fetch(page_url)
        
        if response.status_code == 404:
            break  # No more pages
        
        # Extract member URLs
        soup = BeautifulSoup(response.text, 'lxml')
        new_urls = extract_member_urls(soup, entry_url)
        
        if not new_urls:
            break  # Empty page
        
        member_urls.update(new_urls)
        
        # Log progress
        log.info(f"Page {page}: found {len(new_urls)} URLs, total: {len(member_urls)}")
        
        # Check for next page
        if pagination["type"] == "none" or not has_next_page(soup):
            break
        
        page += 1
    
    return list(member_urls)
```

### Extract Member URLs

```python
def extract_member_urls(soup: BeautifulSoup, base_url: str) -> set[str]:
    urls = set()
    
    # Common member link patterns
    patterns = [
        ('a', {'href': re.compile(r'/member/|/company/|/profile/')}),
        ('a', {'class': re.compile(r'member|company|profile')}),
        ('.member-item a', {}),
        ('.company-card a', {}),
        ('.directory-listing a', {}),
    ]
    
    for selector, attrs in patterns:
        if '.' in selector:
            # CSS selector
            container_class = selector.split()[0]
            for container in soup.select(container_class):
                for link in container.find_all('a', href=True):
                    url = urljoin(base_url, link['href'])
                    if is_member_url(url):
                        urls.add(url)
        else:
            # Tag + attrs
            for link in soup.find_all(selector, attrs, href=True):
                url = urljoin(base_url, link['href'])
                if is_member_url(url):
                    urls.add(url)
    
    return urls

def is_member_url(url: str) -> bool:
    """Filter out non-member URLs"""
    exclude = ['/login', '/register', '/contact', '/about', '/faq',
               '.pdf', '.doc', '.xls', 'javascript:', 'mailto:', 'tel:']
    return not any(ex in url.lower() for ex in exclude)
```

### Infinite Scroll Handling

```python
async def handle_infinite_scroll(page_url: str) -> list[str]:
    """Use Playwright for infinite scroll pages"""
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(page_url)
        
        member_urls = set()
        prev_count = 0
        no_change = 0
        
        while no_change < 3:
            # Extract current URLs
            urls = await page.evaluate('''
                () => Array.from(document.querySelectorAll('a'))
                    .map(a => a.href)
                    .filter(h => /member|company|profile/.test(h))
            ''')
            member_urls.update(urls)
            
            if len(member_urls) == prev_count:
                no_change += 1
            else:
                no_change = 0
            prev_count = len(member_urls)
            
            # Scroll down
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(1500)
        
        await browser.close()
        return list(member_urls)
```

---

## Rate Limiting

**CRITICAL**: Always respect rate limits

```python
RATE_LIMITS = {
    "default": 1.0,  # 1 request/second
    "pma.org": 0.5,
    "makeitelectric.org": 0.5,
    "linkedin.com": 0.2,
}

class RateLimiter:
    def __init__(self):
        self.last_request = {}
    
    async def acquire(self, domain: str):
        rate = RATE_LIMITS.get(domain, RATE_LIMITS["default"])
        min_interval = 1.0 / rate
        
        last = self.last_request.get(domain, 0)
        elapsed = time.time() - last
        
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
        
        self.last_request[domain] = time.time()
```

---

## robots.txt Compliance

**ALWAYS** check robots.txt before crawling:

```python
from urllib.robotparser import RobotFileParser

async def check_robots_txt(base_url: str, path: str) -> bool:
    robots_url = f"{base_url}/robots.txt"
    parser = RobotFileParser()
    
    try:
        response = await fetch(robots_url)
        parser.parse(response.text.splitlines())
    except:
        return True  # Allow if no robots.txt
    
    return parser.can_fetch("NAM-IntelBot", urljoin(base_url, path))
```

---

## Output Files

### Site Map
```
data/raw/{association}/site_map.json
```

### URL Queue
```
data/raw/{association}/urls.jsonl
```

Each line:
```json
{"url": "https://pma.org/member/123", "discovered_at": "2026-01-29T12:00:00Z"}
```

---

## Error Handling

| Error | Action |
|-------|--------|
| robots.txt blocks | Skip domain, log warning |
| 404 Not Found | End pagination, return collected URLs |
| 403 Forbidden | Try different User-Agent, then skip |
| Timeout | Retry up to 3 times |
| No directory found | Raise error, manual investigation needed |

---

## Best Practices

1. **Check robots.txt first** - Always respect site rules
2. **Use descriptive User-Agent** - `NAM-IntelBot/1.0 (contact@example.com)`
3. **Rate limit aggressively** - Better slow than blocked
4. **Deduplicate URLs** - Use set() or hash-based dedup
5. **Log progress** - Large crawls need visibility
6. **Handle dynamic content** - Use Playwright for JS-heavy sites
