#!/usr/bin/env python3
"""
AS/ASBAR -- American Samoa Bar Association Legal Resources Data Fetcher

Fetches legislation (ASCA), case law (ASR), regulations (ASAC), and court rules
from asbar.org via XML sitemaps + HTML scraping.

Strategy:
  - Parse sitemap XMLs to collect all document URLs
  - Fetch each page and extract full text from post-content div
  - Extract metadata from og:title and taxonomy links

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap --full     # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urlparse

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "AS/ASBAR"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AS.ASBAR")

BASE_URL = "https://asbar.org"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap_index.xml"

# Realistic browser headers to avoid VPS IP blocking by CDN/WAF
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

# Content type mapping from URL path
CONTENT_TYPE_MAP = {
    "code-annotated": ("legislation", "ASCA"),
    "case-law": ("legislation_type", "ASR"),
    "regulation": ("legislation", "ASAC"),
    "court-rules": ("legislation", "Court Rules"),
}

# Sitemap slug mapping: content type key -> list of slug prefixes to match
# in sitemap URLs. "court-rules" maps to "court_rule" (singular on the site).
SITEMAP_SLUG_MAP = {
    "code-annotated": ["code_annotated"],
    "case-law": ["case_law"],
    "regulation": ["regulation"],
    "court-rules": ["court_rule"],
}

# Build session with retry strategy
session = requests.Session()
session.headers.update(HEADERS)
retry_strategy = Retry(
    total=4,
    backoff_factor=2.0,
    status_forcelist=[403, 429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def clean_html(html: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<footer[^>]*>.*?</footer>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'</h[1-6]>', '\n\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _fetch_with_retry(url: str, timeout: int = 30) -> requests.Response:
    """Fetch URL using session (which has retry adapter). Raises on failure."""
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp


def get_sitemap_urls() -> dict[str, list[str]]:
    """Parse sitemap index and return URLs grouped by content type."""
    urls_by_type: dict[str, list[str]] = {
        "code-annotated": [],
        "case-law": [],
        "regulation": [],
        "court-rules": [],
    }

    # Step 1: fetch the sitemap index
    try:
        resp = _fetch_with_retry(SITEMAP_INDEX_URL)
    except Exception as e:
        logger.error(f"Failed to fetch sitemap index: {e}")
        return urls_by_type

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        logger.error(f"Failed to parse sitemap index XML: {e}")
        logger.debug(f"Response body (first 500 chars): {resp.text[:500]}")
        return urls_by_type

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    # Step 2: match each sitemap URL to a content type using SITEMAP_SLUG_MAP
    sitemap_urls: list[tuple[str, str]] = []
    for sitemap in root.findall(".//sm:sitemap/sm:loc", ns):
        url = sitemap.text.strip()
        # Skip taxonomy/section sitemaps
        if "regulation_section" in url or "section-sitemap" in url or "series-sitemap" in url:
            continue
        for ctype, slugs in SITEMAP_SLUG_MAP.items():
            if any(slug in url for slug in slugs):
                sitemap_urls.append((ctype, url))
                break

    logger.info(f"Found {len(sitemap_urls)} content sitemaps in index")

    # Step 3: fetch each sub-sitemap individually (errors are non-fatal)
    for ctype, smap_url in sitemap_urls:
        time.sleep(1.5)
        try:
            resp = _fetch_with_retry(smap_url)
            sroot = ET.fromstring(resp.content)
            count_before = len(urls_by_type[ctype])
            for loc in sroot.findall(".//sm:url/sm:loc", ns):
                urls_by_type[ctype].append(loc.text.strip())
            added = len(urls_by_type[ctype]) - count_before
            logger.info(f"  Sitemap {smap_url.split('/')[-1]}: {added} URLs")
        except Exception as e:
            logger.warning(f"Failed to fetch sub-sitemap {smap_url}: {e}")
            continue

    for ctype, urls in urls_by_type.items():
        logger.info(f"  {ctype}: {len(urls)} URLs total")

    return urls_by_type


def detect_content_type(url: str) -> tuple[str, str]:
    """Detect content type from URL path. Returns (_type, label)."""
    path = urlparse(url).path
    if "/code-annotated/" in path:
        return "legislation", "ASCA"
    elif "/case-law/" in path:
        return "case_law", "ASR"
    elif "/regulation/" in path:
        return "legislation", "ASAC"
    elif "/court-rules/" in path or "/court-rule/" in path or "/court_rule/" in path:
        return "legislation", "Court Rules"
    return "legislation", "Unknown"


def extract_page_content(html: str, url: str) -> Optional[dict]:
    """Extract title, metadata, and full text from an ASBAR page."""
    meta = {"url": url}

    # Title from og:title
    og_match = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', html)
    if og_match:
        title = unescape(og_match.group(1))
        title = re.sub(r'\s*[-–]\s*American Samoa Bar Association\s*$', '', title)
        meta["title"] = title.strip()
    else:
        title_match = re.search(r'<title>([^<]+)</title>', html)
        if title_match:
            title = unescape(title_match.group(1))
            title = re.sub(r'\s*[-–]\s*American Samoa Bar Association\s*$', '', title)
            meta["title"] = title.strip()

    # Section/chapter taxonomy
    section_matches = re.findall(
        r'<a\s+href="https://asbar\.org/section/[^"]*"[^>]*>([^<]+)</a>',
        html
    )
    if section_matches:
        meta["sections"] = [s.strip() for s in section_matches]

    # Series taxonomy (case law volumes)
    series_matches = re.findall(
        r'<a\s+href="https://asbar\.org/series/[^"]*"[^>]*>([^<]+)</a>',
        html
    )
    if series_matches:
        meta["series"] = [s.strip() for s in series_matches]

    # Publication/modification date from meta
    date_match = re.search(
        r'<meta\s+property="article:(?:published_time|modified_time)"\s+content="([^"]+)"',
        html
    )
    if date_match:
        meta["date"] = date_match.group(1)[:10]

    # Full text body - look for the post content div
    # Try multiple patterns for the content container
    content = ""

    # Pattern 1: Beaver Builder post content module
    bb_match = re.search(
        r'<div class="fl-module fl-module-fl-post-content[^"]*"[^>]*>.*?'
        r'<div class="fl-module-content[^"]*">(.*?)</div>\s*</div>',
        html, re.DOTALL
    )
    if bb_match:
        content = bb_match.group(1)

    # Pattern 2: entry-content div
    if not content:
        ec_match = re.search(
            r'<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL
        )
        if ec_match:
            content = ec_match.group(1)

    # Pattern 3: article body
    if not content:
        art_match = re.search(
            r'<article[^>]*>(.*?)</article>',
            html, re.DOTALL
        )
        if art_match:
            content = art_match.group(1)

    if content:
        meta["text"] = clean_html(content)
    else:
        # Fallback: extract all <p> content from body
        body_match = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL | re.IGNORECASE)
        if body_match:
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', body_match.group(1), re.DOTALL)
            if paragraphs:
                meta["text"] = clean_html("\n\n".join(paragraphs))

    return meta if meta.get("text") and len(meta["text"]) > 20 else None


def normalize(meta: dict) -> dict:
    """Transform scraped data to standard schema."""
    url = meta["url"]
    doc_type, label = detect_content_type(url)

    # Generate ID from URL slug
    path = urlparse(url).path.strip("/")
    doc_id = re.sub(r'[^a-z0-9-]', '-', path.lower())
    doc_id = re.sub(r'-+', '-', doc_id).strip('-')

    title = meta.get("title", path.split("/")[-1].replace("-", " ").title())
    date = meta.get("date")

    sections = meta.get("sections", [])
    series = meta.get("series", [])

    record = {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": meta["text"],
        "date": date,
        "url": url,
        "content_type": label,
    }

    if sections:
        record["sections"] = sections
    if series:
        record["series"] = series

    return record


def fetch_document(url: str) -> Optional[dict]:
    """Fetch a single document page and extract content."""
    try:
        resp = _fetch_with_retry(url, timeout=45)
        if len(resp.text) < 500:
            logger.warning(f"Suspiciously short response ({len(resp.text)} bytes) from {url}")
            return None
        meta = extract_page_content(resp.text, url)
        if meta:
            return normalize(meta)
        else:
            logger.warning(f"No content extracted from {url} ({len(resp.text)} bytes)")
            return None
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents. If sample=True, fetch only ~20 records."""
    count = 0
    failures = 0
    sample_limit = 20

    # Pre-flight connectivity check
    try:
        resp = _fetch_with_retry(BASE_URL, timeout=20)
        logger.info(f"Connectivity OK: {BASE_URL} -> HTTP {resp.status_code}, {len(resp.text)} bytes")
    except Exception as e:
        logger.error(f"Cannot reach {BASE_URL}: {e}")
        logger.error("This may be due to VPS IP blocking. Try with a different IP or proxy.")
        return

    urls_by_type = get_sitemap_urls()
    if not any(urls_by_type.values()):
        logger.error("No URLs found in sitemaps — site may be blocking this IP")
        return

    # For sample mode, take a few from each type
    if sample:
        selected_urls = []
        for ctype in ["code-annotated", "case-law", "regulation", "court-rules"]:
            type_urls = urls_by_type.get(ctype, [])
            # Take first 5 from each type that has URLs
            selected_urls.extend([(u, ctype) for u in type_urls[:5]])
    else:
        selected_urls = []
        for ctype, urls in urls_by_type.items():
            selected_urls.extend([(u, ctype) for u in urls])

    logger.info(f"Fetching {len(selected_urls)} document pages (sample={sample})")

    for url, ctype in selected_urls:
        if sample and count >= sample_limit:
            return

        # Abort early if too many consecutive failures (likely blocked)
        if failures >= 10:
            logger.error(f"Too many consecutive failures ({failures}). Possible IP block. Aborting.")
            return

        time.sleep(2.0)
        doc = fetch_document(url)
        if doc:
            count += 1
            failures = 0  # reset consecutive failure counter
            yield doc
            logger.info(f"  [{count}] {doc['title'][:70]} ({len(doc['text'])} chars)")
        else:
            failures += 1

    logger.info(f"Total documents fetched: {count}")


def save_sample(records: list[dict]) -> None:
    """Save sample records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    # Clear old samples
    for old in SAMPLE_DIR.glob("record_*.json"):
        old.unlink()
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")


def test_api() -> bool:
    """Test connectivity to asbar.org with detailed diagnostics."""
    try:
        resp = _fetch_with_retry(BASE_URL, timeout=20)
        logger.info(f"Homepage: HTTP {resp.status_code}, {len(resp.text)} bytes")
        # Check for block indicators
        if len(resp.text) < 1000:
            logger.warning(f"Homepage response suspiciously short — may be blocked")
            logger.warning(f"Body preview: {resp.text[:300]}")
        server = resp.headers.get("server", "unknown")
        logger.info(f"Server: {server}")
    except Exception as e:
        logger.error(f"Cannot reach homepage: {e}")
        logger.error("Possible causes: DNS failure, IP block, SSL issue, network timeout")
        return False

    # Test sitemap
    try:
        time.sleep(1.5)
        resp = _fetch_with_retry(SITEMAP_INDEX_URL, timeout=20)
        root = ET.fromstring(resp.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        sitemaps = root.findall(".//sm:sitemap", ns)
        logger.info(f"Sitemap index: {len(sitemaps)} sitemaps")
    except Exception as e:
        logger.error(f"Sitemap fetch/parse failed: {e}")
        return False

    # Test a content page
    try:
        time.sleep(2.0)
        test_url = f"{BASE_URL}/code-annotated/1-0301-seal/"
        resp = _fetch_with_retry(test_url, timeout=30)
        logger.info(f"Test page: HTTP {resp.status_code}, {len(resp.text)} bytes")

        if resp.status_code == 200:
            meta = extract_page_content(resp.text, test_url)
            if meta and meta.get("text"):
                logger.info(f"Content extracted: '{meta.get('title', '')[:50]}' -- {len(meta['text'])} chars")
                return True
            else:
                logger.warning("Could not extract content from test page")
                logger.warning(f"Page size: {len(resp.text)} bytes, has fl-post-content: {'fl-post-content' in resp.text}")
                return False

        return False
    except Exception as e:
        logger.error(f"Test page fetch failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AS/ASBAR data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        is_sample = args.sample or not args.full
        records = []
        for doc in fetch_all(sample=is_sample):
            records.append(doc)

        if records:
            save_sample(records)
            logger.info(f"Bootstrap complete: {len(records)} records")

            # Validate
            texts = [r for r in records if r.get("text") and len(r["text"]) > 50]
            logger.info(f"Records with full text: {len(texts)}/{len(records)}")
            if texts:
                avg_len = sum(len(r["text"]) for r in texts) // len(texts)
                logger.info(f"Average text length: {avg_len} chars")
        else:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
