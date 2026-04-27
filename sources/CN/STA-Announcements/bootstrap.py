#!/usr/bin/env python3
"""
CN/STA-Announcements -- State Taxation Administration English Tax Law Database

Fetches English translations of Chinese tax laws and normative documents
from fgk.chinatax.gov.cn/eng/.

Strategy:
  - Use requests.Session to handle anti-bot cookie challenge (C3VK cookie)
  - Scrape paginated listing pages for two categories (Laws, Normative Documents)
  - Extract document links from each listing page
  - Fetch individual document pages and extract full text
  - Metadata from <meta> tags, text from <p> tags after <div class="zscont">

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from html import unescape

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/STA-Announcements"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.STA-Announcements")

BASE_URL = "https://fgk.chinatax.gov.cn"

CATEGORIES = {
    "laws": "/eng/c102962/c102967/c102997/LAWS",
    "normative": "/eng/c102962/c102967/c102966/LAWS",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DELAY = 2.0

# Global session for cookie persistence
_session = None


def _get_session():
    """Get or create a requests session with proper headers."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def _handle_cookie_challenge(resp, url):
    """Parse and set anti-bot cookie from JS challenge response."""
    if len(resp.text) < 1000:
        cookie_m = re.search(r'cookie="([^"]+)"', resp.text)
        if cookie_m:
            cookie_str = cookie_m.group(1)
            parts = cookie_str.split(";")
            nv = parts[0].split("=", 1)
            if len(nv) == 2:
                _get_session().cookies.set(nv[0].strip(), nv[1].strip(),
                                           domain="fgk.chinatax.gov.cn", path="/")
                logger.debug(f"Set cookie: {nv[0]}={nv[1]}")
                return True
    return False


def _request_with_retry(url, retries=3, backoff=5):
    """GET request with cookie challenge handling and retries."""
    session = _get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()

            # Force UTF-8 encoding (server incorrectly reports ISO-8859-1)
            resp.encoding = 'utf-8'

            # Handle cookie challenge (small JS page that sets a cookie)
            if _handle_cookie_challenge(resp, url):
                time.sleep(1)
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                resp.encoding = 'utf-8'

            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries} for {url}: {e} (wait {wait}s)")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.HTTPError:
            if resp.status_code in (429, 500, 502, 503):
                if attempt < retries - 1:
                    wait = backoff * (attempt + 1)
                    logger.warning(f"HTTP {resp.status_code}, retry {attempt+1}/{retries} (wait {wait}s)")
                    time.sleep(wait)
                else:
                    raise
            else:
                raise


def _get_pagination_info(html_text):
    """Extract total pages and total docs from createPageHTML call."""
    m = re.search(r"createPageHTML\([^,]+,(\d+),\s*\d+,'[^']+','[^']+',(\d+)\)", html_text)
    if m:
        return int(m.group(1)), int(m.group(2))
    return 1, 0


def _extract_doc_links(html_text):
    """Extract document content.html links from a listing page."""
    links = re.findall(
        r'<a[^>]+href="(/eng/[^"]+content\.html)"[^>]*>([^<]+)</a>',
        html_text,
    )
    results = []
    seen = set()
    for href, title in links:
        title = unescape(title).strip()
        if title and len(title) > 10 and href not in seen:
            seen.add(href)
            results.append((href, title))
    return results


def _extract_doc_id(url_path):
    """Extract a unique doc ID from the URL path (last numeric segment before content.html)."""
    parts = url_path.rstrip("/").split("/")
    for part in reversed(parts):
        if part.startswith("c") and part[1:].isdigit():
            return part
    return hashlib.md5(url_path.encode()).hexdigest()[:12]


def _clean_text(html_content):
    """Strip HTML tags and clean up text."""
    text = re.sub(r'<br\s*/?>', '\n', html_content)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = text.replace('\xa0', ' ')
    text = re.sub(r'&ensp;', ' ', text)
    text = re.sub(r'&[a-z]+;', ' ', text)
    lines = []
    for line in text.split('\n'):
        line = line.strip()
        if line:
            lines.append(line)
    return '\n'.join(lines)


SKIP_PREFIXES = ('分享', '享到', 'Share', 'Home>', '首页', '当前位置')


def _extract_content(html_text):
    """Extract title, date, and full text from a document page."""
    title_m = re.search(r'<meta name="ArticleTitle" content="([^"]+)"', html_text)
    date_m = re.search(r'<meta name="PubDate" content="([^"]+)"', html_text)

    title = unescape(title_m.group(1)).strip() if title_m else ""
    pub_date = date_m.group(1).strip() if date_m else ""

    # Content is in <p> tags after the <div class="zscont"> div
    # The zscont div itself is empty; the real content follows after it
    zscont_idx = html_text.find('class="zscont"')
    if zscont_idx > 0:
        content_region = html_text[zscont_idx:]
    else:
        # Fallback: use entire page
        content_region = html_text

    # Extract all <p> tags from the content region
    paras = re.findall(r'<p[^>]*>(.*?)</p>', content_region, re.DOTALL)
    text_parts = []
    for p in paras:
        clean = _clean_text(p)
        if not clean or len(clean) < 4:
            continue
        if any(clean.startswith(prefix) for prefix in SKIP_PREFIXES):
            continue
        text_parts.append(clean)

    # Stop at footer markers
    final_parts = []
    for part in text_parts:
        if '【打印' in part or '【关闭' in part or 'print' in part.lower():
            break
        final_parts.append(part)

    full_text = '\n'.join(final_parts)

    # Parse date to ISO format
    iso_date = None
    if pub_date:
        try:
            dt = datetime.strptime(pub_date.split()[0], "%Y-%m-%d")
            iso_date = dt.strftime("%Y-%m-%d")
        except (ValueError, IndexError):
            iso_date = pub_date

    return title, iso_date, full_text


def fetch_listing_pages(category_key, category_path, sample=False):
    """Fetch all document links from paginated listing pages."""
    first_url = f"{BASE_URL}{category_path}.html"
    logger.info(f"Fetching listing: {first_url}")
    resp = _request_with_retry(first_url)
    time.sleep(DELAY)

    total_pages, total_docs = _get_pagination_info(resp.text)
    logger.info(f"Category '{category_key}': {total_docs} documents across {total_pages} pages")

    all_links = _extract_doc_links(resp.text)

    max_pages = min(total_pages, 2) if sample else total_pages

    for page in range(2, max_pages + 1):
        page_url = f"{BASE_URL}{category_path}_{page}.html"
        logger.info(f"Fetching page {page}/{total_pages}: {page_url}")
        resp = _request_with_retry(page_url)
        all_links.extend(_extract_doc_links(resp.text))
        time.sleep(DELAY)

    return all_links


def normalize(raw, category):
    """Transform raw document data into standard schema."""
    return {
        "_id": raw["doc_id"],
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw["title"],
        "text": raw["text"],
        "date": raw["date"],
        "url": raw["url"],
        "category": category,
        "language": "en",
    }


def fetch_all(sample=False):
    """Yield all documents with full text."""
    for cat_key, cat_path in CATEGORIES.items():
        doc_links = fetch_listing_pages(cat_key, cat_path, sample=sample)

        if sample:
            doc_links = doc_links[:10]

        logger.info(f"Fetching {len(doc_links)} documents from '{cat_key}'")

        for i, (href, listing_title) in enumerate(doc_links):
            doc_url = f"{BASE_URL}{href}"
            doc_id = _extract_doc_id(href)

            logger.info(f"[{i+1}/{len(doc_links)}] Fetching: {listing_title[:60]}...")

            try:
                resp = _request_with_retry(doc_url)
                title, date, text = _extract_content(resp.text)
                time.sleep(DELAY)
            except Exception as e:
                logger.error(f"Failed to fetch {doc_url}: {e}")
                continue

            if not title:
                title = listing_title

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
                continue

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": doc_url,
            }

            yield normalize(raw, cat_key)


def bootstrap(sample=False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(
            f"Saved {record['_id']}: {record['title'][:50]}... "
            f"({len(record['text'])} chars)"
        )

    logger.info(f"Bootstrap complete: {count} records saved to {SAMPLE_DIR}")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CN/STA-Announcements bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~14 docs)")
    parser.add_argument("--full", action="store_true", help="Fetch all documents")
    args = parser.parse_args()

    if args.command == "test-api":
        logger.info("Testing connection to fgk.chinatax.gov.cn...")
        try:
            resp = _request_with_retry(f"{BASE_URL}/eng/home.html")
            logger.info(f"Connection OK: HTTP {resp.status_code}, {len(resp.text)} bytes")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample = args.sample and not args.full
        count = bootstrap(sample=sample)
        if count < 10 and sample:
            logger.error(f"Only {count} records — expected at least 10")
            sys.exit(1)
        logger.info(f"Success: {count} records")
