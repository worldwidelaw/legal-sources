#!/usr/bin/env python3
"""
CN/SAFE -- State Administration of Foreign Exchange Regulations

Fetches English translations of Chinese foreign exchange regulations
from www.safe.gov.cn/en/.

Strategy:
  - Scrape paginated listing at /en/RulesandRegulations/index.html
  - Fetch individual document pages for full text
  - Extract text from <p> tags in <div class="detail"> area
  - Skip PDF-only documents that have no inline text

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
from html import unescape
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/SAFE"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.SAFE")

BASE_URL = "https://www.safe.gov.cn"
LISTING_PATH = "/en/RulesandRegulations/index"
TOTAL_PAGES = 7
DELAY = 2.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_session = None


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def _get_with_retry(url, retries=3, backoff=5):
    """GET with retries."""
    session = _get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = "utf-8"
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


def _clean_html(text):
    """Strip HTML tags and clean text."""
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"&[a-z]+;", " ", text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return "\n".join(lines)


def _extract_doc_links(html_text):
    """Extract document links from a listing page."""
    links = re.findall(
        r'<a[^>]+href="(/en/\d{4}/\d{4}/[^"]+\.html)"[^>]*>([^<]+)</a>',
        html_text,
    )
    results = []
    seen = set()
    for href, title in links:
        title = unescape(title).strip()
        if title and len(title) > 5 and href not in seen:
            seen.add(href)
            results.append((href, title))
    return results


def _extract_date_from_listing(html_text, href):
    """Extract the publication date associated with a document link."""
    # Dates appear near the link, typically in YYYY-MM-DD format
    # Look for the date near the href in the HTML
    idx = html_text.find(href)
    if idx > 0:
        region = html_text[max(0, idx - 200):idx + 500]
        dates = re.findall(r"(\d{4}-\d{2}-\d{2})", region)
        if dates:
            return dates[-1]
    # Fallback: extract from URL /en/YYYY/MMDD/
    m = re.match(r"/en/(\d{4})/(\d{2})(\d{2})/", href)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _extract_doc_id(href):
    """Extract doc ID from URL like /en/2025/0731/2369.html."""
    m = re.search(r"/(\d+)\.html$", href)
    if m:
        return m.group(1)
    return hashlib.md5(href.encode()).hexdigest()[:12]


SKIP_TEXT = ("Contact Us", "For Home", "Join Collection",
             "State Administration of Foreign Exchange",
             "All rights reserved", "Best viewed",
             "FILE:", "Attachment:")


def _extract_content(html_text):
    """Extract full text from a document page."""
    title_m = re.search(r'<meta name="ArticleTitle" content="([^"]+)"', html_text)
    title = unescape(title_m.group(1)).strip() if title_m else ""

    # Find detail div
    idx = html_text.find('class="detail"')
    if idx < 0:
        return title, ""

    region = html_text[idx:]

    # Stop at footer
    footer_idx = region.find('class="footer"')
    if footer_idx > 0:
        region = region[:footer_idx]

    # Extract paragraphs
    paras = re.findall(r"<p[^>]*>(.*?)</p>", region, re.DOTALL)
    parts = []
    for p in paras:
        clean = _clean_html(p)
        if not clean or len(clean) < 5:
            continue
        if any(clean.startswith(s) for s in SKIP_TEXT):
            continue
        # Skip JS/CSS fragments
        if "$(function" in clean or "var " in clean or "{" in clean[:20]:
            continue
        parts.append(clean)

    return title, "\n".join(parts)


def normalize(raw):
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
        "language": "en",
    }


def fetch_all(sample=False):
    """Yield all documents with full text."""
    all_links = []

    max_pages = 2 if sample else TOTAL_PAGES

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"{BASE_URL}{LISTING_PATH}.html"
        else:
            url = f"{BASE_URL}{LISTING_PATH}_{page}.html"

        logger.info(f"Fetching listing page {page}/{max_pages}: {url}")
        resp = _get_with_retry(url)
        page_links = _extract_doc_links(resp.text)

        # Extract dates from listing page
        for href, title in page_links:
            date = _extract_date_from_listing(resp.text, href)
            all_links.append((href, title, date))

        time.sleep(DELAY)

    logger.info(f"Total document links: {len(all_links)}")

    if sample:
        all_links = all_links[:15]

    for i, (href, listing_title, date) in enumerate(all_links):
        doc_url = f"{BASE_URL}{href}"
        doc_id = _extract_doc_id(href)

        logger.info(f"[{i+1}/{len(all_links)}] Fetching: {listing_title[:60]}...")

        try:
            resp = _get_with_retry(doc_url)
            title, text = _extract_content(resp.text)
            time.sleep(DELAY)
        except Exception as e:
            logger.error(f"Failed to fetch {doc_url}: {e}")
            continue

        if not title:
            title = listing_title

        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars — may be PDF-only")
            continue

        raw = {
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "url": doc_url,
        }

        yield normalize(raw)


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
    parser = argparse.ArgumentParser(description="CN/SAFE bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 docs)")
    parser.add_argument("--full", action="store_true", help="Fetch all documents")
    args = parser.parse_args()

    if args.command == "test-api":
        logger.info("Testing connection to safe.gov.cn...")
        try:
            resp = _get_with_retry(f"{BASE_URL}/en/RulesandRegulations/index.html")
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
