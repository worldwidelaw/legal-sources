#!/usr/bin/env python3
"""
CN/MOC -- China Ministry of Commerce Trade Regulations (商务部)

Fetches trade-related laws and regulations from MOFCOM's IP Protection
law database at ipr.mofcom.gov.cn.

Source:
  - List: https://ipr.mofcom.gov.cn/law/list.shtml?pg={page}
  - Detail: https://ipr.mofcom.gov.cn/law/detail.shtml?id={id}
  - 633+ laws, 64 pages of 10 items each
  - Full text embedded in detail pages as static HTML
  - No auth required

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full initial pull
  python bootstrap.py test-api              # Quick connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/MOC"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.MOC")

BASE_URL = "https://ipr.mofcom.gov.cn"
LIST_URL = f"{BASE_URL}/law/list.shtml"
DETAIL_URL = f"{BASE_URL}/law/detail.shtml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def _get(url: str, params: dict = None, retries: int = 3) -> Optional[requests.Response]:
    """GET with retry logic and rate limiting. Forces UTF-8 encoding."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            resp.encoding = "utf-8"
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning("Request failed (attempt %d/%d): %s — %s", attempt + 1, retries, url, e)
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None


def _strip_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    text = re.sub(r'<br\s*/?>', '\n', html_text)
    text = re.sub(r'<p[^>]*>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'&[a-zA-Z]+;', ' ', text)
    text = re.sub(r'\xa0', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def scrape_list_page(page: int) -> list[dict]:
    """Scrape a list page to get law IDs, titles, and dates."""
    resp = _get(LIST_URL, params={"pg": str(page)})
    if not resp:
        logger.error("Failed to fetch list page %d", page)
        return []

    html = resp.text
    entries = []

    # Pattern: <a href="/law/detail.shtml?id=XXXX" ...>TITLE</a>
    # Also look for date info near each entry
    item_pattern = re.compile(
        r'<a[^>]*href=["\'](?:/law/detail\.shtml\?id=(\d+))["\'][^>]*>(.*?)</a>',
        re.DOTALL
    )

    for match in item_pattern.finditer(html):
        law_id = match.group(1)
        title = _strip_html(match.group(2)).strip()
        if title and law_id:
            entries.append({
                "id": law_id,
                "title": title,
            })

    logger.info("List page %d: found %d entries", page, len(entries))
    return entries


def scrape_detail_page(law_id: str) -> Optional[dict]:
    """Scrape a detail page for full text and metadata.

    Page structure:
      <div class="artcle h1"><h1>TITLE</h1></div>
      <div class="artcle p"><span><p>...CONTENT...</p></span></div>
    """
    resp = _get(DETAIL_URL, params={"id": law_id})
    if not resp:
        logger.error("Failed to fetch detail page for id=%s", law_id)
        return None

    html = resp.text

    # Extract title from <div class="artcle h1"><h1>TITLE</h1></div>
    title = ""
    title_match = re.search(r'<div\s+class="artcle\s+h1"[^>]*>\s*<h1>(.*?)</h1>', html, re.DOTALL)
    if title_match:
        title = _strip_html(title_match.group(1))
    if not title:
        # Fallback: any <h1> inside the content area
        h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if h1_match:
            raw = _strip_html(h1_match.group(1))
            if raw and raw != "知识产权法律法规数据库":
                title = raw

    # Extract content from <div class="artcle p"><span>...CONTENT...</span></div>
    full_text = ""
    content_match = re.search(
        r'<div\s+class="artcle\s+p"[^>]*>\s*<span>(.*?)</span>\s*</div>',
        html, re.DOTALL
    )
    if content_match:
        full_text = _strip_html(content_match.group(1))

    # Fallback: look for any large text block after the title
    if len(full_text) < 100:
        # Try broader artcle p div
        content_match2 = re.search(
            r'<div\s+class="artcle\s+p"[^>]*>(.*?)</div>',
            html, re.DOTALL
        )
        if content_match2:
            candidate = _strip_html(content_match2.group(1))
            if len(candidate) > len(full_text):
                full_text = candidate

    # Extract date from the content text (usually in parenthetical near the top)
    # Pattern: （YYYY年M月D日...公布） or similar
    date_str = None
    # Search in the first 500 chars of content for the promulgation date
    search_text = full_text[:500] if full_text else html
    date_patterns = [
        r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日',
    ]
    for dp in date_patterns:
        dm = re.search(dp, search_text)
        if dm:
            try:
                y, m, d = int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
                if 1949 <= y <= 2030 and 1 <= m <= 12 and 1 <= d <= 31:
                    date_str = f"{y:04d}-{m:02d}-{d:02d}"
                    break
            except (ValueError, IndexError):
                pass

    return {
        "id": law_id,
        "title": title,
        "text": full_text,
        "date": date_str,
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all laws from the MOFCOM IPR law database."""
    max_pages = 64
    if sample:
        max_pages = 2  # ~20 entries for sample

    all_entries = []
    for page in range(1, max_pages + 1):
        entries = scrape_list_page(page)
        if not entries:
            logger.warning("No entries on page %d, stopping pagination", page)
            break
        all_entries.extend(entries)
        time.sleep(2)

    logger.info("Found %d law entries across %d pages", len(all_entries), min(max_pages, page))

    if sample:
        all_entries = all_entries[:15]

    for i, entry in enumerate(all_entries):
        logger.info("Fetching detail %d/%d: id=%s %s", i + 1, len(all_entries), entry["id"], entry["title"][:40])
        detail = scrape_detail_page(entry["id"])
        if detail and detail["text"]:
            record = normalize(detail)
            yield record
        else:
            logger.warning("No text for id=%s: %s", entry["id"], entry["title"])
        time.sleep(2)


def normalize(raw: dict) -> dict:
    """Normalize a raw record to standard schema."""
    law_id = raw["id"]
    return {
        "_id": f"CN-MOC-{law_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": f"{DETAIL_URL}?id={law_id}",
        "language": "zh",
    }


def test_api():
    """Quick connectivity test."""
    print("Testing list page...")
    entries = scrape_list_page(1)
    if not entries:
        print("FAIL: Could not fetch list page")
        return False
    print(f"  OK: {len(entries)} entries on page 1")

    first = entries[0]
    print(f"Testing detail page (id={first['id']})...")
    detail = scrape_detail_page(first["id"])
    if not detail:
        print("FAIL: Could not fetch detail page")
        return False
    text_len = len(detail.get("text", ""))
    print(f"  OK: title={detail['title'][:50]}")
    print(f"  OK: text length={text_len} chars")
    print(f"  OK: date={detail.get('date')}")

    if text_len < 100:
        print("WARNING: Text is very short — may need to adjust content extraction")
        return False

    print("ALL TESTS PASSED")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        if not record.get("text"):
            continue
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        text_preview = record["text"][:80].replace("\n", " ")
        logger.info("Saved %s — %d chars — %s", record["_id"], len(record["text"]), text_preview)

    logger.info("Bootstrap complete: %d records saved to %s", count, SAMPLE_DIR)
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CN/MOC MOFCOM Trade Regulations bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch and save records")
    boot.add_argument("--sample", action="store_true", help="Fetch sample (~15 records)")

    sub.add_parser("test-api", help="Quick connectivity test")

    args = parser.parse_args()

    if args.command == "test-api":
        ok = test_api()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        n = bootstrap(sample=args.sample)
        sys.exit(0 if n > 0 else 1)
    else:
        parser.print_help()
        sys.exit(1)
