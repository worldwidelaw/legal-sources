#!/usr/bin/env python3
"""
CN/SupremeCourt -- China Supreme People's Court Guiding Cases & Judicial Interpretations

Fetches guiding cases (指导性案例) and judicial interpretations (司法解释) from court.gov.cn.

Strategy:
  - Paginate listing pages at /fabu/gengduo/151.html (guiding cases, 11 pages)
    and /fabu/gengduo/16.html (judicial interpretations, 20 pages)
  - For each entry, fetch the detail page at /fabu/xiangqing/{ID}.html
  - Extract full text from the .txt_txt div

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test-api
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/SupremeCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.SupremeCourt")

BASE_URL = "https://www.court.gov.cn"

# Listing pages: (category_id, max_pages, data_type_label)
CATEGORIES = [
    ("151", 15, "guiding_case"),       # 指导性案例 (guiding cases)
    ("16", 25, "judicial_interpretation"),  # 司法解释 (judicial interpretations)
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


class HTMLTextExtractor(HTMLParser):
    """Simple HTML tag stripper."""
    def __init__(self):
        super().__init__()
        self.result = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True
        elif tag in ("br", "p", "div", "li", "tr"):
            self.result.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        elif tag == "p":
            self.result.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.result.append(data)

    def get_text(self):
        return "".join(self.result)


def strip_html(html: str) -> str:
    """Strip HTML tags and return clean text."""
    extractor = HTMLTextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """Fetch a page with retries."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.RequestException as e:
            logger.warning("Fetch failed (attempt %d/%d) %s: %s", attempt + 1, retries, url, e)
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    return None


def parse_listing_page(html: str) -> list[dict]:
    """Extract case links from a listing page."""
    entries = []
    # Pattern: <a title="..." href="/fabu/xiangqing/XXXXXX.html">...</a>
    # followed by <i class="date">YYYY-MM-DD</i>
    link_pattern = re.compile(
        r'<a\s+[^>]*title="([^"]*)"[^>]*href="(/fabu/xiangqing/(\d+)\.html)"[^>]*>',
        re.DOTALL
    )
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')

    for match in link_pattern.finditer(html):
        title, path, doc_id = match.group(1), match.group(2), match.group(3)
        # Look for a date near this link
        after_text = html[match.end():match.end() + 200]
        date_match = date_pattern.search(after_text)
        date_str = date_match.group(1) if date_match else None
        entries.append({
            "url": f"{BASE_URL}{path}",
            "doc_id": doc_id,
            "title": title.strip(),
            "date": date_str,
        })
    return entries


def extract_detail(html: str) -> dict:
    """Extract full text and metadata from a detail page."""
    text = ""

    # Find the txt_txt div content
    start_marker = 'class="txt_txt"'
    start_idx = html.find(start_marker)
    if start_idx >= 0:
        content_start = html.find('>', start_idx) + 1
        # Content ends before txt_etr or word_size div
        end_idx = len(html)
        for marker in ['class="txt_etr"', 'class="word_size"', 'class="share"']:
            m_idx = html.find(marker, content_start)
            if m_idx >= 0:
                # Go back to the opening <div of this sibling
                div_idx = html.rfind('<div', content_start, m_idx)
                if div_idx >= 0 and div_idx < end_idx:
                    end_idx = div_idx
        # Also handle closing </div> before txt_etr
        raw_html = html[content_start:end_idx]
        # Remove trailing </div> tags
        raw_html = re.sub(r'(\s*</div>\s*)+$', '', raw_html)
        text = strip_html(raw_html)

    # Extract publication date
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})\s*\d{2}:\d{2}', html)
    date_str = date_match.group(1) if date_match else None

    # Extract source/court
    source_match = re.search(r'来源[：:]\s*([^<\n]+)', html)
    source = source_match.group(1).strip() if source_match else None

    return {
        "text": text,
        "detail_date": date_str,
        "source_org": source,
    }


def fetch_all_listings(category_id: str, max_pages: int, sample: bool = False) -> list[dict]:
    """Fetch all entries from a category's listing pages."""
    all_entries = []
    for page_num in range(1, max_pages + 1):
        if page_num == 1:
            url = f"{BASE_URL}/fabu/gengduo/{category_id}.html"
        else:
            url = f"{BASE_URL}/fabu/gengduo/{category_id}_{page_num}.html"

        logger.info("Fetching listing page %d: %s", page_num, url)
        html = fetch_page(url)
        if not html:
            logger.warning("Failed to fetch page %d, stopping pagination", page_num)
            break

        entries = parse_listing_page(html)
        if not entries:
            logger.info("No entries on page %d, stopping pagination", page_num)
            break

        all_entries.extend(entries)
        logger.info("Page %d: found %d entries (total: %d)", page_num, len(entries), len(all_entries))

        if sample and len(all_entries) >= 15:
            return all_entries[:15]

        time.sleep(1.5)

    return all_entries


def normalize(raw: dict) -> dict:
    """Normalize a raw document into the standard schema."""
    doc_id = raw.get("doc_id", "")
    title = raw.get("title", "")
    text = raw.get("text", "")
    date = raw.get("detail_date") or raw.get("date")
    category = raw.get("category", "guiding_case")

    # Extract case number from title if present
    case_number = None
    cn_match = re.search(r'指导性案例(\d+)号', title)
    if cn_match:
        case_number = f"指导性案例{cn_match.group(1)}号"

    # Extract keywords from text
    keywords = None
    kw_match = re.search(r'关键词[：:\s]*([^\n]+)', text)
    if kw_match:
        keywords = kw_match.group(1).strip()

    data_type = "case_law" if category == "guiding_case" else "legislation"

    return {
        "_id": f"CN/SupremeCourt/{doc_id}",
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": raw.get("url", ""),
        "case_number": case_number,
        "keywords": keywords,
        "category": category,
        "source_org": raw.get("source_org"),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all normalized documents."""
    for category_id, max_pages, category_label in CATEGORIES:
        logger.info("=== Fetching category: %s (id=%s) ===", category_label, category_id)
        entries = fetch_all_listings(category_id, max_pages, sample=sample)
        logger.info("Found %d entries for %s", len(entries), category_label)

        for i, entry in enumerate(entries):
            logger.info("[%d/%d] Fetching detail: %s", i + 1, len(entries), entry["title"][:60])
            html = fetch_page(entry["url"])
            if not html:
                logger.warning("Failed to fetch detail for %s", entry["doc_id"])
                continue

            detail = extract_detail(html)
            entry.update(detail)
            entry["category"] = category_label

            if not entry.get("text"):
                logger.warning("No text extracted for %s", entry["doc_id"])
                continue

            yield normalize(entry)
            time.sleep(1.5)

        if sample:
            break  # Only first category in sample mode


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    max_docs = 15 if sample else 999999

    for doc in fetch_all(sample=sample):
        if count >= max_docs:
            break

        fname = re.sub(r'[^\w\-.]', '_', doc["_id"]) + ".json"
        out_path = SAMPLE_DIR / fname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        text_len = len(doc.get("text", ""))
        logger.info("Saved %s (%d chars text)", fname, text_len)
        count += 1

    logger.info("Bootstrap complete: %d documents saved to %s", count, SAMPLE_DIR)
    return count


def test_api():
    """Quick connectivity test."""
    logger.info("Testing court.gov.cn connectivity...")

    # Test listing page
    url = f"{BASE_URL}/fabu/gengduo/151.html"
    html = fetch_page(url)
    if not html:
        logger.error("FAIL: Cannot fetch listing page")
        return False

    entries = parse_listing_page(html)
    if not entries:
        logger.error("FAIL: No entries parsed from listing page")
        return False
    logger.info("OK: Found %d entries on first listing page", len(entries))

    # Test detail page
    entry = entries[0]
    logger.info("Testing detail page: %s", entry["url"])
    detail_html = fetch_page(entry["url"])
    if not detail_html:
        logger.error("FAIL: Cannot fetch detail page")
        return False

    detail = extract_detail(detail_html)
    if not detail.get("text"):
        logger.error("FAIL: No text extracted from detail page")
        return False

    logger.info("OK: Extracted %d chars of text from %s", len(detail["text"]), entry["title"][:50])
    logger.info("Text preview: %s...", detail["text"][:200])
    return True


def main():
    parser = argparse.ArgumentParser(description="CN/SupremeCourt data fetcher")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch and save documents")
    boot.add_argument("--sample", action="store_true", help="Fetch only ~15 sample documents")
    boot.add_argument("--full", action="store_true", help="Fetch all documents")

    sub.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            sys.exit(1)
    elif args.command == "test-api":
        if not test_api():
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
