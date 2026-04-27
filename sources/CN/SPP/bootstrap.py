#!/usr/bin/env python3
"""
CN/SPP -- China Supreme People's Procuratorate (最高人民检察院)

Fetches procuratorial guiding cases (检察指导案例), typical cases (典型案例),
and normative documents (规范文件) from spp.gov.cn.

Strategy:
  - Paginate listing pages for each category:
      /spp/jczdal/index.shtml  (guiding cases, ~3 pages, ~61 items)
      /spp/zgjdxal/index.shtml (typical cases, ~12 pages, ~332 items)
      /spp/gfwj/index.shtml    (normative docs, ~6 pages, ~175 items)
  - Parse <li><a href="...">title</a>date</li> entries
  - Fetch each detail page and extract full text from content divs

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
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/SPP"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.SPP")

BASE_URL = "https://www.spp.gov.cn"

# (path_prefix, max_pages, data_type, label)
CATEGORIES = [
    ("/spp/jczdal", 5, "case_law", "guiding_case"),
    ("/spp/zgjdxal", 15, "case_law", "typical_case"),
    ("/spp/gfwj", 8, "doctrine", "normative_document"),
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
        elif tag in ("br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4"):
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
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """Fetch a page with retries and rate limiting."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.RequestException as e:
            logger.warning("Fetch failed (attempt %d/%d) %s: %s",
                           attempt + 1, retries, url, e)
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    return None


def parse_listing_page(html: str) -> list[dict]:
    """Extract document links from a listing page.

    Actual pattern:
      <li><a href="URL" target="_blank">title</a><span>YYYY-MM-DD</span></li>
    Inside <ul class="li_line"> container.
    """
    entries = []
    # Restrict to content area (inside ul.li_line)
    list_start = html.find('class="li_line"')
    if list_start < 0:
        list_start = html.find('class="commonList_con"')
    if list_start < 0:
        return entries
    content_html = html[list_start:]

    pattern = re.compile(
        r'<li>\s*<a\s+[^>]*href="([^"]+)"[^>]*>\s*(.*?)\s*</a>\s*'
        r'<span>\s*(\d{4}-\d{2}-\d{2})\s*</span>\s*</li>',
        re.DOTALL
    )
    for match in pattern.finditer(content_html):
        raw_url = match.group(1).strip()
        title = strip_html(match.group(2)).strip()
        date_str = match.group(3)

        # Strip fragment anchors
        raw_url = re.sub(r'#\d+$', '', raw_url)

        # Normalize URL
        if raw_url.startswith("http"):
            url = raw_url
        else:
            url = urljoin(BASE_URL, raw_url)

        # Extract doc ID from URL
        id_match = re.search(r't(\d{8}_\d+)', url)
        doc_id = id_match.group(1) if id_match else url.split("/")[-1].replace(".shtml", "")

        if title and url:
            entries.append({
                "url": url,
                "doc_id": doc_id,
                "title": title,
                "date": date_str,
            })
    return entries


def extract_detail(html: str) -> dict:
    """Extract full text and metadata from a detail page.

    Tries multiple content container patterns used across SPP page types.
    """
    text = ""

    # Try content containers in priority order
    containers = [
        ('class="wsfbh_detail_con"', ['class="wsfbh_detail_bot"', 'class="share"']),
        ('id="fontzoom"', ['class="txt_etr"', 'class="word_size"', 'class="share"']),
        ('class="article_body"', ['class="share"', 'class="article_bot"']),
        ('class="txt_txt"', ['class="txt_etr"', 'class="word_size"', 'class="share"']),
    ]

    for start_marker, end_markers in containers:
        start_idx = html.find(start_marker)
        if start_idx < 0:
            continue

        content_start = html.find('>', start_idx) + 1
        if content_start <= 0:
            continue

        # Find the nearest end marker
        end_idx = len(html)
        for marker in end_markers:
            m_idx = html.find(marker, content_start)
            if 0 < m_idx < end_idx:
                div_idx = html.rfind('<div', content_start, m_idx)
                if div_idx >= 0:
                    end_idx = div_idx

        raw_html = html[content_start:end_idx]
        raw_html = re.sub(r'(\s*</div>\s*)+$', '', raw_html)
        candidate = strip_html(raw_html)
        if len(candidate) > len(text):
            text = candidate

    # Extract publication date
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})\s*\d{2}:\d{2}', html)
    date_str = date_match.group(1) if date_match else None

    # Extract source org
    source_match = re.search(r'来源[：:]\s*([^<\n]+)', html)
    source_org = source_match.group(1).strip() if source_match else None

    return {
        "text": text,
        "detail_date": date_str,
        "source_org": source_org,
    }


def normalize(raw: dict) -> dict:
    """Transform raw record into standard schema."""
    data_type = raw.get("data_type", "case_law")
    date = raw.get("detail_date") or raw.get("date")

    return {
        "_id": f"CN/SPP/{raw['doc_id']}",
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": date,
        "url": raw.get("url", ""),
        "category": raw.get("category", ""),
        "source_org": raw.get("source_org"),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents from SPP across all categories."""
    total = 0
    for path_prefix, max_pages, data_type, label in CATEGORIES:
        logger.info("=== Category: %s (%s) ===", label, path_prefix)
        category_count = 0

        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = f"{BASE_URL}{path_prefix}/index.shtml"
            else:
                url = f"{BASE_URL}{path_prefix}/index_{page_num}.shtml"

            logger.info("Fetching listing page %d: %s", page_num, url)
            html = fetch_page(url)
            if not html:
                logger.warning("Failed to fetch page %d, stopping pagination for %s",
                               page_num, label)
                break

            entries = parse_listing_page(html)
            if not entries:
                logger.info("No entries on page %d, stopping pagination for %s",
                            page_num, label)
                break

            logger.info("Found %d entries on page %d", len(entries), page_num)

            for entry in entries:
                logger.info("Fetching detail: %s", entry["title"][:60])
                detail_html = fetch_page(entry["url"])
                time.sleep(1.5)  # Rate limiting

                if not detail_html:
                    logger.warning("Failed to fetch detail page: %s", entry["url"])
                    continue

                detail = extract_detail(detail_html)
                if not detail["text"] or len(detail["text"]) < 50:
                    logger.warning("Insufficient text (%d chars) for: %s",
                                   len(detail.get("text", "")), entry["title"][:60])
                    continue

                raw = {
                    **entry,
                    **detail,
                    "data_type": data_type,
                    "category": label,
                }
                record = normalize(raw)
                yield record
                category_count += 1
                total += 1

                if sample and total >= 15:
                    logger.info("Sample limit reached (%d records)", total)
                    return

            time.sleep(1)  # Rate limiting between pages

        logger.info("Category %s: %d records", label, category_count)

    logger.info("Total records fetched: %d", total)


def save_sample(records: list[dict], output_dir: Path):
    """Save sample records to JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for rec in records:
        safe_id = rec["_id"].replace("/", "_")
        filepath = output_dir / f"{safe_id}.json"
        filepath.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved: %s", filepath.name)


def test_api():
    """Quick connectivity and structure test."""
    print(f"Testing CN/SPP connectivity...")
    for path_prefix, _, _, label in CATEGORIES:
        url = f"{BASE_URL}{path_prefix}/index.shtml"
        html = fetch_page(url)
        if not html:
            print(f"  FAIL: Cannot reach {url}")
            continue
        entries = parse_listing_page(html)
        print(f"  {label}: {len(entries)} entries on first page")
        if entries:
            detail_html = fetch_page(entries[0]["url"])
            if detail_html:
                detail = extract_detail(detail_html)
                text_len = len(detail.get("text", ""))
                print(f"    First entry: {entries[0]['title'][:50]}...")
                print(f"    Text length: {text_len} chars")
                if text_len > 100:
                    print(f"    Text preview: {detail['text'][:200]}...")
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="CN/SPP data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only a small sample (15 records)")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
        return

    if args.command == "bootstrap":
        sample = args.sample or not args.full
        records = list(fetch_all(sample=sample))

        if not records:
            logger.error("No records fetched!")
            sys.exit(1)

        save_sample(records, SAMPLE_DIR)

        # Summary
        text_lengths = [len(r.get("text", "")) for r in records]
        print(f"\n{'='*60}")
        print(f"CN/SPP Bootstrap Complete")
        print(f"Records: {len(records)}")
        print(f"With text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
        print(f"Avg text length: {sum(text_lengths)//max(len(text_lengths),1)} chars")
        print(f"Min text length: {min(text_lengths) if text_lengths else 0} chars")
        print(f"Categories: {set(r.get('category','') for r in records)}")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
