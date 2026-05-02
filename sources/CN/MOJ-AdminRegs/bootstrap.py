#!/usr/bin/env python3
"""
CN/MOJ-AdminRegs -- China Ministry of Justice Administrative Regulations Database (国家行政法规库)

Fetches administrative regulations from xzfg.moj.gov.cn.

Endpoints:
  - List:   https://xzfg.moj.gov.cn/SearchTitleFront?SiteID=122&PageIndex={page}
  - Detail: https://xzfg.moj.gov.cn/front/law/detail?LawID={id}
  - 611 currently effective regulations, 62 pages of 10

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full initial pull
  python bootstrap.py test-api              # Quick connectivity test
"""

import argparse
import html as html_lib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/MOJ-AdminRegs"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.MOJ-AdminRegs")

BASE_URL = "https://xzfg.moj.gov.cn"
SEARCH_URL = f"{BASE_URL}/SearchTitleFront"
DETAIL_URL = f"{BASE_URL}/front/law/detail"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def _request_with_retry(url, retries=3, backoff=5, **kwargs):
    """Make an HTTP GET request with retries."""
    kwargs.setdefault("timeout", 30)
    kwargs.setdefault("headers", HEADERS)
    for attempt in range(retries):
        try:
            resp = requests.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries} for {url[:80]}... waiting {wait}s")
                time.sleep(wait)
            else:
                raise


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_lib.unescape(text)
    text = re.sub(r'\xa0', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def get_list_page(page: int) -> list:
    """Fetch a search listing page and extract regulation entries.

    Returns list of dicts with keys: law_id, title.
    """
    resp = _request_with_retry(SEARCH_URL, params={
        "SiteID": "122",
        "Query": "",
        "Type": "",
        "QueryAll": "",
        "SortData": "",
        "SortType": "",
        "PageIndex": str(page),
    })
    html = resp.text

    # Extract LawID and title from detail links
    # Pattern: <a ... href="...detail?LawID=123" ...>Title</a>
    entries = re.findall(
        r'<a[^>]*href=["\'][^"\']*detail\?LawID=(\d+)[^"\']*["\'][^>]*>(.*?)</a>',
        html, re.DOTALL
    )

    seen = set()
    results = []
    for law_id, raw_title in entries:
        if law_id in seen:
            continue
        seen.add(law_id)
        title = _clean_html(raw_title).strip()
        if title:
            results.append({"law_id": law_id, "title": title})

    # Extract total page count
    page_match = re.search(r'page-count["\'][^>]*value=["\'](\d+)', html)
    total_pages = int(page_match.group(1)) if page_match else None

    return results, total_pages


def get_detail(law_id: str) -> Optional[dict]:
    """Fetch full regulation detail page and extract text + metadata.

    Returns dict with keys: title, text, publish_date, effective_date, publish_number.
    """
    try:
        resp = _request_with_retry(DETAIL_URL, params={"LawID": law_id})
    except Exception as e:
        logger.warning(f"Failed to fetch detail for LawID={law_id}: {e}")
        return None

    html = resp.text

    # Extract the body content (everything between header and footer)
    body_match = re.search(r'<body>(.*?)</body>', html, re.DOTALL)
    if not body_match:
        return None

    body = body_match.group(1)

    # Remove scripts, styles, header, footer
    body = re.sub(r'<script[^>]*>.*?</script>', '', body, flags=re.DOTALL)
    body = re.sub(r'<style[^>]*>.*?</style>', '', body, flags=re.DOTALL)
    body = re.sub(r'<header>.*?</header>', '', body, flags=re.DOTALL)
    body = re.sub(r'<footer>.*?</footer>', '', body, flags=re.DOTALL)

    # Remove navigation elements
    body = re.sub(r'<div[^>]*class="[^"]*breadcrumb[^"]*"[^>]*>.*?</div>', '', body, flags=re.DOTALL)

    # Use the full body after removing nav/chrome elements
    raw_content = body

    text = _clean_html(raw_content)

    # Remove common non-content lines
    lines = text.split('\n')
    filtered = []
    skip_patterns = [
        r'^首页$', r'^首页\s*>.*$', r'^>$', r'^正文阅读$', r'^下载Word$',
        r'^下载PDF$', r'^扫码下载$', r'^历史沿革$', r'^收起$', r'^展开$',
        r'^暂无历史沿革$', r'^返回$', r'^顶部$', r'^中华人民共和国司法部',
        r'^京ICP备', r'^司法部信息中心', r'^Copyright',
        r'^\[footnote', r'^×$',
    ]
    for line in lines:
        stripped = line.strip()
        if not stripped:
            filtered.append('')
            continue
        if any(re.match(p, stripped) for p in skip_patterns):
            continue
        filtered.append(stripped)

    # Remove leading/trailing blank lines
    text = '\n'.join(filtered).strip()
    text = re.sub(r'\n{3,}', '\n\n', text)

    # Try to extract title from the page
    title_match = re.search(r'<title>(.*?)</title>', html)
    page_title = _clean_html(title_match.group(1)) if title_match else ""

    # Try to extract dates from metadata or text
    # Look for patterns like (YYYY年M月D日...)
    date_pattern = r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日'
    dates = re.findall(date_pattern, text[:500])

    publish_date = None
    if dates:
        y, m, d = dates[0]
        try:
            publish_date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except ValueError:
            pass

    return {
        "title": page_title if page_title and page_title != "国家行政法规库" else "",
        "text": text,
        "publish_date": publish_date,
    }


def normalize(law_id: str, list_title: str, detail: dict) -> dict:
    """Transform to standard schema."""
    title = detail.get("title") or list_title
    text = detail.get("text", "")

    return {
        "_id": f"MOJ-{law_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": detail.get("publish_date"),
        "url": f"{DETAIL_URL}?LawID={law_id}",
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents with full text."""
    records = []

    logger.info("Fetching regulation list from MOJ database...")
    entries, total_pages = get_list_page(1)
    logger.info(f"Page 1: {len(entries)} entries, total pages: {total_pages}")

    # Also get page 2 to have more variety
    if len(entries) < count + 5:
        entries2, _ = get_list_page(2)
        entries.extend(entries2)
        time.sleep(1)

    for entry in entries:
        if len(records) >= count:
            break

        law_id = entry["law_id"]
        title = entry["title"]
        logger.info(f"  Fetching LawID={law_id}: {title[:40]}...")

        detail = get_detail(law_id)
        time.sleep(2)

        if detail and detail.get("text") and len(detail["text"]) > 100:
            normalized = normalize(law_id, title, detail)
            records.append(normalized)
            logger.info(f"  [{len(records)}/{count}] {title[:40]}... ({len(detail['text'])} chars)")
        else:
            text_len = len(detail["text"]) if detail and detail.get("text") else 0
            logger.warning(f"  Skipped LawID={law_id} - insufficient text ({text_len} chars)")

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all regulations with full text."""
    page = 1
    total_yielded = 0

    _, total_pages = get_list_page(1)
    if not total_pages:
        total_pages = 62  # Fallback known value
    logger.info(f"Total pages to process: {total_pages}")

    while page <= total_pages:
        logger.info(f"Fetching list page {page}/{total_pages}...")
        entries, _ = get_list_page(page)

        if not entries:
            logger.warning(f"No entries on page {page}, stopping.")
            break

        for entry in entries:
            law_id = entry["law_id"]
            title = entry["title"]

            detail = get_detail(law_id)
            time.sleep(2)

            if detail and detail.get("text") and len(detail["text"]) > 100:
                normalized = normalize(law_id, title, detail)
                total_yielded += 1
                if total_yielded % 50 == 0:
                    logger.info(f"  Processed {total_yielded} records (page {page})...")
                yield normalized
            else:
                text_len = len(detail["text"]) if detail and detail.get("text") else 0
                logger.warning(f"  Skipped LawID={law_id} - insufficient text ({text_len} chars)")

        page += 1
        time.sleep(1)


def test_api():
    """Test API connectivity."""
    logger.info("Testing MOJ Administrative Regulations Database...")

    try:
        entries, total_pages = get_list_page(1)
        logger.info(f"List OK - page 1 has {len(entries)} entries, {total_pages} total pages")
    except Exception as e:
        logger.error(f"List fetch failed: {e}")
        return False

    if entries:
        law_id = entries[0]["law_id"]
        title = entries[0]["title"]
        logger.info(f"Testing detail for LawID={law_id}: {title}")

        detail = get_detail(law_id)
        if detail and detail.get("text"):
            text = detail["text"]
            logger.info(f"Full text OK - {len(text)} characters")
            logger.info(f"Preview: {text[:200]}...")
            return True
        else:
            logger.error("Full text extraction failed")
            return False

    return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = record["_id"].replace("/", "_")
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info(f"Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="CN/MOJ-AdminRegs Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode")
            count = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all():
                count += 1
                safe_id = record["_id"].replace("/", "_")
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Bootstrap complete: {count} records saved")


if __name__ == "__main__":
    main()
