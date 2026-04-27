#!/usr/bin/env python3
"""
CN/CSRC -- China Securities Regulatory Commission (中国证监会) Data Fetcher

Fetches enforcement decisions, announcements, market bans, and orders
from the CSRC JSON search API.

Strategy:
  - GET /searchList/{channelid}?_isJson=true&_pageSize=50&_pageNo={page}
  - Full text from 'contentHtml' field (stripped HTML)
  - For records with short text, fetches detail page for PDF attachments
  - 4 channels: penalties, market bans, announcements, orders

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
from pathlib import Path
from typing import Generator
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Add project root for common imports
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

try:
    from common.pdf_extract import extract_pdf_markdown
    HAS_PDF_EXTRACT = True
except ImportError:
    HAS_PDF_EXTRACT = False

SOURCE_ID = "CN/CSRC"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.CSRC")

BASE_URL = "https://www.csrc.gov.cn"

# Channel IDs mapped to (label, data_type)
CHANNELS = {
    "17d5ff2fe43e488dba825807ae40d63f": ("penalty", "case_law"),
    "3795869930ca4b70bf55469270a6e641": ("market_ban", "case_law"),
    "625a2f95cc104c1d8b646f98d9f87470": ("announcement", "doctrine"),
    "cd11df89f5894c1eac37ae37cc11e369": ("order", "doctrine"),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

MIN_TEXT_FOR_PDF_FETCH = 500  # chars — fetch PDFs if text shorter than this


def _request_with_retry(url, retries=3, backoff=5, **kwargs):
    """Make GET request with retries on timeout/connection errors."""
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


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '\n', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&ensp;', ' ', text)
    text = re.sub(r'&emsp;', ' ', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&[a-z]+;', ' ', text)
    text = re.sub(r'&#\d+;', '', text)
    # Collapse blank lines but keep paragraph breaks
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(line for line in lines if line)
    return text.strip()


def _enrich_from_detail_page(url: str, doc_id: str) -> str:
    """Fetch detail page and extract text from HTML body and/or PDF attachments."""
    try:
        resp = _request_with_retry(url, timeout=20)
        resp.encoding = "utf-8"
        html = resp.text
    except Exception as e:
        logger.debug(f"Could not fetch detail page {url}: {e}")
        return ""

    # Try HTML body first
    html_text = ""
    match = re.search(r'<div[^>]*class="detail-news"[^>]*>(.*?)</div>', html, re.DOTALL)
    if match:
        html_text = strip_html(match.group(1))
        if len(html_text) > 100:
            logger.info(f"  Detail page HTML: {len(html_text)} chars")

    # Try PDF attachments (may have richer content than the HTML summary)
    pdf_text = ""
    if HAS_PDF_EXTRACT:
        pdf_urls = re.findall(r'href="([^"]*\.pdf)"', html)
        texts = []
        for pdf_path in pdf_urls[:3]:
            if not pdf_path.startswith("http"):
                if pdf_path.startswith("//"):
                    full_pdf_url = "https:" + quote(pdf_path, safe="/:")
                elif pdf_path.startswith("/"):
                    full_pdf_url = BASE_URL + quote(pdf_path, safe="/")
                else:
                    base = url.rsplit("/", 1)[0]
                    full_pdf_url = base + "/" + quote(pdf_path, safe="/")
            else:
                full_pdf_url = pdf_path

            try:
                pdf_resp = _request_with_retry(full_pdf_url, timeout=30)
                pdf_bytes = pdf_resp.content
                if len(pdf_bytes) < 100:
                    continue
                text = extract_pdf_markdown(
                    SOURCE_ID, f"{doc_id}-pdf",
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                    force=True,
                )
                if text and len(text) > 100:
                    texts.append(text)
                    logger.info(f"  PDF extracted: {len(text)} chars")
            except Exception as e:
                logger.warning(f"  PDF extraction failed: {e}")

        pdf_text = "\n\n---\n\n".join(texts)

    # Return the longer result
    return pdf_text if len(pdf_text) > len(html_text) else html_text


def search_channel(channel_id: str, page: int = 1, page_size: int = 50) -> tuple:
    """Search a channel. Returns (results_list, total_count)."""
    url = (
        f"{BASE_URL}/searchList/{channel_id}"
        f"?_isAgg=true&_isJson=true&_pageSize={page_size}"
        f"&_template=index&_pageNo={page}"
    )
    response = _request_with_retry(url)
    data = response.json()

    results = data.get("data", {}).get("results", [])
    total = data.get("data", {}).get("total", 0)
    return results, total


def normalize(record: dict, category: str, data_type: str, fetch_pdfs: bool = False) -> dict:
    """Transform API record to standard schema."""
    manuscript_id = record.get("manuscriptId", "")
    title = strip_html(record.get("title", ""))

    # Prefer contentHtml over content for full text
    content_html = record.get("contentHtml", "") or ""
    content_plain = record.get("content", "") or ""
    text = strip_html(content_html) if content_html else strip_html(content_plain)

    pub_time = record.get("publishedTimeStr", "")
    url_path = record.get("url", "")

    # Build full URL
    if url_path and not url_path.startswith("http"):
        url_path = f"https:{url_path}" if url_path.startswith("//") else f"{BASE_URL}{url_path}"

    # For short records, try detail page HTML then PDF attachments
    if fetch_pdfs and len(text) < MIN_TEXT_FOR_PDF_FETCH and url_path:
        enriched = _enrich_from_detail_page(url_path, manuscript_id)
        if enriched:
            text = text + "\n\n" + enriched if text else enriched
            logger.info(f"  Enriched from detail page: now {len(text)} chars")

    # Parse date to ISO format
    date_iso = ""
    if pub_time:
        try:
            dt = datetime.strptime(pub_time.split(" ")[0], "%Y-%m-%d")
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_iso = pub_time

    return {
        "_id": f"CSRC-{manuscript_id}",
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "manuscriptId": manuscript_id,
        "title": title,
        "text": text,
        "date": date_iso,
        "url": url_path,
        "category": category,
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents from each channel."""
    records = []
    per_channel = max(4, count // len(CHANNELS))

    for channel_id, (category, data_type) in CHANNELS.items():
        logger.info(f"Fetching {category} (channel {channel_id[:8]}...)...")
        try:
            results, total = search_channel(channel_id, page=1, page_size=per_channel)
            logger.info(f"  Total {category}: {total:,}")
        except Exception as e:
            logger.error(f"  Failed to fetch {category}: {e}")
            continue

        for r in results:
            normalized = normalize(r, category, data_type, fetch_pdfs=True)
            text_len = len(normalized.get("text", ""))
            if text_len > 50:
                records.append(normalized)
                logger.info(f"  [{len(records)}] {normalized['title'][:50]}... ({text_len} chars)")
            else:
                logger.warning(f"  Skipped {normalized['manuscriptId']} - text too short ({text_len})")

            if len(records) >= count:
                break

        time.sleep(2)
        if len(records) >= count:
            break

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all documents from all channels."""
    total_yielded = 0

    for channel_id, (category, data_type) in CHANNELS.items():
        logger.info(f"\n=== Channel: {category} ===")
        page = 1
        page_size = 50
        channel_count = 0

        _, total = search_channel(channel_id, page=1, page_size=1)
        logger.info(f"Total {category}: {total:,}")

        while True:
            try:
                results, _ = search_channel(channel_id, page=page, page_size=page_size)
            except Exception as e:
                logger.error(f"Failed page {page} of {category}: {e}")
                break

            if not results:
                break

            for r in results:
                normalized = normalize(r, category, data_type, fetch_pdfs=True)
                if len(normalized.get("text", "")) > 50:
                    total_yielded += 1
                    channel_count += 1
                    if total_yielded % 100 == 0:
                        logger.info(f"  Processed {total_yielded} total ({channel_count} in {category}, page {page})")
                    yield normalized

            page += 1
            time.sleep(2)

        logger.info(f"  {category}: {channel_count} records")


def test_api():
    """Test API connectivity for all channels."""
    logger.info("Testing CSRC JSON API...")
    all_ok = True

    for channel_id, (category, data_type) in CHANNELS.items():
        try:
            results, total = search_channel(channel_id, page=1, page_size=1)
            if results:
                title = strip_html(results[0].get("title", ""))
                content_html = results[0].get("contentHtml", "") or ""
                text = strip_html(content_html) if content_html else strip_html(results[0].get("content", ""))
                logger.info(f"  {category}: {total:,} docs - '{title[:50]}' ({len(text)} chars)")
            else:
                logger.warning(f"  {category}: {total} docs but empty results")
                all_ok = False
        except Exception as e:
            logger.error(f"  {category}: FAILED - {e}")
            all_ok = False
        time.sleep(1)

    return all_ok


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        mid = record["manuscriptId"][:16]
        filename = f"sample_{i:02d}_{record['category']}_{mid}.json"
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

    categories = {}
    for r in records:
        cat = r.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1
    logger.info(f"  - Categories: {categories}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="CN/CSRC Securities Regulator Fetcher")
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
                mid = record["manuscriptId"][:16]
                filepath = SAMPLE_DIR / f"record_{record['category']}_{mid}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
