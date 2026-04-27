#!/usr/bin/env python3
"""
CN/NPC-LocalLaws -- China Local Laws and Regulations (地方性法规)

Fetches local/provincial legislation from flk.npc.gov.cn.
Uses the same API as CN/NPC but with searchRange=2 for local regulations.

Strategy:
  - POST search to list local regulations with pagination (searchRange=2)
  - GET details for each regulation (metadata)
  - GET download URL for DOCX file (signed S3 URL)
  - Download DOCX and extract text with stdlib zipfile+xml

API:
  - Base: https://flk.npc.gov.cn
  - Search: /law-search/search/list (POST, JSON)
  - Details: /law-search/search/flfgDetails?bbbs={id}
  - Download: /law-search/download/pc?bbbs={id}&format=docx
  - No auth required

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full initial pull
  python bootstrap.py test-api              # Quick connectivity test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/NPC-LocalLaws"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.NPC-LocalLaws")

API_BASE = "https://flk.npc.gov.cn"
SEARCH_URL = f"{API_BASE}/law-search/search/list"
DETAILS_URL = f"{API_BASE}/law-search/search/flfgDetails"
DOWNLOAD_URL = f"{API_BASE}/law-search/download/pc"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://flk.npc.gov.cn",
    "Referer": "https://flk.npc.gov.cn/search",
}

# Timeliness status mapping
SXX_MAP = {
    "1": "已被修改",   # Modified
    "3": "现行有效",   # Currently effective
    "5": "已失效",     # Expired
    "7": "尚未生效",   # Not yet effective
    "9": "已废止",     # Repealed
}


def extract_text_from_docx(docx_bytes: bytes) -> str:
    """Extract text from DOCX using stdlib zipfile+xml (no python-docx needed)."""
    try:
        with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
            xml_content = zf.read("word/document.xml")
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        root = ET.fromstring(xml_content)
        paragraphs = []
        for p in root.iter(f"{{{ns['w']}}}p"):
            texts = [t.text for t in p.iter(f"{{{ns['w']}}}t") if t.text]
            line = "".join(texts).strip()
            if line:
                paragraphs.append(line)
        return "\n".join(paragraphs)
    except Exception as e:
        logger.warning(f"Failed to extract text from DOCX: {e}")
        return ""


def _request_with_retry(method, url, retries=3, backoff=5, **kwargs):
    """Make an HTTP request with retries on timeout/connection errors."""
    kwargs.setdefault("timeout", 30)
    kwargs.setdefault("headers", HEADERS)
    for attempt in range(retries):
        try:
            resp = requests.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries} for {url[:60]}... waiting {wait}s")
                time.sleep(wait)
            else:
                raise


def search_local_laws(page: int = 1, page_size: int = 20) -> tuple:
    """Search for local regulations. Returns (rows, total)."""
    payload = {
        "searchContent": "",
        "searchType": 2,
        "searchRange": 1,
        "flfgCodeId": [230, 260, 270, 290, 295, 300, 305, 310],  # Local regulation categories
        "zdjgCodeId": [],
        "sxx": [],
        "gbrq": [],
        "sxrq": [],
        "gbrqYear": [],
        "pageNum": page,
        "pageSize": page_size,
        "orderByParam": {"order": "gbrq", "sort": "DESC"},
    }
    response = _request_with_retry(
        "POST", SEARCH_URL,
        json=payload,
        headers={**HEADERS, "Content-Type": "application/json"},
    )
    data = response.json()

    if data.get("code") == 200:
        return data.get("rows", []), data.get("total", 0)
    else:
        logger.error(f"Search failed: {data.get('msg', 'Unknown error')}")
        return [], 0


def get_details(bbbs: str) -> Optional[dict]:
    """Get regulation details by bbbs ID."""
    try:
        response = _request_with_retry("GET", DETAILS_URL, params={"bbbs": bbbs})
        data = response.json()
        if data.get("code") == 200:
            return data.get("data")
    except Exception as e:
        logger.warning(f"Failed to get details for {bbbs}: {e}")
    return None


def get_download_url(bbbs: str) -> Optional[str]:
    """Get signed download URL for DOCX."""
    try:
        response = _request_with_retry(
            "GET", DOWNLOAD_URL, params={"bbbs": bbbs, "format": "docx"}
        )
        data = response.json()
        if data.get("code") == 200 and data.get("data"):
            return data["data"].get("url")
    except Exception as e:
        logger.warning(f"Failed to get download URL for {bbbs}: {e}")
    return None


def download_and_extract(bbbs: str) -> str:
    """Download DOCX and extract full text."""
    url = get_download_url(bbbs)
    if not url:
        return ""

    try:
        response = _request_with_retry("GET", url, timeout=60)
        if len(response.content) < 100:
            logger.warning(f"Download too small for {bbbs}: {len(response.content)} bytes")
            return ""
        return extract_text_from_docx(response.content)
    except Exception as e:
        logger.warning(f"Failed to download DOCX for {bbbs}: {e}")
        return ""


def normalize(search_record: dict, details: Optional[dict], text: str) -> dict:
    """Transform to standard schema."""
    bbbs = search_record.get("bbbs", "")
    title = search_record.get("title", "")
    title = re.sub(r'<[^>]+>', '', title)

    gbrq = search_record.get("gbrq", "")
    sxrq = search_record.get("sxrq", "")
    flxz = search_record.get("flxz", "")
    sxx = search_record.get("sxx", "")
    zdjg = search_record.get("zdjgName", "")

    return {
        "_id": bbbs,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "bbbs": bbbs,
        "title": title,
        "text": text,
        "date": gbrq,
        "effective_date": sxrq,
        "law_type": flxz,
        "timeliness": SXX_MAP.get(str(sxx), str(sxx)),
        "issuing_body": zdjg,
        "url": f"https://flk.npc.gov.cn/detail?id={bbbs}",
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents with full text."""
    records = []

    logger.info("Searching for recent local laws...")
    rows, total = search_local_laws(page=1, page_size=count + 5)
    logger.info(f"Total local laws available: {total:,}")

    for row in rows:
        if len(records) >= count:
            break

        bbbs = row.get("bbbs")
        title = re.sub(r'<[^>]+>', '', row.get("title", ""))
        if not bbbs:
            continue

        logger.info(f"  Fetching {title[:40]}... (ID: {bbbs[:12]}...)")

        details = get_details(bbbs)
        time.sleep(1)

        text = download_and_extract(bbbs)
        time.sleep(2)

        if text and len(text) > 100:
            normalized = normalize(row, details, text)
            records.append(normalized)
            logger.info(f"  [{len(records)}/{count}] {title[:50]}... ({len(text)} chars)")
        else:
            logger.warning(f"  Skipped {bbbs[:12]} - no/short text ({len(text)} chars)")

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all local regulations with full text."""
    page = 1
    page_size = 50
    total_yielded = 0

    _, total = search_local_laws(page=1, page_size=1)
    logger.info(f"Total local laws: {total:,}")

    while True:
        rows, _ = search_local_laws(page=page, page_size=page_size)
        if not rows:
            break

        for row in rows:
            bbbs = row.get("bbbs")
            if not bbbs:
                continue

            details = get_details(bbbs)
            time.sleep(1)
            text = download_and_extract(bbbs)
            time.sleep(2)

            if text and len(text) > 100:
                normalized = normalize(row, details, text)
                total_yielded += 1
                if total_yielded % 50 == 0:
                    logger.info(f"  Processed {total_yielded} records (page {page})...")
                yield normalized

        page += 1


def test_api():
    """Test API connectivity."""
    logger.info("Testing NPC Local Laws API...")

    try:
        rows, total = search_local_laws(page=1, page_size=2)
        logger.info(f"Search OK - {total:,} total local laws, got {len(rows)} results")
    except Exception as e:
        logger.error(f"Search failed: {e}")
        return False

    if rows:
        bbbs = rows[0]["bbbs"]
        title = re.sub(r'<[^>]+>', '', rows[0].get("title", ""))
        logger.info(f"Testing details for: {title}")

        details = get_details(bbbs)
        if details:
            logger.info(f"Details OK - keys: {list(details.keys())}")

        logger.info("Testing DOCX download...")
        text = download_and_extract(bbbs)
        if text and len(text) > 100:
            logger.info(f"Full text OK - {len(text)} characters")
            logger.info(f"Preview: {text[:200]}...")
            return True
        else:
            logger.error(f"Full text extraction failed ({len(text)} chars)")
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
        safe_id = record["bbbs"][:16]
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

    types = set(r.get("law_type", "") for r in records)
    logger.info(f"  - Law types: {', '.join(sorted(t for t in types if t))}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="CN/NPC-LocalLaws Fetcher")
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
                filepath = SAMPLE_DIR / f"record_{record['bbbs'][:16]}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Bootstrap complete: {count} records saved")


if __name__ == "__main__":
    main()
