#!/usr/bin/env python3
"""
CN/NPC-LocalLaws -- China Local Laws and Regulations (地方性法规)

Fetches local/provincial legislation from flk.npc.gov.cn.
Uses the same API as CN/NPC but with searchRange=2 for local regulations.

Strategy:
  - POST search to list local regulations with pagination (searchRange=2)
  - GET download URL for the stored office file (signed S3 URL)
  - Download it and extract text: .docx via stdlib zipfile+xml, legacy .doc
    via common.doc_extract (olefile piece table)

API:
  - Base: https://flk.npc.gov.cn
  - Search: /law-search/search/list (POST, JSON)
  - Details: /law-search/search/flfgDetails?bbbs={id}
  - Download: /law-search/download/pc?bbbs={id}&format=docx
  - No auth required

Note: the `format` parameter is decorative. The endpoint always hands back
whichever file the publisher stored, and roughly a fifth of the corpus --
concentrated in the older filings -- is Word 97-2003 binary, not DOCX.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap-fast        # Alias the fleet wrapper calls
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

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.doc_extract import extract_doc_text

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/NPC-LocalLaws"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "seen_ids.txt"

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


# File signatures. DOCX is a zip; Word 97-2003 is an OLE2 compound file.
ZIP_MAGIC = b"PK\x03\x04"
OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

# Codepage for the 8-bit pieces of a Chinese .doc. CJK text is normally stored
# as UTF-16 pieces, but mixed documents fall back to the publisher's ANSI page.
DOC_ENCODING = "gbk"

# HTTP statuses worth another attempt: rate limiting and gateway blips. A
# single 502 must not end a 27K-document crawl.
RETRY_STATUSES = {429, 500, 502, 503, 504}

CRAWL_DELAY = 1.0


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


def extract_office_text(content: bytes, bbbs: str, url: str = "") -> str:
    """Extract text from whatever office format the endpoint actually served.

    `format=docx` is not honoured -- the download endpoint returns the stored
    file, and the older filings are Word 97-2003 binaries. Dispatch on the
    magic bytes rather than assuming, and skip (loudly, once) anything that is
    neither, instead of letting zipfile raise per document.
    """
    if content.startswith(ZIP_MAGIC):
        return extract_text_from_docx(content)

    if content.startswith(OLE_MAGIC):
        text = extract_doc_text(content, encoding=DOC_ENCODING)
        if not text:
            logger.warning(f"Legacy .doc extraction returned nothing for {bbbs}")
        return text or ""

    logger.warning(
        f"Skipping {bbbs}: unrecognised file type "
        f"(magic {content[:8]!r}, {len(content)} bytes, {url.split('?')[0][-40:]})"
    )
    return ""


def _request_with_retry(method, url, retries=3, backoff=5, **kwargs):
    """Make an HTTP request with retries on timeouts, drops and 5xx/429."""
    kwargs.setdefault("timeout", 30)
    kwargs.setdefault("headers", HEADERS)
    for attempt in range(retries):
        try:
            resp = requests.request(method, url, **kwargs)
            if resp.status_code in RETRY_STATUSES and attempt < retries - 1:
                raise requests.exceptions.HTTPError(
                    f"HTTP {resp.status_code}", response=resp
                )
            resp.raise_for_status()
            return resp
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
        ) as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            # A 403/404 will not fix itself; only retry the transient classes.
            if status is not None and status not in RETRY_STATUSES:
                raise
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
    """Download the stored office file and extract full text."""
    url = get_download_url(bbbs)
    if not url:
        return ""

    try:
        response = _request_with_retry("GET", url, timeout=60)
        if len(response.content) < 100:
            logger.warning(f"Download too small for {bbbs}: {len(response.content)} bytes")
            return ""
        return extract_office_text(response.content, bbbs, url)
    except Exception as e:
        logger.warning(f"Failed to download document for {bbbs}: {e}")
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
    """Fetch sample documents with full text, spread across the corpus.

    Results are ordered newest-first and only the older filings are legacy
    .doc, so a sample drawn from page 1 alone exercises the DOCX path and
    nothing else -- which is how #1462 stayed invisible until the fleet run
    was 6,792 documents deep. Walk a few pages across the whole range.
    """
    records = []

    logger.info("Searching for local laws...")
    _, total = search_local_laws(page=1, page_size=1)
    logger.info(f"Total local laws available: {total:,}")

    page_size = 5
    last_page = max(1, total // page_size)
    pages = sorted({max(1, int(last_page * f)) for f in (0, 0.2, 0.4, 0.6, 0.8)})
    per_page = max(1, count // len(pages))

    for page in pages:
        if len(records) >= count:
            break
        rows, _ = search_local_laws(page=page, page_size=page_size)
        taken = 0

        for row in rows:
            if len(records) >= count or taken >= per_page:
                break

            bbbs = row.get("bbbs")
            title = re.sub(r'<[^>]+>', '', row.get("title", ""))
            if not bbbs:
                continue

            text = download_and_extract(bbbs)
            time.sleep(CRAWL_DELAY)

            if text and len(text) > 100:
                records.append(normalize(row, None, text))
                taken += 1
                logger.info(f"  [{len(records)}/{count}] {title[:40]} ({len(text)} chars)")
            else:
                logger.warning(f"  Skipped {bbbs[:12]} - no/short text ({len(text)} chars)")

    return records


def load_checkpoint() -> set:
    """Regulation IDs an earlier run already wrote or deliberately skipped."""
    if not CHECKPOINT_PATH.exists():
        return set()
    done = {ln.strip() for ln in CHECKPOINT_PATH.read_text().splitlines() if ln.strip()}
    logger.info(f"Checkpoint: {len(done)} regulations already processed, skipping")
    return done


def mark_done(handle, bbbs: str) -> None:
    """Record a finished regulation so a restart does not re-download it."""
    handle.write(f"{bbbs}\n")
    handle.flush()


def fetch_all(resume: bool = True) -> Generator[dict, None, None]:
    """Fetch all local regulations with full text."""
    page = 1
    page_size = 50
    total_yielded = 0
    skipped = 0

    _, total = search_local_laws(page=1, page_size=1)
    logger.info(f"Total local laws: {total:,}")
    if not total:
        raise RuntimeError(
            f"{SEARCH_URL} reported 0 local regulations — the corpus is ~27,000, "
            "so this vantage is being refused or the search payload changed; "
            "refusing to report an empty corpus"
        )

    done = load_checkpoint() if resume else set()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Append: the checkpoint suppresses what earlier runs already emitted.
    with open(CHECKPOINT_PATH, "a", encoding="utf-8") as ckpt:
        while True:
            rows, _ = search_local_laws(page=page, page_size=page_size)
            if not rows:
                break

            for row in rows:
                bbbs = row.get("bbbs")
                if not bbbs or bbbs in done:
                    continue

                # No get_details() call: normalize() takes everything it needs
                # from the search row, and the extra round trip per document
                # was doubling the wall clock on a 27K-document crawl.
                text = download_and_extract(bbbs)
                time.sleep(CRAWL_DELAY)

                if text and len(text) > 100:
                    normalized = normalize(row, None, text)
                    total_yielded += 1
                    if total_yielded % 50 == 0:
                        logger.info(
                            f"  Processed {total_yielded} records "
                            f"({skipped} unextractable) on page {page}..."
                        )
                    yield normalized
                else:
                    skipped += 1

                # Marked either way: a document whose file will not extract is
                # a permanent skip, not something to retry every restart.
                done.add(bbbs)
                mark_done(ckpt, bbbs)

            page += 1

    logger.info(
        f"Crawl finished: {total_yielded} records with full text, "
        f"{skipped} skipped as unextractable"
    )


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


def bootstrap_full(resume: bool = True) -> bool:
    """Stream the whole corpus to data/records.jsonl."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0

    # Append, not truncate: on resume the checkpoint suppresses the regulations
    # earlier runs already wrote, so a rewrite would discard them.
    with open(RECORDS_PATH, "a", encoding="utf-8") as f:
        for record in fetch_all(resume=resume):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % 200 == 0:
                f.flush()

    logger.info(f"Wrote {count} records to {RECORDS_PATH}")
    # A resumed run that finds everything already done writes nothing new but
    # is still a success — judge on the file, not on this run's delta.
    return RECORDS_PATH.exists() and RECORDS_PATH.stat().st_size > 0


def main():
    parser = argparse.ArgumentParser(description="CN/NPC-LocalLaws Fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Ignore the checkpoint and re-crawl from scratch",
    )

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    # `bootstrap-fast` is what the fleet wrapper invokes; without it argparse
    # exits 2 and the wrapper falls back to re-ingesting sample/.
    if args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        logger.info("Full bootstrap mode")
        success = bootstrap_full(resume=not args.restart)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
