#!/usr/bin/env python3
"""
CN/SAMR-Penalties -- China SAMR Administrative Penalty Decisions

Fetches administrative penalty decisions from the State Administration for
Market Regulation (SAMR) at cfws.samr.gov.cn. ~70K+ publicly disclosed
penalty documents with full text extracted from embedded PDFs.

Strategy:
  - POST /queryDoc to list penalty decisions (max 200 per query)
  - Paginate by date ranges (monthly) to work around 200-result limit
  - POST /getDoc to fetch individual documents with base64-embedded PDFs
  - Extract text from PDFs via pdfplumber
  - DES3 cipher required for anti-bot protection

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test-api
"""

import argparse
import base64
import hashlib
import io
import json
import logging
import random
import re
import string
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Generator

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from Crypto.Cipher import DES3
    from Crypto.Util.Padding import pad
except ImportError:
    try:
        from Cryptodome.Cipher import DES3
        from Cryptodome.Util.Padding import pad
    except ImportError:
        print("ERROR: pycryptodome not installed. Run: pip3 install pycryptodome")
        sys.exit(1)

try:
    import pdfplumber
    PDF_LIBRARY = "pdfplumber"
except ImportError:
    pdfplumber = None
    try:
        from pypdf import PdfReader
        PDF_LIBRARY = "pypdf"
    except ImportError:
        PdfReader = None
        PDF_LIBRARY = None

SOURCE_ID = "CN/SAMR-Penalties"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.SAMR-Penalties")

BASE_URL = "https://cfws.samr.gov.cn"
QUERY_URL = f"{BASE_URL}/queryDoc"
DOC_URL = f"{BASE_URL}/getDoc"


def _generate_cipher():
    """Generate DES3-encrypted timestamp cipher for anti-bot protection."""
    now = datetime.now()
    timestamp = str(int(time.time() * 1000))
    chars = string.ascii_letters + string.digits
    salt = ''.join(random.choice(chars) for _ in range(24))
    iv_str = now.strftime("%Y%m%d")
    key_bytes = salt.encode('utf-8')[:24]
    iv_bytes = iv_str.encode('utf-8')[:8]
    cipher = DES3.new(key_bytes, DES3.MODE_CBC, iv_bytes)
    encrypted = cipher.encrypt(pad(timestamp.encode('utf-8'), 8))
    enc_b64 = base64.b64encode(encrypted).decode('utf-8')
    combined = salt + iv_str + enc_b64
    return ' '.join(bin(ord(ch))[2:] for ch in combined)


def _create_session():
    """Create a requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/list.html",
    })
    # Get initial cookies
    session.get(BASE_URL, timeout=15)
    return session


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
    if pdfplumber:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = ""
            for page in pdf.pages:
                text += (page.extract_text() or "") + "\n"
            return text.strip()
    elif PdfReader:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = ""
        for page in reader.pages:
            text += (page.extract_text() or "") + "\n"
        return text.strip()
    else:
        return ""


def _request_with_retry(session, url, data, retries=3, backoff=3):
    """POST with retry logic."""
    for attempt in range(retries):
        try:
            resp = session.post(url, data=data, timeout=30)
            if resp.status_code == 403:
                logger.warning(f"403 on attempt {attempt+1}, refreshing session...")
                session.get(BASE_URL, timeout=15)
                time.sleep(backoff * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries}... waiting {wait}s")
                time.sleep(wait)
            else:
                raise
    return None


def query_docs(session, page_num=1, page_size=20, query_condition="[]",
               sort_fields=""):
    """Query the document list."""
    cipher = _generate_cipher()
    data = {
        "pageSize": str(page_size),
        "pageNum": str(page_num),
        "queryCondition": query_condition,
        "sortFields": sort_fields,
        "ciphertext": cipher,
    }
    resp = _request_with_retry(session, QUERY_URL, data)
    if not resp:
        return [], 0

    result = resp.json()
    if result.get("code") != 1:
        return [], 0

    inner = result.get("result", {})
    if isinstance(inner, str):
        # Encrypted response — shouldn't happen without secretKey but handle it
        return [], 0

    qr = inner.get("queryResult", {})
    count = qr.get("resultCount", 0)
    result_list = qr.get("resultList", [])
    return result_list, count


def fetch_doc_detail(session, doc_id: str) -> dict:
    """Fetch individual document full text."""
    cipher = _generate_cipher()
    data = {
        "docid": doc_id,
        "ciphertext": cipher,
    }
    resp = _request_with_retry(session, DOC_URL, data)
    if not resp:
        return {}

    result = resp.json()
    if result.get("code") != 1:
        return {}

    return result.get("result", {})


def _make_date_filter(start_date: str, end_date: str) -> str:
    """Create a date range filter for queryCondition.
    Dates in YYYYMMDD format."""
    # The date filter key is "23_s" for start and "23_e" for end
    filters = [
        {"key": "23_s", "id": start_date, "name": f"处罚日期起始：{start_date}"},
        {"key": "23_e", "id": end_date, "name": f"处罚日期结束：{end_date}"},
    ]
    return json.dumps(filters, ensure_ascii=False)


def normalize(doc_list_item: dict, doc_detail: dict, text: str) -> dict:
    """Normalize a penalty document."""
    doc_id = doc_list_item.get("1", doc_list_item.get("rowkey", ""))
    doc_number = doc_list_item.get("2", "")
    agency = doc_list_item.get("14", "")
    date_raw = doc_list_item.get("23", "")
    entity_name = doc_list_item.get("30", "") or doc_list_item.get("36", "")
    penalty_summary = doc_list_item.get("7", "")

    # Parse date from YYYYMMDD
    date_iso = ""
    if date_raw and len(date_raw) >= 8:
        try:
            date_iso = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        except (ValueError, IndexError):
            date_iso = date_raw

    # Detail fields
    penalty_type = doc_detail.get("i4", "")
    legal_basis = doc_detail.get("i5", "")

    title = f"{doc_number}" if doc_number else f"SAMR行政处罚-{doc_id[:8]}"
    if entity_name:
        title = f"{entity_name} — {title}"

    return {
        "_id": f"SAMR-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"{BASE_URL}/detail.html?docid={doc_id}",
        "doc_number": doc_number,
        "agency": agency,
        "entity_name": entity_name,
        "penalty_type": penalty_type,
        "penalty_summary": penalty_summary,
        "legal_basis": legal_basis,
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents."""
    if not PDF_LIBRARY:
        logger.error("No PDF library available (pdfplumber or pypdf)")
        return []

    session = _create_session()
    records = []

    # Get recent docs
    result_list, total = query_docs(session, page_num=1, page_size=count)
    logger.info(f"Total penalty documents: {total:,}")
    logger.info(f"Got {len(result_list)} from first page")

    for item in result_list:
        doc_id = item.get("1", item.get("rowkey", ""))
        if not doc_id:
            continue

        time.sleep(1)  # Rate limit

        detail = fetch_doc_detail(session, doc_id)
        pdf_b64 = detail.get("i7", "")
        text = ""
        if pdf_b64:
            try:
                pdf_bytes = base64.b64decode(pdf_b64)
                text = _extract_pdf_text(pdf_bytes)
            except Exception as e:
                logger.warning(f"PDF extraction failed for {doc_id}: {e}")

        if len(text) < 50:
            logger.warning(f"  Skipping {doc_id}: text too short ({len(text)} chars)")
            continue

        normalized = normalize(item, detail, text)
        records.append(normalized)
        logger.info(f"  [{len(records)}] {normalized['title'][:60]}... ({len(text)} chars)")

        if len(records) >= count:
            break

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all documents by iterating through monthly date ranges."""
    if not PDF_LIBRARY:
        logger.error("No PDF library available")
        return

    session = _create_session()
    total_yielded = 0

    # Get total count
    _, total = query_docs(session, page_num=1, page_size=1)
    logger.info(f"Total penalty documents: {total:,}")

    # Iterate by month from 2018-01 to current month
    current = datetime.now()
    year, month = 2018, 1

    while year < current.year or (year == current.year and month <= current.month):
        start_date = f"{year}{month:02d}01"
        # End of month
        if month == 12:
            end_date = f"{year + 1}0101"
        else:
            end_date = f"{year}{month + 1:02d}01"

        # Use date filter
        query_cond = _make_date_filter(start_date, end_date)

        page = 1
        month_count = 0
        while page <= 10:  # Max 10 pages * 20 = 200 per month
            result_list, count = query_docs(session, page_num=page,
                                            page_size=20,
                                            query_condition=query_cond)
            if not result_list:
                break

            for item in result_list:
                doc_id = item.get("1", item.get("rowkey", ""))
                if not doc_id:
                    continue

                time.sleep(1)

                detail = fetch_doc_detail(session, doc_id)
                pdf_b64 = detail.get("i7", "")
                text = ""
                if pdf_b64:
                    try:
                        pdf_bytes = base64.b64decode(pdf_b64)
                        text = _extract_pdf_text(pdf_bytes)
                    except Exception:
                        pass

                if len(text) < 50:
                    continue

                normalized = normalize(item, detail, text)
                total_yielded += 1
                month_count += 1
                if total_yielded % 100 == 0:
                    logger.info(f"  Yielded {total_yielded} total")
                yield normalized

            page += 1
            time.sleep(0.5)

        logger.info(f"  {year}-{month:02d}: {month_count} records")

        # Next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    logger.info(f"Total yielded: {total_yielded}")


def test_api():
    """Test API connectivity."""
    if not PDF_LIBRARY:
        logger.error(f"No PDF library available")
        return False

    logger.info(f"Testing SAMR penalty API (PDF lib: {PDF_LIBRARY})...")
    try:
        session = _create_session()
        result_list, total = query_docs(session, page_num=1, page_size=3)
        logger.info(f"  Total documents: {total:,}")
        logger.info(f"  Got {len(result_list)} results")

        if result_list:
            doc_id = result_list[0].get("1", "")
            detail = fetch_doc_detail(session, doc_id)
            pdf_b64 = detail.get("i7", "")
            if pdf_b64:
                pdf_bytes = base64.b64decode(pdf_b64)
                text = _extract_pdf_text(pdf_bytes)
                logger.info(f"  Full text length: {len(text)} chars")
                logger.info(f"  Text preview: {text[:100]}...")

        return len(result_list) > 0
    except Exception as e:
        logger.error(f"  FAILED: {e}")
        return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        filename = f"sample_{i:02d}.json"
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
    logger.info(f"  - PDF library: {PDF_LIBRARY}")

    agencies = {}
    for r in records:
        a = r.get("agency", "unknown")
        agencies[a] = agencies.get(a, 0) + 1
    logger.info(f"  - Agencies: {dict(list(agencies.items())[:5])}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="CN/SAMR-Penalties Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")

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
                filepath = SAMPLE_DIR / f"record_{count:07d}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
