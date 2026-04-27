#!/usr/bin/env python3
"""
CN/NFRA -- China National Financial Regulatory Administration (国家金融监督管理总局)

Fetches banking/insurance regulations, normative documents, and notices
from the NFRA internal JSON API.

Strategy:
  - List: GET /cbircweb/DocInfo/SelectDocByItemIdAndChild?itemId={id}&pageSize=50&pageNo={p}
  - Detail: GET /cbircweb/DocInfo/SelectByDocId?docId={id}
  - Full text from docClobNohtml (plain text) or docClob (HTML, stripped)
  - Falls back to PDF download if inline text too short
  - Categories: 927 (laws), 928 (regulations), 925 (notices)

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

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

try:
    from common.pdf_extract import extract_pdf_markdown
    HAS_PDF_EXTRACT = True
except ImportError:
    HAS_PDF_EXTRACT = False

SOURCE_ID = "CN/NFRA"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.NFRA")

BASE_URL = "https://www.nfra.gov.cn"
API_BASE = f"{BASE_URL}/cbircweb"

# Category ID → (label, data_type)
CATEGORIES = {
    927: ("law", "legislation"),
    928: ("regulation", "legislation"),
    925: ("notice", "doctrine"),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

PAGE_SIZE = 50
MIN_TEXT_FOR_PDF = 200


def _get(url, retries=3, backoff=5, **kwargs):
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
    if not text:
        return ""
    # Remove MS Office XML metadata blocks
    text = re.sub(r'<!\[if.*?<!\[endif\]>', '', text, flags=re.DOTALL)
    text = re.sub(r'<o:p>.*?</o:p>', '', text, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<head[^>]*>.*?</head>', '', text, flags=re.DOTALL)
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
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    # Filter out MS Office noise lines (short numeric/hex/metadata)
    clean_lines = []
    for line in lines:
        if not line:
            continue
        # Skip Word metadata artifacts
        if re.match(r'^(cbirc|MicrosoftInternetExplorer\d|DocumentNotSpecified|Web|Normal)$', line):
            continue
        if re.match(r'^[0-9A-Fa-f]{20,}$', line):
            continue
        if re.match(r'^\d{1,5}(-[\d.]+)*$', line) and len(line) < 20:
            continue
        if re.match(r'^\d+\.\d+ 磅$', line):
            continue
        clean_lines.append(line)
    return '\n'.join(clean_lines).strip()


def list_documents(item_id: int, page: int = 1, page_size: int = PAGE_SIZE) -> tuple:
    """List documents for a category. Returns (rows, total)."""
    url = f"{API_BASE}/DocInfo/SelectDocByItemIdAndChild?itemId={item_id}&pageSize={page_size}&pageNo={page}"
    resp = _get(url)
    data = resp.json()
    if data.get("rptCode") != 200:
        logger.error(f"API error for itemId={item_id} page={page}: {data.get('msg')}")
        return [], 0
    payload = data.get("data", {})
    return payload.get("rows", []), payload.get("total", 0)


def fetch_detail(doc_id: int) -> dict:
    """Fetch full document detail including text content."""
    url = f"{API_BASE}/DocInfo/SelectByDocId?docId={doc_id}"
    resp = _get(url)
    data = resp.json()
    if data.get("rptCode") != 200:
        logger.error(f"Detail API error for docId={doc_id}: {data.get('msg')}")
        return {}
    return data.get("data", {})


def _try_pdf(pdf_path: str, doc_id: int) -> str:
    """Download and extract text from a PDF attachment."""
    if not HAS_PDF_EXTRACT or not pdf_path:
        return ""
    full_url = f"{BASE_URL}{pdf_path}" if pdf_path.startswith("/") else pdf_path
    try:
        resp = _get(full_url, timeout=60)
        if len(resp.content) < 100:
            return ""
        text = extract_pdf_markdown(
            SOURCE_ID, f"{doc_id}-pdf",
            pdf_bytes=resp.content,
            table="legislation",
            force=True,
        )
        return text or ""
    except Exception as e:
        logger.warning(f"PDF extraction failed for docId={doc_id}: {e}")
        return ""


def normalize(detail: dict, category: str, data_type: str) -> dict:
    """Transform API response to standard schema."""
    doc_id = detail.get("docId", 0)
    title = detail.get("docTitle", "") or detail.get("docSubtitle", "") or ""

    # Prefer plain text, fall back to HTML stripping
    text = (detail.get("docClobNohtml") or "").strip()
    if not text or len(text) < MIN_TEXT_FOR_PDF:
        html_text = strip_html(detail.get("docClob", ""))
        if len(html_text) > len(text):
            text = html_text

    # Try PDF if text is still too short
    if len(text) < MIN_TEXT_FOR_PDF:
        pdf_url = detail.get("pdfFileUrl") or detail.get("docFileUrl", "")
        if pdf_url and pdf_url.endswith(".pdf"):
            pdf_text = _try_pdf(pdf_url, doc_id)
            if len(pdf_text) > len(text):
                text = pdf_text
                logger.info(f"  PDF enrichment: {len(text)} chars for docId={doc_id}")

    # Parse date
    pub_date = detail.get("publishDate", "") or ""
    date_iso = ""
    if pub_date:
        try:
            date_iso = pub_date.split(" ")[0]
            datetime.strptime(date_iso, "%Y-%m-%d")  # validate
        except ValueError:
            date_iso = pub_date

    doc_url = f"{BASE_URL}/cn/view/pages/ItemDetail.html?docId={doc_id}&itemId=928"

    return {
        "_id": f"NFRA-{doc_id}",
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": doc_url,
        "category": category,
        "document_no": detail.get("documentNo") or detail.get("indexNo") or "",
        "source_dept": detail.get("docSource") or "",
        "language": "zh-CN",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all normalized documents across categories."""
    sample_per_cat = 5 if sample else None
    total_yielded = 0

    for item_id, (cat_label, data_type) in CATEGORIES.items():
        logger.info(f"--- Category: {cat_label} (itemId={item_id}) ---")
        page = 1
        cat_count = 0

        while True:
            rows, total = list_documents(item_id, page=page)
            if not rows:
                break
            logger.info(f"  Page {page}: {len(rows)} docs (total={total})")

            for row in rows:
                doc_id = row.get("docId")
                if not doc_id:
                    continue

                time.sleep(1.5)  # rate limit
                try:
                    detail = fetch_detail(int(doc_id))
                except Exception as e:
                    logger.error(f"Failed to fetch docId={doc_id}: {e}")
                    continue

                if not detail:
                    continue

                record = normalize(detail, cat_label, data_type)
                if not record["text"]:
                    logger.warning(f"  No text for docId={doc_id}: {record['title'][:60]}")
                    continue

                yield record
                cat_count += 1
                total_yielded += 1

                if sample_per_cat and cat_count >= sample_per_cat:
                    break

            if sample_per_cat and cat_count >= sample_per_cat:
                logger.info(f"  Sample limit reached for {cat_label}: {cat_count} docs")
                break

            page += 1
            time.sleep(1)

        logger.info(f"  Category {cat_label}: {cat_count} documents fetched")

    logger.info(f"Total documents fetched: {total_yielded}")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Yield documents published since a given date."""
    try:
        since_dt = datetime.strptime(since, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {since}")
        return

    for item_id, (cat_label, data_type) in CATEGORIES.items():
        page = 1
        while True:
            rows, total = list_documents(item_id, page=page)
            if not rows:
                break

            stop_paging = False
            for row in rows:
                pub_date = (row.get("publishDate") or "").split(" ")[0]
                try:
                    row_dt = datetime.strptime(pub_date, "%Y-%m-%d")
                except ValueError:
                    continue

                if row_dt < since_dt:
                    stop_paging = True
                    break

                doc_id = row.get("docId")
                if not doc_id:
                    continue

                time.sleep(1.5)
                try:
                    detail = fetch_detail(int(doc_id))
                except Exception:
                    continue
                if not detail:
                    continue

                record = normalize(detail, cat_label, data_type)
                if record["text"]:
                    yield record

            if stop_paging:
                break
            page += 1
            time.sleep(1)


def save_samples(records: list, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for rec in records:
        fname = f"{rec['_id']}.json"
        with open(out_dir / fname, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(records)} samples to {out_dir}")


def test_api():
    """Quick connectivity and data check."""
    print("=== Testing CN/NFRA API ===\n")

    for item_id, (label, dtype) in CATEGORIES.items():
        rows, total = list_documents(item_id, page=1, page_size=2)
        print(f"Category '{label}' (itemId={item_id}): total={total}, sample={len(rows)}")
        if rows:
            doc_id = rows[0].get("docId")
            detail = fetch_detail(int(doc_id))
            text = (detail.get("docClobNohtml") or "").strip()
            html = strip_html(detail.get("docClob", ""))
            print(f"  docId={doc_id}: text={len(text)} chars, html={len(html)} chars")
            print(f"  Title: {detail.get('docTitle', '')[:80]}")
        print()


def main():
    parser = argparse.ArgumentParser(description="CN/NFRA Legal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api", "updates"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (5 per category)")
    parser.add_argument("--since", help="For updates: fetch docs since YYYY-MM-DD")
    parser.add_argument("--full", action="store_true", help="Fetch all documents (no sample limit)")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        is_sample = args.sample or not args.full
        records = list(fetch_all(sample=is_sample))
        if records:
            out_dir = SAMPLE_DIR if is_sample else SOURCE_DIR / "data"
            save_samples(records, out_dir)

            # Validation stats
            texts = [len(r["text"]) for r in records]
            print(f"\n=== CN/NFRA Bootstrap Results ===")
            print(f"Records: {len(records)}")
            print(f"Text lengths: min={min(texts)}, max={max(texts)}, avg={sum(texts)//len(texts)}")
            print(f"With text: {sum(1 for t in texts if t > 0)}/{len(records)}")
            cats = {}
            for r in records:
                cats[r["category"]] = cats.get(r["category"], 0) + 1
            print(f"Categories: {cats}")
        else:
            print("ERROR: No records fetched!")
            sys.exit(1)
    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since YYYY-MM-DD required for updates")
            sys.exit(1)
        records = list(fetch_updates(args.since))
        if records:
            save_samples(records, SOURCE_DIR / "updates")
        print(f"Updates since {args.since}: {len(records)} records")


if __name__ == "__main__":
    main()
