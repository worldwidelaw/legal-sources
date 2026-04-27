#!/usr/bin/env python3
"""
CN/CNIPA -- China National Intellectual Property Administration (国家知识产权局)

Fetches IP regulations, patent/trademark laws, examination guidelines,
policy interpretations, announcements, and administrative decisions
from the CNIPA XML dataproxy API.

Strategy:
  - GET /module/web/jpage/dataproxy.jsp?page={page}&columnid={col}&unitid={unit}
  - Parse XML CDATA records for title, date, URL
  - Fetch individual article HTML pages for full text
  - For admin decisions (column 2432), download PDF attachments
  - ~2,300+ documents across 13 columns

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test-api
"""

import argparse
import html as html_module
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from urllib.parse import unquote

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

SOURCE_ID = "CN/CNIPA"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.CNIPA")

BASE_URL = "https://www.cnipa.gov.cn"
DATAPROXY_URL = BASE_URL + "/module/web/jpage/dataproxy.jsp"

# (column_id, unit_id, label, data_type)
COLUMNS = [
    # Legislation: Laws and Regulations
    (97, 4186, "patent_law", "legislation"),
    (98, 4186, "patent_admin_regulation", "legislation"),
    (99, 4186, "patent_dept_rule", "legislation"),
    (95, 4186, "trademark_law", "legislation"),
    (96, 4186, "trademark_admin_regulation", "legislation"),
    (3323, 4186, "trademark_dept_rule", "legislation"),
    (104, 4186, "gi_law", "legislation"),
    (106, 4186, "ic_layout_design", "legislation"),
    # Doctrine: Policy and Guidance
    (74, 17035, "announcement", "doctrine"),
    (75, 17035, "notice", "doctrine"),
    (66, 669, "policy_interpretation", "doctrine"),
    (65, 669, "development_planning", "doctrine"),
    # Doctrine: Administrative Decisions (PDF)
    (2432, 669, "admin_decision", "doctrine"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}


def _request_with_retry(url, retries=3, backoff=5, **kwargs):
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
    text = re.sub(r'<[^>]+>', '\n', text)
    text = html_module.unescape(text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(line for line in lines if line)
    return text.strip()


def fetch_column_listing(col_id: int, unit_id: int, page: int = 1, per_page: int = 100) -> tuple:
    """Fetch document listing from dataproxy. Returns (records_list, total_count)."""
    params = {
        "page": page,
        "webid": 1,
        "columnid": col_id,
        "unitid": unit_id,
        "perpage": per_page,
    }
    resp = _request_with_retry(DATAPROXY_URL, params=params)
    resp.encoding = "utf-8"
    xml_text = resp.text

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        logger.error(f"Failed to parse XML for col={col_id}")
        return [], 0

    total = int(root.findtext("totalrecord", "0"))
    records = []

    for record_el in root.iter("record"):
        cdata = record_el.text or ""
        parsed = _parse_cdata_record(cdata, col_id)
        if parsed:
            records.append(parsed)

    return records, total


def _parse_cdata_record(cdata: str, col_id: int) -> dict:
    """Parse a CDATA record from the dataproxy XML response."""
    # Extract href
    href_match = re.search(r'href="([^"]+)"', cdata)
    if not href_match:
        return None

    raw_url = href_match.group(1)

    # Extract title (text content of <a> tag)
    title_match = re.search(r'>([^<]+)</a>', cdata)
    title = title_match.group(1).strip() if title_match else ""

    # Extract date from <span>
    date_match = re.search(r'<span>(\d{4}-\d{2}-\d{2})</span>', cdata)
    date_str = date_match.group(1) if date_match else ""

    # Determine if this is a PDF link (admin decisions) or HTML article
    is_pdf = raw_url.endswith(".pdf")

    # Build full URL
    if raw_url.startswith("http"):
        full_url = raw_url
    elif raw_url.startswith("/"):
        full_url = BASE_URL + raw_url
    else:
        full_url = BASE_URL + "/" + raw_url

    # Extract doc_id from URL
    doc_id_match = re.search(r'art_\d+_(\d+)', raw_url)
    if doc_id_match:
        doc_id = f"art_{col_id}_{doc_id_match.group(1)}"
    elif is_pdf:
        # PDF filename as ID
        pdf_name = raw_url.split("/")[-1].replace(".pdf", "")
        doc_id = f"pdf_{col_id}_{pdf_name}"
    else:
        doc_id = f"col{col_id}_{hash(raw_url) & 0xFFFFFFFF:08x}"

    return {
        "doc_id": doc_id,
        "title": title,
        "date": date_str,
        "url": full_url,
        "is_pdf": is_pdf,
        "col_id": col_id,
    }


def fetch_article_text(url: str) -> str:
    """Fetch full text from an HTML article page."""
    try:
        resp = _request_with_retry(url, timeout=20)
        resp.encoding = "utf-8"
        html_content = resp.text
    except Exception as e:
        logger.debug(f"Could not fetch article {url}: {e}")
        return ""

    html_text = ""

    # Primary: div.article-content
    match = re.search(
        r'<div[^>]*class="article-content[^"]*"[^>]*>(.*?)(?:</div>\s*<div|</div>\s*<!--)',
        html_content, re.DOTALL
    )
    if match:
        html_text = strip_html(match.group(1))
        html_text = re.sub(r'^begin-->\s*', '', html_text)

    # Fallback: div.TRS_Editor
    if len(html_text) < 50:
        match = re.search(
            r'<div[^>]*class="TRS_Editor"[^>]*>(.*?)</div>',
            html_content, re.DOTALL
        )
        if match:
            html_text = strip_html(match.group(1))

    # Fallback: <p> tags in main
    if len(html_text) < 50:
        match = re.search(r'<div class="main">(.*?)</div>\s*</div>\s*</div>', html_content, re.DOTALL)
        if match:
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', match.group(1), re.DOTALL)
            if paragraphs:
                html_text = "\n\n".join(strip_html(p) for p in paragraphs)

    # If HTML text is short, try PDF attachments (many articles have full text in PDF)
    if len(html_text) < 500:
        pdf_text = _extract_pdfs_from_page(html_content, url)
        if pdf_text and len(pdf_text) > len(html_text):
            return pdf_text

    return html_text


def _extract_pdfs_from_page(html_content: str, page_url: str) -> str:
    """Extract text from PDF attachments linked on a page."""
    if not HAS_PDF_EXTRACT:
        return ""

    # Match both direct .pdf links and downfile.jsp links with .pdf in the URL
    pdf_urls = re.findall(r'href="([^"]*(?:\.pdf|downfile\.jsp[^"]*\.pdf[^"]*))"', html_content, re.I)
    if not pdf_urls:
        return ""

    texts = []
    for pdf_path in pdf_urls[:3]:
        if pdf_path.startswith("http"):
            full_url = pdf_path
        elif pdf_path.startswith("/"):
            full_url = BASE_URL + pdf_path
        else:
            base = page_url.rsplit("/", 1)[0]
            full_url = base + "/" + pdf_path

        try:
            pdf_resp = _request_with_retry(full_url, timeout=30)
            pdf_bytes = pdf_resp.content
            if len(pdf_bytes) < 100:
                continue
            text = extract_pdf_markdown(
                SOURCE_ID, "cnipa-pdf",
                pdf_bytes=pdf_bytes,
                table="doctrine",
                force=True,
            )
            if text and len(text) > 100:
                texts.append(text)
        except Exception as e:
            logger.debug(f"PDF extraction failed for {full_url}: {e}")

    return "\n\n---\n\n".join(texts)


def fetch_pdf_text(url: str) -> str:
    """Download and extract text from a PDF document."""
    if not HAS_PDF_EXTRACT:
        logger.warning("pdf_extract not available, cannot process PDF")
        return ""

    try:
        resp = _request_with_retry(url, timeout=30)
        pdf_bytes = resp.content
        if len(pdf_bytes) < 100:
            return ""
        text = extract_pdf_markdown(
            SOURCE_ID, "cnipa-decision",
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        return text or ""
    except Exception as e:
        logger.debug(f"PDF download/extraction failed for {url}: {e}")
        return ""


def normalize(record: dict, category: str, data_type: str, text: str) -> dict:
    return {
        "_id": f"CNIPA-{record['doc_id']}",
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "doc_id": record["doc_id"],
        "title": record["title"],
        "text": text,
        "date": record["date"],
        "url": record["url"],
        "category": category,
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents from selected columns."""
    records = []
    # Sample from a mix of legislation and doctrine columns
    sample_columns = [
        (99, 4186, "patent_dept_rule", "legislation"),      # 87 records
        (66, 669, "policy_interpretation", "doctrine"),       # 249 records
        (74, 17035, "announcement", "doctrine"),              # 755 records
        (95, 4186, "trademark_law", "legislation"),           # 5 records
        (2432, 669, "admin_decision", "doctrine"),            # 130 PDFs
    ]

    per_column = max(3, count // len(sample_columns))

    for col_id, unit_id, category, data_type in sample_columns:
        logger.info(f"Fetching {category} (column {col_id})...")
        try:
            listing, total = fetch_column_listing(col_id, unit_id, page=1, per_page=per_column)
            logger.info(f"  Total {category}: {total:,}")
        except Exception as e:
            logger.error(f"  Failed to fetch {category}: {e}")
            continue

        for item in listing[:per_column]:
            if item["is_pdf"]:
                text = fetch_pdf_text(item["url"])
            else:
                text = fetch_article_text(item["url"])

            if len(text) < 50:
                logger.warning(f"  Skipped {item['doc_id']} - text too short ({len(text)} chars)")
                continue

            normalized = normalize(item, category, data_type, text)
            records.append(normalized)
            logger.info(f"  [{len(records)}] {item['title'][:50]}... ({len(text)} chars)")

            time.sleep(1.5)

            if len(records) >= count:
                break

        time.sleep(2)
        if len(records) >= count:
            break

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all documents from all columns."""
    total_yielded = 0

    for col_id, unit_id, category, data_type in COLUMNS:
        logger.info(f"\n=== Column: {category} (col={col_id}) ===")
        page = 1
        per_page = 100
        column_count = 0

        # Get total
        _, total = fetch_column_listing(col_id, unit_id, page=1, per_page=1)
        logger.info(f"Total {category}: {total:,}")

        while True:
            try:
                listing, _ = fetch_column_listing(col_id, unit_id, page=page, per_page=per_page)
            except Exception as e:
                logger.error(f"Failed page {page} of {category}: {e}")
                break

            if not listing:
                break

            for item in listing:
                if item["is_pdf"]:
                    text = fetch_pdf_text(item["url"])
                else:
                    text = fetch_article_text(item["url"])

                if len(text) < 50:
                    continue

                normalized = normalize(item, category, data_type, text)
                total_yielded += 1
                column_count += 1

                if total_yielded % 50 == 0:
                    logger.info(f"  Processed {total_yielded} total ({column_count} in {category}, page {page})")

                yield normalized
                time.sleep(1.5)

            page += 1
            time.sleep(2)

        logger.info(f"  {category}: {column_count} records")


def test_api():
    """Test API connectivity for all columns."""
    logger.info("Testing CNIPA dataproxy API...")
    all_ok = True

    for col_id, unit_id, category, data_type in COLUMNS:
        try:
            listing, total = fetch_column_listing(col_id, unit_id, page=1, per_page=1)
            if listing:
                logger.info(f"  {category} (col={col_id}): {total:,} docs - '{listing[0]['title'][:50]}'")
            else:
                logger.warning(f"  {category} (col={col_id}): {total} docs but empty listing")
                all_ok = False
        except Exception as e:
            logger.error(f"  {category} (col={col_id}): FAILED - {e}")
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
        safe_id = re.sub(r'[^\w-]', '_', record["doc_id"])[:40]
        filename = f"sample_{i:02d}_{record['category']}_{safe_id}.json"
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
    parser = argparse.ArgumentParser(description="CN/CNIPA IP Administration Fetcher")
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
                safe_id = re.sub(r'[^\w-]', '_', record["doc_id"])[:40]
                filepath = SAMPLE_DIR / f"record_{record['category']}_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
