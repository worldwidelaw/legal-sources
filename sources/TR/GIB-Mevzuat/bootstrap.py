#!/usr/bin/env python3
"""
TR/GIB-Mevzuat -- Turkish Revenue Administration Legislation

Fetches tax circulars, communiques, general letters, internal circulars,
presidential decrees, council decisions, regulations, and justifications
from GİB's Spring Boot REST API.

API base: https://gib.gov.tr/api/gibportal/mevzuat/
No authentication required. Paginated POST endpoints.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import argparse
import json
import logging
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("TR.GIB-Mevzuat")

SOURCE_ID = "TR/GIB-Mevzuat"
API_BASE = "https://gib.gov.tr/api/gibportal/mevzuat"
PAGE_SIZE = 50
REQUEST_DELAY = 1.0

# Document types to fetch (excluding ozelge, covered by GIB-Ozelgeler)
DOC_TYPES = [
    {"key": "sirkuler", "label": "Sirküler (Circulars)", "date_field": "sirkulerTarih", "number_field": "sirkulerNo"},
    {"key": "teblig", "label": "Tebliğ (Communiques)", "date_field": "tebligTarih", "number_field": "tebligNo"},
    {"key": "genelYazilar", "label": "Genel Yazılar (General Letters)", "date_field": "genelYaziTarih", "number_field": "genelYaziNo"},
    {"key": "icGenelge", "label": "İç Genelge (Internal Circulars)", "date_field": "icGenelgeTarih", "number_field": "icGenelgeNo"},
    {"key": "cbk", "label": "CBK (Presidential Decrees)", "date_field": "cbkTarih", "number_field": "cbkNo"},
    {"key": "bkk", "label": "BKK (Council Decisions)", "date_field": "bkkTarih", "number_field": "bkkNo"},
    {"key": "yonetmelikler", "label": "Yönetmelik (Regulations)", "date_field": "yonetmelikTarih", "number_field": "yonetmelikNo"},
    {"key": "gerekce", "label": "Gerekçe (Justifications)", "date_field": "gerekceTarih", "number_field": "gerekceNo"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# SSL context to handle GIB cert issues
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


class HTMLStripper(HTMLParser):
    """Strip HTML tags and decode entities."""
    def __init__(self):
        super().__init__()
        self.result = []
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip = True
        elif tag in ("br", "p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
            self.result.append("\n")
        elif tag == "td":
            self.result.append("\t")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip = False
        elif tag == "p":
            self.result.append("\n")

    def handle_data(self, data):
        if not self.skip:
            self.result.append(data)

    def get_text(self):
        return re.sub(r'\n{3,}', '\n\n', ''.join(self.result)).strip()


def strip_html(html: str) -> str:
    """Convert HTML to plain text."""
    if not html:
        return ""
    stripper = HTMLStripper()
    try:
        stripper.feed(html)
        return stripper.get_text()
    except Exception:
        return re.sub(r'<[^>]+>', ' ', html).strip()


def http_post(url: str, body: dict, retries: int = 3) -> Optional[dict]:
    """Make HTTP POST request with JSON body and retries."""
    data = json.dumps(body).encode("utf-8")
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
            resp = urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
            code = getattr(e, "code", None)
            if code and code < 500:
                logger.warning(f"Client error {code} for {url}")
                return None
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


def http_get_json(url: str, retries: int = 3) -> Optional[dict]:
    """Make HTTP GET request returning JSON."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            resp = urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
            code = getattr(e, "code", None)
            if code and code < 500:
                logger.warning(f"Client error {code} for {url}")
                return None
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


def fetch_doc_type(doc_type: dict, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all documents of a given type from the GIB API."""
    key = doc_type["key"]
    label = doc_type["label"]
    url = f"{API_BASE}/{key}/list"
    body = {"status": 2, "deleted": False}

    page = 0
    total = None
    fetched = 0
    max_records = 15 if sample else 999999

    while True:
        page_url = f"{url}?page={page}&size={PAGE_SIZE}"
        result = http_post(page_url, body)
        if not result or result.get("status") != 200:
            logger.error(f"Failed to fetch {label} page {page}: {result}")
            break

        container = result.get("resultContainer", {})
        if total is None:
            total = container.get("totalElements", 0)
            logger.info(f"{label}: {total} total documents")

        items = container.get("content", [])
        if not items:
            break

        for item in items:
            yield {**item, "_doc_type": key}
            fetched += 1
            if fetched >= max_records:
                return

        if container.get("last", True):
            break

        page += 1
        time.sleep(REQUEST_DELAY)


def normalize(raw: dict, doc_type: dict) -> dict:
    """Normalize a raw GIB document into standard format."""
    key = doc_type["key"]
    date_field = doc_type["date_field"]
    number_field = doc_type["number_field"]

    # Extract full text from description field (HTML)
    description = raw.get("description", "") or ""
    text = strip_html(description)

    # Extract date
    raw_date = raw.get(date_field) or raw.get("ts") or ""
    date = None
    if raw_date:
        try:
            # Handle ISO datetime format
            if "T" in str(raw_date):
                date = str(raw_date)[:10]
            else:
                date = str(raw_date)[:10]
        except Exception:
            date = None

    doc_id = raw.get("id", "")
    doc_number = raw.get(number_field, "")
    title = raw.get("title", "") or ""
    site_link = raw.get("siteLink", "") or f"https://gib.gov.tr/mevzuat/{key}/{doc_id}"

    # Related law info
    kanun_title = raw.get("kanunTitle", "")
    kanun_no = raw.get("kanunNo", "")

    return {
        "_id": f"TR-GIB-{key}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": site_link,
        "doc_type": key,
        "doc_number": str(doc_number) if doc_number else None,
        "related_law": kanun_title if kanun_title else None,
        "related_law_number": str(kanun_no) if kanun_no else None,
        "language": "tr",
    }


def test_connectivity():
    """Quick connectivity test."""
    logger.info("Testing GIB API connectivity...")
    for dt in DOC_TYPES[:3]:
        url = f"{API_BASE}/{dt['key']}/list?page=0&size=1"
        result = http_post(url, {"status": 2, "deleted": False})
        if result and result.get("status") == 200:
            total = result.get("resultContainer", {}).get("totalElements", 0)
            logger.info(f"  {dt['label']}: {total} documents - OK")
        else:
            logger.error(f"  {dt['label']}: FAILED - {result}")
            return False
    logger.info("Connectivity test passed!")
    return True


def bootstrap(sample: bool = False):
    """Run full or sample bootstrap."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    total_records = 0
    total_with_text = 0
    records_per_type = {}

    for doc_type in DOC_TYPES:
        key = doc_type["key"]
        type_count = 0
        logger.info(f"Fetching {doc_type['label']}...")

        for raw in fetch_doc_type(doc_type, sample=sample):
            record = normalize(raw, doc_type)

            text = record.get("text", "")
            # Skip records with no real text (PDF-only with just "click here" messages)
            if len(text) < 100 or "tıklayınız" in text.lower():
                logger.debug(f"  Skipping {record['_id']}: PDF-only or no text content ({len(text)} chars)")
                continue

            # Save to sample directory
            filename = f"{record['_id'].replace('/', '_')}.json"
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            total_records += 1
            type_count += 1
            if len(record.get("text", "")) > 50:
                total_with_text += 1

        records_per_type[key] = type_count
        logger.info(f"  {doc_type['label']}: {type_count} records saved")

    logger.info(f"\n=== Bootstrap Summary ===")
    logger.info(f"Total records: {total_records}")
    logger.info(f"Records with substantial text: {total_with_text}")
    for key, count in records_per_type.items():
        logger.info(f"  {key}: {count}")
    logger.info(f"Sample directory: {sample_dir}")

    return total_records


def main():
    parser = argparse.ArgumentParser(description="TR/GIB-Mevzuat bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        logger.info(f"Bootstrap complete: {count} records")


if __name__ == "__main__":
    main()
