#!/usr/bin/env python3
"""
AR/NormasGBA -- Buenos Aires Province Normative System Data Fetcher

Fetches legislation from normas.gba.gob.ar (Sistema de Información Normativa y Documental).

Strategy:
  - Search by document type + weekly date ranges to stay under 190-result cap
  - Extract document URLs from search result pages
  - Fetch each document detail page for metadata + full text HTML link
  - Download and clean full text HTML

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urlencode, urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "AR/NormasGBA"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.NormasGBA")

BASE_URL = "https://normas.gba.gob.ar"
SEARCH_URL = f"{BASE_URL}/resultados"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

DOCUMENT_TYPES = [
    "Law",
    "DecreeLaw",
    "Decree",
    "Resolution",
    "Disposition",
    "GeneralOrdinance",
    "JointResolution",
]

TYPE_LABELS = {
    "Law": "Ley",
    "DecreeLaw": "Decreto-ley",
    "Decree": "Decreto",
    "Resolution": "Resolución",
    "Disposition": "Disposición",
    "GeneralOrdinance": "Ordenanza General",
    "JointResolution": "Resolución Conjunta",
}

session = requests.Session()
session.headers.update(HEADERS)


def clean_html(html: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<footer[^>]*>.*?</footer>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def search_documents(doc_type: str, date_from: str, date_to: str, page: int = 1) -> str:
    """Search normas.gba.gob.ar with filters. Returns HTML response."""
    params = {
        "q[terms][raw_type]": doc_type,
        "q[date_ranges][publication_date][gte]": date_from,
        "q[date_ranges][publication_date][lte]": date_to,
        "q[sort]": "by_publication_date_desc",
        "page": page,
    }
    url = f"{SEARCH_URL}?{urlencode(params)}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_result_urls(html: str) -> list[str]:
    """Extract document detail URLs from search results HTML."""
    # Pattern: /ar-b/{type}/{year}/{number}/{hash}
    pattern = r'href="(/ar-b/[^"]+)"'
    urls = re.findall(pattern, html)
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def count_results(html: str) -> int:
    """Extract total result count from search page."""
    match = re.search(r'(\d+)\s+resultado', html)
    if match:
        return int(match.group(1))
    return 0


def extract_document_metadata(html: str, url: str) -> dict:
    """Extract metadata from a document detail page."""
    meta = {"url": url}

    # Title
    title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    if title_match:
        meta["title"] = clean_html(title_match.group(1)).strip()

    # Summary / description
    summary_match = re.search(r'<blockquote[^>]*>(.*?)</blockquote>', html, re.DOTALL)
    if summary_match:
        meta["summary"] = clean_html(summary_match.group(1)).strip()

    # Publication date
    pub_match = re.search(r'Fecha de publicación[^<]*<[^>]*>([^<]+)', html)
    if pub_match:
        meta["publication_date"] = pub_match.group(1).strip()

    # Promulgation date
    prom_match = re.search(r'Fecha de promulgación[^<]*<[^>]*>([^<]+)', html)
    if prom_match:
        meta["promulgation_date"] = prom_match.group(1).strip()

    # Boletin Oficial number
    bo_match = re.search(r'Boletín Oficial[^<]*<[^>]*>([^<]+)', html)
    if bo_match:
        meta["boletin_oficial"] = bo_match.group(1).strip()

    # Full text HTML links - look for /documentos/ links
    doc_links = re.findall(r'href="(/documentos/[^"]+\.html)"', html)
    meta["document_links"] = doc_links

    # PDF links
    pdf_links = re.findall(r'href="(/documentos/[^"]+\.pdf)"', html)
    meta["pdf_links"] = pdf_links

    return meta


def fetch_full_text(doc_path: str) -> Optional[str]:
    """Fetch full text from a /documentos/*.html path."""
    url = urljoin(BASE_URL, doc_path)
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.text

        # Try to extract body content
        body_match = re.search(r'<body[^>]*>(.*?)</body>', content, re.DOTALL | re.IGNORECASE)
        if body_match:
            return clean_html(body_match.group(1))
        return clean_html(content)
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch full text from {url}: {e}")
        return None


def parse_date_ar(date_str: str) -> Optional[str]:
    """Parse Argentine date format (DD/MM/YYYY) to ISO 8601."""
    if not date_str:
        return None
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_type_and_number(url_path: str) -> tuple[str, str, str]:
    """Extract document type, year, number from URL path like /ar-b/ley/2024/15513/xxx."""
    parts = url_path.strip("/").split("/")
    if len(parts) >= 4:
        return parts[1], parts[2], parts[3]
    return "", "", ""


def normalize(meta: dict, full_text: str) -> dict:
    """Transform scraped data to standard schema."""
    url_path = meta.get("url", "")
    doc_type, year, number = extract_type_and_number(url_path)

    title = meta.get("title", "")
    if not title:
        label = doc_type.replace("-", " ").title()
        title = f"{label} {number}/{year}" if number and year else url_path

    date = parse_date_ar(meta.get("publication_date", ""))
    if not date:
        date = parse_date_ar(meta.get("promulgation_date", ""))
    if not date and year:
        date = f"{year}-01-01"

    doc_id = f"ar-b-{doc_type}-{year}-{number}".lower()
    doc_id = re.sub(r'[^a-z0-9-]', '-', doc_id)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": urljoin(BASE_URL, url_path) if url_path.startswith("/") else url_path,
        "document_type": doc_type,
        "number": number,
        "year": year,
        "summary": meta.get("summary", ""),
        "boletin_oficial": meta.get("boletin_oficial", ""),
    }


def fetch_document(detail_path: str) -> Optional[dict]:
    """Fetch a single document: detail page + full text."""
    url = urljoin(BASE_URL, detail_path)
    try:
        time.sleep(1.5)
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        meta = extract_document_metadata(resp.text, detail_path)

        # Get full text from first HTML document link
        full_text = ""
        for doc_link in meta.get("document_links", []):
            time.sleep(1.0)
            text = fetch_full_text(doc_link)
            if text and len(text) > 50:
                full_text = text
                break

        if not full_text:
            logger.warning(f"No full text found for {detail_path}")
            return None

        return normalize(meta, full_text)
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch document {detail_path}: {e}")
        return None


def generate_weekly_ranges(start_year: int, end_year: int) -> list[tuple[str, str]]:
    """Generate weekly date ranges for querying."""
    ranges = []
    current = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    while current <= end:
        week_end = min(current + timedelta(days=6), end)
        ranges.append((current.strftime("%d/%m/%Y"), week_end.strftime("%d/%m/%Y")))
        current = week_end + timedelta(days=1)
    return ranges


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents. If sample=True, fetch only ~15 records."""
    count = 0
    sample_limit = 15

    if sample:
        # For sample: search recent Laws only
        doc_types = ["Law"]
        ranges = generate_weekly_ranges(2024, 2024)[-8:]  # last 2 months of 2024
    else:
        doc_types = DOCUMENT_TYPES
        ranges = generate_weekly_ranges(1990, 2026)

    for doc_type in doc_types:
        logger.info(f"Searching type: {doc_type} ({TYPE_LABELS.get(doc_type, doc_type)})")
        for date_from, date_to in ranges:
            if sample and count >= sample_limit:
                return

            try:
                time.sleep(1.5)
                html = search_documents(doc_type, date_from, date_to, page=1)
                result_count = count_results(html)

                if result_count == 0:
                    continue

                logger.info(f"  {doc_type} {date_from}-{date_to}: {result_count} results")

                # Extract URLs from all pages
                all_urls = extract_result_urls(html)

                # Paginate if needed (max 19 pages)
                total_pages = min((result_count + 9) // 10, 19)
                for page in range(2, total_pages + 1):
                    time.sleep(1.5)
                    page_html = search_documents(doc_type, date_from, date_to, page=page)
                    all_urls.extend(extract_result_urls(page_html))

                if result_count > 190:
                    logger.warning(f"  WARNING: {result_count} results exceed 190 cap for {doc_type} {date_from}-{date_to}")

                # Fetch each document
                for detail_url in all_urls:
                    if sample and count >= sample_limit:
                        return

                    doc = fetch_document(detail_url)
                    if doc and doc.get("text"):
                        count += 1
                        yield doc
                        logger.info(f"  [{count}] {doc['title'][:80]} ({len(doc['text'])} chars)")

            except requests.RequestException as e:
                logger.warning(f"  Search failed for {doc_type} {date_from}-{date_to}: {e}")
                continue

    logger.info(f"Total documents fetched: {count}")


def save_sample(records: list[dict]) -> None:
    """Save sample records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")


def test_api() -> bool:
    """Test connectivity to normas.gba.gob.ar."""
    try:
        resp = session.get(BASE_URL, timeout=15)
        resp.raise_for_status()
        logger.info(f"Homepage: HTTP {resp.status_code}, {len(resp.text)} bytes")

        # Test a search
        time.sleep(1.5)
        html = search_documents("Law", "01/01/2024", "31/01/2024")
        n = count_results(html)
        urls = extract_result_urls(html)
        logger.info(f"Search test: {n} results, {len(urls)} detail URLs extracted")

        if urls:
            time.sleep(1.5)
            doc = fetch_document(urls[0])
            if doc:
                logger.info(f"Document test: '{doc['title'][:60]}' — {len(doc.get('text',''))} chars of text")
                return True
            else:
                logger.warning("Document test: failed to get full text")
                return False

        return n > 0
    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AR/NormasGBA data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        is_sample = args.sample or not args.full
        records = []
        for doc in fetch_all(sample=is_sample):
            records.append(doc)

        if records:
            save_sample(records)
            logger.info(f"Bootstrap complete: {len(records)} records")

            # Validate
            texts = [r for r in records if r.get("text") and len(r["text"]) > 50]
            logger.info(f"Records with full text: {len(texts)}/{len(records)}")
            if texts:
                avg_len = sum(len(r["text"]) for r in texts) // len(texts)
                logger.info(f"Average text length: {avg_len} chars")
        else:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
