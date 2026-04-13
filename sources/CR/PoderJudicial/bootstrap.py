#!/usr/bin/env python3
"""
CR/PoderJudicial -- Costa Rica Poder Judicial Jurisprudence Fetcher

Fetches case law decisions from Costa Rica's judiciary via the NexusPJ REST API
at nexuspj.poder-judicial.go.cr. Full text available directly in search results.

Covers all courts: Sala Constitucional (~674K), Sala Segunda (~135K), Sala Primera,
Sala Tercera, and 60+ tribunals. ~1.77M total documents.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CR/PoderJudicial"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CR.PoderJudicial")

API_BASE = "https://nexuspj.poder-judicial.go.cr"
SEARCH_URL = f"{API_BASE}/api/search"
DOCUMENT_URL = f"{API_BASE}/api/document"
DROPDOWN_URL = f"{API_BASE}/api/getDropDownData"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Referer": "https://nexuspj.poder-judicial.go.cr/",
    "Origin": "https://nexuspj.poder-judicial.go.cr",
    "Cookie": "x-bni-fpc=test123",
}

PAGE_SIZE = 50
REQUEST_DELAY = 1.5

# Major courts to iterate over for sampling
MAJOR_COURTS = [
    "Sala Constitucional",
    "Sala Segunda de la Corte",
    "Sala Primera de la Corte Suprema de Justicia",
    "Sala Tercera de la Corte Suprema de Justicia",
]


def api_search(query: str, page: int = 1, size: int = PAGE_SIZE,
               sort_field: str = "fecha", sort_order: str = "desc",
               retries: int = 3) -> dict:
    """Execute a search query against the NexusPJ API."""
    payload = {
        "q": query,
        "size": size,
        "page": page,
        "sort": {"field": sort_field, "order": sort_order},
        "facets": "",
        "advanced": False,
    }
    for attempt in range(retries):
        try:
            resp = requests.post(
                SEARCH_URL,
                json=payload,
                headers=HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("Search failed (attempt %d/%d): %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return {}


def api_document(doc_id: str, retries: int = 3) -> dict:
    """Fetch a single document by ID from the NexusPJ API."""
    payload = {"id": doc_id}
    for attempt in range(retries):
        try:
            resp = requests.post(
                DOCUMENT_URL,
                json=payload,
                headers=HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("Document fetch failed (attempt %d/%d): %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return {}


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def normalize(hit: dict) -> dict:
    """Transform a NexusPJ search hit into the standard schema."""
    doc_id = hit.get("idDocument", "")
    title = hit.get("title", "") or hit.get("numeroDocumento", "") or doc_id
    despacho = hit.get("despacho", "")
    expediente = hit.get("expediente", "")
    redactor = hit.get("redactor", "")
    tipo_doc = hit.get("tipoDocumento", "")
    descriptores = hit.get("descriptores", "")
    date_str = hit.get("date", "")

    # Normalize date to ISO 8601
    # API returns dates like "27-Dic-2024" (Spanish month abbreviations)
    SPANISH_MONTHS = {
        "Ene": "01", "Feb": "02", "Mar": "03", "Abr": "04",
        "May": "05", "Jun": "06", "Jul": "07", "Ago": "08",
        "Sep": "09", "Oct": "10", "Nov": "11", "Dic": "12",
    }
    if date_str:
        try:
            # Try "DD-Mon-YYYY" Spanish format first
            match = re.match(r'(\d{1,2})-(\w{3})-(\d{4})', date_str)
            if match:
                day, mon, year = match.groups()
                month_num = SPANISH_MONTHS.get(mon)
                if month_num:
                    date_str = f"{year}-{month_num}-{int(day):02d}"
                else:
                    date_str = None
            else:
                # Try ISO formats
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"]:
                    try:
                        dt = datetime.strptime(date_str.split(".")[0].split("+")[0], fmt)
                        date_str = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
                else:
                    date_str = None
        except (ValueError, AttributeError):
            date_str = None

    # Get full text from content field (available in search results)
    content = hit.get("content", "")
    text = clean_html(content) if "<" in content else content

    # Build a meaningful title if the raw title is generic
    if not title or title == doc_id:
        parts = []
        if hit.get("numeroDocumento"):
            parts.append(hit["numeroDocumento"])
        if despacho:
            parts.append(despacho)
        if date_str:
            parts.append(date_str)
        title = " - ".join(parts) if parts else doc_id

    url = f"https://nexuspj.poder-judicial.go.cr/document/{doc_id}"

    return {
        "_id": f"CR/PoderJudicial/{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": url,
        "despacho": despacho,
        "expediente": expediente,
        "redactor": redactor,
        "tipo_documento": tipo_doc,
        "descriptores": descriptores,
    }


def fetch_all(query: str = "*", max_docs: int = 0) -> Generator[dict, None, None]:
    """Yield all documents matching a query with full text."""
    page = 1
    yielded = 0

    while True:
        data = api_search(query, page=page)
        total = data.get("total", 0)
        hits = data.get("hits", [])

        if page == 1:
            logger.info("Query '%s': %d total results", query, total)

        if not hits:
            break

        for hit in hits:
            record = normalize(hit)
            text = record.get("text", "")

            # If search content is too short, try document endpoint
            if len(text) < 100:
                doc_id = hit.get("idDocument", "")
                if doc_id:
                    time.sleep(REQUEST_DELAY)
                    doc_data = api_document(doc_id)
                    html = doc_data.get("html", "")
                    if html:
                        text = clean_html(html)
                        record["text"] = text

            if not text or len(text) < 50:
                logger.debug("Skipping %s: no/short text (%d chars)",
                             record["_id"], len(text))
                continue

            yield record
            yielded += 1

            if max_docs and yielded >= max_docs:
                return

        if page * PAGE_SIZE >= total:
            break

        page += 1
        time.sleep(REQUEST_DELAY)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents modified since a date."""
    try:
        since_dt = datetime.fromisoformat(since)
        year = since_dt.year
    except ValueError:
        year = datetime.now().year - 1

    current_year = datetime.now().year
    for y in range(year, current_year + 1):
        query = f"anno:{y}"
        for record in fetch_all(query=query):
            if record.get("date") and record["date"] >= since:
                yield record


def save_sample(records: list) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    # Clean old samples
    for f in SAMPLE_DIR.glob("sample_*.json"):
        f.unlink()
    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"sample_{i:03d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("Saved sample %d: %s (text: %d chars)",
                     i, record["title"][:60], len(record.get("text", "")))


def test_api():
    """Test API connectivity and basic queries."""
    print("Testing dropdown data endpoint...")
    try:
        resp = requests.get(DROPDOWN_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        print(f"  OK: {json.dumps(data, ensure_ascii=False)[:300]}...")
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\nTesting search (Sala Constitucional, 2024)...")
    data = api_search('despacho.keyword:"Sala Constitucional" AND anno:2024', size=3)
    total = data.get("total", 0)
    hits = data.get("hits", [])
    print(f"  Total: {total}, returned: {len(hits)}")

    if hits:
        hit = hits[0]
        print(f"  First hit ID: {hit.get('idDocument', 'N/A')}")
        print(f"  Title: {hit.get('title', 'N/A')}")
        print(f"  Date: {hit.get('date', 'N/A')}")
        print(f"  Despacho: {hit.get('despacho', 'N/A')}")
        content = hit.get("content", "")
        print(f"  Content length: {len(content)} chars")
        if content:
            text = clean_html(content) if "<" in content else content
            print(f"  Text preview: {text[:300]}...")

        # Test document endpoint
        doc_id = hit.get("idDocument", "")
        if doc_id:
            print(f"\nTesting document endpoint for: {doc_id}")
            doc = api_document(doc_id)
            html = doc.get("html", "")
            print(f"  HTML length: {len(html)} chars")
            if html:
                text = clean_html(html)
                print(f"  Cleaned text: {len(text)} chars")
                print(f"  Preview: {text[:300]}...")

    print("\nTesting generic search (*) ...")
    data = api_search("*", size=2)
    print(f"  Total documents: {data.get('total', 'N/A')}")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    if sample:
        logger.info("Running in SAMPLE mode - fetching ~15 documents across courts")
        records = []
        sample_plans = [
            ('despacho.keyword:"Sala Constitucional" AND anno:2024', 5),
            ('despacho.keyword:"Sala Segunda de la Corte" AND anno:2023', 4),
            ('despacho.keyword:"Sala Primera de la Corte" AND anno:2023', 3),
            ('despacho.keyword:"Sala Tercera de la Corte" AND anno:2023', 3),
        ]
        for query, target in sample_plans:
            logger.info("Sampling: %s (target: %d)", query, target)
            count = 0
            for record in fetch_all(query=query, max_docs=target * 3):
                if len(record.get("text", "")) > 200:
                    records.append(record)
                    count += 1
                if count >= target:
                    break
            time.sleep(REQUEST_DELAY)

        if records:
            save_sample(records)
            with_text = [r for r in records if r.get("text") and len(r["text"]) > 100]
            print(f"\nSample complete: {len(records)} records, {len(with_text)} with full text")
            for r in records:
                text_len = len(r.get("text", ""))
                print(f"  [{r['despacho'][:30]}] {r['title'][:50]} - {text_len} chars")
        else:
            print("ERROR: No records fetched!")
            sys.exit(1)
    else:
        logger.info("Running FULL bootstrap")
        count = 0
        for record in fetch_all():
            count += 1
            if count % 100 == 0:
                logger.info("Processed %d documents", count)
        logger.info("Bootstrap complete: %d documents", count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CR/PoderJudicial data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample data")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)
