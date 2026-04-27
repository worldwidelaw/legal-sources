#!/usr/bin/env python3
"""
PY/LeyesParaguayas — Paraguay Leyes Paraguayas (BACN)

Fetches Paraguayan legislation from the Biblioteca y Archivo Central del
Congreso Nacional (BACN).

Strategy:
  - AJAX pagination endpoint lists all ~7,054 laws across 706 pages
  - Detail pages contain full text as HTML in <div class="entry-content">
  - Spanish language

Source: https://www.bacn.gov.py/
Rate limit: 1.5 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Connectivity test
"""

import sys
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

SOURCE_ID = "PY/LeyesParaguayas"
PAGINATION_URL = "https://www.bacn.gov.py/paginacion/leyes-paraguayas.php"
DETAIL_URL = "https://www.bacn.gov.py/leyes-paraguayas/{id}/x"
SAMPLE_DIR = Path(__file__).parent / "sample"
RATE_LIMIT = 1.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("legal-data-hunter.PY.LeyesParaguayas")

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
}

session = requests.Session()
session.headers.update(HEADERS)


def clean_html(html_str: str) -> str:
    """Strip HTML tags and decode entities."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    for tag in soup(["script", "style", "iframe"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_listing_page(page: int) -> list:
    """Fetch a page of law IDs from the AJAX pagination endpoint."""
    resp = session.post(
        PAGINATION_URL,
        data={"action": "ajax", "page": page},
        headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    time.sleep(RATE_LIMIT)

    soup = BeautifulSoup(resp.text, "html.parser")
    items = []

    # Each article has a link like /leyes-paraguayas/{id}/{slug}
    for link in soup.find_all("a", href=True):
        href = link["href"]
        match = re.search(r"/leyes-paraguayas/(\d+)/", href)
        if match:
            law_id = int(match.group(1))
            title = link.get_text(strip=True)
            if law_id not in [i["id"] for i in items]:
                items.append({"id": law_id, "title": title})

    return items


def fetch_detail(law_id: int) -> dict:
    """Fetch the full detail page for a law."""
    url = DETAIL_URL.format(id=law_id)
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT)

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title from <title> tag
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""
    # Remove site suffix
    title = re.sub(r"\s*[|\-]\s*BACN.*$", "", title).strip()

    # Full text from entry-content div
    content_div = soup.find("div", class_="entry-content")
    text = ""
    if content_div:
        # Remove download/audio elements
        for el in content_div.find_all(["iframe", "script"]):
            el.decompose()
        text = content_div.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Strip metadata prefix (dates, download links) before law body
        text = re.sub(
            r"^.*?Descargar Archivo:.*?\n(?:\S+.*?[MK]B\)\s*\n)?",
            "", text, count=1, flags=re.DOTALL,
        )
        # Remove remaining download size artifacts like "Ley Nº 7292\n(343.59 KB)\n"
        text = re.sub(r"^[^\n]*\n\(\d+[\.,]?\d*\s*[KMG]B\)\s*\n", "", text)

    # Try to extract dates
    promulgation_date = None
    page_text = soup.get_text()

    # Format on page: "Fecha de Promulgación:\nDD-MM-YYYY"
    date_match = re.search(r"Fecha de Promulgaci[oó]n:\s*(\d{1,2})-(\d{1,2})-(\d{4})", page_text)
    if date_match:
        day, month, year = date_match.groups()
        promulgation_date = f"{year}-{int(month):02d}-{int(day):02d}"

    if not promulgation_date:
        # Fallback: "Promulgada: DD de MES de YYYY"
        date_match = re.search(r"Promulgad[ao]:\s*(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4})", page_text)
        if date_match:
            day, month_str, year = date_match.groups()
            months = {
                "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
                "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
                "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
            }
            month = months.get(month_str.lower(), "01")
            promulgation_date = f"{year}-{month}-{int(day):02d}"

    # Extract law number from title
    law_number = None
    num_match = re.search(r"(?:Ley|LEY)\s+(?:N[°ºo.]?\s*)?(\d[\d./]*)", title, re.IGNORECASE)
    if num_match:
        law_number = num_match.group(1)

    return {
        "title": title,
        "text": text,
        "date": promulgation_date,
        "law_number": law_number,
        "url": f"https://www.bacn.gov.py/leyes-paraguayas/{law_id}/",
    }


def normalize(law_id: int, detail: dict) -> dict:
    """Transform raw law data into the standard schema."""
    return {
        "_id": f"PY-ley-{law_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": detail["title"],
        "text": detail["text"],
        "date": detail["date"],
        "url": detail["url"],
        "legislation_number": detail["law_number"],
        "language": "es",
        "bacn_id": law_id,
    }


def fetch_all(sample: bool = False):
    """Yield all normalized legislation records."""
    max_pages = 706
    if sample:
        # Fetch from pages 1, 350, 706 to get diverse samples
        pages_to_fetch = [1, 350, 706]
    else:
        pages_to_fetch = range(1, max_pages + 1)

    total_fetched = 0
    for page in pages_to_fetch:
        log.info(f"Fetching listing page {page}...")
        try:
            items = fetch_listing_page(page)
        except Exception as e:
            log.error(f"Error on page {page}: {e}")
            continue

        if not items:
            log.info(f"  No items on page {page}, stopping")
            if not sample:
                break
            continue

        for item in items:
            law_id = item["id"]
            try:
                detail = fetch_detail(law_id)
                if detail["text"] and len(detail["text"]) > 50:
                    rec = normalize(law_id, detail)
                    yield rec
                    total_fetched += 1
                    if total_fetched % 50 == 0:
                        log.info(f"Progress: {total_fetched} records")
                    if sample and total_fetched >= 15:
                        return
                else:
                    log.warning(f"  No text for law ID {law_id}")
            except Exception as e:
                log.error(f"  Error fetching detail for ID {law_id}: {e}")


def test_connection():
    """Test API connectivity."""
    log.info("Testing BACN Paraguay...")

    # Test pagination endpoint
    log.info("Testing pagination (page 1)...")
    items = fetch_listing_page(1)
    log.info(f"  Found {len(items)} items on page 1")
    for item in items[:3]:
        log.info(f"  ID {item['id']}: {item['title'][:80]}")

    # Test detail page
    if items:
        law_id = items[0]["id"]
        log.info(f"\nFetching detail for ID {law_id}...")
        detail = fetch_detail(law_id)
        log.info(f"  Title: {detail['title'][:100]}")
        log.info(f"  Text length: {len(detail['text'])} chars")
        log.info(f"  Date: {detail['date']}")
        log.info(f"  Law number: {detail['law_number']}")
        log.info(f"  Text preview: {detail['text'][:200]}...")

    log.info("\nTest complete!")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    for rec in fetch_all(sample=sample):
        records.append(rec)

    log.info(f"Total records with text: {len(records)}")

    if sample:
        to_save = sorted(records, key=lambda r: len(r.get("text", "")), reverse=True)[:15]
    else:
        to_save = records

    saved = 0
    for rec in to_save:
        safe_id = re.sub(r"[^\w\-]", "_", rec["_id"])
        path = SAMPLE_DIR / f"{safe_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1

    log.info(f"Saved {saved} records to {SAMPLE_DIR}")

    has_text = sum(1 for r in to_save if r.get("text") and len(r["text"]) > 100)
    log.info(f"Records with substantial text: {has_text}/{saved}")

    if to_save:
        avg_len = sum(len(r.get("text", "")) for r in to_save) // len(to_save)
        log.info(f"Average text length: {avg_len} chars")

    return saved


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PY/LeyesParaguayas bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        test_connection()
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            print("ERROR: No records fetched", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
