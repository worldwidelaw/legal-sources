#!/usr/bin/env python3
"""
AR/MPF-Dictamenes -- Ministerio Público Fiscal Dictámenes Data Fetcher

Fetches Argentine Attorney General (Procurador General) opinions from the
MPF search portal. Full text is extracted from downloadable PDFs.

Data source: https://www.mpf.gob.ar/buscador-dictamenes/
License: Public domain (official government legal opinions)

Strategy:
  - Scrape HTML search results by year (paginated with ?pag=N&cant=10)
  - Extract title, sumario (summary), and PDF link from each result
  - Download PDF and extract full text via pdfplumber/pypdf
  - 55,000+ dictamenes from 1919 to present

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote, urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "AR/MPF-Dictamenes"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.MPF-Dictamenes")

BASE_URL = "https://www.mpf.gob.ar"
SEARCH_URL = f"{BASE_URL}/buscador-dictamenes/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.5",
}

# Month names in Spanish for date parsing
MONTH_MAP = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber, falling back to pypdf."""
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text += t + "\n"
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        if text.strip():
            return clean_text(text)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")

    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            t = page.extract_text()
            if t:
                text += t + "\n"
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        if text.strip():
            return clean_text(text)
    except Exception as e:
        logger.debug(f"pypdf failed: {e}")

    return ""


def clean_text(text: str) -> str:
    """Clean extracted text: normalize whitespace, remove artifacts."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_pdf_path(pdf_path: str) -> dict:
    """Extract metadata from PDF path like /dictamenes/2024\\LMonti\\diciembre\\file.pdf"""
    info = {"year": None, "prosecutor": None, "month": None, "filename": None}
    # Normalize backslashes
    pdf_path = pdf_path.replace("\\", "/")
    parts = pdf_path.strip("/").split("/")
    # Expected: dictamenes/YEAR/AUTHOR/MONTH/FILENAME.pdf
    if len(parts) >= 5:
        info["year"] = parts[1] if parts[1].isdigit() else None
        info["prosecutor"] = parts[2]
        info["month"] = parts[3].lower()
        info["filename"] = parts[4]
    elif len(parts) >= 2:
        info["filename"] = parts[-1]
    return info


def parse_search_page(html: str) -> list:
    """Parse search results page and extract dictamen entries."""
    results = []
    # Split by dictamenes-row markers
    rows = html.split('resoluciones dictamenes-row')

    for row_html in rows[1:]:  # Skip first split (before first row)
        entry = {}

        # Extract title
        title_match = re.search(
            r'<div class="divDicRepTitulo"[^>]*>(.*?)</div>',
            row_html, re.DOTALL
        )
        if title_match:
            entry["title"] = clean_text(title_match.group(1))

        # Extract sumario
        sumario_match = re.search(
            r'<div class="divDicRepTextoSumario dictamenes-sumario"[^>]*>(.*?)</div>',
            row_html, re.DOTALL
        )
        if sumario_match:
            entry["sumario"] = clean_text(sumario_match.group(1))

        # Extract PDF link
        pdf_match = re.search(r'href="(/dictamenes/[^"]+\.pdf)"', row_html)
        if pdf_match:
            entry["pdf_path"] = pdf_match.group(1)
            entry["pdf_url"] = BASE_URL + pdf_match.group(1).replace("\\", "/")

        # Extract references
        ref_match = re.search(
            r'<div class="divDicRepReferencias">(.*?)</div>',
            row_html, re.DOTALL
        )
        if ref_match:
            entry["references"] = clean_text(ref_match.group(1))

        if entry.get("pdf_path") or entry.get("title"):
            results.append(entry)

    return results


def get_total_pages(html: str) -> int:
    """Extract max page number from pagination."""
    pages = re.findall(r'data-pag="(\d+)"', html)
    if pages:
        return max(int(p) for p in pages)
    return 0


def fetch_search_page(year: int, page: int = 0, timeout: int = 30) -> str:
    """Fetch a search results page for a given year and page number."""
    params = {
        "texto": "",
        "anio": str(year),
        "pag": str(page),
        "cant": "10",
    }
    try:
        response = requests.get(
            SEARCH_URL, params=params, headers=HEADERS, timeout=timeout
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Failed to fetch page {page} for year {year}: {e}")
        return ""


def download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download a PDF file and return its bytes."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        if len(response.content) < 100:
            logger.warning(f"PDF too small ({len(response.content)} bytes): {url}")
            return None
        return response.content
    except requests.RequestException as e:
        logger.warning(f"Failed to download PDF {url}: {e}")
        return None


def normalize(raw: dict) -> dict:
    """Transform raw dictamen data into standard schema."""
    pdf_info = parse_pdf_path(raw.get("pdf_path", ""))
    filename = pdf_info.get("filename", "")
    pdf_id = filename.replace(".pdf", "") if filename else raw.get("title", "")[:80]

    # Build date from year/month
    date = None
    if pdf_info.get("year"):
        month_num = MONTH_MAP.get(pdf_info.get("month", ""), "01")
        date = f"{pdf_info['year']}-{month_num}-01"

    # Prosecutor name cleanup
    prosecutor = pdf_info.get("prosecutor", "")
    if prosecutor:
        # Convert camelCase like "LMonti" to "L. Monti"
        prosecutor = re.sub(r'([a-z])([A-Z])', r'\1 \2', prosecutor)
        if len(prosecutor) > 1 and prosecutor[0].isupper() and prosecutor[1].isupper():
            prosecutor = prosecutor[0] + ". " + prosecutor[1:]

    pdf_url = raw.get("pdf_url", "")

    return {
        "_id": pdf_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", f"Dictamen {pdf_id}"),
        "text": raw.get("text", ""),
        "sumario": raw.get("sumario", ""),
        "date": date,
        "year": pdf_info.get("year"),
        "month": pdf_info.get("month"),
        "prosecutor": prosecutor,
        "references": raw.get("references", ""),
        "url": pdf_url,
        "pdf_filename": filename,
    }


def fetch_year(year: int, max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all dictamenes for a given year."""
    logger.info(f"Fetching dictamenes for year {year}...")

    # Get first page to determine total pages
    html = fetch_search_page(year, page=0)
    if not html:
        return

    total_pages = get_total_pages(html)
    logger.info(f"  Year {year}: {total_pages + 1} pages")

    records_yielded = 0
    for page_num in range(0, total_pages + 1):
        if max_records and records_yielded >= max_records:
            break

        if page_num > 0:
            time.sleep(1.5)  # Rate limit
            html = fetch_search_page(year, page=page_num)
            if not html:
                continue

        entries = parse_search_page(html)
        if not entries:
            logger.info(f"  No entries on page {page_num}, stopping year {year}")
            break

        for entry in entries:
            if max_records and records_yielded >= max_records:
                break

            # Download PDF and extract full text
            if entry.get("pdf_url"):
                time.sleep(1.0)  # Rate limit
                pdf_bytes = download_pdf(entry["pdf_url"])
                if pdf_bytes:
                    entry["text"] = extract_pdf_text(pdf_bytes)
                    if entry["text"]:
                        logger.info(
                            f"  [{records_yielded + 1}] {entry.get('title', 'N/A')[:60]}... "
                            f"({len(entry['text'])} chars)"
                        )

            normalized = normalize(entry)
            if normalized.get("text") and len(normalized["text"]) > 100:
                yield normalized
                records_yielded += 1
            else:
                logger.warning(
                    f"  Skipping (no/short text): {entry.get('title', 'N/A')[:60]}"
                )


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents for validation from recent years."""
    records = []
    # Sample from recent years to get good variety
    years = [2024, 2023, 2022]

    for year in years:
        remaining = count - len(records)
        if remaining <= 0:
            break

        for record in fetch_year(year, max_records=remaining):
            records.append(record)
            if len(records) >= count:
                break

    return records


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all dictamenes, newest first."""
    current_year = datetime.now().year
    total = 0

    for year in range(current_year, 1918, -1):
        for record in fetch_year(year):
            yield record
            total += 1
            if max_records and total >= max_records:
                return

    logger.info(f"Total records fetched: {total}")


def test_api():
    """Test connectivity to the MPF dictamenes search."""
    logger.info("Testing MPF Dictamenes connectivity...")

    try:
        html = fetch_search_page(2024, page=0)
        if not html:
            logger.error("Failed to fetch search page")
            return False

        entries = parse_search_page(html)
        logger.info(f"Search OK - Found {len(entries)} entries on page 0 for 2024")

        total_pages = get_total_pages(html)
        logger.info(f"Total pages for 2024: {total_pages + 1}")

        if entries and entries[0].get("pdf_url"):
            pdf_bytes = download_pdf(entries[0]["pdf_url"])
            if pdf_bytes:
                text = extract_pdf_text(pdf_bytes)
                logger.info(
                    f"PDF download + extraction OK - {len(text)} chars from "
                    f"{entries[0].get('pdf_url', 'N/A')}"
                )
                return True
            else:
                logger.warning("PDF download failed")
                return False

        logger.warning("No PDF links found")
        return False

    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AR/MPF-Dictamenes Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=15,
        help="Number of sample records (default: 15)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum records to fetch",
    )

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            records = fetch_sample(count=args.sample_size)
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

            for i, record in enumerate(records, 1):
                safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:60]
                filename = f"sample_{i:02d}_{safe_id}.json"
                filepath = SAMPLE_DIR / filename
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"\nSample complete: {len(records)} records saved to {SAMPLE_DIR}/")

            # Print summary
            if records:
                text_lengths = [len(r.get("text", "")) for r in records]
                avg_len = sum(text_lengths) / len(text_lengths)
                logger.info(f"Avg text length: {avg_len:.0f} chars")
                logger.info(f"Min text length: {min(text_lengths)} chars")
                logger.info(f"Max text length: {max(text_lengths)} chars")
        else:
            count = 0
            for record in fetch_all(max_records=args.max_records):
                print(json.dumps(record, ensure_ascii=False))
                count += 1
            logger.info(f"\nBootstrap complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
