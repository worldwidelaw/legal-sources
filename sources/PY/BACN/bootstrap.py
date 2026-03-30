#!/usr/bin/env python3
"""
PY/BACN -- Biblioteca y Archivo Central del Congreso Nacional (Paraguay)

Fetches Paraguayan legislation from the BACN website.

Data source: https://www.bacn.gov.py/
License: Open Government Data (Paraguay)

Strategy:
  - Use the search endpoint to get all law IDs and titles
  - Fetch each law's HTML page for full text and metadata
  - Extract text from inline HTML content divs
  - Normalize to standard schema

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
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "PY/BACN"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PY.BACN")

# API Configuration
BASE_URL = "https://www.bacn.gov.py"
SEARCH_URL = f"{BASE_URL}/buscar.php?j=si&s=ley"
LAW_URL_TEMPLATE = f"{BASE_URL}/leyes-paraguayas/{{id}}/{{slug}}"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

RATE_LIMIT = 1.5  # seconds between requests


def clean_html(html: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)
    # Replace br/p/div with newlines for readability
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</(?:p|div|li|tr)>', '\n', html, flags=re.IGNORECASE)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', ' ', html)
    text = unescape(text)
    # Normalize whitespace within lines, preserve paragraph breaks
    lines = text.split('\n')
    cleaned = []
    for line in lines:
        line = re.sub(r'[ \t]+', ' ', line).strip()
        if line:
            cleaned.append(line)
    text = '\n'.join(cleaned)
    # Collapse excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def make_slug(title: str) -> str:
    """Create URL slug from title."""
    slug = title.lower()
    slug = re.sub(r'[áà]', 'a', slug)
    slug = re.sub(r'[éè]', 'e', slug)
    slug = re.sub(r'[íì]', 'i', slug)
    slug = re.sub(r'[óò]', 'o', slug)
    slug = re.sub(r'[úù]', 'u', slug)
    slug = re.sub(r'[ñ]', 'n', slug)
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug[:80]


def fetch_law_index() -> list:
    """Fetch all law entries from the search endpoint."""
    logger.info("Fetching law index from search endpoint...")
    response = requests.get(SEARCH_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()
    # Filter to only "Leyes Paraguayas" category
    laws = [
        item for item in data
        if item.get("nombre_categoria") == "Leyes Paraguayas"
    ]
    logger.info(f"Found {len(laws)} laws in index")
    return laws


def fetch_law_page(law_id: str, title: str) -> Optional[dict]:
    """
    Fetch a law page and extract full text and metadata.

    Returns dict with: text, date_promulgation, date_publication, pdf_urls
    """
    slug = make_slug(title)
    url = LAW_URL_TEMPLATE.format(id=law_id, slug=slug)

    try:
        response = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if response.status_code == 302 or response.url.endswith("/error"):
            # Try without slug
            url_no_slug = f"{BASE_URL}/leyes-paraguayas/{law_id}/ley"
            response = requests.get(url_no_slug, headers=HEADERS, timeout=30, allow_redirects=True)

        if response.status_code != 200 or response.url.endswith("/error"):
            logger.warning(f"Failed to fetch law {law_id}: HTTP {response.status_code}")
            return None

        html = response.text
        result = {"url": url}

        # Extract entry-content
        m = re.search(
            r'<div class="entry-content">(.*?)(?:<div class="col-sm-4"|</article>)',
            html, re.DOTALL
        )
        if not m:
            logger.warning(f"No entry-content found for law {law_id}")
            return None

        content = m.group(1)

        # Extract dates
        prom = re.findall(r'Fecha de Promulgación:\s*</strong>\s*([\d/-]+)', content)
        pub = re.findall(r'Fecha de Publicación:\s*</strong>\s*([\d/-]+)', content)
        result["date_promulgation"] = prom[0] if prom else None
        result["date_publication"] = pub[0] if pub else None

        # Extract PDF download links
        pdfs = re.findall(
            r'href="(https://www\.bacn\.gov\.py/descarga/[^"]+)"[^>]*title="([^"]*)"',
            content
        )
        result["pdf_urls"] = [(url, title) for url, title in pdfs]

        # Extract body text
        # The law text typically appears after a comment block or after download sections
        # It's in <div style="text-align: ..."> elements
        # Find the text after the metadata/download sections
        body = content

        # Remove the metadata row div
        body = re.sub(
            r'<div class="row">.*?</div>\s*</div>\s*</div>\s*</div>',
            '', body, count=1, flags=re.DOTALL
        )
        # Remove download links and iframes
        body = re.sub(r'<iframe[^>]*>.*?</iframe>', '', body, flags=re.DOTALL)
        body = re.sub(r'<hr><h4>.*?</h4><hr>', '', body, flags=re.DOTALL)
        body = re.sub(r'<hr>\s*<hr>', '', body)
        # Remove comment block
        body = re.sub(r'<!--.*?-->', '', body, flags=re.DOTALL)
        # Remove standalone hr tags
        body = re.sub(r'<hr\s*/?>', '', body)

        text = clean_html(body)
        result["text"] = text

        return result

    except requests.RequestException as e:
        logger.warning(f"Request failed for law {law_id}: {e}")
        return None


def parse_date(date_str: str) -> Optional[str]:
    """Convert DD-MM-YYYY or DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def normalize(law_id: str, title: str, page_data: dict) -> dict:
    """Transform raw law data to standard schema."""
    date = parse_date(page_data.get("date_promulgation")) or \
           parse_date(page_data.get("date_publication"))

    return {
        "_id": f"PY_BACN_{law_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": page_data.get("text", ""),
        "date": date,
        "url": page_data.get("url", f"{BASE_URL}/leyes-paraguayas/{law_id}"),
        "date_promulgation": parse_date(page_data.get("date_promulgation")),
        "date_publication": parse_date(page_data.get("date_publication")),
        "pdf_urls": [u for u, _ in page_data.get("pdf_urls", [])],
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all normalized law records."""
    laws = fetch_law_index()
    limit = 15 if sample else len(laws)

    logger.info(f"Fetching {'sample of ' + str(limit) if sample else 'all ' + str(len(laws))} laws...")

    fetched = 0
    skipped = 0
    for i, law in enumerate(laws[:limit * 2] if sample else laws):
        if fetched >= limit:
            break

        law_id = law["id_documento"]
        title = law["titulo_documento"]

        logger.info(f"[{fetched + 1}/{limit}] Fetching law {law_id}: {title[:60]}...")
        page_data = fetch_law_page(law_id, title)

        if not page_data or not page_data.get("text"):
            logger.warning(f"  No text content for law {law_id}, skipping")
            skipped += 1
            time.sleep(RATE_LIMIT)
            continue

        record = normalize(law_id, title, page_data)

        if len(record["text"]) < 50:
            logger.warning(f"  Text too short ({len(record['text'])} chars) for law {law_id}, skipping")
            skipped += 1
            time.sleep(RATE_LIMIT)
            continue

        fetched += 1
        yield record
        time.sleep(RATE_LIMIT)

    logger.info(f"Done: {fetched} fetched, {skipped} skipped (no text)")


def test_api():
    """Quick connectivity test."""
    print("Testing BACN search endpoint...")
    try:
        response = requests.get(SEARCH_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()
        laws = [d for d in data if d.get("nombre_categoria") == "Leyes Paraguayas"]
        print(f"  OK: {len(laws)} laws in index")
    except Exception as e:
        print(f"  FAIL: {e}")
        return False

    print("Testing individual law page...")
    if laws:
        law = laws[0]
        page = fetch_law_page(law["id_documento"], law["titulo_documento"])
        if page and page.get("text"):
            print(f"  OK: Got {len(page['text'])} chars of text")
            print(f"  Dates: prom={page.get('date_promulgation')}, pub={page.get('date_publication')}")
            print(f"  PDFs: {len(page.get('pdf_urls', []))}")
        else:
            print("  WARN: No text extracted from first law, trying another...")
            for law2 in laws[1:5]:
                page2 = fetch_law_page(law2["id_documento"], law2["titulo_documento"])
                time.sleep(1)
                if page2 and page2.get("text"):
                    print(f"  OK: Got {len(page2['text'])} chars from law {law2['id_documento']}")
                    break
            else:
                print("  FAIL: Could not extract text from any law")
                return False

    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        filename = SAMPLE_DIR / f"{record['_id'].replace('/', '_')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"  Saved: {filename.name} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {SAMPLE_DIR}")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PY/BACN Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
