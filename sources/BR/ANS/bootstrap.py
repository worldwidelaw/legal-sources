#!/usr/bin/env python3
"""
BR/ANS -- Agência Nacional de Saúde Suplementar (ANS) Legislation

Fetches normative resolutions, operational resolutions, communications,
and other regulatory acts from Brazil's National Health Agency.

Data source: https://www.ans.gov.br/legislacao/busca-de-legislacao
Strategy:
  - Sequential ID enumeration (1 to ~4839) via base64-encoded numeric IDs
  - Full text HTML pages at /component/legislacao/?view=legislacao&task=textoLei&format=raw&id=BASE64
  - Parse <h1> for title, <p class="ementa"> for summary, body from HTML content
  - ~4800 legislative acts from 1998 to present

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import base64
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "BR/ANS"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.ANS")

BASE_URL = "https://www.ans.gov.br"
TEXT_URL = f"{BASE_URL}/component/legislacao/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
}

MAX_ID = 4839

MONTH_MAP = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12",
}


class HTMLTextExtractor(HTMLParser):
    """Extract visible text from HTML, stripping tags."""

    def __init__(self):
        super().__init__()
        self._text = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head"):
            self._skip = False
        if tag in ("p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr"):
            self._text.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._text.append(data)

    def get_text(self):
        return "".join(self._text)


def strip_html(html: str) -> str:
    """Remove HTML tags and extract plain text."""
    extractor = HTMLTextExtractor()
    try:
        extractor.feed(html)
        return extractor.get_text()
    except Exception:
        return re.sub(r'<[^>]+>', ' ', html)


def clean_text(text: str) -> str:
    """Normalize whitespace in text."""
    if not text:
        return ""
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def encode_id(num_id: int) -> str:
    """Base64-encode a numeric ID for the ANS URL parameter."""
    return base64.b64encode(str(num_id).encode()).decode()


def parse_title(html: str) -> Optional[str]:
    """Extract title from <h1> tag."""
    match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL | re.IGNORECASE)
    if match:
        title = strip_html(match.group(1)).strip()
        # Remove anchor tags content
        title = re.sub(r'VOLTAR', '', title).strip()
        return title if title else None
    return None


def parse_ementa(html: str) -> Optional[str]:
    """Extract ementa (summary) from <p class="ementa"> tags."""
    matches = re.findall(
        r'<p\s+class="ementa"[^>]*>(.*?)</p>',
        html, re.DOTALL | re.IGNORECASE
    )
    if matches:
        parts = [strip_html(m).strip() for m in matches]
        ementa = " ".join(p for p in parts if p)
        return ementa if ementa else None
    return None


def parse_date_from_title(title: str) -> Optional[str]:
    """Extract date from title like 'RESOLUÇÃO NORMATIVA ANS Nº 566, DE 29 DE DEZEMBRO DE 2022'."""
    if not title:
        return None
    # Pattern: DE DD DE MONTH DE YYYY
    match = re.search(
        r'DE\s+(\d{1,2})\s+DE\s+(\w+)\s+DE\s+(\d{4})',
        title, re.IGNORECASE
    )
    if match:
        day = match.group(1).zfill(2)
        month_name = match.group(2).lower()
        year = match.group(3)
        month = MONTH_MAP.get(month_name)
        if month:
            return f"{year}-{month}-{day}"
    # Fallback: just year
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    if year_match:
        return f"{year_match.group(1)}-01-01"
    return None


def parse_norm_type(title: str) -> Optional[str]:
    """Extract norm type from title."""
    if not title:
        return None
    title_upper = title.upper()
    type_map = [
        ("RESOLUÇÃO NORMATIVA", "RN"),
        ("RESOLUÇÃO OPERACIONAL", "RO"),
        ("INSTRUÇÃO NORMATIVA", "IN"),
        ("PORTARIA", "Portaria"),
        ("SÚMULA NORMATIVA", "SN"),
        ("RESOLUÇÃO", "Resolução"),
        ("COMUNICADO", "Comunicado"),
        ("LEI", "Lei"),
        ("DECRETO", "Decreto"),
        ("MEDIDA PROVISÓRIA", "MP"),
    ]
    for pattern, norm_type in type_map:
        if pattern in title_upper:
            return norm_type
    return "Outro"


def parse_norm_number(title: str) -> Optional[str]:
    """Extract norm number from title."""
    if not title:
        return None
    match = re.search(r'[Nn][ºo°]\s*(\d+[\.\d]*)', title)
    if match:
        return match.group(1)
    return None


def fetch_norm(num_id: int, timeout: int = 30) -> Optional[dict]:
    """Fetch a single norm by its numeric ID."""
    b64_id = encode_id(num_id)
    url = f"{TEXT_URL}?view=legislacao&task=textoLei&format=raw&id={b64_id}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch ID {num_id}: {e}")
        return None

    html = response.text

    # Check for empty/error responses
    if len(html) < 200 or "alert(" in html[:200]:
        return None

    title = parse_title(html)
    if not title:
        return None

    # Extract body text (everything after the head/script section)
    body_html = html
    # Remove head section
    head_end = re.search(r'</head>', body_html, re.IGNORECASE)
    if head_end:
        body_html = body_html[head_end.end():]

    full_text = clean_text(strip_html(body_html))
    if not full_text or len(full_text) < 50:
        return None

    ementa = parse_ementa(html)
    date = parse_date_from_title(title)
    norm_type = parse_norm_type(title)
    norm_number = parse_norm_number(title)

    return {
        "id": num_id,
        "title": title,
        "ementa": ementa,
        "text": full_text,
        "date": date,
        "norm_type": norm_type,
        "norm_number": norm_number,
        "url": f"{BASE_URL}/component/legislacao/?view=legislacao&task=textoLei&format=raw&id={b64_id}",
    }


def normalize(raw: dict) -> dict:
    """Transform raw norm data into standard schema."""
    norm_id = f"ANS-{raw['norm_type']}-{raw.get('norm_number', raw['id'])}"

    return {
        "_id": norm_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw["title"],
        "text": raw["text"],
        "ementa": raw.get("ementa", ""),
        "date": raw.get("date"),
        "norm_type": raw.get("norm_type"),
        "norm_number": raw.get("norm_number"),
        "url": raw["url"],
        "internal_id": raw["id"],
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all ANS norms by iterating over IDs."""
    total = 0
    skipped = 0

    for num_id in range(1, MAX_ID + 1):
        raw = fetch_norm(num_id)
        if raw:
            record = normalize(raw)
            if record.get("text") and len(record["text"]) > 100:
                yield record
                total += 1
                if total % 100 == 0:
                    logger.info(f"Progress: {total} records fetched, {skipped} skipped, ID {num_id}/{MAX_ID}")
                if max_records and total >= max_records:
                    break
            else:
                skipped += 1
        else:
            skipped += 1

        time.sleep(1.0)

    logger.info(f"Total: {total} records fetched, {skipped} empty/skipped")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from various ID ranges."""
    records = []
    # Sample from different parts of the ID range for variety
    sample_ids = [
        10, 50, 100, 200, 500, 1000, 1500, 2000, 2500, 3000,
        3500, 4000, 4200, 4341, 4500, 4700, 4800, 4830,
    ]

    for num_id in sample_ids:
        if len(records) >= count:
            break
        raw = fetch_norm(num_id)
        if raw:
            record = normalize(raw)
            if record.get("text") and len(record["text"]) > 100:
                records.append(record)
                logger.info(
                    f"  [{len(records)}/{count}] ID {num_id}: {record['title'][:60]}... "
                    f"({len(record['text'])} chars)"
                )
        time.sleep(1.0)

    return records


def test_api():
    """Test connectivity to ANS legislation portal."""
    logger.info("Testing ANS legislation portal connectivity...")

    try:
        raw = fetch_norm(4341)
        if raw:
            logger.info(f"OK - Title: {raw['title'][:80]}")
            logger.info(f"OK - Text length: {len(raw['text'])} chars")
            logger.info(f"OK - Date: {raw.get('date')}")
            logger.info(f"OK - Type: {raw.get('norm_type')}")
            return True
        else:
            logger.error("Failed to fetch test norm (ID 4341)")
            return False
    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="BR/ANS Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    parser.add_argument("--max-records", type=int, default=None, help="Maximum records to fetch")

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
            if records:
                text_lengths = [len(r.get("text", "")) for r in records]
                logger.info(f"Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars")
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
