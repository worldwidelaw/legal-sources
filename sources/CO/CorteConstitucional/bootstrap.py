#!/usr/bin/env python3
"""
CO/CorteConstitucional -- Colombian Constitutional Court Data Fetcher

Fetches Colombian Constitutional Court decisions from datos.gov.co (Socrata)
with full text from corteconstitucional.gov.co/relatoria/.

Strategy:
  - Socrata SODA API for metadata (29,000+ decisions)
  - Construct relatoria URL from sentencia field
  - Fetch HTML full text, strip tags, decode windows-1252

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
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CO/CorteConstitucional"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.CorteConstitucional")

# Socrata SODA API
SOCRATA_URL = "https://www.datos.gov.co/resource/v2k4-2t8s.json"

# Full text base
RELATORIA_BASE = "https://www.corteconstitucional.gov.co/relatoria"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SENTENCIA_TYPES = {
    "T": "Tutela",
    "C": "Constitucionalidad",
    "SU": "Sentencia Unificada",
}


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


def sentencia_to_url(sentencia: str) -> Optional[str]:
    """
    Convert sentencia ID to relatoria URL.
    E.g. "T-012/92" -> https://www.corteconstitucional.gov.co/relatoria/1992/T-012-92.htm
    """
    if not sentencia:
        return None
    m = re.match(r'^([A-Z]+)-(\d+)/(\d{2})$', sentencia.strip())
    if not m:
        return None
    tipo, num, yy = m.groups()
    yy_int = int(yy)
    if yy_int >= 92:
        year = 1900 + yy_int
    else:
        year = 2000 + yy_int
    filename = f"{tipo}-{num}-{yy}.htm"
    return f"{RELATORIA_BASE}/{year}/{filename}"


def fetch_full_text(url: str) -> str:
    """Fetch and extract full text from relatoria HTML page."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        # Try windows-1252 first, fall back to utf-8
        try:
            html = response.content.decode('windows-1252')
        except (UnicodeDecodeError, LookupError):
            html = response.content.decode('utf-8', errors='replace')
        # Extract body content
        body_match = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL | re.IGNORECASE)
        if body_match:
            html = body_match.group(1)
        return clean_html(html)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logger.debug(f"404 for {url}")
        else:
            logger.warning(f"HTTP error fetching {url}: {e}")
        return ""
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return ""


def search_socrata(limit: int = 20, offset: int = 0) -> tuple:
    """Query Socrata SODA API. Returns (records, total_count)."""
    params = {
        "$limit": str(limit),
        "$offset": str(offset),
        "$order": "fecha_sentencia DESC",
    }
    response = requests.get(SOCRATA_URL, params=params, headers={
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json",
    }, timeout=30)
    response.raise_for_status()
    records = response.json()

    # Get total count
    count_params = {"$select": "count(*)"}
    count_resp = requests.get(SOCRATA_URL, params=count_params, headers={
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json",
    }, timeout=30)
    count_resp.raise_for_status()
    total = int(count_resp.json()[0].get("count", 0))

    return records, total


def normalize(record: dict, text: str) -> dict:
    """Transform to standard schema."""
    sentencia = record.get("sentencia", "")
    tipo = record.get("sentencia_tipo", "")
    fecha = record.get("fecha_sentencia", "")
    date = fecha[:10] if fecha else None

    url = sentencia_to_url(sentencia)

    return {
        "_id": sentencia,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "sentencia": sentencia,
        "title": f"Sentencia {sentencia}",
        "text": text,
        "date": date,
        "decision_type": SENTENCIA_TYPES.get(tipo, tipo),
        "magistrado": record.get("magistrado_a", ""),
        "sala": record.get("sala", ""),
        "proceso": record.get("proceso", ""),
        "expediente": f"{record.get('expediente_tipo', '')}-{record.get('expediente_numero', '')}",
        "sv_spv": record.get("sv_spv", ""),
        "av_apv": record.get("av_apv", ""),
        "url": url or f"https://www.corteconstitucional.gov.co/relatoria/?sentencia={sentencia}",
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents with full text."""
    records = []

    logger.info("Querying Socrata API for recent decisions...")
    search_results, total = search_socrata(limit=count + 10, offset=0)
    logger.info(f"Total decisions available: {total:,}")
    logger.info(f"Got {len(search_results)} search results")

    for sr in search_results:
        if len(records) >= count:
            break

        sentencia = sr.get("sentencia", "")
        if not sentencia:
            continue

        url = sentencia_to_url(sentencia)
        if not url:
            logger.warning(f"  Cannot construct URL for {sentencia}")
            continue

        logger.info(f"  Fetching {sentencia}...")
        text = fetch_full_text(url)
        time.sleep(0.5)

        if text and len(text) > 200:
            normalized = normalize(sr, text)
            records.append(normalized)
            logger.info(f"  [{len(records)}/{count}] {sentencia} ({len(text)} chars)")
        else:
            logger.warning(f"  Skipped {sentencia} - no/short text ({len(text)} chars)")

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all decisions with full text."""
    offset = 0
    batch_size = 1000
    total_yielded = 0

    _, total = search_socrata(limit=1, offset=0)
    logger.info(f"Total decisions: {total:,}")

    while offset < total:
        params = {
            "$limit": str(batch_size),
            "$offset": str(offset),
            "$order": "fecha_sentencia DESC",
        }
        response = requests.get(SOCRATA_URL, params=params, headers={
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "application/json",
        }, timeout=30)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break

        for sr in batch:
            sentencia = sr.get("sentencia", "")
            if not sentencia:
                continue

            url = sentencia_to_url(sentencia)
            if not url:
                continue

            text = fetch_full_text(url)
            time.sleep(0.5)

            if text and len(text) > 200:
                normalized = normalize(sr, text)
                total_yielded += 1
                if total_yielded % 100 == 0:
                    logger.info(f"  Processed {total_yielded} records (offset {offset})...")
                yield normalized

        offset += batch_size


def test_api():
    """Test API connectivity."""
    logger.info("Testing Colombian Constitutional Court APIs...")

    # Test Socrata
    try:
        results, total = search_socrata(limit=3, offset=0)
        logger.info(f"Socrata API OK - {total:,} total decisions, got {len(results)} results")
    except Exception as e:
        logger.error(f"Socrata API failed: {e}")
        return False

    if not results:
        logger.error("No results from Socrata")
        return False

    # Test full text
    sentencia = results[0].get("sentencia", "")
    url = sentencia_to_url(sentencia)
    logger.info(f"Testing full text for {sentencia} -> {url}")

    if url:
        text = fetch_full_text(url)
        if text and len(text) > 200:
            logger.info(f"Full text OK - {len(text)} characters")
            logger.info(f"Preview: {text[:200]}...")
            return True
        else:
            logger.error(f"Full text extraction failed ({len(text)} chars)")
            return False

    return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = record["sentencia"].replace("/", "-")
        filename = f"sample_{i:02d}_{safe_id}.json"
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

    types = set(r.get("decision_type", "") for r in records)
    logger.info(f"  - Decision types: {', '.join(sorted(t for t in types if t))}")

    return len(records) >= 10 and avg_text > 500


def main():
    parser = argparse.ArgumentParser(description="CO/CorteConstitucional Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")

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
                safe_id = record["sentencia"].replace("/", "-")
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")
            sys.exit(0)


if __name__ == "__main__":
    main()
