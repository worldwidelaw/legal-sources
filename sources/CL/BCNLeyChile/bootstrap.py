#!/usr/bin/env python3
"""
CL/BCNLeyChile -- Biblioteca del Congreso Nacional Data Fetcher

Fetches Chilean legislation from the Ley Chile platform.

Data source: https://www.bcn.cl/leychile/
License: Open government data (Chile)

Strategy:
  - Use internal JSON API at nuevo.leychile.cl/servicios
  - Search endpoint lists norms with pagination (buscarjson)
  - Full text via get_norma_json endpoint per norm ID
  - Parse HTML sections to extract clean text

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # API connectivity test
"""

import argparse
import json
import logging
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "CL/BCNLeyChile"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

# The corpus is ~415K norms and each detail fetch costs ~0.9s, so a
# single-threaded 1s-delay walk would need ~220h -- far past the fleet's
# 100h wall. Fetch each page's details through a small pool instead, and
# checkpoint completed pages so re-launched slots advance monotonically.
PAGE_SIZE = 200
DETAIL_WORKERS = 4
CHECKPOINT_EVERY = 5  # pages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.BCNLeyChile")

# API Configuration
API_BASE = "https://nuevo.leychile.cl/servicios"
SEARCH_URL = API_BASE + "/buscarjson"
NORM_URL = API_BASE + "/Navegar/get_norma_json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "application/json, text/html, */*",
}

# Norm type mapping
NORM_TYPES = {
    "LEY": "Ley",
    "DTO": "Decreto",
    "DFL": "Decreto con Fuerza de Ley",
    "DL": "Decreto Ley",
    "RES": "Resolución",
    "ORZ": "Ordenanza",
    "REC": "Recurso",
}


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_text_from_html_sections(html_sections) -> str:
    """Extract clean text from the HTML sections returned by get_norma_json."""
    if not html_sections:
        return ""
    if isinstance(html_sections, str):
        return clean_html(html_sections)
    if isinstance(html_sections, list):
        parts = []
        for section in html_sections:
            if isinstance(section, dict) and 't' in section:
                parts.append(clean_html(section['t']))
            elif isinstance(section, str):
                parts.append(clean_html(section))
        return '\n\n'.join(p for p in parts if p)
    return ""


def parse_date(date_str: str) -> Optional[str]:
    """Parse BCN date formats to ISO 8601."""
    if not date_str:
        return None
    # Format: DD-MON-YYYY (e.g., "20-MAR-2026")
    month_map = {
        'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
        'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
        'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12',
        'ENE': '01', 'ABR': '04', 'AGO': '08', 'DIC': '12',
    }
    m = re.match(r'(\d{1,2})-([A-Z]{3})-(\d{4})', date_str)
    if m:
        day, mon, year = m.groups()
        month = month_map.get(mon, '01')
        return f"{year}-{month}-{int(day):02d}"
    # Format: YYYY-MM-DD
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
    if m:
        return date_str
    return None


RETRY_STATUSES = (429, 500, 502, 503, 504)


def search_norms(page: int = 1, per_page: int = 20, tipo_norma: int = 0, max_retries: int = 5) -> tuple:
    """
    Search for norms via the buscarjson endpoint.

    Returns (results_list, total_items).

    The pagination parameter is `npagina`. An earlier version sent `page_num`,
    which buscarjson silently ignores -- every request returned page 1, so a
    1,018-page crawl produced 50,800 records collapsing to 50 distinct ids
    (issue #1589). The response echoes the page it actually served, so assert
    it here: a mismatch means the parameter was dropped again and the crawl
    must fail loud rather than re-emit page 1 forever.

    Retries on transient errors (timeouts, 429/5xx) with exponential backoff,
    then raises -- a swallowed error used to end pagination silently.
    """
    params = {
        'string': '',
        'tipoNorma': str(tipo_norma),
        'npagina': str(page),
        'itemsporpagina': str(per_page),
        'orden': '2',  # Order by publication date desc
    }
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=90)
            response.raise_for_status()
            data = response.json()

            if not (isinstance(data, list) and len(data) >= 2):
                raise RuntimeError(
                    f"buscarjson returned an unexpected shape for page {page}: {type(data).__name__}"
                )

            results = data[0] if isinstance(data[0], list) else []
            meta = data[1] if isinstance(data[1], dict) else {}

            served = meta.get('npagina')
            if served is not None and str(served) != str(page):
                raise RuntimeError(
                    f"buscarjson ignored the pagination parameter: asked for page {page}, "
                    f"served page {served}. Re-check the param name (issue #1589)."
                )

            total = int(meta.get('totalitems', 0))
            return results, total
        except requests.exceptions.HTTPError as e:
            last_error = e
            status = e.response.status_code if e.response is not None else None
            if status in RETRY_STATUSES and attempt < max_retries:
                wait = _retry_after(e.response) or min(2 ** (attempt + 1), 120)
                logger.warning(
                    f"Page {page}: HTTP {status}, retrying in {wait}s "
                    f"(attempt {attempt + 1}/{max_retries})..."
                )
                time.sleep(wait)
                continue
            raise
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < max_retries:
                wait = min(2 ** (attempt + 1), 120)
                logger.warning(f"Page {page}: {type(e).__name__}, retrying in {wait}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait)
                continue
            raise

    raise RuntimeError(f"Page {page}: exhausted retries") from last_error


def _retry_after(response) -> Optional[int]:
    """Honor a Retry-After header when the server sends one."""
    if response is None:
        return None
    try:
        return max(1, min(int(response.headers.get('Retry-After', '')), 120))
    except (TypeError, ValueError):
        return None


def _load_checkpoint() -> set:
    """Return the set of page numbers already crawled in a previous run."""
    try:
        with open(CHECKPOINT_PATH, encoding="utf-8") as f:
            return {int(p) for p in json.load(f).get("completed_pages", [])}
    except (FileNotFoundError, ValueError, TypeError, KeyError):
        return set()


def _save_checkpoint(completed: set) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"completed_pages": sorted(completed)}, f)
    tmp.replace(CHECKPOINT_PATH)


def fetch_norm_text(id_norma: int) -> Optional[dict]:
    """
    Fetch full norm data including text via get_norma_json.

    Returns dict with metadatos and text, or None on failure.
    """
    params = {
        'idNorma': str(id_norma),
        'idVersion': '',
        'idLey': '',
        'tipoVersion': 'Vigente',
        'cve': '',
        'agrupa_partes': '1',
    }
    try:
        response = requests.get(NORM_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        text = extract_text_from_html_sections(data.get('html'))
        metadatos = data.get('metadatos', {})

        return {
            'text': text,
            'metadatos': metadatos,
        }
    except Exception as e:
        logger.warning(f"Failed to fetch norm {id_norma}: {e}")
        return None


def normalize(search_record: dict, norm_data: Optional[dict] = None) -> dict:
    """Transform raw data to standard schema."""
    id_norma = search_record.get('IDNORMA', '')

    # Title
    title = search_record.get('TITULO_NORMA', '')
    norm_label = search_record.get('NORMA', '')
    if not title and norm_label:
        title = norm_label

    # Text from full norm fetch
    text = ''
    if norm_data:
        text = norm_data.get('text', '')

    # Type info
    abrev = search_record.get('ABREVIACION', '')
    descripcion = search_record.get('DESCRIPCION', '')
    numero = search_record.get('NUMERO', '')
    compuesto = search_record.get('COMPUESTO', '')

    # Date
    pub_date = parse_date(search_record.get('FECHA_PUBLICACION', ''))
    prom_date = parse_date(search_record.get('FECHA_PROMULGACION', ''))
    date = pub_date or prom_date

    # Organism
    organismo = search_record.get('ORGANISMO', '')

    # URL
    url = f"https://www.bcn.cl/leychile/navegar?idNorma={id_norma}"

    return {
        '_id': str(id_norma),
        '_source': SOURCE_ID,
        '_type': 'legislation',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'idNorma': str(id_norma),
        'tipo_norma': descripcion or abrev,
        'abreviacion': abrev,
        'numero': numero,
        'compuesto': compuesto,
        'title': title or f"{descripcion} {numero}",
        'text': text,
        'date': date,
        'fecha_publicacion': pub_date,
        'fecha_promulgacion': prom_date,
        'fecha_vigencia': parse_date(search_record.get('FECHA_VIGENCIA', '')),
        'organismo': organismo,
        'version_type': search_record.get('TIPOVERSION_TEXTO', ''),
        'url': url,
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents with full text."""
    records = []

    logger.info("Searching for recent norms...")
    search_results, total = search_norms(page=1, per_page=count * 2)
    logger.info(f"Total norms available: {total:,}")
    logger.info(f"Fetched {len(search_results)} search results")

    for sr in search_results:
        if len(records) >= count:
            break

        id_norma = sr.get('IDNORMA')
        if not id_norma:
            continue

        logger.info(f"  Fetching full text for {sr.get('NORMA', '')} (ID: {id_norma})...")
        norm_data = fetch_norm_text(id_norma)
        time.sleep(1)  # Rate limit

        if norm_data and norm_data.get('text') and len(norm_data['text']) > 50:
            normalized = normalize(sr, norm_data)
            records.append(normalized)
            logger.info(f"  [{len(records)}/{count}] {normalized['title'][:60]}... ({len(norm_data['text'])} chars)")
        else:
            logger.warning(f"  Skipped {id_norma} - no/short text")

    return records


def fetch_all(resume: bool = True) -> Generator[dict, None, None]:
    """
    Fetch all norms with full text.

    Walks buscarjson pages and fetches each page's full texts through a small
    thread pool. Completed pages are checkpointed, so a re-launched run skips
    them with no network calls and advances monotonically toward the ~415K
    corpus instead of restarting.
    """
    completed = _load_checkpoint() if resume else set()
    if completed:
        logger.info(f"Resuming: {len(completed):,} pages already crawled")

    _, total = search_norms(page=1, per_page=1)
    if total <= 0:
        raise RuntimeError("buscarjson reported 0 total norms -- refusing to report an empty corpus")
    last_page = math.ceil(total / PAGE_SIZE)
    logger.info(f"Total norms to process: {total:,} ({last_page:,} pages of {PAGE_SIZE})")

    total_yielded = 0
    seen_ids = set()

    for page in range(1, last_page + 1):
        if page in completed:
            continue

        search_results, _ = search_norms(page=page, per_page=PAGE_SIZE)
        if not search_results:
            logger.info(f"Page {page} is empty -- end of corpus")
            completed.add(page)
            break

        ids = [sr.get('IDNORMA') for sr in search_results]
        with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
            norm_datas = list(pool.map(
                lambda i: fetch_norm_text(i) if i else None,
                ids,
            ))

        for sr, norm_data in zip(search_results, norm_datas):
            id_norma = sr.get('IDNORMA')
            if not id_norma or id_norma in seen_ids:
                continue
            seen_ids.add(id_norma)

            if norm_data and norm_data.get('text') and len(norm_data['text']) > 50:
                total_yielded += 1
                yield normalize(sr, norm_data)

        completed.add(page)
        if page % CHECKPOINT_EVERY == 0:
            _save_checkpoint(completed)
            logger.info(
                f"  Page {page:,}/{last_page:,} -- {total_yielded:,} records, "
                f"{len(seen_ids):,} distinct ids this run"
            )

    _save_checkpoint(completed)
    logger.info(f"Finished: {total_yielded:,} records, {len(seen_ids):,} distinct ids")


def test_api():
    """Test API connectivity."""
    logger.info("Testing BCN Ley Chile API...")

    # Test search
    try:
        results, total = search_norms(page=1, per_page=3)
        logger.info(f"Search API OK - {total:,} total norms, got {len(results)} results")
    except Exception as e:
        logger.error(f"Search API failed: {e}")
        return False

    # Regression check for issue #1589: distinct pages must return distinct
    # norms. A shared id set here means the pagination parameter is being
    # ignored and the whole crawl would collapse to one page of records.
    try:
        page_ids = {}
        for p in (1, 2, 500):
            rows, _ = search_norms(page=p, per_page=10)
            page_ids[p] = {r.get('IDNORMA') for r in rows}
            logger.info(f"Pagination check: page {p} -> {len(page_ids[p])} ids")
        overlap = page_ids[1] & page_ids[2] | page_ids[1] & page_ids[500]
        if overlap:
            logger.error(f"Pagination broken - pages share ids {sorted(overlap)[:5]} (issue #1589)")
            return False
        logger.info("Pagination OK - pages 1, 2 and 500 return disjoint norms")
    except Exception as e:
        logger.error(f"Pagination check failed: {e}")
        return False

    # Test full text
    if results:
        id_norma = results[0].get('IDNORMA')
        if id_norma:
            logger.info(f"Testing full text fetch for norm {id_norma}...")
            norm_data = fetch_norm_text(id_norma)
            if norm_data and norm_data.get('text'):
                logger.info(f"Full text OK - {len(norm_data['text'])} characters")
                logger.info(f"Preview: {norm_data['text'][:200]}...")
                return True
            else:
                logger.error("Full text fetch returned empty")
                return False

    return True


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        filename = f"sample_{i:02d}_{record['idNorma']}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Summary
    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get('text', '')) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info(f"Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0} chars")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0} chars")

    types = set(r.get('tipo_norma', '') for r in records)
    logger.info(f"  - Norm types: {', '.join(sorted(types))}")

    dates = [r.get('date') for r in records if r.get('date')]
    logger.info(f"  - Date range: {min(dates) if dates else 'N/A'} to {max(dates) if dates else 'N/A'}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="CL/BCNLeyChile Legislation Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore the page checkpoint and re-crawl from page 1")

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
            resume = not args.no_resume
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            jsonl_path = DATA_DIR / "records.jsonl"
            # Append when resuming so a re-launched slot keeps the records the
            # earlier slots already wrote; the loader dedups on _id.
            mode = "a" if (resume and _load_checkpoint()) else "w"
            count = 0
            with open(jsonl_path, mode, encoding="utf-8") as f:
                for record in fetch_all(resume=resume):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 500 == 0:
                        f.flush()
            logger.info(f"Processed {count} records to {jsonl_path}")
            sys.exit(0)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
