#!/usr/bin/env python3
"""
CL/DT -- Dirección del Trabajo Jurisprudencia Administrativa Fetcher

Fetches Chilean Labor Authority administrative rulings (dict��menes and
ordinarios) from the DT normativa portal.

Data source: https://www.dt.gob.cl/legislacion/1624/w3-channel.html
License: Open government data (Chile)

Strategy:
  - Fetch year pages → extract month sub-pages (propertyvalue links)
  - Fetch month pages → extract article IDs from relative links
  - Fetch each article page and extract full text from HTML
  - Parse metadata (number, date, subject) from page structure

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # API connectivity test
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

# Setup
SOURCE_ID = "CL/DT"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.DT")

BASE_URL = "https://www.dt.gob.cl/legislacion/1624"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "text/html, */*",
}

# Year -> propertyvalue ID mapping for Dictámenes
DICTAMEN_YEARS = {
    2026: "193891", 2025: "191853", 2024: "188794", 2023: "184682",
    2022: "182142", 2021: "179229", 2020: "176961", 2019: "172974",
    2018: "166905", 2017: "161037", 2016: "157851", 2015: "82250",
    2014: "82237", 2013: "81431", 2012: "28505", 2011: "28492",
    2010: "27422", 2009: "27409", 2008: "27205", 2007: "26882",
    2006: "25598", 2005: "23874", 2004: "22852", 2003: "22838",
    2002: "22825", 2001: "22812", 2000: "22799", 1999: "23417",
    1998: "24410", 1997: "24693", 1996: "25103", 1995: "25116",
    1994: "25611", 1993: "166027", 1992: "191164", 1991: "191163",
    1990: "165877", 1989: "165974", 1988: "191162", 1987: "166010",
    1986: "191161", 1985: "165861", 1984: "191160", 1983: "165963",
    1982: "191159", 1981: "165967", 1980: "165871", 1979: "165991",
    1978: "165875", 1977: "166016", 1976: "165971", 1974: "165872",
    1973: "166020", 1969: "165873", 1968: "165874", 1956: "165876",
    1952: "165880", 1950: "165878", 1945: "165879", 1925: "165903",
    1919: "165950",
}

# Ordinarios year listings
ORDINARIO_PV_ID = "147182"

MONTH_MAP = {
    'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
    'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
    'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12',
    'jan': '01', 'apr': '04', 'aug': '08', 'dec': '12',
}


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse DT date formats to ISO 8601. E.g., '27-dic-2024'."""
    if not date_str:
        return None
    m = re.match(r'(\d{1,2})-(\w{3})-(\d{4})', date_str.strip())
    if m:
        day, mon, year = m.groups()
        month = MONTH_MAP.get(mon.lower(), None)
        if month:
            return f"{year}-{month}-{int(day):02d}"
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str.strip())
    if m:
        return date_str.strip()
    return None


def get_month_pvids_from_year(year_pv_id: str) -> list:
    """Fetch a year page and extract month sub-listing propertyvalue IDs."""
    url = f"{BASE_URL}/w3-propertyvalue-{year_pv_id}.html"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        # Month links have pv-pid-{year_pv_id} and pvid-{month_pv_id}
        month_pvs = re.findall(
            rf'pv-pid-{year_pv_id}\s+pvid-(\d+)\s+cid-912',
            resp.text
        )
        if not month_pvs:
            # Fallback: some older years list articles directly
            month_pvs = [year_pv_id]
        return month_pvs
    except Exception as e:
        logger.warning(f"Failed to fetch year listing {year_pv_id}: {e}")
        return []


# Navigation/footer article IDs that appear on every page — not real dictámenes
NAV_ARTICLE_IDS = {
    '102544', '108710', '113566', '114948', '116560', '116605',
    '118613', '120303', '121379', '127560', '128240', '94445',
    '95122', '97403', '98244', '99167',
}


def get_article_ids_from_month(pv_id: str) -> list:
    """Fetch a month listing page and extract article IDs from relative links."""
    url = f"{BASE_URL}/w3-propertyvalue-{pv_id}.html"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        # Only match relative article links (real dictámenes), not /portal/ nav links
        ids = re.findall(r'href="w3-article-(\d+)\.html"', resp.text)
        # Filter out known navigation IDs
        ids = [aid for aid in ids if aid not in NAV_ARTICLE_IDS]
        return sorted(set(ids))
    except Exception as e:
        logger.warning(f"Failed to fetch month listing {pv_id}: {e}")
        return []


def get_article_ids_from_listing(year_pv_id: str) -> list:
    """Get all article IDs for a year by navigating month sub-pages."""
    month_pvs = get_month_pvids_from_year(year_pv_id)
    all_ids = []
    for mpv in month_pvs:
        if mpv == year_pv_id:
            # Year page itself has articles directly (some older years)
            ids = get_article_ids_from_month(mpv)
        else:
            ids = get_article_ids_from_month(mpv)
        all_ids.extend(ids)
        time.sleep(1)
    return sorted(set(all_ids))


def fetch_article(article_id: str) -> Optional[dict]:
    """Fetch a single article page and extract metadata + full text."""
    url = f"{BASE_URL}/w3-article-{article_id}.html"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.warning(f"Failed to fetch article {article_id}: {e}")
        return None

    # Extract title/presentation block
    title = ""
    numero = ""
    date_str = ""
    subject = ""

    pres_m = re.search(
        r'id="article_i__w3_ar_ArticuloCompleto_presentacion_1"[^>]*>(.*?)</div>',
        html, re.DOTALL
    )
    if pres_m:
        pres_text = re.sub(r'<[^>]+>', '|', pres_m.group(1))
        pres_text = unescape(pres_text)
        parts = [p.strip() for p in pres_text.split('|') if p.strip()]

        # Find ORD. number
        for p in parts:
            if re.match(r'ORD\.\s*N', p, re.IGNORECASE):
                numero = p
            elif re.match(r'\d{1,2}-\w{3}-\d{4}', p):
                date_str = p
            elif len(p) > 10 and not p.startswith('Dictám'):
                if not subject:
                    subject = p

        # Also search for dictamen number pattern
        if not numero:
            num_m = re.search(r'((?:ORD|dictamen)\.\s*N[°º]?\s*[\d/]+)', pres_text, re.IGNORECASE)
            if num_m:
                numero = num_m.group(1)

    # Extract body text
    body_m = re.search(
        r'id="article_i__w3_ar_ArticuloCompleto_cuerpo_1"[^>]*>(.*?)(?:<div\s+id="|<div\s+class="articulo-herramientas|$)',
        html, re.DOTALL
    )
    text = ""
    if body_m:
        text = clean_html(body_m.group(1))

    if not text:
        return None

    # Build title from subject and numero
    if subject and numero:
        title = f"{subject} - {numero}"
    elif numero:
        title = numero
    elif subject:
        title = subject
    else:
        title = f"Dictamen {article_id}"

    date = parse_date(date_str)

    return {
        'article_id': article_id,
        'numero': numero,
        'title': title,
        'subject': subject,
        'text': text,
        'date': date,
        'date_raw': date_str,
        'url': url,
    }


def normalize(raw: dict) -> dict:
    """Transform raw data to standard schema."""
    return {
        '_id': raw['article_id'],
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'article_id': raw['article_id'],
        'numero': raw.get('numero', ''),
        'title': raw.get('title', ''),
        'subject': raw.get('subject', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'date_raw': raw.get('date_raw', ''),
        'organismo': 'Dirección del Trabajo',
        'url': raw.get('url', ''),
    }


def fetch_sample(count: int = 15) -> list:
    """Fetch sample documents with full text from recent years."""
    records = []

    # Get articles from the most recent year listings
    for year in sorted(DICTAMEN_YEARS.keys(), reverse=True):
        if len(records) >= count:
            break
        pv_id = DICTAMEN_YEARS[year]
        logger.info(f"Fetching {year} listing (pv={pv_id})...")
        article_ids = get_article_ids_from_listing(pv_id)
        logger.info(f"  Found {len(article_ids)} articles for {year}")
        time.sleep(1)

        for aid in article_ids:
            if len(records) >= count:
                break

            logger.info(f"  Fetching article {aid}...")
            raw = fetch_article(aid)
            time.sleep(1)

            if raw and raw.get('text') and len(raw['text']) > 50:
                normalized = normalize(raw)
                records.append(normalized)
                logger.info(f"  [{len(records)}/{count}] {normalized['numero'] or normalized['title'][:40]} ({len(raw['text'])} chars)")
            else:
                logger.warning(f"  Skipped {aid} - no/short text")

    return records


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all dictámenes with full text."""
    total_yielded = 0

    for year in sorted(DICTAMEN_YEARS.keys(), reverse=True):
        pv_id = DICTAMEN_YEARS[year]
        logger.info(f"Processing year {year}...")
        article_ids = get_article_ids_from_listing(pv_id)
        logger.info(f"  {len(article_ids)} articles")
        time.sleep(1)

        for aid in article_ids:
            raw = fetch_article(aid)
            time.sleep(1)

            if raw and raw.get('text') and len(raw['text']) > 50:
                normalized = normalize(raw)
                total_yielded += 1
                if total_yielded % 50 == 0:
                    logger.info(f"  Processed {total_yielded} records...")
                yield normalized

    logger.info(f"Total records yielded: {total_yielded}")


def test_api():
    """Test connectivity and extraction."""
    logger.info("Testing DT Normativa site...")

    # Test listing page
    try:
        pv_id = DICTAMEN_YEARS[2024]
        ids = get_article_ids_from_listing(pv_id)
        logger.info(f"Listing OK - {len(ids)} articles for 2024")
    except Exception as e:
        logger.error(f"Listing failed: {e}")
        return False

    # Test article fetch
    if ids:
        aid = ids[0]
        logger.info(f"Fetching article {aid}...")
        raw = fetch_article(aid)
        if raw and raw.get('text'):
            logger.info(f"Article OK - {len(raw['text'])} chars")
            logger.info(f"  Numero: {raw.get('numero', '')}")
            logger.info(f"  Date: {raw.get('date', '')}")
            logger.info(f"  Preview: {raw['text'][:200]}...")
            return True
        else:
            logger.error("Article fetch returned empty")
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
        filename = f"sample_{i:02d}_{record['article_id']}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get('text', '')) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0} chars")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0} chars")

    numeros = [r.get('numero', '') for r in records if r.get('numero')]
    logger.info(f"  - Records with numero: {len(numeros)}/{len(records)}")

    dates = [r.get('date') for r in records if r.get('date')]
    logger.info(f"  - Date range: {min(dates) if dates else 'N/A'} to {max(dates) if dates else 'N/A'}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="CL/DT Jurisprudencia Administrativa Fetcher")
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
                filepath = SAMPLE_DIR / f"record_{record['article_id']}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")
            sys.exit(0)


if __name__ == "__main__":
    main()
