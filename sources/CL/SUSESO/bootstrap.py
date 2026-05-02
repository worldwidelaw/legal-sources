#!/usr/bin/env python3
"""
CL/SUSESO -- Superintendencia de Seguridad Social Dictámenes Fetcher

Fetches Chilean Social Security Superintendent administrative rulings
(dictámenes) via the Newtenberg JSONP search API + HTML detail pages.

Data source: https://www.suseso.gob.cl/612/w3-channel.html
License: Open government data (Chile)

Strategy:
  - Use JSONP search API to enumerate all dictámenes (paginated, 50/page)
  - Fetch each individual article HTML page for full text
  - Parse metadata (number, date, topic, recipient, summary) from API + HTML

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
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
SOURCE_ID = "CL/SUSESO"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.SUSESO")

# Newtenberg JSONP search API
ENGINE_BASE = "https://suseso-engine.newtenberg.com"
SEARCH_URL = (
    ENGINE_BASE
    + "/mod/find/cgi/find.cgi"
    + "?action=jsonquery&engine=SwisheFind"
    + "&cid=512&iid=612&searchon=aid"
    + "&properties=546,523,532,620,548"
    + "&json=1&pvid_and=500:515"
)

# Article HTML pages
ARTICLE_BASE = "https://www.suseso.gob.cl/612"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "text/html, application/json, */*",
}

RPP = 50  # results per page for search API
DELAY = 1.5  # seconds between article fetches


def search_dictamenes(start: int = 0, rpp: int = RPP) -> dict:
    """Query the search API and return parsed JSON."""
    url = f"{SEARCH_URL}&rpp={rpp}&start={start}"
    resp = requests.get(url, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    return resp.json()


def fetch_article_html(aid: str) -> Optional[str]:
    """Fetch individual dictamen page and extract full text."""
    url = f"{ARTICLE_BASE}/w3-article-{aid}.html"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Failed to fetch article %s: %s", aid, e)
        return None

    # Server sends UTF-8 but omits charset in Content-Type header,
    # so requests defaults to ISO-8859-1 causing Mojibake. Force UTF-8.
    resp.encoding = "utf-8"
    html = resp.text

    # Extract from panel-body (main content area)
    m = re.search(
        r'<div[^>]*class="panel-body"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        html,
        re.DOTALL,
    )
    if not m:
        # Fallback: try articulo-generico
        m = re.search(
            r'<div[^>]*class="articulo articulo-generico[^"]*"[^>]*>(.*?)</div>\s*<!--',
            html,
            re.DOTALL,
        )
    if not m:
        logger.warning("No content found for article %s", aid)
        return None

    raw = m.group(1)
    # Remove script/style blocks
    raw = re.sub(r"<script[^>]*>.*?</script>", "", raw, flags=re.DOTALL)
    raw = re.sub(r"<style[^>]*>.*?</style>", "", raw, flags=re.DOTALL)
    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", raw)
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = text.strip()

    # Remove leading "Imprimir ." if present
    text = re.sub(r"^Imprimir\s*\.\s*", "", text)

    return text if len(text) > 50 else None


def parse_date(iso_str: Optional[str]) -> Optional[str]:
    """Parse ISO date from API field like '2026-04-20T00:00:00-0400'."""
    if not iso_str:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", iso_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def normalize(record: dict, full_text: str) -> dict:
    """Normalize a search result + full text into standard schema."""
    aid = str(record.get("aid", record.get("id", "")))
    hl1 = record.get("hl1", "")
    title = record.get("title", hl1)
    date_raw = record.get("property-value_546_iso8601", "")
    date = parse_date(date_raw)
    summary = record.get("property-value_532", "")
    topic = record.get("property-value_620_name", "")
    recipient = record.get("property-value_523_name", "")

    # Extract descriptor names
    descriptors = record.get("property-value_548_name", "")

    return {
        "_id": f"CL-SUSESO-{aid}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "aid": aid,
        "dictamen_number": hl1 or title,
        "title": title,
        "date": date,
        "text": full_text,
        "summary": summary,
        "topic": topic,
        "recipient": recipient,
        "descriptors": descriptors,
        "url": f"{ARTICLE_BASE}/w3-article-{aid}.html",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all dictámenes. If sample=True, fetch ~15 diverse records."""
    logger.info("Querying search API for total count...")
    data = search_dictamenes(start=0, rpp=1)
    total = data.get("articles", {}).get("num_results", 0)
    logger.info("Total dictámenes available: %d", total)

    if sample:
        # Fetch from beginning, middle, and recent for diversity
        offsets = [0, total // 4, total // 2, 3 * total // 4, total - 5]
        records_per_offset = 3
        count = 0
        for offset in offsets:
            if count >= 15:
                break
            data = search_dictamenes(start=max(0, offset), rpp=records_per_offset)
            results = data.get("articles", {}).get("results", [])
            for rec in results:
                if count >= 15:
                    break
                aid = str(rec.get("aid", rec.get("id", "")))
                logger.info("Fetching article %s (%s)...", aid, rec.get("hl1", ""))
                text = fetch_article_html(aid)
                if text:
                    yield normalize(rec, text)
                    count += 1
                else:
                    logger.warning("Skipping %s — no full text", aid)
                time.sleep(DELAY)
        logger.info("Sample complete: %d records", count)
    else:
        count = 0
        start = 0
        consecutive_failures = 0
        while start < total:
            data = search_dictamenes(start=start, rpp=RPP)
            results = data.get("articles", {}).get("results", [])
            if not results:
                break
            for rec in results:
                aid = str(rec.get("aid", rec.get("id", "")))
                text = fetch_article_html(aid)
                if text:
                    yield normalize(rec, text)
                    count += 1
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= 50:
                        logger.error("50 consecutive failures — stopping")
                        return
                if count % 100 == 0:
                    logger.info("Progress: %d records fetched", count)
                time.sleep(DELAY)
            start += RPP
            time.sleep(0.5)
        logger.info("Full bootstrap complete: %d records", count)


def test_api():
    """Quick connectivity test."""
    logger.info("Testing JSONP search API...")
    data = search_dictamenes(start=0, rpp=2)
    total = data.get("articles", {}).get("num_results", 0)
    results = data.get("articles", {}).get("results", [])
    logger.info("API OK — %d total dictámenes, got %d in test", total, len(results))

    if results:
        aid = str(results[0].get("aid", results[0].get("id", "")))
        logger.info("Testing article fetch for aid=%s...", aid)
        text = fetch_article_html(aid)
        if text:
            logger.info("Article OK — %d chars of text", len(text))
        else:
            logger.error("Article fetch returned no text")
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description="CL/SUSESO bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    is_sample = args.sample
    count = 0

    for record in fetch_all(sample=is_sample):
        if is_sample:
            out_path = SAMPLE_DIR / f"{record['_id']}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Saved %s (%d chars text)", out_path.name, len(record.get("text", "")))
        count += 1

    logger.info("Done — %d records %s", count, "sampled" if is_sample else "fetched")


if __name__ == "__main__":
    main()
