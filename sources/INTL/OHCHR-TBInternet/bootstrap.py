#!/usr/bin/env python3
"""
INTL/OHCHR-TBInternet -- UN Treaty Body Documentation (UHRI Export)

Fetches country-specific observations and recommendations from the
Universal Human Rights Index (UHRI) full JSON export. Covers all 10 UN
Treaty Bodies, Special Procedures, and the Universal Periodic Review.

Strategy:
  - Stream the UHRI JSON export (~365MB) from dataex.ohchr.org
  - Parse records incrementally using ijson (streaming JSON parser)
  - Strip HTML tags from text fields
  - ~200,000+ annotations with full text, themes, SDGs

Usage:
  python bootstrap.py bootstrap          # Full initial pull (streams 365MB)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import time
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OHCHR-TBInternet")

EXPORT_URL = "https://dataex.ohchr.org/uhri/export-results/export-full-en.json"

HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")


def strip_html(text: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not text:
        return ""
    text = HTML_TAG_RE.sub(" ", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def stream_json_array(url: str, session: requests.Session, max_records: int = 0):
    """
    Stream a large JSON array from a URL, yielding parsed objects one at a time.
    Uses a chunked download + incremental JSON decoder approach.
    """
    logger.info(f"Starting streaming download from {url}")
    resp = session.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    # Manual incremental JSON array parser
    # The file is a JSON array: [{...}, {...}, ...]
    # We read chunks, accumulate a buffer, and extract complete objects
    buffer = ""
    depth = 0
    in_string = False
    escape_next = False
    obj_start = -1
    yielded = 0

    for chunk in resp.iter_content(chunk_size=65536, decode_unicode=True):
        if chunk is None:
            continue
        buffer += chunk

        i = 0
        while i < len(buffer):
            c = buffer[i]

            if escape_next:
                escape_next = False
                i += 1
                continue

            if c == '\\' and in_string:
                escape_next = True
                i += 1
                continue

            if c == '"' and not escape_next:
                in_string = not in_string
                i += 1
                continue

            if in_string:
                i += 1
                continue

            if c == '{':
                if depth == 0:
                    obj_start = i
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0 and obj_start >= 0:
                    obj_str = buffer[obj_start:i + 1]
                    try:
                        obj = json.loads(obj_str)
                        yield obj
                        yielded += 1
                        if max_records > 0 and yielded >= max_records:
                            resp.close()
                            return
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse object: {e}")
                    obj_start = -1
                    # Trim buffer up to current position
                    buffer = buffer[i + 1:]
                    i = 0
                    continue

            i += 1

    logger.info(f"Stream complete: {yielded} records parsed")


class OHCHRTBInternetScraper(BaseScraper):
    """
    Scraper for INTL/OHCHR-TBInternet -- UN Treaty Body Documentation.
    Country: INTL
    URL: https://tbinternet.ohchr.org/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def normalize(self, raw: dict) -> dict:
        """Transform a UHRI annotation record into standard schema."""
        annotation_id = raw.get("AnnotationId", "")
        symbol = raw.get("Symbol", "")
        annotation_type = raw.get("AnnotationType", "").strip("- ")
        body = raw.get("Body", "").strip("- ")
        text = strip_html(raw.get("Text", ""))
        countries = raw.get("Countries", [])
        themes = raw.get("Themes", [])
        sdgs = raw.get("Sdgs", [])
        affected = raw.get("AffectedPersons", [])
        regions = raw.get("Regions", [])

        # Build title from symbol + annotation type
        title_parts = []
        if symbol:
            title_parts.append(symbol)
        if annotation_type:
            title_parts.append(annotation_type)
        if body:
            title_parts.append(f"({body})")
        title = " — ".join(title_parts[:2])
        if body:
            title += f" ({body})"

        # Parse date
        date_str = raw.get("PublicationDate")
        date = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                date = date_str[:10] if len(date_str) >= 10 else None

        # UPR-specific fields
        upr_cycle = raw.get("UprCycle")
        upr_positions = raw.get("UprPositions", [])
        upr_recommending = raw.get("UprRecommendingCountry", [])

        return {
            "_id": annotation_id,
            "_source": "INTL/OHCHR-TBInternet",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://uhri.ohchr.org/en/search-human-rights-recommendations",
            "symbol": symbol,
            "document_id": raw.get("DocumentId", ""),
            "annotation_type": annotation_type,
            "treaty_body": body,
            "countries": countries,
            "regions": regions,
            "themes": themes,
            "sdgs": sdgs,
            "affected_persons": affected,
            "upr_cycle": upr_cycle,
            "upr_positions": upr_positions,
            "upr_recommending_countries": upr_recommending,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all UHRI annotations by streaming the full JSON export."""
        yielded = 0
        for raw in stream_json_array(EXPORT_URL, self.session):
            text = strip_html(raw.get("Text", ""))
            if len(text) < 50:
                continue
            yield raw
            yielded += 1
            if yielded % 5000 == 0:
                logger.info(f"Streamed {yielded} records so far")
        logger.info(f"fetch_all complete: {yielded} records")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield annotations published after `since` date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            since_dt = datetime.strptime(since, "%Y-%m-%d")

        yielded = 0
        for raw in stream_json_array(EXPORT_URL, self.session):
            pub_date = raw.get("PublicationDate", "")
            if pub_date:
                try:
                    rec_dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                    if rec_dt < since_dt:
                        continue
                except (ValueError, AttributeError):
                    pass
            text = strip_html(raw.get("Text", ""))
            if len(text) < 50:
                continue
            yield raw
            yielded += 1
        logger.info(f"fetch_updates complete: {yielded} records since {since}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="INTL/OHCHR-TBInternet -- UN Treaty Body Documentation"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = OHCHRTBInternetScraper()

    if args.command == "test":
        logger.info("Testing UHRI export connectivity...")
        try:
            resp = scraper.session.head(EXPORT_URL, timeout=30)
            resp.raise_for_status()
            size_mb = int(resp.headers.get("Content-Length", 0)) / (1024 * 1024)
            logger.info(f"Export available: {size_mb:.0f} MB")

            # Fetch first 3 records
            count = 0
            for raw in stream_json_array(EXPORT_URL, scraper.session, max_records=3):
                normalized = scraper.normalize(raw)
                logger.info(f"Record: {normalized['title']}")
                logger.info(f"  Text length: {len(normalized['text'])} chars")
                logger.info(f"  Date: {normalized['date']}")
                logger.info(f"  Countries: {normalized['countries']}")
                count += 1

            if count > 0:
                logger.info("Connectivity test passed!")
            else:
                logger.warning("No records parsed")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
