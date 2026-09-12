#!/usr/bin/env python3
"""
BB/FTC-Decisions -- Barbados Fair Trading Commission Decisions & Orders

Fetches the full text of Commission decisions, orders, merger
determinations, and standards-of-service rulings issued by the Barbados
Fair Trading Commission (utility regulation, competition, consumer
protection).

Strategy:
  The FTC website (ftc.gov.bb, Joomla CMS) is frequently placed in
  "offline for maintenance" mode, which blocks the PHP application
  (index.php) but still serves the static decision PDFs under /library/
  directly. We therefore:
    1. Enumerate decision PDF URLs from the Wayback Machine CDX index
       (a stable, public listing of every /library/*.pdf ever published).
    2. Filter to genuine Commission decisions / orders / determinations.
    3. Download each PDF from the LIVE site (www.ftc.gov.bb/library/...)
       and extract full text with pdfplumber.

  Documents range from 2002 to the present (2025). Older scanned PDFs
  with no extractable text layer are skipped.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import io
import time
import logging
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BB.FTC-Decisions")

LIVE_BASE = "https://www.ftc.gov.bb/library/"
CDX_URL = "http://web.archive.org/cdx/search/cdx"

MIN_TEXT_CHARS = 200

# A path is treated as a Commission decision (case_law) if it contains one of
# these tokens ...
INCLUDE_TOKENS = ("decision", "determination", "ruling", "order")
# ... unless it is clearly a form, guideline, party filing, or procedural doc.
EXCLUDE_TOKENS = (
    "form", "guideline", "notification", "procedure", "_rules", "rules_",
    "regulation", "consultation", "affidavit", "notice_of_motion",
    "notice_motion", "speech", "annual_report", "_si", "checklist",
    "newsletter", "brochure", "application_for",
)

# YYYY-MM-DD date prefix on the file's basename
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    """Collapse excessive whitespace, strip stray control chars."""
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(rel_path: str) -> Optional[str]:
    """Extract an ISO date from the filename basename (YYYY-MM-DD prefix)."""
    base = rel_path.rsplit("/", 1)[-1]
    m = DATE_RE.search(base)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    try:
        datetime(int(y), int(mo), int(d))
    except ValueError:
        return None
    return f"{y}-{mo}-{d}"


def make_title(rel_path: str) -> str:
    """Build a human-readable title from the relative PDF path."""
    base = rel_path.rsplit("/", 1)[-1]
    base = re.sub(r"\.pdf$", "", base, flags=re.I)
    base = DATE_RE.sub("", base).lstrip("_- ")
    base = base.replace("_", " ").replace("-", " ")
    base = re.sub(r"\s+", " ", base).strip()
    return base.title() if base else "FTC Decision"


def is_decision(rel_path: str) -> bool:
    """True if the path looks like a genuine FTC decision/order/determination."""
    low = rel_path.lower()
    if any(tok in low for tok in EXCLUDE_TOKENS):
        # Merger reports live under mergers/ and are genuine determinations
        if low.startswith("mergers/") and ("report" in low or "decision" in low):
            return True
        return False
    if any(tok in low for tok in INCLUDE_TOKENS):
        return True
    if low.startswith("mergers/") and ("report" in low or "decision" in low):
        return True
    return False


class FTCDecisionsScraper(BaseScraper):
    """
    Scraper for BB/FTC-Decisions — Barbados Fair Trading Commission.
    Country: BB
    URL: https://www.ftc.gov.bb/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) LegalDataHunter/1.0 "
                          "(research; https://github.com/ZachLaik/LegalDataHunter)",
        })
        import urllib3
        urllib3.disable_warnings()

    def _discover(self) -> List[Dict[str, Optional[str]]]:
        """
        Enumerate FTC /library/ decision PDFs via the Wayback CDX index.
        Returns a list of dicts: {rel_path, url, date}, deduped & sorted
        most-recent first.
        """
        params = {
            "url": "ftc.gov.bb/library",
            "matchType": "prefix",
            "output": "text",
            "fl": "original",
            "collapse": "urlkey",
            "limit": "10000",
        }
        try:
            r = self.session.get(CDX_URL, params=params, timeout=90)
            r.raise_for_status()
            lines = r.text.splitlines()
        except Exception as e:
            logger.error(f"CDX discovery failed: {e}")
            lines = []

        seen = {}
        for raw in lines:
            u = raw.strip()
            low = u.lower()
            if "/library/" not in low or not low.endswith(".pdf"):
                continue
            rel = u.split("/library/", 1)[1]
            rel = urllib.parse.unquote(rel)
            # Skip mojibake / non-ASCII-corrupted archive artifacts
            if any(ord(c) > 0x2122 for c in rel):
                continue
            if not is_decision(rel):
                continue
            key = rel.lower()
            if key in seen:
                continue
            seen[key] = {
                "rel_path": rel,
                "url": LIVE_BASE + urllib.parse.quote(rel),
                "date": parse_date(rel),
            }

        items = list(seen.values())
        items.sort(key=lambda d: (d["date"] or "0000-00-00"), reverse=True)
        logger.info(f"Discovered {len(items)} candidate FTC decision PDFs")
        return items

    def _download_and_extract(self, item: Dict) -> Optional[dict]:
        try:
            r = self.session.get(item["url"], timeout=90)
            if r.status_code != 200:
                logger.debug(f"HTTP {r.status_code} for {item['url']}")
                return None
            if "pdf" not in r.headers.get("Content-Type", "").lower() \
                    and not r.content[:4] == b"%PDF":
                logger.debug(f"Not a PDF: {item['url']}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {item['url']}: {e}")
            return None

        text = clean_text(extract_pdf_text(r.content))
        if len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text ({len(text)} chars): {item['url']}")
            return None

        return {
            "rel_path": item["rel_path"],
            "url": item["url"],
            "title": make_title(item["rel_path"]),
            "text": text,
            "date": item["date"],
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["url"],
            "_source": "BB/FTC-Decisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "jurisdiction": "BB",
            "court": "Barbados Fair Trading Commission",
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        items = self._discover()
        yielded = 0
        for item in items:
            result = self._download_and_extract(item)
            if result:
                yield result
                yielded += 1
                if yielded % 10 == 0:
                    logger.info(f"Extracted {yielded} decisions...")
            time.sleep(1.0)
        logger.info(f"fetch_all complete: {yielded} decisions with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        items = self._discover()
        yielded = 0
        for item in items:
            if item["date"] and item["date"] >= since:
                result = self._download_and_extract(item)
                if result:
                    yield result
                    yielded += 1
                time.sleep(1.0)
        logger.info(f"fetch_updates complete: {yielded} decisions since {since}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="BB/FTC-Decisions — Barbados Fair Trading Commission"
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

    scraper = FTCDecisionsScraper()

    if args.command == "test":
        logger.info("Testing FTC connectivity...")
        items = scraper._discover()
        if not items:
            logger.error("No decision PDFs discovered")
            sys.exit(1)
        logger.info(f"Top candidate: {items[0]['rel_path']}")
        result = scraper._download_and_extract(items[0])
        if result:
            logger.info(f"Title: {result['title']}")
            logger.info(f"Date: {result['date']}")
            logger.info(f"Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from top candidate")
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
