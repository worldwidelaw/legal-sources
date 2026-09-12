#!/usr/bin/env python3
"""
INTL/LCIA-ChallengeDecisions — LCIA Arbitrator Challenge Decision Database

Fetches publicly available arbitrator challenge decisions from LCIA.

Strategy:
  - Single page at /challenge-decision-database.aspx has two HTML tables
  - Table 0: Decisions 1-24 (Oct 2017 – Mar 2022)
  - Table 1: 32 decisions with case references (Oct 2010 – Jul 2017)
  - Each row links to a PDF via /media/download.aspx?MediaId=NNNN
  - Extract text from PDFs via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.LCIA-ChallengeDecisions")

DATABASE_URL = "https://www.lcia.org/challenge-decision-database.aspx"
BASE_URL = "https://www.lcia.org"


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages).strip()
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
    # Fallback to pdfminer
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(io.BytesIO(pdf_bytes))
        if text and len(text.strip()) > 50:
            return text.strip()
    except Exception as e:
        logger.debug(f"pdfminer failed: {e}")
    return ""


def parse_date(date_str: str) -> str:
    """Parse DD/MM/YYYY date to ISO format."""
    date_str = date_str.strip()
    # Handle malformed dates like "31/072012"
    m = re.match(r'(\d{1,2})/(\d{2})(\d{4})', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1).zfill(2)}"
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class LCIAChallengeDecisionsScraper(BaseScraper):
    """Scraper for LCIA Arbitrator Challenge Decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; +https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
        })

    def _parse_database_page(self) -> list:
        """Fetch and parse the challenge decision database page."""
        resp = self.session.get(DATABASE_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text

        tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
        if len(tables) < 2:
            logger.warning(f"Expected 2 tables, found {len(tables)}")

        decisions = []
        for table_idx, table_html in enumerate(tables):
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
            for row in rows[1:]:  # Skip header
                cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL)
                if len(cells) < 6:
                    continue

                ref = re.sub(r'<[^>]+>', '', cells[0]).strip()
                date_str = re.sub(r'<[^>]+>', '', cells[1]).strip()
                party = re.sub(r'<[^>]+>', '', cells[2]).strip()
                arbitrator = re.sub(r'<[^>]+>', '', cells[3]).strip()
                outcome = re.sub(r'<[^>]+>', '', cells[4]).strip()

                link_match = re.search(r'href="([^"]+)"', cells[5])
                pdf_url = link_match.group(1) if link_match else None

                if not pdf_url:
                    continue

                # Normalize URL
                if not pdf_url.startswith("http"):
                    pdf_url = BASE_URL + pdf_url

                # Extract MediaId for unique ID
                media_match = re.search(r'MediaId=(\d+)', pdf_url)
                media_id = media_match.group(1) if media_match else ref

                decisions.append({
                    "ref": ref,
                    "date_str": date_str,
                    "party": party,
                    "arbitrator": arbitrator,
                    "outcome": outcome,
                    "pdf_url": pdf_url,
                    "media_id": media_id,
                    "table_idx": table_idx,
                })

        return decisions

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text."""
        try:
            resp = self.session.get(pdf_url, timeout=60)
            if resp.status_code == 200 and len(resp.content) > 100:
                return extract_pdf_text(resp.content)
        except requests.RequestException as e:
            logger.warning(f"Failed to download {pdf_url}: {e}")
        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all challenge decisions."""
        decisions = self._parse_database_page()
        logger.info(f"Found {len(decisions)} decisions")

        for i, dec in enumerate(decisions):
            time.sleep(1.5)
            text = self._download_pdf_text(dec["pdf_url"])
            if text and len(text) > 50:
                dec["text"] = text
                logger.info(f"  [{i+1}/{len(decisions)}] {dec['ref']} — {len(text)} chars")
                yield dec
            else:
                logger.warning(f"  [{i+1}/{len(decisions)}] {dec['ref']} — no text extracted")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions since a date (checks all, filters by date)."""
        for dec in self.fetch_all():
            iso_date = parse_date(dec.get("date_str", ""))
            if iso_date and iso_date >= since.strftime("%Y-%m-%d"):
                yield dec

    def normalize(self, raw: dict) -> dict:
        """Transform a raw decision into a standardized record."""
        text = raw.get("text", "")
        if not text or len(text.strip()) < 50:
            return None

        iso_date = parse_date(raw.get("date_str", ""))
        ref = raw["ref"]

        # Build descriptive title
        title = f"LCIA Challenge Decision — {ref} ({raw.get('date_str', 'undated')})"

        return {
            "_id": f"lcia-challenge-{raw['media_id']}",
            "_source": "INTL/LCIA-ChallengeDecisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": raw["pdf_url"],
            "case_reference": ref,
            "challenging_party": raw.get("party"),
            "arbitrator_challenged": raw.get("arbitrator"),
            "outcome": raw.get("outcome"),
            "language": "en",
            "institution": "LCIA",
        }


# ── CLI ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="LCIA Challenge Decisions scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap")
    args = parser.parse_args()

    scraper = LCIAChallengeDecisionsScraper()

    if args.command == "test":
        print("Testing connectivity to lcia.org...")
        try:
            resp = scraper.session.get(DATABASE_URL, timeout=15)
            print(f"OK: HTTP {resp.status_code}, {len(resp.text)} bytes")
            decisions = scraper._parse_database_page()
            print(f"Found {len(decisions)} decisions")
            if decisions:
                print(f"  First: {decisions[0]['ref']} ({decisions[0]['date_str']})")
                print(f"  Last: {decisions[-1]['ref']} ({decisions[-1]['date_str']})")
            print("Test PASSED")
        except Exception as e:
            print(f"Test FAILED: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            logger.info("Running sample bootstrap...")
            records = []
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                if record and record.get("text"):
                    records.append(record)
                    logger.info(f"  [{len(records)}] {record['title'][:70]}... ({len(record['text'])} chars)")
                    if len(records) >= 15:
                        break

            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            for i, rec in enumerate(records):
                path = sample_dir / f"{i+1:03d}_{rec['_id']}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(records)} sample records to {sample_dir}")
            for rec in records:
                text_len = len(rec.get("text", ""))
                print(f"  {rec['_id']}: {rec['title'][:70]} ({text_len} chars)")
        else:
            logger.info("Running full bootstrap...")
            stats = scraper.bootstrap(sample_mode=False)
            print(json.dumps(stats, indent=2))

    elif args.command == "update":
        logger.info("Running update...")
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2))
