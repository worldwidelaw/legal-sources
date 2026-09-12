#!/usr/bin/env python3
"""
IN/LawCommission -- Law Commission of India Reports

Fetches all 289 Law Commission reports (1955-2024) from the official NIC website.
PDFs are hosted on S3WaaS CDN. Newer reports have selectable text; older ones
are scanned images and are skipped.

Strategy:
  - Scrape 22 commission sub-pages (static HTML tables)
  - Parse report metadata: number, title, year, PDF URL(s)
  - Download PDFs and extract full text via common.pdf_extract
  - Skip reports where no text can be extracted (scanned PDFs)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch latest (22nd) commission only
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.LawCommission")

BASE_URL = "https://lawcommissionofindia.nic.in"

# Commission sub-pages in reverse chronological order (newest first for --sample)
COMMISSIONS = [
    ("Twenty-Second", "report_twentysecond", "2020-2024", "278-289"),
    ("Twenty-First", "report_twentyfirst", "2015-2018", "263-277"),
    ("Twentieth", "report_twentieth", "2013-2015", "244-262"),
    ("Nineteenth", "report_nineteenth", "2009-2012", "235-243"),
    ("Eighteenth", "report_eighteenth", "2007-2009", "202-234"),
    ("Seventeenth", "report_seventeenth", "2003-2006", "186-201"),
    ("Sixteenth", "report_sixteenth", "2000-2003", "175-185"),
    ("Fifteenth", "report_fifteenth", "1997-2000", "157-174"),
    ("Fourteenth", "report_fourteenth", "1995-1997", "154-156"),
    ("Thirteenth", "report_thirteenth", "1991-1994", "144-153"),
    ("Twelfth", "report_twelfth", "1988-1991", "132-143"),
    ("Eleventh", "report_eleventh", "1985-1988", "114-131"),
    ("Tenth", "report_tenth", "1981-1985", "88-113"),
    ("Ninth", "report_ninth", "1979-1980", "81-87"),
    ("Eighth", "report_eighth", "1977-1979", "71-80"),
    ("Seventh", "report_seventh", "1974-1977", "62-70"),
    ("Sixth", "report_sixth", "1971-1974", "45-61"),
    ("Fifth", "report_fifth", "1968-1971", "39-44"),
    ("Fourth", "report_fourth", "1964-1968", "29-38"),
    ("Third", "report_third", "1961-1964", "23-28"),
    ("Second", "report_second", "1958-1961", "15-22"),
    ("First", "report_first", "1955-1958", "1-14"),
]

# Minimum chars to consider text extraction successful
MIN_TEXT_CHARS = 200


class LawCommissionScraper(BaseScraper):
    """Scraper for IN/LawCommission -- Law Commission of India Reports."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Referer": f"{BASE_URL}/law-commission-reports/",
        })

    def _parse_date(self, raw: str) -> Optional[str]:
        """Parse dates like '17thMarch 2023' or 'March 2023' to ISO 8601."""
        if not raw:
            return None
        # Remove ordinal suffixes glued to day number
        cleaned = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1 ", raw)
        cleaned = cleaned.strip()
        for fmt in ("%d %B %Y", "%B %Y", "%d %b %Y", "%b %Y", "%Y"):
            try:
                return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _scrape_commission_page(self, commission_name: str, page_slug: str,
                                 tenure: str) -> List[Dict]:
        """Scrape a single commission's page for all report entries.

        Table structure: Col 0 = report number, Col 1 = title/subject,
        Col 2 = date, Col 3 = PDF link(s).
        Supplementary docs use '–' or empty first column.
        """
        url = f"{BASE_URL}/{page_slug}/"
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", page_slug, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        records = []
        last_report_num = None

        rows = soup.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            cell_texts = [c.get_text(strip=True) for c in cells]

            # Find PDF links in this row
            pdf_links = []
            for a in row.find_all("a", href=True):
                href = a["href"]
                if ".pdf" in href:
                    full_url = href if href.startswith("http") else urljoin(BASE_URL + "/", href)
                    pdf_links.append(full_url)

            if not pdf_links:
                continue

            # Column 0: report number
            num_text = cell_texts[0].strip().replace("–", "").replace("-", "")
            report_num = None
            if num_text.isdigit():
                report_num = int(num_text)
                last_report_num = report_num

            # Column 1: title/subject
            title = cell_texts[1] if len(cell_texts) > 1 else ""
            if not title:
                title = f"Report {report_num}" if report_num else "Supplementary Document"

            # Column 2: date
            date_raw = cell_texts[2] if len(cell_texts) > 2 else ""
            date_iso = self._parse_date(date_raw)

            # For supplementary docs (Dissent Note, etc.), link to parent report
            is_supplementary = report_num is None

            records.append({
                "report_number": report_num,
                "parent_report": last_report_num if is_supplementary else None,
                "title": title,
                "date": date_iso,
                "commission": commission_name,
                "tenure": tenure,
                "pdf_url": pdf_links[0],
                "all_pdf_urls": pdf_links,
            })

        logger.info("%s (%s): %d reports found", commission_name, tenure, len(records))
        return records

    def _make_id(self, rec: dict) -> str:
        """Create unique ID from report number or parent+title hash."""
        if rec.get("report_number"):
            return f"LCI-{rec['report_number']}"
        # Supplementary doc: use parent report number + title slug
        parent = rec.get("parent_report", "X")
        slug = re.sub(r"[^a-z0-9]+", "-", rec["title"].lower())[:30].strip("-")
        return f"LCI-{parent}-{slug}"

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "IN/LawCommission",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "report_number": raw.get("report_number"),
            "commission": raw.get("commission", ""),
            "tenure": raw.get("tenure", ""),
        }

    def _process_record(self, rec: dict) -> Optional[dict]:
        """Process a single record: download PDF and extract text."""
        doc_id = self._make_id(rec)

        text = extract_pdf_markdown(
            source="IN/LawCommission",
            source_id=doc_id,
            pdf_url=rec["pdf_url"],
            table="doctrine",
        )

        if not text or len(text) < MIN_TEXT_CHARS:
            logger.info("Skipping %s (no extractable text): %s", doc_id, rec["title"][:60])
            return None

        raw = {
            "_id": doc_id,
            "title": rec["title"],
            "text": text,
            "date": rec.get("date"),
            "pdf_url": rec["pdf_url"],
            "report_number": rec.get("report_number"),
            "commission": rec.get("commission", ""),
            "tenure": rec.get("tenure", ""),
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Law Commission reports across all 22 commissions."""
        for name, slug, tenure, _ in COMMISSIONS:
            logger.info("Fetching commission: %s (%s)", name, tenure)
            records = self._scrape_commission_page(name, slug, tenure)
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch only the latest (22nd) commission for updates."""
        name, slug, tenure, _ = COMMISSIONS[0]
        logger.info("Fetching latest commission: %s (%s)", name, tenure)
        records = self._scrape_commission_page(name, slug, tenure)
        for rec in records:
            doc = self._process_record(rec)
            if doc:
                yield doc


# ── CLI entry point ─────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/LawCommission bootstrap scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only a small sample (15 records)")
    args = parser.parse_args()

    scraper = LawCommissionScraper()

    if args.command == "test":
        logger.info("Testing connectivity to lawcommissionofindia.nic.in ...")
        try:
            resp = scraper.session.get(f"{BASE_URL}/law-commission-reports/", timeout=15)
            resp.raise_for_status()
            logger.info("OK — status %d, %d bytes", resp.status_code, len(resp.text))
        except Exception as e:
            logger.error("FAILED: %s", e)
            sys.exit(1)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else float("inf")

    gen = scraper.fetch_all() if args.command == "bootstrap" else scraper.fetch_updates()

    for doc in gen:
        if count >= limit:
            break
        fname = f"{doc['_id']}.json".replace("/", "_")
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info("[%d] %s — %d chars", count, doc["title"][:60], len(doc.get("text", "")))

    logger.info("Done. %d records saved to %s", count, sample_dir)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
