#!/usr/bin/env python3
"""
WF/JOWF -- Journal Officiel de Wallis-et-Futuna

Fetches legislation from the official journal of Wallis-et-Futuna.
Each issue is a PDF containing arrêtés, délibérations, and regulatory texts
published by the Préfecture and Assemblée Territoriale.

Strategy:
  - HTML listing page: single page with all available JOWF issues (~36)
  - Each issue is a PDF download link
  - Full text extracted from PDFs via common.pdf_extract

Data Coverage:
  - ~36 issues (semi-monthly, last ~12-13 months)
  - Issues numbered N° 704+ (from Jan 2025 onward on the current listing)

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import urljoin, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.WF.JOWF")

BASE_URL = "https://www.wallis-et-futuna.gouv.fr"
LISTING_URL = f"{BASE_URL}/Publications/Publications-administratives/Journal-Officiel-de-Wallis-et-Futuna-JOWF"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# French month names → month numbers
FRENCH_MONTHS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
    "fevrier": "02", "aout": "08", "decembre": "12",
}

# Regex to extract issue entries from HTML
# The <a> tag has id/class attributes before href, e.g.:
# <a id=... class="fr-link fr-link--download" href="/contenu/telechargement/...">
LINK_RE = re.compile(
    r'<a\s[^>]*href="(/contenu/telechargement/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Extract issue number and date from title like "JOWF N° 739 du 31 janvier 2026"
ISSUE_RE = re.compile(
    r'JOWF\s+N[°o]\s*(\d+)\s+du\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
    re.IGNORECASE,
)

# Also match "Numero Special" issues
SPECIAL_RE = re.compile(
    r'(?:Num[ée]ro\s+Sp[ée]cial|NS)\s*[–-]?\s*(.+?)\.pdf',
    re.IGNORECASE,
)


class JOWFScraper(BaseScraper):
    """
    Scraper for WF/JOWF -- Journal Officiel de Wallis-et-Futuna.
    Country: WF
    URL: https://www.wallis-et-futuna.gouv.fr/Publications/Publications-administratives

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
            "Accept-Language": "fr,en;q=0.9",
        })

    def _fetch_listing(self) -> str:
        """Fetch the JOWF listing page."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(LISTING_URL, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return ""

    def _parse_entries(self, html_content: str) -> List[Dict[str, Any]]:
        """Parse JOWF issue entries from the listing page HTML."""
        entries = []
        seen_ids = set()

        for match in LINK_RE.finditer(html_content):
            pdf_path = match.group(1)
            link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip()
            link_text = html_module.unescape(link_text)

            pdf_url = f"{BASE_URL}{pdf_path}"

            # Try to extract issue number and date
            # Check the surrounding context (strong tag before the link)
            # Also check the link text itself and the href filename
            filename = pdf_path.split("/file/")[-1] if "/file/" in pdf_path else ""
            filename = html_module.unescape(filename.replace("%20", " "))

            search_text = f"{link_text} {filename}"
            issue_match = ISSUE_RE.search(search_text)

            if issue_match:
                issue_num = issue_match.group(1)
                day = issue_match.group(2).zfill(2)
                month_name = issue_match.group(3).lower()
                year = issue_match.group(4)
                month = FRENCH_MONTHS.get(month_name, "01")
                date_iso = f"{year}-{month}-{day}"
                title = f"JOWF N° {issue_num} du {day}/{month}/{year}"
                doc_id = f"JOWF-{issue_num}"
            else:
                # Special issue or unusual format
                doc_id = f"JOWF-{pdf_path.split('/')[-3]}"
                title = filename.replace(".pdf", "").strip() if filename else link_text
                date_iso = ""
                # Try to extract upload date from link text (DD/MM/YYYY at end)
                date_match = re.search(r'(\d{2})/(\d{2})/(\d{4})\s*$', link_text)
                if date_match:
                    date_iso = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"

            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            entries.append({
                "doc_id": doc_id,
                "title": title,
                "date": date_iso,
                "pdf_url": pdf_url,
                "filename": filename,
                "link_text": link_text,
            })

        logger.info(f"Found {len(entries)} JOWF issues on listing page")
        return entries

    def _extract_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        try:
            self.rate_limiter.wait()
            # URL-encode the path properly for French characters
            resp = self.session.get(pdf_url, timeout=120, headers={
                "Referer": LISTING_URL,
                "Accept": "application/pdf,*/*",
            })
            if resp.status_code != 200:
                logger.warning(f"PDF download failed {doc_id}: HTTP {resp.status_code}")
                return ""
            if not resp.content or resp.content[:4] != b"%PDF":
                logger.warning(f"Invalid PDF content for {doc_id}")
                return ""
        except Exception as e:
            logger.warning(f"PDF download error {doc_id}: {e}")
            return ""

        text = extract_pdf_markdown(
            source="WF/JOWF",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        )
        return text or ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all JOWF issues from the listing page."""
        logger.info("Starting full JOWF crawl...")

        html_content = self._fetch_listing()
        if not html_content:
            logger.error("Could not fetch listing page")
            return

        entries = self._parse_entries(html_content)

        for entry in entries:
            pdf_url = entry["pdf_url"]
            doc_id = entry["doc_id"]

            text = self._extract_pdf_text(pdf_url, doc_id)
            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
                continue

            entry["full_text"] = text
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent issues (all on single page, filter by date)."""
        logger.info(f"Fetching updates since {since.isoformat()}...")
        since_date = since.date()

        html_content = self._fetch_listing()
        if not html_content:
            return

        entries = self._parse_entries(html_content)

        for entry in entries:
            if entry["date"]:
                try:
                    entry_date = datetime.strptime(entry["date"], "%Y-%m-%d").date()
                    if entry_date < since_date:
                        continue
                except ValueError:
                    pass

            pdf_url = entry["pdf_url"]
            doc_id = entry["doc_id"]

            text = self._extract_pdf_text(pdf_url, doc_id)
            if text and len(text) >= 50:
                entry["full_text"] = text
                yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "WF/JOWF",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("pdf_url", ""),
            "filename": raw.get("filename", ""),
            "language": "fr",
        }

    def test_api(self):
        """Quick connectivity and listing test."""
        print("Testing JOWF site...")

        html = self._fetch_listing()
        if not html:
            print("ERROR: Could not fetch listing page")
            return

        entries = self._parse_entries(html)
        print(f"Found {len(entries)} JOWF issues")

        for e in entries[:3]:
            print(f"\n  ID: {e['doc_id']}")
            print(f"  Title: {e['title']}")
            print(f"  Date: {e['date']}")
            print(f"  PDF: {e['pdf_url']}")

        if entries:
            print(f"\nTesting PDF extraction for first entry...")
            text = self._extract_pdf_text(entries[0]["pdf_url"], entries[0]["doc_id"])
            if text:
                print(f"  Text: {len(text)} chars")
                print(f"  Preview: {text[:200]}...")
            else:
                print("  WARNING: No text extracted")

        print("\nTest complete!")


def main():
    scraper = JOWFScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
