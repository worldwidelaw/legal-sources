#!/usr/bin/env python3
"""
PM/JOSPM -- Journal Officiel de Saint-Pierre-et-Miquelon

Fetches legislation from the official journal of Saint-Pierre-et-Miquelon.
Documents include deliberations, decisions/arrêtés, official journals, and
legal notices from the Conseil Territorial and sub-sites (Prefecture, Mairies).

Strategy:
  - Paginated HTML listing: GET index.php?npage={N}&f_deliberations=checked&...
  - 5 documents per page, metadata in HTML <div class="cadre_doc">
  - Full text via PDF download: GET doc_jo/{filename}.pdf
  - Text extracted from PDFs via common.pdf_extract

Data Coverage:
  - ~9,600 documents from 2009 to present
  - 4 sub-sites: Conseil Territorial, Prefecture, Mairie SP, Mairie ML

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
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PM.JOSPM")

BASE_URL = "https://www.jo-spm.fr/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Sub-sites to crawl
SUBSITES = [
    {"path": "", "name": "Conseil Territorial", "prefix": "CT"},
    {"path": "prefecture/", "name": "Préfecture", "prefix": "PREF"},
    {"path": "mairie-SP/", "name": "Mairie Saint-Pierre", "prefix": "MSP"},
    {"path": "mairie-ML/", "name": "Mairie Miquelon-Langlade", "prefix": "MML"},
]

# All document type checkboxes
DOC_TYPE_PARAMS = {
    "f_deliberations": "checked",
    "f_decisions_et_arretes": "checked",
    "f_journaux_officiels": "checked",
    "f_annonces_legales": "checked",
}

# Regex to parse document entries from listing HTML
DOC_ENTRY_RE = re.compile(
    r'<div\s+class="cadre_doc">(.*?)</div>\s*(?=<div\s+class="cadre_doc">|<div\s+class="cadre_page|$)',
    re.DOTALL
)

# Extract PDF link
PDF_LINK_RE = re.compile(
    r'<a\s+href="(doc_jo/[^"]+\.pdf)"[^>]*>',
    re.IGNORECASE
)

# Extract date and title from consecutive bold spans
# <span style="...font-weight:600...">14/04/2026</span>
# <span style="...font-weight:600...">Annonce légale</span>
DATE_RE = re.compile(
    r'<span[^>]*font-weight:\s*600[^>]*>(\d{2}/\d{2}/\d{4})</span>',
    re.IGNORECASE
)

# Extract document type (second bold span after date)
TITLE_RE = re.compile(
    r'<span[^>]*font-weight:\s*600[^>]*>\d{2}/\d{2}/\d{4}</span>\s*'
    r'<span[^>]*font-weight:\s*600[^>]*>(.*?)</span>',
    re.DOTALL | re.IGNORECASE
)

# Extract subject from div with numeric ID attribute
SUBJECT_RE = re.compile(
    r'<div\s+id="(\d+)"\s+style="[^"]*"[^>]*>(.*?)</div>',
    re.DOTALL
)


class JOSPMScraper(BaseScraper):
    """
    Scraper for PM/JOSPM -- Journal Officiel de Saint-Pierre-et-Miquelon.
    Country: PM
    URL: https://www.jo-spm.fr/

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

    def _fetch_listing_page(self, subsite_path: str, page: int, timeout: int = 30) -> str:
        """Fetch a listing page from a sub-site."""
        url = urljoin(BASE_URL, f"{subsite_path}index.php")
        params = {"npage": str(page)}
        params.update(DOC_TYPE_PARAMS)
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {subsite_path} page {page}: {e}")
            return ""

    def _get_total_count(self, html_content: str) -> int:
        """Extract total document count from listing page."""
        # Look for "X documents" or similar count indicator
        m = re.search(r'(\d[\d\s]*)\s*document', html_content, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(" ", ""))
        # Count pages from pagination
        pages = re.findall(r'npage=(\d+)', html_content)
        if pages:
            return max(int(p) for p in pages) * 5
        return 0

    def _parse_entries(self, html_content: str, subsite_path: str) -> List[Dict[str, Any]]:
        """Parse document entries from a listing page."""
        entries = []

        # Split by cadre_doc divs (class may include extra classes like "ombrage coins_arrondis")
        blocks = re.split(r'<div\s+class="cadre_doc[^"]*">', html_content)

        for block in blocks[1:]:  # skip first (before first entry)
            entry = {}

            # Extract PDF link
            pdf_match = PDF_LINK_RE.search(block)
            if pdf_match:
                pdf_path = pdf_match.group(1)
                entry["pdf_url"] = urljoin(BASE_URL, f"{subsite_path}{pdf_path}")
                entry["pdf_filename"] = pdf_path.split("/")[-1]

            # Extract date
            date_match = DATE_RE.search(block)
            if date_match:
                entry["date_raw"] = date_match.group(1)

            # Extract document type/title
            title_match = TITLE_RE.search(block)
            if title_match:
                title = re.sub(r'<[^>]+>', '', title_match.group(1))
                entry["doc_type_title"] = html_module.unescape(title.strip())

            # Extract subject from div with numeric ID
            subject_match = SUBJECT_RE.search(block)
            if subject_match:
                entry["db_id"] = subject_match.group(1)
                subject = re.sub(r'<[^>]+>', '', subject_match.group(2))
                entry["subject"] = html_module.unescape(subject.strip())

            if entry.get("pdf_url") or entry.get("db_id"):
                entries.append(entry)

        return entries

    def _extract_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download PDF with proper headers and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=60, headers={
                "Referer": BASE_URL,
                "Accept": "application/pdf,*/*",
            })
            if resp.status_code != 200:
                logger.warning(f"PDF download failed {pdf_url}: HTTP {resp.status_code}")
                return ""
            if not resp.content or resp.content[:4] != b"%PDF":
                logger.warning(f"Invalid PDF content from {pdf_url}")
                return ""
        except Exception as e:
            logger.warning(f"PDF download error {pdf_url}: {e}")
            return ""

        text = extract_pdf_markdown(
            source="PM/JOSPM",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        )
        return text or ""

    def _crawl_subsite(self, subsite: Dict[str, str]) -> Generator[Dict[str, Any], None, None]:
        """Crawl all pages of a sub-site, yielding document metadata + text."""
        path = subsite["path"]
        prefix = subsite["prefix"]
        name = subsite["name"]

        logger.info(f"Crawling sub-site: {name} ({path or '/'})")

        # Get first page to determine total
        first_page = self._fetch_listing_page(path, 1)
        if not first_page:
            logger.error(f"Could not access sub-site: {name}")
            return

        total = self._get_total_count(first_page)
        max_pages = (total // 5) + 2 if total > 0 else 2000
        logger.info(f"{name}: ~{total} documents, ~{max_pages} pages")

        page = 1
        consecutive_empty = 0

        while page <= max_pages:
            if page == 1:
                html_content = first_page
            else:
                html_content = self._fetch_listing_page(path, page)

            if not html_content:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                continue

            entries = self._parse_entries(html_content, path)
            if not entries:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                continue

            consecutive_empty = 0

            for entry in entries:
                pdf_url = entry.get("pdf_url", "")
                db_id = entry.get("db_id", entry.get("pdf_filename", f"p{page}"))
                doc_id = f"{prefix}-{db_id}"

                if not pdf_url:
                    continue

                # Extract text from PDF
                text = self._extract_pdf_text(pdf_url, doc_id)
                if not text or len(text) < 20:
                    logger.warning(f"Insufficient text for {doc_id}: {len(text)} chars")
                    continue

                entry["full_text"] = text
                entry["doc_id"] = doc_id
                entry["subsite"] = name
                yield entry

            page += 1

            if page % 50 == 0:
                logger.info(f"{name}: processed page {page}/{max_pages}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all sub-sites."""
        logger.info("Starting full JOSPM crawl...")
        for subsite in SUBSITES:
            yield from self._crawl_subsite(subsite)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents from page 1 of each sub-site (most recent)."""
        logger.info(f"Fetching updates since {since.isoformat()}...")
        since_date = since.date()

        for subsite in SUBSITES:
            path = subsite["path"]
            prefix = subsite["prefix"]
            page = 1

            while True:
                html_content = self._fetch_listing_page(path, page)
                if not html_content:
                    break

                entries = self._parse_entries(html_content, path)
                if not entries:
                    break

                found_old = False
                for entry in entries:
                    # Check date
                    date_raw = entry.get("date_raw", "")
                    if date_raw:
                        try:
                            entry_date = datetime.strptime(date_raw, "%d/%m/%Y").date()
                            if entry_date < since_date:
                                found_old = True
                                continue
                        except ValueError:
                            pass

                    pdf_url = entry.get("pdf_url", "")
                    db_id = entry.get("db_id", entry.get("pdf_filename", ""))
                    doc_id = f"{prefix}-{db_id}"

                    if not pdf_url:
                        continue

                    text = self._extract_pdf_text(pdf_url, doc_id)
                    if text and len(text) >= 20:
                        entry["full_text"] = text
                        entry["doc_id"] = doc_id
                        entry["subsite"] = subsite["name"]
                        yield entry

                if found_old:
                    break
                page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        doc_id = raw.get("doc_id", "")

        # Parse date
        date_raw = raw.get("date_raw", "")
        date_iso = ""
        if date_raw:
            try:
                dt = datetime.strptime(date_raw, "%d/%m/%Y")
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                date_iso = date_raw

        # Build title from type + subject
        doc_type = raw.get("doc_type_title", "")
        subject = raw.get("subject", "")
        if doc_type and subject:
            title = f"{doc_type} — {subject}"
        elif doc_type:
            title = doc_type
        elif subject:
            title = subject
        else:
            title = raw.get("pdf_filename", doc_id)

        return {
            "_id": doc_id,
            "_source": "PM/JOSPM",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date_iso,
            "url": raw.get("pdf_url", ""),
            "doc_type": doc_type,
            "subject": subject,
            "subsite": raw.get("subsite", ""),
            "pdf_filename": raw.get("pdf_filename", ""),
            "language": "fr",
        }

    def test_api(self):
        """Quick connectivity and listing test."""
        print("Testing JOSPM site...")

        for subsite in SUBSITES:
            path = subsite["path"]
            name = subsite["name"]
            print(f"\n--- {name} ({path or '/'}) ---")

            html = self._fetch_listing_page(path, 1)
            if not html:
                print("  ERROR: Could not fetch page")
                continue

            total = self._get_total_count(html)
            print(f"  Total documents: ~{total}")

            entries = self._parse_entries(html, path)
            print(f"  Entries on page 1: {len(entries)}")

            if entries:
                e = entries[0]
                print(f"  First entry:")
                print(f"    Date: {e.get('date_raw', 'N/A')}")
                print(f"    Type: {e.get('doc_type_title', 'N/A')}")
                print(f"    Subject: {e.get('subject', 'N/A')[:80]}")
                print(f"    PDF: {e.get('pdf_url', 'N/A')}")
                print(f"    DB ID: {e.get('db_id', 'N/A')}")

                # Test PDF text extraction for first entry
                if e.get("pdf_url"):
                    print(f"  Testing PDF extraction...")
                    text = self._extract_pdf_text(e["pdf_url"], f"test-{e.get('db_id', '0')}")
                    if text:
                        print(f"    Text: {len(text)} chars")
                        print(f"    Preview: {text[:150]}...")
                    else:
                        print(f"    WARNING: No text extracted")

        print("\nTest complete!")


def main():
    scraper = JOSPMScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
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
