#!/usr/bin/env python3
"""
CY/CompetitionCommission -- Cyprus Commission for the Protection of Competition

Scrapes competition decisions from the CPC website (Lotus Domino backend).
Multiple category pages list decisions with links to individual document pages.
Each document page contains metadata and a PDF attachment with the full decision text.

Categories: mergers, collusions, dominant position, economic dependence.
Both current and archive pages are scraped.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Set, Tuple
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CY.CompetitionCommission")

BASE_URL = "https://www.competition.gov.cy"
NSF_BASE = "/competition/competition.nsf"

# Category listing pages (English + Greek) with their category labels
LISTING_PAGES = [
    # Main decisions pages
    (f"{NSF_BASE}/desicions_en/desicions_en?OpenDocument", "decisions"),
    (f"{NSF_BASE}/desicions_gr/desicions_gr?OpenDocument", "decisions"),
    # Mergers
    (f"{NSF_BASE}/page31_en/page31_en?OpenDocument", "mergers"),
    (f"{NSF_BASE}/page31_gr/page31_gr?OpenDocument", "mergers"),
    # Collusions / Cartels
    (f"{NSF_BASE}/page27_en/page27_en?OpenDocument", "collusions"),
    (f"{NSF_BASE}/page27_gr/page27_gr?OpenDocument", "collusions"),
    # Dominant Position
    (f"{NSF_BASE}/page28_en/page28_en?OpenDocument", "dominant_position"),
    (f"{NSF_BASE}/page28_gr/page28_gr?OpenDocument", "dominant_position"),
    # Economic Dependence
    (f"{NSF_BASE}/page29_en/page29_en?OpenDocument", "economic_dependence"),
    (f"{NSF_BASE}/page29_gr/page29_gr?OpenDocument", "economic_dependence"),
]

# Archive pages
ARCHIVE_PAGES = [
    (f"{NSF_BASE}/decisions2_arch_en/decisions2_arch_en?OpenDocument", "decisions"),
    (f"{NSF_BASE}/decisions2_arch_gr/decisions2_arch_gr?OpenDocument", "decisions"),
    (f"{NSF_BASE}/page31_arch_en/page31_arch_en?OpenDocument", "mergers"),
    (f"{NSF_BASE}/page31_arch_gr/page31_arch_gr?OpenDocument", "mergers"),
    (f"{NSF_BASE}/page27_arch_en/page27_arch_en?OpenDocument", "collusions"),
    (f"{NSF_BASE}/page27_arch_gr/page27_arch_gr?OpenDocument", "collusions"),
    (f"{NSF_BASE}/page28_arch_en/page28_arch_en?OpenDocument", "dominant_position"),
    (f"{NSF_BASE}/page28_arch_gr/page28_arch_gr?OpenDocument", "dominant_position"),
    (f"{NSF_BASE}/page29_arch_en/page29_arch_en?OpenDocument", "economic_dependence"),
    (f"{NSF_BASE}/page29_arch_gr/page29_arch_gr?OpenDocument", "economic_dependence"),
]

# Pattern to match UNID links in listing pages
UNID_PATTERN = re.compile(
    r'/competition/competition\.nsf/All/([A-F0-9]{32})\?OpenDocument',
    re.IGNORECASE,
)


def _parse_date(date_str: str) -> str:
    """Parse CPC date format (DD/MM/YYYY) to ISO format."""
    date_str = date_str.strip()
    if not date_str:
        return ""
    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def _clean_text(text: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


class CompetitionCommissionScraper(BaseScraper):
    """Scraper for CY/CompetitionCommission."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/pdf,*/*",
        })

    def _fetch_url(self, path: str) -> str:
        """Fetch a page by relative or absolute URL."""
        self.rate_limiter.wait()
        url = path if path.startswith("http") else f"{BASE_URL}{path}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _collect_unids_from_page(self, path: str) -> Set[str]:
        """Extract all document UNIDs from a listing page."""
        try:
            html = self._fetch_url(path)
        except Exception as e:
            logger.warning(f"Failed to fetch {path}: {e}")
            return set()
        return set(UNID_PATTERN.findall(html))

    def _collect_all_unids(self, sample: bool = False) -> List[Tuple[str, str]]:
        """Collect all unique (UNID, category) pairs from listing pages."""
        seen: Dict[str, str] = {}  # UNID -> category

        pages = LISTING_PAGES if sample else LISTING_PAGES + ARCHIVE_PAGES

        for path, category in pages:
            logger.info(f"Scanning {category} listing: {path}")
            unids = self._collect_unids_from_page(path)
            for unid in unids:
                upper = unid.upper()
                if upper not in seen:
                    seen[upper] = category
            logger.info(f"  Found {len(unids)} links ({len(seen)} unique total)")

            if sample and len(seen) >= 30:
                break

        return [(unid, cat) for unid, cat in seen.items()]

    def _fetch_document_page(self, unid: str) -> Dict[str, Any]:
        """Fetch a single document page and extract metadata + PDF link."""
        path = f"{NSF_BASE}/All/{unid}?OpenDocument"
        try:
            html = self._fetch_url(path)
        except Exception as e:
            logger.warning(f"Failed to fetch document {unid}: {e}")
            return {}

        doc: Dict[str, Any] = {"unid": unid}

        # Extract title - look for the main heading or decision text
        # Pattern: "Decision CPC: XX/YYYY - Title text"
        title_match = re.search(
            r'(?:Απόφαση|Decision)\s+(?:ΕΠΑ|CPC)[:\s]*(\d+/\d{4})\s*[-–]\s*(.*?)(?:<|$)',
            html, re.DOTALL | re.IGNORECASE,
        )
        if title_match:
            doc["decision_number"] = f"CPC:{title_match.group(1).strip()}"
            doc["title_text"] = _clean_text(title_match.group(2))
        else:
            # Broader title search
            h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
            if h1:
                doc["page_heading"] = _clean_text(h1.group(1))
            # Try to find decision number anywhere
            num_match = re.search(r'(?:CPC|ΕΠΑ)[:\s]*(\d+/\d{4})', html)
            if num_match:
                doc["decision_number"] = f"CPC:{num_match.group(1)}"

        # Extract date - look for date pattern DD/MM/YYYY
        date_match = re.search(r'(\d{2}/\d{2}/\d{4})', html)
        if date_match:
            doc["date"] = _parse_date(date_match.group(1))

        # Extract PDF link - look for $file links
        pdf_match = re.search(
            r'href="([^"]*\$file[^"]*\.pdf)"',
            html, re.IGNORECASE,
        )
        if pdf_match:
            pdf_href = pdf_match.group(1)
            if not pdf_href.startswith("http"):
                # Relative URL - construct full path
                if pdf_href.startswith("/"):
                    doc["pdf_url"] = f"{BASE_URL}{pdf_href}"
                else:
                    doc["pdf_url"] = f"{BASE_URL}{NSF_BASE}/All/{pdf_href}"
            else:
                doc["pdf_url"] = pdf_href

        # Also check for "only available in greek" note
        if re.search(r'(?:only available in greek|μόνο στα ελληνικά)', html, re.IGNORECASE):
            doc["greek_only"] = True

        return doc

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download a PDF and extract text."""
        if not pdf_url:
            return ""

        self.rate_limiter.wait()
        try:
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF for {doc_id}: {e}")
            return ""

        if len(pdf_bytes) < 100:
            return ""

        text = extract_pdf_markdown(
            source="CY/CompetitionCommission",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

        return text

    def normalize(self, raw: dict) -> dict:
        unid = raw.get("unid", "")
        decision_number = raw.get("decision_number", "")
        title_text = raw.get("title_text", raw.get("page_heading", ""))
        category = raw.get("category", "")

        title = f"{decision_number} - {title_text}" if decision_number and title_text else (
            decision_number or title_text or f"Decision {unid[:8]}"
        )

        doc_id = decision_number.replace(":", "").replace("/", "-").replace(" ", "") if decision_number else unid[:16]

        return {
            "_id": f"CY/CompetitionCommission/{doc_id}",
            "_source": "CY/CompetitionCommission",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_prefetched_text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("pdf_url", f"{BASE_URL}{NSF_BASE}/All/{unid}?OpenDocument"),
            "decision_number": decision_number,
            "category": category,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        limit = 15 if sample else None
        count = 0

        entries = self._collect_all_unids(sample=sample)
        logger.info(f"Collected {len(entries)} unique documents to process")

        for unid, category in entries:
            if limit and count >= limit:
                break

            logger.info(f"Processing {unid[:16]}... ({category})")
            doc = self._fetch_document_page(unid)
            if not doc:
                continue

            doc["category"] = category
            pdf_url = doc.get("pdf_url", "")

            if pdf_url:
                text = self._download_pdf_text(pdf_url, doc.get("decision_number", unid[:16]))
                if not text or len(text) < 50:
                    logger.warning(f"  Skipping {unid[:16]} - no/short text from PDF")
                    continue
                doc["_prefetched_text"] = text
            else:
                logger.warning(f"  Skipping {unid[:16]} - no PDF link")
                continue

            yield doc
            count += 1
            dn = doc.get("decision_number", unid[:16])
            logger.info(f"  [{count}] {dn} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CompetitionCommissionScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing CPC access...")
        entries = scraper._collect_all_unids(sample=True)
        print(f"Found {len(entries)} unique documents")
        if entries:
            unid, cat = entries[0]
            print(f"  First: {unid} ({cat})")
            doc = scraper._fetch_document_page(unid)
            print(f"  Metadata: {doc}")
            if doc.get("pdf_url"):
                text = scraper._download_pdf_text(doc["pdf_url"], "test")
                print(f"  PDF text: {len(text)} chars")
                if text:
                    print(f"  Preview: {text[:200]}...")
        print("Test PASSED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
