#!/usr/bin/env python3
"""
LB/ConstitutionalCouncil -- Lebanon Constitutional Council Decisions

Fetches decisions from cc.gov.lb (المجلس الدستوري اللبناني).

Strategy:
  - Use AJAX endpoint (X-Requested-With: XMLHttpRequest) on decision list pages
  - Parse returned JSON containing paginated HTML of decisions
  - Extract metadata (title, date, petition number, law challenged, PDF link)
  - Download PDFs and extract text with PyMuPDF (fitz)

Categories:
  - Constitutionality review (قرارات دستورية القوانين): 21 pages
  - Parliamentary election disputes (قرارات الطعون النيابية): 23 pages

Data:
  - ~170 decisions since 1994
  - Full text in Arabic (some English translations exist)
  - License: Public Domain (Government Works)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import io
import json
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

import fitz  # PyMuPDF
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LB.ConstitutionalCouncil")

BASE_URL = "https://cc.gov.lb"

# Decision category endpoints (Arabic versions have all content)
CATEGORIES = [
    {
        "path": "/ar/%D8%A7%D9%84%D9%82%D8%B1%D8%A7%D8%B1%D8%A7%D8%AA/%D9%82%D8%B1%D8%A7%D8%B1%D8%A7%D8%AA-%D8%AF%D8%B3%D8%AA%D9%88%D8%B1%D9%8A%D8%A9-%D8%A7%D9%84%D9%82%D9%88%D8%A7%D9%86%D9%8A%D9%86/",
        "type": "constitutionality_review",
        "label": "Constitutionality Review",
    },
    {
        "path": "/ar/%D8%A7%D9%84%D9%82%D8%B1%D8%A7%D8%B1%D8%A7%D8%AA/%D9%82%D8%B1%D8%A7%D8%B1%D8%A7%D8%AA-%D8%A7%D9%84%D8%B7%D8%B9%D9%88%D9%86-%D8%A7%D9%84%D9%86%D9%8A%D8%A7%D8%A8%D9%8A%D8%A9/",
        "type": "parliamentary_election",
        "label": "Parliamentary Election Disputes",
    },
]

MAX_PAGES = 50  # Safety cap per category


class ConstitutionalCouncilScraper(BaseScraper):
    """
    Scraper for LB/ConstitutionalCouncil -- Lebanon Constitutional Council.
    Country: LB
    URL: https://cc.gov.lb

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json, text/html",
        })

    def _fetch_page(self, path: str, page: int = 1) -> Dict[str, Any]:
        """Fetch a paginated decision list page via AJAX."""
        self.rate_limiter.wait()
        url = f"{BASE_URL}{path}"
        params = {"page": page} if page > 1 else {}
        resp = self.session.get(
            url,
            params=params,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_decisions(self, html: str, decision_type: str) -> List[Dict[str, Any]]:
        """Parse decision entries from HTML content."""
        decisions = []

        # Split into decision blocks
        blocks = re.split(
            r'<div\s+class="flex-1\s+border\s+border-neutral-300',
            html,
        )

        for block in blocks[1:]:  # skip everything before first block
            # Title
            title_match = re.search(
                r'class="col-span-12 text-primary-500 text-xl[^"]*"[^>]*>([^<]+)',
                block,
            )
            if not title_match:
                continue
            title = title_match.group(1).strip()

            # Date (in no-digit-conversion div)
            date_match = re.search(
                r'no-digit-conversion"[^>]*>([^<]+)', block
            )
            date_str = date_match.group(1).strip() if date_match else None

            # Parse date from DD/MM/YYYY to ISO
            iso_date = None
            if date_str:
                dm = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_str)
                if dm:
                    iso_date = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"

            # Extract all field values (label-value pairs)
            fields = {}
            field_pairs = re.findall(
                r'<div class="text-primary-500 text-sm col-span-4[^"]*"[^>]*>\s*'
                r'(?:<[^>]*>)?\s*([^<]+?)\s*(?:</[^>]*>)?\s*</div>\s*'
                r'<div class="text-neutral-500 text-sm col-span-8[^"]*"[^>]*>\s*'
                r'(?:<div[^>]*>)?\s*(.*?)\s*(?:</div>)?\s*</div>',
                block,
                re.DOTALL,
            )
            for label, value in field_pairs:
                label = label.strip()
                value = re.sub(r'<[^>]+>', '', value).strip()
                if label in ("رقم المراجعة", "Petition No."):
                    fields["petition_number"] = value
                elif label in ("القانون المطلوب ابطاله", "Law Number"):
                    fields["law_challenged"] = value
                elif label in ("الجهة المستدعية", "Petitioner(s)"):
                    fields["petitioners"] = value

            # PDF link
            pdf_match = re.search(r'href="(/documents/[^"]+\.pdf)"', block)
            pdf_url = f"{BASE_URL}{pdf_match.group(1)}" if pdf_match else None

            # Generate ID from title
            # Title format: "قرار رقم 2026/7" or "Decision No. 7/2014"
            id_match = re.search(r'(\d{4}[/\\]\d+|\d+[/\\]\d{4})', title)
            if id_match:
                decision_id = id_match.group(1).replace("\\", "/")
            else:
                decision_id = hashlib.md5(title.encode()).hexdigest()[:10]

            decisions.append({
                "decision_id": decision_id,
                "title": title,
                "date": iso_date,
                "decision_type": decision_type,
                "petition_number": fields.get("petition_number", ""),
                "law_challenged": fields.get("law_challenged", ""),
                "petitioners": fields.get("petitioners", ""),
                "pdf_url": pdf_url,
            })

        return decisions

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using PyMuPDF."""
        try:
            self.rate_limiter.wait()
            logger.info(f"Downloading PDF: {unquote(pdf_url.split('/')[-1])[:60]}...")

            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()

            content = resp.content
            size_mb = len(content) / (1024 * 1024)
            if size_mb > 50:
                logger.warning(f"PDF too large ({size_mb:.1f} MB), skipping")
                return None

            doc = fitz.open(stream=content, filetype="pdf")
            text_parts = []
            for page in doc:
                page_text = page.get_text()
                if page_text:
                    text_parts.append(page_text.strip())
            doc.close()

            full_text = "\n\n".join(text_parts)

            if len(full_text) < 50:
                logger.warning(f"PDF yielded very little text ({len(full_text)} chars)")
                return None

            return full_text

        except Exception as e:
            logger.error(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw decision record."""
        return {
            "_id": f"LB/ConstitutionalCouncil/{raw['decision_id']}",
            "_source": "LB/ConstitutionalCouncil",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("pdf_url", f"{BASE_URL}/ar/"),
            "decision_type": raw.get("decision_type", ""),
            "petition_number": raw.get("petition_number", ""),
            "law_challenged": raw.get("law_challenged", ""),
            "petitioners": raw.get("petitioners", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions across all categories."""
        total_yielded = 0
        seen_pdfs = set()

        for category in CATEGORIES:
            logger.info(f"Fetching category: {category['label']}")

            # Get first page to determine total pages
            try:
                data = self._fetch_page(category["path"], page=1)
            except Exception as e:
                logger.error(f"Failed to fetch {category['label']}: {e}")
                continue

            total_pages = data.get("total_pages", 1)
            logger.info(f"  Total pages: {total_pages}")

            for page_num in range(1, min(total_pages + 1, MAX_PAGES + 1)):
                if page_num > 1:
                    try:
                        data = self._fetch_page(category["path"], page=page_num)
                    except Exception as e:
                        logger.error(f"Failed page {page_num}: {e}")
                        continue

                decisions = self._parse_decisions(
                    data.get("html", ""), category["type"]
                )
                logger.info(f"  Page {page_num}: {len(decisions)} decisions")

                for decision in decisions:
                    pdf_url = decision.get("pdf_url")
                    if not pdf_url:
                        logger.warning(
                            f"No PDF for: {decision['title']}, skipping"
                        )
                        continue

                    # Skip duplicate PDFs (same PDF may appear on multiple decisions)
                    if pdf_url in seen_pdfs:
                        logger.info(f"  Skipping duplicate PDF: {decision['title']}")
                        continue
                    seen_pdfs.add(pdf_url)

                    # Extract text from PDF
                    text = self._extract_text_from_pdf(pdf_url)
                    if not text:
                        logger.warning(
                            f"No text extracted for: {decision['title']}"
                        )
                        continue

                    decision["text"] = text
                    yield decision
                    total_yielded += 1

        logger.info(f"Total decisions yielded: {total_yielded}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions newer than given date."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for raw in self.fetch_all():
            if raw.get("date") and raw["date"] >= since:
                yield raw

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            data = self._fetch_page(CATEGORIES[0]["path"], page=1)
            pages = data.get("total_pages", 0)
            logger.info(f"Test OK: constitutionality page has {pages} pages")
            return pages > 0
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ConstitutionalCouncilScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        scraper.run_bootstrap(sample=sample)
    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "2024-01-01"
        scraper.run_update(since=since)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
