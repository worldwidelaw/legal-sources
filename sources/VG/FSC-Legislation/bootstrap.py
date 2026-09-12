#!/usr/bin/env python3
"""
VG/FSC-Legislation -- BVI Financial Services Legislation (full text)

Fetches the consolidated, revised-edition full text of the principal British
Virgin Islands financial-services and corporate statutes published by the BVI
Financial Services Commission (FSC) as PDFs at
https://www.bvifsc.vg/library/legislation.

Why a curated seed list (not an HTML crawl):
  The FSC legislation *index* and detail HTML pages are behind Cloudflare and
  return a challenge shell to a headless client, so the document listing cannot
  be enumerated programmatically. The PDF files themselves
  (/sites/default/files/{slug}.pdf) are served directly and download cleanly
  (HTTP 200, application/pdf) with the project User-Agent. We therefore maintain
  a curated list of the core BVI offshore/financial statutes -- exactly the
  investigation-relevant corpus (Business Companies Act, Insolvency Act,
  Securities and Investment Business Act, AML, BOSS Act, etc.). New revised
  editions keep the same slug, so the list is stable across re-runs.

  Each entry was verified to return a full-text PDF on 2026-06-21. If the FSC
  drops Cloudflare on the index, this can be upgraded to a full crawl.

Data:
  - ~18 principal financial-services / corporate Acts and Regulations
  - Full consolidated text extracted from the official revised-edition PDFs
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py bootstrap-fast     # Concurrent full pull (VPS pipeline)
  python bootstrap.py test               # Connectivity test
"""

import sys
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VG.FSC-Legislation")

BASE_URL = "https://www.bvifsc.vg"
PDF_DIR = f"{BASE_URL}/sites/default/files"
SOURCE_ID = "VG/FSC-Legislation"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/pdf,*/*",
}

# Curated list of (pdf_slug, human title). Each slug maps to
# {PDF_DIR}/{slug}.pdf and was verified to return a full-text PDF (HTTP 200,
# application/pdf) on 2026-06-21. Ordered roughly by investigative relevance.
LEGISLATION = [
    ("bvi_business_companies_act", "BVI Business Companies Act (Revised)"),
    ("insolvency_act", "Insolvency Act (Revised)"),
    ("securities_and_investment_business_act", "Securities and Investment Business Act (Revised)"),
    ("financial_services_commission_act", "Financial Services Commission Act (Revised)"),
    ("regulatory_code", "Regulatory Code"),
    ("beneficial_ownership_secure_search_system_act", "Beneficial Ownership Secure Search System Act"),
    ("anti-money_laundering_regulations", "Anti-Money Laundering Regulations"),
    ("anti-money_laundering_and_terrorist_financing_code_of_practice", "Anti-Money Laundering and Terrorist Financing Code of Practice"),
    ("proceeds_of_criminal_conduct_act", "Proceeds of Criminal Conduct Act (Revised)"),
    ("limited_partnership_act", "Limited Partnership Act (Revised)"),
    ("partnership_act", "Partnership Act (Revised)"),
    ("trustee_act", "Trustee Act (Revised)"),
    ("virgin_islands_special_trusts_act", "Virgin Islands Special Trusts Act (Revised)"),
    ("banks_and_trust_companies_act", "Banks and Trust Companies Act (Revised)"),
    ("company_management_act", "Company Management Act (Revised)"),
    ("financing_and_money_services_act", "Financing and Money Services Act (Revised)"),
    ("mutual_funds_regulations", "Mutual Funds Regulations"),
    ("insurance_act", "Insurance Act (Revised)"),
]


class SourceBlockedError(RuntimeError):
    """Raised when Cloudflare refuses the PDF path from this vantage."""


class VGFSCLegislationScraper(BaseScraper):
    """Scraper for VG/FSC-Legislation -- BVI financial-services statutes."""

    # The corpus is a fixed 18-item list, so a run that loses most of it is a
    # blocked vantage, not a handful of dead slugs. Fail loud after this many
    # consecutive fetch failures rather than writing a silent partial corpus
    # (issue #1576: the fleet wrote 10 of 18 and the shortfall looked like
    # ordinary per-document skips).
    MAX_CONSECUTIVE_FAILURES = 3

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self._consecutive_failures = 0

    def _pdf_url(self, slug: str) -> str:
        return f"{PDF_DIR}/{slug}.pdf"

    def _note_failure(self, slug: str, reason: str) -> None:
        self._consecutive_failures += 1
        logger.warning(f"  {reason} for {slug} "
                       f"({self._consecutive_failures}/{self.MAX_CONSECUTIVE_FAILURES})")
        if self._consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
            raise SourceBlockedError(
                f"{self._consecutive_failures} consecutive failures fetching "
                f"{PDF_DIR}/*.pdf (last: {slug} -- {reason}). bvifsc.vg is "
                f"Cloudflare-fronted and 403s its HTML paths for every client; when "
                f"the PDF path is refused too, the vantage is blocked. Refusing to "
                f"write a partial corpus -- re-run from a residential/US vantage."
            )

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        slug = raw["slug"]
        title = raw["title"]
        url = self._pdf_url(slug)

        # Download with the project UA (Cloudflare serves the PDF path but
        # rejects the default requests UA), then extract via the shared backend.
        try:
            resp = requests.get(url, headers=HEADERS, timeout=(15, 90))
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            self._note_failure(slug, f"Fetch failed ({e})")
            return None

        text = extract_pdf_markdown(
            SOURCE_ID, slug, pdf_bytes=pdf_bytes, table="legislation", force=True
        )
        if not text or len(text.strip()) < 500:
            # A Cloudflare challenge body downloads as a 200 with an HTML payload,
            # so an "insufficient text" run is the same signal as a refused fetch.
            self._note_failure(slug, f"Insufficient text ({len(text or '')} chars)")
            return None

        self._consecutive_failures = 0
        now = datetime.now(timezone.utc).isoformat()
        return {
            "_id": f"{SOURCE_ID}/{slug}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": text.strip(),
            "date": None,
            "url": url,
            "doc_id": slug,
            "jurisdiction": "VG",
            "publisher": "BVI Financial Services Commission",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        for slug, title in LEGISLATION:
            logger.info(f"Queuing {slug}")
            yield {"slug": slug, "title": title}

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # Revised editions reuse the same slug; a full re-pull refreshes text.
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        url = self._pdf_url(LEGISLATION[0][0])
        try:
            r = requests.get(url, headers=HEADERS, timeout=30, stream=True)
            ok = r.status_code == 200 and "pdf" in r.headers.get("Content-Type", "").lower()
            print(f"GET {url} -> {r.status_code} ({r.headers.get('Content-Type')})")
            print("OK" if ok else "FAILED")
            return ok
        except Exception as e:
            print(f"FAILED: {e}")
            return False


if __name__ == "__main__":
    scraper = VGFSCLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "bootstrap-fast":
        scraper.bootstrap_fast()
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
