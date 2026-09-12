#!/usr/bin/env python3
"""
INTL/ECSRC -- Eastern Caribbean Securities Regulatory Commission

Downloads securities legislation, regulations, rules, policies, and guidance
notes from the ECSRC website. Full text extracted from PDFs via pdfplumber.

Strategy:
  - The ECSRC website hosts regulatory PDFs across multiple URL patterns:
    /gallery/documents/sendFile/{id}  — Acts, Agreement, Guidelines
    /img/WorkflowsDocument/           — Securities Regulations
    /img/source/                      — Territory-specific Rules (8 ECCU territories)
    /files/documents/ecsrc_rules/     — Corporate Governance, Repurchase rules
  - We maintain a curated catalog of all known documents.
  - Each PDF is downloaded and text is extracted via pdfplumber.

Coverage:
  - Securities Act 2001, Securities (Amendment) Act 2004
  - 11 Securities Regulations (conduct of business, prospectus, etc.)
  - Territory-specific Accounting & Financial Reports Rules (8 territories)
  - Territory-specific Registration Statement Rules (8 territories)
  - ECSRC Agreement, Corporate Governance Rules, Repurchase Rules
  - Policy Statements, Guidance Notes
  - Proposed legislation (Securities Bill 2020, Investment Funds Bill 2020)

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ECSRC")

BASE_URL = "https://www.ecsrc.com"

# Curated catalog of ECSRC regulatory documents.
# Format: (relative_url, title, category)
# URL types:
#   sendFile — /gallery/documents/sendFile/{id}
#   workflow — /img/WorkflowsDocument/{filename}
#   source   — /img/source/{filename}
#   rules    — /files/documents/ecsrc_rules/{filename}
DOCUMENT_CATALOG = [
    # === Acts (sendFile) ===
    ("/gallery/documents/sendFile/65",
     "Securities Act 2001", "Acts"),
    ("/gallery/documents/sendFile/66",
     "Securities (Amendment) Act 2004", "Acts"),
    ("/gallery/documents/sendFile/67",
     "Securities Amendment Act (Disciplinary Committee)", "Acts"),

    # === ECSRC Agreement (sendFile) ===
    ("/gallery/documents/sendFile/150",
     "ECSRC Agreement", "Agreement"),

    # === Securities Regulations (WorkflowsDocument) ===
    ("/img/WorkflowsDocument/1393266826_ECCBLIB-566054-v1-Securities_(Foreign_Securities_and_Intermediaries)_Regulations_2004.pdf",
     "Securities (Foreign Securities and Intermediaries) Regulations 2004", "Regulations"),
    ("/img/WorkflowsDocument/1393267294_Securities (Accounting and Financial Statements) Regulations .pdf",
     "Securities (Accounting and Financial Statements) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393267315_Securities (Advertisements) Regulations.pdf",
     "Securities (Advertisements) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393267344_Securities (Collective Investment Schemes) Regulations.pdf",
     "Securities (Collective Investment Schemes) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393267365_Securities (Conduct of Business) Regulations.pdf",
     "Securities (Conduct of Business) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393267387_Securities (Discipline) Regulations.pdf",
     "Securities (Discipline) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393267415_Securities (Licences and Fees) Regulations.pdf",
     "Securities (Licences and Fees) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393267434_Securities (Prospectus) Regulations.pdf",
     "Securities (Prospectus) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393267488_Securities Regulations - Continuing Disclosure Obligations of Issuers.pdf",
     "Securities (Continuing Disclosure Obligations of Issuers) Regulations", "Regulations"),
    ("/img/WorkflowsDocument/1393266757_ECCBLIB-538255-v1-Securities_Uncertificated_Regulations.pdf",
     "Securities (Uncertificated) Regulations 2004", "Regulations"),
    ("/img/WorkflowsDocument/1393267207_ECCBLIB-622127-v1-Current_Version_Securities_Amendment_Bill_Listing_of_Foreign_Securities-20.pdf",
     "Securities (Amendment) Bill — Listing of Foreign Securities", "Regulations"),

    # === ECSRC Rules — Corporate Governance & Repurchase (files/documents) ===
    ("/files/documents/ecsrc_rules/ECSRC Finalised_Rules_for_Repurchase_Agreements.pdf",
     "ECSRC Finalized Rules for Repurchase Agreements", "Rules"),
    ("/files/documents/ecsrc_rules/ECSRC_CORP_FIN_Corporate_Governance_Rules.pdf",
     "ECSRC Corporate Governance Rules", "Rules"),

    # === Territory-specific Accounting & Financial Reports Rules (img/source) ===
    ("/img/source/Anguilla Securities (Accounting and Financial Reports) Rules 2015.PDF",
     "Anguilla Securities (Accounting and Financial Reports) Rules 2015", "Rules"),
    ("/img/source/Antigua Securities (Accounting and Financial Reports) Rules 2015.pdf",
     "Antigua Securities (Accounting and Financial Reports) Rules 2015", "Rules"),
    ("/img/source/Dominica Securities (Accounting and Financial Reports) Rule 2015.PDF",
     "Dominica Securities (Accounting and Financial Reports) Rules 2015", "Rules"),
    ("/img/source/Grenada Securities (Accounting and Financial Reports) Rules 2015.PDF",
     "Grenada Securities (Accounting and Financial Reports) Rules 2015", "Rules"),
    ("/img/source/Montserrat Securities (Accounting and Financial Reports) Rules 2015.pdf",
     "Montserrat Securities (Accounting and Financial Reports) Rules 2015", "Rules"),
    ("/img/source/St Kitts-Nevis Securities (Accounting and Financial Reports) Rules 2015.PDF",
     "St Kitts-Nevis Securities (Accounting and Financial Reports) Rules 2015", "Rules"),
    ("/img/source/St Lucia Securities (Accounting and Financial Reports) Rules of 2015.PDF",
     "St Lucia Securities (Accounting and Financial Reports) Rules 2015", "Rules"),
    ("/img/source/St Vincent Securities (Accounting and Financial Reports) Rules 2015.pdf",
     "St Vincent Securities (Accounting and Financial Reports) Rules 2015", "Rules"),

    # === Territory-specific Registration Statement Rules (img/source via beta) ===
    ("/img/source/Anguilla Securities (Registration Statement) Rules 2015.PDF",
     "Anguilla Securities (Registration Statement) Rules 2015", "Rules"),
    ("/img/source/Antigua Securities (Registration Statement) Rules 2015.pdf",
     "Antigua Securities (Registration Statement) Rules 2015", "Rules"),
    ("/img/source/Dominica Securities (Registration Statement) Rules 2015.PDF",
     "Dominica Securities (Registration Statement) Rules 2015", "Rules"),
    ("/img/source/Grenada Securities (Registration Statement) Rules 2015.PDF",
     "Grenada Securities (Registration Statement) Rules 2015", "Rules"),
    ("/img/source/Montserrat Securities (Registration Statement) Rules 2015.pdf",
     "Montserrat Securities (Registration Statement) Rules 2015", "Rules"),
    ("/img/source/St Kitts-Nevis Securities (Registration Statement) Rules 2015.PDF",
     "St Kitts-Nevis Securities (Registration Statement) Rules 2015", "Rules"),
    ("/img/source/St Lucia Securities (Registration Statement) Rules 2015.PDF",
     "St Lucia Securities (Registration Statement) Rules 2015", "Rules"),
    ("/img/source/St Vincent Securities (Registration Statement) Rules 2015.pdf",
     "St Vincent Securities (Registration Statement) Rules 2015", "Rules"),

    # === Registration Statement Forms (sendFile) ===
    ("/gallery/documents/sendFile/132",
     "Securities Registration Statement Form RS-1", "Forms"),
    ("/gallery/documents/sendFile/133",
     "Securities Registration Statement Form RS-2", "Forms"),

    # === Guidelines / Guidance Notes (sendFile) ===
    ("/gallery/documents/sendFile/151",
     "ECSRC Electronic Filing Guidance Note", "Guidelines"),
    ("/gallery/documents/sendFile/152",
     "Compliance and Internal Control Guidelines for Licensed Entities", "Guidelines"),

    # === Tokenisation ===
    ("/files/documents/ECSRC_pilot_testing_and_Implementation_plan_for_Tokenised_Securities_.pdf",
     "ECSRC Pilot Testing Implementation Plan for Securities Tokenisation", "Tokenisation"),
]


def _make_doc_id(url: str) -> str:
    """Create a stable, short document ID from URL."""
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _encode_url(relative: str) -> str:
    """Encode a relative URL path, preserving structure but encoding spaces/parens."""
    parts = relative.split("/")
    # Encode just the filename part (last segment)
    encoded_parts = parts[:-1] + [quote(parts[-1], safe="._-")]
    return "/".join(encoded_parts)


class ECSRCScraper(BaseScraper):
    """Scraper for INTL/ECSRC — ECSRC regulatory documents."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/pdf,*/*",
            })
        return self.session

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"INTL/ECSRC/{raw['doc_id']}",
            "_source": "INTL/ECSRC",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["url"],
            "category": raw.get("category", ""),
        }

    def _download_and_extract(self, relative_url: str, doc_id: str) -> Optional[str]:
        """Download PDF from ECSRC and extract text."""
        encoded = _encode_url(relative_url)
        url = BASE_URL + encoded
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to download %s: %s", relative_url, e)
            return None

        if len(resp.content) < 200:
            logger.warning("Skipping %s - too small (%d bytes)", relative_url, len(resp.content))
            return None

        # Check it's actually a PDF
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type and not resp.content[:5] == b"%PDF-":
            logger.warning("Skipping %s - not a PDF (Content-Type: %s)", relative_url, content_type)
            return None

        text = extract_pdf_markdown(
            source="INTL/ECSRC",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        ) or ""

        return text if len(text) >= 50 else None

    def fetch_all(self, sample=False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for relative_url, title, category in DOCUMENT_CATALOG:
            if limit and count >= limit:
                break

            doc_id = _make_doc_id(relative_url)
            logger.info("[%d/%d] %s", count + 1, len(DOCUMENT_CATALOG), title[:60])

            text = self._download_and_extract(relative_url, doc_id)
            if not text:
                continue

            full_url = BASE_URL + relative_url

            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": "",
                "url": full_url,
                "category": category,
            }
            count += 1
            logger.info("  OK: %s (%d chars)", title[:40], len(text))

        logger.info("Total records yielded: %d / %d catalog entries", count, len(DOCUMENT_CATALOG))

    def fetch_updates(self, since=None):
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test — download one PDF."""
        try:
            sess = self._get_session()
            url = BASE_URL + "/gallery/documents/sendFile/65"
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            ok = len(resp.content) > 200
            logger.info("Test: Securities Act 2001 (%d bytes) — %s",
                        len(resp.content), "OK" if ok else "FAIL")
            return ok
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ECSRCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
