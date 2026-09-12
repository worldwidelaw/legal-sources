#!/usr/bin/env python3
"""
SZ/ERS-TaxLegislation -- Eswatini Revenue Service — Tax Legislation & Practice Notes

Fetches tax legislation, amendments, VAT regulations, and double taxation
agreements from ers.org.sz. PDFs are hosted on a Node.js server on port 8000.

Strategy:
  1. Hardcoded catalog of PDF URLs extracted from Next.js SPA chunks
  2. Download each PDF and extract full text via common.pdf_extract
  3. Skip scanned/image-only PDFs that yield no extractable text

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SZ.ERS-TaxLegislation")

BASE_URL = "https://www.ers.org.sz:8000/documents"
DELAY = 2.0

# Catalog of documents extracted from the ERS Next.js SPA
# Categories: primary_legislation, amendment, vat_regulation, tax_treaty
DOCUMENT_CATALOG = [
    # Primary Legislations
    {
        "filename": "RevenueAuthorityAct2008.pdf",
        "title": "Revenue Authority Act 2008",
        "category": "primary_legislation",
        "year": 2008,
    },
    {
        "filename": "CustomsandExciseAct1971.pdf",
        "title": "Customs and Excise Act 1971",
        "category": "primary_legislation",
        "year": 1971,
    },
    {
        "filename": "IncomeTaxOrder1975.pdf",
        "title": "Income Tax Order 1975",
        "category": "primary_legislation",
        "year": 1975,
    },
    {
        "filename": "ValueAddedTaxAct2011.pdf",
        "title": "Value Added Tax Act 2011",
        "category": "primary_legislation",
        "year": 2011,
    },
    {
        "filename": "Alcohol-And-Tobacco-Levy-Act.pdf",
        "title": "Alcohol and Tobacco Levy Act",
        "category": "primary_legislation",
        "year": None,
    },
    {
        "filename": "TheFuelTaxAct2022.pdf",
        "title": "The Fuel Tax Act 2022",
        "category": "primary_legislation",
        "year": 2022,
    },
    {
        "filename": "GradedTaxAct1968.pdf",
        "title": "Graded Tax Act 1968",
        "category": "primary_legislation",
        "year": 1968,
    },
    # Amendments
    {
        "filename": "THE_VALUE_ADDED_TAX_ACT_LEGAL_NOTICE_30_OF_2025.pdf",
        "title": "The Value Added Tax Act (Amendment of Schedules) Legal Notice 130 of 2025",
        "category": "amendment",
        "year": 2025,
    },
    {
        "filename": "IncomeTaxAmendmentAct2023.pdf",
        "title": "Income Tax Amendment Act 2023",
        "category": "amendment",
        "year": 2023,
    },
    {
        "filename": "TheValueAddedTax(Amendment)Act 2022_Schedules.pdf",
        "title": "The Value Added Tax (Amendment) Act 2022 Schedules",
        "category": "amendment",
        "year": 2022,
    },
    {
        "filename": "TheValueAddedTaxAmendmentAct2022.pdf",
        "title": "The Value Added Tax Amendment Act 2022",
        "category": "amendment",
        "year": 2022,
    },
    {
        "filename": "The-Income-Tax-(Compliance-Certificates)-regulation-2022.pdf",
        "title": "The Income Tax (Compliance Certificates) Regulations 2022",
        "category": "amendment",
        "year": 2022,
    },
    # VAT Regulations
    {
        "filename": "TheVATRegulations.pdf",
        "title": "The VAT Regulations",
        "category": "vat_regulation",
        "year": None,
    },
    {
        "filename": "VATRateAmendmentRegulation2018.pdf",
        "title": "VAT Rate Amendment Regulation 2018",
        "category": "vat_regulation",
        "year": 2018,
    },
    # Double Taxation Agreements
    {
        "filename": "SouthAfrica.pdf",
        "title": "Double Taxation Agreement — South Africa",
        "category": "tax_treaty",
        "year": None,
    },
    {
        "filename": "Mauritius.pdf",
        "title": "Double Taxation Agreement — Mauritius",
        "category": "tax_treaty",
        "year": None,
    },
    {
        "filename": "UnitedKingdom.pdf",
        "title": "Double Taxation Agreement — United Kingdom",
        "category": "tax_treaty",
        "year": None,
    },
    {
        "filename": "Seychelles.pdf",
        "title": "Double Taxation Agreement — Seychelles",
        "category": "tax_treaty",
        "year": None,
    },
    {
        "filename": "RepublicofChinaOnTaiwan.pdf",
        "title": "Double Taxation Agreement — Republic of China (Taiwan)",
        "category": "tax_treaty",
        "year": None,
    },
    {
        "filename": "Botswana.pdf",
        "title": "Double Taxation Agreement — Botswana",
        "category": "tax_treaty",
        "year": None,
    },
    {
        "filename": "Lesotho.pdf",
        "title": "Double Taxation Agreement — Lesotho",
        "category": "tax_treaty",
        "year": None,
    },
]


def _make_id(filename: str) -> str:
    """Create a stable document ID from filename."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", filename).strip("_").lower()
    slug = re.sub(r"_pdf$", "", slug)
    return f"SZ_ERS_{slug}"


def _download_pdf(filename: str) -> Optional[bytes]:
    """Download a PDF from the ERS document server."""
    import requests
    from urllib.parse import quote

    url = f"{BASE_URL}/{quote(filename, safe='')}"
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            r = requests.get(url, timeout=60, verify=False, headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)"
            })
            if r.status_code == 200 and len(r.content) > 200:
                return r.content
            logger.warning("PDF download attempt %d for %s: HTTP %d, %d bytes",
                           attempt + 1, filename, r.status_code, len(r.content))
        except Exception as e:
            logger.warning("PDF download attempt %d for %s: %s", attempt + 1, filename, e)
        if attempt < 2:
            time.sleep(3)
    return None


class ERSTaxLegislationScraper(BaseScraper):
    """Scraper for SZ/ERS-TaxLegislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        date = None
        if raw.get("year"):
            date = f"{raw['year']}-01-01"
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "SZ/ERS-TaxLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0
        for doc in DOCUMENT_CATALOG:
            if max_records and count >= max_records:
                return

            filename = doc["filename"]
            title = doc["title"]
            doc_id = _make_id(filename)
            url = f"{BASE_URL}/{filename}"

            logger.info("Downloading PDF [%d/%d]: %s",
                        count + 1, len(DOCUMENT_CATALOG), title)

            pdf_bytes = _download_pdf(filename)
            if pdf_bytes is None:
                logger.warning("Failed to download: %s", filename)
                continue
            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a PDF: %s", filename)
                continue

            try:
                text = extract_pdf_markdown(
                    source="SZ/ERS-TaxLegislation",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                )
            except Exception as e:
                logger.warning("PDF extraction failed for %s: %s", filename, e)
                continue

            if not text or len(text) < 50:
                logger.warning("Insufficient text (%d chars), skipping scanned PDF: %s",
                               len(text or ""), title)
                continue

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "year": doc.get("year"),
                "url": url,
                "category": doc["category"],
            }
            count += 1
            yield raw

        logger.info("Completed: %d documents with extractable text out of %d total",
                     count, len(DOCUMENT_CATALOG))

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        logger.info("Testing access to ers.org.sz:8000...")
        pdf_bytes = _download_pdf("RevenueAuthorityAct2008.pdf")
        if pdf_bytes and len(pdf_bytes) > 200:
            logger.info("PDF download OK: %d bytes", len(pdf_bytes))
            return True
        logger.error("Cannot download PDFs from ers.org.sz:8000")
        return False


def main():
    parser = argparse.ArgumentParser(description="SZ/ERS-TaxLegislation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ERSTaxLegislationScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
