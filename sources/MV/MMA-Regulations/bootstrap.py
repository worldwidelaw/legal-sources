#!/usr/bin/env python3
"""
MV/MMA-Regulations -- Maldives Monetary Authority Regulations & Guidelines

Fetches prudential regulations, banking/insurance guidelines, AML/CFT
compliance documents, and monetary policy rules published by the MMA.

Strategy:
  1. Maintain a catalogue of known PDF URLs on mma.gov.mv
  2. Download each PDF and extract text via common/pdf_extract
  3. Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full pull (~30 PDFs)
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records
  python bootstrap.py update             # (same as bootstrap -- static docs)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MV.MMA-Regulations")

USER_AGENT = (
    "LegalDataHunter/1.0 (open-data research; "
    "https://github.com/worldwidelaw/legal-sources)"
)
BASE_URL = "https://www.mma.gov.mv"
REQUEST_DELAY = 1.5

# ── Known PDF catalogue ─────────────────────────────────────────────
# Each entry: (relative_path, title, category)
# Paths are relative to BASE_URL.
PDF_CATALOGUE: List[Tuple[str, str, str]] = [
    # === Acts / Primary Legislation ===
    ("/files/mmaact-english.pdf",
     "Maldives Monetary Authority Act 1981", "act"),
    ("/documents/Laws/Maldives%20Banking%20Act%20(english).pdf",
     "Maldives Banking Act (Law No. 24/2010)", "act"),
    ("/documents/Laws/First%20Amendment%20to%20Maldives%20Banking%20Act%20(english).pdf",
     "First Amendment to the Maldives Banking Act", "act"),
    ("/documents/Laws/Prevention%20of%20Money%20Laundering%20and%20Terrorism%20Financing%20Act%20(english).pdf",
     "Prevention of Money Laundering and Terrorism Financing Act (Law No. 10/2014)", "act"),
    ("/documents/Laws/National%20Payment%20System%20Act%208-2021%20(english).pdf",
     "National Payment System Act (Law No. 8/2021)", "act"),

    # === Banking Regulations ===
    ("/documents/Laws/Regulation%20on%20Capital%20Adequacy%20(english).pdf",
     "Regulation on Capital Adequacy", "banking"),
    ("/documents/Laws/Regulation%20on%20Corporate%20Governance%20for%20Banks,%20Insurance%20Companies%20and%20Finance%20Companies%20(english).pdf",
     "Regulation on Corporate Governance for Banks, Insurance Companies and Finance Companies", "banking"),
    ("/documents/Laws/Regulation%20on%20Transactions%20with%20Related%20Persons%20(english).pdf",
     "Regulation on Transactions with Related Persons (No. 2015/R-1731)", "banking"),
    ("/documents/Laws/Regulation%20on%20Fit%20and%20Proper%20Requirements%20(english).pdf",
     "Regulation on Fit and Proper Requirements", "banking"),
    ("/documents/Laws/Regulation%20on%20Limits%20on%20Loans%20to%20Related%20Persons%20(english).pdf",
     "Regulation on Limits on Loans to Related Persons", "banking"),
    ("/documents/Laws/Risk%20Management%20Guidelines%20(english).pdf",
     "Risk Management Guidelines for Banks", "banking"),
    ("/documents/Laws/Regulation%20on%20Financing%20Business%20(english).pdf",
     "Regulation on Financing Business", "banking"),
    ("/documents/Laws/Regulation%20on%20Asset%20Classification%20Provisioning%20and%20Suspension%20of%20Interest%20(english).pdf",
     "Regulation on Asset Classification, Provisioning and Suspension of Interest", "banking"),
    ("/documents/Laws/Regulation%20on%20Single%20Borrower%20and%20Large%20Exposure%20Limits%20(english).pdf",
     "Regulation on Single Borrower and Large Exposure Limits", "banking"),
    ("/documents/Laws/Regulation%20on%20Fees%20and%20Charges%20Applicable%20to%20Financial%20Institutions%20(english).pdf",
     "Regulation on Fees and Charges Applicable to Financial Institutions", "banking"),
    ("/documents/Laws/Islamic%20Banking%20Regulation%20(english).pdf",
     "Islamic Banking Regulation 2011", "banking"),

    # === Monetary / Foreign Currency ===
    ("/documents/Laws/Monetary%20Regulation%20(english).pdf",
     "Monetary Regulation (1987)", "monetary"),
    ("/documents/Laws/General%20Regulation%20on%20Foreign%20Currency%20(English).pdf",
     "General Regulation on Foreign Currency", "monetary"),
    ("/documents/Laws/Regulation%20on%20Foreign%20Currency%20Exposure%20Limits%20(english).pdf",
     "Regulation on Foreign Currency Exposure Limits", "monetary"),
    ("/documents/Laws/Regulation%20on%20Cross%20Border%20Currency%20Declaration%20Amount%20(english).pdf",
     "Regulation on Cross-Border Currency Declaration Amount", "monetary"),

    # === Payment Systems ===
    ("/documents/Laws/Regulation%20on%20Payment%20Services%20(english).pdf",
     "Regulation on Payment Services", "payment"),
    ("/documents/Laws/Mobile%20Payment%20Services%20Regulation%20(english).pdf",
     "Regulation on Mobile Payment Services", "payment"),

    # === Insurance Regulations ===
    ("/files/regulations/insuranceIndustryRegulationsEnglish.pdf",
     "Insurance Industry Regulations", "insurance"),
    ("/files/regulations/guidelines_administration_of_insurance_agents.pdf",
     "Guidelines for the Administration of Insurance Agents", "insurance"),
    ("/files/regulations/guidelines_for_insurance_brokers.pdf",
     "Guidelines for Insurance Brokers 2011", "insurance"),
    ("/files/regulations/guidelines-on-fit-and-proper-criteria-for-insurance-undertakings.pdf",
     "Guidelines on Fit and Proper Criteria for Insurance Undertakings", "insurance"),
    ("/files/regulations/GPR.pdf",
     "Guideline on Prudential Requirements for Insurance Undertakings 2010", "insurance"),
    ("/files/regulations/nonbankregulation-eng.pdf",
     "Regulation for Non-Bank Financial Businesses", "insurance"),

    # === AML/CFT ===
    ("/documents/Laws/Regulation%20for%20Banks%20on%20Prevention%20of%20Money%20Laundering%20and%20Financing%20of%20Terrorism%20(english).pdf",
     "Regulation for Banks on Prevention of Money Laundering and Financing of Terrorism", "aml_cft"),
    ("/documents/Laws/Regulation%20on%20Prevention%20of%20Money%20Laundering%20and%20Financing%20of%20Terrorism%20for%20Money%20Transfer%20Business%20and%20Money%20Changing%20Business%20(english).pdf",
     "Regulation on Prevention of Money Laundering for Money Transfer and Money Changing Business", "aml_cft"),
    ("/files/fiu/AMLCFT-Guidelines-for-Banks.pdf",
     "AML/CFT Compliance — General Guidelines for Banks", "aml_cft"),
    ("/files/fiu/AMLCFT-Guidelines-for-Money-Remittance-Institutions.pdf",
     "AML/CFT Compliance Guidelines for Money Remittance Institutions", "aml_cft"),
    ("/files/fiu/AMLCFT-Guidelines-for-Securities-Institutions.pdf",
     "AML/CFT Compliance Guidelines for Securities Institutions", "aml_cft"),
    ("/files/fiu/STRGuidelines.pdf",
     "AML/CFT Compliance Guidelines — Suspicious Transaction Reporting", "aml_cft"),
]


def _download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download a PDF and verify it starts with %PDF."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        if data and b"%PDF" in data[:20]:
            return data
        logger.warning(f"Not a valid PDF: {url}")
    except (HTTPError, URLError) as e:
        logger.warning(f"PDF download failed for {url}: {e}")
    return None


def _make_doc_id(path: str) -> str:
    """Derive a stable document ID from the PDF path."""
    fname = path.rsplit("/", 1)[-1]
    # Remove .pdf extension
    doc_id = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)
    # URL-decode common patterns
    doc_id = doc_id.replace("%20", "_").replace("%2C", "").replace("%28", "(").replace("%29", ")")
    # Sanitize
    doc_id = re.sub(r"[^a-zA-Z0-9_()-]", "_", doc_id)
    doc_id = re.sub(r"_+", "_", doc_id).strip("_")
    return doc_id[:120]


class MMARegulationsScraper(BaseScraper):
    """
    Scraper for MV/MMA-Regulations.
    Downloads known MMA regulatory PDFs and extracts full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_docs(self, max_records: int = 999999) -> Generator[dict, None, None]:
        count = 0
        for rel_path, title, category in PDF_CATALOGUE:
            if count >= max_records:
                return

            pdf_url = f"{BASE_URL}{rel_path}"
            doc_id = _make_doc_id(rel_path)

            time.sleep(REQUEST_DELAY)
            logger.info(f"Downloading: {title[:70]}...")
            pdf_bytes = _download_pdf(pdf_url)
            if not pdf_bytes:
                logger.warning(f"  SKIP (download failed): {title[:60]}")
                continue

            text = extract_pdf_markdown(
                source="MV/MMA-Regulations",
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text) < 100:
                logger.warning(f"  Insufficient text for {title[:60]}: {len(text)} chars")
                continue

            yield {
                "doc_id": doc_id,
                "category": category,
                "title": title,
                "text": text,
                "url": pdf_url,
            }
            count += 1
            logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def normalize(self, raw: dict) -> dict:
        doc_id = raw.get("doc_id", "unknown")
        category = raw.get("category", "other")

        return {
            "_id": doc_id,
            "_source": "MV/MMA-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": None,
            "url": raw.get("url", ""),
            "doc_id": doc_id,
            "category": category,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = MMARegulationsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        # Quick connectivity check: try downloading the first PDF header
        from urllib.request import Request, urlopen
        test_url = f"{BASE_URL}{PDF_CATALOGUE[0][0]}"
        req = Request(test_url, headers={"User-Agent": USER_AGENT}, method="HEAD")
        try:
            resp = urlopen(req, timeout=15)
            print(f"OK: {test_url} → {resp.status}")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        limit = 12 if sample else 999999

        if sample:
            logger.info("=== SAMPLE MODE: fetching ~12 records ===")

        for raw in scraper._fetch_docs(max_records=limit):
            record = scraper.normalize(raw)
            out_file = sample_dir / f"record_{count:04d}.json"
            out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
            count += 1
            logger.info(f"Saved [{count}]: {record['title'][:70]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
