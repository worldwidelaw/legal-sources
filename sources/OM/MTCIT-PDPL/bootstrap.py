#!/usr/bin/env python3
"""
OM/MTCIT-PDPL -- Oman MTCIT Laws & Regulations

Fetches laws and executive regulations published by the Oman Ministry of
Transport, Communications and Information Technology (MTCIT). Covers data
protection (PDPL), cybercrime, e-transactions, telecoms, maritime, civil
aviation, land transport, and postal services.

Strategy:
  1. Maintain a catalogue of MTCIT content IDs and metadata
  2. Download each PDF via the content attachment endpoint
  3. Extract text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full pull (~20 PDFs)
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
logger = logging.getLogger("legal-data-hunter.OM.MTCIT-PDPL")

USER_AGENT = (
    "LegalDataHunter/1.0 (open-data research; "
    "https://github.com/worldwidelaw/legal-sources)"
)
BASE_URL = "https://mtcit.gov.om"
DOWNLOAD_URL = f"{BASE_URL}/web/content/mtcit.content/{{cid}}/attachment?download=True"
REQUEST_DELAY = 2.0

# ── Known document catalogue ────────────────────────────────────────
# Each entry: (content_id, title, date_iso, category)
DOC_CATALOGUE: List[Tuple[str, str, Optional[str], str]] = [
    # === ICT / Data Protection Laws ===
    ("1034", "Personal Data Protection Law (Royal Decree No. 6/2022)",
     "2022-02-13", "data_protection"),
    ("1030", "Cybercrime Combat Law (Royal Decree No. 12/2011)",
     "2011-02-01", "cybercrime"),
    ("1031", "Electronic Transactions Law (Royal Decree No. 39/2025)",
     "2025-04-13", "e_transactions"),
    ("1033", "Telecommunications Regulation Law",
     "2002-01-01", "telecom"),
    ("1032", "Postal Services Regulation Law",
     "2012-12-29", "postal"),

    # === Executive Regulations (ICT) ===
    ("1035", "Executive Regulations of the Personal Data Protection Law (MD No. 34/2024)",
     "2024-02-04", "data_protection"),

    # === Maritime Laws ===
    ("191", "The Maritime Law (Royal Decree No. 19/2023)",
     "2023-03-30", "maritime"),
    ("1249", "Royal Decree 19/2023 Issuing the Maritime Law",
     "2023-03-30", "maritime"),
    ("352", "Royal Decree No. 47/2016 — Amendments to Maritime Law Provisions",
     "2016-10-12", "maritime"),
    ("351", "Law on Regulation of Maritime Navigation in Territorial Waters (RD No. 81/98)",
     "1998-01-01", "maritime"),
    # ("350", "The Maritime Law (Royal Decree No. 81/35)", "1981-01-01", "maritime"),  # 404

    # === Transport Laws ===
    ("225", "Land Transport Law",
     "2016-03-01", "transport"),
    ("372", "Land Transport System",
     "2016-03-06", "transport"),
    ("1279", "The Civil Aviation Law and its Executive Regulations",
     "2019-11-06", "aviation"),

    # === Maritime Regulations ===
    ("353", "Ship and Port Security Regulation (MD No. 423/2024)",
     "2024-11-26", "maritime"),
    ("196", "Civil Aviation Regulation (CAR 13) — Aircraft Accident Investigation 2024",
     "2024-10-01", "aviation"),
    ("192", "Marine Accidents Investigation Regulation (MD No. 75/2020)",
     "2020-10-04", "maritime"),
    ("356", "Maritime Units Inspection and Examination Regulation (MD No. 101/2018)",
     "2018-07-26", "maritime"),
    ("302", "Regulatory Bylaw for Marine Piers Outside Ports (MD No. 125/2022)",
     "2022-08-09", "maritime"),
]

# Also try the direct English PDF from prod.mtcit.gov.om
EXTRA_PDFS: List[Tuple[str, str, Optional[str], str]] = [
    ("prod-pdpl-en",
     "Personal Data Protection Law — Official Gazette English Translation (RD 6/2022)",
     "2022-02-13", "data_protection"),
]
EXTRA_PDF_URLS = {
    "prod-pdpl-en": "https://prod.mtcit.gov.om/ITAPortal//Data/English/DocLibrary/2024115132533256/PROMULGATING%20THE%20PERSONAL%20DATA%20PROTECTION%20LAW.pdf",
}


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


class MTCITPDPLScraper(BaseScraper):
    """
    Scraper for OM/MTCIT-PDPL.
    Downloads MTCIT laws and regulations PDFs and extracts full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_docs(self, max_records: int = 999999) -> Generator[dict, None, None]:
        count = 0

        # Fetch from the main catalogue
        for cid, title, date_iso, category in DOC_CATALOGUE:
            if count >= max_records:
                return

            pdf_url = DOWNLOAD_URL.format(cid=cid)
            time.sleep(REQUEST_DELAY)
            logger.info(f"Downloading [{cid}]: {title[:65]}...")
            pdf_bytes = _download_pdf(pdf_url)
            if not pdf_bytes:
                logger.warning(f"  SKIP (download failed): {title[:60]}")
                continue

            text = extract_pdf_markdown(
                source="OM/MTCIT-PDPL",
                source_id=f"mtcit-{cid}",
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text) < 100:
                logger.warning(f"  Insufficient text for {title[:60]}: {len(text)} chars")
                continue

            yield {
                "doc_id": f"mtcit-{cid}",
                "category": category,
                "title": title,
                "text": text,
                "date": date_iso,
                "url": pdf_url,
            }
            count += 1
            logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

        # Fetch extra PDFs (direct URLs)
        for doc_id, title, date_iso, category in EXTRA_PDFS:
            if count >= max_records:
                return

            pdf_url = EXTRA_PDF_URLS.get(doc_id, "")
            if not pdf_url:
                continue

            time.sleep(REQUEST_DELAY)
            logger.info(f"Downloading [extra]: {title[:65]}...")
            pdf_bytes = _download_pdf(pdf_url)
            if not pdf_bytes:
                logger.warning(f"  SKIP (download failed): {title[:60]}")
                continue

            text = extract_pdf_markdown(
                source="OM/MTCIT-PDPL",
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
                "date": date_iso,
                "url": pdf_url,
            }
            count += 1
            logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self._fetch_docs()

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw.get("doc_id", "unknown"),
            "_source": "OM/MTCIT-PDPL",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": raw.get("doc_id", ""),
            "category": raw.get("category", "other"),
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = MTCITPDPLScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        test_url = DOWNLOAD_URL.format(cid="1034")
        req = Request(test_url, headers={"User-Agent": USER_AGENT}, method="HEAD")
        try:
            resp = urlopen(req, timeout=15)
            print(f"OK: PDPL PDF reachable → {resp.status}")
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
