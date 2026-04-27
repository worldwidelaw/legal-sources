#!/usr/bin/env python3
"""
INTL/ITU-Legal -- International Telecommunication Union Legal Framework

Fetches ITU treaty instruments with full text extracted from PDFs:
  - Constitution (amended through PP-22)
  - Convention (amended through PP-22)
  - 127 Plenipotentiary Conference Resolutions
  - 5 Plenipotentiary Conference Decisions
  - Radio Regulations (3 volumes, 2024 edition)
  - International Telecommunication Regulations (1988 Melbourne)
  - Council Decisions collection

Strategy:
  - All documents are freely available as individual PDFs at known URLs
  - No scraping needed — direct PDF downloads with pdfplumber text extraction
  - ~140 documents total

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # No-op (treaty texts rarely change)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ITU-Legal")

BASIC_TEXTS_BASE = "https://www.itu.int/en/council/Documents/basic-texts-2023/"
HISTORY_BASE = "https://search.itu.int/history/HistoryDigitalCollectionDocLibrary/"

# All known resolution numbers (discovered via HEAD scan)
RESOLUTION_NUMBERS = [
    2, 5, 6, 7, 14, 16, 21, 22, 25, 30, 32, 33, 34, 37, 38, 41, 45, 46,
    48, 51, 53, 55, 56, 57, 58, 59, 60, 64, 66, 68, 69, 70, 71, 75, 77,
    80, 86, 91, 94, 96, 98, 99, 100, 101, 102, 111, 114, 118, 119, 122,
    123, 124, 125, 126, 127, 130, 131, 133, 135, 136, 137, 138, 139, 140,
    143, 144, 145, 146, 148, 150, 151, 152, 154, 157, 158, 159, 160, 161,
    162, 164, 165, 167, 168, 169, 170, 173, 174, 175, 176, 177, 178, 179,
    180, 181, 182, 183, 184, 186, 188, 189, 190, 191, 193, 195, 196, 197,
    198, 199, 200, 201, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212,
    213, 214, 215, 216, 217, 218, 219,
]

DECISION_NUMBERS = [3, 5, 11, 12, 14]

# Core treaty documents with their URLs and metadata
CORE_DOCUMENTS: List[Dict[str, str]] = [
    {
        "id": "ITU-Constitution",
        "title": "Constitution of the International Telecommunication Union (Geneva, 1992, amended through PP-22)",
        "url": BASIC_TEXTS_BASE + "Constitution-E.pdf",
        "document_type": "constitution",
        "date": "2022-10-14",
    },
    {
        "id": "ITU-Convention",
        "title": "Convention of the International Telecommunication Union (Geneva, 1992, amended through PP-22)",
        "url": BASIC_TEXTS_BASE + "Convention-E.pdf",
        "document_type": "convention",
        "date": "2022-10-14",
    },
    {
        "id": "ITU-RadioRegulations-Vol1",
        "title": "Radio Regulations, Edition of 2024 — Volume 1 (Articles)",
        "url": HISTORY_BASE + "1.49.48.en.101.pdf",
        "document_type": "regulation",
        "date": "2024-01-01",
    },
    {
        "id": "ITU-RadioRegulations-Vol2",
        "title": "Radio Regulations, Edition of 2024 — Volume 2 (Appendices)",
        "url": HISTORY_BASE + "1.49.48.en.102.pdf",
        "document_type": "regulation",
        "date": "2024-01-01",
    },
    {
        "id": "ITU-RadioRegulations-Vol3",
        "title": "Radio Regulations, Edition of 2024 — Volume 3 (Resolutions and Recommendations)",
        "url": HISTORY_BASE + "1.49.48.en.103.pdf",
        "document_type": "regulation",
        "date": "2024-01-01",
    },
    {
        "id": "ITU-ITRs-1988",
        "title": "International Telecommunication Regulations (Melbourne, 1988)",
        "url": "https://www.itu.int/osg/csd/wtpf/wtpf2009/documents/ITU_ITRs_88.pdf",
        "document_type": "regulation",
        "date": "1988-12-09",
    },
    {
        "id": "ITU-CouncilDecisions-2023",
        "title": "Collection of Council Decisions (2023 edition)",
        "url": "https://www.itu.int/dms_pub/itu-s/opb/conf/S-CONF-CL-2023-PDF-E.pdf",
        "document_type": "decision_collection",
        "date": "2023-01-01",
    },
]


class ITULegalScraper(BaseScraper):
    """Scraper for INTL/ITU-Legal -- ITU Treaty Instruments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/pdf,*/*;q=0.8",
        })

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/ITU-Legal",
            source_id="",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    def _build_document_list(self, sample: bool = False) -> List[Dict[str, str]]:
        """Build the full list of documents to fetch."""
        docs = []

        # Core documents (Constitution, Convention, Radio Regs, ITRs, Council)
        docs.extend(CORE_DOCUMENTS)

        # Plenipotentiary Resolutions
        for n in RESOLUTION_NUMBERS:
            docs.append({
                "id": f"ITU-PP-RES-{n:03d}",
                "title": f"ITU Plenipotentiary Conference Resolution {n} (PP-22)",
                "url": f"{BASIC_TEXTS_BASE}RES-{n:03d}-E.pdf",
                "document_type": "resolution",
                "date": "2022-10-14",
            })

        # Plenipotentiary Decisions
        for n in DECISION_NUMBERS:
            docs.append({
                "id": f"ITU-PP-DEC-{n:03d}",
                "title": f"ITU Plenipotentiary Conference Decision {n} (PP-22)",
                "url": f"{BASIC_TEXTS_BASE}DEC-{n:03d}-E.pdf",
                "document_type": "decision",
                "date": "2022-10-14",
            })

        if sample:
            # Return a representative sample: 2 core + 11 resolutions + 2 decisions
            sample_docs = docs[:2]  # Constitution + Convention
            sample_docs.extend(docs[7:18])  # First 11 resolutions
            sample_docs.extend(docs[-5:-3])  # 2 decisions
            return sample_docs[:15]

        return docs

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw ITU record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "INTL/ITU-Legal",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "document_type": raw.get("document_type", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all ITU legal documents."""
        docs = self._build_document_list(sample=False)
        for i, doc_meta in enumerate(docs):
            logger.info("[%d/%d] %s", i + 1, len(docs), doc_meta["title"][:70])
            text = self._download_pdf_text(doc_meta["url"])
            if text:
                yield {
                    "_id": doc_meta["id"],
                    "title": doc_meta["title"],
                    "text": text,
                    "date": doc_meta["date"],
                    "url": doc_meta["url"],
                    "document_type": doc_meta["document_type"],
                }
            else:
                logger.warning("Skipped (no text): %s", doc_meta["title"])

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """ITU treaty texts rarely change; re-fetch core documents only."""
        for doc_meta in CORE_DOCUMENTS:
            text = self._download_pdf_text(doc_meta["url"])
            if text:
                yield {
                    "_id": doc_meta["id"],
                    "title": doc_meta["title"],
                    "text": text,
                    "date": doc_meta["date"],
                    "url": doc_meta["url"],
                    "document_type": doc_meta["document_type"],
                }

    def test_connection(self) -> bool:
        """Quick connectivity test — check Constitution PDF is reachable."""
        try:
            self.rate_limiter.wait()
            resp = self.session.head(
                BASIC_TEXTS_BASE + "Constitution-E.pdf", timeout=15,
                allow_redirects=True,
            )
            ok = resp.status_code == 200
            logger.info("Connection %s (HTTP %d)", "OK" if ok else "FAILED",
                        resp.status_code)
            return ok
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        docs = self._build_document_list(sample=sample)
        label = "SAMPLE" if sample else "FULL"
        logger.info("Running %s bootstrap: %d documents", label, len(docs))

        count = 0
        for i, doc_meta in enumerate(docs):
            logger.info("[%d/%d] %s", i + 1, len(docs),
                        doc_meta["title"][:70])
            text = self._download_pdf_text(doc_meta["url"])
            if not text:
                logger.warning("Skipped (no text): %s", doc_meta["title"])
                continue

            raw = {
                "_id": doc_meta["id"],
                "title": doc_meta["title"],
                "text": text,
                "date": doc_meta["date"],
                "url": doc_meta["url"],
                "document_type": doc_meta["document_type"],
            }
            normalized = self.normalize(raw)

            fname = re.sub(r'[^\w\-.]', '_', f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info("  -> %d chars of text", len(normalized["text"]))

        logger.info("%s bootstrap complete: %d/%d records saved",
                    label, count, len(docs))
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="INTL/ITU-Legal Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ITULegalScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for doc in scraper.fetch_updates():
            normalized = scraper.normalize(doc)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
