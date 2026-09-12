#!/usr/bin/env python3
"""
INTL/OHADA-UniformActs -- OHADA Uniform Acts & Treaty

Fetches the 10 Uniform Acts, Revised Treaty, and CCJA Arbitration Rules from
the Organisation for the Harmonization of Business Law in Africa (OHADA).

Strategy:
  - Download English PDF translations from ohadalegis.com
  - Extract full text via pdfplumber
  - 12 documents total covering 17 African member states

Data:
  - 10 Uniform Acts (commercial law, companies, securities, insolvency,
    arbitration, mediation, cooperatives, accounting, carriage, recovery)
  - Revised Treaty (2008) and CCJA Arbitration Rules (2017)
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap --sample # Fetch 15 sample records -> sample/
  python bootstrap.py bootstrap-fast     # Alias for the full bootstrap (fleet entry point)
  python bootstrap.py update             # Fetch all (same as bootstrap)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OHADA-UniformActs")

SOURCE_ID = "INTL/OHADA-UniformActs"
BASE_URL = "https://www.ohadalegis.com/anglais/telAUGB"

# Below this fraction of distinct lines, an extraction is running headers, not
# a document. See normalize(); measured 0.93-1.00 across all 12 real acts.
MIN_DISTINCT_LINE_RATIO = 0.5

# All 12 documents with metadata and PDF URLs
DOCUMENTS = [
    {
        "id": "ohada-revised-treaty-2008",
        "title": "Treaty on the Harmonization of Business Law in Africa (Revised 2008)",
        "short_title": "Revised Treaty",
        "date": "2008-10-17",
        "adopted_location": "Quebec, Canada",
        "category": "treaty",
        "pdf_url": f"{BASE_URL}/2008-Ohada-Revised-Treaty-en.pdf",
        "language": "en",
    },
    {
        "id": "ohada-general-commercial-law-2010",
        "title": "Uniform Act on General Commercial Law (Revised 2010)",
        "short_title": "General Commercial Law",
        "date": "2010-12-15",
        "adopted_location": "Lomé, Togo",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/2010-Ohada-General-Commercial-Law-en.pdf",
        "language": "en",
    },
    {
        "id": "ohada-commercial-companies-2014",
        "title": "Uniform Act on Commercial Companies and Economic Interest Groups (Revised 2014)",
        "short_title": "Commercial Companies",
        "date": "2014-01-30",
        "adopted_location": "Ouagadougou, Burkina Faso",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/2014-Ohada-Revised-Uniform-Act-Commercial-companies-en.pdf",
        "language": "en",
    },
    {
        "id": "ohada-securities-2010",
        "title": "Uniform Act on Security Interests (Revised 2010)",
        "short_title": "Securities",
        "date": "2010-12-15",
        "adopted_location": "Lomé, Togo",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/2010-Ohada-Revised-Uniform-Act-Securities.pdf",
        "language": "en",
    },
    {
        "id": "ohada-insolvency-2015",
        "title": "Uniform Act on Insolvency Proceedings (Revised 2015)",
        "short_title": "Insolvency",
        "date": "2015-09-10",
        "adopted_location": "Grand-Bassam, Côte d'Ivoire",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/2015-Revised-Uniform-Act-bankruptcy-proceedings-en.pdf",
        "language": "en",
    },
    {
        "id": "ohada-simplified-recovery-2023",
        "title": "Uniform Act on Simplified Debt Collection Procedures and Enforcement Measures (Revised 2023)",
        "short_title": "Simplified Recovery",
        "date": "2023-10-17",
        "adopted_location": "Kinshasa, DRC",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/Ohada-Uniform-Act-simplified-recovery-procedures_execution-measures.17.10.2023.pdf",
        "language": "en",
    },
    {
        "id": "ohada-arbitration-2017",
        "title": "Uniform Act on Arbitration (Revised 2017)",
        "short_title": "Arbitration",
        "date": "2017-11-23",
        "adopted_location": "Conakry, Guinea",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/2017-Ohada-uniform-Act-Arbitration-en.pdf",
        "language": "en",
    },
    {
        "id": "ohada-mediation-2017",
        "title": "Uniform Act on Mediation (2017)",
        "short_title": "Mediation",
        "date": "2017-11-23",
        "adopted_location": "Conakry, Guinea",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/Ohada-Uniform-Acte-Mediation-en.pdf",
        "language": "en",
    },
    {
        "id": "ohada-carriage-goods-2003",
        "title": "Uniform Act on the Contract for the Carriage of Goods by Road (2003)",
        "short_title": "Carriage of Goods",
        "date": "2003-03-22",
        "adopted_location": "Yaoundé, Cameroon",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/2003-Ohada-Uniform-Act-contract-carriage-goods-road.pdf",
        "language": "en",
    },
    {
        "id": "ohada-cooperatives-2010",
        "title": "Uniform Act on Cooperative Societies (2010)",
        "short_title": "Cooperatives",
        "date": "2010-12-15",
        "adopted_location": "Lomé, Togo",
        "category": "uniform_act",
        "pdf_url": f"{BASE_URL}/Ohada-Uniform-Act-Cooperatives-en.pdf",
        "language": "en",
    },
    {
        "id": "ohada-audcif-2017",
        "title": "Acte uniforme relatif au droit comptable et à l'information financière - AUDCIF (2017)",
        "short_title": "Accounting & Financial Information (AUDCIF)",
        "date": "2017-01-26",
        "adopted_location": "Brazzaville, Congo",
        "category": "uniform_act",
        # ohadalegis serves the whole 1,246-page Journal Officiel special issue,
        # whose SYSCOHADA accounting tables are scanned images with no text
        # layer — extraction returned nothing but the running page headers.
        # OHADA's own digital library publishes the Act on its own (54 pages,
        # OCR'd but complete: articles 1-113 plus the signature page).
        "pdf_url": "https://biblio.ohada.org/doc_num.php?explnum_id=2061",
        "language": "fr",
    },
    {
        "id": "ohada-ccja-arbitration-rules-2017",
        "title": "CCJA Arbitration Rules (Revised 2017)",
        "short_title": "CCJA Arbitration Rules",
        "date": "2017-11-23",
        "adopted_location": "Conakry, Guinea",
        "category": "procedural_rules",
        "pdf_url": f"{BASE_URL}/2017 CCJA-Arbitration-rules-en.pdf",
        "language": "en",
    },
]

# OHADA member states (ISO 3166-1 alpha-2)
MEMBER_STATES = [
    "BJ",  # Benin
    "BF",  # Burkina Faso
    "CM",  # Cameroon
    "CF",  # Central African Republic
    "TD",  # Chad
    "KM",  # Comoros
    "CG",  # Congo
    "CD",  # Democratic Republic of Congo
    "CI",  # Côte d'Ivoire
    "GQ",  # Equatorial Guinea
    "GA",  # Gabon
    "GN",  # Guinea
    "GW",  # Guinea-Bissau
    "ML",  # Mali
    "NE",  # Niger
    "SN",  # Senegal
    "TG",  # Togo
]


class OHADAUniformActsScraper(BaseScraper):
    """Scraper for INTL/OHADA-UniformActs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text from every page via pdfplumber."""
        try:
            logger.info("Downloading PDF: %s", pdf_url)
            resp = self.session.get(pdf_url, timeout=180)
            resp.raise_for_status()

            if not resp.content or b"%PDF" not in resp.content[:10]:
                logger.warning("Response is not a PDF: %s", pdf_url)
                return None

            pdf = pdfplumber.open(io.BytesIO(resp.content))
            total_pages = len(pdf.pages)
            pages_text = []

            for page in pdf.pages:
                text = page.extract_text()
                if text and text.strip():
                    pages_text.append(text.strip())

            pdf.close()
            full_text = "\n\n".join(pages_text)
            logger.info("Extracted %d chars from %d pages", len(full_text), total_pages)
            return full_text if full_text.strip() else None

        except Exception as e:
            logger.error("PDF extraction failed for %s: %s", pdf_url, e)
            return None

    def _clean_text(self, text: str) -> str:
        """Clean extracted PDF text."""
        # Remove excessive whitespace but preserve paragraph breaks
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Remove page numbers (standalone numbers on a line)
        text = re.sub(r"\n\d{1,4}\n", "\n", text)
        return text.strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all OHADA documents with full text from PDFs."""
        for doc in DOCUMENTS:
            time.sleep(2)  # Rate limit
            text = self._download_pdf_text(doc["pdf_url"])
            if text:
                text = self._clean_text(text)
                yield {**doc, "text": text}
            else:
                logger.warning("No text extracted for %s", doc["id"])

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """OHADA acts change rarely; full refetch."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        # A scanned PDF whose only text layer is the running header extracts as
        # the same two lines repeated for hundreds of pages, which is long
        # enough to sail past the length check above. Real acts sit at 0.93+
        # distinct lines; the header-only AUDCIF extraction sat at 0.15.
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        distinct_ratio = len(set(lines)) / len(lines)
        if distinct_ratio < MIN_DISTINCT_LINE_RATIO:
            logger.error(
                "%s: %.0f%% of lines are repeats (%d chars) — PDF has no body "
                "text layer, only running headers. Dropping.",
                raw["id"], (1 - distinct_ratio) * 100, len(text),
            )
            return None

        return {
            "_id": raw["id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "short_title": raw.get("short_title", ""),
            "text": text,
            "date": raw.get("date"),
            "category": raw.get("category", "uniform_act"),
            "language": raw.get("language", "en"),
            "adopted_location": raw.get("adopted_location", ""),
            "member_states": MEMBER_STATES,
            "url": raw.get("pdf_url", "https://www.ohada.org/en/uniform-acts/"),
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/OHADA-UniformActs scraper")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial fetch")
    boot.add_argument("--sample", action="store_true", help="Fetch sample records only")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    sub.add_parser("bootstrap-fast", help="Alias for the full bootstrap (fleet entry point)")
    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = OHADAUniformActsScraper()

    if args.command == "test":
        try:
            resp = scraper.session.head(DOCUMENTS[0]["pdf_url"], timeout=15)
            print(f"OK: {resp.status_code}")
            sys.exit(0)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=getattr(args, "sample", False), sample_size=15)
        print(json.dumps(stats, indent=2, default=str))
    elif args.command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
