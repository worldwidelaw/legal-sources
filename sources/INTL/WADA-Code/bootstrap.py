#!/usr/bin/env python3
"""
INTL/WADA-Code -- World Anti-Doping Agency Code and Standards

Downloads and extracts full text from WADA's core regulatory documents:
  - World Anti-Doping Code (2021)
  - 8 International Standards (ISCCS, ISE, ISL, ISRM, ISPPPI, ISTI, ISTUE)
  - Prohibited Lists (2025, 2026)
  - Athlete's Guide to the 2021 Code

PDFs are fetched from ITA.sport mirrors with WADA direct URLs as fallback.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Re-scan (same as bootstrap for static docs)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, List, Optional

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WADA-Code")

SOURCE_ID = "INTL/WADA-Code"
MIN_TEXT_CHARS = 200

# Each document: (id_slug, title, abbreviation, doc_type, effective_date, urls_to_try)
# urls_to_try is a list of mirrors; we use the first that succeeds.
DOCUMENTS = [
    (
        "wadc-2021",
        "World Anti-Doping Code 2021",
        "WADC",
        "code",
        "2021-01-01",
        [
            "https://ita.sport/uploads/2021/01/2021_wada_code.pdf",
            "https://www.wada-ama.org/sites/default/files/resources/files/2021_wada_code.pdf",
            "https://www.usada.org/wp-content/uploads/2021_wada_code.pdf",
        ],
    ),
    (
        "isccs-2021",
        "International Standard for Code Compliance by Signatories (ISCCS) 2021",
        "ISCCS",
        "international_standard",
        "2021-04-01",
        [
            "https://ita.sport/uploads/2021/01/international_standard_isccs_2020.pdf",
            "https://www.wada-ama.org/sites/default/files/resources/files/international_standard_isccs_2021.pdf",
        ],
    ),
    (
        "ise-2021",
        "International Standard for Education (ISE) 2021",
        "ISE",
        "international_standard",
        "2021-01-01",
        [
            "https://ita.sport/uploads/2021/01/international_standard_ise_2020.pdf",
            "https://www.wada-ama.org/sites/default/files/resources/files/international_standard_ise_2021.pdf",
        ],
    ),
    (
        "isl-2021",
        "International Standard for Laboratories (ISL) 2021",
        "ISL",
        "international_standard",
        "2021-01-01",
        [
            "https://ita.sport/uploads/2021/02/isl_2021.pdf",
            "https://www.wada-ama.org/sites/default/files/resources/files/isl_2021.pdf",
        ],
    ),
    (
        "isrm-2021",
        "International Standard for Results Management (ISRM) 2021",
        "ISRM",
        "international_standard",
        "2021-05-20",
        [
            "https://ita.sport/uploads/2021/06/international_standard_isrm_-_final_english_-_post_exco_20_may_2021.pdf",
            "https://www.wada-ama.org/sites/default/files/resources/files/2021_isrm_0.pdf",
        ],
    ),
    (
        "ispppi-2021",
        "International Standard for the Protection of Privacy and Personal Information (ISPPPI) 2021",
        "ISPPPI",
        "international_standard",
        "2021-01-01",
        [
            "https://ita.sport/uploads/2021/02/2021_ispppi_en_24092020.pdf",
            "https://www.wada-ama.org/sites/default/files/2022-01/international_standard_ispppi_-_november_2021_0.pdf",
        ],
    ),
    (
        "isti-2023",
        "International Standard for Testing and Investigations (ISTI) 2023",
        "ISTI",
        "international_standard",
        "2023-01-01",
        [
            "https://ita.sport/uploads/2022/12/isti_2023_w_annex_k_final_clean.pdf",
            "https://www.wada-ama.org/sites/default/files/resources/files/international_standard_isti_-_2021.pdf",
        ],
    ),
    (
        "istue-2021",
        "International Standard for Therapeutic Use Exemptions (ISTUE) 2021",
        "ISTUE",
        "international_standard",
        "2021-01-01",
        [
            "https://www.wada-ama.org/sites/default/files/resources/files/international_standard_istue_-_2021.pdf",
        ],
    ),
    (
        "prohibited-list-2026",
        "Prohibited List 2026",
        "PL-2026",
        "prohibited_list",
        "2026-01-01",
        [
            "https://ita.sport/uploads/2025/09/2026list_en_final_clean_september_2025.pdf",
            "https://www.wada-ama.org/sites/default/files/2025-09/2026list_en.pdf",
        ],
    ),
    (
        "prohibited-list-2025",
        "Prohibited List 2025",
        "PL-2025",
        "prohibited_list",
        "2025-01-01",
        [
            "https://ita.sport/uploads/2024/09/2025list_en_final_clean_12_september_2024.pdf",
            "https://www.wada-ama.org/sites/default/files/2024-09/2025list_en_final_clean_12_september_2024.pdf",
        ],
    ),
    (
        "athlete-guide-2021-code",
        "Athlete's Guide to the 2021 Code",
        "AG-2021",
        "guide",
        "2021-01-01",
        [
            "https://www.orad-pan.org/wp-content/uploads/2021/01/Athlete-Guide-2021-Code_English_LIVE.pdf",
        ],
    ),
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
})


def _download_pdf(urls: List[str], timeout: int = 120) -> Optional[bytes]:
    """Try each mirror URL in order; return PDF bytes or None."""
    for url in urls:
        try:
            r = SESSION.get(url, timeout=timeout)
            if r.status_code == 200 and len(r.content) > 1000:
                return r.content
            logger.warning("Bad response from %s: %s (%d bytes)", url, r.status_code, len(r.content))
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
        time.sleep(1)
    return None


def _extract_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    pages_text = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            pages_text.append(t)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    text = "\n\n".join(pages_text)
    # Clean up excessive whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


class WADACodeScraper(BaseScraper):
    """Scraper for WADA Code and International Standards."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all WADA documents with full text."""
        for doc_id, title, abbrev, doc_type, eff_date, urls in DOCUMENTS:
            logger.info("Fetching %s (%s)...", abbrev, title)
            pdf_bytes = _download_pdf(urls)
            if pdf_bytes is None:
                logger.error("Could not download %s from any mirror", doc_id)
                continue

            try:
                text = _extract_text(pdf_bytes)
            except Exception as e:
                logger.error("PDF extraction failed for %s: %s", doc_id, e)
                continue

            if len(text) < MIN_TEXT_CHARS:
                logger.warning("Insufficient text for %s: %d chars", doc_id, len(text))
                continue

            yield {
                "id": doc_id,
                "title": title,
                "abbreviation": abbrev,
                "doc_type": doc_type,
                "effective_date": eff_date,
                "text": text,
                "pages": len(text.split("\n\n")),
                "pdf_url": urls[0],
                "pdf_size": len(pdf_bytes),
            }
            time.sleep(2)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all documents (static corpus, no incremental updates)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": f"WADA_{raw['id']}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["effective_date"],
            "url": raw["pdf_url"],
            "abbreviation": raw["abbreviation"],
            "doc_type": raw["doc_type"],
            "issuing_body": "World Anti-Doping Agency (WADA)",
            "language": "en",
            "pdf_size_bytes": raw["pdf_size"],
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WADA-Code bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WADACodeScraper()

    if args.command == "test":
        logger.info("Testing connectivity to ITA.sport...")
        try:
            r = SESSION.get("https://ita.sport/uploads/2021/01/2021_wada_code.pdf",
                            timeout=30, stream=True)
            logger.info("ITA.sport responded: %s (content-length: %s)",
                        r.status_code, r.headers.get("content-length", "?"))
            r.close()
            print("OK")
        except Exception as e:
            logger.error("Connection failed: %s", e)
            sys.exit(1)
        return

    if args.command in ("bootstrap", "update"):
        sample_mode = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=10)
        logger.info("Done: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
