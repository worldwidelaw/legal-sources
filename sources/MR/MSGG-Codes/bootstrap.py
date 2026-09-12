#!/usr/bin/env python3
"""
MR/MSGG-Codes -- Mauritania Consolidated Legal Codes

Fetches 16 consolidated legal codes from the Secrétariat Général du
Gouvernement of Mauritania (msgg.gov.mr). Codes are available as direct
PDF downloads.

Strategy:
  - Scrape the codes listing page to extract PDF URLs and titles
  - Download each PDF and extract text with pdfminer
  - 16 codes total, French language, covering 2000-2025

Usage:
  python bootstrap.py bootstrap          # Fetch all 16 codes
  python bootstrap.py bootstrap --sample # Same (only 16 total)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import logging
import re
import time
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser
import html as html_mod

import requests
from pdfminer.high_level import extract_text as pdfminer_extract

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MR.MSGG-Codes")

LISTING_URL = "https://msgg.gov.mr/fr/droit-mauritanien/les-codes-consolides.html"
BASE_URL = "https://msgg.gov.mr"


def _parse_codes_page(html: str) -> List[Dict[str, str]]:
    """Extract code entries (title + PDF URL) from the listing page."""
    codes = []
    for match in re.finditer(
        r'<a[^>]*href="(https://msgg\.gov\.mr/codes/[^"]+\.pdf)"[^>]*>(.*?)</a>',
        html,
        re.DOTALL,
    ):
        url = match.group(1)
        raw_text = re.sub(r"<[^>]+>", "", match.group(2)).strip()
        # Clean whitespace and extract title (after "DM" prefix)
        lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
        title = " ".join(l for l in lines if l != "DM")
        if not title:
            title = url.rsplit("/", 1)[-1].replace(".pdf", "").replace("-", " ")
        # Extract year from URL or title
        year_match = re.search(r"(\d{4})", url.rsplit("/", 1)[-1])
        year = year_match.group(1) if year_match else None
        code_id = url.rsplit("/", 1)[-1].replace(".pdf", "")
        codes.append(
            {"code_id": code_id, "title": title, "url": url, "year": year}
        )
    return codes


def _download_and_extract_pdf(
    session: requests.Session, url: str
) -> Optional[str]:
    """Download a PDF and extract text using pdfminer."""
    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        if len(resp.content) < 100:
            return None
        text = pdfminer_extract(io.BytesIO(resp.content))
        text = text.strip()
        if len(text) < 50:
            return None
        return text
    except Exception as e:
        logger.warning("PDF extraction failed for %s: %s", url, e)
        return None


class MRMSGGCodesScraper(BaseScraper):
    SOURCE_ID = "MR/MSGG-Codes"

    def __init__(self):
        super().__init__(source_dir=str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "LegalDataHunter/1.0 (legal research)"}
        )

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all consolidated codes with full text."""
        # Fetch listing page
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        codes = _parse_codes_page(resp.text)
        logger.info("Found %d codes on listing page", len(codes))

        for code in codes:
            logger.info("Downloading %s: %s", code["code_id"], code["url"][:80])
            text = _download_and_extract_pdf(self.session, code["url"])
            if not text:
                logger.warning("No text extracted for %s", code["code_id"])
                continue
            time.sleep(2)
            yield self.normalize({**code, "text": text})

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """All codes are static; re-fetch everything."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": f"mr-msgg-{raw['code_id']}",
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "code_id": raw["code_id"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("year"),
            "url": raw["url"],
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.session.get(LISTING_URL, timeout=15)
            resp.raise_for_status()
            codes = _parse_codes_page(resp.text)
            if codes:
                logger.info("Test OK: found %d codes", len(codes))
                return True
            logger.error("Test failed: no codes found")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="MR/MSGG-Codes bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "test"], help="Command to run"
    )
    parser.add_argument(
        "--sample", action="store_true", help="Fetch sample records"
    )
    parser.add_argument(
        "--full", action="store_true", help="Fetch all records"
    )
    args = parser.parse_args()

    scraper = MRMSGGCodesScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    # bootstrap
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in scraper.fetch_all():
        count += 1
        fname = sample_dir / f"{record['_id']}.json"
        fname.write_text(json.dumps(record, ensure_ascii=False, indent=2))
        logger.info(
            "[%d] %s — %d chars",
            count,
            record["title"][:60],
            len(record.get("text", "")),
        )
    logger.info("Done: %d records saved to %s", count, sample_dir)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
