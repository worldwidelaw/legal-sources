#!/usr/bin/env python3
"""
NL/Belastingdienst-AdvanceTaxRulings -- Dutch Advance Tax Rulings (ATR/APA/Overige)

Fetches anonymised ruling summaries from the Dutch Tax Authority (Belastingdienst).

Strategy:
  - Scrapes 3 listing pages on belastingdienst.nl to extract ruling identifiers
  - Constructs direct PDF download URLs from identifiers
  - Downloads PDFs and extracts full text via common.pdf_extract

Data:
  - ~800 Advance Tax Rulings (ATR) since 2019
  - ~1100 Advance Pricing Agreements (APA) since 2019
  - ~285 Other international rulings (RULOV/TONNAGE) since 2019
  Total: ~2200 rulings

All documents are _type "doctrine" (official tax guidance/interpretation).

License: Public regulatory data (Netherlands)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.Belastingdienst-AdvanceTaxRulings")

BASE_URL = "https://www.belastingdienst.nl"
PDF_BASE = "https://download.belastingdienst.nl/belastingdienst/docs"

# Listing pages
LISTING_PAGES = {
    "ATR": "/wps/wcm/connect/bldcontentnl/standaard_functies/prive/contact/rechten_en_plichten_bij_de_belastingdienst/ruling/atr",
    "APA": "/wps/wcm/connect/bldcontentnl/standaard_functies/prive/contact/rechten_en_plichten_bij_de_belastingdienst/ruling/apa",
    "Overige": "/wps/wcm/connect/bldcontentnl/standaard_functies/prive/contact/rechten_en_plichten_bij_de_belastingdienst/ruling/overige-internationale-rulings",
}

# Regex patterns for extracting ruling identifiers from listing page links
# ATR: advance-tax-ruling-20260414-atr-000010
# APA: advance-pricing-agreement-20260414-apa-000009
# Overige: overige-ruling-20260331-rulov-000014 or overige-ruling-20240101-tonnage-000001
RULING_PATTERNS = {
    "ATR": re.compile(r'advance-tax-ruling-(\d{8})-atr-(\d{6})'),
    "APA": re.compile(r'advance-pricing-agreement-(\d{8})-apa-(\d{6})'),
    "Overige": re.compile(r'overige-ruling-(\d{8})-(rulov|tonnage)-(\d{6})'),
}


def _date_from_str(date_str: str) -> Optional[str]:
    """Convert YYYYMMDD string to ISO date."""
    if not date_str or len(date_str) != 8:
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


class DutchRulingsScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "nl,en;q=0.9",
            },
        )

    def _extract_rulings_from_page(self, html: str, ruling_type: str) -> List[Dict[str, Any]]:
        """Extract ruling identifiers from a listing page."""
        results = []
        seen = set()

        if ruling_type == "Overige":
            pattern = RULING_PATTERNS["Overige"]
            for m in pattern.finditer(html):
                date_str, sub_type, number = m.groups()
                key = f"{date_str}-{sub_type}-{number}"
                if key in seen:
                    continue
                seen.add(key)
                pdf_url = f"{PDF_BASE}/rul-{date_str}-{sub_type.lower()}-{number}.pdf"
                results.append({
                    "date_str": date_str,
                    "ruling_type": f"Overige ({sub_type.upper()})",
                    "ruling_number": f"{sub_type.upper()} {number}",
                    "pdf_url": pdf_url,
                    "title": f"Overige ruling {date_str} {sub_type.upper()} {number}",
                })
        else:
            type_lower = ruling_type.lower()
            pattern = RULING_PATTERNS[ruling_type]
            for m in pattern.finditer(html):
                date_str, number = m.groups()
                key = f"{date_str}-{type_lower}-{number}"
                if key in seen:
                    continue
                seen.add(key)
                pdf_url = f"{PDF_BASE}/rul-{date_str}-{type_lower}-{number}.pdf"
                type_full = "Advance Tax Ruling" if ruling_type == "ATR" else "Advance Pricing Agreement"
                results.append({
                    "date_str": date_str,
                    "ruling_type": ruling_type,
                    "ruling_number": f"{ruling_type} {number}",
                    "pdf_url": pdf_url,
                    "title": f"{type_full} {date_str} {ruling_type} {number}",
                })

        return results

    def _get_all_rulings(self) -> List[Dict[str, Any]]:
        """Fetch all ruling entries from the 3 listing pages."""
        all_rulings = []

        for ruling_type, page_path in LISTING_PAGES.items():
            logger.info(f"Fetching {ruling_type} listing page...")
            resp = self.client.get(page_path)
            if not resp or resp.status_code != 200:
                logger.warning(f"  Failed to fetch {ruling_type} page: {resp.status_code if resp else 'no response'}")
                continue

            items = self._extract_rulings_from_page(resp.text, ruling_type)
            all_rulings.extend(items)
            logger.info(f"  Found {len(items)} {ruling_type} rulings")
            time.sleep(1)

        logger.info(f"Total rulings to process: {len(all_rulings)}")
        return all_rulings

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF from download.belastingdienst.nl."""
        import requests
        try:
            resp = requests.get(url, timeout=(15, 60))
            resp.raise_for_status()
            if len(resp.content) < 200:
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"  PDF download failed {url}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all rulings with full text from PDFs."""
        rulings = self._get_all_rulings()

        for i, ruling in enumerate(rulings):
            url = ruling["pdf_url"]
            title = ruling["title"]
            logger.info(f"[{i+1}/{len(rulings)}] {title}...")

            source_id = hashlib.md5(url.encode()).hexdigest()

            pdf_bytes = self._download_pdf(url)
            if not pdf_bytes:
                logger.warning(f"  Could not download PDF")
                continue

            try:
                text = extract_pdf_markdown(
                    source="NL/Belastingdienst-AdvanceTaxRulings",
                    source_id=source_id,
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                )
            except Exception as e:
                logger.warning(f"  PDF extraction failed: {e}")
                continue

            if not text or len(text.strip()) < 50:
                logger.warning(f"  Insufficient text ({len(text) if text else 0} chars)")
                continue

            yield self.normalize({
                "url": url,
                "title": title,
                "date": _date_from_str(ruling["date_str"]),
                "text": text,
                "ruling_type": ruling["ruling_type"],
                "ruling_number": ruling["ruling_number"],
            })

            time.sleep(0.5)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield documents updated since the given date."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        url = raw["url"]
        doc_id = hashlib.md5(url.encode()).hexdigest()

        return {
            "_id": f"NL/Belastingdienst-AdvanceTaxRulings/{doc_id}",
            "_source": "NL/Belastingdienst-AdvanceTaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": url,
            "ruling_type": raw.get("ruling_type", ""),
            "ruling_number": raw.get("ruling_number", ""),
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="NL/Belastingdienst-AdvanceTaxRulings scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    scraper = DutchRulingsScraper()

    if args.command == "test":
        logger.info("Testing connectivity to belastingdienst.nl...")
        resp = scraper.client.get(LISTING_PAGES["ATR"])
        if resp and resp.status_code == 200:
            logger.info(f"OK — got {len(resp.text)} bytes from ATR listing page")
        else:
            logger.error(f"FAIL — status {resp.status_code if resp else 'no response'}")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        for doc in scraper.fetch_all():
            count += 1
            text_len = len(doc.get("text", ""))
            logger.info(
                f"  #{count} {doc['title'][:50]}... "
                f"({text_len} chars, {doc['ruling_type']})"
            )

            # Save sample
            if count <= 20:
                fname = re.sub(r'[^\w\-]', '_', doc["_id"])[:80] + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)

            if count >= max_records:
                break

        logger.info(f"Done — {count} records fetched")


if __name__ == "__main__":
    main()
