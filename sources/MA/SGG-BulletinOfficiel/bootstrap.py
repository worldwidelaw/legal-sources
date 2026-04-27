#!/usr/bin/env python3
"""
MA/SGG-BulletinOfficiel -- Morocco Official Bulletin (Bulletin Officiel)

Fetches gazette issues from sgg.gov.ma via a JSON API that returns the full
catalog for each module (Arabic general + French translation).  PDFs are
downloaded and full text is extracted via common/pdf_extract.

API endpoint (no auth):
  GET /DesktopModules/MVC/TableListBO/BO/AjaxMethod
  Headers: ModuleId, TabId

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MA.SGG-BulletinOfficiel")

BASE_URL = "https://www.sgg.gov.ma"
API_ENDPOINT = f"{BASE_URL}/DesktopModules/MVC/TableListBO/BO/AjaxMethod"

# Primary modules to scrape
MODULES = {
    "ar": {"module_id": "3111", "tab_id": "847", "label": "Arabic General"},
    "fr": {"module_id": "2873", "tab_id": "775", "label": "French Translation"},
}


def _parse_dotnet_date(s: str) -> Optional[str]:
    """Parse .NET /Date(timestamp)/ to ISO 8601 date string."""
    if not s:
        return None
    m = re.search(r"/Date\((\-?\d+)", s)
    if m:
        ts_ms = int(m.group(1))
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    return None


def _normalise_pdf_url(raw_url: str) -> Optional[str]:
    """Ensure the PDF URL is absolute."""
    if not raw_url or not raw_url.strip():
        return None
    url = raw_url.strip()
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return BASE_URL + url
    return BASE_URL + "/" + url


class MoroccoBulletinOfficielScraper(BaseScraper):
    """Scraper for MA/SGG-BulletinOfficiel."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{BASE_URL}/arabe/BulletinOfficiel.aspx",
        })

    def _fetch_catalog(self, lang: str) -> List[Dict[str, Any]]:
        """Fetch the full bulletin catalog for a language module."""
        mod = MODULES[lang]
        headers = {
            "ModuleId": mod["module_id"],
            "TabId": mod["tab_id"],
        }
        logger.info(f"Fetching {mod['label']} catalog (ModuleId={mod['module_id']})...")
        try:
            resp = self.session.get(API_ENDPOINT, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                logger.info(f"  -> {len(data)} records for {mod['label']}")
                return data
            logger.warning(f"Unexpected response type: {type(data)}")
            return []
        except Exception as e:
            logger.error(f"Failed to fetch catalog for {mod['label']}: {e}")
            return []

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw API record + extracted text into standard schema."""
        lang = raw.get("lang", "ar")
        bo_num = str(raw.get("BoNum", "")).strip()
        bo_date = _parse_dotnet_date(raw.get("BoDate", ""))
        pdf_url = raw.get("pdf_url", "")

        doc_id = f"MA-BO-{lang}-{bo_num}" if bo_num else f"MA-BO-{lang}-{raw.get('BoId', '')}"

        title_parts = [f"Bulletin Officiel n° {bo_num}"]
        if bo_date:
            title_parts.append(f"({bo_date})")
        if lang == "fr":
            title_parts.append("[FR]")
        title = " ".join(title_parts)

        return {
            "_id": doc_id,
            "_source": "MA/SGG-BulletinOfficiel",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": bo_date,
            "url": pdf_url,
            "bulletin_number": bo_num,
            "language": lang,
        }

    def _fetch_documents(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Core fetcher: get catalog, download PDFs, extract text."""
        total = 0
        failures = 0

        # For sample mode: fetch only Arabic, pick 15 evenly-spaced issues
        langs = ["ar"] if sample else ["ar", "fr"]

        for lang in langs:
            catalog = self._fetch_catalog(lang)
            if not catalog:
                continue

            existing = preload_existing_ids("MA/SGG-BulletinOfficiel", table="legislation")

            if sample:
                # Pick 15 evenly-spaced items from the catalog
                step = max(1, len(catalog) // 15)
                items = catalog[::step][:15]
            else:
                items = catalog

            for entry in items:
                bo_num = str(entry.get("BoNum", "")).strip()
                bo_id = str(entry.get("BoId", "")).strip()
                raw_url = entry.get("BoUrl", "")
                pdf_url = _normalise_pdf_url(raw_url)

                if not pdf_url:
                    logger.warning(f"No PDF URL for BO #{bo_num}")
                    failures += 1
                    continue

                doc_id = f"MA-BO-{lang}-{bo_num}" if bo_num else f"MA-BO-{lang}-{bo_id}"

                if doc_id in existing:
                    continue

                self.rate_limiter.wait()
                text = extract_pdf_markdown(
                    source="MA/SGG-BulletinOfficiel",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )

                if not text or len(text) < 50:
                    logger.warning(f"Insufficient text for BO #{bo_num}: {len(text) if text else 0} chars")
                    failures += 1
                    continue

                raw = {
                    **entry,
                    "lang": lang,
                    "pdf_url": pdf_url,
                    "text": text,
                }
                total += 1
                logger.info(f"[{total}] BO #{bo_num} ({lang}): {len(text)} chars")
                yield raw

                if sample and total >= 15:
                    break

            if sample and total >= 15:
                break

        logger.info(f"TOTAL: {total} records, {failures} failures")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def test_api(self):
        """Quick connectivity and extraction test."""
        logger.info("Testing sgg.gov.ma API...")

        catalog = self._fetch_catalog("ar")
        if not catalog:
            logger.error("API returned no records")
            return

        logger.info(f"API OK: {len(catalog)} Arabic bulletin records")

        # Test first entry PDF
        entry = catalog[0]
        pdf_url = _normalise_pdf_url(entry.get("BoUrl", ""))
        if pdf_url:
            logger.info(f"Testing PDF: {pdf_url}")
            text = extract_pdf_markdown(
                source="MA/SGG-BulletinOfficiel",
                source_id="test",
                pdf_url=pdf_url,
                table="legislation",
                force=True,
            )
            if text:
                logger.info(f"PDF extraction OK: {len(text)} chars")
                logger.info(f"Preview: {text[:200]}...")
            else:
                logger.error("PDF extraction returned empty")
        else:
            logger.error("No PDF URL in first entry")


def main():
    scraper = MoroccoBulletinOfficielScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "update"):
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
