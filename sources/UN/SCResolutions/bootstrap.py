#!/usr/bin/env python3
"""
UN/SCResolutions -- Corpus of UN Security Council Resolutions (Zenodo/Fobbe)

Fetches all 2,798 UNSC resolutions (1946-2025) with full text from Zenodo.

Strategy:
  - Download metadata CSV (1.3 MB) for structured fields (title, date, symbol, etc.)
  - Download English TXT ZIP (8.1 MB) for pre-extracted full text
  - Join metadata + text by doc_id filename
  - Yield normalized records

Data: 2,798 resolutions, CC-BY-4.0 license.
Rate limit: minimal (2 bulk downloads only).

Usage:
  python bootstrap.py bootstrap            # Full pull (all 2,798 resolutions)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import csv
import json
import logging
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UN.SCResolutions")

ZENODO_RECORD = "15154519"
ZENODO_API = f"https://zenodo.org/api/records/{ZENODO_RECORD}"
META_FILE = "CR-UNSC_2025-12-22_ALL_CSV_META.zip"
TEXT_FILE = "CR-UNSC_2025-12-22_EN_TXT_BEST.zip"


class SCResolutionsScraper(BaseScraper):
    """
    Scraper for UN/SCResolutions -- Zenodo corpus of UNSC resolutions.
    Country: UN
    URL: https://zenodo.org/records/15154519

    Data types: legislation
    Auth: none (CC-BY-4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "*/*",
            },
            timeout=120,
        )
        self._metadata = None
        self._texts = None

    # -- Data loading -------------------------------------------------------

    def _load_metadata(self) -> dict:
        """Download and parse the metadata CSV. Returns {doc_id: row_dict}."""
        if self._metadata is not None:
            return self._metadata

        url = f"{ZENODO_API}/files/{META_FILE}/content"
        logger.info(f"Downloading metadata CSV from Zenodo...")
        resp = self.client.get(url, timeout=120)
        if resp is None or resp.status_code != 200:
            raise RuntimeError(f"Failed to download metadata: {resp.status_code if resp else 'None'}")

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]

        meta = {}
        with z.open(csv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                doc_id = row.get("doc_id", "")
                if doc_id:
                    meta[doc_id] = row

        logger.info(f"Loaded metadata for {len(meta)} documents")
        self._metadata = meta
        return meta

    def _load_texts(self) -> dict:
        """Download and extract TXT files. Returns {doc_id: text}."""
        if self._texts is not None:
            return self._texts

        url = f"{ZENODO_API}/files/{TEXT_FILE}/content"
        logger.info(f"Downloading English TXT ZIP from Zenodo (~8 MB)...")
        resp = self.client.get(url, timeout=180)
        if resp is None or resp.status_code != 200:
            raise RuntimeError(f"Failed to download texts: {resp.status_code if resp else 'None'}")

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        texts = {}
        for name in z.namelist():
            if name.endswith(".txt"):
                with z.open(name) as f:
                    text = f.read().decode("utf-8", errors="replace").strip()
                    if text:
                        texts[name] = text

        logger.info(f"Loaded {len(texts)} text files")
        self._texts = texts
        return texts

    # -- Normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("_text", "")
        if not text or len(text) < 20:
            return None

        res_no = raw.get("res_no", "")
        year = raw.get("year", "")
        symbol = raw.get("symbol", "")
        if not symbol and res_no and year:
            symbol = f"S/RES/{res_no}({year})"

        doc_id = f"UN-SCRES-{res_no}" if res_no else raw.get("doc_id", "unknown")

        return {
            "_id": doc_id,
            "_source": "UN/SCResolutions",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "res_no": int(res_no) if res_no else None,
            "symbol": symbol,
            "title": raw.get("title", f"Security Council Resolution {res_no}"),
            "text": text,
            "date": raw.get("date", "") or None,
            "year": int(year) if year else None,
            "url": f"https://digitallibrary.un.org/record/{raw.get('undl_id', '')}",
            "body": "Security Council",
            "doc_type": "resolution",
        }

    # -- Fetch methods ------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all UNSC resolutions with full text."""
        metadata = self._load_metadata()
        texts = self._load_texts()

        total = 0
        skipped_no_text = 0

        # Join metadata with text files by doc_id
        for doc_id, meta_row in sorted(metadata.items()):
            text = texts.get(doc_id, "")
            if not text:
                # Try matching without GOLD suffix
                alt_id = doc_id.replace("_GOLD", "")
                text = texts.get(alt_id, "")
            if not text:
                # Try with GOLD suffix
                alt_id = doc_id.replace(".txt", "_GOLD.txt") if "_GOLD" not in doc_id else doc_id
                text = texts.get(alt_id, "")

            if not text:
                skipped_no_text += 1
                continue

            total += 1
            yield {
                "doc_id": doc_id,
                "res_no": meta_row.get("res_no", ""),
                "symbol": meta_row.get("symbol", ""),
                "title": meta_row.get("title", ""),
                "date": meta_row.get("date", ""),
                "year": meta_row.get("year", ""),
                "undl_id": meta_row.get("undl_id", ""),
                "_text": text,
            }

        logger.info(f"Total: {total} resolutions with text, {skipped_no_text} skipped (no text)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield resolutions from date range (limited for Zenodo bulk data)."""
        since_str = since.strftime("%Y-%m-%d")
        for raw in self.fetch_all():
            if raw.get("date", "") >= since_str:
                yield raw

    # -- CLI ----------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UN/SCResolutions Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCResolutionsScraper()

    if args.command == "test-api":
        logger.info("Testing Zenodo API access...")
        import requests
        resp = requests.get(f"https://zenodo.org/api/records/{ZENODO_RECORD}", timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            files = data.get("files", [])
            logger.info(f"Record OK: {len(files)} files available")
            for f in files[:5]:
                logger.info(f"  {f['key']}: {f['size']/(1024*1024):.1f} MB")
        else:
            logger.error(f"Failed: HTTP {resp.status_code}")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=365)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
