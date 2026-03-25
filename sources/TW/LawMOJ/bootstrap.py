#!/usr/bin/env python3
"""
TW/LawMOJ -- Taiwan Laws & Regulations Database (Ministry of Justice)

Fetches Taiwanese legislation from the MOJ official API or GitHub mirror.

Strategy:
  - Primary: Bulk JSON download from law.moj.gov.tw/api/{lang}/law/json (ZIP)
  - Fallback: Individual JSON files from kong0107/mojLawSplitJSON on GitHub
  - Both provide full-text article content in structured JSON

Data: 11,752 laws (Chinese), ~1,200 with English translations.
License: Government Open Data License (Taiwan).

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
"""

import sys
import json
import logging
import time
import io
import zipfile
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TW.LawMOJ")

# Official MOJ API
MOJ_API_BASE = "https://law.moj.gov.tw/api"

# GitHub mirror (fallback)
GITHUB_BASE = "https://raw.githubusercontent.com/kong0107/mojLawSplitJSON/master"

# Max concurrent downloads for GitHub mirror fallback
MAX_WORKERS = 8


class TaiwanLawMOJScraper(BaseScraper):
    """
    Scraper for TW/LawMOJ -- Taiwan Laws & Regulations Database.
    Country: TW
    URL: https://law.moj.gov.tw

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=30,
        )
        # Separate client for MOJ with short timeout (often unreachable)
        self.moj_client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=15,
            max_retries=0,
        )
        self._en_index = {}  # PCode -> English law data (loaded lazily)

    # -- Data access methods ------------------------------------------------

    def _try_moj_bulk(self, endpoint: str) -> Optional[list]:
        """Try downloading bulk JSON ZIP from official MOJ API."""
        url = f"{MOJ_API_BASE}{endpoint}"
        logger.info(f"Trying MOJ bulk API: {url}")
        try:
            import requests as req
            # Stream download with generous timeout for large ZIPs (25MB+)
            resp = req.get(
                url,
                headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
                timeout=(15, 120),  # 15s connect, 120s read
                stream=True,
            )
            if resp.status_code != 200:
                return None
            # Read full content in chunks to avoid IncompleteRead
            chunks = []
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    chunks.append(chunk)
            data = b"".join(chunks)
            resp.close()
            logger.info(f"Downloaded {len(data)} bytes from MOJ API")
            if data[:2] == b"PK":  # ZIP file
                zf = zipfile.ZipFile(io.BytesIO(data))
                content = zf.read(zf.namelist()[0])
                parsed = json.loads(content)
            else:
                parsed = json.loads(data)
            # API may return a dict wrapper (e.g. {"Laws": [...]}) or a list
            if isinstance(parsed, dict):
                # Find the first list value in the dict
                for v in parsed.values():
                    if isinstance(v, list):
                        return v
                logger.warning(f"MOJ API returned dict with no list values: {list(parsed.keys())}")
                return None
            if isinstance(parsed, list):
                return parsed
            return None
        except Exception as e:
            logger.warning(f"MOJ bulk API failed: {e}")
            return None

    def _fetch_github_index(self) -> list:
        """Fetch the law index from GitHub mirror."""
        url = f"{GITHUB_BASE}/index.json"
        logger.info(f"Fetching GitHub mirror index: {url}")
        resp = self.client.get(url, timeout=60)
        if resp is None or resp.status_code != 200:
            raise RuntimeError("Failed to fetch GitHub index")
        return resp.json()

    def _fetch_github_law(self, pcode: str, english: bool = False) -> Optional[dict]:
        """Fetch a single law from GitHub mirror."""
        prefix = "Eng_FalVMingLing" if english else "FalVMingLing"
        url = f"{GITHUB_BASE}/{prefix}/{pcode}.json"
        try:
            resp = self.client.get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                return None
            return resp.json()
        except Exception:
            return None

    def _load_english_index(self):
        """Load English translations index for merging."""
        if self._en_index:
            return
        logger.info("Loading English translations index...")
        index = self._fetch_github_index()
        for entry in index:
            if entry.get("english"):
                self._en_index[entry["PCode"]] = entry["english"]
        logger.info(f"Found {len(self._en_index)} laws with English translations")

    # -- Article text extraction -------------------------------------------

    @staticmethod
    def _extract_articles_text(articles: list) -> str:
        """Extract full text from the structured articles array."""
        parts = []
        for item in articles:
            if not isinstance(item, dict):
                continue
            # Chapter/section headers
            if "編章節" in item:
                parts.append(f"\n{item['編章節']}\n")
            # Article content
            if "條號" in item and "條文內容" in item:
                text = str(item["條文內容"]).replace("\r\n", "\n").strip()
                parts.append(f"{item['條號']}\n{text}")
        return "\n\n".join(parts).strip()

    # -- Normalize ---------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw MOJ JSON into standard schema."""
        # Determine field names based on source (Chinese vs English structure)
        pcode = raw.get("PCode", "")
        if not pcode:
            # Extract from URL if not directly available
            url = raw.get("法規網址", "")
            m = re.search(r"pcode=([A-Z0-9]+)", url, re.IGNORECASE)
            if m:
                pcode = m.group(1)
            else:
                return None

        title = raw.get("法規名稱", raw.get("中文法規名稱", ""))
        title_en = raw.get("英文法規名稱", self._en_index.get(pcode, ""))
        nature = raw.get("法規性質", "")
        category = raw.get("法規類別", "")
        url = raw.get("法規網址", f"https://law.moj.gov.tw/LawClass/LawAll.aspx?pcode={pcode}")
        date_str = raw.get("最新異動日期", raw.get("lastUpdate", ""))
        history = raw.get("沿革內容", "").replace("\r\n", "\n").strip()
        preamble = raw.get("前言", "").replace("\r\n", "\n").strip()

        # Extract full article text
        articles = raw.get("法規內容", [])
        text = self._extract_articles_text(articles) if articles else ""

        if not text:
            return None

        # Parse date: YYYYMMDD -> ISO 8601
        date_iso = None
        if date_str and len(date_str) == 8:
            try:
                date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except (ValueError, IndexError):
                date_iso = None

        # Merge English text if available
        text_en = ""
        en_data = raw.get("_en_data")
        if en_data and en_data.get("法規內容"):
            text_en = self._extract_articles_text(en_data["法規內容"])

        record = {
            "_id": f"TW-{pcode}",
            "_source": "TW/LawMOJ",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "pcode": pcode,
            "title": title,
            "title_en": title_en or None,
            "text": text,
            "text_en": text_en or None,
            "date": date_iso,
            "url": url,
            "nature": nature or None,
            "category": category or None,
            "preamble": preamble or None,
            "history": history or None,
        }
        return record

    # -- Fetch methods -----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Taiwanese laws with full text.

        Strategy:
        1. Try official MOJ bulk JSON API (returns ZIP with all laws)
        2. Fall back to GitHub mirror (individual files per law)
        """
        # Try MOJ bulk API first
        laws = self._try_moj_bulk("/ch/law/json")
        if laws:
            logger.info(f"MOJ bulk API: got {len(laws)} laws")
            # Also try orders
            orders = self._try_moj_bulk("/ch/order/json")
            if orders:
                logger.info(f"MOJ bulk API: got {len(orders)} orders")
                laws = laws + orders  # use + instead of extend to avoid mutation issues
            # Add PCode from URL if not present; skip non-dict items
            for law in laws:
                if not isinstance(law, dict):
                    continue
                if "PCode" not in law:
                    url = law.get("法規網址", "")
                    m = re.search(r"pcode=([A-Z0-9]+)", url, re.IGNORECASE)
                    if m:
                        law["PCode"] = m.group(1)
                yield law
            return

        # Fallback: GitHub mirror
        logger.info("MOJ API unavailable, using GitHub mirror fallback")
        index = self._fetch_github_index()
        logger.info(f"GitHub index: {len(index)} laws")

        # Fetch individual law files concurrently in batches
        def fetch_one(entry):
            pcode = entry["PCode"]
            law = self._fetch_github_law(pcode, english=False)
            if law:
                law["PCode"] = pcode
                # Try to get English version too
                if entry.get("english"):
                    en = self._fetch_github_law(pcode, english=True)
                    if en:
                        law["_en_data"] = en
            return law

        fetched = 0
        batch_size = 100
        for batch_start in range(0, len(index), batch_size):
            batch = index[batch_start:batch_start + batch_size]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(fetch_one, entry): entry for entry in batch}
                for future in as_completed(futures):
                    try:
                        law = future.result()
                        if law:
                            fetched += 1
                            if fetched % 500 == 0:
                                logger.info(f"Fetched {fetched}/{len(index)} laws")
                            yield law
                    except Exception as e:
                        logger.warning(f"Error fetching law: {e}")
                        continue

        logger.info(f"Total fetched: {fetched}/{len(index)}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield laws updated since the given date."""
        since_str = since.strftime("%Y%m%d")
        for raw in self.fetch_all():
            date_str = raw.get("最新異動日期", raw.get("lastUpdate", ""))
            if date_str and date_str >= since_str:
                yield raw

    # -- CLI ---------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="TW/LawMOJ Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Concurrent workers")
    args = parser.parse_args()

    scraper = TaiwanLawMOJScraper()

    if args.command == "test-api":
        # Quick connectivity test
        logger.info("Testing MOJ API connectivity...")
        laws = scraper._try_moj_bulk("/ch/law/json")
        if laws:
            logger.info(f"MOJ API OK: {len(laws)} laws")
        else:
            logger.info("MOJ API unreachable, testing GitHub fallback...")
            index = scraper._fetch_github_index()
            logger.info(f"GitHub mirror OK: {len(index)} laws in index")
            law = scraper._fetch_github_law(index[0]["PCode"])
            if law:
                logger.info(f"Sample law OK: {law.get('法規名稱', 'N/A')}")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta

        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
