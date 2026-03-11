#!/usr/bin/env python3
"""
TW/MOJ -- Taiwan National Laws Database (全國法規資料庫) Data Fetcher

Fetches Taiwan legislation from the Ministry of Justice Open API.

Strategy:
  - Bootstrap: Downloads bulk JSON ZIP files containing all laws.
  - Update: Re-downloads bulk files and compares modified dates.
  - Sample: Extracts 10+ laws for validation.

API: https://law.moj.gov.tw/api/
License: Open Government Data License, version 1.0 (OGDL-Taiwan-1.0)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Re-fetch and compare
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import io
import zipfile
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict
from urllib.parse import urlparse, parse_qs

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TW.MOJ")

# API endpoints
API_BASE = "https://law.moj.gov.tw/api"
ENDPOINTS = {
    "chinese_laws": "/Ch/Law/JSON",
    "english_laws": "/En/Law/JSON",
}


class TaiwanMOJScraper(BaseScraper):
    """
    Scraper for TW/MOJ -- Taiwan National Laws Database.
    Country: TW
    URL: https://law.moj.gov.tw

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
        })
        self._chinese_laws: Optional[Dict[str, dict]] = None
        self._english_laws: Optional[Dict[str, dict]] = None

    def _download_zip(self, endpoint: str) -> dict:
        """
        Download a ZIP file from the API and extract the JSON.

        Returns the parsed JSON data.
        """
        url = f"{API_BASE}{endpoint}"
        logger.info(f"Downloading {url}...")

        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()

        # Extract from ZIP
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # Find the JSON file
            json_files = [n for n in zf.namelist() if n.endswith(".json")]
            if not json_files:
                raise ValueError(f"No JSON file found in ZIP from {endpoint}")

            json_filename = json_files[0]
            logger.info(f"Extracting {json_filename}...")

            with zf.open(json_filename) as f:
                # Handle BOM encoding
                content = f.read().decode("utf-8-sig")
                return json.loads(content)

    def _extract_pcode(self, url: str) -> str:
        """
        Extract the pcode (law identifier) from a law URL.

        Example: https://law.moj.gov.tw/LawClass/LawAll.aspx?pcode=A0000001
        Returns: A0000001
        """
        if not url:
            return ""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        pcodes = params.get("pcode") or params.get("PCode") or []
        return pcodes[0] if pcodes else ""

    def _load_laws(self):
        """Load Chinese and English law data if not already loaded."""
        if self._chinese_laws is None:
            data = self._download_zip(ENDPOINTS["chinese_laws"])
            laws = data.get("Laws", [])
            # Index by pcode for easy lookup
            self._chinese_laws = {}
            for law in laws:
                pcode = self._extract_pcode(law.get("LawURL", ""))
                if pcode:
                    self._chinese_laws[pcode] = law
            logger.info(f"Loaded {len(self._chinese_laws)} Chinese laws")

        if self._english_laws is None:
            data = self._download_zip(ENDPOINTS["english_laws"])
            laws = data.get("Laws", [])
            # English laws don't have pcode in URL, index by Chinese name
            self._english_laws = {}
            for law in laws:
                # Use Chinese law name as key to match with Chinese laws
                law_name = law.get("LawName", "")
                if law_name:
                    self._english_laws[law_name] = law
            logger.info(f"Loaded {len(self._english_laws)} English laws")

    def _extract_full_text(self, articles: list) -> str:
        """
        Extract full text from the LawArticles array.

        Handles both article content ('A') and chapter headings ('C').
        """
        if not articles:
            return ""

        text_parts = []
        for article in articles:
            article_type = article.get("ArticleType", "")
            article_no = article.get("ArticleNo", "").strip()
            content = article.get("ArticleContent", "").strip()

            if article_type == "C":
                # Chapter heading
                if content:
                    text_parts.append(f"\n{content}\n")
            elif article_type == "A":
                # Regular article
                if article_no and content:
                    text_parts.append(f"{article_no}\n{content}")
                elif content:
                    text_parts.append(content)

        full_text = "\n\n".join(text_parts)
        # Clean up whitespace
        full_text = re.sub(r"\r\n", "\n", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        return full_text.strip()

    def _parse_date(self, date_str: str) -> str:
        """
        Parse date from YYYYMMDD format to ISO 8601.

        Returns empty string if invalid.
        """
        if not date_str or len(date_str) != 8:
            return ""
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all laws from the Taiwan MOJ database.

        Downloads bulk JSON files and yields each law with merged
        Chinese and English data.
        """
        self._load_laws()

        # Yield all Chinese laws with English data merged in
        for pcode, ch_law in self._chinese_laws.items():
            # Match English law by Chinese law name
            law_name = ch_law.get("LawName", "")
            en_law = self._english_laws.get(law_name)
            yield {
                "pcode": pcode,
                "chinese": ch_law,
                "english": en_law,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield laws modified since the given date.

        Re-downloads bulk files and filters by LawModifiedDate.
        """
        # Clear cache to force re-download
        self._chinese_laws = None
        self._english_laws = None
        self._load_laws()

        since_str = since.strftime("%Y%m%d")

        for pcode, ch_law in self._chinese_laws.items():
            modified = ch_law.get("LawModifiedDate", "")
            if modified >= since_str:
                # Match English law by Chinese law name
                law_name = ch_law.get("LawName", "")
                en_law = self._english_laws.get(law_name)
                yield {
                    "pcode": pcode,
                    "chinese": ch_law,
                    "english": en_law,
                }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw law data into standard schema.

        CRITICAL: Extracts and includes FULL TEXT from article content.
        """
        pcode = raw.get("pcode", "")
        ch_law = raw.get("chinese", {})
        en_law = raw.get("english") or {}

        # Extract full text from both Chinese and English articles
        ch_articles = ch_law.get("LawArticles", [])
        en_articles = en_law.get("EngLawArticles", [])  # English uses EngLawArticles

        ch_text = self._extract_full_text(ch_articles)
        en_text = self._extract_full_text(en_articles)

        # Combine texts with clear separation
        if ch_text and en_text:
            full_text = f"【中文】\n\n{ch_text}\n\n【English】\n\n{en_text}"
        elif ch_text:
            full_text = ch_text
        elif en_text:
            full_text = en_text
        else:
            full_text = ""

        # Parse dates
        modified_date = self._parse_date(ch_law.get("LawModifiedDate", ""))
        effective_date = self._parse_date(ch_law.get("LawEffectiveDate", ""))

        # Build URL
        url = ch_law.get("LawURL", f"https://law.moj.gov.tw/LawClass/LawAll.aspx?pcode={pcode}")

        return {
            # Required base fields
            "_id": pcode,
            "_source": "TW/MOJ",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": ch_law.get("LawName", ""),
            "title_en": en_law.get("LawName") or ch_law.get("EngLawName", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": modified_date,
            "url": url,
            # Source-specific fields
            "pcode": pcode,
            "law_level": ch_law.get("LawLevel", ""),
            "law_category": ch_law.get("LawCategory", ""),
            "effective_date": effective_date,
            "effective_note": ch_law.get("LawEffectiveNote", ""),
            "abandon_note": ch_law.get("LawAbandonNote", ""),
            "has_english": ch_law.get("LawHasEngVersion", "") == "Y",
            "foreword": ch_law.get("LawForeword", ""),
            "histories": ch_law.get("LawHistories", ""),
            "articles": ch_articles,  # Keep structured articles for reference
            "attachments": ch_law.get("LawAttachements", []),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API version test."""
        print("Testing Taiwan MOJ API...")

        for name, endpoint in ENDPOINTS.items():
            url = f"{API_BASE}{endpoint}"
            print(f"\nChecking {name}: {url}")

            try:
                # Just check headers to verify endpoint works
                resp = self.session.head(url, timeout=30)
                print(f"  Status: {resp.status_code}")
                print(f"  Content-Type: {resp.headers.get('Content-Type', 'N/A')}")

                # Actually download to get stats
                data = self._download_zip(endpoint)
                laws = data.get("Laws", [])
                print(f"  Laws count: {len(laws)}")
                if laws:
                    first = laws[0]
                    print(f"  First law: {first.get('LawName', 'N/A')[:50]}")
                    articles = first.get("LawArticles", [])
                    print(f"  Articles: {len(articles)}")

            except Exception as e:
                print(f"  Error: {e}")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = TaiwanMOJScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
