#!/usr/bin/env python3
"""
JP/JLTrans -- Japanese Law Translation (MOJ) Data Fetcher

Fetches official English translations of Japanese laws from the Ministry
of Justice Japanese Law Translation Database System.

Strategy:
  - Iterate through law IDs (1-5200), fetch each law page
  - Extract title, law number, and TXT download link from HTML
  - Download TXT file for bilingual full text (Japanese/English)

API: No official API. Uses HTML scraping + TXT file downloads.
URL pattern: https://www.japaneselawtranslation.go.jp/en/laws/view/{ID}
TXT download: /en/laws/download/{ID}/13/{filename}.txt

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~5,000 laws)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap (no date filter)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
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
logger = logging.getLogger("legal-data-hunter.JP.JLTrans")

BASE_URL = "https://www.japaneselawtranslation.go.jp"
MAX_ID = 5200


class JLTransScraper(BaseScraper):
    """
    Scraper for JP/JLTrans -- Japanese Law Translation (MOJ).
    Country: JP
    URL: https://www.japaneselawtranslation.go.jp/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )

    def _fetch_law_page(self, law_id: int) -> Optional[dict]:
        """Fetch a single law page and extract metadata + TXT download link."""
        url = f"{BASE_URL}/en/laws/view/{law_id}"
        self.rate_limiter.wait()

        try:
            resp = self.client.get(url, allow_redirects=True)
        except Exception as e:
            logger.debug(f"  ID {law_id}: request error: {e}")
            return None

        if resp.status_code == 404:
            return None
        resp.raise_for_status()

        html = resp.text

        # Resolve redirects: get the actual ID from the final URL
        actual_id = law_id
        final_url = str(resp.url) if hasattr(resp, 'url') else url
        id_match = re.search(r'/laws/view/(\d+)', final_url)
        if id_match:
            actual_id = int(id_match.group(1))

        # Extract title from <title> tag: "Law Title - Japanese/English - Japanese Law Translation"
        title_match = re.search(r'<title>([^<]+)</title>', html)
        title = ""
        if title_match:
            raw_title = title_match.group(1).strip()
            # Remove suffix
            title = re.sub(r'\s*-\s*Japanese(/English)?\s*-\s*Japanese Law Translation.*$', '', raw_title).strip()

        if not title:
            # Try LawTitle_text class
            lt_match = re.search(r'class="LawTitle_text"[^>]*>([^<]+)', html)
            if lt_match:
                title = lt_match.group(1).strip()

        # Extract law number from LawNum class
        law_num = ""
        num_match = re.search(r'class="LawNum"[^>]*>([^<]+)', html)
        if num_match:
            law_num = num_match.group(1).strip()

        # Extract TXT download link
        txt_link = None
        txt_match = re.search(r'href="(/en/laws/download/[^"]+\.txt)"', html)
        if txt_match:
            txt_link = txt_match.group(1)

        return {
            "law_id": actual_id,
            "original_id": law_id,
            "title": title,
            "law_number": law_num,
            "txt_link": txt_link,
            "url": f"{BASE_URL}/en/laws/view/{actual_id}",
        }

    def _download_txt(self, txt_link: str) -> str:
        """Download the TXT file and return its content."""
        url = f"{BASE_URL}{txt_link}"
        self.rate_limiter.wait()

        resp = self.client.get(url)
        resp.raise_for_status()

        # TXT files are UTF-8 with BOM
        text = resp.text
        if text.startswith('\ufeff'):
            text = text[1:]
        return text.strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all laws with full text."""
        seen_ids = set()
        not_found_streak = 0

        for law_id in range(1, MAX_ID + 1):
            try:
                info = self._fetch_law_page(law_id)
            except Exception as e:
                logger.warning(f"  ID {law_id}: error: {e}")
                not_found_streak += 1
                if not_found_streak > 100:
                    logger.info(f"  100+ consecutive failures, stopping at ID {law_id}")
                    break
                continue

            if info is None:
                not_found_streak += 1
                if not_found_streak > 100:
                    logger.info(f"  100+ consecutive 404s, stopping at ID {law_id}")
                    break
                continue

            not_found_streak = 0
            actual_id = info["law_id"]

            # Skip duplicates from redirects
            if actual_id in seen_ids:
                continue
            seen_ids.add(actual_id)

            # Download full text
            if info["txt_link"]:
                try:
                    info["full_text"] = self._download_txt(info["txt_link"])
                except Exception as e:
                    logger.warning(f"  ID {actual_id}: TXT download failed: {e}")
                    info["full_text"] = ""
            else:
                logger.warning(f"  ID {actual_id}: no TXT download link found")
                info["full_text"] = ""

            yield info

            if law_id % 100 == 0:
                logger.info(f"  Processed ID {law_id}/{MAX_ID}, {len(seen_ids)} unique laws found")

        logger.info(f"Done: {len(seen_ids)} unique laws fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No date filter available — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw JLT data into standard schema."""
        law_id = raw.get("law_id", "")
        title = raw.get("title", "")
        law_num = raw.get("law_number", "")
        full_text = raw.get("full_text", "")

        # Parse date from law number (e.g., "Act No. 20 of March 28, 1947")
        date_iso = self._parse_law_date(law_num)

        return {
            "_id": f"JP-JLT-{law_id}",
            "_source": "JP/JLTrans",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": raw.get("url", f"{BASE_URL}/en/laws/view/{law_id}"),
            "law_number": law_num,
        }

    # Japanese era base years
    _ERA_BASE = {
        "明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018,
    }
    # Kanji digit lookup
    _KANJI_DIGITS = {
        "〇": 0, "一": 1, "二": 2, "三": 3, "四": 4,
        "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
    }

    def _kanji_to_int(self, s: str) -> Optional[int]:
        """Convert kanji number string to int (e.g., 二十二 -> 22, 元 -> 1)."""
        if not s:
            return None
        if s == "元":
            return 1
        result = 0
        current = 0
        for ch in s:
            if ch == "十":
                result += (current if current else 1) * 10
                current = 0
            elif ch == "百":
                result += (current if current else 1) * 100
                current = 0
            elif ch in self._KANJI_DIGITS:
                current = self._KANJI_DIGITS[ch]
            else:
                return None
        return result + current

    def _parse_law_date(self, law_num: str) -> Optional[str]:
        """Extract date from law number string (Japanese era or English format)."""
        if not law_num:
            return None

        # Try Japanese era format: 昭和二十二年三月二十八日
        era_pat = r'(明治|大正|昭和|平成|令和)(元|[〇一二三四五六七八九十百]+)年(?:(元|[〇一二三四五六七八九十百]+)月(?:(元|[〇一二三四五六七八九十百]+)日)?)?'
        era_match = re.search(era_pat, law_num)
        if era_match:
            era_name = era_match.group(1)
            era_year = self._kanji_to_int(era_match.group(2))
            if era_name in self._ERA_BASE and era_year is not None:
                western_year = self._ERA_BASE[era_name] + era_year
                month = self._kanji_to_int(era_match.group(3)) if era_match.group(3) else None
                day = self._kanji_to_int(era_match.group(4)) if era_match.group(4) else None
                if month and day:
                    return f"{western_year:04d}-{month:02d}-{day:02d}"
                elif month:
                    return f"{western_year:04d}-{month:02d}-01"
                else:
                    return f"{western_year:04d}-01-01"

        # Try English full date: "Month DD, YYYY"
        date_match = re.search(
            r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
            law_num
        )
        if date_match:
            try:
                return datetime.strptime(
                    date_match.group(0).replace(',', ''), "%B %d %Y"
                ).strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try just year
        year_match = re.search(r'\b(1[89]\d{2}|20[0-2]\d)\b', law_num)
        if year_match:
            return f"{year_match.group(1)}-01-01"

        return None


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = JLTransScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing JLT connectivity...")
        try:
            info = scraper._fetch_law_page(5111)
            if info:
                print(f"Title: {info['title']}")
                print(f"Law number: {info['law_number']}")
                print(f"TXT link: {info['txt_link']}")
                if info['txt_link']:
                    text = scraper._download_txt(info['txt_link'])
                    print(f"Text length: {len(text)} chars")
                    print(f"First 200 chars: {text[:200]}")
                print("Test passed!")
            else:
                print("Test failed: could not fetch law page")
        except Exception as e:
            print(f"Test failed: {e}")
            import traceback
            traceback.print_exc()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
