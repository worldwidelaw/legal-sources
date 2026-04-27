#!/usr/bin/env python3
"""
JP/NTTR -- Japan National Tax Tribunal (国税不服審判所) Published Rulings

Fetches full-text tax tribunal rulings from kfs.go.jp.

Strategy:
  - GET the main index at /service/JP/index.html to enumerate volumes (43-140+)
  - For each volume, GET /service/JP/idx/{vol}.html to list cases
  - For each case, GET /service/JP/{vol}/{case}/index.html for full text
  - Pages are Shift-JIS encoded; convert to UTF-8
  - Extract text from HTML, strip navigation/boilerplate

Data:
  - ~2000 rulings across 98+ volumes (1992-2025)
  - Tax disputes: income tax, corporate tax, consumption tax, inheritance,
    national tax procedural law, etc.
  - Full text includes: facts, disputed issues, arguments, tribunal reasoning
  - Japanese language

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JP.NTTR")

BASE_URL = "https://www.kfs.go.jp"

# Japanese era year conversion helpers
ERA_OFFSETS = {
    "令和": 2018,   # Reiwa: year 1 = 2019
    "平成": 1988,   # Heisei: year 1 = 1989
    "昭和": 1925,   # Showa: year 1 = 1926
}


def japanese_date_to_iso(text: str) -> Optional[str]:
    """Convert Japanese era date to ISO format.

    Handles both formats:
      - Full: '令和７年９月26日'
      - Short: '平4.2.24' or '令7.9.26'
    """
    text = text.replace("\u3000", " ").strip()
    # Normalize full-width digits to ASCII
    trans = str.maketrans("０１２３４５６７８９", "0123456789")
    text = text.translate(trans)

    # Try full format: 令和7年9月26日
    m = re.search(r"(令和|平成|昭和)(\d+)年(\d+)月(\d+)日", text)
    if m:
        era, year, month, day = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
        western_year = ERA_OFFSETS.get(era, 0) + year
        try:
            return f"{western_year:04d}-{month:02d}-{day:02d}"
        except (ValueError, OverflowError):
            return None

    # Try short format: 平4.2.24 or 令7.9.26
    ERA_SHORT = {"令": "令和", "平": "平成", "昭": "昭和"}
    m = re.search(r"(令|平|昭)(\d+)\.(\d+)\.(\d+)", text)
    if m:
        era_short, year, month, day = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
        era = ERA_SHORT.get(era_short, "")
        western_year = ERA_OFFSETS.get(era, 0) + year
        try:
            return f"{western_year:04d}-{month:02d}-{day:02d}"
        except (ValueError, OverflowError):
            return None

    return None


class NTTRScraper(BaseScraper):
    """Scraper for JP/NTTR -- Japan National Tax Tribunal Rulings."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _fetch_page(self, url: str) -> str:
        """Fetch a page and decode from Shift-JIS to UTF-8."""
        time.sleep(self.config.get("fetch", {}).get("rate_limit", 2.0))
        resp = self.session.get(url, timeout=self.config.get("fetch", {}).get("timeout", 30))
        resp.raise_for_status()
        resp.encoding = "shift_jis"
        return resp.text

    def _get_volumes(self) -> List[int]:
        """Get list of all volume numbers from the main index."""
        url = f"{BASE_URL}/service/JP/index.html"
        html = self._fetch_page(url)
        # Find volume links like idx/43.html through idx/140.html
        volumes = sorted(set(int(m) for m in re.findall(r'idx/(\d+)\.html', html)))
        logger.info(f"Found {len(volumes)} volumes: {volumes[0]}-{volumes[-1]}")
        return volumes

    def _get_cases_in_volume(self, vol: int) -> List[Tuple[int, str]]:
        """Get list of (case_number, date_text) for a volume."""
        url = f"{BASE_URL}/service/JP/idx/{vol}.html"
        html = self._fetch_page(url)
        # Extract case links: ../140/01/index.html
        case_nums = sorted(set(
            int(m) for m in re.findall(rf'\.\./{ vol}/(\d+)/index\.html', html)
        ))
        # Extract dates from title attributes
        dates = re.findall(r'title="([^"]*裁決事例)"', html)
        # Map dates to cases (each case has a summary link and a case link)
        date_map = {}
        date_matches = re.findall(
            rf'<a href="\.\./{ vol}/(\d+)/index\.html"\s+title="([^"]*)"',
            html
        )
        for case_str, title in date_matches:
            date_map[int(case_str)] = title

        result = []
        for cn in case_nums:
            date_text = date_map.get(cn, "")
            result.append((cn, date_text))
        return result

    def _extract_ruling_text(self, html: str) -> str:
        """Extract the main ruling text from the HTML page."""
        soup = BeautifulSoup(html, "html.parser")

        # Remove script, style, navigation elements
        for tag in soup.find_all(["script", "style", "noscript"]):
            tag.decompose()

        # Find the main content div
        main = soup.find("div", id="main")
        if not main:
            main = soup.find("div", id="contents")
        if not main:
            main = soup.body

        if not main:
            return ""

        # Get text content
        text = main.get_text(separator="\n")

        # Clean up the text
        lines = []
        skip_patterns = [
            "本文へジャンプ", "サイト内検索", "検索の仕方", "利用案内",
            "サイトマップ", "関連リンク", "ホーム", "公表裁決事例集等の紹介",
            "公表裁決事例", "トップに戻る", "審判所の概要", "審査請求の状況",
            "実績の評価", "パンフレット等", "不服申立手続等", "制度の概要図",
            "不服申立ての対象等", "再調査の請求との関係", "審査請求書の提出",
            "代理人と総代", "審理と裁決", "提出書類一覧", "提出先一覧",
            "Q&Aコーナー", "裁決要旨の検索", "調達情報", "情報公開",
            "個人情報保護", "Copyright", "National Tax Tribunal",
        ]

        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if any(pat in line for pat in skip_patterns):
                continue
            # Skip breadcrumb-like short lines and volume headers
            if line in (">>", ">", ">>"):
                continue
            if re.match(r'^裁決事例集\s*No\.\d+$', line):
                continue
            lines.append(line)

        result = "\n".join(lines)
        # Remove leading >> breadcrumb artifacts
        result = re.sub(r'^>>\s*\n?', '', result)
        return result.strip()

    def _extract_title(self, html: str) -> str:
        """Extract the ruling title from the page."""
        soup = BeautifulSoup(html, "html.parser")
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)
        title_tag = soup.find("title")
        if title_tag:
            t = title_tag.get_text(strip=True)
            # Remove " | 公表裁決事例等の紹介 | 国税不服審判所" suffix
            t = re.sub(r'\s*\|.*$', '', t)
            return t
        return ""

    def _extract_tax_category(self, vol: int, html_index: str, case_num: int) -> str:
        """Extract the tax law category for a case from the volume index page."""
        # This is best-effort; the category comes from the h2/h3 sections
        # in the volume index page preceding the case link
        soup = BeautifulSoup(html_index, "html.parser")
        current_category = ""
        for elem in soup.find_all(["h2", "a"]):
            if elem.name == "h2":
                span = elem.find("span")
                if span:
                    current_category = span.get_text(strip=True)
            elif elem.name == "a":
                href = elem.get("href", "")
                if f"../{vol}/{case_num:02d}/index.html" in href:
                    return current_category
        return current_category

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all rulings from all volumes."""
        volumes = self._get_volumes()
        for vol in volumes:
            logger.info(f"Processing volume {vol}")
            try:
                # Fetch volume index page (needed for category extraction)
                vol_url = f"{BASE_URL}/service/JP/idx/{vol}.html"
                vol_html = self._fetch_page(vol_url)

                cases = self._get_cases_in_volume(vol)
                logger.info(f"  Volume {vol}: {len(cases)} cases")

                for case_num, date_text in cases:
                    case_url = f"{BASE_URL}/service/JP/{vol}/{case_num:02d}/index.html"
                    try:
                        case_html = self._fetch_page(case_url)
                        text = self._extract_ruling_text(case_html)
                        title = self._extract_title(case_html)
                        # Try date from index page, then from title, then from text
                        date_iso = (japanese_date_to_iso(date_text)
                                    or japanese_date_to_iso(title)
                                    or japanese_date_to_iso(text[:200]))
                        category = self._extract_tax_category(vol, vol_html, case_num)

                        if not text or len(text) < 100:
                            logger.warning(f"  Skipping vol {vol} case {case_num}: insufficient text ({len(text)} chars)")
                            continue

                        yield {
                            "_vol": vol,
                            "_case": case_num,
                            "_url": case_url,
                            "_date_text": date_text,
                            "_date_iso": date_iso,
                            "_title": title,
                            "_category": category,
                            "_text": text,
                        }
                    except requests.RequestException as e:
                        logger.error(f"  Error fetching vol {vol} case {case_num}: {e}")
                        continue
            except requests.RequestException as e:
                logger.error(f"Error fetching volume {vol} index: {e}")
                continue

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch only the latest volume(s)."""
        volumes = self._get_volumes()
        # Fetch just the last 2 volumes for updates
        for vol in volumes[-2:]:
            vol_url = f"{BASE_URL}/service/JP/idx/{vol}.html"
            vol_html = self._fetch_page(vol_url)
            cases = self._get_cases_in_volume(vol)
            for case_num, date_text in cases:
                case_url = f"{BASE_URL}/service/JP/{vol}/{case_num:02d}/index.html"
                try:
                    case_html = self._fetch_page(case_url)
                    text = self._extract_ruling_text(case_html)
                    title = self._extract_title(case_html)
                    date_iso = japanese_date_to_iso(date_text) or japanese_date_to_iso(title)
                    category = self._extract_tax_category(vol, vol_html, case_num)
                    if text and len(text) >= 100:
                        yield {
                            "_vol": vol,
                            "_case": case_num,
                            "_url": case_url,
                            "_date_text": date_text,
                            "_date_iso": date_iso,
                            "_title": title,
                            "_category": category,
                            "_text": text,
                        }
                except requests.RequestException as e:
                    logger.error(f"Error fetching vol {vol} case {case_num}: {e}")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw ruling data into standard schema."""
        vol = raw["_vol"]
        case = raw["_case"]
        return {
            "_id": f"JP-NTTR-{vol:03d}-{case:02d}",
            "_source": "JP/NTTR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["_title"],
            "text": raw["_text"],
            "date": raw.get("_date_iso"),
            "url": raw["_url"],
            "volume": vol,
            "case_number": case,
            "tax_category": raw.get("_category", ""),
            "language": "ja",
            "court": "国税不服審判所 (National Tax Tribunal)",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="JP/NTTR Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NTTRScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
