#!/usr/bin/env python3
"""
JP/NTA-AdvanceRulings -- Japan NTA Written Response Examples (文書回答事例)

Fetches ~700 advance ruling documents from the NTA website. Each ruling
includes a formal inquiry, official NTA response, and detailed appendix
analysis. Documents are organised by tax category across both central NTA
pages (/law/bunshokaito/) and regional office pages (/about/organization/).

Pages are Shift-JIS encoded HTML on nta.go.jp.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JP.NTA-AdvanceRulings")

BASE_URL = "https://www.nta.go.jp"
DELAY = 2.0

# Tax categories: key -> (index page number, display name)
CATEGORIES = {
    "shotoku":    ("02", "Income Tax (所得税)"),
    "gensen":     ("03", "Withholding Tax (源泉所得税)"),
    "joto-sanrin":("04", "Capital Gains Tax (譲渡所得・山林所得)"),
    "sozoku":     ("05", "Inheritance Tax (相続税)"),
    "zoyo":       ("06", "Gift Tax (贈与税)"),
    "hyoka":      ("07", "Property Valuation (財産評価)"),
    "hojin":      ("08", "Corporate Tax (法人税)"),
    "shohi":      ("09", "Consumption Tax (消費税)"),
    "shozei":     ("10", "Other Taxes (諸税)"),
}

# Navigation pages to skip (not individual rulings)
NAV_PAGES = {"/law/bunshokaito/01.htm"}
for cat, (num, _) in CATEGORIES.items():
    NAV_PAGES.add(f"/law/bunshokaito/{cat}/{num}.htm")
    NAV_PAGES.add(f"/law/bunshokaito/{cat}/{num}_1.htm")


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html,
                  flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr|dt|dd)[^>]*/?>', '\n', text,
                  flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _category_from_path(path: str) -> Optional[str]:
    """Determine the tax category from the URL path."""
    for cat in CATEGORIES:
        if f"/{cat}/" in path:
            return cat
    return None


class NTAAdvanceRulings(BaseScraper):
    SOURCE_ID = "JP/NTA-AdvanceRulings"

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
            "Accept": "text/html",
            "Accept-Language": "ja",
        })

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page and decode from Shift-JIS."""
        try:
            resp = self.session.get(url, timeout=30)
            time.sleep(DELAY)
            if resp.status_code != 200:
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
            try:
                return resp.content.decode('cp932')
            except UnicodeDecodeError:
                return resp.content.decode('utf-8', errors='replace')
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

    def _discover_all_links(self) -> List[tuple]:
        """Discover all individual ruling page URLs from all category index pages.

        Returns list of (path, category) tuples, deduplicated.
        """
        seen = set()
        results = []

        for cat_key, (page_num, cat_name) in CATEGORIES.items():
            # Each category has a main page and optionally a _1 continuation page
            for suffix in [f"{page_num}.htm", f"{page_num}_1.htm"]:
                index_url = f"{BASE_URL}/law/bunshokaito/{cat_key}/{suffix}"
                html = self._fetch_page(index_url)
                if not html:
                    continue

                # Find all links containing "bunshokaito"
                links = re.findall(r'href="([^"]+bunshokaito[^"]+\.htm(?:#[^"]*)?)"', html)
                for raw_link in links:
                    path = raw_link.split('#')[0]
                    if path in seen or path in NAV_PAGES:
                        continue
                    seen.add(path)

                    # Determine category from URL path
                    link_cat = _category_from_path(path) or cat_key
                    results.append((path, link_cat))

            logger.info("Category %s: %d unique links so far",
                        cat_name, len(results))

        logger.info("Total unique ruling links discovered: %d", len(results))
        return results

    def _find_subpages(self, html: str, base_path: str) -> List[str]:
        """Find sub-pages (besshi, appendices) linked from the main page."""
        base_dir = base_path.rsplit('/', 1)[0] + '/'
        links = re.findall(r'href="([^"]+\.htm(?:#[^"]*)?)"', html)
        subpages = set()
        for link in links:
            clean = link.split('#')[0]
            if not clean:
                continue
            # Resolve relative links
            if not clean.startswith('/'):
                full = base_dir + clean
            else:
                full = clean
            # Only pages in the same directory, not the entry page itself
            if full.startswith(base_dir) and full != base_path:
                # Skip if it's the same file
                fname = full.rsplit('/', 1)[-1]
                if fname != base_path.rsplit('/', 1)[-1]:
                    subpages.add(full)
        return sorted(subpages)

    def _extract_body_text(self, html: str) -> str:
        """Extract main body text from a page."""
        # Try bodyArea div first
        body = re.search(
            r'id="bodyArea">(.*?)(?:</div>\s*</div>|<p\s+class="page-top)',
            html, re.DOTALL
        )
        if body:
            return strip_html(body.group(1))

        # Fallback: content between breadcrumb and right-menu/footer
        body = re.search(
            r'</ol>\s*(.*?)<(?:div\s+class="(?:right-|footer)|p\s+class="page-top)',
            html, re.DOTALL
        )
        if body:
            return strip_html(body.group(1))

        # Last fallback: whole body
        body = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL)
        return strip_html(body.group(1)) if body else ""

    def _clean_text(self, text: str) -> str:
        """Remove navigation/footer noise from extracted text."""
        text = re.sub(r'このページの先頭へ.*$', '', text, flags=re.DOTALL).strip()
        text = re.sub(r'サイトマップ.*$', '', text, flags=re.DOTALL).strip()
        text = re.sub(r'法令等\s*税法.*$', '', text, flags=re.DOTALL).strip()
        # Remove breadcrumb noise at start
        text = re.sub(r'^.*?(?=取引等に係る|〔照会〕|照会者|照会の内容|別紙|事前照会の趣旨)',
                       '', text, count=1, flags=re.DOTALL)
        return text.strip()

    def _parse_ruling(self, path: str, category: str) -> Optional[Dict[str, Any]]:
        """Parse a ruling: main page + appendix sub-pages."""
        url = f"{BASE_URL}{path}"
        html = self._fetch_page(url)
        if not html:
            return None

        # Extract title
        title_match = re.search(r'<title>([^<]+)</title>', html)
        title = title_match.group(1).strip() if title_match else ""
        title = re.sub(r'[｜|]\s*(国税庁|東京国税局|大阪国税局|名古屋国税局|'
                       r'札幌国税局|仙台国税局|関東信越国税局|金沢国税局|'
                       r'広島国税局|高松国税局|福岡国税局|熊本国税局|沖縄国税事務所)\s*$',
                       '', title).strip()

        # Extract main page text
        main_text = self._extract_body_text(html)
        cleaned = self._clean_text(main_text)
        # If cleaning removed too much, use the original extraction
        if len(cleaned) > 50:
            main_text = cleaned

        # Find and fetch sub-pages (besshi/appendices)
        subpages = self._find_subpages(html, path)
        appendix_texts = []
        for sp in subpages[:5]:  # Limit sub-pages per ruling
            sp_url = f"{BASE_URL}{sp}"
            sp_html = self._fetch_page(sp_url)
            if sp_html:
                sp_text = self._extract_body_text(sp_html)
                sp_text = self._clean_text(sp_text)
                if len(sp_text) > 50:
                    appendix_texts.append(sp_text)

        # Combine main + appendix text
        full_text = main_text
        if appendix_texts:
            full_text += "\n\n" + "\n\n".join(appendix_texts)

        # Try to extract response date
        date_match = re.search(r'回答年月日\s*(\S+)', full_text)
        response_date = None
        if date_match:
            response_date = self._parse_date(date_match.group(1))

        if len(full_text) < 100:
            logger.warning("Very short text (%d chars) for %s", len(full_text), path)
            return None

        return {
            "path": path,
            "title": title,
            "text": full_text,
            "url": url,
            "category": category,
            "response_date": response_date,
        }

    def _parse_date(self, raw: str) -> Optional[str]:
        """Parse Japanese date string to ISO format."""
        era_map = {'令和': 2018, '平成': 1988, '昭和': 1925, '大正': 1911}
        for era, offset in era_map.items():
            m = re.match(rf'{era}(\d+)年(\d+)月(\d+)日', raw)
            if m:
                year = offset + int(m.group(1))
                month = int(m.group(2))
                day = int(m.group(3))
                try:
                    return f"{year:04d}-{month:02d}-{day:02d}"
                except ValueError:
                    return None
        # Standard yyyy年mm月dd日
        m = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', raw)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a ruling into the standard schema."""
        cat_key = raw["category"]
        cat_name = CATEGORIES.get(cat_key, (None, cat_key))[1]
        return {
            "_id": raw["path"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("response_date"),
            "url": raw["url"],
            "language": "ja",
            "category": cat_name,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all advance rulings from all categories.

        Yields raw dicts (not normalized) — BaseScraper handles normalization.
        """
        all_links = self._discover_all_links()
        total = 0

        for path, category in all_links:
            raw = self._parse_ruling(path, category)
            if raw and raw["text"]:
                yield raw
                total += 1

                if total % 20 == 0:
                    logger.info("  Progress: %d / %d documents fetched",
                                total, len(all_links))

        logger.info("Done. Total documents: %d", total)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Full refresh — advance rulings don't have reliable update dates."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        html = self._fetch_page(f"{BASE_URL}/law/bunshokaito/01.htm")
        if html and '文書回答事例' in html:
            logger.info("Connectivity OK — advance rulings index page accessible")
            return True
        logger.error("Connectivity test FAILED")
        return False


if __name__ == "__main__":
    scraper = NTAAdvanceRulings()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif cmd == "bootstrap":
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif cmd == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
