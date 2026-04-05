#!/usr/bin/env python3
"""
JP/NTA-QA -- Japan National Tax Agency Q&A Cases (質疑応答事例)

Fetches ~1,850 individual tax doctrine Q&A documents from the NTA website.
Each case includes a query (照会要旨), response (回答要旨), and related
law references (関係法令通達).

Categories: income tax (所得税), withholding tax (源泉所得税),
capital gains (譲渡所得), inheritance tax (相続税), property valuation (財産評価),
corporate tax (法人税), consumption tax (消費税), stamp tax (印紙税),
statutory reporting (法定調書).

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
logger = logging.getLogger("legal-data-hunter.JP.NTA-QA")

BASE_URL = "https://www.nta.go.jp"
DELAY = 2.0

# Tax categories and their index pages
CATEGORIES = {
    "shotoku": "Income Tax (所得税)",
    "gensen": "Withholding Tax (源泉所得税)",
    "joto": "Capital Gains Tax (譲渡所得税)",
    "sozoku": "Inheritance Tax (相続税)",
    "hyoka": "Property Valuation (財産評価)",
    "hojin": "Corporate Tax (法人税)",
    "shohi": "Consumption Tax (消費税)",
    "inshi": "Stamp Tax (印紙税)",
    "hotei": "Statutory Reporting (法定調書)",
}


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


class NTAQACases(BaseScraper):
    SOURCE_ID = "JP/NTA-QA"

    def __init__(self):
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
            # NTA pages are Shift-JIS
            try:
                return resp.content.decode('shift_jis')
            except UnicodeDecodeError:
                return resp.content.decode('utf-8', errors='replace')
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

    def _discover_qa_links(self, category: str) -> List[str]:
        """Discover all individual Q&A page paths for a category."""
        index_url = f"{BASE_URL}/law/shitsugi/{category}/01.htm"
        html = self._fetch_page(index_url)
        if not html:
            return []

        # Find links like /law/shitsugi/shotoku/01/01.htm
        pattern = rf'/law/shitsugi/{category}/\d+/\d+\.htm'
        links = re.findall(pattern, html)
        unique = sorted(set(links))
        logger.info("  %s: found %d Q&A pages", category, len(unique))
        return unique

    def _parse_qa_page(self, path: str, category: str) -> Optional[Dict[str, Any]]:
        """Parse a single Q&A page and extract structured content."""
        url = f"{BASE_URL}{path}"
        html = self._fetch_page(url)
        if not html:
            return None

        # Extract title from <title> tag
        title_match = re.search(r'<title>([^<]+)</title>', html)
        title = title_match.group(1).strip() if title_match else ""
        # Remove "｜国税庁" suffix
        title = re.sub(r'[｜|]\s*国税庁\s*$', '', title).strip()

        # Extract main content area
        content_match = re.search(
            r'<div\s+class="left-content[^"]*"[^>]*>(.*?)</div>\s*(?:<div\s+class="right-content|<footer)',
            html, re.DOTALL
        )
        if not content_match:
            # Fallback: try to find content between breadcrumb and footer nav
            content_match = re.search(
                r'</ol>\s*(.*?)<div\s+class="(?:right-content|footer)',
                html, re.DOTALL
            )

        if content_match:
            raw_content = content_match.group(1)
        else:
            # Last fallback: whole body
            body_match = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL)
            raw_content = body_match.group(1) if body_match else html

        text = strip_html(raw_content)

        # Remove navigation/breadcrumb noise from beginning
        text = re.sub(r'^.*?(?=【照会要旨】|【回答要旨】|\S{10,})', '', text,
                       count=1, flags=re.DOTALL)
        if not text.strip():
            text = strip_html(raw_content)

        # Remove footer noise
        text = re.sub(r'このページの先頭へ.*$', '', text, flags=re.DOTALL).strip()
        text = re.sub(r'サイトマップ.*$', '', text, flags=re.DOTALL).strip()
        text = re.sub(r'法令等\s*税法.*$', '', text, flags=re.DOTALL).strip()

        if len(text) < 50:
            logger.warning("Very short text (%d chars) for %s", len(text), path)
            return None

        return {
            "path": path,
            "title": title,
            "text": text,
            "url": url,
            "category": category,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a Q&A document into the standard schema."""
        return {
            "_id": raw["path"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": None,
            "url": raw["url"],
            "language": "ja",
            "category": CATEGORIES.get(raw["category"], raw["category"]),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Q&A cases from all categories."""
        total = 0
        sample_limit = 15 if sample else None

        for cat_key, cat_name in CATEGORIES.items():
            if sample_limit and total >= sample_limit:
                break

            logger.info("Category: %s", cat_name)
            links = self._discover_qa_links(cat_key)

            for path in links:
                if sample_limit and total >= sample_limit:
                    break

                raw = self._parse_qa_page(path, cat_key)
                if raw and raw["text"]:
                    record = self.normalize(raw)
                    yield record
                    total += 1

                    if total % 50 == 0:
                        logger.info("  Progress: %d documents fetched", total)

        logger.info("Done. Total documents: %d", total)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Check for new Q&A cases added since a date."""
        # NTA Q&A pages don't have reliable dates, so full refresh is needed
        yield from self.fetch_all(sample=False)

    def test(self) -> bool:
        """Quick connectivity test."""
        html = self._fetch_page(f"{BASE_URL}/law/shitsugi/01.htm")
        if html and '質疑応答事例' in html:
            logger.info("Connectivity OK — Q&A index page accessible")
            return True
        logger.error("Connectivity test FAILED")
        return False


if __name__ == "__main__":
    scraper = NTAQACases()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    elif cmd == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        import json
        count = 0
        for record in scraper.fetch_all(sample=sample):
            out_file = sample_dir / f"{count:04d}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count <= 3:
                logger.info("Sample %d: %s (%d chars)",
                            count, record["title"][:60], len(record["text"]))

        logger.info("Saved %d records to %s", count, sample_dir)

    elif cmd == "update":
        import json
        for record in scraper.fetch_updates("2020-01-01"):
            print(json.dumps(record, ensure_ascii=False))

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
