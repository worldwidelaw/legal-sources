#!/usr/bin/env python3
"""
TM/CBT-BankingLaws -- Central Bank of Turkmenistan — Banking Laws

Fetches banking laws and regulations from cbt.tm.
Full text is embedded in HTML pages (no PDFs needed).

Covers:
  - 3 English-language laws (Central Bank, Lending Institutions, Foreign Exchange)
  - 5 Turkmen-language laws (above + Microfinance, Credit Unions)
  - 4 Turkmen-language regulations (Bank Accounts, Non-Cash Settlements,
    Mortgage Procedures, Cash Operations)

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TM.CBT-BankingLaws")

BASE_URL = "https://www.cbt.tm"
DELAY = 2.0

# All known law/regulation pages: (lang_prefix, relative_path, title, doc_type, date_hint)
PAGES = [
    # English laws
    ("en", "laws/tmb_hakynda_kanun.html",
     "Law of Turkmenistan on Central Bank of Turkmenistan",
     "legislation", "2011-03-25"),
    ("en", "laws/karz_edaralary_hakynda.html",
     "Law of Turkmenistan on Lending Institutions and Banking",
     "legislation", "2011-03-25"),
    ("en", "laws/dasary_ykdysady_hakynda.html",
     "Law of Turkmenistan on Foreign Exchange Regulation and Control in Foreign Economic Relations",
     "legislation", "2011-10-01"),
    # Turkmen laws
    ("tm", "laws/tmb_hakynda_kanun.html",
     "Türkmenistanyň Merkezi banky hakynda Türkmenistanyň kanuny",
     "legislation", "2011-03-25"),
    ("tm", "laws/karz_edaralary_hakynda.html",
     "Karz edaralary we bank işi hakynda Türkmenistanyň kanuny",
     "legislation", "2011-03-25"),
    ("tm", "laws/dasary_ykdysady_hakynda.html",
     "Daşary ykdysady gatnaşyklarda walýuta düzgünleşdirmesi we walýuta gözegçiligi hakynda Türkmenistanyň kanuny",
     "legislation", "2011-10-01"),
    ("tm", "laws/mikromaliye_guramalary_hakynda.html",
     "Mikromaliýe guramalary we mikromaliýeleşdirmek hakynda Türkmenistanyň kanuny",
     "legislation", "2011-10-01"),
    ("tm", "laws/karz_birleshmeleri_hakynda.html",
     "Karz birleşmeleri hakynda Türkmenistanyň kanuny",
     "legislation", None),
    # Turkmen regulations
    ("tm", "laws/bank_hasaby_hakynda.html",
     "Bank hasaby hakynda Düzgünnama",
     "legislation", None),
    ("tm", "laws/nagt_dal_hasabat_hakynda.html",
     "Türkmenistanda nagt däl hasaplaşyklar hakynda Düzgünnama",
     "legislation", "2009-01-19"),
    ("tm", "laws/ipoteka_tertibi.html",
     "Aşgabat şäherinde ýaşaýyş jaýlaryny satyn almak üçin karz bermek Tertibi",
     "legislation", "2008-05-12"),
    ("tm", "laws/kassa_amallar.html",
     "Türkmenistanda kassa amallaryny alyp barmagyň Tertibi",
     "legislation", "2006-11-27"),
]


def _get_session():
    """Create a requests session."""
    import requests
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
        "Accept-Language": "en,tk,ru",
    })
    return session


def _strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    # Remove script and style blocks
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    # Replace br/p/div/li/tr tags with newlines
    text = re.sub(r'<(?:br|/p|/div|/li|/tr|/h[1-6])[^>]*>', '\n', text, flags=re.IGNORECASE)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _extract_body(html: str) -> str:
    """Extract the main body content from a CBT law page."""
    # The law text is typically in the main content area
    # Try to find content between common markers
    
    # Look for the content div - CBT pages use various structures
    # Try extracting from after the navigation/header area
    body_match = re.search(
        r'<body[^>]*>(.*)</body>',
        html, re.DOTALL | re.IGNORECASE
    )
    if not body_match:
        return _strip_html(html)
    
    body = body_match.group(1)
    
    # Remove navigation menus, headers, footers
    # CBT pages have nav in <ul> at the top and footer at the bottom
    body = re.sub(r'<nav[^>]*>.*?</nav>', '', body, flags=re.DOTALL | re.IGNORECASE)
    body = re.sub(r'<header[^>]*>.*?</header>', '', body, flags=re.DOTALL | re.IGNORECASE)
    body = re.sub(r'<footer[^>]*>.*?</footer>', '', body, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove the top navigation links (Home, Sitemap, Contacts, language switcher)
    body = re.sub(r'<ul[^>]*class="[^"]*nav[^"]*"[^>]*>.*?</ul>', '', body, flags=re.DOTALL | re.IGNORECASE)
    
    # The main content area typically starts after the breadcrumb/navigation
    # and ends before the footer
    text = _strip_html(body)
    
    # Remove common navigation text remnants
    nav_patterns = [
        r'(?:Home|Baş sahypa|Главная)\s*(?:Site Map|Saýtyň kartasy|Карта сайта)\s*(?:Contacts|Salgylarymyz|Контакты)',
        r'(?:About Bank|Banking System|Payments System|Legislation|National Currency|Foreign Exchange|Archive)',
        r'(?:Bank hakynda|Bank ulgamy|Töleg ulgamy|Kanunçylyk|Milli pul|Walýuta|Arhiw)',
        r'©\s*\d{4}.*?(?:Central Bank|Merkezi bank).*',
        r'Designed by.*$',
    ]
    for pat in nav_patterns:
        text = re.sub(pat, '', text, flags=re.IGNORECASE | re.MULTILINE)
    
    # Trim leading/trailing whitespace and excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _make_id(lang: str, path: str) -> str:
    """Create a stable document ID."""
    slug = re.sub(r'\.html$', '', path.split('/')[-1])
    return f"TM_cbt_{lang}_{slug}"


class CBTBankingLawsScraper(BaseScraper):
    """Scraper for TM/CBT-BankingLaws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": "TM/CBT-BankingLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "language": raw.get("language", "tk"),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        session = _get_session()
        count = 0

        for lang, path, title, doc_type, date_hint in PAGES:
            if max_records and count >= max_records:
                return

            url = f"{BASE_URL}/{lang}/{path}"
            doc_id = _make_id(lang, path)

            logger.info("Fetching [%d]: %s (%s)", count + 1, title[:60], lang)

            try:
                time.sleep(DELAY)
                r = session.get(url, timeout=30)
                if r.status_code != 200:
                    logger.warning("HTTP %d for %s", r.status_code, url)
                    continue
            except Exception as e:
                logger.warning("Failed to fetch %s: %s", url, e)
                continue

            text = _extract_body(r.text)

            # Skip pages with "No information" placeholder
            if len(text) < 200 or "No information" in text[:100]:
                logger.warning("Insufficient content (%d chars) for %s, skipping",
                               len(text), title[:60])
                continue

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date_hint,
                "url": url,
                "language": lang if lang != "tm" else "tk",
            }
            count += 1
            yield raw

        logger.info("Completed: %d documents fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        logger.info("Testing access to cbt.tm...")
        import requests
        try:
            r = requests.get(
                f"{BASE_URL}/en/laws/tmb_hakynda_kanun.html",
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)"},
            )
            if r.status_code == 200 and len(r.text) > 1000:
                logger.info("Access OK: %d bytes", len(r.text))
                return True
        except Exception as e:
            logger.error("Access failed: %s", e)
        return False


def main():
    parser = argparse.ArgumentParser(description="TM/CBT-BankingLaws data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBTBankingLawsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
