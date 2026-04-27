#!/usr/bin/env python3
"""
BR/DOU -- Diário Oficial da União (Brazilian Federal Official Gazette) Fetcher

Fetches legislation from the Brazilian Federal Official Gazette via the
search endpoint which embeds JSON results in HTML, then fetches individual
article pages for full text content.

Strategy:
  - Use the search endpoint with date-range pagination
  - Parse embedded JSON from HTML response
  - Fetch individual article pages for full text
  - Focus on Section 1 (DO1) which contains laws, decrees, regulations

Usage:
  python bootstrap.py bootstrap          # Full initial pull (recent dates)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from html import unescape
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.DOU")

BASE_URL = "https://www.in.gov.br"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# DOU sections: DO1 = laws/decrees, DO2 = personnel acts, DO3 = contracts
SECTIONS = ["do1"]  # Focus on legislation


def _strip_html(html_text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not html_text:
        return ""
    text = re.sub(r'<br\s*/?\s*>', '\n', html_text)
    text = re.sub(r'<p[^>]*>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class BrazilDOUScraper(BaseScraper):
    """
    Scraper for BR/DOU -- Diário Oficial da União.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(headers=HEADERS, timeout=30)
        except ImportError:
            import urllib.request
            self.client = None

    def _http_get(self, url: str) -> Optional[str]:
        """Fetch URL with retries."""
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(url)
                    if resp.status_code == 200:
                        return resp.text
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers=HEADERS)
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _fetch_journal_listing(self, date_str: str, section: str = "dou1") -> List[Dict]:
        """Fetch article listing for a date/section from journal page."""
        url = f"{BASE_URL}/leiturajornal?secao={section}&data={date_str}"
        html = self._http_get(url)
        if not html:
            return []

        # Extract embedded JSON from script tag
        pattern = r'<script[^>]*type="application/json"[^>]*>(.*?)</script>'
        matches = re.findall(pattern, html, re.DOTALL)

        articles = []
        for match in matches:
            try:
                data = json.loads(match)
                if isinstance(data, dict) and "jsonArray" in data:
                    articles.extend(data["jsonArray"])
                elif isinstance(data, list):
                    articles.extend(data)
            except json.JSONDecodeError:
                continue

        return articles

    def _fetch_search_results(self, date_from: str, date_to: str, section: str = "do1") -> List[Dict]:
        """Fetch articles via the search endpoint."""
        url = (
            f"{BASE_URL}/consulta/-/buscar/dou?"
            f"q=*&s={section}&exactDate=personalizado"
            f"&publishFrom={date_from}&publishTo={date_to}&sortType=0"
        )
        html = self._http_get(url)
        if not html:
            return []

        # Look for the embedded JSON script tag
        pattern = r'id="_br_com_seatecnologia_in_buscadou_BuscaDouPortlet_params"[^>]*>(.*?)</script>'
        match = re.search(pattern, html, re.DOTALL)
        if not match:
            # Try alternate pattern
            pattern = r'"jsonArray"\s*:\s*(\[.*?\])\s*[,}]'
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    pass
            return []

        try:
            data = json.loads(match.group(1))
            return data.get("jsonArray", [])
        except json.JSONDecodeError:
            return []

    def _fetch_article_text(self, url_title: str) -> Optional[str]:
        """Fetch full text of an individual DOU article."""
        url = f"{BASE_URL}/en/web/dou/-/{url_title}"
        html = self._http_get(url)
        if not html:
            # Try Portuguese version
            url = f"{BASE_URL}/web/dou/-/{url_title}"
            html = self._http_get(url)
        if not html:
            return None

        # Extract article body - look for the main text container
        patterns = [
            r'<div[^>]*class="texto-dou"[^>]*>(.*?)</div>',
            r'<div[^>]*class="materia"[^>]*>(.*?)</div>',
            r'<div[^>]*id="materia"[^>]*>(.*?)</div>',
            r'<article[^>]*>(.*?)</article>',
            r'<div[^>]*class="journal-content-article"[^>]*>(.*?)</div>',
        ]

        for pat in patterns:
            match = re.search(pat, html, re.DOTALL | re.IGNORECASE)
            if match:
                return _strip_html(match.group(1))

        # Fallback: extract text between identifica and assina markers
        match = re.search(
            r'<p[^>]*class="identifica"[^>]*>(.*?)(?:<p[^>]*class="assina"[^>]*>|$)',
            html, re.DOTALL
        )
        if match:
            return _strip_html(match.group(1))

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        class_pk = str(raw.get("classPK", raw.get("id", "")))
        title = raw.get("title", raw.get("identifica", ""))
        url_title = raw.get("urlTitle", "")
        pub_date = raw.get("pubDate", "")
        section = raw.get("pubName", "do1")
        hierarchy = raw.get("hierarchyStr", raw.get("hierarchy", ""))
        art_type = raw.get("artType", "")
        content = raw.get("content", "")

        # Parse date
        date_iso = None
        if pub_date:
            for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
                try:
                    date_iso = datetime.strptime(pub_date.strip()[:10], fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        url = f"{BASE_URL}/web/dou/-/{url_title}" if url_title else ""
        text = raw.get("_full_text", "") or _strip_html(content)

        return {
            "_id": f"BR-DOU-{class_pk}",
            "_source": "BR/DOU",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": url,
            "section": section,
            "art_type": art_type,
            "hierarchy": hierarchy,
            "class_pk": class_pk,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch DOU articles by iterating over recent dates."""
        today = datetime.now()
        count = 0

        # Iterate backwards from today
        for days_back in range(0, 365):
            date = today - timedelta(days=days_back)
            if date.weekday() >= 5:  # Skip weekends (no gazette)
                continue

            date_str = date.strftime("%d-%m-%Y")
            logger.info(f"Fetching DOU for {date_str}...")

            for section in SECTIONS:
                articles = self._fetch_journal_listing(date_str, section)
                if not articles:
                    articles = self._fetch_search_results(
                        date_str, date_str, section
                    )

                for art in articles:
                    url_title = art.get("urlTitle", "")
                    if not url_title:
                        continue

                    # Fetch full text
                    time.sleep(1)
                    full_text = self._fetch_article_text(url_title)
                    if full_text and len(full_text) > 50:
                        art["_full_text"] = full_text
                    elif art.get("content"):
                        art["_full_text"] = _strip_html(art["content"])

                    normalized = self.normalize(art)
                    if normalized.get("text") and len(normalized["text"]) > 50:
                        count += 1
                        yield normalized

            time.sleep(1)

        logger.info(f"Completed: {count} DOU articles")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch articles from recent days."""
        today = datetime.now()
        days = 7
        if since:
            try:
                since_date = datetime.strptime(since[:10], "%Y-%m-%d")
                days = (today - since_date).days + 1
            except ValueError:
                pass

        count = 0
        for days_back in range(0, min(days, 30)):
            date = today - timedelta(days=days_back)
            if date.weekday() >= 5:
                continue

            date_str = date.strftime("%d-%m-%Y")
            for section in SECTIONS:
                articles = self._fetch_journal_listing(date_str, section)
                for art in articles:
                    url_title = art.get("urlTitle", "")
                    if not url_title:
                        continue
                    time.sleep(1)
                    full_text = self._fetch_article_text(url_title)
                    if full_text:
                        art["_full_text"] = full_text
                    normalized = self.normalize(art)
                    if normalized.get("text") and len(normalized["text"]) > 50:
                        count += 1
                        yield normalized

        logger.info(f"Updates: {count} articles")

    def test(self) -> bool:
        """Quick connectivity test."""
        today = datetime.now()
        for days_back in range(0, 7):
            date = today - timedelta(days=days_back)
            if date.weekday() >= 5:
                continue
            date_str = date.strftime("%d-%m-%Y")
            articles = self._fetch_journal_listing(date_str, "dou1")
            if articles:
                logger.info(f"Test passed: {len(articles)} articles on {date_str}")
                return True
            articles = self._fetch_search_results(date_str, date_str, "do1")
            if articles:
                logger.info(f"Test passed (search): {len(articles)} articles on {date_str}")
                return True
        logger.error("Test failed: no articles found for any recent date")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BR/DOU data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BrazilDOUScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

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
