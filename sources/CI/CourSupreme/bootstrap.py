#!/usr/bin/env python3
"""
CI/CourSupreme -- Côte d'Ivoire Supreme Court Decisions via Juricaf

Fetches Ivorian court decisions with full text from juricaf.org
(AHJUCAF francophone court decisions database).

Strategy:
  - Paginate search results for Côte d'Ivoire (10 results/page, ~18 pages)
  - Fetch each decision page for full text + Dublin Core metadata
  - ~174 decisions: Cour de Cassation, Cour d'appel, Tribunal

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CI.CourSupreme")

BASE_URL = "https://juricaf.org"
SEARCH_URL = f"{BASE_URL}/recherche/+/facet_pays:C%C3%B4te_d'Ivoire"
MAX_PAGES = 25  # ~174 decisions / 10 per page = ~18 pages, with margin

FR_MONTHS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
}


class JuricafCIScraper(BaseScraper):
    """Scraper for CI/CourSupreme -- Ivorian court decisions via Juricaf."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.5,en;q=0.3",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with 2-second delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_search_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a Juricaf search results page for decision links."""
        soup = BeautifulSoup(html, "html.parser")
        decisions = []

        # Juricaf search results have links to /arret/COUNTRY-COURT-DATE-NUMBER
        for link in soup.find_all("a", href=lambda h: h and "/arret/" in str(h)):
            href = link.get("href", "")
            if not href.startswith("/arret/"):
                continue

            title = link.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            full_url = BASE_URL + href
            decisions.append({
                "title": title,
                "url": full_url,
                "href": href,
            })

        # Deduplicate by href
        seen = set()
        unique = []
        for d in decisions:
            if d["href"] not in seen:
                seen.add(d["href"])
                unique.append(d)

        return unique

    def _extract_decision(self, html: str, url: str) -> Dict[str, str]:
        """Extract full text and metadata from a Juricaf decision page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "date": "", "title": "", "court": "", "docket_number": ""}

        # Dublin Core metadata from meta tags
        for meta in soup.find_all("meta"):
            name = meta.get("name", "").lower()
            content = meta.get("content", "")
            if not content:
                continue
            if name == "dc.creator":
                result["court"] = content.strip()
            elif name == "dc.date":
                result["date"] = content.strip()
            elif name == "dc.description":
                pass  # Summary, not full text
            elif name == "docketnumber":
                result["docket_number"] = content.strip()

        # Title from h1 or dc.title
        h1 = soup.find("h1")
        if h1:
            result["title"] = h1.get_text(strip=True)
        if not result["title"]:
            dc_title = soup.find("meta", attrs={"name": "dc.title"})
            if dc_title:
                result["title"] = dc_title.get("content", "").strip()

        # Full text: look for the decision text container
        # Juricaf typically puts decision text in a specific div
        text_candidates = []

        # Try common containers
        for selector in [
            "div.arret-text", "div.decision-text", "div.contenu",
            "div#texte", "div.texte", "article", "div.content",
        ]:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(separator="\n", strip=True)
                text = re.sub(r"\n{3,}", "\n\n", text)
                text = re.sub(r" {2,}", " ", text)
                if len(text) > 200:
                    text_candidates.append(text)

        # Fallback: find largest text block in body
        if not text_candidates:
            body = soup.find("body")
            if body:
                # Remove nav, header, footer, sidebar elements
                for tag in body.find_all(["nav", "header", "footer", "aside", "script", "style"]):
                    tag.decompose()

                # Find all divs with substantial text
                for div in body.find_all("div"):
                    text = div.get_text(separator="\n", strip=True)
                    text = re.sub(r"\n{3,}", "\n\n", text)
                    text = re.sub(r" {2,}", " ", text)
                    if len(text) > 500:
                        text_candidates.append(text)

        # Use the longest text candidate
        if text_candidates:
            result["text"] = max(text_candidates, key=len).strip()

        # Parse date if in DD/MM/YYYY or YYYYMMDD format
        if result["date"]:
            raw_date = result["date"]
            # Try YYYY-MM-DD already
            if re.match(r"\d{4}-\d{2}-\d{2}", raw_date):
                pass  # Already good
            # Try DD/MM/YYYY
            elif m := re.match(r"(\d{2})/(\d{2})/(\d{4})", raw_date):
                result["date"] = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            # Try YYYYMMDD
            elif re.match(r"\d{8}$", raw_date):
                result["date"] = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

        # Try to extract date from URL pattern: YYYYMMDD
        if not result["date"]:
            m = re.search(r"-(\d{8})-", url)
            if m:
                d = m.group(1)
                result["date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

        return result

    def _make_decision_id(self, href: str) -> str:
        """Create stable ID from Juricaf URL path."""
        # /arret/COTEDIVOIRE-COURSUPREME-20180208-126 -> COTEDIVOIRE-COURSUPREME-20180208-126
        doc_id = href.replace("/arret/", "").strip("/")
        return doc_id

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        decision_id = self._make_decision_id(raw.get("href", ""))
        return {
            "_id": decision_id,
            "_source": "CI/CourSupreme",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "court": raw.get("court", ""),
            "docket_number": raw.get("docket_number", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Ivorian court decisions from Juricaf."""
        count = 0
        seen_hrefs = set()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{SEARCH_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch page {page_num}, stopping")
                break

            decisions = self._parse_search_page(resp.text)
            if not decisions:
                logger.info(f"No decisions on page {page_num}, stopping pagination")
                break

            logger.info(f"Page {page_num}: {len(decisions)} decision links")

            for dec in decisions:
                if max_records and count >= max_records:
                    return

                if dec["href"] in seen_hrefs:
                    continue
                seen_hrefs.add(dec["href"])

                # Fetch individual decision page
                dec_resp = self._request(dec["url"])
                if dec_resp is None:
                    logger.warning(f"Failed to fetch: {dec['title'][:60]}")
                    continue

                extracted = self._extract_decision(dec_resp.text, dec["url"])
                if not extracted["text"] or len(extracted["text"]) < 100:
                    logger.warning(f"Insufficient text ({len(extracted.get('text', ''))} chars): {dec['title'][:60]}")
                    continue

                raw = {
                    "href": dec["href"],
                    "title": extracted["title"] or dec["title"],
                    "text": extracted["text"],
                    "date": extracted["date"],
                    "court": extracted["court"],
                    "docket_number": extracted["docket_number"],
                    "url": dec["url"],
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (first 2 pages)."""
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{SEARCH_URL}?page=1")
        if resp is None:
            logger.error("Cannot reach Juricaf search page")
            return False

        decisions = self._parse_search_page(resp.text)
        if not decisions:
            logger.error("No decision links found on search page")
            return False

        logger.info(f"Search page OK: {len(decisions)} decision links on page 1")

        # Test fetching a single decision
        dec_resp = self._request(decisions[0]["url"])
        if dec_resp:
            extracted = self._extract_decision(dec_resp.text, decisions[0]["url"])
            logger.info(
                f"Decision OK: {decisions[0]['title'][:60]} "
                f"({len(extracted['text'])} chars, court={extracted['court']})"
            )
        else:
            logger.warning("Could not fetch sample decision")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CI/CourSupreme data fetcher (Juricaf)")
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

    scraper = JuricafCIScraper()

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
