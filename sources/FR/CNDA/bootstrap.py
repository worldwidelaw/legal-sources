#!/usr/bin/env python3
"""
FR/CNDA -- French National Asylum Court (Cour nationale du droit d'asile)

Fetches CNDA case law decisions with full text via PDF extraction.

Strategy:
  - Paginate the jurisprudential decisions listing (17+ pages, ~30 decisions/page)
  - Extract individual decision page URLs from listing
  - On each decision page, find the PDF link ("Voir la décision")
  - Download PDF and extract text via common/pdf_extract.extract_pdf_markdown
  - 2-second crawl delay between requests

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FR.CNDA")

BASE_URL = "https://www.cnda.fr"
LISTING_URL = f"{BASE_URL}/decisions-de-justice/jurisprudence/decisions-jurisprudentielles"
MAX_PAGES = 20

# French month names for date parsing
FRENCH_MONTHS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
}


def parse_french_date(text: str) -> str:
    """Parse French date like '23 mars 2026' to ISO format '2026-03-23'."""
    if not text:
        return ""
    text = text.strip().lower()
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if m:
        day, month_name, year = m.groups()
        month = FRENCH_MONTHS.get(month_name, "")
        if month:
            return f"{year}-{month}-{int(day):02d}"
    return ""


class CNDAScraper(BaseScraper):
    """Scraper for FR/CNDA -- French National Asylum Court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with crawl delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)  # Polite crawl delay
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
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a listing page for decision links and metadata."""
        soup = BeautifulSoup(html, "html.parser")

        # Find all links to individual decision pages
        links = soup.find_all("a", href=lambda h: h and "/decisions-jurisprudentielles/" in str(h))

        # Group by URL — some links are image wrappers (no text), others have h3/title
        url_titles: Dict[str, str] = {}
        for link in links:
            href = link.get("href", "")
            if "?page=" in href or href == "#":
                continue
            full_url = href if href.startswith("http") else BASE_URL + href

            # Get title from h3 inside, or direct text
            h3 = link.find("h3")
            title = h3.get_text(strip=True) if h3 else link.get_text(strip=True)

            # Keep the longest title for each URL
            if title and len(title) > len(url_titles.get(full_url, "")):
                url_titles[full_url] = title

        decisions = []
        for full_url, title in url_titles.items():
            if not title or len(title) < 5:
                continue
            slug = full_url.rstrip("/").split("/")[-1]
            decisions.append({
                "title": title,
                "url": full_url,
                "slug": slug,
            })

        return decisions

    def _extract_decision_info(self, html: str) -> Dict[str, str]:
        """Extract metadata and PDF link from an individual decision page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"pdf_url": "", "date": "", "decision_number": "", "summary": ""}

        # Find PDF link - look for "Voir la décision" or Media links
        pdf_link = soup.find("a", href=lambda h: h and "/Media/" in str(h))
        if pdf_link:
            href = pdf_link.get("href", "")
            result["pdf_url"] = href if href.startswith("http") else BASE_URL + href

        # Also try links with "decision" or "décision" text
        if not result["pdf_url"]:
            for a in soup.find_all("a"):
                text = a.get_text(strip=True).lower()
                href = a.get("href", "")
                if ("décision" in text or "decision" in text) and href:
                    if "/Media/" in href or href.endswith(".pdf"):
                        result["pdf_url"] = href if href.startswith("http") else BASE_URL + href
                        break

        # Extract date - look for patterns like "23 mars 2026"
        page_text = soup.get_text()
        date_patterns = [
            r"(\d{1,2}\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4})",
        ]
        for pattern in date_patterns:
            m = re.search(pattern, page_text, re.IGNORECASE)
            if m:
                result["date"] = parse_french_date(m.group(1))
                break

        # Extract decision number - look for "n°XXXXXXXX" or "n° XXXXXXXX"
        num_match = re.search(r"n[°o]\s*(\d{6,10})", page_text)
        if num_match:
            result["decision_number"] = num_match.group(1)

        # Extract summary text from main content
        # Look for the main article/content area
        content_div = soup.find("div", class_=re.compile(r"content|article|body", re.I))
        if not content_div:
            content_div = soup.find("article")
        if not content_div:
            content_div = soup.find("main")

        if content_div:
            paragraphs = content_div.find_all("p")
            summary_parts = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 30:
                    summary_parts.append(text)
            result["summary"] = "\n\n".join(summary_parts[:5])

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw decision data to standard schema."""
        slug = raw.get("slug", "")
        decision_number = raw.get("decision_number", "")
        doc_id = f"CNDA-{decision_number}" if decision_number else f"CNDA-{slug[:80]}"

        return {
            "_id": doc_id,
            "_source": "FR/CNDA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "decision_number": decision_number,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all jurisprudential decisions from paginated listing."""
        count = 0
        seen_urls = set()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{LISTING_URL}?page={page_num}" if page_num > 1 else LISTING_URL
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch listing page {page_num}")
                break

            decisions = self._parse_listing_page(resp.text)
            if not decisions:
                logger.info(f"No decisions on page {page_num}, stopping pagination")
                break

            logger.info(f"Page {page_num}: {len(decisions)} decisions found")

            for dec in decisions:
                if dec["url"] in seen_urls:
                    continue
                seen_urls.add(dec["url"])

                # Fetch individual decision page
                dec_resp = self._request(dec["url"])
                if dec_resp is None:
                    logger.warning(f"Failed to fetch: {dec['title'][:60]}")
                    continue

                info = self._extract_decision_info(dec_resp.text)

                # Extract full text from PDF
                text = ""
                if info["pdf_url"]:
                    doc_id = f"CNDA-{info['decision_number']}" if info["decision_number"] else f"CNDA-{dec['slug'][:80]}"
                    try:
                        md = extract_pdf_markdown(
                            source="FR/CNDA",
                            source_id=doc_id,
                            pdf_url=info["pdf_url"],
                            table="case_law",
                        )
                        if md and len(md) >= 100:
                            text = md
                    except Exception as e:
                        logger.warning(f"PDF extraction failed for {dec['title'][:60]}: {e}")

                # If PDF extraction failed, use summary as fallback indicator
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for: {dec['title'][:60]}")
                    continue

                raw = {
                    "slug": dec["slug"],
                    "title": dec["title"],
                    "text": text,
                    "date": info["date"],
                    "url": dec["url"],
                    "decision_number": info["decision_number"],
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (first 3 pages)."""
        count = 0
        seen_urls = set()

        for page_num in range(1, 4):
            url = f"{LISTING_URL}?page={page_num}" if page_num > 1 else LISTING_URL
            resp = self._request(url)
            if resp is None:
                continue

            decisions = self._parse_listing_page(resp.text)
            for dec in decisions:
                if dec["url"] in seen_urls:
                    continue
                seen_urls.add(dec["url"])

                dec_resp = self._request(dec["url"])
                if dec_resp is None:
                    continue

                info = self._extract_decision_info(dec_resp.text)

                text = ""
                if info["pdf_url"]:
                    doc_id = f"CNDA-{info['decision_number']}" if info["decision_number"] else f"CNDA-{dec['slug'][:80]}"
                    try:
                        md = extract_pdf_markdown(
                            source="FR/CNDA",
                            source_id=doc_id,
                            pdf_url=info["pdf_url"],
                            table="case_law",
                        )
                        if md and len(md) >= 100:
                            text = md
                    except Exception as e:
                        logger.warning(f"PDF extraction failed: {e}")

                if not text or len(text) < 100:
                    continue

                raw = {
                    "slug": dec["slug"],
                    "title": dec["title"],
                    "text": text,
                    "date": info["date"],
                    "url": dec["url"],
                    "decision_number": info["decision_number"],
                }
                count += 1
                yield raw

        logger.info(f"Updates: {count} decisions fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(LISTING_URL)
        if resp is None:
            logger.error("Cannot reach CNDA listing page")
            return False

        decisions = self._parse_listing_page(resp.text)
        if not decisions:
            logger.error("No decisions found on listing page")
            return False

        logger.info(f"Listing OK: {len(decisions)} decisions on page 1")

        # Test one decision page
        dec_resp = self._request(decisions[0]["url"])
        if dec_resp:
            info = self._extract_decision_info(dec_resp.text)
            logger.info(f"Decision OK: {decisions[0]['title'][:60]}")
            logger.info(f"  PDF URL: {info['pdf_url'][:80] if info['pdf_url'] else 'NOT FOUND'}")
            logger.info(f"  Date: {info['date']}")
            logger.info(f"  Number: {info['decision_number']}")
            return bool(info["pdf_url"])

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FR/CNDA data fetcher")
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

    scraper = CNDAScraper()

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
