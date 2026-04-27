#!/usr/bin/env python3
"""
CI/CNDJ -- Cote d'Ivoire Legal Information Fetcher (CivLII / Laws.Africa)

Fetches Ivorian legislation and case law with full text from Akoma Ntoso HTML
via civlii.laws.africa (Centre National de Documentation Juridique).

Strategy:
  - Paginate legislation listing (~16 pages, ~798 acts)
  - Paginate judgments listing (~56 pages, ~1,556 judgments)
  - Fetch each document page, extract text from la-akoma-ntoso element
  - Respect 5-second crawl delay per robots.txt

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
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

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CI.CNDJ")

BASE_URL = "https://civlii.laws.africa"
LEGISLATION_URL = f"{BASE_URL}/en/legislation/"
JUDGMENTS_URL = f"{BASE_URL}/en/judgments/all/"
MAX_LEGISLATION_PAGES = 20
MAX_JUDGMENT_PAGES = 60


class CivLIIScraper(BaseScraper):
    """Scraper for CI/CNDJ -- Ivorian legislation and case law via CivLII."""

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
        """HTTP GET with 5-second crawl delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(5)  # robots.txt Crawl-delay: 5
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
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_listing_page(self, html: str, link_pattern: str) -> List[Dict[str, str]]:
        """Parse a listing page for document links matching a pattern."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        links = soup.find_all("a", href=lambda h: h and link_pattern in str(h))
        for link in links:
            href = link.get("href", "")
            if href in seen:
                continue
            seen.add(href)

            title = link.get_text(strip=True)
            if not title:
                continue

            full_url = href if href.startswith("http") else BASE_URL + href
            documents.append({
                "title": title,
                "url": full_url,
                "href": href,
            })

        return documents

    def _extract_full_text(self, html: str) -> Dict[str, str]:
        """Extract full text and metadata from a document page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "date": "", "title": ""}

        # Title from h1
        h1 = soup.find("h1")
        if h1:
            result["title"] = h1.get_text(strip=True)

        # Full text from la-akoma-ntoso element
        akn = soup.find("la-akoma-ntoso")
        if akn:
            text = akn.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            result["text"] = text.strip()

        # Fallback: try article content or main content area
        if not result["text"]:
            for selector in ["article", ".document-content", ".content-body", "main"]:
                el = soup.select_one(selector)
                if el:
                    text = el.get_text(separator="\n", strip=True)
                    text = re.sub(r"\n{3,}", "\n\n", text)
                    text = re.sub(r" {2,}", " ", text)
                    if len(text) > 200:
                        result["text"] = text.strip()
                        break

        # Date from assent-date or commencement-date metadata
        for attr in ["assent-date", "commencement-date", "publication-date"]:
            el = soup.find(attrs={"class": attr})
            if el:
                date_text = el.get_text(strip=True)
                # Try various date formats
                for fmt in ["%d %B %Y", "%d %b %Y", "%d/%m/%Y"]:
                    date_m = re.search(r"(\d{1,2}[\s/]\w+[\s/]\d{4})", date_text)
                    if date_m:
                        try:
                            parsed = datetime.strptime(date_m.group(1), fmt)
                            result["date"] = parsed.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            pass
                if result["date"]:
                    break

        # Try French month names
        if not result["date"]:
            fr_months = {
                "janvier": "01", "février": "02", "mars": "03", "avril": "04",
                "mai": "05", "juin": "06", "juillet": "07", "août": "08",
                "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
            }
            for attr in ["assent-date", "commencement-date", "publication-date"]:
                el = soup.find(attrs={"class": attr})
                if el:
                    date_text = el.get_text(strip=True).lower()
                    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_text)
                    if m:
                        day, month_name, year = m.groups()
                        if month_name in fr_months:
                            result["date"] = f"{year}-{fr_months[month_name]}-{int(day):02d}"
                            break

        # Fallback date from URL
        if not result["date"]:
            date_m = re.search(r"@(\d{4}-\d{2}-\d{2})", html[:5000])
            if date_m:
                result["date"] = date_m.group(1)

        return result

    def _make_doc_id(self, href: str, doc_type: str) -> str:
        """Create stable ID from AKN path."""
        doc_id = href
        # Remove /en/ prefix
        doc_id = re.sub(r"^/en/", "/", doc_id)
        # Remove language/date suffix
        doc_id = re.sub(r"/\w{3}@[\d-]+$", "", doc_id)
        # Convert to ID format
        doc_id = doc_id.strip("/").replace("/", "-")
        return doc_id

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("doc_type", "legislation")
        doc_id = self._make_doc_id(raw.get("href", ""), doc_type)

        return {
            "_id": doc_id,
            "_source": "CI/CNDJ",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
        }

    def _fetch_documents(self, listing_url: str, link_pattern: str, doc_type: str,
                         max_pages: int, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Generic paginated document fetcher."""
        count = 0
        seen_urls = set()

        for page_num in range(1, max_pages + 1):
            url = f"{listing_url}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                break

            docs = self._parse_listing_page(resp.text, link_pattern)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping")
                break

            logger.info(f"[{doc_type}] Page {page_num}: {len(docs)} documents")

            for doc in docs:
                if max_records and count >= max_records:
                    return

                doc_url = doc["url"]
                if doc_url in seen_urls:
                    continue
                seen_urls.add(doc_url)

                doc_resp = self._request(doc_url)
                if doc_resp is None:
                    logger.warning(f"Failed to fetch: {doc['title'][:60]}")
                    continue

                extracted = self._extract_full_text(doc_resp.text)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    logger.warning(f"Insufficient text: {doc['title'][:60]}")
                    continue

                raw = {
                    "href": doc["href"],
                    "title": extracted["title"] or doc["title"],
                    "text": extracted["text"],
                    "date": extracted["date"],
                    "url": doc_url,
                    "doc_type": doc_type,
                }
                count += 1
                yield raw

        logger.info(f"[{doc_type}] Completed: {count} documents fetched")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation (judgments are PDF-only, no text extraction)."""
        yield from self._fetch_documents(
            LEGISLATION_URL, "/akn/ci/act/", "legislation", MAX_LEGISLATION_PAGES
        )

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation (first 2 pages)."""
        yield from self._fetch_documents(
            LEGISLATION_URL, "/akn/ci/act/", "legislation", 2
        )

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test legislation
        resp = self._request(f"{LEGISLATION_URL}?page=1")
        if resp is None:
            logger.error("Cannot reach CivLII legislation page")
            return False

        docs = self._parse_listing_page(resp.text, "/akn/ci/act/")
        if not docs:
            logger.error("No legislation found on listing page")
            return False

        logger.info(f"Legislation listing OK: {len(docs)} acts on page 1")

        # Test fetching a document
        doc_resp = self._request(docs[0]["url"])
        if doc_resp:
            extracted = self._extract_full_text(doc_resp.text)
            logger.info(f"Doc OK: {docs[0]['title'][:60]} ({len(extracted['text'])} chars)")
        else:
            logger.warning("Could not fetch sample document")

        # Test judgments
        resp2 = self._request(f"{JUDGMENTS_URL}?page=1")
        if resp2:
            jdocs = self._parse_listing_page(resp2.text, "/akn/")
            logger.info(f"Judgments listing OK: {len(jdocs)} judgments on page 1")
        else:
            logger.warning("Could not reach judgments listing")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CI/CNDJ data fetcher")
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

    scraper = CivLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all():
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
