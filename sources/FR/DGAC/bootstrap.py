#!/usr/bin/env python3
"""
FR/DGAC -- French Civil Aviation Authority (DGAC) Official Publications

Fetches DGAC doctrine from the Bulletin Officiel du développement durable.

Strategy:
  - Paginate through Aviation civile themed documents (2292+ docs)
  - Extract PDF links from search results
  - Download PDFs and extract text via common/pdf_extract.extract_pdf_markdown
  - 2-second crawl delay between requests

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
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
logger = logging.getLogger("legal-data-hunter.FR.DGAC")

BASE_URL = "https://www.bulletin-officiel.developpement-durable.gouv.fr"
SEARCH_URL = f"{BASE_URL}/recherche"
MAX_PAGES = 25  # ~2300 docs at 100/page = 23 pages


class DGACScraper(BaseScraper):
    """Scraper for FR/DGAC -- French Civil Aviation Authority publications."""

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
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_search_page(self, html: str) -> List[Dict[str, str]]:
        """Parse search results page for documents with PDF links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []

        # Each result is an <li> containing a detailsResultat div
        for li in soup.find_all("li"):
            detail_div = li.find("div", class_="detailsResultat")
            if not detail_div:
                continue

            # Extract title from the TITRE LIEN anchor
            title = ""
            detail_url = ""
            title_link = li.find("a", class_="TITRE")
            if not title_link:
                title_link = li.find("a", href=lambda h: h and "notice?" in str(h))
            if title_link:
                title = title_link.get_text(strip=True)
                notice_href = title_link.get("href", "")
                notice_href = re.sub(r";jsessionid=[^?]*", "", notice_href)
                detail_url = notice_href if notice_href.startswith("http") else BASE_URL + "/" + notice_href.lstrip("/")

            # Extract metadata from spans
            reference = ""
            nor_span = detail_div.find("span", class_="NUMERO-NOR")
            if nor_span:
                reference = nor_span.get_text(strip=True)

            doc_type = ""
            type_span = detail_div.find("span", class_="TYPE")
            if type_span:
                doc_type = type_span.get_text(strip=True)

            date_sig = ""
            sig_span = detail_div.find("span", class_="DATE_SIGNATURE")
            if sig_span:
                sig_match = re.search(r"(\d{2})-(\d{2})-(\d{4})", sig_span.get_text())
                if sig_match:
                    date_sig = f"{sig_match.group(3)}-{sig_match.group(2)}-{sig_match.group(1)}"

            date_pub = ""
            pub_span = detail_div.find("span", class_="DATE_PARUTION")
            if pub_span:
                pub_match = re.search(r"(\d{2})-(\d{2})-(\d{4})", pub_span.get_text())
                if pub_match:
                    date_pub = f"{pub_match.group(3)}-{pub_match.group(2)}-{pub_match.group(1)}"

            # Extract PDF URL from pieceJointe div
            pdf_url = ""
            doc_id = ""
            pj_div = li.find("div", class_="pieceJointe")
            if pj_div:
                pdf_link = pj_div.find("a", href=lambda h: h and ".pdf" in str(h))
                if pdf_link:
                    pdf_href = pdf_link.get("href", "")
                    pdf_href = re.sub(r";jsessionid=[^?]*", "", pdf_href)
                    pdf_url = pdf_href if pdf_href.startswith("http") else BASE_URL + "/" + pdf_href.lstrip("/")
                    id_match = re.search(r"Bulletinofficiel-(\d+)", pdf_href)
                    if id_match:
                        doc_id = id_match.group(1)

            if not pdf_url:
                continue

            if not title:
                title = reference or f"DGAC-{doc_id}"

            documents.append({
                "doc_id": doc_id,
                "reference": reference,
                "title": title,
                "doc_type": doc_type,
                "date_publication": date_pub,
                "date_signature": date_sig,
                "pdf_url": pdf_url,
                "detail_url": detail_url,
            })

        return documents

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw document data to standard schema."""
        doc_id = raw.get("doc_id", "")
        reference = raw.get("reference", "")
        stable_id = f"DGAC-{doc_id}" if doc_id else f"DGAC-{reference}"

        # Prefer signature date, fall back to publication date
        date = raw.get("date_signature") or raw.get("date_publication") or ""

        return {
            "_id": stable_id,
            "_source": "FR/DGAC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("detail_url") or raw.get("pdf_url", ""),
            "reference": reference,
            "doc_type": raw.get("doc_type", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all DGAC Aviation civile documents from bulletin officiel."""
        count = 0
        seen_ids = set()

        for page_num in range(1, MAX_PAGES + 1):
            params = {
                "sort": "date_publication_from",
                "order": "desc",
                "hpp": "100",
                "p": str(page_num),
                "themes": "Aviation civile",
            }
            url = f"{SEARCH_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch search page {page_num}")
                break

            docs = self._parse_search_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping")
                break

            logger.info(f"Page {page_num}: {len(docs)} documents found")

            for doc in docs:
                # Deduplicate by doc_id or reference
                dedup = doc["doc_id"] or doc["reference"]
                if dedup in seen_ids:
                    continue
                seen_ids.add(dedup)

                if not doc["pdf_url"]:
                    continue

                # Extract text from PDF
                stable_id = f"DGAC-{doc['doc_id']}" if doc["doc_id"] else f"DGAC-{doc['reference']}"
                try:
                    md = extract_pdf_markdown(
                        source="FR/DGAC",
                        source_id=stable_id,
                        pdf_url=doc["pdf_url"],
                        table="doctrine",
                    )
                    if md and len(md) >= 100:
                        doc["text"] = md
                    else:
                        logger.warning(f"Insufficient text from PDF: {doc['title'][:60]}")
                        continue
                except Exception as e:
                    logger.warning(f"PDF extraction failed for {doc['title'][:60]}: {e}")
                    continue

                count += 1
                yield doc

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (first 3 pages)."""
        count = 0
        seen_ids = set()

        for page_num in range(1, 4):
            params = {
                "sort": "date_publication_from",
                "order": "desc",
                "hpp": "100",
                "p": str(page_num),
                "themes": "Aviation civile",
            }
            url = f"{SEARCH_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
            resp = self._request(url)
            if resp is None:
                continue

            docs = self._parse_search_page(resp.text)
            for doc in docs:
                dedup = doc["doc_id"] or doc["reference"]
                if dedup in seen_ids:
                    continue
                seen_ids.add(dedup)

                if not doc["pdf_url"]:
                    continue

                stable_id = f"DGAC-{doc['doc_id']}" if doc["doc_id"] else f"DGAC-{doc['reference']}"
                try:
                    md = extract_pdf_markdown(
                        source="FR/DGAC",
                        source_id=stable_id,
                        pdf_url=doc["pdf_url"],
                        table="doctrine",
                    )
                    if md and len(md) >= 100:
                        doc["text"] = md
                    else:
                        continue
                except Exception:
                    continue

                count += 1
                yield doc

        logger.info(f"Updates: {count} documents fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        params = {
            "sort": "date_publication_from",
            "order": "desc",
            "hpp": "10",
            "p": "1",
            "themes": "Aviation civile",
        }
        url = f"{SEARCH_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        resp = self._request(url)
        if resp is None:
            logger.error("Cannot reach Bulletin Officiel search")
            return False

        docs = self._parse_search_page(resp.text)
        if not docs:
            logger.error("No documents found in search results")
            return False

        logger.info(f"Search OK: {len(docs)} documents on page 1")

        # Check first document
        doc = docs[0]
        logger.info(f"  Title: {doc['title'][:80]}")
        logger.info(f"  PDF: {doc['pdf_url'][:80]}")
        logger.info(f"  Type: {doc['doc_type']}")
        logger.info(f"  Date: {doc['date_publication']}")
        logger.info(f"  Ref: {doc['reference']}")
        return bool(doc["pdf_url"])


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FR/DGAC data fetcher")
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

    scraper = DGACScraper()

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
