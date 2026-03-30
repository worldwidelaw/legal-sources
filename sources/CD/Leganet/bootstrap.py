#!/usr/bin/env python3
"""
CD/Leganet -- DRC (Congo) Legislation Portal Fetcher

Fetches legislation from leganet.cd, a static HTML site with full-text
legal documents covering DRC law from 1886 to present.

Strategy:
  - Crawl 8 category index pages to discover document URLs
  - Fetch each HTML document and extract full text
  - Parse title tag for metadata (date, type, description)

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
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set
from urllib.parse import urljoin, unquote, quote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CD.Leganet")

BASE_URL = "https://www.leganet.cd"

CATEGORY_PAGES = [
    "/Legislation/Tables/droit_civil.htm",
    "/Legislation/Tables/droit_economique.htm",
    "/Legislation/Tables/droit_judiciaire.htm",
    "/Legislation/Tables/droit_penal.htm",
    "/Legislation/Tables/droit_public.htm",
    "/Legislation/Tables/droit_social.htm",
    "/Legislation/Tables/droitfiscal.htm",
    "/Legislation/Tables/provinces.htm",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
}


class LeganetScraper(BaseScraper):
    """Scraper for CD/Leganet -- DRC legislation portal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 406:
                    logger.warning(f"ModSecurity blocked request to {url[:80]}")
                    return None
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                # Handle windows-1252 encoding
                if 'charset' not in resp.headers.get('content-type', ''):
                    resp.encoding = 'windows-1252'
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _discover_documents(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Crawl category pages to discover all document URLs."""
        seen_urls: Set[str] = set()
        documents: List[Dict[str, str]] = []

        for cat_path in CATEGORY_PAGES:
            cat_url = BASE_URL + cat_path
            cat_name = cat_path.split("/")[-1].replace(".htm", "")
            logger.info(f"Crawling category: {cat_name}")

            resp = self._request(cat_url)
            if resp is None:
                logger.warning(f"Failed to fetch category page: {cat_name}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                link_text = a.get_text(strip=True)

                # Skip navigation, category links, PDFs, and external links
                if not href or href.startswith("http") or href.startswith("#"):
                    continue
                if "Tables/" in href or href == "../../" or not link_text:
                    continue

                # Only HTML documents
                if not (href.endswith(".htm") or href.endswith(".html")):
                    continue

                # Skip table-of-contents and index pages
                lower_href = href.lower()
                if any(skip in lower_href for skip in ["table.htm", "index.htm", "sommaire"]):
                    continue

                full_url = urljoin(cat_url, href)

                # Only documents under /Legislation/
                if "/Legislation/" not in full_url:
                    continue

                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                documents.append({
                    "url": full_url,
                    "link_text": link_text[:200],
                    "category": cat_name,
                })

                if max_docs and len(documents) >= max_docs:
                    return documents

        logger.info(f"Discovered {len(documents)} unique document URLs")
        return documents

    def _extract_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a document page and extract metadata + full text."""
        resp = self._request(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract title
        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            # Clean up multi-line titles
            title = re.sub(r"\s+", " ", title)

        if not title or title.lower() in ("404 not found", "not found"):
            return None

        # Extract body text
        body = soup.find("body")
        if not body:
            return None

        # Remove script and style tags
        for tag in body.find_all(["script", "style"]):
            tag.decompose()

        text = body.get_text(separator="\n", strip=True)

        # Remove the LEGANET.CD header watermarks
        text = re.sub(r"(LEGANET\.CD\s*)+", "", text)
        # Clean excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        if len(text) < 100:
            return None

        # Parse date from title
        date = self._parse_date(title)

        # Generate document ID from URL path
        path = unquote(url.replace(BASE_URL, ""))
        doc_id = hashlib.md5(path.encode("utf-8")).hexdigest()[:12]

        return {
            "document_id": f"CD-LEG-{doc_id}",
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "path": path,
        }

    def _parse_date(self, title: str) -> Optional[str]:
        """Extract date from document title like '15 février 1965. - ORDONNANCE 44'."""
        # French month names
        months = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
            "fevrier": "02", "aout": "08",  # without accents
        }

        # Pattern: DD month YYYY
        m = re.search(
            r"(\d{1,2})\s+(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|"
            r"septembre|octobre|novembre|d[eé]cembre)\s+(\d{4})",
            title, re.IGNORECASE,
        )
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower().replace("é", "e").replace("û", "u")
            year = m.group(3)
            month = months.get(month_name, "01")
            return f"{year}-{month}-{day:02d}"

        # Pattern: just a year
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("document_id", ""),
            "_source": "CD/Leganet",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation documents."""
        documents = self._discover_documents()
        count = 0

        for doc_info in documents:
            doc = self._extract_document(doc_info["url"])
            if doc is None:
                continue

            doc["category"] = doc_info.get("category", "")
            normalized = self.normalize(doc)
            if normalized.get("text"):
                count += 1
                yield normalized

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (static site, so just first pages)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + CATEGORY_PAGES[0])
        if resp is None:
            logger.error("Cannot reach leganet.cd")
            return False

        soup = BeautifulSoup(resp.text, "html.parser")
        links = [a for a in soup.find_all("a", href=True)
                 if a["href"].endswith((".htm", ".html"))]
        logger.info(f"Category page OK: {len(links)} links found")

        # Test a document page
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if (href.endswith((".htm", ".html")) and "Tables/" not in href
                    and not href.startswith("http") and not href.startswith("#")):
                full_url = urljoin(BASE_URL + CATEGORY_PAGES[0], href)
                if "/Legislation/" in full_url:
                    doc = self._extract_document(full_url)
                    if doc:
                        logger.info(f"Document OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
                        return True

        logger.error("No document could be fetched")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CD/Leganet data fetcher")
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
    args = parser.parse_args()

    scraper = LeganetScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            # Discover limited docs and fetch them
            docs = scraper._discover_documents(max_docs=50)
            count = 0

            for doc_info in docs:
                if count >= 15:
                    break

                doc = scraper._extract_document(doc_info["url"])
                if doc is None:
                    continue

                doc["category"] = doc_info.get("category", "")
                record = scraper.normalize(doc)
                if not record.get("text"):
                    continue

                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                text_len = len(record.get("text", ""))
                logger.info(
                    f"[{count + 1}] {record.get('title', '?')[:80]} "
                    f"({text_len:,} chars)"
                )
                count += 1

            logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        else:
            count = 0
            for record in scraper.fetch_all():
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                text_len = len(record.get("text", ""))
                logger.info(
                    f"[{count + 1}] {record.get('title', '?')[:80]} "
                    f"({text_len:,} chars)"
                )
                count += 1

            logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
