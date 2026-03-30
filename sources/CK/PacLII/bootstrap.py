#!/usr/bin/env python3
"""
CK/PacLII -- Cook Islands Legal Information (PacLII) Fetcher

Fetches case law and legislation from the Pacific Islands Legal Information
Institute (PacLII) for Cook Islands.

Strategy:
  - Case law: iterate court databases by year, collect document links
  - Legislation: iterate alpha-indexed listing pages, collect document links
  - Fetch each HTML document for full text

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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CK.PacLII")

BASE_URL = "https://www.paclii.org"

COURTS = [
    {"code": "CKHC", "name": "High Court", "path": "/ck/cases/CKHC/"},
    {"code": "CKCA", "name": "Court of Appeal", "path": "/ck/cases/CKCA/"},
    {"code": "CKLC", "name": "High Court Land Division", "path": "/ck/cases/CKLC/"},
    {"code": "CK-UKPC", "name": "UK Privy Council", "path": "/ck/cases/CK-UKPC/"},
]

LEGIS_DATABASES = [
    {"type": "num_act", "name": "Sessional Legislation", "path": "/ck/legis/num_act/"},
    {"type": "sub_leg", "name": "Subsidiary Legislation", "path": "/ck/legis/sub_leg/"},
    {"type": "consol_act", "name": "Consolidated Legislation", "path": "/ck/legis/consol_act/"},
    {"type": "ck-nz_act", "name": "NZ Legislation for CK", "path": "/ck/legis/ck-nz_act/"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ALPHA_LETTERS = list("0ABCDEFGHIJKLMNOPQRSTUVWXYZ")


class PacLIIScraper(BaseScraper):
    """Scraper for CK/PacLII -- Cook Islands legal data."""

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
                if resp.status_code == 410:
                    logger.warning(f"410 Gone: {url[:80]}")
                    return None
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _discover_case_urls(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Discover case law document URLs by iterating court/year listings."""
        documents = []
        seen = set()
        current_year = datetime.now().year

        for court in COURTS:
            # Get the court's main page to find available years
            court_url = BASE_URL + court["path"]
            resp = self._request(court_url)
            if resp is None:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find year links
            years = set()
            for a in soup.find_all("a", href=True):
                href = a["href"].rstrip("/")
                m = re.search(r"/(\d{4})/?$", href)
                if m:
                    years.add(int(m.group(1)))

            if not years:
                # Try iterating common years
                years = set(range(1970, current_year + 1))

            logger.info(f"Court {court['code']}: found {len(years)} years")

            for year in sorted(years, reverse=True):
                year_url = f"{court_url}{year}/"
                resp = self._request(year_url)
                if resp is None:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    # Match case document links like ../2024/3.html or 3.html
                    if not href.endswith(".html"):
                        continue
                    filename = href.rstrip("/").split("/")[-1].replace(".html", "")
                    if not filename.isdigit():
                        continue

                    full_url = urljoin(year_url, href)
                    if full_url in seen:
                        continue
                    seen.add(full_url)

                    title = a.get_text(strip=True)[:200]
                    documents.append({
                        "url": full_url,
                        "title": title,
                        "court": court["code"],
                        "year": str(year),
                        "doc_type": "case_law",
                    })

                    if max_docs and len(documents) >= max_docs:
                        return documents

        logger.info(f"Discovered {len(documents)} case law URLs")
        return documents

    def _discover_legis_urls(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Discover legislation document URLs from alpha-indexed listing pages."""
        documents = []
        seen = set()

        for db in LEGIS_DATABASES:
            db_url = BASE_URL + db["path"]

            # Check which toc pages exist
            for letter in ALPHA_LETTERS:
                toc_letter = "0-9" if letter == "0" else letter
                toc_url = f"{db_url}toc-{toc_letter}.html"
                resp = self._request(toc_url)
                if resp is None:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                for li in soup.find_all("li"):
                    a = li.find("a", href=True)
                    if not a:
                        continue
                    href = a["href"]
                    if href.startswith("http") or href.startswith("/"):
                        continue

                    # Legislation links are directory slugs (no .html)
                    full_url = urljoin(toc_url, href)
                    if full_url in seen:
                        continue
                    seen.add(full_url)

                    title = a.get_text(strip=True)[:200]
                    documents.append({
                        "url": full_url,
                        "title": title,
                        "db_type": db["type"],
                        "db_name": db["name"],
                        "doc_type": "legislation",
                    })

                    if max_docs and len(documents) >= max_docs:
                        return documents

        logger.info(f"Discovered {len(documents)} legislation URLs")
        return documents

    def _extract_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a document page and extract full text."""
        resp = self._request(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            title = re.sub(r"\s+", " ", title)

        if not title or "not found" in title.lower():
            return None

        body = soup.find("body")
        if not body:
            return None

        for tag in body.find_all(["script", "style"]):
            tag.decompose()

        text = body.get_text(separator="\n", strip=True)

        # Remove PacLII navigation boilerplate
        nav_end_markers = [
            "Download original PDF",
            "Download RTF",
            "Download original",
        ]
        for marker in nav_end_markers:
            idx = text.find(marker)
            if idx > 0:
                text = text[idx + len(marker):]
                break

        # Remove trailing boilerplate
        trail_markers = [
            "PacLII: Copyright Policy",
            "Disclaimers",
            "Privacy Policy",
        ]
        for marker in trail_markers:
            idx = text.find(marker)
            if idx > 0:
                text = text[:idx]

        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        if len(text) < 50:
            return None

        # Generate document ID from URL path
        path = url.replace(BASE_URL, "").strip("/")
        doc_id = hashlib.md5(path.encode("utf-8")).hexdigest()[:12]

        return {
            "document_id": f"CK-PAC-{doc_id}",
            "title": title,
            "text": text,
            "url": url,
        }

    def _parse_case_date(self, title: str) -> Optional[str]:
        """Extract date from case title like '[2024] CKHC 3 (29 January 2024)'."""
        m = re.search(r"\((\d{1,2})\s+(\w+)\s+(\d{4})\)\s*$", title)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            year = m.group(3)
            months = {
                "january": "01", "february": "02", "march": "03", "april": "04",
                "may": "05", "june": "06", "july": "07", "august": "08",
                "september": "09", "october": "10", "november": "11", "december": "12",
            }
            month = months.get(month_name, "01")
            return f"{year}-{month}-{day:02d}"

        m = re.search(r"\[(\d{4})\]", title)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def _parse_legis_date(self, title: str) -> Optional[str]:
        """Extract year from legislation title like 'Admiralty Act 2004'."""
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
        if m:
            return f"{m.group(1)}-01-01"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("doc_type", "legislation")
        title = raw.get("title", "")

        if doc_type == "case_law":
            date = self._parse_case_date(title)
        else:
            date = self._parse_legis_date(title)

        return {
            "_id": raw.get("document_id", ""),
            "_source": "CK/PacLII",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "court": raw.get("court", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Cook Islands documents from PacLII."""
        # Fetch cases
        case_urls = self._discover_case_urls()
        count = 0
        for doc_info in case_urls:
            doc = self._extract_document(doc_info["url"])
            if doc is None:
                continue
            doc["doc_type"] = "case_law"
            doc["court"] = doc_info.get("court", "")
            normalized = self.normalize(doc)
            if normalized.get("text"):
                count += 1
                yield normalized

        # Fetch legislation
        legis_urls = self._discover_legis_urls()
        for doc_info in legis_urls:
            doc = self._extract_document(doc_info["url"])
            if doc is None:
                continue
            doc["doc_type"] = "legislation"
            normalized = self.normalize(doc)
            if normalized.get("text"):
                count += 1
                yield normalized

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents."""
        # For PacLII, just re-fetch recent years
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + "/countries/ck.html")
        if resp is None:
            logger.error("Cannot reach PacLII")
            return False

        logger.info("PacLII Cook Islands page OK")

        # Test a case document
        doc = self._extract_document(BASE_URL + "/ck/cases/CKHC/2024/1.html")
        if doc:
            logger.info(f"Case OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
        else:
            logger.error("Failed to fetch case document")
            return False

        # Test a legislation document
        doc = self._extract_document(BASE_URL + "/ck/legis/num_act/aa2004115/")
        if doc:
            logger.info(f"Legislation OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
        else:
            logger.error("Failed to fetch legislation document")
            return False

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CK/PacLII data fetcher")
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

    scraper = PacLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            count = 0

            # Get a few cases
            case_urls = scraper._discover_case_urls(max_docs=10)
            for doc_info in case_urls:
                if count >= 10:
                    break
                doc = scraper._extract_document(doc_info["url"])
                if doc is None:
                    continue
                doc["doc_type"] = "case_law"
                doc["court"] = doc_info.get("court", "")
                record = scraper.normalize(doc)
                if not record.get("text"):
                    continue

                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"[{count+1}] {record['title'][:80]} ({len(record['text']):,} chars)")
                count += 1

            # Get a few legislation docs
            legis_urls = scraper._discover_legis_urls(max_docs=10)
            for doc_info in legis_urls:
                if count >= 15:
                    break
                doc = scraper._extract_document(doc_info["url"])
                if doc is None:
                    continue
                doc["doc_type"] = "legislation"
                record = scraper.normalize(doc)
                if not record.get("text"):
                    continue

                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"[{count+1}] {record['title'][:80]} ({len(record['text']):,} chars)")
                count += 1

            logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        else:
            count = 0
            for record in scraper.fetch_all():
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    logger.info(f"Saved {count} records...")
            logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
