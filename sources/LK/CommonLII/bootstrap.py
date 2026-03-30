#!/usr/bin/env python3
"""
LK/CommonLII -- Sri Lanka Legal Information (CommonLII) Fetcher

Fetches case law from the Commonwealth Legal Information Institute (CommonLII)
for Sri Lanka: Supreme Court, Court of Appeal, High Court, and historical
Ceylon court reports.

Strategy:
  - Iterate court databases by year, collect document links
  - Fetch each HTML document for full text
  - Skip PDF-only documents (no PDF extraction library available)

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
from typing import Generator, Optional, Dict, Any, List
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
logger = logging.getLogger("legal-data-hunter.LK.CommonLII")

BASE_URL = "https://www.commonlii.org"

COURTS = [
    {"code": "LKSC", "name": "Supreme Court of Sri Lanka", "path": "/lk/cases/LKSC/"},
    {"code": "LKCA", "name": "Court of Appeal of Sri Lanka", "path": "/lk/cases/LKCA/"},
    {"code": "LKHC", "name": "High Court of Sri Lanka", "path": "/lk/cases/LKHC/"},
    {"code": "CeySCRp", "name": "Supreme Court of Ceylon Reports", "path": "/lk/cases/CeySCRp/"},
    {"code": "CeySCAppRp", "name": "Supreme Court of Ceylon - Appeal Reports", "path": "/lk/cases/CeySCAppRp/"},
    {"code": "BalasRp", "name": "Balasingham's Reports of Cases", "path": "/lk/cases/BalasRp/"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class CommonLIIScraper(BaseScraper):
    """Scraper for LK/CommonLII -- Sri Lanka case law."""

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

        for court in COURTS:
            court_url = BASE_URL + court["path"]
            resp = self._request(court_url)
            if resp is None:
                logger.warning(f"Court {court['code']}: index page unavailable")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find year links
            years = set()
            for a in soup.find_all("a", href=True):
                href = a["href"].rstrip("/")
                m = re.search(r"/(\d{4})/?$", href)
                if m:
                    years.add(int(m.group(1)))

            # Also check for alpha-indexed TOC pages (some courts use toc-A.html)
            has_toc = False
            for a in soup.find_all("a", href=True):
                if "toc-" in a["href"]:
                    has_toc = True
                    break

            if has_toc and not years:
                # Alpha-indexed court (e.g., historical reports)
                documents.extend(self._discover_toc_urls(court, max_docs, seen))
                if max_docs and len(documents) >= max_docs:
                    return documents[:max_docs]
                continue

            if not years:
                logger.warning(f"Court {court['code']}: no year links found")
                continue

            logger.info(f"Court {court['code']}: found {len(years)} years")

            for year in sorted(years, reverse=True):
                year_url = f"{court_url}{year}/"
                resp = self._request(year_url)
                if resp is None:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    # Match HTML case document links (skip PDFs)
                    if not href.endswith(".html"):
                        continue
                    filename = href.rstrip("/").split("/")[-1].replace(".html", "")
                    if not filename.isdigit():
                        continue

                    full_url = urljoin(year_url, href)
                    if full_url in seen:
                        continue
                    seen.add(full_url)

                    title = a.get_text(strip=True)[:300]
                    documents.append({
                        "url": full_url,
                        "title": title,
                        "court": court["code"],
                        "court_name": court["name"],
                        "year": str(year),
                        "doc_type": "case_law",
                    })

                    if max_docs and len(documents) >= max_docs:
                        return documents

        logger.info(f"Discovered {len(documents)} case law URLs")
        return documents

    def _discover_toc_urls(self, court: Dict, max_docs: Optional[int], seen: set) -> List[Dict[str, str]]:
        """Discover documents from alpha-indexed TOC pages."""
        documents = []
        court_url = BASE_URL + court["path"]
        letters = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

        for letter in letters:
            toc_url = f"{court_url}toc-{letter}.html"
            resp = self._request(toc_url)
            if resp is None:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.endswith(".html"):
                    continue
                filename = href.rstrip("/").split("/")[-1].replace(".html", "")
                if not filename.isdigit():
                    continue

                full_url = urljoin(toc_url, href)
                if full_url in seen:
                    continue
                seen.add(full_url)

                title = a.get_text(strip=True)[:300]
                documents.append({
                    "url": full_url,
                    "title": title,
                    "court": court["code"],
                    "court_name": court["name"],
                    "year": "",
                    "doc_type": "case_law",
                })

                if max_docs and len(documents) >= max_docs:
                    return documents

        return documents

    def _extract_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a document page and extract full text."""
        resp = self._request(url)
        if resp is None:
            return None

        # Detect encoding
        if resp.encoding and resp.encoding.lower() != "utf-8":
            resp.encoding = resp.apparent_encoding or "utf-8"

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

        # Remove CommonLII navigation header boilerplate
        # Pattern: "Home | Databases | WorldLII ... LawCite | Help\n"
        # Find the end of nav by looking for "LawCite\n|\nHelp" or "Database Search"
        header_markers = ["LawCite\n|\nHelp", "Help\n"]
        for marker in header_markers:
            idx = text.find(marker)
            if 0 < idx < 800:
                text = text[idx + len(marker):]
                break
        else:
            # Fallback: skip "You are here:" breadcrumb block
            idx = text.find("Database Search")
            if 0 < idx < 800:
                # Find next newline after "Database Search | Name Search..."
                end = text.find("\n", idx + 15)
                if end > 0:
                    text = text[end:]

        # Remove trailing boilerplate
        trail_markers = [
            "CommonLII:\nCopyright Policy",
            "CommonLII: Copyright Policy",
            "AustLII:\nCopyright Policy",
            "AustLII: Copyright Policy",
        ]
        for marker in trail_markers:
            idx = text.rfind(marker)
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
            "document_id": f"LK-CLII-{doc_id}",
            "title": title,
            "text": text,
            "url": url,
        }

    def _parse_case_date(self, title: str) -> Optional[str]:
        """Extract date from case title like '[2012] LKSC 6 (29 January 2012)'."""
        # Full date in parentheses: (29 January 2012)
        m = re.search(r"\((\d{1,2})\s+(\w+)\s+(\d{4})\)", title)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            months = {
                "january": "01", "february": "02", "march": "03", "april": "04",
                "may": "05", "june": "06", "july": "07", "august": "08",
                "september": "09", "october": "10", "november": "11", "december": "12",
            }
            month = months.get(month_name)
            if month:
                return f"{m.group(3)}-{month}-{day:02d}"

        # Year in brackets: [2012]
        m = re.search(r"\[(\d{4})\]", title)
        if m:
            return f"{m.group(1)}-01-01"

        # Bare year
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")
        date = self._parse_case_date(title)

        return {
            "_id": raw.get("document_id", ""),
            "_source": "LK/CommonLII",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "court": raw.get("court", ""),
            "court_name": raw.get("court_name", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Sri Lanka case law from CommonLII."""
        case_urls = self._discover_case_urls()
        count = 0
        for doc_info in case_urls:
            doc = self._extract_document(doc_info["url"])
            if doc is None:
                continue
            doc["court"] = doc_info.get("court", "")
            doc["court_name"] = doc_info.get("court_name", "")
            normalized = self.normalize(doc)
            if normalized.get("text"):
                count += 1
                yield normalized

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (re-fetches all since CommonLII has no update API)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + "/lk/")
        if resp is None:
            logger.error("Cannot reach CommonLII Sri Lanka")
            return False

        logger.info("CommonLII Sri Lanka index page OK")

        # Test fetching a case document
        test_urls = [
            BASE_URL + "/lk/cases/LKSC/2012/1.html",
            BASE_URL + "/lk/cases/LKSC/2011/1.html",
            BASE_URL + "/lk/cases/LKSC/2010/1.html",
        ]
        for url in test_urls:
            doc = self._extract_document(url)
            if doc:
                logger.info(f"Case OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
                return True

        logger.error("Failed to fetch any test case document")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="LK/CommonLII data fetcher")
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

    scraper = CommonLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            count = 0
            case_urls = scraper._discover_case_urls(max_docs=20)
            for doc_info in case_urls:
                if count >= 15:
                    break
                doc = scraper._extract_document(doc_info["url"])
                if doc is None:
                    continue
                doc["court"] = doc_info.get("court", "")
                doc["court_name"] = doc_info.get("court_name", "")
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
