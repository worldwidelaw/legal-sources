#!/usr/bin/env python3
"""
LR/LiberLII -- Liberia Legal Information Institute Fetcher

Fetches case law and legislation from LiberLII (liberlii.org).
Supreme Court decisions (1861-2017), plus legislation databases.

Strategy:
  - Case law: iterate Supreme Court by year, collect document links
  - Legislation: iterate alpha-indexed TOC pages per database
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
logger = logging.getLogger("legal-data-hunter.LR.LiberLII")

BASE_URL = "http://www.liberlii.org"

COURTS = [
    {"code": "LRSC", "name": "Supreme Court of Liberia", "path": "/lr/cases/LRSC/"},
]

LEGIS_DATABASES = [
    {"type": "codes", "name": "Codes of Laws Revised", "path": "/lr/legis/codes/", "doc_type": "legislation"},
    {"type": "acts", "name": "Legislative Acts (Handbills)", "path": "/lr/legis/acts/", "doc_type": "legislation"},
    {"type": "const", "name": "Constitutions", "path": "/lr/legis/const/", "doc_type": "legislation"},
    {"type": "exec_orders", "name": "Executive Orders", "path": "/lr/legis/exec_orders/", "doc_type": "legislation"},
    {"type": "rules", "name": "Court Rules", "path": "/lr/legis/rules/", "doc_type": "legislation"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ALPHA_LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


class LiberLIIScraper(BaseScraper):
    """Scraper for LR/LiberLII -- Liberia legal data."""

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
                if resp.status_code in (403, 404):
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
                # Fall back to alphabetical TOC pages
                logger.info(f"Court {court['code']}: no year links, using alpha TOC")
                toc_docs = self._discover_case_toc_urls(court, max_docs, seen)
                documents.extend(toc_docs)
                if max_docs and len(documents) >= max_docs:
                    return documents[:max_docs]
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

    def _discover_case_toc_urls(self, court: Dict, max_docs: Optional[int], seen: set) -> List[Dict[str, str]]:
        """Discover case URLs from alphabetical TOC pages."""
        documents = []
        court_url = BASE_URL + court["path"]

        for letter in ALPHA_LETTERS:
            toc_url = f"{court_url}toc-{letter}.html"
            resp = self._request(toc_url)
            if resp is None:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.endswith(".html"):
                    continue
                # Extract numeric filename from paths like ../2017/1.html
                parts = href.rstrip("/").split("/")
                filename = parts[-1].replace(".html", "")
                if not filename.isdigit():
                    continue

                full_url = urljoin(toc_url, href)
                if full_url in seen:
                    continue
                seen.add(full_url)

                # Try to extract year from URL
                year = ""
                if len(parts) >= 2:
                    m = re.match(r"\d{4}$", parts[-2])
                    if m:
                        year = parts[-2]

                title = a.get_text(strip=True)[:300]
                documents.append({
                    "url": full_url,
                    "title": title,
                    "court": court["code"],
                    "court_name": court["name"],
                    "year": year,
                    "doc_type": "case_law",
                })

                if max_docs and len(documents) >= max_docs:
                    return documents

        return documents

    def _discover_legis_urls(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Discover legislation document URLs from alpha-indexed TOC pages."""
        documents = []
        seen = set()

        for db in LEGIS_DATABASES:
            db_url = BASE_URL + db["path"]

            # Try alphabetical TOC pages
            found_any = False
            for letter in ALPHA_LETTERS:
                toc_url = f"{db_url}toc-{letter}.html"
                resp = self._request(toc_url)
                if resp is None:
                    continue

                found_any = True
                soup = BeautifulSoup(resp.text, "html.parser")

                for li in soup.find_all("li"):
                    a = li.find("a", href=True)
                    if not a:
                        continue
                    href = a["href"]

                    full_url = urljoin(toc_url, href)
                    if full_url in seen or full_url.endswith("toc-"):
                        continue
                    seen.add(full_url)

                    title = a.get_text(strip=True)[:300]
                    documents.append({
                        "url": full_url,
                        "title": title,
                        "db_type": db["type"],
                        "db_name": db["name"],
                        "doc_type": db["doc_type"],
                    })

                    if max_docs and len(documents) >= max_docs:
                        return documents

            # If no TOC pages, try the index page directly
            if not found_any:
                resp = self._request(db_url)
                if resp is None:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("toc-") or href.startswith("http"):
                        continue
                    if href.startswith("/") and "/legis/" not in href:
                        continue

                    full_url = urljoin(db_url, href)
                    if full_url in seen:
                        continue
                    seen.add(full_url)

                    title = a.get_text(strip=True)[:300]
                    if not title or len(title) < 3:
                        continue

                    documents.append({
                        "url": full_url,
                        "title": title,
                        "db_type": db["type"],
                        "db_name": db["name"],
                        "doc_type": db["doc_type"],
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

        # Remove LiberLII/AustLII navigation header
        header_markers = ["LawCite\n|\nHelp", "Help\n"]
        for marker in header_markers:
            idx = text.find(marker)
            if 0 < idx < 800:
                text = text[idx + len(marker):]
                break
        else:
            idx = text.find("Database Search")
            if 0 < idx < 800:
                end = text.find("\n", idx + 15)
                if end > 0:
                    text = text[end:]

        # Remove trailing boilerplate
        trail_markers = [
            "LiberLII:\nCopyright Policy",
            "LiberLII: Copyright Policy",
            "AustLII:\nCopyright Policy",
            "AustLII: Copyright Policy",
            "CommonLII:\nCopyright Policy",
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

        path = url.replace(BASE_URL, "").strip("/")
        doc_id = hashlib.md5(path.encode("utf-8")).hexdigest()[:12]

        return {
            "document_id": f"LR-LII-{doc_id}",
            "title": title,
            "text": text,
            "url": url,
        }

    def _parse_case_date(self, title: str) -> Optional[str]:
        """Extract date from case title."""
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

        m = re.search(r"\[(\d{4})\]", title)
        if m:
            return f"{m.group(1)}-01-01"

        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("doc_type", "case_law")
        title = raw.get("title", "")
        date = self._parse_case_date(title)

        return {
            "_id": raw.get("document_id", ""),
            "_source": "LR/LiberLII",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "court": raw.get("court", ""),
            "court_name": raw.get("court_name", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Liberia documents from LiberLII."""
        case_urls = self._discover_case_urls()
        count = 0
        for doc_info in case_urls:
            doc = self._extract_document(doc_info["url"])
            if doc is None:
                continue
            doc["doc_type"] = "case_law"
            doc["court"] = doc_info.get("court", "")
            doc["court_name"] = doc_info.get("court_name", "")
            normalized = self.normalize(doc)
            if normalized.get("text"):
                count += 1
                yield normalized

        legis_urls = self._discover_legis_urls()
        for doc_info in legis_urls:
            doc = self._extract_document(doc_info["url"])
            if doc is None:
                continue
            doc["doc_type"] = doc_info.get("doc_type", "legislation")
            normalized = self.normalize(doc)
            if normalized.get("text"):
                count += 1
                yield normalized

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + "/lr/cases/LRSC/")
        if resp is None:
            logger.error("Cannot reach LiberLII")
            return False

        logger.info("LiberLII Supreme Court index page OK")

        test_urls = [
            BASE_URL + "/lr/cases/LRSC/2017/1.html",
            BASE_URL + "/lr/cases/LRSC/2016/1.html",
            BASE_URL + "/lr/cases/LRSC/2015/1.html",
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

    parser = argparse.ArgumentParser(description="LR/LiberLII data fetcher")
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

    scraper = LiberLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            count = 0

            # Get case law samples
            case_urls = scraper._discover_case_urls(max_docs=12)
            for doc_info in case_urls:
                if count >= 10:
                    break
                doc = scraper._extract_document(doc_info["url"])
                if doc is None:
                    continue
                doc["doc_type"] = "case_law"
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

            # Get legislation samples
            legis_urls = scraper._discover_legis_urls(max_docs=10)
            for doc_info in legis_urls:
                if count >= 15:
                    break
                doc = scraper._extract_document(doc_info["url"])
                if doc is None:
                    continue
                doc["doc_type"] = doc_info.get("doc_type", "legislation")
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
