#!/usr/bin/env python3
"""
MH/PacLII -- Marshall Islands Legal Information (PacLII)

Fetches case law and legislation from the Pacific Islands Legal Information
Institute (PacLII) for the Marshall Islands.

Strategy:
  - Case law: iterate court databases by year, collect document links,
    download PDFs and extract text via pdfplumber
  - Legislation: iterate alpha-indexed listing pages for consolidated acts,
    download PDFs and extract text
  - All content on PacLII-MH is PDF-based (embedded in HTML wrappers)

Courts: MHSC (Supreme Court), MHHC (High Court), MHTRC (Traditional Rights)
Legislation: consol_act (2024 Revised Code, 287 acts), num_act, sub_leg

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import sys
import time
import logging
import re
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
logger = logging.getLogger("legal-data-hunter.MH.PacLII")

BASE_URL = "https://www.paclii.org"

COURTS = [
    {"code": "MHSC", "name": "Supreme Court", "path": "/mh/cases/MHSC/"},
    {"code": "MHHC", "name": "High Court", "path": "/mh/cases/MHHC/"},
    {"code": "MHTRC", "name": "Traditional Rights Court", "path": "/mh/cases/MHTRC/"},
]

LEGIS_DATABASES = [
    {"type": "consol_act", "name": "Consolidated Legislation (2024)", "path": "/mh/legis/consol_act/"},
    {"type": "num_act", "name": "Sessional Legislation", "path": "/mh/legis/num_act/"},
    {"type": "sub_leg", "name": "Subsidiary Legislation", "path": "/mh/legis/sub_leg/"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ALPHA_LETTERS = list("0ABCDEFGHIJKLMNOPQRSTUVWXYZ")


class PacLIIScraper(BaseScraper):
    """Scraper for MH/PacLII -- Marshall Islands legal data."""

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
                if resp.status_code in (404, 410):
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        resp = self._request(pdf_url, timeout=60)
        if resp is None:
            return None

        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                parts = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        parts.append(t.strip())
                text = "\n\n".join(parts)
                return text if len(text) >= 50 else None
        except Exception as e:
            logger.warning(f"pdfplumber failed for {pdf_url[:60]}: {e}")

        try:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(resp.content))
            parts = []
            for page in reader.pages:
                t = page.extract_text()
                if t:
                    parts.append(t.strip())
            text = "\n\n".join(parts)
            return text if len(text) >= 50 else None
        except Exception as e:
            logger.warning(f"pypdf failed for {pdf_url[:60]}: {e}")

        return None

    def _get_pdf_url_from_page(self, page_url: str) -> Optional[str]:
        """Fetch an HTML page and extract the embedded PDF URL."""
        resp = self._request(page_url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        obj = soup.find("object", {"type": "application/pdf"})
        if obj and obj.get("data"):
            return urljoin(page_url, obj["data"])

        # Fallback: replace .html with .pdf
        if page_url.endswith(".html"):
            return page_url.replace(".html", ".pdf")
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
            years = set()
            for a in soup.find_all("a", href=True):
                href = a["href"].rstrip("/")
                m = re.search(r"/(\d{4})/?$", href)
                if m:
                    years.add(int(m.group(1)))
                elif href.rstrip("/").split("/")[-1].isdigit():
                    y = int(href.rstrip("/").split("/")[-1])
                    if 1970 <= y <= 2030:
                        years.add(y)

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

    def _discover_legis_urls(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Discover legislation URLs from alpha-indexed TOC pages."""
        documents = []
        seen = set()

        for db in LEGIS_DATABASES:
            db_url = BASE_URL + db["path"]

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
                    if href.startswith("http") or href.startswith("/") or href.startswith("#"):
                        continue

                    full_url = urljoin(toc_url, href)
                    if full_url in seen:
                        continue
                    seen.add(full_url)

                    title = a.get_text(strip=True)[:300]
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

    def _parse_case_date(self, title: str) -> Optional[str]:
        """Extract date from case title like '[2023] MHSC 1 (17 January 2023)'."""
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
        """Extract year from legislation title like 'Banking Act 1987'."""
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
        if m:
            return f"{m.group(1)}-01-01"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("doc_type", "case_law")
        title = raw.get("title", "")

        if doc_type == "case_law":
            date = self._parse_case_date(title)
        else:
            date = self._parse_legis_date(title)

        return {
            "_id": raw.get("document_id", ""),
            "_source": "MH/PacLII",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "court": raw.get("court", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Marshall Islands documents from PacLII."""
        count = 0

        # Fetch case law
        case_urls = self._discover_case_urls()
        for doc_info in case_urls:
            pdf_url = self._get_pdf_url_from_page(doc_info["url"])
            if not pdf_url:
                continue

            text = self._extract_pdf_text(pdf_url)
            if not text:
                continue

            path = doc_info["url"].replace(BASE_URL, "").strip("/")
            doc_id = f"MH-{doc_info['court']}-{hashlib.md5(path.encode()).hexdigest()[:10]}"

            raw = {
                "document_id": doc_id,
                "title": doc_info["title"],
                "text": text,
                "url": doc_info["url"],
                "doc_type": "case_law",
                "court": doc_info.get("court", ""),
            }
            count += 1
            yield raw

        # Fetch legislation
        legis_urls = self._discover_legis_urls()
        for doc_info in legis_urls:
            pdf_url = self._get_pdf_url_from_page(doc_info["url"])
            if not pdf_url:
                continue

            text = self._extract_pdf_text(pdf_url)
            if not text:
                continue

            path = doc_info["url"].replace(BASE_URL, "").strip("/")
            doc_id = f"MH-LEG-{hashlib.md5(path.encode()).hexdigest()[:10]}"

            raw = {
                "document_id": doc_id,
                "title": doc_info["title"],
                "text": text,
                "url": doc_info["url"],
                "doc_type": "legislation",
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + "/countries/mh.html")
        if resp is None:
            logger.error("Cannot reach PacLII")
            return False
        logger.info("PacLII Marshall Islands page OK")

        # Test a case PDF
        pdf_url = self._get_pdf_url_from_page(BASE_URL + "/mh/cases/MHSC/2023/1.html")
        if pdf_url:
            text = self._extract_pdf_text(pdf_url)
            if text:
                logger.info(f"Case PDF OK: {len(text)} chars")
            else:
                logger.error("Failed to extract text from case PDF")
                return False
        else:
            logger.error("Failed to get PDF URL from case page")
            return False

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MH/PacLII data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = PacLIIScraper()

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
