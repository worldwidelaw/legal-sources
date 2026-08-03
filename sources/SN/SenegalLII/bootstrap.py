#!/usr/bin/env python3
"""
SN/SenegalLII -- Senegal Legislation Fetcher (senlii.org / Laws.Africa)

Fetches Senegalese legislation with full text from Akoma Ntoso HTML
via senlii.org (Senegal Legal Information Institute).

Strategy:
  - Paginate legislation listing (2 pages, ~75 acts/decrees)
  - Fetch each document page, extract text from la-akoma-ntoso element
  - Respect 5-second crawl delay per robots.txt
  - Judgments are disallowed by robots.txt and excluded

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

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None


def _pdf_text(pdf_bytes: bytes) -> str:
    """Extract a text layer from a born-digital PDF (PyMuPDF, with a shared
    pdfplumber/pypdf fallback). Returns '' if no usable text."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 200:
                return text
        except Exception:
            pass
    try:
        from common import pdf_extract as _pe
        for fn in ("_extract_with_pdfplumber", "_extract_with_pypdf"):
            f = getattr(_pe, fn, None)
            if f:
                try:
                    t = f(pdf_bytes)
                    if t and len(t) >= 200:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SN.SenegalLII")

BASE_URL = "https://senlii.org"
LEGISLATION_URL = f"{BASE_URL}/en/legislation/all"
MAX_PAGES = 5  # Only 2 pages exist but allow headroom


class SenegalLIIScraper(BaseScraper):
    """Scraper for SN/SenegalLII -- Senegalese legislation via senlii.org."""

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

    def _extract_pdf_text(self, doc_url: str) -> str:
        """Fetch the born-digital source PDF for a document and extract text.

        On the Laws.Africa / Indigo platform each document exposes its original
        publication PDF at `{doc_url}/source.pdf` (the Journal Officiel gazette
        scan/typeset). For senlii.org only ~5/75 acts carry an Akoma Ntoso HTML
        body; the rest are PDF-only, so this recovers their full text.
        """
        pdf_url = doc_url.rstrip("/") + "/source.pdf"
        resp = self._request(pdf_url, timeout=120)
        if resp is None:
            return ""
        content = resp.content
        if not content[:5].startswith(b"%PDF"):
            return ""
        return _pdf_text(content)

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a legislation listing page for document links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        links = soup.find_all("a", href=lambda h: h and "/akn/sn/act/" in str(h))
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

        # Detect PDF-only pages (no actual text, just viewer chrome)
        if result["text"] and "Loading PDF" in result["text"]:
            result["text"] = ""

        # Fallback: try article content or main content area
        if not result["text"]:
            for selector in [".document-content", "article", ".content-body"]:
                el = soup.select_one(selector)
                if el:
                    text = el.get_text(separator="\n", strip=True)
                    if "Loading PDF" in text:
                        continue
                    text = re.sub(r"\n{3,}", "\n\n", text)
                    text = re.sub(r" {2,}", " ", text)
                    if len(text) > 500:
                        result["text"] = text.strip()
                        break

        # Date from URL pattern (@YYYY-MM-DD)
        date_m = re.search(r"@(\d{4}-\d{2}-\d{2})", html[:5000])
        if date_m:
            result["date"] = date_m.group(1)

        # Try metadata elements for more precise dates
        fr_months = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
        }
        for cls in ["assent-date", "commencement-date", "publication-date"]:
            el = soup.find(attrs={"class": cls})
            if el:
                date_text = el.get_text(strip=True).lower()
                m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_text)
                if m:
                    day, month_name, year = m.groups()
                    if month_name in fr_months:
                        result["date"] = f"{year}-{fr_months[month_name]}-{int(day):02d}"
                        break

        return result

    def _make_doc_id(self, href: str) -> str:
        """Create stable ID from AKN path."""
        doc_id = href
        doc_id = re.sub(r"^/en/", "/", doc_id)
        doc_id = re.sub(r"/\w{3}@[\d-]+$", "", doc_id)
        doc_id = doc_id.strip("/").replace("/", "-")
        return doc_id

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = self._make_doc_id(raw.get("href", ""))
        return {
            "_id": doc_id,
            "_source": "SN/SenegalLII",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "text_source": raw.get("text_source", "html"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation documents."""
        count = 0
        seen_urls = set()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{LEGISLATION_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                break

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping")
                break

            logger.info(f"Page {page_num}: {len(docs)} documents")

            for doc in docs:
                doc_url = doc["url"]
                if doc_url in seen_urls:
                    continue
                seen_urls.add(doc_url)

                doc_resp = self._request(doc_url)
                if doc_resp is None:
                    logger.warning(f"Failed to fetch: {doc['title'][:60]}")
                    continue

                extracted = self._extract_full_text(doc_resp.text)
                source = "html"
                if not extracted["text"] or len(extracted["text"]) < 500:
                    # PDF-only document — recover full text from source.pdf
                    pdf_text = self._extract_pdf_text(doc_url)
                    if len(pdf_text) >= 500:
                        extracted["text"] = pdf_text
                        source = "pdf"
                    else:
                        logger.warning(f"Insufficient text (html={len(extracted['text'])}, "
                                       f"pdf={len(pdf_text)} chars): {doc['title'][:60]}")
                        continue

                raw = {
                    "href": doc["href"],
                    "title": extracted["title"] or doc["title"],
                    "text": extracted["text"],
                    "date": extracted["date"],
                    "url": doc_url,
                    "text_source": source,
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} legislation documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation (first page only)."""
        seen_urls = set()
        resp = self._request(f"{LEGISLATION_URL}?page=1")
        if resp is None:
            return

        docs = self._parse_listing_page(resp.text)
        for doc in docs:
            doc_url = doc["url"]
            if doc_url in seen_urls:
                continue
            seen_urls.add(doc_url)

            doc_resp = self._request(doc_url)
            if doc_resp is None:
                continue

            extracted = self._extract_full_text(doc_resp.text)
            source = "html"
            if not extracted["text"] or len(extracted["text"]) < 500:
                pdf_text = self._extract_pdf_text(doc_url)
                if len(pdf_text) >= 500:
                    extracted["text"] = pdf_text
                    source = "pdf"
                else:
                    continue

            yield {
                "href": doc["href"],
                "title": extracted["title"] or doc["title"],
                "text": extracted["text"],
                "date": extracted["date"],
                "url": doc_url,
                "text_source": source,
            }

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{LEGISLATION_URL}?page=1")
        if resp is None:
            logger.error("Cannot reach SenegalLII legislation page")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No legislation found on listing page")
            return False

        logger.info(f"Listing OK: {len(docs)} acts on page 1")

        # Test fetching a document
        doc_resp = self._request(docs[0]["url"])
        if doc_resp:
            extracted = self._extract_full_text(doc_resp.text)
            logger.info(f"Doc OK: {docs[0]['title'][:60]} ({len(extracted['text'])} chars)")
            return len(extracted["text"]) > 100
        else:
            logger.error("Could not fetch sample document")
            return False


def main():
    scraper = SenegalLIIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        max_records = 15 if sample_mode else None

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            filename = sample_dir / f"{record['_id'][:80]}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"[{count}] Saved: {record['title'][:60]} ({len(record['text'])} chars)")

            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        sys.exit(0 if count >= 10 else 1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
