#!/usr/bin/env python3
"""
CD/DroitCongolais -- DRC Legislation and Case Law Portal Fetcher

Fetches legislation, treaties, administrative decisions, and case law from
droitcongolais.info, an NGO-run portal with the systematic collection
(Recueil systématique) of Congolese law.

Strategy:
  - Crawl category/index HTML pages to discover all PDF links
  - Download each PDF and extract full text with pdfplumber
  - Parse metadata (date, type, category) from filenames and link text

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
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CD.DroitCongolais")

BASE_URL = "https://www.droitcongolais.info"

# Legislation category pages
LEGISLATION_PAGES = [
    "1a----subdiv.--rs-1-a-115---cst,-droits-fondamentaux,-etrangers--.html",
    "1b---subdiv.--rs-12--territoire-administration-decentralisee--.html",
    "1c----subdiv.-rs-13-14--legislatif-executif-administration-centrale-.html",
    "1d----subdiv.-rs-15-a-18--autorites-judiciaires--.html",
    "2a-----subdiv.--rs-21--droit-civil-.html",
    "2b----subdiv.rs-22-26--droit-des-obligations--.html",
    "3.-droit-penal-.html",
    "4.-securite-.html",
    "5.-formation-culture-.html",
    "6a---subdiv.-rs-61-624-.html",
    "6b---subdiv.-rs-625-649.html",
    "6c---subdiv.-rs-650-682.html",
    "7a---subdiv.-rs-71-734-.html",
    "7b---subdiv.-rs-735-753.html",
    "8.-sante-securite-sociale.html",
    "9.-economie.html",
]

# International treaties
TREATY_PAGES = [
    "0.1-droit-international-public.html",
    "0.2-droit-prive-.html",
    "0.3-droit-penal-.html",
    "0.4-droit-de-la-guerre-.html",
    "0.5-science-culture.html",
    "0.6-finances-.html",
    "0.7-environnement-mines-.html",
    "0.8-sante-securite-sociale-.html",
    "0.9-economie-.html",
]

# Administrative decisions
ADMIN_PAGES = [
    "presidence,-gouvernement,-ministeres.html",
    "cvdm,-arptc.html",
]

# Jurisprudence (case law)
CASELAW_PAGES = [
    "matieres-constitutionnelles.html",
    "matieres-administratives-.html",
    "matieres-de-droit-prive-.html",
    "matieres-penales.html",
    "cour-des-comptes.html",
    "tribunaux-internationaux-.html",
    "tribunaux-etrangers-.html",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
}

# French month names for date parsing
FRENCH_MONTHS = {
    "janvier": "01", "fevrier": "02", "février": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
    "aout": "08", "août": "08", "septembre": "09", "octobre": "10",
    "novembre": "11", "decembre": "12", "décembre": "12",
}


class DroitCongolaisScraper(BaseScraper):
    """Scraper for CD/DroitCongolais -- DRC legislation and case law portal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 60, stream: bool = False) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, stream=stream)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _discover_pdfs(self, pages: List[str], data_type: str,
                       seen: Set[str], max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Crawl index pages to discover PDF links."""
        documents: List[Dict[str, str]] = []

        for page_path in pages:
            page_url = f"{BASE_URL}/{page_path}"
            logger.info(f"Crawling: {page_path}")

            resp = self._request(page_url)
            if resp is None:
                logger.warning(f"Failed to fetch: {page_path}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                link_text = a.get_text(strip=True)

                if not href or not link_text:
                    continue

                # Only PDF files in the files/ directory
                lower_href = href.lower()
                if not (lower_href.endswith(".pdf") and "files/" in lower_href):
                    continue

                # Skip external links
                if href.startswith("http") and BASE_URL not in href:
                    continue

                full_url = urljoin(page_url, href)

                if full_url in seen:
                    continue
                seen.add(full_url)

                # Extract category from page path
                category = page_path.split(".html")[0].split("--")[-1].strip("-").strip()
                if not category or len(category) > 60:
                    category = page_path.replace(".html", "")

                documents.append({
                    "url": full_url,
                    "link_text": link_text[:300],
                    "data_type": data_type,
                    "category": category,
                })

                if max_docs and len(documents) >= max_docs:
                    return documents

        return documents

    def _discover_all_documents(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Discover all PDF documents across all categories."""
        seen: Set[str] = set()
        all_docs: List[Dict[str, str]] = []

        remaining = max_docs

        # Legislation
        docs = self._discover_pdfs(LEGISLATION_PAGES, "legislation", seen, remaining)
        all_docs.extend(docs)
        if remaining:
            remaining -= len(docs)
            if remaining <= 0:
                return all_docs

        # Treaties (also legislation type)
        docs = self._discover_pdfs(TREATY_PAGES, "legislation", seen, remaining)
        all_docs.extend(docs)
        if remaining:
            remaining -= len(docs)
            if remaining <= 0:
                return all_docs

        # Administrative decisions
        docs = self._discover_pdfs(ADMIN_PAGES, "legislation", seen, remaining)
        all_docs.extend(docs)
        if remaining:
            remaining -= len(docs)
            if remaining <= 0:
                return all_docs

        # Case law
        docs = self._discover_pdfs(CASELAW_PAGES, "case_law", seen, remaining)
        all_docs.extend(docs)

        logger.info(f"Discovered {len(all_docs)} unique PDF documents")
        return all_docs

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract full text with pdfplumber."""
        resp = self._request(url, timeout=120, stream=True)
        if resp is None:
            return None

        # Check file size — skip if > 50MB
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > 50 * 1024 * 1024:
            logger.warning(f"Skipping oversized PDF ({content_length} bytes): {url[:80]}")
            return None

        try:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                tmp.flush()

                with pdfplumber.open(tmp.name) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            pages_text.append(page_text)
                        try:
                            page.flush_cache(); page.get_textmap.cache_clear()
                        except Exception:
                            pass

                    text = "\n\n".join(pages_text)

            # Clean up text
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            text = text.strip()

            return text if len(text) >= 50 else None

        except Exception as e:
            logger.warning(f"PDF extraction failed for {url[:80]}: {e}")
            return None

    def _parse_date_from_filename(self, filename: str) -> Optional[str]:
        """Extract date from PDF filename patterns like '101.02.06' or text like 'du-18-fevrier-2006'."""
        decoded = unquote(filename)

        # Pattern: DD-month-YYYY in filename
        m = re.search(
            r"(\d{1,2})[-_\s]*(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|"
            r"septembre|octobre|novembre|d[eé]cembre)[-_\s]*(\d{4})",
            decoded, re.IGNORECASE,
        )
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            # Normalize accents
            for accented, plain in [("é", "e"), ("û", "u")]:
                month_name = month_name.replace(accented, plain)
            year = int(m.group(3))
            month = FRENCH_MONTHS.get(month_name, "01")
            if 1800 <= year <= 2030 and 1 <= day <= 31:
                return f"{year}-{month}-{day:02d}"

        # Pattern: just a 4-digit year
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", decoded)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def _parse_date_from_text(self, link_text: str) -> Optional[str]:
        """Extract date from link text."""
        # Pattern: DD month YYYY
        m = re.search(
            r"(\d{1,2})\s+(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|"
            r"septembre|octobre|novembre|d[eé]cembre)\s+(\d{4})",
            link_text, re.IGNORECASE,
        )
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            for accented, plain in [("é", "e"), ("û", "u")]:
                month_name = month_name.replace(accented, plain)
            year = int(m.group(3))
            month = FRENCH_MONTHS.get(month_name, "01")
            if 1800 <= year <= 2030 and 1 <= day <= 31:
                return f"{year}-{month}-{day:02d}"

        # Just year
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", link_text)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def _extract_document(self, doc_info: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download and extract a single PDF document."""
        url = doc_info["url"]
        link_text = doc_info.get("link_text", "")

        text = self._extract_pdf_text(url)
        if not text:
            return None

        # Parse filename from URL
        filename = url.split("/")[-1]
        decoded_filename = unquote(filename)

        # Build title: if link text is just a code number (e.g. "101.02.06"),
        # use the filename which has the descriptive title
        is_just_code = bool(re.match(r"^[\d.]+$", link_text.strip()))
        if is_just_code or not link_text:
            # Parse title from filename: "101.02.06-Constitution-18-fevrier-2006.pdf"
            name_part = re.sub(r"\.(pdf|PDF)$", "", decoded_filename)
            # Remove the leading RS code (digits and dots)
            name_part = re.sub(r"^[\d.]+[-_\s]*", "", name_part)
            title = name_part.replace("-", " ").replace("_", " ")
            if not title:
                title = decoded_filename.replace("-", " ").replace(".pdf", "").replace(".PDF", "")
        else:
            title = link_text
        title = re.sub(r"\s+", " ", title).strip()

        # Parse date
        date = self._parse_date_from_filename(decoded_filename)
        if not date:
            date = self._parse_date_from_text(link_text)

        # Generate stable document ID from URL path
        url_path = url.replace(BASE_URL, "")
        doc_id = hashlib.md5(url_path.encode("utf-8")).hexdigest()[:12]

        return {
            "document_id": f"CD-DC-{doc_id}",
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "pdf_filename": decoded_filename,
            "data_type": doc_info.get("data_type", "legislation"),
            "category": doc_info.get("category", ""),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("document_id", ""),
            "_source": "CD/DroitCongolais",
            "_type": raw.get("data_type", "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
            "pdf_filename": raw.get("pdf_filename", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents."""
        documents = self._discover_all_documents()
        count = 0
        failed = 0

        for doc_info in documents:
            doc = self._extract_document(doc_info)
            if doc is None:
                failed += 1
                continue

            if doc.get("text"):
                count += 1
                yield doc

        logger.info(f"Completed: {count} documents fetched, {failed} failed")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Static site — re-fetch all."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test index page
        resp = self._request(f"{BASE_URL}/{LEGISLATION_PAGES[0]}")
        if resp is None:
            logger.error("Cannot reach droitcongolais.info")
            return False

        soup = BeautifulSoup(resp.text, "html.parser")
        pdf_links = [a for a in soup.find_all("a", href=True)
                     if a["href"].lower().endswith(".pdf")]
        logger.info(f"Index page OK: {len(pdf_links)} PDF links found")

        if not pdf_links:
            logger.error("No PDF links found on index page")
            return False

        # Test PDF extraction
        first_pdf_url = urljoin(f"{BASE_URL}/{LEGISLATION_PAGES[0]}", pdf_links[0]["href"])
        text = self._extract_pdf_text(first_pdf_url)
        if text:
            logger.info(f"PDF extraction OK: {len(text)} chars from {pdf_links[0]['href'][:60]}")
            return True

        logger.error("PDF extraction failed")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CD/DroitCongolais data fetcher")
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

    scraper = DroitCongolaisScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
