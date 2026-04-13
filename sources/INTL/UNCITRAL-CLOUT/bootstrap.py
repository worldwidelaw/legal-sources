#!/usr/bin/env python3
"""
INTL/UNCITRAL-CLOUT -- UNCITRAL Case Law on Texts (CLOUT)

Fetches case abstracts from the UNCITRAL CLOUT database.

Strategy:
  - Paginate search results to discover all case URLs
  - Fetch each case detail page for metadata + abstract PDF link
  - Download abstract compilation PDFs from UN ODS
  - Extract and split abstract text by case number
  - ~2,255 cases from 92 countries

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UNCITRAL-CLOUT")

SEARCH_URL = "https://www.uncitral.org/clout/search.jspx?inline=true&start={offset}&lng=en"
DETAIL_BASE = "https://www.uncitral.org"


class UNCITRALCLOUTScraper(BaseScraper):
    """
    Scraper for INTL/UNCITRAL-CLOUT -- UNCITRAL Case Law on Texts.
    Country: INTL
    URL: https://www.uncitral.org/clout/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        # Cache: pdf_jn -> {case_number: abstract_text}
        self._pdf_cache: dict[str, dict[int, str]] = {}

    # ------------------------------------------------------------------
    # Phase 1: Discover case URLs from search pagination
    # ------------------------------------------------------------------
    def _get_case_urls(self, max_pages: Optional[int] = None) -> list[str]:
        """Paginate search results and extract case detail URLs."""
        urls = []
        offset = 0
        page = 0
        while True:
            if max_pages and page >= max_pages:
                break
            url = SEARCH_URL.format(offset=offset)
            logger.info("Fetching search page offset=%d", offset)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            # Extract case URLs from onclick attributes
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select("a[onclick]")
            page_urls = []
            for link in links:
                onclick = link.get("onclick", "")
                m = re.search(r"document\.location='([^']+)'", onclick)
                if m:
                    path = m.group(1)
                    if "clout_case" in path:
                        full_url = DETAIL_BASE + path
                        page_urls.append(full_url)

            if not page_urls:
                break

            urls.extend(page_urls)
            logger.info("  Found %d cases (total: %d)", len(page_urls), len(urls))
            offset += 10
            page += 1
            time.sleep(1)

        return urls

    # ------------------------------------------------------------------
    # Phase 2: Fetch case detail page metadata
    # ------------------------------------------------------------------
    def _parse_case_detail(self, url: str) -> Optional[dict]:
        """Fetch and parse a case detail page for metadata."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        data = {"url": url}

        # Extract case number from URL
        m = re.search(r"clout_case_(\d+)", url)
        if m:
            data["case_number"] = int(m.group(1))

        # Parse cloutDetailItem elements
        items = soup.select(".cloutDetailItem")
        for item in items:
            text = item.get_text(" ", strip=True)

            if "Legislative text" in text:
                content = text.replace("Legislative text", "", 1).strip()
                data["legislative_text"] = content
            elif "Clout issue" in text:
                m2 = re.search(r"(\d+)", text)
                if m2:
                    data["clout_issue"] = int(m2.group(1))
            elif "Articles" in text:
                data["articles"] = text.replace("Articles", "", 1).strip()
            elif "Country" in text:
                country_div = item.select_one(".textPlain")
                if country_div:
                    data["country"] = country_div.get_text(strip=True)
                else:
                    data["country"] = text.replace("Country", "", 1).strip()
            elif "Court name" in text:
                data["court_name"] = text.replace("Court name", "", 1).strip()
            elif "Court reference" in text:
                data["court_reference"] = text.replace("Court reference", "", 1).strip()
            elif "Parties" in text:
                data["parties"] = text.replace("Parties", "", 1).strip()
            elif "Decision date" in text:
                date_text = text.replace("Decision date", "", 1).strip()
                data["decision_date_raw"] = date_text
                # Parse dd/mm/yyyy
                m3 = re.search(r"(\d{2})/(\d{2})/(\d{4})", date_text)
                if m3:
                    day, month, year = m3.groups()
                    try:
                        data["date"] = f"{year}-{month}-{day}"
                    except ValueError:
                        pass
            elif "Comments" in text:
                data["comments"] = text.replace("Comments", "", 1).strip()
            elif "Keywords" in text:
                data["keywords"] = text.replace("Keywords", "", 1).strip()

        # Find PDF links
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            # Direct case PDF (full court decision)
            if "/res/clout/" in href and href.endswith(".pdf#"):
                data["direct_pdf_url"] = "https://www.uncitral.org" + href.rstrip("#")
            elif "/res/clout/" in href and href.endswith(".pdf"):
                data["direct_pdf_url"] = "https://www.uncitral.org" + href
            # Compilation abstract PDF (JN= style)
            elif "daccess-ods.un.org" in href and "JN=" in href:
                data["pdf_url"] = href
                m4 = re.search(r"JN=([a-zA-Z0-9]+)", href)
                if m4:
                    data["pdf_jn"] = m4.group(1)
            # Compilation abstract PDF (OpenAgent&DS= style)
            elif "daccess-ods.un.org" in href and "DS=" in href:
                data["pdf_url"] = href
                # Extract DS code for caching
                m4 = re.search(r"DS=([^&]+)", href)
                if m4:
                    data["pdf_ds"] = m4.group(1)

        return data

    # ------------------------------------------------------------------
    # Phase 3: Download PDF and extract abstracts per case
    # ------------------------------------------------------------------
    def _extract_abstracts_from_pdf(self, pdf_jn: str) -> dict[int, str]:
        """Download a CLOUT issue PDF and extract per-case abstracts."""
        if pdf_jn in self._pdf_cache:
            return self._pdf_cache[pdf_jn]

        pdf_url = f"http://daccess-ods.un.org/access.nsf/Get?Open&JN={pdf_jn}"
        logger.info("Downloading PDF: %s", pdf_jn)

        try:
            resp = self.session.get(pdf_url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to download PDF %s: %s", pdf_jn, e)
            self._pdf_cache[pdf_jn] = {}
            return {}

        # Extract text from PDF
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(resp.content))
            full_text = ""
            for page in reader.pages:
                full_text += page.extract_text() + "\n"
        except Exception as e:
            logger.warning("Failed to parse PDF %s: %s", pdf_jn, e)
            self._pdf_cache[pdf_jn] = {}
            return {}

        abstracts = self._split_abstracts(full_text)
        logger.info("  Extracted %d case abstracts from PDF %s", len(abstracts), pdf_jn)
        self._pdf_cache[pdf_jn] = abstracts
        return abstracts

    def _clean_pdf_text(self, text: str) -> str:
        """Clean up PDF-extracted text."""
        # Remove common PDF artifacts
        # Remove page numbers and document references
        text = re.sub(r"\n\s*\d+/\d+\s*\n", "\n", text)
        text = re.sub(r"V\.\d{2}\s*-\d+\s*", "", text)
        text = re.sub(r"A/CN\.9/SER\.C/ABSTRACTS/\d+\s*", "", text)
        # Normalize whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    # ------------------------------------------------------------------
    # Phase 3b: Download direct case PDF (full court decision)
    # ------------------------------------------------------------------
    def _extract_direct_pdf(self, url: str) -> str:
        """Download a direct case PDF and extract text."""
        try:
            resp = self.session.get(url, timeout=60, allow_redirects=True)
            if resp.status_code != 200:
                return ""
            reader = PyPDF2.PdfReader(io.BytesIO(resp.content))
            text = ""
            for page in reader.pages:
                text += page.extract_text() + "\n"
            return self._clean_pdf_text(text)
        except Exception as e:
            logger.debug("Failed to extract direct PDF %s: %s", url, e)
            return ""

    # ------------------------------------------------------------------
    # Phase 3c: Download compilation PDF via DS= URL
    # ------------------------------------------------------------------
    def _extract_abstracts_from_ds(self, pdf_url: str, ds_key: str) -> dict[int, str]:
        """Download a compilation PDF via OpenAgent DS= URL."""
        cache_key = f"ds:{ds_key}"
        if cache_key in self._pdf_cache:
            return self._pdf_cache[cache_key]

        logger.info("Downloading compilation PDF: %s", ds_key)
        try:
            resp = self.session.get(pdf_url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to download DS PDF %s: %s", ds_key, e)
            self._pdf_cache[cache_key] = {}
            return {}

        try:
            reader = PyPDF2.PdfReader(io.BytesIO(resp.content))
            full_text = ""
            for page in reader.pages:
                full_text += page.extract_text() + "\n"
        except Exception as e:
            logger.warning("Failed to parse DS PDF %s: %s", ds_key, e)
            self._pdf_cache[cache_key] = {}
            return {}

        abstracts = self._split_abstracts(full_text)
        logger.info("  Extracted %d case abstracts from DS PDF %s", len(abstracts), ds_key)
        self._pdf_cache[cache_key] = abstracts
        return abstracts

    def _split_abstracts(self, full_text: str) -> dict[int, str]:
        """Split compilation PDF text into per-case abstracts."""
        abstracts = {}
        parts = re.split(r"(?=\bCase\s+(\d+)\s*[:\.])", full_text)
        i = 0
        while i < len(parts):
            if i + 2 < len(parts):
                try:
                    case_num = int(parts[i + 1])
                    abstract_text = parts[i + 2].strip()
                    abstract_text = self._clean_pdf_text(abstract_text)
                    if len(abstract_text) > 50:
                        abstracts[case_num] = abstract_text
                    i += 2
                except (ValueError, IndexError):
                    i += 1
            else:
                i += 1
        return abstracts

    # ------------------------------------------------------------------
    # Phase 4: Build normalized records
    # ------------------------------------------------------------------
    def _build_record(self, case_data: dict) -> Optional[dict]:
        """Build a normalized record from case metadata + extracted abstract."""
        case_num = case_data.get("case_number")
        if not case_num:
            return None

        # Strategy: try direct case PDF first, then compilation abstract
        text = ""

        # 1. Try direct case PDF (full court decision)
        direct_pdf = case_data.get("direct_pdf_url")
        if direct_pdf:
            text = self._extract_direct_pdf(direct_pdf)
            if text:
                logger.debug("Got full text from direct PDF for case %d (%d chars)", case_num, len(text))

        # 2. Fall back to compilation abstract PDF (JN= style)
        if not text:
            pdf_jn = case_data.get("pdf_jn")
            if pdf_jn:
                abstracts = self._extract_abstracts_from_pdf(pdf_jn)
                text = abstracts.get(case_num, "")

        # 3. Fall back to compilation abstract PDF (DS= style)
        if not text:
            pdf_ds = case_data.get("pdf_ds")
            pdf_url = case_data.get("pdf_url")
            if pdf_ds and pdf_url:
                abstracts = self._extract_abstracts_from_ds(pdf_url, pdf_ds)
                text = abstracts.get(case_num, "")

        # Build title
        title = f"CLOUT case {case_num}"
        if case_data.get("parties") and case_data["parties"] != "-":
            title += f" - {case_data['parties']}"

        return {
            "_id": f"CLOUT-{case_num}",
            "_source": "INTL/UNCITRAL-CLOUT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": case_data.get("date"),
            "url": case_data.get("url", ""),
            "country": case_data.get("country", ""),
            "court_name": case_data.get("court_name", ""),
            "court_reference": case_data.get("court_reference", ""),
            "parties": case_data.get("parties", ""),
            "legislative_text": case_data.get("legislative_text", ""),
            "clout_issue": case_data.get("clout_issue"),
            "articles": case_data.get("articles", ""),
            "keywords": case_data.get("keywords", ""),
            "comments": case_data.get("comments", ""),
            "pdf_url": case_data.get("pdf_url", ""),
        }

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------
    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all CLOUT case abstracts. Yields records as pages are discovered."""
        logger.info("Starting full fetch of UNCITRAL CLOUT database")
        offset = 0
        total = 0
        while True:
            url = SEARCH_URL.format(offset=offset)
            logger.info("Fetching search page offset=%d", offset)
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning("Search page failed at offset %d: %s", offset, e)
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select("a[onclick]")
            page_urls = []
            for link in links:
                onclick = link.get("onclick", "")
                m = re.search(r"document\.location='([^']+)'", onclick)
                if m and "clout_case" in m.group(1):
                    page_urls.append(DETAIL_BASE + m.group(1))

            if not page_urls:
                break

            total += len(page_urls)
            logger.info("  Found %d cases (total discovered: %d)", len(page_urls), total)

            for case_url in page_urls:
                case_data = self._parse_case_detail(case_url)
                if not case_data:
                    continue
                record = self._build_record(case_data)
                if record and record.get("text"):
                    yield record
                elif record:
                    logger.warning("No abstract text for case %s", record["_id"])
                    yield record
                time.sleep(1)

            offset += 10
            time.sleep(1)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent CLOUT cases (first few pages of results)."""
        logger.info("Fetching updates since %s", since)
        case_urls = self._get_case_urls(max_pages=5)
        logger.info("Checking %d recent case URLs", len(case_urls))

        since_date = datetime.fromisoformat(since).date() if since else None

        for url in case_urls:
            case_data = self._parse_case_detail(url)
            if not case_data:
                continue

            record = self._build_record(case_data)
            if not record:
                continue

            # Filter by date if possible
            if since_date and record.get("date"):
                try:
                    rec_date = datetime.fromisoformat(record["date"]).date()
                    if rec_date < since_date:
                        continue
                except ValueError:
                    pass

            yield record
            time.sleep(1)

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record (already normalized during build)."""
        return raw


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/UNCITRAL-CLOUT data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = UNCITRALCLOUTScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            urls = scraper._get_case_urls(max_pages=1)
            logger.info("OK: %d case URLs from first page", len(urls))
            if urls:
                case_data = scraper._parse_case_detail(urls[0])
                if case_data:
                    logger.info("Case %s: %s (%s)",
                                case_data.get("case_number"),
                                case_data.get("court_name", "?"),
                                case_data.get("country", "?"))
                    if case_data.get("pdf_jn"):
                        abstracts = scraper._extract_abstracts_from_pdf(case_data["pdf_jn"])
                        case_num = case_data["case_number"]
                        if case_num in abstracts:
                            logger.info("Abstract text: %d chars", len(abstracts[case_num]))
                            logger.info("Preview: %s", abstracts[case_num][:200])
                        else:
                            logger.warning("Case %d not found in PDF (found: %s)",
                                           case_num, list(abstracts.keys())[:5])
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error("Connectivity test failed: %s", e)
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
