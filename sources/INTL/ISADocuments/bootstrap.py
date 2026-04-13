#!/usr/bin/env python3
"""
INTL/ISADocuments -- International Seabed Authority Documents & National Legislation

Fetches ISA official documents (Council decisions, Assembly resolutions, LTC reports)
via the WordPress REST API, plus national deep-sea mining legislation from member states.

Strategy:
  - WP REST API /wp-json/wp/v2/documents for session documents (~400 docs)
  - Scrape national-legislation-database page for country PDF links
  - Download PDFs and extract text with pypdf
  - Each document page lists PDF attachments in multiple languages (EN preferred)

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap            # Full fetch all documents
    python bootstrap.py test                 # Quick connectivity test
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

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
logger = logging.getLogger("legal-data-hunter.INTL.ISADocuments")

API_URL = "https://isa.org.jm/wp-json/wp/v2/documents"
NATL_LEG_URL = "https://isa.org.jm/national-legislation-database/"
RATE_LIMIT = 2


class ISADocumentsScraper(BaseScraper):
    """Scraper for INTL/ISADocuments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json, text/html, */*",
        })

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/ISADocuments",
            source_id="",
            pdf_url=url,
            table="legislation",
        ) or ""

    def _get_pdf_urls_from_doc_page(self, page_url: str) -> list[str]:
        """Scrape a document page for PDF attachment URLs, preferring English."""
        try:
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch doc page {page_url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        pdf_links = []
        en_links = []

        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.lower().endswith(".pdf"):
                full_url = href if href.startswith("http") else f"https://isa.org.jm{href}"
                pdf_links.append(full_url)
                # Check for English indicator
                link_text = a.get_text(strip=True).upper()
                if link_text in ("EN", "E") or "_E." in href.upper() or "_E/" in href.upper():
                    en_links.append(full_url)

        # Prefer English PDFs, otherwise return all
        return en_links if en_links else pdf_links[:1]

    def _fetch_wp_documents(self) -> list[dict]:
        """Fetch all documents from the WP REST API."""
        all_docs = []
        page = 1
        while True:
            params = {"per_page": 100, "page": page}
            try:
                resp = self.session.get(API_URL, params=params, timeout=30)
                if resp.status_code == 400:
                    break  # No more pages
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"API page {page} failed: {e}")
                break

            docs = resp.json()
            if not docs:
                break

            all_docs.extend(docs)
            logger.info(f"API page {page}: {len(docs)} documents (total: {len(all_docs)})")
            page += 1
            time.sleep(1)

        return all_docs

    def _fetch_national_legislation(self) -> list[dict]:
        """Scrape national legislation database page for PDF links."""
        try:
            resp = self.session.get(NATL_LEG_URL, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch national legislation page: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        entries = []

        # Find country sections - look for headings followed by links
        # The page has country names as bold text or headings with PDF links
        content = soup.find("div", class_="entry-content") or soup.find("article") or soup.find("main")
        if not content:
            content = soup

        # Find all PDF links on the page
        for a in content.find_all("a", href=True):
            href = a.get("href", "")
            if not href.lower().endswith(".pdf"):
                continue

            full_url = href if href.startswith("http") else f"https://isa.org.jm{href}"
            link_text = a.get_text(strip=True)

            # Try to find the country context
            country = ""
            parent = a.parent
            for _ in range(5):
                if parent is None:
                    break
                prev = parent.find_previous(["h2", "h3", "h4", "strong", "b"])
                if prev:
                    country = prev.get_text(strip=True)
                    break
                parent = parent.parent

            entries.append({
                "url": full_url,
                "title": link_text or f"National legislation ({country})",
                "country_name": country,
                "doc_type": "national_legislation",
            })

        logger.info(f"Found {len(entries)} national legislation PDFs")
        return entries

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ISA documents with full text."""
        # Part 1: WP REST API documents
        wp_docs = self._fetch_wp_documents()
        for doc in wp_docs:
            wp_id = doc.get("id", "")
            title_raw = doc.get("title", {})
            title = title_raw.get("rendered", "") if isinstance(title_raw, dict) else str(title_raw)
            title = BeautifulSoup(title, "html.parser").get_text(strip=True)
            link = doc.get("link", "")
            date_str = doc.get("date", "")

            # Get PDF URLs from the document page
            time.sleep(RATE_LIMIT)
            pdf_urls = self._get_pdf_urls_from_doc_page(link)

            if not pdf_urls:
                logger.warning(f"No PDFs for {title} ({link})")
                continue

            # Extract text from the first (English) PDF
            text = self._extract_pdf_text(pdf_urls[0])
            if not text:
                logger.warning(f"No text extracted from PDF for {title}")
                continue

            yield {
                "wp_id": wp_id,
                "title": title,
                "link": link,
                "date": date_str,
                "text": text,
                "pdf_url": pdf_urls[0],
                "doc_type": "session_document",
            }

        # Part 2: National legislation
        natl_entries = self._fetch_national_legislation()
        for entry in natl_entries:
            time.sleep(RATE_LIMIT)
            text = self._extract_pdf_text(entry["url"])
            if not text:
                logger.warning(f"No text from national legislation: {entry['title']}")
                continue

            yield {
                "title": entry["title"],
                "link": entry["url"],
                "text": text,
                "pdf_url": entry["url"],
                "country_name": entry.get("country_name", ""),
                "doc_type": "national_legislation",
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents modified since a given date."""
        wp_docs = self._fetch_wp_documents()
        for doc in wp_docs:
            modified = doc.get("modified", "")
            if modified and modified >= since.isoformat():
                wp_id = doc.get("id", "")
                title_raw = doc.get("title", {})
                title = title_raw.get("rendered", "") if isinstance(title_raw, dict) else str(title_raw)
                title = BeautifulSoup(title, "html.parser").get_text(strip=True)
                link = doc.get("link", "")

                time.sleep(RATE_LIMIT)
                pdf_urls = self._get_pdf_urls_from_doc_page(link)
                if not pdf_urls:
                    continue

                text = self._extract_pdf_text(pdf_urls[0])
                if not text:
                    continue

                yield {
                    "wp_id": wp_id,
                    "title": title,
                    "link": link,
                    "date": doc.get("date", ""),
                    "text": text,
                    "pdf_url": pdf_urls[0],
                    "doc_type": "session_document",
                }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw ISA document into standardized schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "Unknown ISA Document")
        doc_type = raw.get("doc_type", "session_document")

        # Extract ISBA document symbol from title
        doc_symbol = ""
        m = re.search(r'(ISBA/\d+/[A-Z]+(?:/\S+)?)', title)
        if m:
            doc_symbol = m.group(1)
        elif not doc_symbol:
            # Try from text header
            m = re.search(r'(ISBA/\d+/[A-Z]+(?:/\S+)?)', text[:500])
            if m:
                doc_symbol = m.group(1)

        # Parse date
        date = raw.get("date", "")
        if date:
            date = date[:10]  # Keep YYYY-MM-DD

        # Build ID
        if doc_symbol:
            safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_symbol)
        elif doc_type == "national_legislation":
            country = raw.get("country_name", "unknown")
            safe_country = re.sub(r'[^a-zA-Z0-9_-]', '_', country)[:30]
            safe_title = re.sub(r'[^a-zA-Z0-9_-]', '_', title)[:50]
            safe_id = f"NATL-{safe_country}-{safe_title}"
        else:
            safe_id = f"WP-{raw.get('wp_id', 'unknown')}"

        url = raw.get("link", raw.get("pdf_url", ""))

        # Clean text: remove excessive whitespace but keep structure
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = text.strip()

        return {
            "_id": f"INTL-ISA-{safe_id}",
            "_source": "INTL/ISADocuments",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date or None,
            "url": url,
            "doc_symbol": doc_symbol,
            "doc_type": doc_type,
            "country_name": raw.get("country_name", ""),
        }


if __name__ == "__main__":
    scraper = ISADocumentsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        print("Testing ISA Documents connectivity...")
        try:
            resp = scraper.session.get(API_URL, params={"per_page": 1}, timeout=15)
            resp.raise_for_status()
            docs = resp.json()
            print(f"OK: WP API returned {len(docs)} document(s)")

            if docs:
                title = docs[0].get("title", {}).get("rendered", "?")
                link = docs[0].get("link", "")
                print(f"  First: {title}")
                pdf_urls = scraper._get_pdf_urls_from_doc_page(link)
                if pdf_urls:
                    text = scraper._extract_pdf_text(pdf_urls[0])
                    if text:
                        print(f"  PDF text: {len(text)} chars")
                    else:
                        print("  WARN: Could not extract PDF text")
                else:
                    print("  WARN: No PDF links on document page")

            resp2 = scraper.session.get(NATL_LEG_URL, timeout=15)
            resp2.raise_for_status()
            print(f"OK: National legislation page: {resp2.status_code}")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample else None

        for raw in scraper.fetch_all():
            normalized = scraper.normalize(raw)
            if normalized is None:
                continue

            count += 1
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            logger.info(
                f"Saved {normalized.get('doc_symbol') or normalized['title'][:40]} "
                f"({count} total, {len(normalized['text'])} chars)"
            )

            if limit and count >= limit:
                break

        print(f"Saved {count} records to {sample_dir}/")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
