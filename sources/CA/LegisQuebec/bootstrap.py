#!/usr/bin/env python3
"""
CA/LegisQuebec -- Quebec Provincial Legislation Fetcher

Fetches consolidated Quebec statutes (CQLR) and regulations from the official
LegisQuebec portal (legisquebec.gouv.qc.ca).

Strategy:
  - Enumerate all statutes/regulations via alphabetical index pages
  - Fetch full text HTML for each document
  - Strip HTML to plain text

URL patterns:
  Index: /fr/chapitres?corpus=lois&selection={LETTER}&langCont=en
  Full text: /en/document/cs/{CODE} (statutes) or /en/document/cr/{CODE} (regs)
  No auth required.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import string
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Generator, Optional
from urllib.parse import quote, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.LegisQuebec")

SITE_BASE = "https://www.legisquebec.gouv.qc.ca"


class HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text, skipping hidden/history elements."""

    def __init__(self):
        super().__init__()
        self._text = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)
        cls = attr_dict.get("class", "")
        if tag in ("script", "style"):
            self._skip_depth += 1
        elif "Hidden" in cls or "HistoryLink" in cls:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip_depth = max(0, self._skip_depth - 1)
        elif self._skip_depth > 0:
            self._skip_depth = max(0, self._skip_depth - 1)
        if tag in ("p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "td"):
            self._text.append("\n")

    def handle_data(self, data):
        if self._skip_depth == 0:
            self._text.append(data)

    def get_text(self):
        text = "".join(self._text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()


def html_to_text(html_content: str) -> str:
    """Convert HTML content to plain text."""
    if not html_content:
        return ""
    extractor = HTMLTextExtractor()
    try:
        extractor.feed(html_content)
        return extractor.get_text()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html_content)
        text = html_module.unescape(text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()


class LegisQuebecScraper(BaseScraper):
    """
    Scraper for CA/LegisQuebec -- Quebec codified legislation.
    Country: CA
    URL: https://www.legisquebec.gouv.qc.ca/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=SITE_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )

    def _enumerate_index(self, corpus: str = "lois", doc_prefix: str = "lc") -> list:
        """Enumerate all documents from alphabetical index pages.

        Args:
            corpus: 'lois' for statutes, 'regs' for regulations
            doc_prefix: 'lc' for statutes, 'rc' for regulations
        """
        all_entries = []
        seen_codes = set()

        for letter in string.ascii_uppercase:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(
                    f"/fr/chapitres",
                    params={
                        "corpus": corpus,
                        "selection": letter,
                        "langCont": "en",
                    },
                )
                resp.raise_for_status()
                page_html = resp.text
            except Exception as e:
                logger.warning(f"Failed to fetch index for letter {letter}: {e}")
                continue

            # Extract entries: links to /fr/document/{doc_prefix}/{CODE}?langCont=en
            pattern = rf'href="/fr/document/{doc_prefix}/([^"?]+)\?langCont=en"[^>]*>([^<]+)</a>'
            entries = re.findall(pattern, page_html)

            added = 0
            for code_raw, title in entries:
                code = unquote(code_raw).strip()
                if code in seen_codes:
                    continue
                # Skip entries that don't start with the current letter
                # (the server falls back to showing 'A' for letters with no entries)
                if not code.upper().startswith(letter):
                    continue
                seen_codes.add(code)
                all_entries.append({
                    "code": code,
                    "title": title.strip(),
                    "corpus": corpus,
                    "doc_prefix": doc_prefix,
                })
                added += 1

            if added > 0:
                logger.info(f"  {corpus} letter {letter}: {added} entries")

        return all_entries

    def _fetch_full_text(self, doc_type: str, code: str) -> Optional[str]:
        """Fetch full text HTML for a document.

        Args:
            doc_type: 'cs' for statutes, 'cr' for regulations
            code: chapter code (e.g., 'C-12', 'CCQ-1991')
        """
        self.rate_limiter.wait()
        try:
            encoded_code = quote(code, safe="")
            resp = self.client.get(f"/en/document/{doc_type}/{encoded_code}")
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {doc_type}/{code}: {e}")
            return None

    def _extract_document_text(self, page_html: str) -> str:
        """Extract the law text from a document page HTML."""
        # Find the sections area - from first section to end of content
        first_section = re.search(r'<div\s+class="section" id="se:', page_html)
        if first_section:
            content_html = page_html[first_section.start():]
            # Find the end of the document content (footer area)
            footer = re.search(r'<footer|<div[^>]+class="[^"]*footer', content_html)
            if footer:
                content_html = content_html[:footer.start()]
            return html_to_text(content_html)

        # Fallback: try to get text from the whole page body
        body_match = re.search(r'<body[^>]*>(.*)</body>', page_html, re.DOTALL)
        if body_match:
            return html_to_text(body_match.group(1))

        return html_to_text(page_html)

    def _extract_date(self, page_html: str) -> Optional[str]:
        """Extract the currency/current-to date from a document page."""
        # Look for date patterns like "À jour au 2026-03-31" or "Current to"
        date_match = re.search(
            r'class="[^"]*date-txt-statut[^"]*"[^>]*>(\d{4})</span>',
            page_html,
        )
        if date_match:
            return date_match.group(1)

        # Try another pattern: "statut" date section
        date_match = re.search(
            r'(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre|January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            page_html,
        )
        if date_match:
            month_map = {
                "janvier": "01", "février": "02", "mars": "03", "avril": "04",
                "mai": "05", "juin": "06", "juillet": "07", "août": "08",
                "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
                "january": "01", "february": "02", "march": "03", "april": "04",
                "may": "05", "june": "06", "july": "07", "august": "08",
                "september": "09", "october": "10", "november": "11", "december": "12",
            }
            day = date_match.group(1).zfill(2)
            month = month_map.get(date_match.group(2).lower(), "01")
            year = date_match.group(3)
            return f"{year}-{month}-{day}"

        return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        code = raw.get("code", "")
        corpus = raw.get("corpus", "lois")
        doc_type = "cs" if corpus == "lois" else "cr"

        encoded_code = quote(code, safe="")
        url = f"{SITE_BASE}/en/document/{doc_type}/{encoded_code}"

        return {
            "_id": f"CA-QC-{doc_type}-{code}",
            "_source": "CA/LegisQuebec",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "chapter_code": code,
            "corpus": corpus,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Quebec statutes and regulations with full text."""
        logger.info("Starting LegisQuebec fetch...")

        # Enumerate statutes
        logger.info("Enumerating statutes...")
        statutes = self._enumerate_index("lois", "lc")
        logger.info(f"Found {len(statutes)} statutes")

        # Enumerate regulations
        logger.info("Enumerating regulations...")
        regulations = self._enumerate_index("regs", "rc")
        logger.info(f"Found {len(regulations)} regulations")

        all_docs = statutes + regulations

        for i, doc in enumerate(all_docs):
            code = doc["code"]
            corpus = doc["corpus"]
            doc_type = "cs" if corpus == "lois" else "cr"

            logger.info(f"  [{i+1}/{len(all_docs)}] Fetching {doc_type}/{code}...")

            page_html = self._fetch_full_text(doc_type, code)
            if not page_html:
                continue

            text = self._extract_document_text(page_html)
            if not text or len(text) < 50:
                logger.warning(f"  {doc_type}/{code}: text too short ({len(text) if text else 0} chars), skipping")
                continue

            doc["text"] = text
            doc["date"] = self._extract_date(page_html)

            yield doc

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch documents updated since a given date."""
        yield from self.fetch_all()

    def test_api(self):
        """Quick connectivity test."""
        logger.info("Testing LegisQuebec connectivity...")

        # Test statute index
        self.rate_limiter.wait()
        resp = self.client.get(
            "/fr/chapitres",
            params={"corpus": "lois", "selection": "C", "langCont": "en"},
        )
        logger.info(f"Statute index (letter C): HTTP {resp.status_code}")

        entries = re.findall(
            r'href="/fr/document/lc/([^"?]+)\?langCont=en"[^>]*>([^<]+)</a>',
            resp.text,
        )
        logger.info(f"  Found {len(entries)} statutes for letter C")

        if entries:
            code, title = entries[0]
            code = unquote(code)
            logger.info(f"  First: {code} - {title.strip()}")

            # Test full text fetch
            self.rate_limiter.wait()
            encoded = quote(code, safe="")
            resp2 = self.client.get(f"/en/document/cs/{encoded}")
            logger.info(f"  Full text fetch: HTTP {resp2.status_code}, {len(resp2.text)} bytes")

            text = self._extract_document_text(resp2.text)
            logger.info(f"  Extracted text: {len(text)} chars")
            logger.info(f"  Preview: {text[:200]}...")


if __name__ == "__main__":
    scraper = LegisQuebecScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|test-api]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        scraper.test_api()
    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
