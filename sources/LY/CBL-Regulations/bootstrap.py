#!/usr/bin/env python3
"""
LY/CBL-Regulations -- Central Bank of Libya — Laws, Circulars & Decisions

Fetches banking laws, financial legislation, and regulatory circulars
from cbl.gov.ly (WordPress site with PDF uploads).

Strategy:
  1. Scrape /en/laws/ for direct PDF links (wp-content/uploads)
  2. Scrape /en/publications/ paginated for circular PDF links
  3. Download and extract full text from all PDFs

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

# Most of the CBL back-catalogue (the pre-2015 circulars especially) is scanned
# image-only PDF with no text layer at all, so it reaches pdf_extract's OCR
# fallback. That fallback defaults to the English tesseract model, which reads
# an Arabic scan as nothing — every one of those documents came back "all
# backends returned empty" and was skipped as untextable. Selecting the Arabic
# model recovers them. Must be set before common.pdf_extract is imported: it
# snapshots PDF_OCR_LANG into a module-level constant at import time.
os.environ.setdefault("PDF_OCR_LANG", "ara")

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LY.CBL-Regulations")

BASE_URL = "https://cbl.gov.ly"
DELAY = 2.0


def _curl_get(url: str, timeout: int = 60) -> Optional[bytes]:
    """Fetch a URL using curl subprocess."""
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            result = subprocess.run(
                ["curl", "-sL", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                 "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                 url],
                capture_output=True,
                timeout=timeout + 10,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
            logger.warning("curl attempt %d failed for %s (rc=%d)", attempt + 1, url, result.returncode)
        except subprocess.TimeoutExpired:
            logger.warning("curl timeout attempt %d for %s", attempt + 1, url)
        except Exception as e:
            logger.warning("curl attempt %d error for %s: %s", attempt + 1, url, e)
        if attempt < 2:
            time.sleep(5)
    return None


def _curl_get_text(url: str, timeout: int = 60) -> Optional[str]:
    """Fetch URL text content via curl."""
    data = _curl_get(url, timeout)
    if data:
        return data.decode("utf-8", errors="replace")
    return None


class _PDFLinkExtractor(HTMLParser):
    """Extract PDF links from WordPress pages (wp-content/uploads)."""

    def __init__(self):
        super().__init__()
        self.in_link = False
        self.current_url = ""
        self.text_parts: List[str] = []
        self.links: List[Tuple[str, str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href") or ""
            if href.lower().endswith(".pdf"):
                self.in_link = True
                self.current_url = href
                self.text_parts = []

    def handle_data(self, data):
        if self.in_link:
            self.text_parts.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self.in_link:
            title = " ".join(p for p in self.text_parts if p).strip()
            if self.current_url:
                self.links.append((title, self.current_url))
            self.in_link = False


def _normalize_url(url: str) -> str:
    """Normalize URL for deduplication — decode percent-encoding, strip /en/ prefix."""
    decoded = unquote(url)
    # cbl.gov.ly/en/micifaf/... and cbl.gov.ly/micifaf/... are the same
    decoded = re.sub(r"cbl\.gov\.ly/en/", "cbl.gov.ly/", decoded)
    return decoded


def _make_id(url: str) -> str:
    """Create a stable ID from a PDF URL.

    Digested with SHA-256, not the builtin ``hash()``: string hashing is
    salt-randomized per interpreter process (PEP 456), so the previous
    ``abs(hash(slug))`` handed every document a different ``_id`` on every run
    \u2014 each fleet crawl would have re-inserted the whole corpus under fresh
    keys instead of upserting onto the existing rows.
    """
    normalized = _normalize_url(url)
    m = re.search(r"/([^/]+\.pdf)", normalized, re.IGNORECASE)
    if m:
        key = m.group(1)
        key = re.sub(r"\.pdf$", "", key, flags=re.IGNORECASE)
        key = re.sub(r"[^a-zA-Z0-9\u0600-\u06FF]+", "_", key).strip("_")
        if len(key) > 80:
            key = key[:80]
    else:
        key = normalized
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"LY_CBL_{int(digest[:16], 16) % 10**10:010d}"


def _title_from_filename(url: str) -> str:
    """Extract a readable title from a PDF URL."""
    m = re.search(r"/([^/]+\.pdf)", url, re.IGNORECASE)
    if m:
        name = unquote(m.group(1))
        name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[_-]+", " ", name)
        return name.strip()
    return ""


def _extract_year(text: str) -> Optional[str]:
    """Try to extract a year from title or filename."""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        year = int(m.group(1))
        if 1950 <= year <= 2030:
            return f"{year}-01-01"
    return None


_CID_RE = re.compile(r"\(cid:\d+\)")


def _is_undecodable(text: str) -> bool:
    """True when a text layer decoded to placeholders rather than to letters.

    A few CBL PDFs embed a legacy non-Unicode Arabic font with no ToUnicode
    CMap, so the extractors emit `(cid:N)` for most glyphs and mojibake Latin
    for the rest — e.g. `القانون المالي للدولة` came out as 129,422 chars that
    contain no Arabic word at all. That sails past a bare length check and gets
    stored as a statute's full text, but it is exactly as unsearchable as the
    character-reversed text this source was flagged for (#1560), so treat it as
    no text rather than as a long document.
    """
    if not text:
        return True
    return len(_CID_RE.findall(text)) * 7 > 0.25 * len(text)


def _classify_doc(title: str, category: str) -> str:
    """All CBL documents are legislation (laws + regulatory circulars)."""
    return "legislation"


class CBLRegulationsScraper(BaseScraper):
    """Scraper for LY/CBL-Regulations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _scrape_laws_page(self) -> List[Dict[str, Any]]:
        """Scrape /en/laws/ for direct PDF links."""
        url = f"{BASE_URL}/en/laws/"
        html = _curl_get_text(url)
        if html is None:
            logger.warning("Cannot fetch laws page")
            return []

        parser = _PDFLinkExtractor()
        parser.feed(html)

        results = []
        seen = set()
        for anchor_text, href in parser.links:
            full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
            if full_url in seen:
                continue
            seen.add(full_url)

            title = anchor_text.strip() if anchor_text else ""
            if not title or len(title) < 4 or title.lower() == "download":
                title = _title_from_filename(full_url)
            if not title or len(title) < 3:
                continue

            results.append({
                "title": title,
                "url": full_url,
                "category": "Laws",
            })

        logger.info("Laws page: %d PDF links", len(results))
        return results

    def _scrape_publications(self, max_pages: int = 15) -> List[Dict[str, Any]]:
        """Scrape /en/publications/ paginated for circular PDF links."""
        results = []
        seen = set()

        for page in range(1, max_pages + 1):
            url = f"{BASE_URL}/en/publications/" if page == 1 else f"{BASE_URL}/en/publications/?sf_paged={page}"
            html = _curl_get_text(url)
            if html is None:
                break

            parser = _PDFLinkExtractor()
            parser.feed(html)

            if not parser.links:
                logger.info("Publications page %d: no links, stopping", page)
                break

            new_count = 0
            for anchor_text, href in parser.links:
                full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                if full_url in seen:
                    continue
                seen.add(full_url)

                title = anchor_text.strip() if anchor_text else ""
                if not title or len(title) < 4 or title.lower() == "download":
                    title = _title_from_filename(full_url)
                if not title or len(title) < 3:
                    continue

                results.append({
                    "title": title,
                    "url": full_url,
                    "category": "Circulars",
                })
                new_count += 1

            logger.info("Publications page %d: %d new PDFs", page, new_count)

            if new_count == 0:
                break

        logger.info("Total publications: %d", len(results))
        return results

    def _get_all_docs(self) -> List[Dict[str, Any]]:
        """Combine laws and publications, deduplicating by normalized URL."""
        laws = self._scrape_laws_page()
        pubs = self._scrape_publications()

        # Dedup on the same key `_make_id` uses — the PDF filename — not on the
        # full URL. WordPress multisite serves one upload under several alias
        # paths (`/micifaf/2022/07/x.pdf`, `/en/micifaf/sites/4/2022/07/x.pdf`,
        # `/en/wp-content/uploads/2022/07/x.pdf`), which a URL-keyed set treats
        # as three documents: the crawl downloaded and extracted each copy, and
        # since they already share an `_id` two of the three were destined to be
        # discarded at ingest anyway. Keying the list the way the id is keyed
        # keeps the two consistent.
        seen = set()
        all_docs = []
        for doc in laws + pubs:
            key = _make_id(doc["url"])
            if key in seen:
                continue
            seen.add(key)
            all_docs.append(doc)

        logger.info("Total unique documents: %d", len(all_docs))
        return all_docs

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "LY/CBL-Regulations",
            "_type": _classify_doc(title, raw.get("category", "")),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        doc_list = self._get_all_docs()
        if not doc_list:
            logger.error("No documents found")
            return

        count = 0
        for doc in doc_list:
            if max_records and count >= max_records:
                return

            doc_id = _make_id(doc["url"])
            title = doc["title"]
            url = doc["url"]
            logger.info("Downloading [%d/%d]: %s", count + 1, len(doc_list), title[:60])

            pdf_bytes = _curl_get(url, timeout=90)
            if pdf_bytes is None:
                logger.warning("Failed to download: %s", url)
                continue
            if len(pdf_bytes) < 200:
                logger.warning("File too small (%d bytes): %s", len(pdf_bytes), url)
                continue
            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a PDF file: %s", url)
                continue

            try:
                text = extract_pdf_markdown(
                    source="LY/CBL-Regulations",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                )
            except Exception as e:
                logger.warning("Failed to extract %s: %s", url, e)
                continue

            if not text or len(text) < 50:
                logger.warning("Insufficient text (%d chars): %s",
                             len(text or ""), title[:50])
                continue

            if _is_undecodable(text):
                logger.warning(
                    "Undecodable font — text layer is (cid:N) placeholders "
                    "(%d chars): %s", len(text), title[:50])
                continue

            date = _extract_year(title) or _extract_year(url)

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "category": doc.get("category", ""),
            }
            count += 1
            yield raw

        logger.info("Completed: %d documents fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        doc_list = self._get_all_docs()
        if not doc_list:
            logger.error("Cannot fetch any documents")
            return False

        logger.info("Document list OK: %d documents found", len(doc_list))
        cats = {}
        for d in doc_list:
            c = d.get("category", "?")
            cats[c] = cats.get(c, 0) + 1
        for c, n in sorted(cats.items()):
            logger.info("  %s: %d", c, n)

        title, url = doc_list[0]["title"], doc_list[0]["url"]
        logger.info("Testing download: %s", title[:60])
        pdf_bytes = _curl_get(url)
        if pdf_bytes and len(pdf_bytes) > 200:
            logger.info("PDF download OK: %d bytes", len(pdf_bytes))
        else:
            logger.warning("PDF download issue")

        return True


def main():
    parser = argparse.ArgumentParser(description="LY/CBL-Regulations data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBLRegulationsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
