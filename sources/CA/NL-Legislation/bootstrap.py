#!/usr/bin/env python3
"""
CA/NL-Legislation -- Newfoundland and Labrador Consolidated Statutes & Regulations

Fetches all consolidated statutes (~407) and regulations (~1,691) from the
House of Assembly of Newfoundland and Labrador.

Data source:
  Statutes:    https://assembly.nl.ca/legislation/sr/titleindex.htm
  Regulations: https://assembly.nl.ca/legislation/sr/regulations/titleindex2.htm

License: Crown Copyright, Province of Newfoundland and Labrador
Format: Word-generated HTML pages, one per statute/regulation

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import html
import json
import logging
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.NL-Legislation")

SOURCE_ID = "CA/NL-Legislation"

BASE = "https://assembly.nl.ca"
STATUTES_INDEX = f"{BASE}/legislation/sr/titleindex.htm"
REGULATIONS_INDEX = f"{BASE}/legislation/sr/regulations/titleindex2.htm"
STATUTES_BASE = f"{BASE}/Legislation/sr/statutes/"
REGULATIONS_BASE = f"{BASE}/Legislation/sr/regulations/"

DELAY = 1.5
CURL_TIMEOUT = 60


def fetch_page(url: str, retries: int = 2) -> Optional[str]:
    """Fetch a page using curl with retries."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "User-Agent: Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                 "-w", "\n%{http_code}", url],
                capture_output=True, timeout=CURL_TIMEOUT + 10
            )
            try:
                stdout = result.stdout.decode('utf-8')
            except UnicodeDecodeError:
                stdout = result.stdout.decode('windows-1252', errors='replace')

            parts = stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status = parts[0], parts[1].strip()
            else:
                body, status = stdout, "000"

            if status == "404":
                return None
            if not status.startswith("2"):
                if attempt == retries:
                    logger.warning("HTTP %s for %s", status, url)
                    return None
                time.sleep(3)
                continue
            if body:
                return body
            if attempt == retries:
                return None
            time.sleep(3)
        except Exception as e:
            if attempt == retries:
                logger.warning("Failed to fetch %s: %s", url, e)
                return None
            time.sleep(3)
    return None


def strip_html_to_text(raw_html: str) -> str:
    """Strip HTML tags from Word-generated HTML and return clean text."""
    if not raw_html:
        return ""
    text = re.sub(r'<head>.*?</head>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<(?:p|div|h[1-6]|li|tr)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_title_from_html(raw_html: str) -> str:
    """Extract document title from <title> tag."""
    match = re.search(r'<title>(.*?)</title>', raw_html, re.IGNORECASE | re.DOTALL)
    if match:
        title = re.sub(r'<[^>]+>', '', match.group(1)).strip()
        return html.unescape(title)
    return ""


def remove_boilerplate(text: str) -> str:
    """Remove standard Crown copyright boilerplate from extracted text."""
    # Remove opening boilerplate
    markers = [
        "How current is this statute?",
        "How current is this regulation?",
        "Responsible Department",
    ]
    best_idx = -1
    for marker in markers:
        idx = text.find(marker)
        if idx > 0:
            end = text.find('\n', idx)
            if end > 0:
                candidate = end
            else:
                candidate = idx + len(marker)
            if candidate > best_idx:
                best_idx = candidate

    if best_idx > 0 and best_idx < len(text) * 0.3:
        text = text[best_idx:].strip()

    # Remove trailing boilerplate
    for marker in ["©Queen's Printer", "©King's Printer", "King's Printer"]:
        idx = text.rfind(marker)
        if idx > 0 and idx > len(text) * 0.8:
            text = text[:idx].strip()

    return text


def parse_statute_index(index_html: str) -> list[dict]:
    """Parse the statutes title index page to get all statute links."""
    statutes = []
    seen = set()
    pattern = re.compile(
        r'href="[^"]*statutes/([^"]+\.htm)"[^>]*>\s*(.*?)\s*</a>',
        re.IGNORECASE | re.DOTALL
    )
    for match in pattern.finditer(index_html):
        filename = match.group(1).strip()
        title_raw = match.group(2).strip()
        title = re.sub(r'<[^>]+>', '', title_raw).strip()
        title = html.unescape(title)

        if not title or not filename or filename in seen:
            continue
        seen.add(filename)

        doc_id = filename.replace('.htm', '')
        statutes.append({
            "doc_id": doc_id,
            "filename": filename,
            "title": title,
            "url": STATUTES_BASE + filename,
            "doc_type": "statute",
        })
    return statutes


def parse_regulation_index(index_html: str) -> list[dict]:
    """Parse the regulations title index page to get all regulation links."""
    regulations = []
    seen = set()
    pattern = re.compile(
        r'href="[^"]*regulations/([^"]+\.htm)"[^>]*>\s*(.*?)\s*</a>',
        re.IGNORECASE | re.DOTALL
    )
    for match in pattern.finditer(index_html):
        filename = match.group(1).strip()
        title_raw = match.group(2).strip()
        title = re.sub(r'<[^>]+>', '', title_raw).strip()
        title = html.unescape(title)

        # Skip index pages themselves
        if filename.startswith('titleindex'):
            continue
        if not title or not filename or filename in seen:
            continue
        seen.add(filename)

        doc_id = filename.replace('.htm', '')
        regulations.append({
            "doc_id": doc_id,
            "filename": filename,
            "title": title,
            "url": REGULATIONS_BASE + filename,
            "doc_type": "regulation",
        })
    return regulations


class NLLegislationScraper(BaseScraper):
    """Scraper for Newfoundland and Labrador consolidated statutes and regulations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a fetched document into the standard schema."""
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        doc_type = raw.get("doc_type", "statute")

        if not text or len(text) < 50:
            return None

        return {
            "_id": f"CA-NL-{doc_type}-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,
            "url": raw.get("url", ""),
            "language": "en",
            "doc_type": doc_type,
            "doc_id": doc_id,
            "jurisdiction": "CA-NL",
        }

    def _fetch_documents(self, docs: list[dict], limit: int) -> Generator[dict, None, None]:
        """Fetch HTML for each document and yield with extracted text."""
        count = 0
        for doc in docs:
            if count >= limit:
                break

            page_html = fetch_page(doc["url"])
            if not page_html:
                continue

            # Extract title from page if index title is generic
            page_title = extract_title_from_html(page_html)
            if page_title and len(page_title) > len(doc.get("title", "")):
                doc["title"] = page_title

            text = strip_html_to_text(page_html)
            text = remove_boilerplate(text)

            if len(text) < 50:
                logger.debug("Skipping %s — too short (%d chars)", doc["doc_id"], len(text))
                continue

            doc["text"] = text
            yield doc
            count += 1

            if count % 50 == 0:
                logger.info("Fetched %d documents...", count)

            time.sleep(DELAY)

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all NL statutes and regulations."""
        limit = 15 if sample else 999999

        # Fetch statutes index
        logger.info("Fetching statutes index...")
        stat_html = fetch_page(STATUTES_INDEX)
        if not stat_html:
            logger.error("Could not fetch statutes index")
            return

        statutes = parse_statute_index(stat_html)
        logger.info("Found %d statutes", len(statutes))

        # Fetch regulations index
        logger.info("Fetching regulations index...")
        reg_html = fetch_page(REGULATIONS_INDEX)
        if not reg_html:
            logger.error("Could not fetch regulations index")
            return

        regulations = parse_regulation_index(reg_html)
        logger.info("Found %d regulations", len(regulations))

        total_docs = statutes + regulations
        logger.info("Total documents to fetch: %d", len(total_docs))

        yield from self._fetch_documents(total_docs, limit)

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch all documents (no incremental update API available)."""
        yield from self.fetch_all()


def main():
    scraper = NLLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity...")
        page = fetch_page(STATUTES_INDEX)
        if page and "statutes/" in page.lower():
            logger.info("OK: Statutes index accessible")
        else:
            logger.error("FAIL: Cannot access statutes index")
            sys.exit(1)

        page = fetch_page(REGULATIONS_INDEX)
        if page and "regulations/" in page.lower():
            logger.info("OK: Regulations index accessible")
        else:
            logger.error("FAIL: Cannot access regulations index")
            sys.exit(1)

        logger.info("All connectivity tests passed")
        return

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        if fetched == 0:
            logger.error("No records fetched!")
            sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
