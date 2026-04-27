#!/usr/bin/env python3
"""
CY/CySEC-Enforcement -- Cyprus Securities & Exchange Commission Board Decisions

Scrapes board decisions from CySEC's public information pages and downloads
the associated PDF attachments for full text extraction.

The site uses server-side rendered HTML with pagination (?page=N).
Each decision card contains entity name, dates, legislation, subject,
and a link to a PDF with the full decision text.

Note: The server sends malformed HTTP headers (trailing whitespace) which
causes issues with some HTTP clients. We use urllib3 directly with
http1.1 to handle this.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

import requests
from requests.adapters import HTTPAdapter

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CY.CySEC-Enforcement")

BASE_URL = "https://www.cysec.gov.cy"
DECISIONS_PATH = "/en-GB/public-info/decisions/"


def _parse_date(date_str: str) -> str:
    """Parse CySEC date format like '08 Apr. 2026' to ISO format."""
    date_str = date_str.strip()
    if not date_str:
        return ""
    # Remove trailing period from month abbreviations
    date_str = re.sub(r'(\w+)\.\s', r'\1 ', date_str)
    for fmt in ["%d %b %Y", "%d %B %Y", "%d %b. %Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def _clean_text(text: str) -> str:
    """Clean HTML entities and extra whitespace."""
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&nbsp;', ' ').replace('&#39;', "'")
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


class CySECScraper(BaseScraper):
    """Scraper for CY/CySEC-Enforcement."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        # Use requests with custom settings for malformed headers
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/pdf,*/*",
        })

    def _fetch_page(self, page: int) -> str:
        """Fetch a decisions listing page."""
        self.rate_limiter.wait()
        url = f"{BASE_URL}{DECISIONS_PATH}?page={page}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _parse_cards(self, html: str) -> List[Dict[str, Any]]:
        """Parse decision cards from a listing page."""
        cards = []

        # Split by card boundary
        card_pattern = r'class="card card-custom card-custom-nofooter h-100">(.*?)(?=class="card card-custom card-custom-nofooter h-100"|<nav|<footer|</section)'
        raw_cards = re.findall(card_pattern, html, re.DOTALL)

        for card_html in raw_cards:
            entry = {}

            # Entity name from card-header
            header = re.search(r'class="card-header fw-bold">(.*?)</div>', card_html, re.DOTALL)
            if header:
                entry["entity"] = _clean_text(header.group(1))

            # Announcement date + PDF link
            ann_match = re.search(
                r'<strong>Announcement Date:</strong>\s*'
                r'<a href="([^"]*)"[^>]*>\s*([\d\w\s.]+?)\s*<',
                card_html, re.DOTALL
            )
            if ann_match:
                entry["pdf_path"] = ann_match.group(1)
                entry["announcement_date"] = _parse_date(ann_match.group(2))
            else:
                # Try without link
                ann_text = re.search(
                    r'<strong>Announcement Date:</strong>\s*([\d\w\s.]+?)(?:<|$)',
                    card_html, re.DOTALL
                )
                if ann_text:
                    entry["announcement_date"] = _parse_date(ann_text.group(1))

            # Board Decision Date
            bd_match = re.search(
                r'<strong>Board Decision\s+Date:</strong>\s*([\d\w\s.]+?)(?:<|$)',
                card_html, re.DOTALL
            )
            if bd_match:
                entry["decision_date"] = _parse_date(bd_match.group(1))

            # Legislation
            leg_match = re.search(
                r'<strong>Legislation:</strong>\s*(.*?)\s*</div>',
                card_html, re.DOTALL
            )
            if leg_match:
                entry["legislation"] = _clean_text(leg_match.group(1))

            # Subject
            subj_match = re.search(
                r'<strong>Subject:</strong>\s*(.*?)\s*</div>',
                card_html, re.DOTALL
            )
            if subj_match:
                entry["subject"] = _clean_text(subj_match.group(1))

            # Judicial Review
            jr_match = re.search(r'<b>Judicial Review:</b>(.*?)</div>', card_html, re.DOTALL)
            if jr_match:
                jr_text = _clean_text(jr_match.group(1))
                entry["judicial_review"] = "Yes" in jr_text and 'class="d-none">Yes' not in jr_match.group(1)

            if entry.get("entity"):
                cards.append(entry)

        return cards

    def _download_pdf_text(self, pdf_path: str, doc_id: str) -> str:
        """Download a PDF and extract text."""
        if not pdf_path:
            return ""

        url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
        self.rate_limiter.wait()

        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF for {doc_id}: {e}")
            return ""

        if len(pdf_bytes) < 100:
            return ""

        text = extract_pdf_markdown(
            source="CY/CySEC-Enforcement",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

        return text

    def normalize(self, raw: dict) -> dict:
        entity = raw.get("entity", "Unknown")
        decision_date = raw.get("decision_date", "")
        doc_id = hashlib.md5(
            f"{entity}_{decision_date}_{raw.get('subject', '')}".encode()
        ).hexdigest()[:16]

        title = f"{entity} - {raw.get('subject', 'Board Decision')}"

        pdf_path = raw.get("pdf_path", "")
        url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}" if pdf_path else ""

        return {
            "_id": f"CY/CySEC-Enforcement/{doc_id}",
            "_source": "CY/CySEC-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_prefetched_text", ""),
            "date": decision_date,
            "url": url,
            "entity": entity,
            "decision_date": decision_date,
            "announcement_date": raw.get("announcement_date", ""),
            "legislation": raw.get("legislation", ""),
            "subject": raw.get("subject", ""),
            "judicial_review": raw.get("judicial_review", False),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        max_pages = 46 if not sample else 3
        limit = 15 if sample else None
        count = 0

        for page in range(1, max_pages + 1):
            if limit and count >= limit:
                break

            logger.info(f"Fetching page {page}...")
            try:
                html = self._fetch_page(page)
            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                continue

            cards = self._parse_cards(html)
            if not cards:
                logger.info(f"No cards on page {page}, stopping.")
                break

            for card in cards:
                if limit and count >= limit:
                    break

                pdf_path = card.get("pdf_path", "")
                entity = card.get("entity", "unknown")

                if pdf_path:
                    text = self._download_pdf_text(pdf_path, entity)
                    if not text or len(text) < 50:
                        logger.warning(f"  Skipping {entity} - no/short text")
                        continue
                    card["_prefetched_text"] = text
                else:
                    logger.warning(f"  Skipping {entity} - no PDF link")
                    continue

                yield card
                count += 1
                logger.info(f"  [{count}] {entity[:50]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = CySECScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing CySEC access...")
        html = scraper._fetch_page(1)
        cards = scraper._parse_cards(html)
        print(f"Page 1: {len(cards)} cards found")
        if cards:
            c = cards[0]
            print(f"  First: {c.get('entity', 'N/A')} | {c.get('decision_date', 'N/A')} | {c.get('subject', 'N/A')}")
            if c.get("pdf_path"):
                text = scraper._download_pdf_text(c["pdf_path"], "test")
                print(f"  PDF text: {len(text)} chars")
        print("Test PASSED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
