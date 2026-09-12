#!/usr/bin/env python3
"""
INTL/UEFA-Disciplinary -- UEFA Disciplinary Decisions & Regulations

Two data streams:
  1. Regulations/statutes from documents.uefa.com Knowledge Hub API (JSON → PDF)
  2. Disciplinary decisions from editorial.uefa.com (scraped PDF list)

Both UEFA domains require curl (Python requests gets 403 from bot protection).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Re-scan for new documents
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UEFA-Disciplinary")

SOURCE_ID = "INTL/UEFA-Disciplinary"

KHUB_API = "https://documents.uefa.com/api/khub/documents"
KHUB_CONTENT = "https://documents.uefa.com/api/khub/documents/{doc_id}/content"
DECISIONS_PAGE = "https://www.uefa.com/running-competitions/disciplinary/meeting-decisions/"

MIN_TEXT_CHARS = 200


def curl_get(url: str, timeout: int = 120, binary: bool = False):
    """Fetch a URL using curl subprocess (bypasses Python SSL/bot-protection issues)."""
    cmd = ["curl", "-sL", "--http1.1", "--max-time", str(timeout), url]
    result = subprocess.run(cmd, capture_output=True, timeout=timeout + 30)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}) for {url}: {result.stderr.decode()[:200]}")
    return result.stdout if binary else result.stdout


def curl_get_json(url: str, timeout: int = 120) -> dict:
    """Fetch JSON from a URL using curl."""
    data = curl_get(url, timeout=timeout)
    return json.loads(data)


class UEFADisciplinaryScraper(BaseScraper):
    """Scraper for UEFA disciplinary decisions and regulations."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfminer."""
        try:
            text = pdf_extract_text(io.BytesIO(pdf_bytes))
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    # ------------------------------------------------------------------
    # Part 1: Regulations from Knowledge Hub API
    # ------------------------------------------------------------------

    def _fetch_regulations_list(self) -> list:
        """Fetch the full document list from the Knowledge Hub API."""
        logger.info(f"Fetching regulation list from {KHUB_API}")
        return curl_get_json(KHUB_API)

    def _get_meta(self, doc: dict, key: str) -> Optional[str]:
        """Extract a metadata value from a khub document."""
        for m in doc.get("metadata", []):
            if m["key"] == key and m.get("values"):
                return m["values"][0]
        return None

    def _is_english_public(self, doc: dict) -> bool:
        locale = self._get_meta(doc, "ft:locale")
        restriction = self._get_meta(doc, "FT_GroupRestrictionLevel")
        pub_state = self._get_meta(doc, "FT_PublicationState")
        return locale == "en-GB" and restriction == "Public" and pub_state == "Online"

    def _parse_enforcement_date(self, doc: dict) -> Optional[str]:
        raw = self._get_meta(doc, "EnforcementDate")
        if not raw:
            edition = self._get_meta(doc, "FullSeasonYears")
            if edition and edition.isdigit():
                return f"{edition}-01-01"
            return None
        for fmt in ["%d %B %Y", "%B %Y", "%Y"]:
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _classify_regulation(self, doc: dict) -> str:
        ct = (self._get_meta(doc, "FT_ContentType") or "").lower()
        if "regulation" in ct:
            return "regulation"
        elif "guideline" in ct:
            return "guideline"
        elif "report" in ct:
            return "report"
        elif "strategy" in ct:
            return "strategy"
        return "regulation"

    def _fetch_regulation_records(self, sample: bool = False) -> Generator[dict, None, None]:
        docs = self._fetch_regulations_list()
        english_docs = [d for d in docs if self._is_english_public(d)]
        logger.info(f"Found {len(english_docs)} English public regulations out of {len(docs)} total")

        if sample:
            english_docs = english_docs[:8]

        for i, doc in enumerate(english_docs):
            doc_id = doc["id"]
            title = doc.get("title", "")
            logger.info(f"[{i+1}/{len(english_docs)}] Regulation: {title}")

            pdf_url = KHUB_CONTENT.format(doc_id=doc_id)
            try:
                time.sleep(1)
                pdf_bytes = curl_get(pdf_url, timeout=120, binary=True)
            except Exception as e:
                logger.warning(f"Failed to download PDF for {title}: {e}")
                continue

            if len(pdf_bytes) < 1000:
                logger.warning(f"PDF too small for {title}: {len(pdf_bytes)} bytes")
                continue

            text = self._extract_pdf_text(pdf_bytes)
            if len(text) < MIN_TEXT_CHARS:
                logger.warning(f"Insufficient text for {title}: {len(text)} chars")
                continue

            subject = self._get_meta(doc, "FT_Competition") or ""
            category = self._get_meta(doc, "FT_Category") or ""
            edition = self._get_meta(doc, "FullSeasonYears") or ""

            yield {
                "_id": f"reg-{doc_id}",
                "_source": SOURCE_ID,
                "_type": "legislation",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": text,
                "date": self._parse_enforcement_date(doc),
                "url": f"https://documents.uefa.com/v/u/{doc_id}",
                "body": "UEFA",
                "doc_type": self._classify_regulation(doc),
                "subject": subject,
                "category": category,
                "edition": edition,
                "filename": doc.get("filename", ""),
            }

    # ------------------------------------------------------------------
    # Part 2: Disciplinary decisions
    # ------------------------------------------------------------------

    def _scrape_decision_pdf_links(self) -> list[dict]:
        """Scrape the meeting-decisions page for PDF links."""
        logger.info(f"Scraping decision links from {DECISIONS_PAGE}")
        html = curl_get(DECISIONS_PAGE).decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        decisions = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "editorial.uefa.com/resources/" in href and href.endswith(".pdf"):
                link_text = a_tag.get_text(strip=True)
                if not link_text:
                    parent = a_tag.find_parent()
                    if parent:
                        link_text = parent.get_text(strip=True)
                # Clean "Last updated" suffix from link text
                if link_text:
                    link_text = re.sub(r"\s*Last\s*updated.*$", "", link_text).strip()
                decisions.append({
                    "url": href,
                    "title": link_text or Path(href).stem.replace("_", " "),
                })

        logger.info(f"Found {len(decisions)} decision PDFs")
        return decisions

    def _parse_decision_date(self, title: str) -> Optional[str]:
        m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", title)
        if m:
            day, month, year = m.groups()
            try:
                return f"{year}-{int(month):02d}-{int(day):02d}"
            except ValueError:
                pass
        m = re.search(r"(\d{4})", title)
        if m:
            return f"{m.group(1)}-01-01"
        return None

    def _classify_decision_body(self, title: str) -> str:
        t = title.upper()
        if "AB" in t and "CEDB" in t:
            return "CEDB/AB"
        if "AB " in t or t.startswith("AB"):
            return "AB"
        if "JSA" in t:
            return "CEDB-JSA"
        if "CEDB" in t:
            return "CEDB"
        return "UEFA Disciplinary"

    def _fetch_decision_records(self, sample: bool = False) -> Generator[dict, None, None]:
        pdf_links = self._scrape_decision_pdf_links()

        if sample:
            pdf_links = pdf_links[:8]

        for i, info in enumerate(pdf_links):
            url = info["url"]
            title = info["title"]
            logger.info(f"[{i+1}/{len(pdf_links)}] Decision: {title}")

            try:
                time.sleep(1)
                pdf_bytes = curl_get(url, timeout=120, binary=True)
            except Exception as e:
                logger.warning(f"Failed to download decision PDF: {e}")
                continue

            if len(pdf_bytes) < 1000:
                logger.warning(f"PDF too small for {title}: {len(pdf_bytes)} bytes")
                continue

            text = self._extract_pdf_text(pdf_bytes)
            if len(text) < MIN_TEXT_CHARS:
                logger.warning(f"Insufficient text for {title}: {len(text)} chars")
                continue

            pdf_filename = Path(url).stem
            doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", pdf_filename)

            yield {
                "_id": f"dec-{doc_id}",
                "_source": SOURCE_ID,
                "_type": "case_law",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": f"UEFA {title}",
                "text": text,
                "date": self._parse_decision_date(title),
                "url": url,
                "body": self._classify_decision_body(title),
                "doc_type": "decision",
                "filename": Path(url).name,
            }

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._fetch_regulation_records(sample=False)
        yield from self._fetch_decision_records(sample=False)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/UEFA-Disciplinary bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UEFADisciplinaryScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        data = curl_get_json(KHUB_API)
        logger.info(f"Knowledge Hub API: {len(data)} documents")
        html = curl_get(DECISIONS_PAGE)
        logger.info(f"Decisions page: {len(html)} bytes")
        logger.info("Connectivity OK")
        return

    sample_mode = args.sample and not args.full
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    if args.command == "bootstrap":
        logger.info("=== Fetching UEFA Regulations ===")
        for record in scraper._fetch_regulation_records(sample=sample_mode):
            if sample_mode:
                fname = sample_dir / f"{record['_id']}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False)
            count += 1
            logger.info(f"  Saved: {record['title'][:80]} ({len(record['text'])} chars)")

        logger.info("=== Fetching UEFA Disciplinary Decisions ===")
        for record in scraper._fetch_decision_records(sample=sample_mode):
            if sample_mode:
                fname = sample_dir / f"{record['_id']}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False)
            count += 1
            logger.info(f"  Saved: {record['title'][:80]} ({len(record['text'])} chars)")

    elif args.command == "update":
        for record in scraper.fetch_all():
            count += 1

    logger.info(f"Done. Total records: {count}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
