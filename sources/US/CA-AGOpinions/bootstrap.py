#!/usr/bin/env python3
"""
US/CA-AGOpinions -- California Attorney General Formal Opinions

Fetches the full text of formal legal opinions issued by the California
Office of the Attorney General. Opinions are indexed by year at
oag.ca.gov/opinions/yearly-index and published as official PDFs.

Strategy:
  1. For each year, fetch the yearly index (filtered via query param) and
     parse the opinions table: opinion number, question, conclusion(s),
     issued date, and the PDF link.
  2. Download each opinion PDF and extract full text via pdfplumber.
  3. Normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all years)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import io
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-AGOpinions")

BASE_URL = "https://oag.ca.gov"
INDEX_PATH = "/opinions/yearly-index"
# Opinions are indexed back to 1976; current year is discovered dynamically.
EARLIEST_YEAR = 1976


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_issued_date(raw: str) -> str | None:
    """Convert MM/DD/YYYY -> ISO YYYY-MM-DD."""
    raw = (raw or "").strip()
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
        return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
    return None


class CAAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get(self, url: str, binary: bool = False, retries: int = 4):
        """Fetch URL with rate limiting.

        Binary (PDF) fetches stream the body via iter_content so a truncated
        response surfaces as a catchable exception (IncompleteRead /
        ChunkedEncodingError) and is retried with exponential backoff, instead
        of returning a short/empty buffer. oag.ca.gov intermittently truncates
        PDF bodies, which caused 100% IncompleteRead failures from the VPS.
        """
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                if binary:
                    resp = self.http.get(
                        url, stream=True, headers={"Accept": "application/pdf,*/*"}
                    )
                    if resp.status_code == 404:
                        logger.debug(f"404: {url}")
                        resp.close()
                        return b""
                    if resp.status_code != 200:
                        logger.warning(f"HTTP {resp.status_code} for {url}")
                        resp.close()
                        raise IOError(f"status {resp.status_code}")
                    buf = bytearray()
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            buf.extend(chunk)
                    resp.close()
                    return bytes(buf)
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 404:
                    logger.debug(f"404: {url}")
                    return ""
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < retries:
                    time.sleep(2 ** attempt)
        return b"" if binary else ""

    def _index_url(self, year: int) -> str:
        return f"{BASE_URL}{INDEX_PATH}?conclusion-year%5Bvalue%5D%5Byear%5D={year}"

    def test_api(self) -> bool:
        """Test connectivity and the opinions index."""
        logger.info("Testing CA AG opinions yearly index...")
        try:
            html = self._get(self._index_url(2020))
            docs = self.parse_index(html)
            if docs:
                logger.info(f"  Index parse OK ({len(docs)} opinions in 2020)")
            else:
                logger.error("  Index parse returned no opinions")
                return False

            pdf_bytes = self._get(docs[0]["pdf_url"], binary=True)
            text = self.extract_pdf(pdf_bytes)
            if text and len(text) > 200:
                logger.info(f"  PDF extraction OK ({len(text)} chars)")
            else:
                logger.error("  PDF extraction failed or too short")
                return False

            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def parse_index(self, html: str) -> list:
        """Parse the yearly index table into a list of opinion dicts."""
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return []

        docs = []
        seen = set()
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 4:
                continue
            link = cells[0].find("a", href=True)
            if not link:
                continue
            href = link["href"]
            if ".pdf" not in href.lower():
                continue
            pdf_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            number = cells[0].get_text(" ", strip=True)
            number = re.sub(r"\s+", "", number)
            if not number or number in seen:
                continue
            seen.add(number)

            question = cells[1].get_text(" ", strip=True)
            conclusion = cells[2].get_text(" ", strip=True)
            issued = cells[3].get_text(" ", strip=True)

            docs.append({
                "opinion_number": number,
                "question": question,
                "conclusion": conclusion,
                "date": parse_issued_date(issued),
                "pdf_url": pdf_url,
            })
        return docs

    def extract_pdf(self, pdf_bytes: bytes) -> str:
        """Extract full text from PDF bytes via pdfplumber."""
        if not pdf_bytes:
            return ""
        try:
            parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    parts.append(page.extract_text() or "")
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            return clean_text("\n".join(parts))
        except Exception as e:
            logger.warning(f"PDF extraction error: {e}")
            return ""

    def fetch_document(self, doc: dict) -> dict | None:
        """Download a single opinion PDF and attach full text."""
        pdf_bytes = self._get(doc["pdf_url"], binary=True)
        text = self.extract_pdf(pdf_bytes)
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {doc['opinion_number']} ({doc['pdf_url']})")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        number = raw["opinion_number"]
        question = (raw.get("question") or "").strip()
        title = f"California AG Opinion No. {number}"
        if question:
            short_q = question if len(question) <= 160 else question[:157] + "..."
            title = f"{title} — {short_q}"
        return {
            "_id": f"US/CA-AGOpinions/{number}",
            "_source": "US/CA-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "question": question,
            "conclusion": (raw.get("conclusion") or "").strip(),
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records from newest year backwards."""
        current_year = datetime.now(timezone.utc).year
        emitted = 0
        for year in range(current_year, EARLIEST_YEAR - 1, -1):
            html = self._get(self._index_url(year))
            docs = self.parse_index(html)
            if docs:
                logger.info(f"Year {year}: {len(docs)} opinions")
            for doc in docs:
                raw = self.fetch_document(doc)
                if raw and raw.get("text"):
                    yield self.normalize(raw)
                    emitted += 1
                    if sample and emitted >= 12:
                        return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch opinions issued on/after `since` (ISO date)."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for record in self.fetch_all():
            if not since or (record.get("date") and record["date"] >= since):
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CA-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for record in gen:
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
