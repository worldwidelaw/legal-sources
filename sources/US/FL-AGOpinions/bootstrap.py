#!/usr/bin/env python3
"""
US/FL-AGOpinions -- Florida Attorney General Formal Opinions

Fetches the full text of formal legal opinions issued by the Florida
Office of the Attorney General ("AGOs"), published openly at
myfloridalegal.com/ag-opinions. Each opinion answers a legal question
posed by a public official and is an authoritative (advisory) state
legal interpretation (doctrine).

Strategy:
  1. Walk the paginated index at /ag-opinions?page=N. Each table row gives
     the opinion number (e.g. "AGO 2026-05"), issued date, title, and the
     /node/{id} detail link.
  2. Fetch each /node/{id} detail page and extract the full opinion body
     from the Drupal `field--name-body` region (HTML, no PDF).
  3. Normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all pages)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FL-AGOpinions")

BASE_URL = "https://www.myfloridalegal.com"
INDEX_PATH = "/ag-opinions"
# The index paginates 30 rows/page; ~168 pages as of 2026. Discovered
# dynamically by walking until a page yields no new rows.
MAX_PAGES = 300


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted HTML text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_index_date(raw: str) -> str | None:
    """Convert ISO datetime (from <time datetime>) -> YYYY-MM-DD."""
    raw = (raw or "").strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


class FLAGOpinionsScraper(BaseScraper):

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

    def _get(self, url: str, retries: int = 4) -> str:
        """Fetch a URL (HTML) with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
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
        return ""

    def _index_url(self, page: int) -> str:
        return f"{BASE_URL}{INDEX_PATH}?page={page}"

    def parse_index(self, html: str) -> list:
        """Parse one index page's table into a list of opinion stubs."""
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return []
        tbody = table.find("tbody") or table

        docs = []
        for row in tbody.find_all("tr"):
            num_cell = row.find("td", class_=re.compile(r"views-field-field-number"))
            if not num_cell:
                continue
            link = num_cell.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            m = re.search(r"/node/(\d+)", href)
            if not m:
                continue
            node_id = m.group(1)
            number = num_cell.get_text(" ", strip=True)

            date_cell = row.find("td", class_=re.compile(r"views-field-field-diarydate"))
            date_iso = None
            if date_cell:
                t = date_cell.find("time")
                if t and t.get("datetime"):
                    date_iso = parse_index_date(t["datetime"])

            title_cell = row.find("td", class_=re.compile(r"views-field-title"))
            title = title_cell.get_text(" ", strip=True) if title_cell else ""

            docs.append({
                "node_id": node_id,
                "opinion_number": number,
                "title": title,
                "date": date_iso,
                "url": f"{BASE_URL}/node/{node_id}",
            })
        return docs

    def extract_body(self, html: str) -> str:
        """Extract the full opinion text from a /node detail page."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        body = soup.find("div", class_=re.compile(r"field--name-body"))
        if not body:
            return ""
        for tag in body.find_all(["script", "style"]):
            tag.decompose()
        return clean_text(body.get_text("\n", strip=True))

    def test_api(self) -> bool:
        """Test connectivity and parsing of the index + a detail page."""
        logger.info("Testing FL AG opinions index...")
        try:
            html = self._get(self._index_url(0))
            docs = self.parse_index(html)
            if not docs:
                logger.error("  Index parse returned no opinions")
                return False
            logger.info(f"  Index parse OK ({len(docs)} opinions on page 0)")

            detail = self._get(docs[0]["url"])
            text = self.extract_body(detail)
            if text and len(text) > 200:
                logger.info(f"  Body extraction OK ({len(text)} chars)")
            else:
                logger.error("  Body extraction failed or too short")
                return False

            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_document(self, doc: dict) -> dict | None:
        """Fetch a detail page and attach full text."""
        html = self._get(doc["url"])
        text = self.extract_body(html)
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {doc['opinion_number']} ({doc['url']})")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = raw["opinion_number"]
        opinion_title = (raw.get("title") or "").strip()
        title = f"Florida AG Opinion {number}"
        if opinion_title:
            title = f"{title} — {opinion_title}"
        return {
            "_id": f"US/FL-AGOpinions/{raw['node_id']}",
            "_source": "US/FL-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records by walking index pages newest-first."""
        emitted = 0
        seen_nodes = set()
        for page in range(0, MAX_PAGES):
            html = self._get(self._index_url(page))
            docs = self.parse_index(html)
            fresh = [d for d in docs if d["node_id"] not in seen_nodes]
            if not fresh:
                logger.info(f"Page {page}: no new opinions, stopping")
                break
            logger.info(f"Page {page}: {len(fresh)} opinions")
            for doc in fresh:
                seen_nodes.add(doc["node_id"])
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
        for record in self.fetch_all():
            if not since or (record.get("date") and record["date"] >= since):
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FL-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FLAGOpinionsScraper()

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
