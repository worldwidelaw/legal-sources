#!/usr/bin/env python3
"""
IN/RajyaSabhaDebates -- Rajya Sabha Verbatim Debates

Fetches debate transcripts from the Rajya Sabha Official Debates portal
(rsdebate.nic.in), a DSpace 5.6 instance. Full text is extracted from
text-layer PDFs using pdfplumber.

Coverage: Sessions 1–267 (1952–present), ~746K debate entries across
Part 1 (Questions & Answers) and Part 2 (Other proceedings).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch debates from last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import time
import re
import html
import logging
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import quote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.RajyaSabhaDebates")

BASE_URL = "https://rsdebate.nic.in"
PAGE_SIZE = 100
MIN_TEXT_CHARS = 200
REQUEST_TIMEOUT = 60
DELAY_BETWEEN_REQUESTS = 1.5


def _decode_html(s: str) -> str:
    """Decode HTML entities like &#x20; and &quot;."""
    return html.unescape(s.replace("&#x20;", " "))


def _parse_date(date_str: str) -> Optional[str]:
    """Convert DD-Mon-YYYY or D-Mon-YYYY to ISO 8601 date."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    return None


class RajyaSabhaDebatesScraper(BaseScraper):
    SOURCE_ID = "IN/RajyaSabhaDebates"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research project)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get(self, url: str, params: dict = None, stream: bool = False) -> requests.Response:
        """Make a GET request with retry logic."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT, stream=stream)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt < 2:
                    wait = (attempt + 1) * 5
                    logger.warning("Request failed (attempt %d/3): %s — retrying in %ds", attempt + 1, e, wait)
                    time.sleep(wait)
                else:
                    raise

    def _search_page(self, start: int = 0) -> str:
        """Fetch a simple-search results page sorted by date descending."""
        params = {
            "query": "*",
            "rpp": str(PAGE_SIZE),
            "start": str(start),
            "sort_by": "dc.date.accessioned_dt",
            "order": "desc",
        }
        url = f"{BASE_URL}/simple-search"
        resp = self._get(url, params=params)
        return resp.text

    def _get_total(self, page_html: str) -> int:
        """Extract total result count from search results page."""
        m = re.search(r'of\s+([\d,]+)', page_html)
        if m:
            return int(m.group(1).replace(",", ""))
        return 0

    def _parse_search_rows(self, page_html: str) -> List[Dict[str, Any]]:
        """Parse search results table rows into structured records."""
        results = []
        # Find the results table
        table_match = re.search(r'<table[^>]*>(.*?)</table>', page_html, re.S)
        if not table_match:
            return results

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_match.group(1), re.S)
        for row in rows:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
            if len(tds) < 6:
                continue

            # Extract handle from the View link
            handle_match = re.search(r'/handle/(123456789/\d+)', row)
            if not handle_match:
                continue

            handle_id = handle_match.group(1)

            def clean(s):
                # Remove highlighted-fulltext divs and their content entirely
                s = re.sub(r"<div[^>]*class='highlighted-fulltext'[^>]*>.*?</div>", '', s, flags=re.S)
                s = re.sub(r'<[^>]+>', '', s).strip()
                s = re.sub(r'Keyword\s*Count:\s*\S*\s*\d+', '', s).strip()
                return _decode_html(s)

            session_num = clean(tds[0])
            date_raw = clean(tds[1])
            debate_type = clean(tds[2])
            subject = clean(tds[3])
            title = clean(tds[4])
            members_raw = clean(tds[5])

            results.append({
                "handle_id": handle_id,
                "session_number": session_num,
                "date_raw": date_raw,
                "date": _parse_date(date_raw),
                "debate_type": debate_type,
                "subject": subject,
                "title": title,
                "members": [m.strip() for m in members_raw.split(";") if m.strip()] if members_raw else [],
            })

        return results

    def _get_pdf_url(self, handle_id: str) -> Optional[str]:
        """Fetch item page and extract the text PDF URL (PD_* prefix)."""
        url = f"{BASE_URL}/handle/{handle_id}"
        try:
            resp = self._get(url)
        except Exception as e:
            logger.warning("Failed to fetch item page %s: %s", handle_id, e)
            return None

        # Look for text PDF (PD_ prefix = text version, ID_ = image version)
        pdf_matches = re.findall(r'/bitstream/[^\"\']+\.pdf', resp.text, re.I)
        for pdf_path in pdf_matches:
            filename = pdf_path.rsplit("/", 1)[-1]
            if filename.startswith("PD_"):
                return f"{BASE_URL}{pdf_path}"

        # Fallback: use any PDF that isn't help/disclaimer
        for pdf_path in pdf_matches:
            if "/help/" not in pdf_path:
                return f"{BASE_URL}{pdf_path}"

        return None

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text using pdfplumber."""
        try:
            resp = self._get(pdf_url, stream=True)
            content = resp.content
            if len(content) < 100:
                return None

            pages_text = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text.strip())
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            full_text = "\n\n".join(pages_text)
            if len(full_text) >= MIN_TEXT_CHARS:
                return full_text
            else:
                logger.debug("PDF text too short (%d chars) from %s", len(full_text), pdf_url)
                return None
        except Exception as e:
            logger.warning("Failed to extract text from PDF %s: %s", pdf_url, e)
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into the standard schema."""
        handle_id = raw.get("handle_id", "")
        return {
            "_id": handle_id.replace("/", "_"),
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "Rajya Sabha Debates"),
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{BASE_URL}/handle/{handle_id}" if handle_id else None,
            "session_number": raw.get("session_number"),
            "debate_type": raw.get("debate_type"),
            "subject": raw.get("subject"),
            "members": raw.get("members", []),
            "language": "en",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Yield all debate records with full text."""
        max_records = 15 if sample else None
        count = 0
        skipped = 0
        start = 0

        # Get total count from first page
        first_html = self._search_page(0)
        total = self._get_total(first_html)
        logger.info("Total items in collection: %d", total)

        # Process first page
        rows = self._parse_search_rows(first_html)
        all_pages = [(0, rows)]

        page_num = 0
        while True:
            if max_records and count >= max_records:
                return

            if page_num == 0:
                current_rows = all_pages[0][1]
            else:
                logger.info("Fetching search page at offset %d", start)
                try:
                    page_html = self._search_page(start)
                except Exception as e:
                    logger.error("Failed to fetch search page at offset %d: %s", start, e)
                    break
                current_rows = self._parse_search_rows(page_html)

            if not current_rows:
                logger.info("No more rows at offset %d, stopping", start)
                break

            for row in current_rows:
                if max_records and count >= max_records:
                    return

                time.sleep(DELAY_BETWEEN_REQUESTS)

                # Get PDF URL from item page
                pdf_url = self._get_pdf_url(row["handle_id"])
                if not pdf_url:
                    skipped += 1
                    logger.info("Skipping %s — no text PDF found", row["handle_id"])
                    continue

                time.sleep(DELAY_BETWEEN_REQUESTS)

                # Extract text from PDF
                text = self._extract_text_from_pdf(pdf_url)
                if not text:
                    skipped += 1
                    logger.info("Skipping %s — no extractable text from PDF", row["handle_id"])
                    continue

                row["text"] = text
                yield self.normalize(row)
                count += 1
                logger.info("Fetched %d/%s: %s — %s", count, max_records or total,
                            row.get("date", "unknown"), row.get("title", "")[:60])

            start += PAGE_SIZE
            page_num += 1

        logger.info("Completed: %d records fetched, %d skipped", count, skipped)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch debates issued since the given date."""
        since_date = datetime.fromisoformat(since).date() if isinstance(since, str) else since
        logger.info("Fetching debates since %s", since_date)

        for record in self.fetch_all():
            if record.get("date"):
                try:
                    rec_date = datetime.fromisoformat(record["date"]).date()
                    if rec_date < since_date:
                        logger.info("Reached records before %s, stopping", since_date)
                        return
                except ValueError:
                    pass
            yield record

    def test(self) -> bool:
        """Quick connectivity check."""
        try:
            page_html = self._search_page(0)
            total = self._get_total(page_html)
            rows = self._parse_search_rows(page_html)
            logger.info("Connection OK. %d total items, %d on first page.", total, len(rows))
            return total > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/RajyaSabhaDebates bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--since", type=str, help="ISO date for incremental update")
    args = parser.parse_args()

    scraper = RajyaSabhaDebatesScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    # Sample mode: write to sample/ for validation.
    if args.command in ("bootstrap", "bootstrap-fast") and args.sample:
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=True):
            count += 1
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("Done. Saved %d sample records to %s", count, sample_dir)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        return

    # Full corpus (bootstrap / bootstrap-fast) and updates stream to
    # data/records.jsonl — this is what the ingest pipeline consumes.
    if args.command == "update":
        since = args.since or (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
        records = scraper.fetch_updates(since)
    else:
        records = scraper.fetch_all(sample=False)

    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = data_dir / "records.jsonl"
    count = 0
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % 100 == 0:
                logger.info("Progress: %d records written", count)

    logger.info("Done. Wrote %d records to %s", count, jsonl_path)
    if count == 0:
        logger.error("No records fetched!")
        sys.exit(1)


if __name__ == "__main__":
    main()
