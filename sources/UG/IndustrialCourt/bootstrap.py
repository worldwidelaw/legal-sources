#!/usr/bin/env python3
"""
UG/IndustrialCourt -- Industrial Court of Uganda

Fetches labour-court rulings and awards via the WordPress REST API.
Titles like `LDR-NO-70-OF-2016-AWARD-PARTY-A-VS-PARTY-B` encode the case
number and the document type. Cause lists are filtered out.

Strategy:
  - WP Media API: ~564 attachments. Keep PDFs whose titles contain
    RULING / AWARD / JUDGEMENT / JUDGMENT. Skip cause lists and forms.
  - PDF text via pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import time
import html
import logging
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import signal
import requests
import pdfplumber


class _PdfTimeout(Exception):
    pass


def _alarm_handler(signum, frame):
    raise _PdfTimeout("pdfplumber timeout")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UG.IndustrialCourt")

API_BASE = "https://industrialcourt.go.ug/wp-json/wp/v2"
USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"

MIN_TEXT_LENGTH = 200

CASE_KEYWORDS = ("RULING", "AWARD", "JUDGEMENT", "JUDGMENT")
SKIP_KEYWORDS = ("CAUSELIST", "CAUSE LIST", "CAUSE-LIST", "ROSTER")

CASE_NUMBER_RE = re.compile(
    r"\b((?:LDR|LDA|LDC|LDMA|MA|HCT|ICCMIS)[- _]?[A-Z]*[- _]?(?:NO[- _]?)?\d+[- _]?(?:OF[- _]?)?\d{2,4})",
    re.IGNORECASE,
)


def strip_html(raw_html: str) -> str:
    text = re.sub(r"<[^>]+>", "", raw_html or "")
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def download_pdf_text(url: str, session: requests.Session, hard_timeout_s: int = 45) -> Optional[str]:
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        if len(resp.content) > 50_000_000:
            logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
            return None
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(resp.content)
            tmp_path = f.name
        try:
            signal.signal(signal.SIGALRM, _alarm_handler)
            signal.alarm(hard_timeout_s)
            try:
                with pdfplumber.open(tmp_path) as pdf:
                    pages_text = []
                    for page in pdf.pages[:150]:
                        page_text = page.extract_text()
                        if page_text:
                            pages_text.append(page_text)
                    return "\n\n".join(pages_text) if pages_text else None
            finally:
                signal.alarm(0)
        finally:
            os.unlink(tmp_path)
    except _PdfTimeout:
        logger.warning(f"PDF extraction timed out after {hard_timeout_s}s: {url}")
        return None
    except Exception as e:
        logger.warning(f"PDF extraction failed for {url}: {e}")
        return None


def classify_title(title: str) -> str:
    t = title.upper()
    if "AWARD" in t:
        return "award"
    if "JUDGEMENT" in t or "JUDGMENT" in t:
        return "judgement"
    if "RULING" in t:
        return "ruling"
    return "decision"


def is_case_doc(title: str) -> bool:
    t = title.upper()
    if any(s in t for s in SKIP_KEYWORDS):
        return False
    return any(k in t for k in CASE_KEYWORDS)


def parse_case_number(title: str) -> Optional[str]:
    m = CASE_NUMBER_RE.search(title)
    if not m:
        return None
    return re.sub(r"[_\s]+", "-", m.group(1).upper())


class IndustrialCourtScraper(BaseScraper):
    """
    Scraper for UG/IndustrialCourt -- Industrial Court of Uganda.
    Country: UG
    URL: https://industrialcourt.go.ug/
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT

    def _paginate_wp(self, endpoint: str, params: dict = None, max_pages: int = 50) -> Generator[dict, None, None]:
        if params is None:
            params = {}
        params.setdefault("per_page", 100)
        page = 1
        while page <= max_pages:
            params["page"] = page
            resp = None
            for attempt in range(4):
                try:
                    resp = self._session.get(
                        f"{API_BASE}/{endpoint}", params=params, timeout=90
                    )
                    resp.raise_for_status()
                    break
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 400:
                        return
                    if attempt == 3:
                        logger.warning(f"Pagination HTTPError page={page}: {e}")
                        return
                    time.sleep(2 ** attempt + 5)
                except requests.exceptions.RequestException as e:
                    if attempt == 3:
                        logger.warning(f"Pagination failed page={page}: {e}")
                        return
                    time.sleep(2 ** attempt)
            if resp is None:
                return
            data = resp.json()
            if not data:
                break
            for item in data:
                yield item
            total_pages = int(resp.headers.get("X-WP-TotalPages", max_pages))
            if page >= total_pages:
                break
            page += 1
            time.sleep(1.0)

    def _normalize_media(self, media: dict, text: str) -> dict:
        title = strip_html(media.get("title", {}).get("rendered", ""))
        media_id = media.get("id", 0)
        source_url = media.get("source_url", "")
        date_str = media.get("date", "")[:10] if media.get("date") else None
        doc_type = classify_title(title)
        case_no = parse_case_number(title)

        return {
            "_id": f"ug-ic-{media_id}",
            "_source": "UG/IndustrialCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": source_url,
            "case_number": case_no,
            "document_type": doc_type,
        }

    def normalize(self, raw: dict) -> dict:
        return raw

    def fetch_all(self) -> Generator[dict, None, None]:
        yielded = 0
        logger.info("Fetching case PDFs...")
        for media in self._paginate_wp("media", {"media_type": "application"}):
            if media.get("mime_type") != "application/pdf":
                continue
            title = strip_html(media.get("title", {}).get("rendered", ""))
            if not is_case_doc(title):
                continue

            source_url = media.get("source_url", "")
            logger.info(f"  Downloading: {title[:60]}...")

            text = download_pdf_text(source_url, self._session)
            if not text or len(text) < MIN_TEXT_LENGTH:
                logger.debug(f"  Skipped (no text): {title[:50]}")
                continue

            record = self._normalize_media(media, text)
            yield record
            yielded += 1
            logger.info(f"  [{yielded}] {title[:60]} ({len(text)} chars)")
            time.sleep(1.0)

        logger.info(f"fetch_all complete: {yielded} records")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        for media in self._paginate_wp(
            "media", {"media_type": "application", "after": f"{since}T00:00:00"}
        ):
            if media.get("mime_type") != "application/pdf":
                continue
            title = strip_html(media.get("title", {}).get("rendered", ""))
            if not is_case_doc(title):
                continue
            text = download_pdf_text(media.get("source_url", ""), self._session)
            if text and len(text) >= MIN_TEXT_LENGTH:
                yield self._normalize_media(media, text)
                time.sleep(1.0)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="UG/IndustrialCourt -- Industrial Court of Uganda"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = IndustrialCourtScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            resp = requests.get(
                f"{API_BASE}/media",
                params={"per_page": 1, "media_type": "application"},
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            resp.raise_for_status()
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"Total media: {total}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
