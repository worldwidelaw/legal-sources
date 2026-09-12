#!/usr/bin/env python3
"""
SK/UVO-Namietky -- Slovakia, Public Procurement Office objection decisions

Full text of the decisions on objections (rozhodnutia o námietkach) issued by
the Slovak Public Procurement Office (Úrad pre verejné obstarávanie, ÚVO)
under the review-proceedings register published at

    https://www.uvo.gov.sk/dohlad/namietky/prehlad-rozhodnuti-o-namietkach-vseobecna-agenda

Strategy (TYPO3 HTML register + per-decision PDF):

  1. Listing — ``GET ?page=N`` returns a 7-column table, 20 rows per page.
     Columns: contracting authority, subject of the contract, notice
     reference in the Vestník, statutory ground of the objection, decision
     number + link to the full-text PDF, decision date, and the operative
     outcome. All of the metadata therefore comes from the register itself;
     nothing has to be mined out of the PDF body.

     The page count is read from the pagination widget (``?page=N`` links),
     so the walk never relies on a hardcoded ceiling.

  2. Full text — the row's ``rozhodnutie-download/{id}?cHash={hash}`` link
     streams the decision PDF (born-digital, extracts cleanly).

GOTCHAS (verified live 2026-08-02):

  - The ``cHash`` token is MANDATORY and is only obtainable from the listing;
    the numeric id alone does not resolve. So ids are never enumerated —
    every download URL is harvested from the row that owns it.
  - There are TWO id spaces: positive (recent) and NEGATIVE (pre-~2015).
    A naive ``\\d+`` regex silently drops the whole historical corpus.
  - ``Content-Type`` is ``application/octet-stream``, not ``application/pdf``,
    so the ``%PDF-`` magic is sniffed instead of trusting the header.
  - Pages are NOT strictly chronological and the rows carrying a PDF are not
    contiguous: pages 400 and 450 hold 20 rows with zero PDF links while page
    470 holds one. "Stop at the first page with no PDFs" would truncate the
    corpus, so every page up to the last is walked and PDF-less rows (the
    oldest entries, ~2002-2010, which the register only summarises) are
    skipped as having no full text.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import threading
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import _extract as extract_pdf_text  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SK.UVO-Namietky")

SOURCE_ID = "SK/UVO-Namietky"
BASE_URL = "https://www.uvo.gov.sk"
LIST_PATH = "/dohlad/namietky/prehlad-rozhodnuti-o-namietkach-vseobecna-agenda"
LIST_URL = f"{BASE_URL}{LIST_PATH}"

USER_AGENT = (
    "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://legaldatahunter.com)"
)
LIST_DELAY = 0.8        # polite delay between listing pages
MAX_RETRIES = 5
PAGE_CEILING = 2000     # sanity bound on the pagination widget

TR_RE = re.compile(r"<tr>(.*?)</tr>", re.S)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
# id may be negative (historical decisions) — the sign is part of the id
DOWNLOAD_RE = re.compile(r"rozhodnutie-download/(-?\d+)\?cHash=([0-9a-fA-F]+)")
PAGE_LINK_RE = re.compile(r"[?&]page=(\d+)")
DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
DECISION_NO_RE = re.compile(r"([0-9]{3,6}\s*-\s*\d{4}/\d{4}[^\s<]*|[0-9]+-\d+/\d{4}[^\s<]*)")
TAG_RE = re.compile(r"<[^>]+>")


def strip_tags(fragment: Optional[str]) -> str:
    """Turn an HTML table cell into clean single-spaced text."""
    if not fragment:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", fragment)
    text = TAG_RE.sub(" ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def clean_pdf_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_sk_date(cell: str) -> Optional[str]:
    """'06.07.2026' -> '2026-07-06'."""
    m = DATE_RE.search(cell or "")
    if not m:
        return None
    day, month, year = m.groups()
    try:
        return datetime(int(year), int(month), int(day)).date().isoformat()
    except ValueError:
        return None


class SKUvoNamietkyScraper(BaseScraper):
    """Scraper for the ÚVO register of decisions on objections."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "sk,en;q=0.8",
            }
        )
        self._use_checkpoint = True
        self._checkpoint_path = Path(__file__).resolve().parent / "data" / "checkpoint.json"
        self._checkpoint_lock = threading.Lock()
        self._done_pages: set[int] = set()

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str, params: Optional[dict] = None, stream_pdf: bool = False):
        """GET with backoff on 429/5xx and transport errors."""
        delay = 2.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, params=params, timeout=180)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        "HTTP %s for %s — retry %s/%s in %.0fs",
                        resp.status_code, url, attempt, MAX_RETRIES, wait,
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "Request error for %s — retry %s/%s in %.0fs (%s)",
                    url, attempt, MAX_RETRIES, delay, exc,
                )
                time.sleep(delay)
                delay = min(delay * 2, 120)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Exhausted retries for {url}")

    # ------------------------------------------------------------ checkpoint

    def _load_checkpoint(self) -> None:
        if not self._use_checkpoint:
            return
        try:
            data = json.loads(self._checkpoint_path.read_text(encoding="utf-8"))
            self._done_pages = {int(p) for p in data.get("done_pages", [])}
            if self._done_pages:
                logger.info(
                    "Checkpoint: %s listing pages already walked, skipping them",
                    len(self._done_pages),
                )
        except (OSError, ValueError):
            self._done_pages = set()

    def _mark_page_done(self, page: int) -> None:
        if not self._use_checkpoint:
            return
        with self._checkpoint_lock:
            self._done_pages.add(page)
            if len(self._done_pages) % 10 != 0:
                return  # flush every 10 pages to keep IO down
            try:
                self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                self._checkpoint_path.write_text(
                    json.dumps({"done_pages": sorted(self._done_pages)}),
                    encoding="utf-8",
                )
            except OSError as exc:
                logger.warning("Could not write checkpoint: %s", exc)

    # --------------------------------------------------------------- listing

    def _last_page(self) -> int:
        """Read the highest ``?page=N`` from the pagination widget."""
        resp = self._get(LIST_URL, params={"page": 1})
        pages = [int(n) for n in PAGE_LINK_RE.findall(resp.text)]
        pages = [p for p in pages if 0 < p <= PAGE_CEILING]
        if not pages:
            raise RuntimeError(
                f"{LIST_URL} returned no pagination links — layout changed or "
                "the register is unreachable from this vantage"
            )
        return max(pages)

    def _parse_page(self, html: str, page: int) -> list[dict]:
        """Extract the rows of one listing page that carry a full-text PDF."""
        out: list[dict] = []
        for row in TR_RE.findall(html):
            cells = TD_RE.findall(row)
            if len(cells) < 7:
                continue
            m = DOWNLOAD_RE.search(cells[4])
            if not m:
                # Historical entries (~2002-2010) are summary-only in the
                # register — no PDF, therefore no full text. Skip them.
                continue
            doc_id, chash = m.group(1), m.group(2)
            decision_no = strip_tags(
                re.sub(r"(?s)<a\b.*?</a>", " ", cells[4])
            )
            out.append(
                {
                    "doc_id": doc_id,
                    "chash": chash,
                    "pdf_url": f"{LIST_URL}/rozhodnutie-download/{doc_id}?cHash={chash}",
                    "authority": strip_tags(cells[0]),
                    "subject": strip_tags(cells[1]),
                    "notice_ref": strip_tags(cells[2]),
                    "legal_ground": strip_tags(cells[3]),
                    "decision_no": decision_no,
                    "date": parse_sk_date(strip_tags(cells[5])),
                    "outcome": strip_tags(cells[6]),
                    "listing_page": page,
                }
            )
        return out

    def fetch_all(self) -> Generator[dict, None, None]:
        self._load_checkpoint()
        last = self._last_page()
        logger.info("Register has %s listing pages (~%s rows)", last, last * 20)

        seen: set[str] = set()
        empty_streak = 0
        for page in range(1, last + 1):
            if page in self._done_pages:
                continue
            resp = self._get(LIST_URL, params={"page": page})
            rows = self._parse_page(resp.text, page)
            if not rows and "<tr>" not in resp.text:
                empty_streak += 1
                if empty_streak >= 5:
                    raise RuntimeError(
                        f"5 consecutive listing pages with no table markup "
                        f"(last tried page {page}) — the register layout changed "
                        "or this vantage is being served an error shell"
                    )
            else:
                empty_streak = 0
            for row in rows:
                if row["doc_id"] in seen:
                    continue
                seen.add(row["doc_id"])
                yield row
            self._mark_page_done(page)
            if page % 25 == 0:
                logger.info("Listing page %s/%s — %s decisions queued", page, last, len(seen))
            time.sleep(LIST_DELAY)

        if not seen:
            raise RuntimeError(
                "Walked the whole register and found zero decisions with a PDF "
                "link — uvo.gov.sk is blocking this vantage or the layout changed"
            )
        logger.info("Listing complete — %s decisions with full text", len(seen))

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Recent decisions only — the register is sorted newest-first on page 1."""
        if isinstance(since, datetime):
            since_iso = since.date().isoformat()
        else:
            since_iso = str(since)[:10]
        self._use_checkpoint = False
        resp = self._get(LIST_URL, params={"page": 1})
        last = self._last_page()
        page = 1
        while page <= last:
            if page > 1:
                resp = self._get(LIST_URL, params={"page": page})
            rows = self._parse_page(resp.text, page)
            stale = 0
            for row in rows:
                if row["date"] and row["date"] < since_iso:
                    stale += 1
                    continue
                yield row
            # page 1 is newest-first; once a whole page is older than `since`, stop
            if rows and stale == len(rows):
                return
            page += 1
            time.sleep(LIST_DELAY)

    # ------------------------------------------------------------- normalize

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url")
        if not pdf_url:
            return None
        try:
            resp = self._get(pdf_url)
        except Exception as exc:  # noqa: BLE001 — logged, record skipped
            logger.warning("PDF download failed for %s: %s", raw.get("doc_id"), exc)
            return None

        content = resp.content
        # Content-Type is application/octet-stream — sniff the magic instead.
        if not content.startswith(b"%PDF-"):
            logger.warning(
                "Not a PDF for %s (%s bytes, starts %r)",
                raw.get("doc_id"), len(content), content[:16],
            )
            return None

        text = clean_pdf_text(extract_pdf_text(content))
        if len(text) < 200:
            logger.warning(
                "Empty/short extraction for %s (%s chars) — likely a scan",
                raw.get("doc_id"), len(text),
            )
            return None

        decision_no = raw.get("decision_no") or ""
        authority = raw.get("authority") or ""
        subject = raw.get("subject") or ""
        head = "Rozhodnutie o námietkach"
        if decision_no:
            head += f" {decision_no}"
        title = " — ".join([head] + [b for b in (authority, subject) if b])

        return {
            "_id": f"sk-uvo-namietky-{raw['doc_id']}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": raw.get("date"),
            "url": pdf_url,
            "country": "SK",
            "language": "sk",
            "court": "Úrad pre verejné obstarávanie (Public Procurement Office)",
            "decision_number": decision_no or None,
            "contracting_authority": authority or None,
            "contract_subject": subject or None,
            "notice_reference": raw.get("notice_ref") or None,
            "legal_ground": raw.get("legal_ground") or None,
            "outcome": raw.get("outcome") or None,
            "document_type": "rozhodnutie o námietkach",
            "register": "všeobecná agenda",
        }

    # ------------------------------------------------------------------ test

    def test_api(self) -> bool:
        logger.info("Testing uvo.gov.sk ...")
        try:
            last = self._last_page()
            resp = self._get(LIST_URL, params={"page": 1})
            rows = self._parse_page(resp.text, 1)
            logger.info("  listing OK — %s pages, %s PDF-backed rows on page 1", last, len(rows))
            if not rows:
                logger.error("  page 1 carried no PDF links")
                return False
            rec = self.normalize(rows[0])
            if not rec or not rec.get("text"):
                logger.error("  full-text extraction failed for %s", rows[0]["doc_id"])
                return False
            logger.info(
                "  %s OK — %s chars, date=%s, no=%s",
                rec["_id"], len(rec["text"]), rec["date"], rec["decision_number"],
            )
            logger.info("API test PASSED")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("API test FAILED: %s", exc)
            return False


def main():
    parser = argparse.ArgumentParser(description="SK/UVO-Namietky bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api", "updates"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = SKUvoNamietkyScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    if args.command == "updates":
        since = args.since or datetime.now(timezone.utc).date().isoformat()
        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                print(json.dumps(rec, ensure_ascii=False))
        return

    if args.sample:
        scraper._use_checkpoint = False
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
