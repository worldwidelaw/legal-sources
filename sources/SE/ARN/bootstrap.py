#!/usr/bin/env python3
"""
SE/ARN -- Allmänna reklamationsnämnden (Swedish National Board for Consumer
Complaints), vägledande beslut (guiding decisions).

ARN is the Swedish state consumer ADR body: it hears ~10,000 consumer
disputes a year and issues recommendations that traders follow in the large
majority of cases. Only the *vägledande* (guiding) decisions — the referat
the Board itself selects as precedent — are published, so the ~137 documents
here are the full public corpus, not a sample of it.

Strategy (single server-rendered index, no API, no auth):

  GET /om-arn/vagledande-beslut/
      -> 200, one page carrying every referat. The page is a sequence of

           <h3>{Ärendeområde}, beslut {YYYY-MM-DD}</h3>
           <p>… editorial summary …</p>
           <p><a href="/globalassets/…/referat-{YYYY}/…pdf">Referat {caseno}</a></p>
           <hr />

         blocks, which give the category, the decision date and the
         summary alongside the link to the full referat PDF.

  The PDFs are born-digital and extract cleanly with the shared
  ``common/pdf_extract`` backends.

GOTCHAS:
  - The folder year is the year of PUBLICATION, not of the decision:
    referat-2019/ holds 2018 case numbers and referat-2018/ holds 2017 ones.
    The date therefore comes from the index heading (or the PDF's own
    "Beslut YYYY-MM-DD; {caseno}" line), never from the URL.
  - Two referat cover a pair of jointly decided cases and carry both case
    numbers in the filename ("referat-2017-07814-referat-2017-13660.pdf").
    Both numbers are kept in ``case_numbers``; the first one anchors ``_id``.
  - Referat published from ~2019 on carry a page footer stamped upside-down,
    which the extractor reads as a reversed case number and date
    ("24770-6202 30-70-6202" for 2026-07742 / 2026-07-03). Those lines are
    stripped, and the reversed date is used as a date fallback.
  - The index carries a few section headings that are not decisions; blocks
    without a referat link are ignored.

Usage:
  python bootstrap.py bootstrap            # Full pull (all referat)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample referat
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import _extract as extract_pdf_text  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SE.ARN")

SOURCE_ID = "SE/ARN"
BASE_URL = "https://www.arn.se"
INDEX_URL = f"{BASE_URL}/om-arn/vagledande-beslut/"

USER_AGENT = (
    "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://legaldatahunter.com)"
)
REQUEST_DELAY = 1.0
MAX_RETRIES = 5
MIN_TEXT_CHARS = 400

TAG_RE = re.compile(r"<[^>]+>")
BLOCK_RE = re.compile(r"<h3[^>]*>(.*?)</h3>(.*?)(?=<h3[^>]*>|<h2[^>]*>|\Z)", re.S | re.I)
PDF_HREF_RE = re.compile(r'href="([^"]*/pdfer/[^"]+\.pdf)"', re.I)
PARA_RE = re.compile(r"<p[^>]*>(.*?)</p>", re.S | re.I)

# "Bostad, beslut 2026-07-02" / "Allmänna, Beslut 2025-12-29"
HEADING_RE = re.compile(r"^(.*?),\s*beslut\s*(\d{4}-\d{2}-\d{2})\s*$", re.I)
ISO_DATE_RE = re.compile(r"\b((?:19|20)\d{2}-\d{2}-\d{2})\b")
# "Beslut 2018-05-29; 2017-13781" in the head of the older referat
BESLUT_LINE_RE = re.compile(
    r"Beslut\s+((?:19|20)\d{2}-\d{2}-\d{2})\s*;\s*((?:19|20)\d{2}-\d{4,5})", re.I
)
CASE_NO_RE = re.compile(r"((?:19|20)\d{2})-(\d{4,5})")
# The upside-down page stamp, read back-to-front: "24770-6202 30-70-6202"
FLIPPED_STAMP_RE = re.compile(r"^[\d\- ]{8,40}$")


def strip_html(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", unescape(TAG_RE.sub(" ", value))).strip()


def is_flipped_stamp(line: str) -> bool:
    """True for a page footer the extractor read upside-down.

    The stamp is the case number and the decision date rotated 180°, so it
    is recognised by reversing the line and matching the two real formats.
    """
    stripped = line.strip()
    if not stripped or not FLIPPED_STAMP_RE.match(stripped):
        return False
    tokens = stripped.split()
    if not tokens:
        return False
    return all(
        re.fullmatch(r"(?:19|20)\d{2}-\d{2,5}|(?:19|20)\d{2}-\d{2}-\d{2}", t[::-1])
        for t in tokens
    )


def flipped_stamp_date(text: str) -> Optional[str]:
    """The decision date carried by the upside-down page stamp, if any."""
    for line in text.split("\n"):
        if not is_flipped_stamp(line):
            continue
        for token in line.split():
            candidate = token[::-1]
            if re.fullmatch(r"(?:19|20)\d{2}-\d{2}-\d{2}", candidate):
                return candidate
    return None


def clean_pdf_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    kept = [ln for ln in text.split("\n") if not is_flipped_stamp(ln)]
    text = "\n".join(kept)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def case_numbers_from(value: str) -> list[str]:
    """Every ARN case number in a filename or a heading, in order, deduped."""
    out: list[str] = []
    for year, seq in CASE_NO_RE.findall(value):
        number = f"{year}-{seq}"
        if number not in out:
            out.append(number)
    return out


class SEArnScraper(BaseScraper):
    """Scraper for the ARN guiding-decisions index."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/pdf,*/*;q=0.8",
                "Accept-Language": "sv,en;q=0.8",
            }
        )
        self._short_text: list[str] = []

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str) -> requests.Response:
        delay = 2.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=180)
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
                if getattr(exc.response, "status_code", None) == 404:
                    raise
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

    # --------------------------------------------------------------- listing

    def _index_items(self) -> list[dict]:
        html = self._get(INDEX_URL).text
        items: list[dict] = []
        seen: set[str] = set()

        for raw_heading, body in BLOCK_RE.findall(html):
            hrefs = PDF_HREF_RE.findall(body)
            if not hrefs:
                # A section heading rather than a decision.
                continue
            heading = strip_html(raw_heading)
            category, date = None, None
            match = HEADING_RE.match(heading)
            if match:
                category, date = match.group(1).strip(), match.group(2)
            else:
                iso = ISO_DATE_RE.search(heading)
                date = iso.group(1) if iso else None
                category = ISO_DATE_RE.sub("", heading).strip(" ,;") or None

            summary = " ".join(
                text
                for text in (strip_html(p) for p in PARA_RE.findall(body))
                if text and not text.lower().startswith("referat")
            ).strip()

            for href in hrefs:
                url = urljoin(BASE_URL, href)
                if url in seen:
                    continue
                seen.add(url)
                items.append(
                    {
                        "pdf_url": url,
                        "category": category,
                        "index_date": date,
                        "summary": summary,
                        "case_numbers": case_numbers_from(href.rsplit("/", 1)[-1]),
                    }
                )

        # Any referat the block walk missed (a layout change would show up here).
        for href in PDF_HREF_RE.findall(html):
            url = urljoin(BASE_URL, href)
            if url in seen:
                continue
            seen.add(url)
            logger.warning("Referat outside a decision block: %s", url)
            items.append(
                {
                    "pdf_url": url,
                    "category": None,
                    "index_date": None,
                    "summary": "",
                    "case_numbers": case_numbers_from(href.rsplit("/", 1)[-1]),
                }
            )

        if not items:
            raise RuntimeError(
                f"{INDEX_URL} returned no referat PDFs — the page layout changed "
                "or arn.se is blocking this vantage"
            )
        return items

    def fetch_all(self) -> Generator[dict, None, None]:
        items = self._index_items()
        logger.info("ARN guiding decisions: %s referat", len(items))
        for item in items:
            yield item

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Referat decided on or after ``since`` — the index is one page."""
        if isinstance(since, datetime):
            since_iso = since.date().isoformat()
        else:
            since_iso = str(since)[:10]
        for item in self._index_items():
            if not item["index_date"] or item["index_date"] >= since_iso:
                yield item

    # ------------------------------------------------------------- normalize

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url")
        if not pdf_url:
            return None
        try:
            resp = self._get(pdf_url)
        except Exception as exc:  # noqa: BLE001 — logged, record skipped
            logger.warning("PDF download failed for %s: %s", pdf_url, exc)
            return None

        content = resp.content
        if not content.startswith(b"%PDF-"):
            logger.warning(
                "Not a PDF at %s (%s bytes, starts %r)",
                pdf_url, len(content), content[:16],
            )
            return None

        extracted = extract_pdf_text(content)
        text = clean_pdf_text(extracted)
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(
                "Insufficient text for %s (%s chars) — skipped", pdf_url, len(text)
            )
            self._short_text.append(pdf_url)
            return None

        case_numbers = list(raw.get("case_numbers") or [])
        date = raw.get("index_date")

        # The older referat state both in the head of the document.
        beslut = BESLUT_LINE_RE.search(text[:4000])
        if beslut:
            date = date or beslut.group(1)
            if beslut.group(2) not in case_numbers:
                case_numbers.append(beslut.group(2))
        if not date:
            date = flipped_stamp_date(extracted or "")

        if not case_numbers:
            logger.warning("No case number for %s — skipped", pdf_url)
            return None

        title = f"ARN {'/'.join(case_numbers)}"
        if raw.get("category"):
            title = f"{title} — {raw['category']}"

        return {
            "_id": f"arn-{case_numbers[0]}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": date,
            "url": pdf_url,
            "country": "SE",
            "language": "sv",
            "court": "Allmänna reklamationsnämnden",
            "case_number": case_numbers[0],
            "case_numbers": case_numbers,
            "category": raw.get("category"),
            "summary": raw.get("summary") or None,
            "index_url": INDEX_URL,
        }

    # ------------------------------------------------------------------ test

    def test_api(self) -> bool:
        logger.info("Testing arn.se guiding-decisions index ...")
        try:
            items = self._index_items()
            logger.info("  index OK — %s referat", len(items))
            for item in items:
                rec = self.normalize(item)
                if rec and rec.get("text"):
                    logger.info(
                        "  %s OK — %s chars, date=%s, category=%s",
                        rec["_id"], len(rec["text"]), rec["date"], rec["category"],
                    )
                    logger.info("API test PASSED")
                    return True
            logger.error("  no referat yielded full text")
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("API test FAILED: %s", exc)
            return False


def main():
    parser = argparse.ArgumentParser(description="SE/ARN bootstrap")
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

    scraper = SEArnScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        if scraper._short_text:
            logger.info("Referat with too little text: %s", scraper._short_text)
        return

    if args.command == "updates":
        since = args.since or datetime.now(timezone.utc).date().isoformat()
        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                print(json.dumps(rec, ensure_ascii=False))
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")
    if scraper._short_text:
        logger.info("Referat with too little text: %s", scraper._short_text)


if __name__ == "__main__":
    main()
