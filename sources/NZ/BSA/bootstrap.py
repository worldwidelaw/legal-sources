#!/usr/bin/env python3
"""
NZ/BSA -- Broadcasting Standards Authority Decisions

Fetches the complete set of BSA complaint decisions (1990-present) from the
Authority's own website.

Strategy:
  - Walk /decisions/all-decisions/?start=N (10 decisions per page, newest first)
    to enumerate every decision URL. The pagination block advertises the last
    ``start`` offset, so the expected corpus size is known up front and a short
    crawl fails loudly instead of silently truncating.
  - Fetch each decision page and take the full text from the in-page
    ``wysiwyg-content`` block.
  - Decisions from the early 1990s have NO in-page text — the page is a stub
    that only links a born-digital PDF. Those fall back to PDF extraction.
    Handling only the HTML era would silently drop the oldest ~330 decisions
    (see issue #1262 / #1420 for the same class of era-shaped data loss).

Source: https://www.bsa.govt.nz/decisions/ (NZ Crown entity, open access)
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap-fast       # Same, concurrent normalize (fleet entry point)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import json
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.BSA")

BASE_URL = "https://www.bsa.govt.nz"
LISTING_PATH = "/decisions/all-decisions/"
PAGE_SIZE = 10

# `timeout` is per socket operation, so `wall_timeout` is what actually bounds
# one fetch (urllib3 retries and Retry-After sleeps happen inside it).
REQUEST_TIMEOUT = (10, 30)
REQUEST_WALL_TIMEOUT = 90

# A host that starts dropping our packets looks exactly like the end of the
# corpus, so transport failures are counted separately from honest empty pages
# and abort the run loudly rather than reporting a short crawl as a success.
MAX_CONSECUTIVE_TRANSPORT_ERRORS = 12

# Below this the in-page body is a stub (the pre-1994 decisions render as
# "Download a PDF of Decision No. X" and nothing else) and the PDF is the only
# real text.
MIN_HTML_TEXT = 1200

# Below this a record carries no usable document body at all and is dropped.
MIN_TEXT = 250

PROGRESS_EVERY = 100


def _strip_html(fragment: str) -> str:
    """Turn an HTML fragment into readable plain text."""
    fragment = re.sub(r"<script.*?</script>|<style.*?</style>", "", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"</(p|li|h\d|div|tr|blockquote)>", "\n", fragment, flags=re.I)
    text = html_mod.unescape(re.sub(r"<[^>]+>", "", fragment))
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _resolve_asset(href: str) -> str:
    """Resolve a decision-page asset href against the site root.

    The legacy decision stubs link their PDF as ``images/assets/PDF-Decisions/...``
    — no leading slash — but the file lives at the site root, not under the
    decision's own path. Joining against the page URL yields a 404 and silently
    empties every pre-1994 decision.
    """
    if href.startswith(("http://", "https://")):
        return href
    return urljoin(BASE_URL + "/", href.lstrip("/"))


class BSAScraper(BaseScraper):
    """
    Scraper for NZ/BSA -- Broadcasting Standards Authority.
    Country: NZ
    URL: https://www.bsa.govt.nz/decisions/
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-NZ,en;q=0.9",
            },
            timeout=REQUEST_TIMEOUT,
            wall_timeout=REQUEST_WALL_TIMEOUT,
        )
        self.max_records: Optional[int] = None

    # ── HTTP ──────────────────────────────────────────────────────────

    def _get(self, url: str) -> Optional[requests.Response]:
        """GET with the shared rate limiter. Returns None on transport failure."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.debug(f"Transport error on {url}: {e}")
            return None
        if resp is None or resp.status_code in (429, 500, 502, 503, 504):
            logger.debug(f"Retryable status on {url}: {getattr(resp, 'status_code', None)}")
            return None
        return resp

    # ── Discovery ─────────────────────────────────────────────────────

    def _discover_total(self) -> Optional[int]:
        """Read the expected decision count from the pagination block.

        The last pagination link is ``?start=N`` for the final page, so the
        corpus holds N + PAGE_SIZE decisions at most. Knowing this up front is
        what lets a truncated crawl fail loudly.
        """
        resp = self._get(f"{BASE_URL}{LISTING_PATH}")
        if resp is None or resp.status_code != 200:
            return None
        starts = [int(s) for s in re.findall(r"\?start=(\d+)", resp.text)]
        if not starts:
            return None
        return max(starts) + PAGE_SIZE

    def _listing_page(self, start: int) -> Optional[list]:
        """Return the decision paths on one listing page, or None on failure."""
        url = f"{BASE_URL}{LISTING_PATH}?start={start}" if start else f"{BASE_URL}{LISTING_PATH}"
        resp = self._get(url)
        if resp is None:
            return None
        if resp.status_code != 200:
            logger.debug(f"Listing start={start} returned {resp.status_code}")
            return []
        # dict.fromkeys keeps first-seen order while dropping duplicates.
        return list(dict.fromkeys(re.findall(r'href="(/decisions/all-decisions/[^"?#]+/)"', resp.text)))

    def _iter_decision_urls(self) -> Generator[str, None, None]:
        """Yield every decision URL, newest first."""
        expected = self._discover_total()
        if expected:
            logger.info(f"Listing advertises ~{expected} decisions")
        else:
            logger.warning("Could not read pagination — crawling until pages run dry")

        seen = set()
        start = 0
        transport_errors = 0
        empty_pages = 0

        while True:
            paths = self._listing_page(start)

            if paths is None:
                transport_errors += 1
                if transport_errors >= MAX_CONSECUTIVE_TRANSPORT_ERRORS:
                    raise RuntimeError(
                        f"{MAX_CONSECUTIVE_TRANSPORT_ERRORS} consecutive transport failures "
                        f"fetching {LISTING_PATH}?start={start} — bsa.govt.nz is refusing or "
                        f"dropping this vantage; refusing to report a truncated corpus"
                    )
                continue  # retry the same offset
            transport_errors = 0

            fresh = [p for p in paths if p not in seen]
            if not paths:
                empty_pages += 1
                # One empty page can be a blip; two in a row is the end.
                if empty_pages >= 2:
                    break
            else:
                empty_pages = 0

            for p in fresh:
                seen.add(p)
                yield urljoin(BASE_URL, p)

            if self.max_records and len(seen) >= self.max_records:
                return

            start += PAGE_SIZE
            if expected and start >= expected + PAGE_SIZE:
                break
            if start > 100000:  # runaway guard
                break

        if not seen:
            raise RuntimeError(
                "Discovery found 0 decision URLs on /decisions/all-decisions/ — the listing "
                "layout changed or the host is serving us a challenge page"
            )
        if expected and len(seen) < expected * 0.9:
            self.record_coverage_gap(
                "all-decisions",
                "listing walk returned fewer decisions than the pagination advertises",
                found=len(seen),
                expected=expected,
            )
            logger.warning(f"Discovered only {len(seen)} of ~{expected} advertised decisions")
        else:
            self.clear_coverage_gap("all-decisions")
            logger.info(f"Discovered {len(seen)} decision URLs")

    # ── Parsing ───────────────────────────────────────────────────────

    @staticmethod
    def _parse_details(page: str) -> dict:
        """Pull the labelled metadata boxes out of the decision-details block."""
        block = re.search(
            r'<div class="row decision-details".*?>(.*?)(?=<div class="content")',
            page, re.S,
        )
        if not block:
            return {}
        details = {}
        for label, value in re.findall(
            r'<div class="col-sm-\d+">\s*<h5>(.*?)</h5>(.*?)</div>', block.group(1), re.S
        ):
            key = _strip_html(label)
            val = re.sub(r"\s*\n\s*", "; ", _strip_html(value)).strip("; ")
            if key and val:
                details[key] = val
        return details

    @staticmethod
    def _parse_body(page: str) -> str:
        """Return the in-page decision text, or '' when the page is a PDF stub."""
        anchor = page.find('<div class="content">')
        start = page.find('wysiwyg-content', anchor if anchor >= 0 else 0)
        if start < 0:
            return ""
        # Start after the opening tag closes, or the class name itself leaks
        # into the extracted text.
        tag_end = page.find(">", start)
        segment = page[tag_end + 1:] if tag_end > 0 else page[start:]
        # "Next Decision" opens the sibling-navigation block that follows the body.
        cut = segment.find("Next Decision")
        if cut < 0:
            cut = segment.find("</main>")
        return _strip_html(segment[:cut] if cut > 0 else segment)

    def _parse_decision(self, url: str) -> Optional[dict]:
        """Fetch and parse one decision page into a raw dict."""
        resp = self._get(url)
        if resp is None:
            return None
        if resp.status_code != 200:
            logger.debug(f"Decision {url} returned {resp.status_code}")
            return {}
        page = resp.text

        title_m = re.search(r'<article class="typography">\s*<h2>(.*?)</h2>', page, re.S)
        if not title_m:
            title_m = re.search(r"<title>(.*?)</title>", page, re.S)
        title = _strip_html(title_m.group(1)) if title_m else ""
        title = re.sub(r"\s*\|\s*Broadcasting Standards Authority\s*$", "", title).strip()
        if not title:
            return {}

        details = self._parse_details(page)
        body = self._parse_body(page)

        date_m = re.search(r'<time datetime="(\d{4}-\d{2}-\d{2})', page)
        date_iso = date_m.group(1) if date_m else None

        pdf_url = None
        pdf_m = re.search(r'href="([^"]*PDF-Decisions[^"]*\.pdf)"', page, re.I)
        if not pdf_m:
            pdf_m = re.search(r'href="([^"]+\.pdf)"', page, re.I)
        if pdf_m:
            pdf_url = _resolve_asset(html_mod.unescape(pdf_m.group(1)))

        number = details.get("Number") or ""
        slug = url.rstrip("/").rsplit("/", 1)[-1]

        return {
            "url": url,
            "slug": slug,
            "title": title,
            "number": number,
            "date": date_iso,
            "details": details,
            "html_text": body,
            "pdf_url": pdf_url,
        }

    # ── BaseScraper interface ─────────────────────────────────────────

    def iter_sample_raw(self, n: int = 15) -> Generator[dict, None, None]:
        """Yield ~n raw decisions spread evenly across the whole corpus.

        Sampling only the newest page would validate the HTML era and never
        touch the pre-1994 PDF-only decisions, which is exactly where this
        source breaks. Striding the listing keeps both eras in sample/.
        """
        total = self._discover_total() or PAGE_SIZE * n
        last_page = max(0, (total - 1) // PAGE_SIZE)
        step = max(1, last_page // max(1, n - 1))
        offsets = sorted({min(last_page, i * step) * PAGE_SIZE for i in range(n)})

        for offset in offsets:
            paths = self._listing_page(offset)
            if not paths:
                continue
            raw = self._parse_decision(urljoin(BASE_URL, paths[0]))
            if raw:
                yield raw

    def fetch_all(self) -> Generator[dict, None, None]:
        fetched = 0
        missing = 0
        for i, url in enumerate(self._iter_decision_urls(), 1):
            raw = self._parse_decision(url)
            if raw is None:
                logger.warning(f"Transport failure on {url} — skipping")
                continue
            if not raw:
                missing += 1
                continue
            yield raw
            fetched += 1
            if fetched % PROGRESS_EVERY == 0:
                logger.info(f"Fetched {fetched} decisions (page walk at #{i})")
            if self.max_records and fetched >= self.max_records:
                break
        logger.info(f"fetch_all complete: {fetched} decisions, {missing} unparseable pages")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions dated on/after `since`.

        The listing is newest-first, so the walk stops at the first page whose
        decisions all predate the cutoff.
        """
        cutoff = since.date().isoformat()
        stale_streak = 0
        for raw in self.fetch_all():
            if raw.get("date") and raw["date"] < cutoff:
                stale_streak += 1
                if stale_streak >= PAGE_SIZE * 2:
                    logger.info(f"Reached decisions older than {cutoff} — stopping update walk")
                    return
                continue
            stale_streak = 0
            yield raw

    def normalize(self, raw: dict) -> dict:
        details = raw.get("details") or {}
        text = raw.get("html_text") or ""

        # Pre-1994 decisions are PDF-only stubs; without this fallback the
        # oldest era of the corpus would land as empty-text rows.
        if len(text) < MIN_HTML_TEXT and raw.get("pdf_url"):
            source_id = raw.get("number") or raw.get("slug")
            try:
                pdf_text = extract_pdf_markdown(
                    "NZ/BSA", source_id, pdf_url=raw["pdf_url"], table="case_law"
                )
            except Exception as e:
                logger.debug(f"PDF extraction failed for {raw.get('url')}: {e}")
                pdf_text = None
            if pdf_text and len(pdf_text) > len(text):
                text = pdf_text

        number = raw.get("number") or ""
        doc_id = f"bsa-{number}" if number else f"bsa-{raw.get('slug')}"

        record = {
            "_id": doc_id,
            "_source": "NZ/BSA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "decision_number": number or None,
            "court": "Broadcasting Standards Authority",
            "jurisdiction": "New Zealand",
            "members": details.get("Members"),
            "complainant": details.get("Complainant"),
            "broadcaster": details.get("Broadcaster"),
            "programme": details.get("Programme"),
            "channel": details.get("Channel/Station"),
            "standards": details.get("Standards"),
            "standards_breached": details.get("Standards Breached"),
            "pdf_url": raw.get("pdf_url"),
            "language": "en",
        }
        return record

    def _dedup_key(self, record: dict) -> str:
        return record.get("_id", "")

    # ── Connectivity ──────────────────────────────────────────────────

    def test_api(self) -> bool:
        total = self._discover_total()
        if not total:
            logger.error("Could not read the decision listing at bsa.govt.nz")
            return False
        logger.info(f"OK — listing advertises ~{total} decisions")
        paths = self._listing_page(0)
        if not paths:
            logger.error("Listing page 0 yielded no decision links")
            return False
        raw = self._parse_decision(urljoin(BASE_URL, paths[0]))
        if not raw:
            logger.error("Could not parse the newest decision page")
            return False
        rec = self.normalize(raw)
        logger.info(f"OK — {rec['_id']}: {len(rec['text'])} chars")
        return len(rec["text"]) >= MIN_TEXT


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NZ/BSA bootstrap")
    # The fleet wrapper invokes `bootstrap-fast`; without it argparse exits 2
    # and the wrapper falls back to re-ingesting sample/.
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Full fetch (all records)")
    args = parser.parse_args()

    scraper = BSAScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.sample:
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in scraper.iter_sample_raw(15):
            record = scraper.normalize(raw)
            if len(record.get("text", "")) < MIN_TEXT:
                logger.warning(f"Skipping {record['_id']} — only {len(record.get('text',''))} chars")
                continue
            count += 1
            (sample_dir / f"{count:04d}.json").write_text(
                json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            logger.info(
                f"[{count}] {record['_id']} — {record['title'][:60]} "
                f"({len(record['text'])} chars)"
            )
            if count >= 15:
                break

        logger.info(f"Done: {count} sample records fetched")
        return

    # Full corpus — BaseScraper streams normalized records to data/records.jsonl.
    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap()

    logger.info(
        f"Done: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    if stats.get("error_message"):
        logger.error(f"Bootstrap failed: {stats['error_message']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
