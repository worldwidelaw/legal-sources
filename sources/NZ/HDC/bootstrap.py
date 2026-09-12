#!/usr/bin/env python3
"""
NZ/HDC -- Health and Disability Commissioner Decisions

Fetches the Commissioner's formal opinions on complaints about New Zealand
health and disability services.

Strategy:
  - Walk /decisions/search-decisions/?page=N (12 tiles per page). Each tile
    carries the decision URL, its title and its publication date, so the date
    comes from the index rather than being guessed from the case reference
    (a reference like 21HDC02726 encodes the COMPLAINT year, not the decision
    year — that decision was published in 2026).
  - Fetch each decision page and take the full text from the in-page
    `c-rte__body-text` block.
  - Fall back to the linked born-digital PDF when the page carries no body.

Source: https://www.hdc.org.nz/decisions/search-decisions/ (NZ Crown entity)
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.HDC")

BASE_URL = "https://www.hdc.org.nz"
LISTING_PATH = "/decisions/search-decisions/"

REQUEST_TIMEOUT = (10, 30)
REQUEST_WALL_TIMEOUT = 90

# A host that starts dropping packets looks exactly like the end of the corpus,
# so transport failures abort loudly instead of reporting a truncated crawl.
MAX_CONSECUTIVE_TRANSPORT_ERRORS = 12

MIN_HTML_TEXT = 1500
MIN_TEXT = 250
PROGRESS_EVERY = 100

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

TILE_RE = re.compile(
    r'<a[^>]+href="(/decisions/search-decisions/[^"?#]+/)"[^>]*>\s*'
    r'<div class="o-tile__body">\s*'
    r'<h3[^>]*>(.*?)</h3>\s*'
    r'<p[^>]*>(.*?)</p>',
    re.S,
)


def _strip_html(fragment: str) -> str:
    fragment = re.sub(r"<script.*?</script>|<style.*?</style>", "", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"</(p|li|h\d|div|tr|blockquote)>", "\n", fragment, flags=re.I)
    text = html_mod.unescape(re.sub(r"<[^>]+>", "", fragment)).replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _parse_date(raw: str) -> Optional[str]:
    """'17 Aug 2026' -> '2026-08-17'."""
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})", raw or "")
    if not m:
        return None
    month = MONTHS.get(m.group(2).lower())
    if not month:
        return None
    return f"{int(m.group(3)):04d}-{month:02d}-{int(m.group(1)):02d}"


class HDCScraper(BaseScraper):
    """
    Scraper for NZ/HDC -- Health and Disability Commissioner.
    Country: NZ
    URL: https://www.hdc.org.nz/decisions/search-decisions/
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

    def _get(self, url: str):
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.debug(f"Transport error on {url}: {e}")
            return None
        if resp is None or resp.status_code in (429, 500, 502, 503, 504):
            return None
        return resp

    # ── Discovery ─────────────────────────────────────────────────────

    def _discover_last_page(self) -> Optional[int]:
        resp = self._get(f"{BASE_URL}{LISTING_PATH}")
        if resp is None or resp.status_code != 200:
            return None
        pages = [int(p) for p in re.findall(r"\?page=(\d+)", resp.text)]
        return max(pages) if pages else None

    def _listing_page(self, page: int) -> Optional[list]:
        """Return [(url, title, date_iso)] for one index page, None on failure."""
        url = f"{BASE_URL}{LISTING_PATH}?page={page}"
        resp = self._get(url)
        if resp is None:
            return None
        if resp.status_code != 200:
            return []
        tiles = []
        for path, title, meta in TILE_RE.findall(resp.text):
            tiles.append((urljoin(BASE_URL, path), _strip_html(title), _parse_date(_strip_html(meta))))
        return tiles

    def _iter_tiles(self) -> Generator[tuple, None, None]:
        last_page = self._discover_last_page()
        if last_page:
            logger.info(f"Listing advertises {last_page} pages")
        else:
            logger.warning("Could not read pagination — crawling until pages run dry")

        seen = set()
        page = 1
        transport_errors = 0
        empty_pages = 0

        while True:
            tiles = self._listing_page(page)

            if tiles is None:
                transport_errors += 1
                if transport_errors >= MAX_CONSECUTIVE_TRANSPORT_ERRORS:
                    raise RuntimeError(
                        f"{MAX_CONSECUTIVE_TRANSPORT_ERRORS} consecutive transport failures on "
                        f"{LISTING_PATH}?page={page} — hdc.org.nz is refusing this vantage; "
                        f"refusing to report a truncated corpus"
                    )
                continue
            transport_errors = 0

            if not tiles:
                empty_pages += 1
                if empty_pages >= 2:
                    break
            else:
                empty_pages = 0

            for url, title, date_iso in tiles:
                if url in seen:
                    continue
                seen.add(url)
                yield url, title, date_iso

            if self.max_records and len(seen) >= self.max_records:
                return

            page += 1
            if last_page and page > last_page:
                break
            if page > 5000:
                break

        if not seen:
            raise RuntimeError(
                "Discovery found 0 decisions on /decisions/search-decisions/ — the tile layout "
                "changed or the host is serving a challenge page"
            )
        expected = (last_page or 0) * 12
        if expected and len(seen) < expected * 0.85:
            self.record_coverage_gap(
                "search-decisions", "index walk returned fewer decisions than advertised",
                found=len(seen), expected=expected,
            )
            logger.warning(f"Discovered only {len(seen)} of ~{expected} advertised decisions")
        else:
            self.clear_coverage_gap("search-decisions")
            logger.info(f"Discovered {len(seen)} decisions")

    # ── Parsing ───────────────────────────────────────────────────────

    def _parse_decision(self, url: str, title: str, date_iso: Optional[str]) -> Optional[dict]:
        resp = self._get(url)
        if resp is None:
            return None
        if resp.status_code != 200:
            return {}
        page = resp.text

        if not title:
            h1 = re.search(r"<h1[^>]*>(.*?)</h1>", page, re.S)
            title = _strip_html(h1.group(1)) if h1 else ""
        if not title:
            return {}

        body = ""
        i = page.find("c-rte__body-text")
        if i >= 0:
            tag_end = page.find(">", i)
            segment = page[tag_end + 1:] if tag_end > 0 else page[i:]
            cut = segment.find("</main>")
            body = _strip_html(segment[:cut] if cut > 0 else segment)

        pdf_m = re.search(r'href="(/media/[^"]+\.pdf)"', page, re.I)
        pdf_url = urljoin(BASE_URL, html_mod.unescape(pdf_m.group(1))) if pdf_m else None

        # Case reference is the last URL segment, e.g. .../2026/21hdc02726/
        ref = url.rstrip("/").rsplit("/", 1)[-1].upper()

        return {
            "url": url,
            "ref": ref,
            "title": title,
            "date": date_iso,
            "html_text": body,
            "pdf_url": pdf_url,
        }

    # ── BaseScraper interface ─────────────────────────────────────────

    def iter_sample_raw(self, n: int = 15) -> Generator[dict, None, None]:
        """Yield ~n raw decisions spread evenly across the index.

        Sampling only page 1 would validate the newest decisions and never touch
        the older end of the corpus, where layout drift tends to hide.
        """
        last_page = self._discover_last_page() or n
        step = max(1, (last_page - 1) // max(1, n - 1))
        pages = sorted({min(last_page, 1 + i * step) for i in range(n)})

        for page in pages:
            tiles = self._listing_page(page)
            if not tiles:
                continue
            raw = self._parse_decision(*tiles[0])
            if raw:
                yield raw

    def fetch_all(self) -> Generator[dict, None, None]:
        fetched = 0
        for url, title, date_iso in self._iter_tiles():
            raw = self._parse_decision(url, title, date_iso)
            if raw is None:
                logger.warning(f"Transport failure on {url} — skipping")
                continue
            if not raw:
                continue
            yield raw
            fetched += 1
            if fetched % PROGRESS_EVERY == 0:
                logger.info(f"Fetched {fetched} decisions")
            if self.max_records and fetched >= self.max_records:
                break
        logger.info(f"fetch_all complete: {fetched} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published on/after `since` (index is newest first)."""
        cutoff = since.date().isoformat()
        stale_streak = 0
        for raw in self.fetch_all():
            if raw.get("date") and raw["date"] < cutoff:
                stale_streak += 1
                if stale_streak >= 24:
                    logger.info(f"Reached decisions older than {cutoff} — stopping update walk")
                    return
                continue
            stale_streak = 0
            yield raw

    def normalize(self, raw: dict) -> dict:
        text = raw.get("html_text") or ""
        if len(text) < MIN_HTML_TEXT and raw.get("pdf_url"):
            try:
                pdf_text = extract_pdf_markdown(
                    "NZ/HDC", raw["ref"], pdf_url=raw["pdf_url"], table="case_law"
                )
            except Exception as e:
                logger.debug(f"PDF extraction failed for {raw.get('url')}: {e}")
                pdf_text = None
            if pdf_text and len(pdf_text) > len(text):
                text = pdf_text

        return {
            "_id": f"hdc-{raw['ref'].lower()}",
            "_source": "NZ/HDC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "case_reference": raw.get("ref"),
            "court": "Health and Disability Commissioner",
            "jurisdiction": "New Zealand",
            "pdf_url": raw.get("pdf_url"),
            "language": "en",
        }

    def _dedup_key(self, record: dict) -> str:
        return record.get("_id", "")

    def test_api(self) -> bool:
        last = self._discover_last_page()
        if not last:
            logger.error("Could not read the decision index at hdc.org.nz")
            return False
        logger.info(f"OK — index advertises {last} pages (~{last * 12} decisions)")
        tiles = self._listing_page(1)
        if not tiles:
            logger.error("Index page 1 yielded no decision tiles")
            return False
        raw = self._parse_decision(*tiles[0])
        if not raw:
            logger.error("Could not parse the newest decision page")
            return False
        rec = self.normalize(raw)
        logger.info(f"OK — {rec['_id']} ({rec['date']}): {len(rec['text'])} chars")
        return len(rec["text"]) >= MIN_TEXT


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NZ/HDC bootstrap")
    # The fleet wrapper invokes `bootstrap-fast`; without it argparse exits 2
    # and the wrapper falls back to re-ingesting sample/.
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Full fetch (all records)")
    args = parser.parse_args()

    scraper = HDCScraper()

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
                f"[{count}] {record['_id']} — {record['title'][:60]} ({len(record['text'])} chars)"
            )
            if count >= 15:
                break
        logger.info(f"Done: {count} sample records fetched")
        return

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
