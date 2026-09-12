#!/usr/bin/env python3
"""
US/GA-TaxTribunal -- Georgia Tax Tribunal (Decisions & Orders)

Fetches the full text of every decision and order of the Georgia Tax
Tribunal — Georgia's independent quasi-judicial tribunal (est. 2012,
operative 2013) that hears appeals of tax matters arising under the laws
administered by the Georgia Department of Revenue (income tax, sales/use
tax, property-tax digests, motor-fuel tax, penalty/refund disputes, etc.;
taxpayer v. Commissioner of the Georgia Department of Revenue). Each
"Decision" or "Order" resolves a specific tax controversy, so the corpus
is case_law.

The Tribunal publishes its decisions on a server-rendered Drupal listing
(gataxtribunal.georgia.gov/decisions), paginated 10 per page via ?page=N.
Each list item links the decision document under /document/decisions/{slug}/download
and carries a clean `data-text` caption of the form
"<parties> - <doc type>, <YYYY-N> Ga. Tax Tribunal, <Month D, YYYY>".
The documents are born-digital text-layer PDFs — no JavaScript, no
CAPTCHA, no auth.

Strategy:
  1. Walk the paginated /decisions listing and parse each row's PDF href
     and `data-text` caption (parties, citation, date).
  2. Download each PDF and extract its text layer via common.pdf_extract.
  3. Normalize into the standard case_law schema.

Vantage fallback (issue #1245):
  gataxtribunal.georgia.gov sits behind a Cloudflare rule that answers
  datacenter and non-US IPs with HTTP 410/403 on every path, so a
  live-only run off the fleet yields nothing (see #1115). The whole site
  is mirrored in the Internet Archive, so every fetch is live-first with
  a Wayback fallback:

      https://web.archive.org/web/3000id_/{url}

  ("3000" = latest capture, "id_" = raw bytes, no IA banner injection.)
  When the latest capture is itself an archived error body, we fall back
  to a CDX query (limit=-8), drop 403/404/429/5xx rows, KEEP revisit rows
  (statuscode "-", which the archive replays fine) and replay the newest
  survivor as /web/{ts}id_/{url}.

  Discovery runs off the archive too: a CDX prefix query over /decisions
  yields the newest good capture of each ?page=N listing (8 pages, all
  captured in 2026), which carry the same anchors + data-text captions as
  live. That anchor set is unioned with a CDX prefix query over
  /document/decisions, which surfaces decisions that have since rotated
  off the listing; their metadata is parsed out of the slug. Measured
  2026-07-30: 75 anchors from the archived listings + 5 listing-only-in-
  the-past documents = 80 decisions, 78 of them with a PDF capture.

  After three consecutive live failures with no live success the scraper
  latches into archive-only mode so it stops paying the Cloudflare block
  on every remaining URL.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample decisions
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.GA-TaxTribunal")

BASE_URL = "https://gataxtribunal.georgia.gov"
HOST = "gataxtribunal.georgia.gov"
INDEX_PATH = "/decisions"
DOC_PREFIX = "/document/decisions"
MAX_PAGES = 30  # safety ceiling; real corpus is ~8 pages (~80 docs)

# Internet Archive replay of the latest capture, raw bytes (no IA banner).
WAYBACK_LATEST = "https://web.archive.org/web/3000id_/"
WAYBACK_REPLAY = "https://web.archive.org/web/{ts}id_/{url}"
CDX_URL = (
    "http://web.archive.org/cdx/search/cdx?url={url}&matchType={match}"
    "&output=text&fl=timestamp,original,statuscode&limit={limit}"
)
# CDX statuscodes that mean "the archive stored an error body, not the doc".
# Revisit records carry "-" and replay fine, so they are deliberately kept.
BAD_STATUS = {"403", "404", "410", "429", "500", "502", "503", "504"}
# Consecutive live failures (with zero live successes) before we stop trying
# the live host at all and read everything from the archive.
LIVE_FAIL_LATCH = 3

# <a href="/document/decisions/{slug}/download" data-text="caption">
ROW_RE = re.compile(
    r'<a\s+href="(/document/decisions/[^"]+?)"\s+data-text="([^"]*)"',
    re.I,
)
# Citation like "2025-3 Ga. Tax Tribunal"
CITATION_RE = re.compile(r"(\d{4})-(\d+)\s+Ga\.?\s+Tax\s+Tribunal", re.I)
# Trailing date "Month D, YYYY"
DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
_MONTH_ALT = "|".join(MONTHS)
# Slugs spell out the same caption, e.g.
# "alice-d-doby-decision-2014-3-ga-tax-tribunal-february-17-2014pdf"
SLUG_CITATION_RE = re.compile(r"-(\d{4})-(\d+)-ga-tax-tribunal", re.I)
SLUG_DATE_RE = re.compile(rf"-({_MONTH_ALT})-(\d{{1,2}})-(\d{{4}})", re.I)


class GATaxTribunalScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        # Vantage state (#1245): Cloudflare 410/403s datacenter and non-US IPs.
        self._live_ok = False        # the origin answered at least once
        self._live_fails = 0         # consecutive failures since the last success
        self._archive_only = False   # latched into Wayback-only mode
        self._listing_captures: dict[int, str] | None = None

    # ---------------------------------------------------------------- fetching

    def _curl_bytes(self, url: str, attempts: int = 4) -> bytes | None:
        """Raw curl. -f/--fail so a Cloudflare block body is never mistaken for
        a valid-but-empty page (see #1115)."""
        for attempt in range(attempts):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-f", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            if attempt + 1 < attempts:
                time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _is_pdf(body: bytes | None) -> bool:
        """A complete PDF: magic header AND a trailing %%EOF.

        The EOF check matters for the archive path — Common Crawl truncates
        payloads at 5 MiB, so the newest capture of a large decision can be a
        headless half-PDF that every extractor rejects with "Unexpected EOF".
        Failing it here makes _wayback_bytes fall through to an older, complete
        capture instead of dropping the document.
        """
        return bool(body) and body[:4] == b"%PDF" and b"%%EOF" in body[-4096:]

    def _live_bytes(self, url: str, want_pdf: bool) -> bytes | None:
        """One live attempt against the origin, tracking the block latch.

        Once we have seen a live success we retry normally; until then a single
        probe per URL keeps the latch cheap (~seconds) instead of paying the
        Cloudflare block timeout four times on every one of ~80 URLs.
        """
        if self._archive_only:
            return None
        body = self._curl_bytes(url, attempts=4 if self._live_ok else 1)
        if body and (not want_pdf or self._is_pdf(body)):
            self._live_ok = True
            self._live_fails = 0
            return body
        self._live_fails += 1
        if not self._live_ok and self._live_fails >= LIVE_FAIL_LATCH:
            self._archive_only = True
            logger.warning(
                f"{self._live_fails} consecutive live failures with no success — "
                f"latching into Internet Archive-only mode ({HOST} Cloudflare-blocks "
                "datacenter/non-US IPs, #1245)"
            )
        return None

    def _cdx(self, url: str, match: str = "exact", limit: int = -8,
             collapse: str = "") -> list[tuple]:
        """CDX rows as (timestamp, original, statuscode), oldest-first."""
        query = CDX_URL.format(
            url=urllib.parse.quote(url, safe=":/"), match=match, limit=limit
        )
        if collapse:
            query += f"&collapse={collapse}"
        body = self._curl_bytes(query, attempts=3)
        if not body:
            return []
        rows = []
        for line in body.decode("utf-8", "replace").splitlines():
            parts = line.split(" ")
            if len(parts) >= 3:
                rows.append((parts[0], parts[1], parts[2]))
        return rows

    def _wayback_bytes(self, url: str, want_pdf: bool) -> bytes | None:
        """Replay the newest usable capture of `url` from the Internet Archive.

        `3000id_` (latest capture, raw bytes) is the cheap path, but the latest
        capture can itself be an archived error body — so on failure fall back
        to CDX and replay the newest row whose statuscode is not an error.
        Revisit rows (statuscode "-") are kept: the archive replays them fine
        and filtering them out halves apparent coverage.
        """
        body = self._curl_bytes(WAYBACK_LATEST + url, attempts=2)
        if body and (not want_pdf or self._is_pdf(body)):
            return body
        for ts, original, status in reversed(self._cdx(url)):
            if status in BAD_STATUS:
                continue
            body = self._curl_bytes(
                WAYBACK_REPLAY.format(ts=ts, url=original), attempts=2
            )
            if body and (not want_pdf or self._is_pdf(body)):
                logger.info(f"Archive replay {ts} for {url}")
                return body
        return None

    def _fetch(self, url: str, want_pdf: bool = False) -> bytes | None:
        """Live-first, Internet Archive fallback."""
        body = self._live_bytes(url, want_pdf)
        if body is not None:
            return body
        return self._wayback_bytes(url, want_pdf)

    @staticmethod
    def _unescape(s: str | None) -> str:
        if not s:
            return ""
        s = (s.replace("&amp;", "&").replace("&#039;", "'")
              .replace("&#39;", "'").replace("&quot;", '"')
              .replace("&nbsp;", " "))
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _norm_date(caption: str) -> str | None:
        m = DATE_RE.search(caption)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1980 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug(href: str) -> str:
        # /document/decisions/{slug}/download -> {slug}
        parts = [p for p in href.split("/") if p]
        slug = parts[-2] if parts and parts[-1] == "download" else parts[-1]
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", slug).strip("-")
        return slug[:180]

    @staticmethod
    def _case_name(caption: str) -> str | None:
        # caption: "<parties> - <doc type>, <citation>, <date>"
        name = caption.split(" - ", 1)[0].strip()
        return name or None

    @staticmethod
    def _citation(caption: str) -> str | None:
        m = CITATION_RE.search(caption)
        return f"{m.group(1)}-{m.group(2)} Ga. Tax Tribunal" if m else None

    # --------------------------------------------------------------- discovery

    def _archived_listing_captures(self) -> dict[int, str]:
        """Newest usable archived capture of each ?page=N decisions listing.

        The archived listing URLs carry a Drupal `instance_overrides_key` query
        param that changes between site revisions, so the page number has to be
        read back out of the captured URL rather than constructed.
        """
        if self._listing_captures is not None:
            return self._listing_captures
        best: dict[int, tuple[str, str]] = {}
        for ts, original, status in self._cdx(
            f"{HOST}{INDEX_PATH}", match="prefix", limit=100000
        ):
            if status in BAD_STATUS or DOC_PREFIX in original:
                continue
            m = re.search(r"[?&]page=(\d+)", original)
            page = int(m.group(1)) if m else 0
            if page not in best or ts > best[page][0]:
                best[page] = (ts, original)
        self._listing_captures = {
            page: WAYBACK_REPLAY.format(ts=ts, url=original)
            for page, (ts, original) in best.items()
        }
        if self._listing_captures:
            logger.info(f"Archived listing captures for pages "
                        f"{sorted(self._listing_captures)}")
        return self._listing_captures

    def _listing_html(self, page: int) -> str | None:
        """Listing page HTML, live-first then from the Internet Archive."""
        body = self._live_bytes(f"{BASE_URL}{INDEX_PATH}?page={page}", want_pdf=False)
        if body is None:
            replay = self._archived_listing_captures().get(page)
            body = self._curl_bytes(replay, attempts=2) if replay else None
        return body.decode("utf-8", "replace") if body else None

    def _archived_doc_hrefs(self) -> list[str]:
        """Every /document/decisions/... path the archive has ever seen.

        Catches decisions that have rotated off the live listing. Ordered
        newest-capture-first so the sample crawl favours recent decisions.
        """
        seen: dict[str, str] = {}
        for ts, original, status in self._cdx(
            f"{HOST}{DOC_PREFIX}", match="prefix", limit=100000, collapse="urlkey"
        ):
            if status in BAD_STATUS or HOST not in original:
                continue
            href = original.split(HOST, 1)[1]
            if href.startswith(DOC_PREFIX):
                seen.setdefault(href, ts)
        return sorted(seen, key=lambda h: seen[h], reverse=True)

    def _doc_from_href(self, href: str, caption: str = "") -> dict:
        """Build a discovery record from an anchor (with caption) or a bare
        archived href (metadata parsed back out of the slug)."""
        slug = self._slug(href)
        if not caption:
            # Rebuild a listing-style "<parties> - <doc type>, <citation>, <date>"
            # caption from the slug, which spells out the same fields.
            words = re.sub(r"\.?pdf$", "", slug).replace("-", " ").strip()
            m = re.search(r"\b(decision|order)\b", words, re.I)
            if m and m.start():
                parties = words[:m.start()].strip().title()
                caption = f"{parties} - {words[m.start():].strip().capitalize()}"
            else:
                caption = words.capitalize()
        date = self._norm_date(caption)
        citation = self._citation(caption)
        if not date:
            m = SLUG_DATE_RE.search(slug)
            if m:
                date = (f"{int(m.group(3)):04d}-{MONTHS[m.group(1).lower()]:02d}-"
                        f"{int(m.group(2)):02d}")
        if not citation:
            m = SLUG_CITATION_RE.search(slug)
            if m:
                citation = f"{m.group(1)}-{m.group(2)} Ga. Tax Tribunal"
        return {
            "title": caption,
            "case_name": self._case_name(caption),
            "citation": citation,
            "date": date,
            "pdf_url": urllib.parse.urljoin(BASE_URL, href),
            "slug": slug,
        }

    def discover_documents(self, sample: bool = False) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for page in range(MAX_PAGES):
            html = self._listing_html(page)
            if not html:
                logger.warning(f"Failed to fetch listing page {page} "
                               "(live and archive)")
                break
            rows = ROW_RE.findall(html)
            if not rows:
                logger.info(f"Page {page}: no decision rows — stopping")
                break
            new_on_page = 0
            for href, caption in rows:
                doc = self._doc_from_href(href, self._unescape(caption))
                if doc["pdf_url"] in seen:
                    continue
                seen.add(doc["pdf_url"])
                new_on_page += 1
                out.append(doc)
            logger.info(f"Page {page}: {new_on_page} new decisions "
                        f"(total {len(out)})")
            if new_on_page == 0:
                break
            if sample and len(out) >= 14:
                break

        # Union in decisions that have rotated off the live listing but are
        # preserved in the archive. Skipped in sample mode — the listing pages
        # already cover more than the 12 samples need.
        if not sample:
            extra = 0
            for href in self._archived_doc_hrefs():
                doc = self._doc_from_href(href)
                if doc["pdf_url"] in seen:
                    continue
                seen.add(doc["pdf_url"])
                out.append(doc)
                extra += 1
            if extra:
                logger.info(f"Union: +{extra} archive-only decisions "
                            f"(total {len(out)})")

        if not out:
            # Fail LOUD rather than reporting a clean "0 fetched" that would
            # ingest only the bundled samples (#1115).
            raise RuntimeError(
                f"No decisions discovered from {BASE_URL}{INDEX_PATH} live or "
                "from the Internet Archive. Aborting to avoid a false-positive "
                "complete with 0 real records."
            )
        # Newest first by decision date (undated last).
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} Georgia Tax Tribunal decisions "
                    f"({'archive-only' if self._archive_only else 'live-first'} mode)")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._fetch(doc["pdf_url"], want_pdf=True)
        if not self._is_pdf(pdf_bytes):
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/GA-TaxTribunal", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        return doc

    def test_api(self) -> bool:
        logger.info("Testing Georgia Tax Tribunal listing + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions (sample crawl, "
                        f"{'archive-only' if self._archive_only else 'live-first'})")
            # The newest decisions are the ones least likely to be archived yet,
            # so accept the first document that yields text.
            for doc in docs[:5]:
                raw = self._build_raw(doc)
                if raw and len(raw["text"]) > 150:
                    logger.info(f"  PDF text extraction OK ({len(raw['text'])} "
                                f"chars) — {raw.get('case_name')}")
                    break
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        case_name = (raw.get("case_name") or "").strip()
        title = (raw.get("title") or case_name
                 or "Georgia Tax Tribunal — Decision")
        title = title[:300]
        return {
            "_id": f"US/GA-TaxTribunal/{raw['slug']}",
            "_source": "US/GA-TaxTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "citation": raw.get("citation"),
            "court": "Georgia Tax Tribunal",
            "case_name": case_name or None,
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-GA",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/GA-TaxTribunal bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GATaxTribunalScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
