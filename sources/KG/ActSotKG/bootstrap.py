#!/usr/bin/env python3
"""
KG/ActSotKG -- Kyrgyz Republic Judicial Acts Portal (act.sot.kg)

Full text of judicial acts (ТОКТОМ / ЧЕЧИМ / АНЫКТАМА / Обвинительный ...)
issued by the courts of the Kyrgyz Republic — district, city, oblast, and
military courts as well as the Supreme Court — published under the judicial
transparency mandate on the State Judicial Acts Portal at act.sot.kg.

Strategy (server-rendered listing + born-digital act PDFs):

  1. fetch_all() walks the portal's act search listing

       GET /kg/search?...&submit-act=Актылар&page=N
              &sort=act.created&direction=asc

     which is plain server-rendered HTML (no auth, no JS). Each page holds
     10 rows; the row element carries the act id directly
     (``<tr id="/act/download/{id}.pdf">``) plus the case number, case
     category, act type, judge, court, act-approval date and publication
     date. As of 2026-08-02 the listing runs to page 37,815 (~378,150 acts,
     id space 70..414,841).

     The walk is sorted **ascending on act.created** so newly published acts
     append at the tail instead of shifting every page — this makes the
     pagination stable across restarts. Completed pages are recorded in
     ``data/checkpoint.json`` so a re-launched fleet slot resumes at the
     first unfinished page with no network calls for the pages already done.

     The corpus is far larger than one 100-hour fleet slot (#1429: 145,500
     records ingested, then SIGTERM/exit 124 mid-crawl), so the checkpoint is
     what makes the source completable at all — it is flushed every 20 pages,
     on SIGTERM/SIGINT, and kept ``CHECKPOINT_LAG_PAGES`` behind the page
     being yielded so records still sitting in the writer's batch are re-fetched
     rather than stranded on the next run.

     A listing page that stays unreachable after ``LISTING_ATTEMPTS`` is not
     silently dropped (that cost 348 pages ≈ 3,480 acts in #1429): it is
     recorded as a coverage gap — counted in the run stats and written to
     status.yaml — *and* queued in the checkpoint, retried at the end of the
     run and again, first thing, on the next run.

  2. normalize() downloads ``/act/download/{id}.pdf`` and extracts the full
     text. The acts are born-digital (0 scanned in probing), so pdfplumber
     returns clean Kyrgyz/Russian Cyrillic. pdfplumber is used *first* here
     on purpose: PyMuPDF emits the Kyrgyz-specific letters ө/ү as detached
     glyphs on their own lines for these documents, which mangles the text.
     fitz is kept only as a fallback if pdfplumber yields nothing.

Usage:
  python bootstrap.py bootstrap            # Full pull (all acts)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KG.ActSotKG")

SOURCE_ID = "KG/ActSotKG"
BASE_URL = "https://act.sot.kg"
SEARCH_PATH = "/kg/search"
PDF_URL = BASE_URL + "/act/download/{act_id}.pdf"

# The listing form must be submitted with submit-act (Актылар = "acts");
# submit-case would return the case index instead.
SEARCH_PARAMS = {
    "caseno": "",
    "judge": "all",
    "side1": "",
    "side2": "",
    "from": "",
    "to": "",
    "actType": "all",
    "submit-act": "Актылар",
    "caseType": "all",
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

ROW_ID_RE = re.compile(r"^/act/download/(\d+)\.pdf$")
DATE_RE = re.compile(r"(\d{2})-(\d{2})-(\d{4})")

REQUEST_DELAY = 1.0          # seconds between listing requests
MAX_DELAY = 10.0             # ceiling for the adaptive listing delay
MAX_RETRIES = 5
CHECKPOINT_FLUSH_PAGES = 20  # persist progress every N pages

# act.sot.kg drops connections under sustained crawling (#1429: 348 listing
# pages lost to ConnectTimeout). A connect that has not completed in 15s never
# will, so cap it separately from the read timeout — the old flat 60s spent
# ~5.5 minutes per dead page (5 attempts + backoff) and burned roughly a third
# of the 100-hour fleet slot doing nothing.
LISTING_TIMEOUT = (15, 60)   # (connect, read)
PDF_TIMEOUT = (15, 90)
LISTING_ATTEMPTS = 3         # then the page is deferred to the retry queue

# The checkpoint is written behind the page actually being yielded: records are
# normalized in worker threads and written in batches of 100, so the pages most
# recently yielded may not be on disk when the 100h cap SIGTERMs the run.
# 15 pages = 150 acts covers a full unflushed batch plus everything in flight.
CHECKPOINT_LAG_PAGES = 15


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------

def _extract_with_pdfplumber(pdf_bytes: bytes) -> str:
    """Extract text with pdfplumber, flushing per-page caches (issue #934 class)."""
    try:
        import pdfplumber
    except ImportError:
        return ""
    out = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                try:
                    out.append(page.extract_text() or "")
                finally:
                    # pdfplumber retains every visited page's parsed layout and
                    # its textmap LRU cache for the document's lifetime -> GBs
                    # of RSS on large PDFs. Release both immediately.
                    try:
                        page.flush_cache()
                        page.get_textmap.cache_clear()
                    except Exception:
                        pass
    except Exception as exc:  # pragma: no cover - corrupt PDF
        logger.debug(f"pdfplumber failed: {exc}")
        return ""
    return "\n".join(out)


def _extract_with_fitz(pdf_bytes: bytes) -> str:
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return ""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return "\n".join(page.get_text("text", sort=True) for page in doc)
    except Exception as exc:  # pragma: no cover
        logger.debug(f"fitz failed: {exc}")
        return ""


def extract_text(pdf_bytes: bytes) -> str:
    """
    pdfplumber first: PyMuPDF splits the Kyrgyz letters ө/ү onto separate
    lines for these documents, which corrupts the text.
    """
    text = _extract_with_pdfplumber(pdf_bytes)
    if len(text.strip()) < 200:
        alt = _extract_with_fitz(pdf_bytes)
        if len(alt.strip()) > len(text.strip()):
            text = alt
    return clean_text(text)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_date(value: Optional[str]) -> Optional[str]:
    """'20-06-2024' or '21-06-2024 08:28' -> '2024-06-20'."""
    if not value:
        return None
    m = DATE_RE.search(value)
    if not m:
        return None
    day, month, year = m.groups()
    try:
        return datetime(int(year), int(month), int(day)).date().isoformat()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class ActSotKGScraper(BaseScraper):
    """Kyrgyz Republic judicial acts portal (act.sot.kg)."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ky-KG,ky;q=0.9,ru;q=0.8,en;q=0.7",
        })
        self._module_dir = Path(__file__).resolve().parent
        self._ckpt_path = self._module_dir / "data" / "checkpoint.json"
        self._ckpt = self._load_checkpoint()
        self._use_checkpoint = True
        self._delay = REQUEST_DELAY
        self._page_ok = True

    # ---- checkpoint -------------------------------------------------------

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._ckpt_path, encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                pending = data.get("pending_pages") or []
                return {
                    "last_page_done": int(data.get("last_page_done", 0)),
                    "pending_pages": sorted({int(p) for p in pending}),
                }
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning(f"Ignoring unreadable checkpoint: {exc}")
        return {"last_page_done": 0, "pending_pages": []}

    def _defer_page(self, page: int, reason: str) -> None:
        """Queue a listing page that could not be fetched for a later retry.

        A lost listing page costs all 10 acts behind it, so it is recorded as a
        first-class coverage gap (surfaced in the run stats and status.yaml)
        *and* persisted in the checkpoint so the next run retries it before
        moving forward.
        """
        pending = self._ckpt.setdefault("pending_pages", [])
        if page not in pending:
            pending.append(page)
            pending.sort()
        # one gap entry per page, however many times it is retried
        self.clear_coverage_gap(f"listing page {page}")
        self.record_coverage_gap(f"listing page {page}", reason, page=page)
        self._save_checkpoint()

    def _resolve_page(self, page: int) -> None:
        """Drop a deferred page after a retry succeeded."""
        pending = self._ckpt.get("pending_pages") or []
        if page in pending:
            pending.remove(page)
            self._save_checkpoint()
        self.clear_coverage_gap(f"listing page {page}")

    def _install_signal_handlers(self) -> None:
        """Persist the checkpoint when the fleet's 100h cap SIGTERMs the run."""
        def _handler(signum, frame):
            logger.warning(
                f"Signal {signum} received — persisting checkpoint "
                f"(last_page_done={self._ckpt.get('last_page_done')}, "
                f"{len(self._ckpt.get('pending_pages') or [])} pages pending)"
            )
            self._save_checkpoint()
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _handler)
            except (ValueError, OSError):  # not the main thread / unsupported
                pass

    def _save_checkpoint(self) -> None:
        try:
            self._ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._ckpt_path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self._ckpt, fh)
            tmp.replace(self._ckpt_path)
        except Exception as exc:  # pragma: no cover
            logger.warning(f"Could not persist checkpoint: {exc}")

    # ---- HTTP -------------------------------------------------------------

    def _get(self, url: str, params: Optional[dict] = None,
             timeout=LISTING_TIMEOUT,
             attempts: int = MAX_RETRIES) -> Optional[requests.Response]:
        delay = 2.0
        for attempt in range(1, attempts + 1):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (404, 410):
                    return None
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        f"HTTP {resp.status_code} for {url} "
                        f"(attempt {attempt}/{attempts}) — sleeping {wait:.0f}s"
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
            except requests.RequestException as exc:
                logger.warning(
                    f"{type(exc).__name__} for {url} "
                    f"(attempt {attempt}/{attempts}): {exc}"
                )
                time.sleep(min(delay, 120))
                delay = min(delay * 2, 120)
        return None

    def _listing_params(self, page: int, direction: str = "asc") -> dict:
        params = dict(SEARCH_PARAMS)
        params.update({
            "page": page,
            "sort": "act.created",
            "direction": direction,
        })
        return params

    # ---- listing parsing --------------------------------------------------

    @staticmethod
    def _parse_rows(html: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for tr in soup.find_all("tr"):
            m = ROW_ID_RE.match(tr.get("id") or "")
            if not m:
                continue
            act_id = m.group(1)
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 5:
                continue

            # td[1] — case number link + category + party labels
            case_no, case_url, category = None, None, None
            case_link = tds[1].find("a")
            if case_link:
                case_no = case_link.get_text(strip=True) or None
                href = case_link.get("href")
                if href:
                    case_url = BASE_URL + href
            cat_span = tds[1].find("span", class_="block")
            if cat_span:
                category = cat_span.get_text(strip=True) or None

            # td[2] — the download link's label is the act's published name
            # ("Актынын аталышы"): either an act type ("Обвинительный") or a
            # descriptive caption of what the act decides.
            act_name = None
            act_link = tds[2].find("a")
            if act_link:
                act_name = " ".join(act_link.get_text(strip=True).split()) or None

            # td[3] — judge (span.fio) then court (span.block)
            judge, court = None, None
            fio = tds[3].find("span", class_="fio")
            if fio:
                judge = " ".join(fio.get_text(strip=True).split()) or None
            for span in tds[3].find_all("span", class_="block"):
                if "fio" in (span.get("class") or []):
                    continue
                court = " ".join(span.get_text(strip=True).split()) or None
                break

            rows.append({
                "act_id": act_id,
                "case_number": case_no,
                "case_url": case_url,
                "category": category,
                "act_name": act_name,
                "judge": judge,
                "court": court,
                "act_date": tds[4].get_text(strip=True) or None,
                "published": tds[5].get_text(strip=True) if len(tds) > 5 else None,
            })
        return rows

    def _last_page(self) -> int:
        """Read the '>>' (last) pagination link to learn the corpus depth."""
        resp = self._get(BASE_URL + SEARCH_PATH, params=self._listing_params(1))
        if resp is None:
            raise RuntimeError(
                "act.sot.kg listing unreachable (page 1) — the portal is "
                "blocking this vantage or is down; refusing to report 0 acts."
            )
        soup = BeautifulSoup(resp.text, "html.parser")
        last = 0
        for a in soup.select("ul.pagination a[href]"):
            m = re.search(r"[?&]page=(\d+)", a["href"])
            if m:
                last = max(last, int(m.group(1)))
        if last == 0:
            raise RuntimeError(
                "act.sot.kg listing returned no pagination — page layout "
                "changed or the response was an error page."
            )
        return last

    # ---- iteration --------------------------------------------------------

    def _fetch_page(self, page: int, direction: str, seen: set,
                    defer: bool) -> Generator[dict, None, None]:
        """Yield the act rows of one listing page, deferring it if unreachable.

        Sets ``self._page_ok`` so the caller can pace itself: the host is
        unstable under sustained crawling, so failures widen the inter-page
        delay and successes narrow it back towards REQUEST_DELAY.
        """
        resp = self._get(BASE_URL + SEARCH_PATH,
                         params=self._listing_params(page, direction),
                         attempts=LISTING_ATTEMPTS)
        if resp is None:
            self._page_ok = False
            if defer:
                self._defer_page(page, "listing unreachable after retries")
            else:
                logger.warning(f"Listing page {page} unavailable")
            return

        rows = self._parse_rows(resp.text)
        if not rows:
            # An empty page past the end of the corpus is normal; an empty page
            # inside it means the response was an error/challenge body.
            self._page_ok = False
            if defer:
                self._defer_page(page, "listing returned no act rows")
            else:
                logger.warning(f"Listing page {page} had no act rows")
            return

        self._page_ok = True
        if defer:
            self._resolve_page(page)
        for row in rows:
            # duplicate ids appear across adjacent pages when the
            # underlying ordering ties on act.created
            if row["act_id"] in seen:
                continue
            seen.add(row["act_id"])
            yield row

    def _pace(self) -> None:
        """Adaptive inter-page delay — back off while the host is struggling."""
        if self._page_ok:
            self._delay = max(REQUEST_DELAY, self._delay * 0.9)
        else:
            self._delay = min(MAX_DELAY, max(REQUEST_DELAY, self._delay) * 1.5)
        time.sleep(self._delay)

    def _walk_listing(self, direction: str = "asc",
                      use_checkpoint: bool = True) -> Generator[dict, None, None]:
        last_page = self._last_page()
        start = (self._ckpt["last_page_done"] + 1) if use_checkpoint else 1
        if start > 1:
            logger.info(f"Resuming at page {start} (checkpoint)")
        logger.info(f"act.sot.kg: {last_page} listing pages (~{last_page * 10} acts)")

        seen = set()
        self._page_ok = True

        # 1. Pages an earlier run could never fetch come first — otherwise a
        #    corpus this size never gets back to them (#1429).
        carried = list(self._ckpt.get("pending_pages") or []) if use_checkpoint else []
        if carried:
            logger.info(f"Retrying {len(carried)} page(s) skipped by an earlier run")
            for page in carried:
                yield from self._fetch_page(page, direction, seen, defer=True)
                self._pace()

        # 2. Forward walk.
        for page in range(start, last_page + 1):
            yield from self._fetch_page(page, direction, seen, defer=use_checkpoint)

            if use_checkpoint:
                # Lag the resume point behind the yielded page so a SIGTERM at
                # the 100h cap cannot strand records still in the write batch.
                self._ckpt["last_page_done"] = max(
                    self._ckpt["last_page_done"], page - CHECKPOINT_LAG_PAGES
                )
                if page % CHECKPOINT_FLUSH_PAGES == 0:
                    self._save_checkpoint()
                    logger.info(f"  page {page}/{last_page} ({len(seen)} acts seen)")
            if len(seen) > 200_000:
                seen = set()  # bound memory; the loader dedups on _id
            self._pace()

        # 3. Retry what this run skipped before declaring the walk finished.
        retry = list(self._ckpt.get("pending_pages") or []) if use_checkpoint else []
        if retry:
            logger.info(f"Retrying {len(retry)} page(s) skipped during this run")
            for page in retry:
                yield from self._fetch_page(page, direction, seen, defer=True)
                self._pace()

        if use_checkpoint:
            still_pending = self._ckpt.get("pending_pages") or []
            self._ckpt["last_page_done"] = last_page
            self._save_checkpoint()
            if still_pending:
                logger.error(
                    f"{len(still_pending)} listing page(s) still unfetched after "
                    f"retry — corpus incomplete, carried to the next run: "
                    f"{still_pending[:20]}"
                )
            else:
                logger.info(f"Listing walk complete through page {last_page}")

    def fetch_all(self) -> Generator[dict, None, None]:
        if self._use_checkpoint:
            self._install_signal_handlers()
        yield from self._walk_listing(direction="asc",
                                      use_checkpoint=self._use_checkpoint)

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Newest-first walk, stopping once acts predate `since`."""
        if isinstance(since, str):
            since = datetime.fromisoformat(since.replace("Z", "+00:00"))
        cutoff = since.date().isoformat()
        page = 1
        stale = 0
        while page <= 5000:
            resp = self._get(BASE_URL + SEARCH_PATH,
                             params=self._listing_params(page, "desc"))
            if resp is None:
                break
            rows = self._parse_rows(resp.text)
            if not rows:
                break
            for row in rows:
                pub = parse_date(row.get("published")) or parse_date(row.get("act_date"))
                if pub and pub < cutoff:
                    stale += 1
                    continue
                yield row
            if stale >= 20:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

    # ---- normalization ----------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        act_id = raw.get("act_id")
        if not act_id:
            return None
        pdf_url = PDF_URL.format(act_id=act_id)

        resp = self._get(pdf_url, timeout=PDF_TIMEOUT)
        if resp is None:
            logger.debug(f"act {act_id}: PDF unavailable")
            return None
        text = extract_text(resp.content)
        if len(text) < 200:
            logger.debug(f"act {act_id}: insufficient text ({len(text)} chars)")
            return None

        act_name = raw.get("act_name")
        case_number = raw.get("case_number")
        short_name = act_name or ""
        if len(short_name) > 200:
            short_name = short_name[:197].rstrip() + "…"
        title_parts = [p for p in (case_number, short_name) if p]
        title = " — ".join(title_parts) if title_parts else f"Сот акты {act_id}"
        if raw.get("court"):
            title = f"{title} ({raw['court']})"

        return {
            "_id": f"actsotkg-{act_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": parse_date(raw.get("act_date")),
            "url": pdf_url,
            "act_id": act_id,
            "act_name": act_name,
            "case_number": case_number,
            "case_url": raw.get("case_url"),
            "category": raw.get("category"),
            "court": raw.get("court"),
            "judge": raw.get("judge"),
            "published_date": parse_date(raw.get("published")),
            "language": "ky",
            "country": "KG",
        }

    # ---- diagnostics ------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing act.sot.kg ...")
        try:
            last = self._last_page()
            logger.info(f"  listing OK — {last} pages (~{last * 10} acts)")
            resp = self._get(BASE_URL + SEARCH_PATH, params=self._listing_params(1))
            rows = self._parse_rows(resp.text)
            logger.info(f"  page 1 rows: {len(rows)}")
            if not rows:
                logger.error("  no act rows parsed")
                return False
            rec = self.normalize(rows[0])
            if not rec:
                logger.error("  first act yielded no text")
                return False
            logger.info(
                f"  act {rec['act_id']} OK — {len(rec['text'])} chars, "
                f"date={rec['date']}, court={rec.get('court')}"
            )
            logger.info("API test PASSED")
            return True
        except Exception as exc:
            logger.error(f"API test FAILED: {exc}")
            return False


def main():
    parser = argparse.ArgumentParser(description="KG/ActSotKG bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api", "updates"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--count", type=int, default=12, help="Sample size")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = ActSotKGScraper()

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
        # sampling must not advance the full-run resume point
        scraper._use_checkpoint = False
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
