#!/usr/bin/env python3
"""
GE/CommonCourts-Decisions -- Georgia, Common Courts Unified Decisions Database

Full text of decisions of the common courts of Georgia published in the
unified decisions database at https://ecd.court.ge/ — first-instance city
and district courts, courts of appeal, and the cassation instance, across
criminal (სისხლის სამართლის), civil (სამოქალაქო) and administrative
(ადმინისტრაციული) case categories. ~44,489 decisions.

Strategy (undocumented JSON API, no auth, no captcha):

  1. Listing — ``POST /Decision/DecisionDocuments`` (form-encoded,
     ``X-Requested-With: XMLHttpRequest``) returns
     ``{"success":true,"data":{"Total":N,"Items":[...]}}``. Each item
     carries the instance, court, case number, case category, decision
     type, barcode and decision date, so no metadata has to be scraped
     out of the decision body.

     GOTCHA: ``Take`` caps at 50 and ``Skip`` caps at ~10,000 (higher
     values return HTTP 500), so the corpus cannot be walked in one
     stream. fetch_all() therefore partitions it into
     (InstanceId × CaseCategoryId × decision-date window) units and
     recursively bisects any window whose Total exceeds the safe skip
     ceiling.

     NOTE: contrary to the original research note, ``DecisionDateTo`` IS
     honoured — verified live 2026-08-02 for InstanceId=1,
     CaseCategoryId=1: unbounded 10,375 vs ``To=01.01.2019`` 4,759 +
     ``From=01.01.2019&To=01.01.2020`` 4,796 + ``From=01.01.2020`` 821
     (= 10,376, one record double-counted on the shared boundary). That
     makes clean date-window partitioning possible; boundary duplicates
     are de-duplicated on ``_id``.

     GOTCHA (#1426): those bounds must be sent as ISO ``YYYY-MM-DD``.
     The site's own UI renders dates as dd.mm.yyyy, but the endpoint parses
     a dotted date as **mm.dd.yyyy** — so ``15.06.2019`` is month 15, which
     it rejects by *silently dropping the date filter* and returning the
     whole unfiltered unit with HTTP 200. That is invisible in any window
     whose day-of-month is ≤ 12 (``01.01.2019`` means the same thing either
     way), which is why the original verification missed it.

     The damage was not a lost filter but a non-terminating partition: every
     window with a day > 12 reported the unit's full Total, so _windows()
     never bisected below the skip ceiling and kept splitting down to single
     days, and each of the thousands of resulting "windows" re-walked the
     same global first 9,500 rows — 165,700 records written, 9,499 distinct.
     _count() now also validates that the first and last row of a filtered
     result really fall inside the requested window, so a future regression
     in date handling reports an empty window instead of looping.

  2. Full text — ``POST /Decision/DecisionDocumentText`` with
     ``InstanceId`` + ``DecisionDocumentId`` returns ``data.RawData``,
     the complete decision text (Georgian). Party names are already
     pseudonymised by the publisher.

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
import time
from datetime import date, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GE.CommonCourts-Decisions")

SOURCE_ID = "GE/CommonCourts-Decisions"
BASE_URL = "https://ecd.court.ge"
LIST_URL = f"{BASE_URL}/Decision/DecisionDocuments"
TEXT_URL = f"{BASE_URL}/Decision/DecisionDocumentText"

PAGE_SIZE = 50          # Take caps at 50; higher returns HTTP 500
SKIP_CEILING = 9_500    # Skip caps at ~10,000; stay safely below
REQUEST_DELAY = 0.6
MAX_RETRIES = 5
MAX_STALE_PAGES = 3     # consecutive all-duplicate pages before a window is abandoned

INSTANCES = {1: "პირველი ინსტანცია", 2: "სააპელაციო", 3: "საკასაციო"}
CASE_CATEGORIES = {1: "სისხლის სამართლის", 2: "სამოქალაქო", 3: "ადმინისტრაციული"}

CORPUS_START = date(2000, 1, 1)

MS_DATE_RE = re.compile(r"/Date\((-?\d+)")
TAG_RE = re.compile(r"<[^>]+>")


def parse_ms_date(value: Optional[str]) -> Optional[str]:
    """'/Date(1588260918000)/' -> '2020-04-30'."""
    if not value:
        return None
    m = MS_DATE_RE.search(str(value))
    if not m:
        return None
    try:
        return datetime.fromtimestamp(int(m.group(1)) / 1000,
                                      tz=timezone.utc).date().isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def clean_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = raw
    if "<" in text and TAG_RE.search(text):
        text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", text)
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</(p|div|tr|h\d)>", "\n", text)
        text = TAG_RE.sub(" ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def fmt(d: date) -> str:
    """Render a window bound for DecisionDateFrom/DecisionDateTo.

    ISO, *not* the dd.mm.yyyy the site's own UI shows: the endpoint parses
    dotted dates as **mm.dd.yyyy**, so `15.06.2019` is month 15 — invalid,
    and silently unfiltered. See the #1426 note in the module docstring.
    """
    return d.strftime("%Y-%m-%d")


class GECommonCourtsScraper(BaseScraper):
    """Georgia common courts unified decisions database (ecd.court.ge)."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/",
        })
        self._module_dir = Path(__file__).resolve().parent
        self._ckpt_path = self._module_dir / "data" / "checkpoint.json"
        self._ckpt = self._load_checkpoint()
        self._use_checkpoint = True
        self._seen_ids: set = set()

    # ---- checkpoint -------------------------------------------------------

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._ckpt_path, encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return {
                    "done_units": list(data.get("done_units", [])),
                    "unit": data.get("unit"),
                    "skip": int(data.get("skip", 0)),
                }
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning(f"Ignoring unreadable checkpoint: {exc}")
        return {"done_units": [], "unit": None, "skip": 0}

    def _save_checkpoint(self) -> None:
        try:
            self._ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._ckpt_path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self._ckpt, fh)
            tmp.replace(self._ckpt_path)
        except Exception as exc:
            logger.warning(f"Could not persist checkpoint: {exc}")

    # ---- HTTP -------------------------------------------------------------

    def _post(self, url: str, data: dict, timeout: int = 90) -> Optional[dict]:
        delay = 2.0
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.post(url, data=data, timeout=timeout)
                if resp.status_code == 200:
                    try:
                        payload = resp.json()
                    except ValueError:
                        logger.warning(f"Non-JSON response from {url}")
                        return None
                    if not payload.get("success"):
                        logger.debug(f"success=false from {url}: {data}")
                        return None
                    return payload.get("data")
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        f"HTTP {resp.status_code} from {url} "
                        f"(attempt {attempt}/{MAX_RETRIES}) — sleeping {wait:.0f}s"
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                logger.warning(f"HTTP {resp.status_code} from {url}")
                return None
            except requests.RequestException as exc:
                logger.warning(
                    f"{type(exc).__name__} from {url} "
                    f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
                )
                time.sleep(min(delay, 120))
                delay = min(delay * 2, 120)
        return None

    # ---- listing ----------------------------------------------------------

    def _query(self, instance: int, category: int, start: date, end: date,
               skip: int = 0, take: int = PAGE_SIZE) -> Optional[dict]:
        return self._post(LIST_URL, {
            "Skip": skip,
            "Take": take,
            "InstanceId": instance,
            "CaseCategoryId": category,
            "DecisionDateFrom": fmt(start),
            "DecisionDateTo": fmt(end),
        })

    def _in_window(self, item: dict, start: date, end: date) -> Optional[bool]:
        """True/False if the row's decision date is inside [start, end];
        None when the row carries no parsable date (cannot judge).

        A day of slack each side: the endpoint filters on Georgian local
        dates (UTC+4) while parse_ms_date renders UTC, so a row sitting on
        either boundary can legitimately land one day outside.
        """
        d = parse_ms_date(item.get("DecisionDate"))
        if d is None:
            return None
        return ((start - timedelta(days=1)).isoformat() <= d
                <= (end + timedelta(days=1)).isoformat())

    def _count(self, instance: int, category: int, start: date, end: date) -> int:
        """Size of a date window, 0 if the endpoint ignored the date filter.

        An empty window makes ecd.court.ge drop DecisionDateFrom/To and echo
        the unfiltered unit (see module docstring / #1426). The tell is that
        the returned rows sit outside the window, so probe both ends of the
        result set: a genuinely filtered result has *all* of its rows inside
        the window by construction, an unfiltered one gives itself away at
        the newest or the oldest reachable row.
        """
        data = self._query(instance, category, start, end, skip=0, take=1)
        time.sleep(REQUEST_DELAY)
        if not data:
            return 0
        total = int(data.get("Total", 0))
        items = data.get("Items") or []
        if total <= 0 or not items:
            return 0

        probes = [items[0]]
        last_skip = min(total - 1, SKIP_CEILING - 1)
        if last_skip > 0:
            tail = self._query(instance, category, start, end, skip=last_skip, take=1)
            time.sleep(REQUEST_DELAY)
            tail_items = (tail or {}).get("Items") or []
            if tail_items:
                probes.append(tail_items[0])

        verdicts = [v for v in (self._in_window(p, start, end) for p in probes)
                    if v is not None]
        if verdicts and not all(verdicts):
            logger.debug(
                f"i{instance}c{category} {start}..{end}: date filter ignored "
                f"(echoed {total} unfiltered rows) — treating window as empty"
            )
            return 0
        return total

    def _windows(self, instance: int, category: int,
                 start: date, end: date) -> list:
        """Bisect the date range until every window fits under the skip ceiling."""
        total = self._count(instance, category, start, end)
        if total == 0:
            return []
        if total <= SKIP_CEILING or (end - start).days <= 1:
            if total > SKIP_CEILING:
                logger.warning(
                    f"i{instance}c{category} {start}..{end}: {total} records in a "
                    f"single day — the tail beyond Skip={SKIP_CEILING} is unreachable"
                )
            return [(start, end, total)]
        mid = start + (end - start) / 2
        return (self._windows(instance, category, start, mid)
                + self._windows(instance, category, mid + timedelta(days=1), end))

    def _walk_window(self, instance: int, category: int, start: date, end: date,
                     total: int, unit_key: str,
                     start_skip: int = 0) -> Generator[dict, None, None]:
        skip = start_skip
        stale_pages = 0
        while skip < min(total, SKIP_CEILING):
            data = self._query(instance, category, start, end, skip=skip)
            if data is None:
                logger.warning(f"{unit_key}: skip={skip} failed — stopping window")
                break
            items = data.get("Items") or []
            if not items:
                break
            fresh = 0
            for item in items:
                doc_id = item.get("Id")
                if doc_id is not None:
                    if doc_id in self._seen_ids:
                        continue
                    self._seen_ids.add(doc_id)
                fresh += 1
                yield item
            # A page that adds nothing new means the endpoint is replaying rows
            # we already hold (the #1426 failure mode). Give it a couple of
            # pages of grace for legitimate window-boundary overlap, then stop.
            stale_pages = stale_pages + 1 if fresh == 0 else 0
            if stale_pages >= MAX_STALE_PAGES:
                logger.warning(
                    f"{unit_key}: {stale_pages} consecutive pages with no new "
                    f"decisions at skip={skip} — abandoning window"
                )
                break
            skip += PAGE_SIZE
            if skip % (PAGE_SIZE * 20) == 0:
                logger.info(f"{unit_key}: skip={skip}/{min(total, SKIP_CEILING)}, "
                            f"{len(self._seen_ids)} distinct decisions so far")
            if self._use_checkpoint:
                self._ckpt["unit"] = unit_key
                self._ckpt["skip"] = skip
                if skip % (PAGE_SIZE * 20) == 0:
                    self._save_checkpoint()
            time.sleep(REQUEST_DELAY)

        if self._use_checkpoint:
            if unit_key not in self._ckpt["done_units"]:
                self._ckpt["done_units"].append(unit_key)
            self._ckpt["unit"] = None
            self._ckpt["skip"] = 0
            self._save_checkpoint()

    def fetch_all(self) -> Generator[dict, None, None]:
        probe = self._post(LIST_URL, {"Skip": 0, "Take": 1})
        if not probe or not probe.get("Total"):
            raise RuntimeError(
                "ecd.court.ge /Decision/DecisionDocuments returned no results — "
                "the API is unreachable, blocked from this vantage, or changed; "
                "refusing to report an empty corpus."
            )
        corpus_total = int(probe["Total"])
        logger.info(f"ecd.court.ge: {corpus_total} decisions in total")

        today = datetime.now(timezone.utc).date()
        done = set(self._ckpt["done_units"]) if self._use_checkpoint else set()

        for instance in sorted(INSTANCES):
            for category in sorted(CASE_CATEGORIES):
                windows = self._windows(instance, category, CORPUS_START, today)
                if not windows:
                    continue
                logger.info(
                    f"i{instance}c{category}: {sum(w[2] for w in windows)} decisions "
                    f"in {len(windows)} date window(s)"
                )
                for start, end, total in windows:
                    unit_key = f"i{instance}c{category}:{start}:{end}"
                    if unit_key in done:
                        continue
                    start_skip = 0
                    if self._use_checkpoint and self._ckpt.get("unit") == unit_key:
                        start_skip = max(0, int(self._ckpt.get("skip", 0)))
                        if start_skip:
                            logger.info(f"{unit_key}: resuming at skip={start_skip}")
                    yield from self._walk_window(instance, category, start, end,
                                                 total, unit_key, start_skip)

        reached = len(self._seen_ids)
        logger.info(f"fetch_all complete: {reached} distinct decisions of "
                    f"{corpus_total} reported by the API "
                    f"({reached / corpus_total:.0%})")
        if done:
            return  # a resumed run only walks what the checkpoint left over
        if reached == 0:
            raise RuntimeError(
                "ecd.court.ge yielded no decisions despite reporting "
                f"{corpus_total} — refusing to report an empty corpus."
            )
        if reached < corpus_total * 0.8:
            logger.warning(
                f"only {reached}/{corpus_total} decisions were reachable — "
                "date windows are losing rows (skip-ceiling tails, dropped "
                "windows, or the date filter being ignored again; see #1426)"
            )

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Newest-first sweep bounded by `since` (the API sorts by date desc)."""
        if isinstance(since, str):
            since = datetime.fromisoformat(since.replace("Z", "+00:00"))
        start = since.date()
        today = datetime.now(timezone.utc).date()
        for instance in sorted(INSTANCES):
            for category in sorted(CASE_CATEGORIES):
                total = self._count(instance, category, start, today)
                if not total:
                    continue
                yield from self._walk_window(
                    instance, category, start, today, total,
                    f"upd-i{instance}c{category}", 0,
                )

    # ---- normalization ----------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        doc_id = raw.get("DecisionDocumentId")
        instance = raw.get("InstanceId")
        if not doc_id or not instance:
            return None

        text = clean_text(raw.get("DecisionText"))
        if len(text) < 200:
            data = self._post(TEXT_URL, {
                "InstanceId": instance,
                "DecisionDocumentId": doc_id,
            })
            if data:
                text = clean_text(data.get("RawData"))
        if len(text) < 200:
            logger.debug(f"decision {doc_id}: insufficient text ({len(text)} chars)")
            return None

        decision_date = parse_ms_date(raw.get("DecisionDate"))
        court = raw.get("CourtName")
        doc_type = raw.get("TypeName")
        case_no = raw.get("CaseNo")

        title_bits = [b for b in (doc_type, case_no) if b]
        title = " — ".join(title_bits) if title_bits else f"გადაწყვეტილება {doc_id}"
        if court:
            title = f"{title} ({court})"

        return {
            "_id": f"ge-ecd-{raw.get('Id') or f'{instance}-{doc_id}'}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": f"{BASE_URL}/#/decision/{instance}/{doc_id}",
            "decision_document_id": str(doc_id),
            "case_id": str(raw.get("CaseId")) if raw.get("CaseId") else None,
            "case_number": case_no,
            "court": court,
            "court_code": raw.get("CourtCode"),
            "instance": raw.get("InstanceName") or INSTANCES.get(instance),
            "instance_id": instance,
            "case_category": raw.get("CaseCategoryName") or CASE_CATEGORIES.get(
                raw.get("CaseCategoryId")),
            "document_type": doc_type,
            "barcode": raw.get("Barcode"),
            "created_date": parse_ms_date(raw.get("DecisionCreateDate")),
            "language": "ka",
            "country": "GE",
        }

    # ---- diagnostics ------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing ecd.court.ge ...")
        try:
            data = self._post(LIST_URL, {"Skip": 0, "Take": 5})
            if not data:
                logger.error("  listing endpoint returned nothing")
                return False
            logger.info(f"  listing OK — Total={data.get('Total')}, "
                        f"{len(data.get('Items') or [])} items")
            items = data.get("Items") or []
            if not items:
                logger.error("  no items returned")
                return False
            rec = self.normalize(items[0])
            if not rec:
                logger.error("  first decision yielded no text")
                return False
            logger.info(
                f"  {rec['_id']} OK — {len(rec['text'])} chars, date={rec['date']}, "
                f"court={rec['court']}, type={rec['document_type']}"
            )
            logger.info("API test PASSED")
            return True
        except Exception as exc:
            logger.error(f"API test FAILED: {exc}")
            return False


def main():
    parser = argparse.ArgumentParser(description="GE/CommonCourts-Decisions bootstrap")
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

    scraper = GECommonCourtsScraper()

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
