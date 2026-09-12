#!/usr/bin/env python3
"""
IS/Stjornartidindi -- Iceland Official Gazette (Stjórnartíðindi)

The official State Gazette of Iceland, published under Lög um Stjórnartíðindi og
Lögbirtingablað nr. 15/2005 on the island.is national platform. Three series:
  - A-deild: Acts of the Althingi (LÖG) and presidential/legislative instruments
  - B-deild: regulations, tariffs and administrative rules (reglugerðir)
  - C-deild: treaties and international agreements

Strategy:
  - Enumerate via the public JSON API:
        https://api.stjornartidindi.is/api/v1/adverts?department={dep}&page=N&pageSize=100
    The paging block carries totalPages/totalItems. Each advert object already
    embeds the full born-digital text as document.html (inline) — no per-document
    fetch or OCR is required.
  - normalize() strips the HTML to clean text and maps the metadata.

~39,600 documents total (a-deild ~3,860, b-deild ~34,840, c-deild ~970).

fetch_all() yields RAW advert dicts (already carrying full text); normalize()
does no network, so bootstrap/bootstrap-fast are both efficient.

Full-path hardening (issue #1228 — fleet run exited 1 with 0 records):
  - the CLI accepts the wrapper's `--full` flag in any position, including as
    the only argument, instead of falling through to `sys.exit(1)`;
  - every request runs under a wall-clock deadline and a capped Retry-After so
    a slow/AWOL api.stjornartidindi.is cannot wedge the crawl;
  - page-level checkpoint/resume in data/checkpoint.json, so a relaunch picks
    up at the page it stopped on rather than re-walking 399 pages;
  - a single bad page no longer truncates its department silently — it is
    retried, then skipped and counted, and the run fails loud if nothing at all
    could be enumerated;
  - the whole entrypoint is wrapped so any surviving exception prints a full
    traceback (the fleet only saw a bare "exit: 1").

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # full pull, concurrent
  python bootstrap.py bootstrap --full     # same; --full accepted for the fleet wrapper
  python bootstrap.py --full               # same
  python bootstrap.py update 2025          # documents published in a year onward
  python bootstrap.py test                 # connectivity/parse test

Environment:
  LDH_DEADLINE_SECONDS   overall wall-clock budget for a full run (default 0 = none)
"""

import os
import re
import sys
import json
import time
import logging
import traceback
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import CappedRetry, request_with_deadline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IS.Stjornartidindi")

API = "https://api.stjornartidindi.is/api/v1/adverts"
DEPARTMENTS = ["a-deild", "b-deild", "c-deild"]
PAGE_SIZE = 100

# Per-request budgets. The API answers a 100-item page (with inline full text)
# in a couple of seconds; anything past REQUEST_WALL_TIMEOUT is a stall.
REQUEST_TIMEOUT = (15, 90)
REQUEST_WALL_TIMEOUT = 150
PAGE_ATTEMPTS = 4
MAX_CONSECUTIVE_PAGE_FAILURES = 8

DEPT_LABEL = {
    "a-deild": "A-deild (Lög / Acts)",
    "b-deild": "B-deild (Reglugerðir / Regulations)",
    "c-deild": "C-deild (Milliríkjasamningar / Treaties)",
}


class StjornartidindiScraper(BaseScraper):
    SOURCE_ID = "IS/Stjornartidindi"

    def __init__(self, source_dir=None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "application/json",
        })
        # Cap Retry-After: urllib3 honours the header verbatim inside
        # session.request, so a 503 with a huge value stalls the crawl silently.
        adapter = HTTPAdapter(max_retries=CappedRetry(
            total=2, connect=2, read=2, status=2,
            status_forcelist=(429, 500, 502, 503, 504),
            backoff_factor=1.0,
            allowed_methods=frozenset({"GET"}),
        ))
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.checkpoint_path = Path(self.source_dir) / "data" / "checkpoint.json"
        self.deadline_at = None      # set by run_full()
        self.pages_skipped = 0

    # ── Checkpoint ────────────────────────────────────────────────────
    def _load_checkpoint(self) -> Dict[str, Any]:
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data.setdefault("completed_departments", [])
                data.setdefault("page", {})
                return data
        except (OSError, ValueError):
            pass
        return {"completed_departments": [], "page": {}}

    def _save_checkpoint(self, state: Dict[str, Any]) -> None:
        try:
            self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.checkpoint_path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f)
            tmp.replace(self.checkpoint_path)
        except OSError as e:
            logger.warning("Could not save checkpoint: %s", e)

    def _out_of_time(self) -> bool:
        return self.deadline_at is not None and time.time() >= self.deadline_at

    # ── HTTP ──────────────────────────────────────────────────────────
    def _get_page(self, department: str, page: int) -> Optional[dict]:
        url = f"{API}?department={department}&page={page}&pageSize={PAGE_SIZE}"
        for attempt in range(PAGE_ATTEMPTS):
            if self._out_of_time():
                logger.warning("Wall-clock deadline reached before %s page %d",
                               department, page)
                return None
            try:
                resp = request_with_deadline(
                    self.session, "GET", url,
                    wall_timeout=REQUEST_WALL_TIMEOUT,
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                logger.warning("Page %s of %s attempt %d/%d failed: %s: %s",
                               page, department, attempt + 1, PAGE_ATTEMPTS,
                               type(e).__name__, e)
                if attempt < PAGE_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 30))
        return None

    # ── Helpers ───────────────────────────────────────────────────────
    @staticmethod
    def _html_to_text(html: str) -> str:
        html = re.sub(r"<script.*?</script>", " ", html, flags=re.S)
        html = re.sub(r"<style.*?</style>", " ", html, flags=re.S)
        # keep paragraph/line structure
        html = re.sub(r"</(p|div|h[1-6]|li|tr|br|table)\s*>", "\n", html, flags=re.I)
        html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", html)
        text = _html.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text

    @staticmethod
    def _iso_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return value[:10]

    # ── Enumeration ───────────────────────────────────────────────────
    def _iter_department(
        self,
        department: str,
        state: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Walk one series. With ``state`` the walk is checkpointed per page.

        The checkpoint records the *next* page to fetch, saved only after that
        page's adverts have been yielded, so a killed run resumes at the first
        page whose records may not have reached storage. Re-yielded records
        dedup on ``_id`` in the storage index.
        """
        start_page = 1
        if state:
            start_page = max(1, int(state.get("page", {}).get(department, 1)))

        first = self._get_page(department, start_page)
        if not first:
            logger.error("%s: could not fetch page %d — series skipped",
                         department, start_page)
            return

        paging = first.get("paging") or {}
        total_pages = paging.get("totalPages") or 1
        total_items = paging.get("totalItems") or 0
        logger.info("%s: %d items across %d pages (starting at page %d)",
                    department, total_items, total_pages, start_page)

        page = start_page
        data = first
        consecutive_failures = 0

        while True:
            if data is not None:
                for advert in data.get("adverts", []):
                    if isinstance(advert, dict):
                        advert["_department_slug"] = department
                        yield advert
                consecutive_failures = 0

            page += 1
            if page > total_pages:
                break
            if self._out_of_time():
                logger.warning("%s: wall-clock deadline reached at page %d/%d",
                               department, page, total_pages)
                if state:
                    state["page"][department] = page
                    self._save_checkpoint(state)
                return

            if state:
                state["page"][department] = page
                self._save_checkpoint(state)

            time.sleep(0.3)
            data = self._get_page(department, page)
            if data is None:
                consecutive_failures += 1
                self.pages_skipped += 1
                logger.warning("%s: page %d/%d unreadable (%d consecutive) — skipping",
                               department, page, total_pages, consecutive_failures)
                if consecutive_failures >= MAX_CONSECUTIVE_PAGE_FAILURES:
                    logger.error("%s: %d consecutive page failures — abandoning series",
                                 department, consecutive_failures)
                    return

        if state:
            if department not in state["completed_departments"]:
                state["completed_departments"].append(department)
            state["page"][department] = 1
            self._save_checkpoint(state)
        logger.info("%s: series complete (%d pages)", department, total_pages)

    def fetch_all(self, resume: Optional[bool] = None) -> Generator[Dict[str, Any], None, None]:
        # base_scraper.bootstrap()/bootstrap_fast() call fetch_all() with no
        # arguments, so run_full() flips the resume switch on the instance.
        if resume is None:
            resume = getattr(self, "_resume", False)
        state = self._load_checkpoint() if resume else None
        yielded = 0
        for dep in DEPARTMENTS:
            if state and dep in state.get("completed_departments", []):
                logger.info("%s: already complete per checkpoint — skipping", dep)
                continue
            if self._out_of_time():
                logger.warning("Wall-clock deadline reached — stopping before %s", dep)
                break
            for advert in self._iter_department(dep, state):
                yielded += 1
                yield advert
        if yielded == 0 and not (state and state.get("completed_departments")):
            # Nothing enumerated at all: an IP block or an API change, not an
            # empty gazette. Fail loud rather than reporting a clean 0-record run.
            raise RuntimeError(
                "api.stjornartidindi.is returned no adverts for any series — "
                "the endpoint is unreachable, blocked, or has changed shape"
            )

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        since_str = str(since)[:10]
        # pad a bare year to a full date for comparison
        if re.fullmatch(r"\d{4}", since_str):
            since_str = f"{since_str}-01-01"
        for advert in self.fetch_all():
            pub = self._iso_date(advert.get("publicationDate"))
            if pub and pub >= since_str:
                yield advert

    # ── Normalization ─────────────────────────────────────────────────
    @staticmethod
    def _sub(raw: Dict[str, Any], key: str) -> Dict[str, Any]:
        """Nested object, tolerating the API handing back a scalar or null."""
        value = raw.get(key)
        return value if isinstance(value, dict) else {}

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None
        doc = self._sub(raw, "document")
        html = doc.get("html") or ""
        if not isinstance(html, str) or not html:
            return None
        text = self._html_to_text(html)
        if len(text) < 40:
            return None

        dep = raw.get("_department_slug") or ""
        pub_num = self._sub(raw, "publicationNumber")
        full_num = str(pub_num.get("full") or raw.get("id") or "")
        if not full_num:
            return None
        year = pub_num.get("year")

        dep_code = dep.split("-")[0].upper() if dep else "X"  # A / B / C
        doc_id = f"IS/Stjornartidindi/{dep_code}-{full_num.replace('/', '-')}"

        party = self._sub(raw, "involvedParty").get("title")
        type_title = self._sub(raw, "type").get("title")

        return {
            "_id": doc_id,
            "_source": "IS/Stjornartidindi",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "") or full_num,
            "text": text,
            "date": self._iso_date(raw.get("signatureDate")) or self._iso_date(raw.get("publicationDate")),
            "url": doc.get("pdfUrl") or f"https://www.stjornartidindi.is/Advert.aspx?ID={raw.get('id')}",
            "law_number": full_num,
            "act_type": type_title or DEPT_LABEL.get(dep, dep),
            "series": DEPT_LABEL.get(dep, dep),
            "issuing_body": party,
            "publication_date": self._iso_date(raw.get("publicationDate")),
            "year": year,
            "jurisdiction": "IS",
            "language": "is",
        }

    # ── Full run ──────────────────────────────────────────────────────
    def run_full(self, fast: bool = True) -> dict:
        """Full corpus pull, checkpointed and time-bounded.

        Returns the base-scraper stats dict. bootstrap()/bootstrap_fast() both
        swallow crawl exceptions and report stats, so the caller decides the
        exit code from ``records_fetched``.
        """
        self._resume = True
        budget = int(os.environ.get("LDH_DEADLINE_SECONDS", "0") or 0)
        if budget > 0:
            self.deadline_at = time.time() + budget
            logger.info("Wall-clock budget: %d seconds", budget)

        state = self._load_checkpoint()
        if state.get("completed_departments") or state.get("page"):
            logger.info("Resuming from checkpoint: %s", state)

        stats = self.bootstrap_fast() if fast else self.bootstrap()
        if self.pages_skipped:
            logger.warning("%d page(s) were unreadable and skipped", self.pages_skipped)
        return stats

    def test(self) -> bool:
        try:
            data = self._get_page("a-deild", 1)
            if not data or not data.get("adverts"):
                logger.error("No adverts returned")
                return False
            advert = data["adverts"][0]
            advert["_department_slug"] = "a-deild"
            rec = self.normalize(advert)
            ok = bool(rec and len(rec["text"]) > 40)
            if ok:
                logger.info("Test OK: %s -> %d chars", rec["law_number"], len(rec["text"]))
            return ok
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def _write_samples(scraper: "StjornartidindiScraper", target: int = 15) -> int:
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)
    count = 0
    per_department = max(1, target // len(DEPARTMENTS))
    for dep in DEPARTMENTS:
        data = scraper._get_page(dep, 1)
        if not data:
            continue
        taken = 0
        for advert in data.get("adverts", []):
            if not isinstance(advert, dict):
                continue
            advert["_department_slug"] = dep
            record = scraper.normalize(advert)
            if not record:
                continue
            with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            taken += 1
            logger.info("[%d] %s %s — %d chars", count, dep,
                        record.get("law_number"), len(record["text"]))
            if taken >= per_department:
                break
        if count >= target:
            break
    logger.info("Done: %d sample records saved", count)
    return count


def main(argv) -> int:
    args = argv[1:]
    flags = {a for a in args if a.startswith("-")}
    positional = [a for a in args if not a.startswith("-")]
    command = positional[0] if positional else "bootstrap"
    sample_mode = "--sample" in flags

    scraper = StjornartidindiScraper()

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        return 0 if ok else 1

    # The fleet wrapper re-invokes with `--full` (sometimes as the only
    # argument) after detecting a sample-only ingest. Treat every full-run
    # spelling — bootstrap, bootstrap-fast, bare --full — as the full path;
    # an unrecognised flag must never become an "unknown command" exit 1.
    if command in ("bootstrap", "bootstrap-fast", "full", "bootstrap_fast"):
        if sample_mode:
            return 0 if _write_samples(scraper) >= 10 else 1

        stats = scraper.run_full(fast=command != "bootstrap")
        logger.info("bootstrap_fast complete: %d fetched, %d new, %d errors",
                    stats.get("records_fetched", 0),
                    stats.get("records_new", 0),
                    stats.get("errors", 0))
        if stats.get("error_message"):
            logger.error("Crawl ended early: %s", stats["error_message"])
        if not stats.get("records_fetched"):
            logger.error("No records written — treating the run as a failure")
            return 1
        return 0

    if command == "update":
        since = positional[1] if len(positional) > 1 else str(datetime.now().year)
        count = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            if record:
                count += 1
                logger.info("[%d] %s", count, record.get("law_number"))
        logger.info("Update done: %d records", count)
        return 0

    print(f"Unknown command: {command}")
    print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample|--full]")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        # The fleet only ever saw "Bootstrap exit: 1" (#1228) — always print
        # the exception that caused it.
        traceback.print_exc()
        sys.exit(1)
