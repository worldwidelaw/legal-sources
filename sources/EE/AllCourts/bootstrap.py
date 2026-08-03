#!/usr/bin/env python3
"""
EE/AllCourts -- Estonian Court Decisions (Kohtulahendid, all courts)

Riigi Teataja (the official State Gazette of Estonia) publishes the full text of
decisions of all Estonian courts -- the county courts (maakohus), administrative
courts (halduskohus), circuit courts of appeal (ringkonnakohus) and, for older
material, the Supreme Court (Riigikohus, also covered separately by
EE/SupremeCourt). Each published decision (kohtumäärus / kohtuotsus) finally
adjudicates a specific case = case_law, and is a public, official government work
(Estonian Copyright Act (Autoriõiguse seadus) §5 excludes court decisions and
other official documents from copyright protection -> public domain).

History: this source was previously blocked ("no_full_text_access") because the
old riigiteataja.ee/kohtulahendid/ site served the full text via a Cloudflare-JS
gated fail.html endpoint. Riigi Teataja has since been rebuilt as an Angular SPA
backed by an OPEN public JSON API, which is what this scraper uses.

Strategy:
  - Search endpoint (POST, JSON):
        /public-api/api/v1/kohtuteave/otsing/kohtulahendid
    body: {"general": {"searchText": "", "sort": "LahendiKuulutamiseAeg",
                        "sortAscending": <bool>, "searchAfter": <offset>},
           "precise": {}}
    returns {"kokku": <total>, "tulemused": [ ...30 decisions... ]}.
    `searchAfter` is a numeric offset (increments of 30); walking it ascending
    (oldest first) keeps the frontier stable so a checkpointed run resumes and
    completes over the ~722,000-decision corpus without offset drift.
  - Full text: each result carries `avalikustatudFailiId` (the published file);
    the born-digital PDF is at
        /public-api/api/v1/kohtuteave/kohtulahendid/{avalikustatudFailiId}/file
    extracted with PyMuPDF (fitz); no OCR needed for born-digital files.

Data:
  - ~722,000 decisions of all Estonian courts (2006-present for lower courts)
  - Language: Estonian (et)
  - Auth: None (open public API)

Usage:
  python bootstrap.py bootstrap          # Full pull (checkpointed, resumable)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (newest decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EE.AllCourts")

BASE_URL = "https://www.riigiteataja.ee"
API = "/public-api/api/v1/kohtuteave"
SEARCH_PATH = f"{API}/otsing/kohtulahendid"
FILE_PATH = f"{API}/kohtulahendid/{{fid}}/file"
PAGE_SIZE = 30  # fixed server-side
SORT_FIELD = "LahendiKuulutamiseAeg"

CHECKPOINT = Path(__file__).parent / "data" / "checkpoint.json"


def _pdf_text(pdf_bytes: bytes) -> str:
    if not fitz:
        raise RuntimeError("PyMuPDF (fitz) is required to extract Estonian court decision PDFs")
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return "\n".join(page.get_text() for page in doc)
    finally:
        doc.close()


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in text.split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _iso_date(val: Optional[str]) -> Optional[str]:
    """'2026-07-17T00:00:00+03:00' -> '2026-07-17'."""
    if not val:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", val)
    return m.group(1) if m else None


class EstonianCourtsScraper(BaseScraper):
    """Scraper for all Estonian court decisions via the Riigi Teataja public API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Language": "et,en;q=0.8",
            },
            timeout=90,
        )
        self._newest_first = False  # sample/update flip this on

    # ── API helpers ────────────────────────────────────────────────
    def _search(self, offset: int, newest_first: bool) -> Optional[Dict[str, Any]]:
        body = {
            "general": {
                "searchText": "",
                "searchText2": "",
                "logicalOperator": "AND",
                "sort": SORT_FIELD,
                "sortAscending": not newest_first,
                "searchAfter": offset,
            },
            "precise": {},
        }
        try:
            self.rate_limiter.wait()
            resp = self.client.post(SEARCH_PATH, json_data=body)
            if resp.status_code != 200:
                logger.warning(f"search offset {offset}: HTTP {resp.status_code}")
                return None
            return resp.json()
        except Exception as e:
            logger.warning(f"Error searching offset {offset}: {e}")
            return None

    def _fetch_file(self, fid: int) -> Optional[bytes]:
        url = FILE_PATH.format(fid=fid)
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url, headers={"Accept": "application/pdf,*/*"})
            if resp.status_code != 200:
                logger.warning(f"file {fid}: HTTP {resp.status_code}")
                return None
            data = resp.content
            if not data[:5].startswith(b"%PDF"):
                logger.warning(f"file {fid}: not a PDF ({data[:16]!r})")
                return None
            return data
        except Exception as e:
            logger.warning(f"Error fetching file {fid}: {e}")
            return None

    # ── checkpoint ─────────────────────────────────────────────────
    def _load_checkpoint(self) -> int:
        try:
            if CHECKPOINT.exists():
                return int(json.loads(CHECKPOINT.read_text()).get("offset", 0))
        except Exception:
            pass
        return 0

    def _save_checkpoint(self, offset: int) -> None:
        try:
            CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
            CHECKPOINT.write_text(json.dumps({"offset": offset}))
        except Exception as e:
            logger.warning(f"checkpoint save failed: {e}")

    # ── iteration ──────────────────────────────────────────────────
    def _iter_results(self, newest_first: bool, start_offset: int = 0,
                      max_items: Optional[int] = None,
                      use_checkpoint: bool = False) -> Generator[Dict[str, Any], None, None]:
        offset = start_offset
        total = None
        emitted = 0
        first = True
        while True:
            page = self._search(offset, newest_first)
            if page is None:
                # transient error — stop rather than silently truncate
                if first:
                    raise RuntimeError(
                        "Riigi Teataja court-search API returned no response — "
                        "site blocked or API changed"
                    )
                logger.warning(f"stopping at offset {offset} after failed page")
                break
            first = False
            if total is None:
                total = page.get("kokku", 0)
                logger.info(f"corpus size: {total} decisions (starting at offset {offset})")
            results = page.get("tulemused") or []
            if not results:
                break
            for r in results:
                yield r
                emitted += 1
                if max_items and emitted >= max_items:
                    return
            offset += PAGE_SIZE
            if use_checkpoint:
                self._save_checkpoint(offset)
            if total and offset >= total:
                break

    def _result_to_raw(self, r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        fid = r.get("avalikustatudFailiId")
        if not fid:
            return None  # no published full-text file (summary-only / restricted)
        pdf = self._fetch_file(fid)
        if not pdf:
            return None
        try:
            raw_text = _pdf_text(pdf)
        except Exception as e:
            logger.warning(f"extract file {fid} failed: {e}")
            return None
        return {"meta": r, "text": raw_text}

    # ── BaseScraper contract ───────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        if self._newest_first:
            # sample mode: recent, born-digital decisions, no checkpoint churn
            for r in self._iter_results(newest_first=True):
                raw = self._result_to_raw(r)
                if raw:
                    yield raw
            return
        start = self._load_checkpoint()
        for r in self._iter_results(newest_first=False, start_offset=start,
                                    use_checkpoint=True):
            raw = self._result_to_raw(r)
            if raw:
                yield raw

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since = since if since.tzinfo else since.replace(tzinfo=timezone.utc)
        for r in self._iter_results(newest_first=True):
            d = _iso_date(r.get("lahendiKuulutamiseAeg"))
            if d:
                try:
                    if datetime.fromisoformat(d).replace(tzinfo=timezone.utc) < since:
                        break  # newest-first: everything after is older
                except ValueError:
                    pass
            raw = self._result_to_raw(r)
            if raw:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        meta = raw.get("meta") or {}
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None  # scanned/empty — needs OCR, skip

        obj_id = meta.get("objektId")
        case_no = (meta.get("kohtuasjaNumber") or "").strip()
        court = (meta.get("toiminguEsitajaAsutus") or "").strip() or None
        proc_type = (meta.get("menetlusLiikKlVaartus") or "").strip() or None
        status = (meta.get("staatusKlVaartus") or "").strip() or None
        date = _iso_date(meta.get("lahendiKuulutamiseAeg"))

        categories = []
        for c in (meta.get("asjaKategooriaList") or []):
            v = c.get("asjaKategooriaVaartus")
            if v:
                categories.append(v)

        title_bits = [b for b in (court, case_no, proc_type) if b]
        title = " — ".join(title_bits) if title_bits else (case_no or f"Kohtulahend {obj_id}")

        uid = obj_id if obj_id is not None else (case_no or meta.get("avalikustatudFailiId"))

        return {
            "_id": f"EE-{uid}",
            "_source": "EE/AllCourts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}{FILE_PATH.format(fid=meta.get('avalikustatudFailiId'))}",
            "case_number": case_no or None,
            "court": court,
            "proceeding_type": proc_type,
            "status": status,
            "categories": categories or None,
            "ecli": None,
            "jurisdiction": "EE",
            "language": "et",
        }

    # ── connectivity test ──────────────────────────────────────────
    def test_connection(self):
        print("Testing Riigi Teataja court-decisions API...")
        page = self._search(0, newest_first=True)
        if not page:
            print("  search FAILED")
            return
        print(f"  total decisions: {page.get('kokku')}")
        results = page.get("tulemused") or []
        print(f"  page 1: {len(results)} results")
        for r in results:
            if r.get("avalikustatudFailiId"):
                raw = self._result_to_raw(r)
                if raw:
                    norm = self.normalize(raw)
                    print(f"  sample: {r.get('kohtuasjaNumber')} | "
                          f"{_iso_date(r.get('lahendiKuulutamiseAeg'))} | "
                          f"{len(norm['text']) if norm else 0} chars - OK")
                break


def main():
    scraper = EstonianCourtsScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            scraper._newest_first = True
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap (checkpointed)")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
