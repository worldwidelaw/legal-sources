#!/usr/bin/env python3
"""
US/NC-NCUC -- North Carolina Utilities Commission Orders

Fetches the full text of Orders and decisions issued by the North Carolina
Utilities Commission (NCUC) adjudicating utility dockets — electric, natural
gas, water/sewer, and telephone matters: general rate cases, fuel/rider
adjustments, certificates of public convenience and necessity, complaints,
integrated resource plans, securitization proceedings, competitive-procurement
(RFP) dockets, and rulemakings. Each Commission Order is an administrative
adjudication / edict of a specific docket = case_law. Public domain (US state
government edict, 17 U.S.C. § 105 analogue).

Strategy (Internet Archive / Wayback Machine):

  The NCUC document portal at starw1.ncuc.gov ("STAR" filing system) is
  fronted by Cloudflare, which returns an "Attention Required!" 403 managed-
  challenge page to every datacenter / non-browser vantage (this build vantage
  and Anthropic's fetch vantage both get 403 as of 2026-07-27). The searchable
  Orders / DocketDetails listing pages AND the ViewFile.aspx PDF handler are
  all behind that challenge, so the live host cannot be enumerated or
  downloaded without browser automation / a residential-proxy Cloudflare
  solver.

  The corpus is therefore read from the Internet Archive, which has a large
  crawl of the Commission's public documents: ~14,000 distinct order/filing
  PDFs served at ``starw1.ncuc.gov/NCUC/ViewFile.aspx?Id={GUID}``.

  1. fetch_all() enumerates every archived ``ViewFile.aspx?...&Id={GUID}``
     PDF snapshot via the Wayback CDX API (prefix match, mimetype
     application/pdf, statuscode 200) and de-duplicates on the document GUID
     (keeping the most recent snapshot).

  2. normalize() downloads the raw archived PDF
     (``https://web.archive.org/web/{ts}id_/{original}``) and extracts full
     text. Most of this corpus is *scanned*: roughly three quarters of the
     archived ViewFile PDFs are image-only, so a text-layer-only read returned
     nothing for them and they were silently dropped (issue #1258 — 3,452
     fetched / 10,616 skipped). Extraction therefore runs fitz/PyMuPDF as the
     cheap fast path and, whenever that yields little or no text, hands the
     bytes to the shared ``common.pdf_extract`` cascade (opendataloader →
     pdfplumber → pypdf → fitz → Tesseract OCR).

     OCR is thousands of times more expensive than a text-layer read, so the
     negative outcomes are checkpointed (see ``data/ncuc_checkpoint.json``):
     a GUID that came back as "no text" or "not an order" is not re-OCR'd on
     the next run. The checkpoint records whether OCR was available when the
     verdict was reached and is invalidated when that changes, so a host that
     later gains Tesseract re-reads everything it could not read before rather
     than trusting a verdict taken without it. Orders themselves are always
     re-emitted (they are cheap to re-read once the text layer is known good),
     which keeps a refresh from degenerating into a 0-record run.

     Only genuine Commission ORDERS are kept: the ViewFile URL carries no
     metadata, so each PDF is classified from its own text. A document is an
     order iff it carries the NCUC caption header AND either an ordering /
     decretal clause ("IT IS, THEREFORE, ORDERED", "BY ORDER OF THE
     COMMISSION", ...) or an ``ORDER`` caption doc-type on page 1. Party
     filings (testimony, comments, motions, complaints, briefs, applications)
     are dropped. The docket number, order caption (title) and issued date are
     parsed from the PDF text.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import threading
import time
from collections import Counter
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NC-NCUC")

# Three quarters of this corpus is scanned, and MuPDF emits several lines of
# "SFNT font table missing" / "object is not a stream" / "unknown cid font type"
# per such page straight to stderr. That flood is what the bootstrap.log in
# issue #1258 was made of, and it buries the progress lines that tell an
# operator whether the run is alive.
try:
    fitz.TOOLS.mupdf_display_errors(False)
except Exception:  # pragma: no cover - older PyMuPDF without TOOLS
    pass

SOURCE_ID = "US/NC-NCUC"

CDX_URL = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "https://web.archive.org/web/{ts}id_/{url}"

# A page of an NCUC order runs to thousands of characters. Anything under this
# is a scan whose only text layer is a filing stamp, so it goes to the full
# extraction cascade (and ultimately OCR) rather than being dropped.
MIN_TEXT_LAYER_CHARS = 400

CDX_PARAMS = {
    "url": "starw1.ncuc.gov/NCUC/ViewFile.aspx",
    "matchType": "prefix",
    "filter": ["mimetype:application/pdf", "statuscode:200"],
    "collapse": "digest",
    "output": "text",
    "fl": "original,timestamp",
}

GUID_RE = re.compile(r"[?&]id=([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                     r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", re.I)

# NCUC header markers.
NCUC_MARKERS = ("NORTH CAROLINA", "UTILITIES COMMISSION")

# Decretal / ordering clauses that mark a Commission order (searched on the
# whitespace-collapsed, upper-cased full text).
ORDER_SIGNATURES = (
    "IT IS, THEREFORE, ORDERED",
    "IT IS THEREFORE ORDERED",
    "IT IS, THEREFORE ORDERED",
    "IT IS FURTHER ORDERED",
    "IT IS HEREBY ORDERED",
    "IT IS ORDERED",
    "BY ORDER OF THE COMMISSION",
    "ISSUED BY ORDER OF THE COMMISSION",
    "ORDERED, ADJUDGED",
    "HEREBY ORDERED THAT",
    "ACCORDINGLY, IT IS ORDERED",
)

# Page-1 caption doc-type: an ORDER descriptor sitting in the right-hand
# caption box (after the ")" column of "In the Matter of ...").
CAPTION_ORDER_RE = re.compile(
    r"\)\s*(?:\)\s*)*"
    r"((?:FINAL|RECOMMENDED|INTERLOCUTORY|SUPPLEMENTAL|ERRATA|AMENDED|"
    r"CORRECTED|SECOND|THIRD|FOURTH|FIFTH|SUBSEQUENT)?\s*"
    r"ORDER\b[ A-Z0-9,'&./\-]{0,90})"
)
# Fallback caption search without the ")" anchor (some layouts lose the column).
BARE_ORDER_CAP_RE = re.compile(
    r"\b((?:FINAL|RECOMMENDED|INTERLOCUTORY|SUPPLEMENTAL|ERRATA|AMENDED|"
    r"CORRECTED|SECOND|THIRD|FOURTH|FIFTH|SUBSEQUENT)\s+"
    r"ORDER\b[ A-Z0-9,'&./\-]{0,90})"
)

# Docket number, e.g. "DOCKET NO. E-7, SUB 1276", "DOCKET NO. W-1300, SUB 60",
# "DOCKET NO. SP-13695, SUB 1", "DOCKET NO. EMP-103".
DOCKET_RE = re.compile(
    r"DOCKET\s+NO\.?\s*([A-Z]{1,4}-\d+[A-Z]?(?:\s*,?\s*SUB\s*\d+)?)", re.I
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
# "This the 22nd day of October, 2025" — the order execution/issued date.
EXEC_DATE_RE = re.compile(
    r"this\s+the\s+(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+"
    r"(January|February|March|April|May|June|July|August|September|October|"
    r"November|December),?\s+(\d{4})",
    re.I,
)
# Generic "Month DD, YYYY" fallback.
MONTH_DAY_YEAR_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),?\s+(\d{4})",
    re.I,
)


def _mk_date(mon_name: str, day: str, year: str) -> str | None:
    mon = MONTHS.get(mon_name.lower())
    if not mon:
        return None
    d, y = int(day), int(year)
    if not (1 <= d <= 31 and 1980 <= y <= 2100):
        return None
    return f"{y:04d}-{mon:02d}-{d:02d}"


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_docket(raw: str) -> str:
    d = re.sub(r"\s+", " ", raw).strip().upper()
    d = d.replace(" SUB", ", SUB") if "SUB" in d and ", SUB" not in d else d
    d = re.sub(r",\s*,", ",", d)
    d = re.sub(r"\s*,\s*SUB", ", SUB", d)
    return d


def parse_date(text: str) -> str | None:
    # 1. order execution clause "this the Nth day of Month, YYYY"
    m = EXEC_DATE_RE.search(text)
    if m:
        d = _mk_date(m.group(2), m.group(1), m.group(3))
        if d:
            return d
    # 2. an issued/dated/entered anchor followed by a Month DD, YYYY
    for anchor in ("ISSUED", "DATED", "ENTERED", "EFFECTIVE"):
        idx = text.upper().rfind(anchor)
        if idx != -1:
            m = MONTH_DAY_YEAR_RE.search(text[idx:idx + 120])
            if m:
                d = _mk_date(m.group(1), m.group(2), m.group(3))
                if d:
                    return d
    # 3. last Month DD, YYYY in the document (order date usually near the end)
    matches = list(MONTH_DAY_YEAR_RE.finditer(text))
    if matches:
        m = matches[-1]
        d = _mk_date(m.group(1), m.group(2), m.group(3))
        if d:
            return d
    return None


class NCUCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 0.4

        # Negative-outcome checkpoint (see module docstring).
        self._ckpt_path = Path(__file__).parent / "data" / "ncuc_checkpoint.json"
        self._ocr_available = pdf_extract._ocr_available()
        if not self._ocr_available:
            logger.warning(
                "OCR is not available on this host (pytesseract/PyMuPDF/tesseract "
                "binary) — the ~75%% of NCUC orders that are image-only scans "
                "cannot be read and will be skipped (issue #1258)."
            )
        self._skipped: dict[str, str] = self._load_checkpoint()
        self._ckpt_dirty = 0
        self._ckpt_lock = threading.Lock()
        # Per-run skip tally, so the log says *why* documents did not land
        # rather than leaving one undifferentiated "normalize returned None".
        self._skip_counts: Counter = Counter()

    # ---- checkpoint ---------------------------------------------------------

    def _load_checkpoint(self) -> dict[str, str]:
        """GUIDs already known to yield no order, from a previous run.

        Verdicts reached without OCR are discarded once OCR becomes available:
        "this PDF has no text" is only true relative to the backends the host
        had at the time, and caching it forever is how a fixable gap turns
        into a permanent one.
        """
        try:
            raw = json.loads(self._ckpt_path.read_text())
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.warning(f"Ignoring unreadable checkpoint {self._ckpt_path}: {e}")
            return {}
        if raw.get("ocr_available") != self._ocr_available:
            logger.info(
                "OCR availability changed since the last run "
                f"({raw.get('ocr_available')} -> {self._ocr_available}) — "
                "discarding cached skip verdicts and re-reading every document"
            )
            return {}
        skipped = raw.get("skipped") or {}
        if skipped:
            logger.info(f"Checkpoint: skipping {len(skipped)} known non-order documents")
        return skipped

    def _save_checkpoint(self, force: bool = False) -> None:
        with self._ckpt_lock:
            if not force and self._ckpt_dirty < 100:
                return
            self._ckpt_dirty = 0
            payload = {"ocr_available": self._ocr_available, "skipped": self._skipped}
        try:
            self._ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._ckpt_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(payload))
            tmp.replace(self._ckpt_path)
        except Exception as e:
            logger.warning(f"Could not write checkpoint: {e}")

    def _tally(self, reason: str) -> None:
        # bootstrap_fast normalizes on worker threads, so the counters this
        # source reports have to be incremented under the lock.
        with self._ckpt_lock:
            self._skip_counts[reason] += 1

    def _skip(self, guid: str, reason: str) -> None:
        with self._ckpt_lock:
            self._skipped[guid] = reason
            self._skip_counts[reason] += 1
            self._ckpt_dirty += 1
        self._save_checkpoint()

    # ---- low-level fetch ----------------------------------------------------

    def _get(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.session.get(url, timeout=90)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- CDX enumeration ----------------------------------------------------

    def _cdx_rows(self) -> list[tuple[str, str]]:
        for attempt in range(5):
            try:
                resp = self.session.get(CDX_URL, params=CDX_PARAMS, timeout=240)
                if resp.status_code == 200:
                    rows = []
                    for line in resp.text.splitlines():
                        parts = line.split()
                        if len(parts) >= 2:
                            rows.append((parts[0], parts[1]))
                    return rows
                logger.warning(f"CDX HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"CDX error (attempt {attempt + 1}): {e}")
            time.sleep(3 * (attempt + 1))
        return []

    def _enumerate_docs(self) -> list[dict]:
        """Unique archived NCUC document PDFs (best snapshot per GUID)."""
        rows = self._cdx_rows()
        logger.info(f"CDX returned {len(rows)} archived ViewFile PDF snapshots")
        best: dict[str, dict] = {}
        for orig, ts in rows:
            m = GUID_RE.search(orig)
            if not m:
                continue
            guid = m.group(1).lower()
            cur = best.get(guid)
            if cur is None or ts > cur["ts"]:
                best[guid] = {"guid": guid, "orig": orig, "ts": ts}
        records = sorted(best.values(), key=lambda r: r["ts"])
        logger.info(f"{len(records)} unique document GUIDs after dedup")
        return records

    # ---- PDF text extraction ------------------------------------------------

    def _fitz_text(self, pdf_bytes: bytes) -> str:
        """Cheap text-layer read — the fast path for born-digital orders."""
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            logger.debug(f"PDF open failed: {e}")
            return ""
        try:
            return clean_text("\n".join(page.get_text() for page in doc))
        except Exception as e:
            logger.debug(f"PDF text-layer read failed: {e}")
            return ""
        finally:
            doc.close()

    def _extract_pdf(self, pdf_bytes: bytes, doc_id: str) -> str:
        """Full text of one archived order PDF.

        fitz first because it is cheap and ~a quarter of the corpus is
        born-digital; everything else falls through to the shared cascade,
        which ends in Tesseract OCR for the image-only majority.
        """
        text = self._fitz_text(pdf_bytes)
        if len(text) >= MIN_TEXT_LAYER_CHARS:
            return text

        # force=True: the Neon skip-if-already-ingested guard would make
        # normalize() return None for every previously ingested order, and
        # resume is handled by this source's own checkpoint instead.
        try:
            deep = pdf_extract.extract_pdf_markdown(
                SOURCE_ID,
                doc_id,
                pdf_bytes=pdf_bytes,
                table="case_law",
                force=True,
            )
        except Exception as e:
            logger.warning(f"Extraction cascade failed for {doc_id}: {e}")
            deep = None
        deep = clean_text(deep or "")
        return deep if len(deep) > len(text) else text

    # ---- order classification ----------------------------------------------

    @staticmethod
    def _trim_caption(cap: str) -> str:
        # Cut the signature/body that follows the caption box on page 1.
        cap = re.sub(r"\s+", " ", cap).strip(" ,.-")
        cap = re.split(
            r"\s+(?:BY THE |BEFORE THE |HEARD BEFORE |ISSUED |IN THE MATTER)",
            cap, maxsplit=1, flags=re.I,
        )[0]
        return cap.strip(" ,.-")

    def _order_caption(self, page1_collapsed: str) -> str | None:
        m = CAPTION_ORDER_RE.search(page1_collapsed)
        if m:
            cap = self._trim_caption(m.group(1))
            # Avoid catching prose like "ORDERING PARAGRAPH"; keep short caps.
            if 5 <= len(cap) <= 95 and "ORDERED" not in cap.upper():
                return cap.title()
        m = BARE_ORDER_CAP_RE.search(page1_collapsed)
        if m:
            cap = self._trim_caption(m.group(1))
            if 8 <= len(cap) <= 95:
                return cap.title()
        return None

    def _classify(self, text: str) -> tuple[bool, str | None]:
        """Return (is_order, order_caption)."""
        up = text.upper()
        if not all(mk in up for mk in NCUC_MARKERS):
            return False, None
        page1 = re.sub(r"\s+", " ", text[:1500])
        caption = self._order_caption(page1)
        has_clause = any(sig in up for sig in ORDER_SIGNATURES)
        return (bool(caption) or has_clause), caption

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        guid = raw["guid"]
        doc_id = f"{SOURCE_ID}/{guid}"
        wb = WB_RAW.format(ts=raw["ts"], url=raw["orig"])
        pdf = self._get(wb)
        if not pdf or pdf[:5] != b"%PDF-":
            # Not checkpointed: a failed archive download is transient.
            self._tally("download_failed")
            return None

        text = self._extract_pdf(pdf, doc_id)
        if not text or len(text) < MIN_TEXT_LAYER_CHARS:
            self._skip(guid, "no_text" if self._ocr_available else "no_text_no_ocr")
            return None

        is_order, caption = self._classify(text)
        if not is_order:
            self._skip(guid, "not_an_order")
            return None

        dk = DOCKET_RE.search(re.sub(r"\s+", " ", text[:1500]))
        docket = normalize_docket(dk.group(1)) if dk else None
        date = parse_date(text)

        if caption:
            title = f"NCUC Docket {docket} — {caption}" if docket else f"NCUC — {caption}"
        else:
            title = f"NCUC Docket {docket} — Order" if docket else "NCUC Order"

        # Live-host canonical URL (Cloudflare-blocked, but the canonical ref).
        orig_live = raw["orig"]
        m = GUID_RE.search(orig_live)
        canonical = (
            f"https://starw1.ncuc.gov/NCUC/ViewFile.aspx?Id={m.group(1)}"
            if m else orig_live
        )

        return {
            "_id": f"US/NC-NCUC/{raw['guid']}",
            "_source": "US/NC-NCUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "docket_number": docket,
            "title": title,
            "text": text,
            "url": canonical,
            "archive_url": wb,
            "date": date,
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing NCUC order enumeration via Wayback CDX...")
        try:
            records = self._enumerate_docs()
            if len(records) < 500:
                logger.error(f"  Too few docs enumerated: {len(records)}")
                return False
            logger.info(f"  Enumerated {len(records)} unique documents")
            got = 0
            for r in records[-40:]:
                rec = self.normalize(r)
                if rec and len(rec["text"]) > 400:
                    got += 1
                    logger.info(
                        f"  Order OK ({len(rec['text'])} chars, "
                        f"docket={rec.get('docket_number')}, date={rec.get('date')})"
                    )
                    if got >= 2:
                        break
            if got == 0:
                logger.error("  No orders extracted from recent sample")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # ---- iteration ----------------------------------------------------------

    def _iter_pending(self) -> Generator[dict, None, None]:
        """Every archived document that is not a checkpointed non-order.

        Orders are deliberately re-emitted every run: re-reading a known
        text layer is cheap, and skipping them would turn each refresh into a
        0-record run that the fleet reads as a failure.
        """
        records = self._enumerate_docs()
        pending = [r for r in records if r["guid"] not in self._skipped]
        if len(pending) != len(records):
            logger.info(
                f"{len(pending)} documents to read "
                f"({len(records) - len(pending)} checkpointed as non-orders)"
            )
        try:
            yield from pending
        finally:
            # Also runs when the consumer stops early — sample mode takes the
            # first 12 orders and closes the generator, and a fleet teardown
            # kills a full run mid-corpus. Without this the expensive verdicts
            # of a partial run would be thrown away.
            self._save_checkpoint(force=True)
            if self._skip_counts:
                logger.info(
                    "skip summary: "
                    + ", ".join(f"{k}={v}" for k, v in sorted(self._skip_counts.items()))
                )

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_pending()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # No per-doc date at enumeration time; re-scan and let normalize filter.
        yield from self._iter_pending()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NC-NCUC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NCUCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
