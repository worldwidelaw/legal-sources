#!/usr/bin/env python3
"""
US/USDA-AgricultureDecisions -- USDA "Agriculture Decisions"
(OALJ Initial Decisions + Secretary's Judicial Officer final decisions)

Fetches the full text of decisions and orders issued in USDA adjudicatory
proceedings. "Agriculture Decisions" is the official combined reporter of
the U.S. Department of Agriculture's Office of Administrative Law Judges
(OALJ) initial decisions and the Secretary's Judicial Officer (JO) final
decisions, issued under the Packers & Stockyards Act, Animal Welfare Act,
Perishable Agricultural Commodities Act, Agricultural Marketing Agreement
Act, Horse Protection Act, Organic Foods Production Act, the meat/poultry
inspection acts, plant/animal quarantine statutes, and other
USDA-administered laws. Each entry resolves a specific contested case =
case_law. These are U.S. federal-government works in the public domain
(17 U.S.C. § 105, government edicts).

Access (no JavaScript, no CAPTCHA, no auth):
  The official USDA OALJ site (oalj.oha.usda.gov -> www.usda.gov/oha/oalj)
  is unreachable from most build vantages (HTTP/2 INTERNAL_ERROR /
  connect-timeout). The full published run is mirrored, born-digital with a
  text layer, by the National Agricultural Law Center (a USDA-funded
  academic center) as semi-annual COMPILATION PDFs:

      https://nationalaglawcenter.org/wp-content/uploads/assets/
          agdecisions/VOLUME-{NN}-BOOK-{N}.pdf

  where NN is the volume number (roughly Vol 55 = 1996 ... Vol 78 = 2019)
  and N is the book (Book One = Jan-Jun, Book Two = Jul-Dec). Each
  compilation concatenates many individual decisions. Every departmental
  decision begins with a caption block:

      In re: <RESPONDENT NAME>.
      Docket No. <NN-NNNN>.
      <Decision type>.
      Filed <Month DD, YYYY>.

  and court decisions with "<NAME> v. USDA. No. <docket>. ... Filed <date>."
  We split each compilation on that caption/"Filed <date>" header into one
  record per decision so one record = one contested case.

Strategy:
  1. Enumerate the mirror's compilation PDFs (Vol 55 + Vols 59-78, Books 1-2;
     others 404 and are skipped gracefully).
  2. Download each PDF (curl, browser UA, ~1 req/s); extract text via
     PyMuPDF (fitz) born-digital text layer.
  3. Whitespace-normalize, then split on the per-decision caption anchored by
     "Filed <Month DD, YYYY>"; emit one raw record per decision (caption
     title, docket number, filed date, full body text).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
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
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.USDA-AgricultureDecisions")

MIRROR_BASE = (
    "https://nationalaglawcenter.org/wp-content/uploads/assets/agdecisions"
)
# Volumes confirmed present on the mirror (206/200); missing ones (56-58, 79)
# simply 404 and are skipped. We probe a generous range so future additions
# are picked up automatically.
VOLUME_RANGE = list(range(55, 82))
BOOKS = (1, 2)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

# "Filed <Month DD, YYYY>" closes each decision's caption block. This is the
# universal anchor across born-digital (2013+) and OCR'd (older) volumes.
FILED_RE = re.compile(
    r"Filed\s+("
    r"January|February|March|April|May|June|July|August|September|"
    r"October|November|December"
    r")\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
# Court-decision caption (e.g. "LAMERS DAIRY, INC. v. USDA.")
COURT_CAP_RE = re.compile(
    r"[A-Z][A-Z0-9&.,\-'()/ ]{2,110}\sv\.\s[A-Z][A-Za-z0-9&.,\-'()/ ]{2,110}\.",
)
# Docket number(s) inside a caption block. Captions may carry a year and a
# statute prefix ("2003 AMA Docket No. F&V 989-7.") and multiple
# semicolon/comma-separated dockets ("Docket Nos. 15-0058; 15-0059.").
DOCKET_RE = re.compile(
    r"Docket\s+Nos?\.?\s*"
    r"([0-9A-Z][0-9A-Za-z&./\-;, ]{1,90}?)\.\s",
    re.I,
)


class USDAAgDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "180", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=210,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _pdf_text(blob: bytes) -> str:
        """Extract the born-digital text layer with PyMuPDF."""
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required but not installed")
        doc = fitz.open(stream=blob, filetype="pdf")
        try:
            parts = []
            for i in range(doc.page_count):
                parts.append(doc[i].get_text())
                # keep peak memory low on large multi-hundred-page volumes
                if hasattr(doc[i], "clean_contents"):
                    pass
            return "\n".join(parts)
        finally:
            doc.close()

    @staticmethod
    def _normalize_ws(txt: str) -> str:
        txt = re.sub(r"[ \t]*\n[ \t]*", " ", txt)
        return re.sub(r"\s+", " ", txt).strip()

    @staticmethod
    def _clean_title(caption: str) -> str:
        # trim at the docket line; collapse whitespace
        cap = re.split(r"\s+(?:Docket|(?:19|20)\d{2}\s+[A-Z][A-Z&.]*\s+Docket)",
                       caption, maxsplit=1)[0]
        cap = re.sub(r"\s+", " ", cap).strip().rstrip(",;").strip()
        # drop a leading ALL-CAPS statute-section header if it slipped in
        cap = re.sub(r"^(?:[A-Z][A-Z&'./\- ]{6,}?)(In re:|[A-Z][a-z])", r"\1", cap)
        return cap[:300]

    @classmethod
    def _statute(cls, body: str) -> str | None:
        # keyword line just after "Filed <date>." usually starts "AWA – ..."
        m = re.search(
            r"Filed\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4}\.?\s*\*?\s*"
            r"([A-Z][A-Za-z&]{1,7})\s+[–—-]",
            body,
        )
        return m.group(1) if m else None

    # --------------------------------------------------------- discovery
    def discover_documents(self) -> list[dict]:
        out = []
        for vol in VOLUME_RANGE:
            for book in BOOKS:
                out.append({
                    "vol": vol,
                    "book": book,
                    "doc_url": f"{MIRROR_BASE}/VOLUME-{vol}-BOOK-{book}.pdf",
                    "safe_slug": f"vol{vol:02d}-book{book}",
                })
        # newest first
        out.sort(key=lambda r: (r["vol"], r["book"]), reverse=True)
        return out

    # ------------------------------------------------- split a compilation
    def _split_decisions(self, comp: dict, raw_text: str) -> list[dict]:
        norm = self._normalize_ws(raw_text)
        starts = []  # (caption_start, filed_end, y, m, d)
        for f in FILED_RE.finditer(norm):
            wstart = max(0, f.start() - 750)
            window = norm[wstart:f.start()]
            if "ocket No" not in window:
                continue  # not a real decision caption block
            ci = window.rfind("In re:")
            if ci == -1:
                cm = list(COURT_CAP_RE.finditer(window))
                if not cm:
                    continue
                ci = cm[-1].start()
            start = wstart + ci
            mo = MONTHS.get(f.group(1).lower())
            day = int(f.group(2))
            yr = int(f.group(3))
            starts.append((start, f.end(), yr, mo, day))
        starts.sort()
        # drop near-duplicate starts (same caption matched twice)
        dedup = []
        for s in starts:
            if dedup and s[0] - dedup[-1][0] < 40:
                continue
            dedup.append(s)

        records = []
        for i, (s, fe, yr, mo, day) in enumerate(dedup):
            end = dedup[i + 1][0] if i + 1 < len(dedup) else len(norm)
            body = norm[s:end].strip()
            if len(body) < 400:
                continue
            caption = norm[s:fe]
            title = self._clean_title(caption)
            dm = DOCKET_RE.search(caption)
            docket = re.sub(r"\s+", " ", dm.group(1)).strip() if dm else None
            date = None
            if mo and 1 <= day <= 31 and 1990 <= yr <= 2035:
                date = f"{yr:04d}-{mo:02d}-{day:02d}"
            records.append(self._make_raw(comp, title, docket, date, body, i))
        return records

    def _make_raw(self, comp, title, docket, date, body, seq) -> dict:
        h = hashlib.sha1(body.encode("utf-8", "replace")).hexdigest()[:10]
        dslug = re.sub(r"[^A-Za-z0-9]+", "-", docket).strip("-") if docket else ""
        idbase = f"{comp['safe_slug']}-{dslug or seq}"
        return {
            "title": title or f"USDA Agriculture Decisions Vol. {comp['vol']}",
            "docket": docket,
            "statute": self._statute(body),
            "text": body,
            "date": date,
            "doc_url": comp["doc_url"],
            "volume": comp["vol"],
            "book": comp["book"],
            "id_slug": f"{idbase}-{h}",
        }

    def _process_pdf(self, comp: dict) -> list[dict]:
        blob = self._curl_bytes(comp["doc_url"])
        if not blob:
            return []
        if blob[:4] != b"%PDF":
            # 404 HTML page or other non-PDF — skip
            return []
        try:
            text = self._pdf_text(blob)
        except Exception as e:
            logger.warning(f"PDF extract failed for {comp['doc_url']}: {e}")
            return []
        if not text or len(text.strip()) < 400:
            return []
        recs = self._split_decisions(comp, text)
        logger.info(
            f"  Vol {comp['vol']} Book {comp['book']}: {len(recs)} decisions"
        )
        return recs

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing USDA Agriculture Decisions mirror + split...")
        try:
            comps = self.discover_documents()
            # find the first reachable volume
            for comp in comps:
                recs = self._process_pdf(comp)
                if recs:
                    r0 = recs[0]
                    logger.info(
                        f"  Split OK — Vol {comp['vol']} Book {comp['book']}: "
                        f"{len(recs)} decisions; first '{r0['title'][:50]}' "
                        f"docket={r0.get('docket')} date={r0.get('date')} "
                        f"({len(r0['text'])} chars)"
                    )
                    logger.info("API test PASSED")
                    return True
            logger.error("  No reachable compilation produced decisions")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/USDA-AgricultureDecisions/{raw['id_slug']}",
            "_source": "US/USDA-AgricultureDecisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["id_slug"],
            "case_number": raw.get("docket"),
            "issuer": "USDA Office of Administrative Law Judges / Judicial Officer",
            "statute": raw.get("statute"),
            "volume": raw.get("volume"),
            "book": raw.get("book"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_ids: set[str] = set()
        comps = self.discover_documents()
        for comp in comps:
            recs = self._process_pdf(comp)
            for raw in recs:
                if raw["id_slug"] in seen_ids:
                    continue
                seen_ids.add(raw["id_slug"])
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and emitted >= 12:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/USDA-AgricultureDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = USDAAgDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
