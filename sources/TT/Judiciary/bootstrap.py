#!/usr/bin/env python3
"""
TT/Judiciary -- Trinidad and Tobago Judiciary Judgments (Court of Appeal & High Court)

The Judiciary of Trinidad and Tobago publishes its written judgments as PDF/RTF/HTM
documents on its library server at ``webopac.ttlawcourts.org`` (linked from the
"Recent Judgments" index at https://www.ttlawcourts.org/). That host (IP
200.1.109.12, a Trinidad-only network block) refuses connections from foreign /
datacenter IPs, so the documents are not directly retrievable off-island.

This scraper instead reads the judgment corpus from the **Internet Archive Wayback
Machine**, which has captured the ``webopac.ttlawcourts.org/LibraryJud/Judgments/*``
tree (Court of Appeal ``coa/``, High Court ``HC/`` and Magistrates ``mag/``
folders). We enumerate every archived judgment document via the Wayback CDX API and
fetch each captured file through the raw ``/web/<timestamp>id_/<url>`` endpoint,
then extract the full text (PDF via PyMuPDF, RTF/HTM via a light text strip).

Path layouts encountered:
  coa/{year}/{judge}/{FILE}.pdf     -- Court of Appeal
  HC/{judge}/{year}/{FILE}.pdf      -- High Court (judge/year order swapped)
  mag/.../{FILE}.pdf                -- Magistrates' Court

Filenames encode the case reference + decision date, e.g.
  CvA_08_45DD16nov2011.pdf   -> Civil Appeal 45 of 2008, decided 16 Nov 2011
  cv_08_00264DD17jan2011.pdf -> High Court civil claim CV 264 of 2008, 17 Jan 2011

Usage:
  python bootstrap.py bootstrap          # Full fetch
  python bootstrap.py bootstrap --sample # Sample records -> sample/
  python bootstrap.py bootstrap-fast     # Concurrent full run (fleet entry point)
  python bootstrap.py test               # Connectivity / enumeration test
"""

import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Dict, Any, Optional, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TT.Judiciary")

ORIGIN_HOST = "webopac.ttlawcourts.org"
JUDG_PREFIX = "LibraryJud/Judgments"
CDX_URL = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "http://web.archive.org/web/{ts}id_/{url}"

# Extensions we can extract full text from (skip image/* scanned page photos).
TEXT_EXTS = (".pdf", ".rtf", ".htm", ".html", ".doc", ".txt")
MIN_TEXT_CHARS = 200

COURT_NAMES = {
    "coa": "Court of Appeal",
    "hc": "High Court",
    "mag": "Magistrates' Court",
}

CASE_TYPE_NAMES = {
    "cva": "Civil Appeal",
    "cra": "Criminal Appeal",
    "cv": "Civil",
    "cr": "Criminal",
    "hca": "High Court Action",
    "fh": "Family",
    "pca": "Privy Council Appeal",
    "mag": "Magisterial Appeal",
}

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t ]+")
MULTINL_RE = re.compile(r"\n{3,}")
DATE_RE = re.compile(r"DD(\d{1,2})([A-Za-z]{3,4})(\d{2,4})", re.IGNORECASE)


def _clean_text(text: str) -> str:
    text = unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = WS_RE.sub(" ", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = MULTINL_RE.sub("\n\n", text)
    return text.strip()


def _strip_html(html: str) -> str:
    html = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    html = re.sub(r"(?i)</(p|div|h[1-6]|li|tr|br)\s*>", "\n", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = TAG_RE.sub(" ", html)
    return _clean_text(html)


def _strip_rtf(data: bytes) -> str:
    """Minimal, dependency-free RTF -> plain text."""
    try:
        text = data.decode("latin-1", errors="ignore")
    except Exception:
        text = data.decode("utf-8", errors="ignore")
    # Drop RTF header groups (fonts/colour tables etc.)
    text = re.sub(r"\\\*?\\[a-z]+-?\d* ?", " ", text)  # control words
    text = re.sub(r"[{}]", " ", text)
    text = re.sub(r"\\'[0-9a-fA-F]{2}", " ", text)     # hex escapes
    return _clean_text(text)


def _extract_pdf(data: bytes) -> str:
    import fitz  # PyMuPDF
    doc = fitz.open(stream=data, filetype="pdf")
    try:
        parts = []
        for page in doc:
            parts.append(page.get_text())
        return _clean_text("\n".join(parts))
    finally:
        doc.close()


class TTJudiciaryScraper(BaseScraper):
    """Scraper for TT/Judiciary via the Internet Archive Wayback Machine."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Legal Data Research)",
            "Accept": "*/*",
        })

    # ------------------------------------------------------------------ listing
    def _enumerate(self) -> List[Dict[str, str]]:
        """List every archived judgment document (dedup by URL, keep newest capture)."""
        params = {
            "url": f"{ORIGIN_HOST}/{JUDG_PREFIX}*",
            "output": "text",
            "fl": "original,timestamp,mimetype",
            "filter": "statuscode:200",
            "collapse": "urlkey",
        }
        for attempt in range(4):
            try:
                resp = self.session.get(CDX_URL, params=params, timeout=120)
                resp.raise_for_status()
                break
            except Exception as e:
                logger.warning(f"CDX attempt {attempt + 1} failed: {e}")
                time.sleep(5 * (attempt + 1))
        else:
            raise RuntimeError("Wayback CDX enumeration failed after retries")

        best: Dict[str, Dict[str, str]] = {}
        for line in resp.text.splitlines():
            parts = line.split(" ")
            if len(parts) < 2:
                continue
            original, ts = parts[0], parts[1]
            path = self._rel_path(original)
            if not path:
                continue
            low = path.lower()
            if low.startswith("photographs/") or "/photographs/" in low:
                continue
            if not low.endswith(TEXT_EXTS):
                continue
            key = low
            # Keep the most recent capture per document.
            if key not in best or ts > best[key]["ts"]:
                best[key] = {"original": original, "ts": ts, "path": path}
        docs = sorted(best.values(), key=lambda d: d["path"])
        logger.info(f"Enumerated {len(docs)} archived judgment documents")
        return docs

    @staticmethod
    def _rel_path(original: str) -> str:
        m = re.search(r"/LibraryJud/Judgments/(.+)$", original, re.IGNORECASE)
        return m.group(1) if m else ""

    # ------------------------------------------------------------------ download
    def _download(self, ts: str, original: str) -> Optional[bytes]:
        url = WB_RAW.format(ts=ts, url=original)
        for attempt in range(4):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=90)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                if resp.status_code == 429:
                    time.sleep(20 * (attempt + 1))
                    continue
                logger.warning(f"HTTP {resp.status_code} for {original}")
                return None
            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1} for {original}: {e}")
                time.sleep(5 * (attempt + 1))
        return None

    def _extract(self, path: str, data: bytes) -> str:
        low = path.lower()
        try:
            if low.endswith(".pdf"):
                return _extract_pdf(data)
            if low.endswith(".rtf"):
                return _strip_rtf(data)
            if low.endswith((".htm", ".html")):
                return _strip_html(data.decode("latin-1", errors="ignore"))
            return _clean_text(data.decode("latin-1", errors="ignore"))
        except Exception as e:
            logger.warning(f"Extraction failed for {path}: {e}")
            return ""

    # ------------------------------------------------------------- path parsing
    def _parse_meta(self, path: str) -> Dict[str, Any]:
        segs = [s for s in path.split("/") if s]
        court_key = segs[0].lower() if segs else ""
        court = COURT_NAMES.get(court_key, "High Court")
        filename = segs[-1] if segs else path
        stem = re.sub(r"\.[A-Za-z0-9]+$", "", filename)  # drop extension
        stem = re.sub(r"\(\d+\)$", "", stem).strip()      # drop "(2)" dupes

        # folder year (4-digit segment anywhere in the path)
        folder_year = None
        for s in segs[:-1]:
            if re.fullmatch(r"(19|20)\d{2}", s):
                folder_year = s
                break

        # judge folder: the non-year, non-court middle segment(s)
        judge = ""
        for s in segs[1:-1]:
            if not re.fullmatch(r"(19|20)\d{2}", s):
                judge = s
                break
        judge = re.sub(r"[_-]+", " ", judge).strip().title()

        # decision date from ...DD<dd><mon><yyyy>
        decision_date = None
        dm = DATE_RE.search(stem)
        case_ref = stem
        if dm:
            case_ref = stem[:dm.start()]
            dd, mon, yy = dm.groups()
            mon_n = MONTHS.get(mon.lower()[:3])
            if mon_n:
                year = int(yy)
                if year < 100:
                    year += 2000 if year <= 40 else 1900
                try:
                    decision_date = f"{year:04d}-{mon_n:02d}-{int(dd):02d}"
                except ValueError:
                    decision_date = None

        # human-readable case number from the reference token
        case_number = self._format_case_ref(case_ref)
        date = decision_date or (f"{folder_year}-01-01" if folder_year else None)
        return {
            "court": court,
            "judge": judge,
            "case_ref": case_ref.strip("_ "),
            "case_number": case_number,
            "date": date,
            "decision_date": decision_date,
        }

    @staticmethod
    def _format_case_ref(ref: str) -> str:
        ref = ref.strip("_ ")
        pm = re.match(r"([A-Za-z]+)[_-]?(.+)$", ref)
        if not pm:
            return re.sub(r"[_-]+", " ", ref).strip()
        prefix, rest = pm.groups()
        name = CASE_TYPE_NAMES.get(prefix.lower(), prefix.upper())
        # Tokens: an optional letter sub-prefix (P/S) may sit before the number.
        # Keep the full digit run (do NOT strip leading zeros here) so that a
        # zero-padded 2-digit year like "08" is still recognised as a year.
        tokens = re.findall(r"([A-Za-z]?)(\d+)", rest)
        if not tokens:
            return re.sub(r"[_-]+", " ", ref).strip()

        # The 2-digit (or 19xx/20xx) token is the year; the other is the number,
        # since filenames appear both year-first (cv_14_02731) and number-first
        # (HC_1301_05).
        year = None
        num_disp = None
        for sub, digits in tokens:
            if year is None and (len(digits) == 2 or re.fullmatch(r"(19|20)\d{2}", digits)):
                y = int(digits)
                year = y + (2000 if y <= 40 else 1900) if y < 100 else y
            elif num_disp is None:
                num_disp = f"{sub.upper()}{int(digits)}" if sub else str(int(digits))
        if num_disp is None:  # only one token, or both looked like years
            sub, digits = tokens[-1]
            num_disp = f"{sub.upper()}{int(digits)}" if sub else str(int(digits))
        if year is None:
            return f"{name} No. {num_disp}"
        return f"{name} No. {num_disp} of {year}"

    @staticmethod
    def _extract_parties(text: str) -> str:
        """Best-effort 'A v B' title from the 'BETWEEN ... AND ...' block."""
        m = re.search(
            r"BETWEEN\s+(.{3,160}?)\s+AND\s+(.{3,160}?)\s+(?:BEFORE|PANEL|CORAM|"
            r"APPEARANCES|JUDGMENT|REASONS|Delivered|Before)",
            text, re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return ""
        def tidy(s: str) -> str:
            s = re.sub(r"\s+", " ", s)
            s = re.sub(r"(?i)\b(Appellant|Respondent|Claimant|Defendant|Applicant|"
                       r"Plaintiff|Petitioner)s?\b", "", s)
            return s.strip(" .,-")
        a, b = tidy(m.group(1)), tidy(m.group(2))
        if 2 < len(a) < 120 and 2 < len(b) < 120:
            return f"{a} v {b}"
        return ""

    # --------------------------------------------------------------- normalize
    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = raw.get("_text") or ""
        if not text or len(text) < MIN_TEXT_CHARS:
            return None
        path = raw["path"]
        meta = self._parse_meta(path)
        now = datetime.now(timezone.utc).isoformat()

        parties = self._extract_parties(text)
        base = parties or meta["case_number"]
        title = f"{base} ({meta['court']}, Trinidad and Tobago)"

        stem = re.sub(r"\.[A-Za-z0-9]+$", "", path)
        _id = f"TT/Judiciary/{stem}"

        return {
            "_id": _id,
            "_source": "TT/Judiciary",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": meta["date"],
            "url": f"https://{ORIGIN_HOST}/{JUDG_PREFIX}/{path}",
            "court": meta["court"],
            "judge": meta["judge"],
            "case_number": meta["case_number"],
            "jurisdiction": "Trinidad and Tobago",
            "language": "en",
            "archived_via": WB_RAW.format(ts=raw["ts"], url=raw["original"]),
        }

    # ----------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        try:
            docs = self._enumerate()
        except Exception as e:
            logger.error(f"Enumeration failed: {e}")
            return
        total = len(docs)
        ok = 0
        for i, doc in enumerate(docs, 1):
            data = self._download(doc["ts"], doc["original"])
            if not data:
                continue
            text = self._extract(doc["path"], data)
            if not text or len(text) < MIN_TEXT_CHARS:
                logger.debug(f"Skip (short/scanned): {doc['path']} ({len(text)} chars)")
                continue
            doc["_text"] = text
            yield doc
            ok += 1
            if i % 25 == 0:
                logger.info(f"  Processed {i}/{total} (kept {ok})")
        logger.info(f"Fetched {ok}/{total} judgment documents with full text")

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        # Archive-backed corpus; re-list and let upsert dedup.
        yield from self.fetch_all()

    def test_connection(self):
        docs = self._enumerate()
        print(f"OK — enumerated {len(docs)} archived judgment documents")
        if docs:
            d = docs[len(docs) // 2]
            data = self._download(d["ts"], d["original"])
            text = self._extract(d["path"], data or b"")
            print(f"Sample {d['path']}: {len(text)} chars")
            print(self._parse_meta(d["path"]))


if __name__ == "__main__":
    scraper = TTJudiciaryScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "bootstrap-fast":
        scraper.bootstrap_fast()
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
