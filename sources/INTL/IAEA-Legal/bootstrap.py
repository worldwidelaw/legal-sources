#!/usr/bin/env python3
"""
INTL/IAEA-Legal -- IAEA Information Circulars (INFCIRC)

Fetches the full text of the IAEA Information Circulars (INFCIRC series) — the
International Atomic Energy Agency's public "INFORMATION CIRCULAR" documents that
carry the authoritative texts of nuclear-law instruments: safeguards agreements
(e.g. INFCIRC/153, INFCIRC/540 — the Model Additional Protocol), the texts of
international conventions (physical protection INFCIRC/274, early notification
INFCIRC/335, assistance INFCIRC/336, nuclear safety, joint convention), export
guidelines (INFCIRC/254 — the NSG Guidelines), Agency statutes/relationship
agreements and member-state notifications. These are official IGO government-edict
works, distributed publicly ("Distr. GENERAL").

Why this route (Internet Archive):
  The live host www.iaea.org sits behind Cloudflare and returns HTTP 403 to
  datacenter / foreign vantages for both the listing and the
  /sites/default/files/publications/documents/infcircs/{year}/infcirc{n}.pdf PDFs
  (issue #1093). The Internet Archive has preserved ~540 English INFCIRC PDFs at
  that exact path; we enumerate them via the Wayback CDX API and fetch the raw
  bytes through the `/web/{timestamp}id_/{url}` replay endpoint, which bypasses
  the Cloudflare block. The canonical iaea.org URL is retained as each record's
  `url`. Newer circulars are born-digital (clean text layer); a minority of the
  oldest (1959-era) scans have no text layer and are skipped (logged), not OCR'd.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap-fast       # Alias for bootstrap (VPS pipeline)
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IAEA-Legal")

SOURCE_ID = "INTL/IAEA-Legal"
CANONICAL_HOST = "https://www.iaea.org"
INFCIRC_PATH_PREFIX = "iaea.org/sites/default/files/publications/documents/infcircs"
CDX_URL = "http://web.archive.org/cdx/search/cdx"
UA = "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +research)"
MIN_TEXT_CHARS = 400  # below this we treat the PDF as a non-text scan and skip

# Language suffixes we exclude to keep the English primary documents.
_LANG_SUFFIX = re.compile(
    r"_(fr|rus?|sp|es|ar|ch|zh|cn|de|jp|ja|it|pt|"
    r"arabic|french|spanish|russian|chinese|german|japanese)\.pdf$",
    re.IGNORECASE,
)
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


class IAEALegalScraper(BaseScraper):
    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.data_dir = Path(__file__).resolve().parent / "data"
        self.data_dir.mkdir(exist_ok=True)
        self.checkpoint_path = self.data_dir / "processed.json"
        self._processed = self._load_checkpoint()

    # ── checkpoint / resume ─────────────────────────────────────────────
    def _load_checkpoint(self) -> set:
        try:
            return set(json.loads(self.checkpoint_path.read_text()))
        except Exception:
            return set()

    def _save_checkpoint(self) -> None:
        try:
            self.checkpoint_path.write_text(json.dumps(sorted(self._processed)))
        except Exception as e:
            logger.debug(f"checkpoint write failed: {e}")

    # ── discovery via Wayback CDX ───────────────────────────────────────
    def _discover(self) -> List[Dict[str, Any]]:
        """Return list of {ts, orig, filename, year} for English INFCIRC PDFs,
        newest year first (born-digital documents come first)."""
        params = {
            "url": INFCIRC_PATH_PREFIX + "*",
            "output": "text",
            "fl": "timestamp,original,statuscode,mimetype",
            "collapse": "urlkey",
            "filter": "statuscode:200",
            "limit": "5000",
        }
        for attempt in range(5):
            try:
                r = self.session.get(CDX_URL, params=params, timeout=90)
                if r.status_code == 429:
                    time.sleep(30 * (attempt + 1))
                    continue
                r.raise_for_status()
                break
            except Exception as e:
                logger.warning(f"CDX attempt {attempt+1} failed: {e}")
                time.sleep(15 * (attempt + 1))
        else:
            raise RuntimeError("Wayback CDX enumeration failed after retries")

        docs: Dict[str, Dict[str, Any]] = {}
        for line in r.text.splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            ts, orig, status = parts[0], parts[1], parts[2]
            mime = parts[3] if len(parts) > 3 else ""
            if status != "200":
                continue
            fn = orig.rsplit("/", 1)[-1].lower()
            if not re.match(r"^infcirc[0-9]", fn):
                continue
            if not fn.endswith(".pdf"):
                continue
            if _LANG_SUFFIX.search(fn):
                continue
            if "pdf" not in mime and mime:
                continue
            m = re.search(r"/infcircs/(\d{4})/", orig)
            year = int(m.group(1)) if m else 0
            key = f"{year}/{fn}"
            # keep first (collapse already 1 per urlkey); dedup on year/filename
            if key not in docs:
                docs[key] = {"ts": ts, "orig": orig, "filename": fn, "year": year, "key": key}
        out = list(docs.values())
        # newest year first so born-digital docs and sampling come first
        out.sort(key=lambda d: (-d["year"], d["filename"]))
        logger.info(f"Discovered {len(out)} English INFCIRC PDFs via CDX")
        return out

    def _fetch_pdf_bytes(self, ts: str, orig: str) -> Optional[bytes]:
        url = f"https://web.archive.org/web/{ts}id_/{orig}"
        for attempt in range(4):
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code == 429:
                    time.sleep(20 * (attempt + 1))
                    continue
                if r.status_code != 200:
                    return None
                content = r.content
                if content[:4] != b"%PDF":
                    return None
                return content
            except Exception as e:
                logger.debug(f"fetch attempt {attempt+1} failed for {orig}: {e}")
                time.sleep(8 * (attempt + 1))
        return None

    @staticmethod
    def _extract_text(pdf_bytes: bytes) -> str:
        """Extract text with fitz (PyMuPDF), flushing per-page to bound memory."""
        try:
            import fitz  # PyMuPDF
        except Exception:
            fitz = None
        if fitz is not None:
            try:
                doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                parts = []
                for page in doc:
                    parts.append(page.get_text())
                doc.close()
                text = "\n".join(parts)
                if len(text.strip()) >= MIN_TEXT_CHARS:
                    return text
            except Exception as e:
                logger.debug(f"fitz extract failed: {e}")
        # fallback: pdfplumber
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                parts = []
                for page in pdf.pages:
                    parts.append(page.extract_text() or "")
                    page.flush_cache()
                return "\n".join(parts)
        except Exception as e:
            logger.debug(f"pdfplumber extract failed: {e}")
        return ""

    # ── BaseScraper contract ────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        docs = self._discover()
        for d in docs:
            if d["key"] in self._processed:
                continue
            pdf_bytes = self._fetch_pdf_bytes(d["ts"], d["orig"])
            self._processed.add(d["key"])
            if len(self._processed) % 20 == 0:
                self._save_checkpoint()
            if not pdf_bytes:
                logger.info(f"skip (no bytes): {d['key']}")
                continue
            text = self._extract_text(pdf_bytes)
            if len(text.strip()) < MIN_TEXT_CHARS:
                logger.info(f"skip (scanned/no-text): {d['key']}")
                continue
            yield {
                "filename": d["filename"],
                "year": d["year"],
                "orig_url": d["orig"].replace("http://", "https://"),
                "text": text,
            }
            time.sleep(2)  # be gentle with Internet Archive
        self._save_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # INFCIRC is an append-mostly historical series; a full re-scan with
        # checkpoint/resume is the update path (new circulars appear rarely).
        yield from self.fetch_all()

    # ── parsing helpers ─────────────────────────────────────────────────
    @staticmethod
    def _symbol_from(filename: str, text: str) -> str:
        # Prefer the symbol printed in the document body.
        m = re.search(r"INFCIRC/\s*(\d+)((?:/(?:Add|Rev|Mod|Corr)\.?\s*\d+)*)",
                      text, re.IGNORECASE)
        if m:
            base = f"INFCIRC/{m.group(1)}"
            tail = re.sub(r"\s+", "", m.group(2) or "")
            return base + tail
        # Fallback: derive from filename infcirc<N>[a<M>][r<R>].pdf
        stem = filename[:-4]  # drop .pdf
        mm = re.match(r"infcirc0*(\d+)(?:r(\d+))?(?:a(\d+))?", stem, re.IGNORECASE)
        if mm:
            sym = f"INFCIRC/{mm.group(1)}"
            if mm.group(2):
                sym += f"/Rev.{mm.group(2)}"
            if mm.group(3):
                sym += f"/Add.{mm.group(3)}"
            return sym
        return "INFCIRC/" + stem.replace("infcirc", "")

    @staticmethod
    def _date_from(text: str, year: int) -> Optional[str]:
        # e.g. "2 December 1963" or "December 1963"
        head = text[:3000]
        m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", head)
        if m and m.group(2).lower() in _MONTHS:
            d, mon, y = int(m.group(1)), _MONTHS[m.group(2).lower()], int(m.group(3))
            try:
                return datetime(y, mon, d).date().isoformat()
            except ValueError:
                pass
        m = re.search(r"([A-Za-z]+)\s+(\d{4})", head)
        if m and m.group(1).lower() in _MONTHS:
            mon, y = _MONTHS[m.group(1).lower()], int(m.group(2))
            return datetime(y, mon, 1).date().isoformat()
        if year and 1955 <= year <= 2100:
            return f"{year}-01-01"
        return None

    @staticmethod
    def _title_from(text: str, symbol: str) -> str:
        # The title sits between the masthead (which ends with the
        # "Original: ..." line, or the older "INFORMATION CIRCULAR" line) and
        # the first numbered paragraph / boilerplate footer.
        lines = [l.strip() for l in text.splitlines()]
        start = 0
        for i, l in enumerate(lines[:40]):
            if re.match(r"^original\s*:", l, re.IGNORECASE):
                start = i + 1
                break
            if "INFORMATION CIRCULAR" in l.upper():
                start = i + 1  # keep scanning for a later "Original:" line
        stop_re = re.compile(
            r"^(\d+\.\s|\d+\.$|atoms for peace|attachment\b|annex\b|the text of|"
            r"i\.\s|distr\.?|original\s*:|date\s*:|general distribution$)",
            re.IGNORECASE,
        )
        title_parts: List[str] = []
        for l in lines[start:start + 40]:
            if not l:
                if title_parts:
                    break
                continue
            if re.match(r"^INFCIRC", l, re.IGNORECASE):
                continue
            if stop_re.match(l):
                if title_parts:
                    break
                continue
            title_parts.append(l)
            if len(" ".join(title_parts)) > 200:
                break
        title = re.sub(r"\s+", " ", " ".join(title_parts)).strip(" .,")
        if len(title) < 5:
            title = symbol
        return title[:300]

    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("text") or "").strip()
        if len(text) < MIN_TEXT_CHARS:
            return None
        symbol = self._symbol_from(raw["filename"], text)
        title = self._title_from(text, symbol)
        date = self._date_from(text, raw.get("year", 0))
        doc_id = raw["filename"][:-4] if raw["filename"].endswith(".pdf") else raw["filename"]
        return {
            "_id": f"IAEA-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "infcirc_symbol": symbol,
            "title": title,
            "text": text,
            "date": date,
            "year": raw.get("year") or None,
            "url": raw["orig_url"],
        }


def _test_api() -> int:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    params = {
        "url": INFCIRC_PATH_PREFIX + "*",
        "output": "text", "fl": "original,statuscode",
        "collapse": "urlkey", "filter": "statuscode:200", "limit": "50",
    }
    r = s.get(CDX_URL, params=params, timeout=60)
    pdfs = [l for l in r.text.splitlines() if l.lower().endswith(".pdf 200")
            or (".pdf" in l.lower() and l.strip().endswith("200"))]
    print(f"CDX HTTP {r.status_code}; sample rows: {len(r.text.splitlines())}")
    for l in r.text.splitlines()[:5]:
        print("  ", l)
    return 0


def main():
    args = sys.argv[1:]
    cmd = args[0] if args else "bootstrap"
    config_path = Path(__file__).resolve().parent / "config.yaml"

    if cmd == "test-api":
        sys.exit(_test_api())

    scraper = IAEALegalScraper()

    if cmd in ("bootstrap", "bootstrap-fast", "update"):
        sample = "--sample" in args
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(stats, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
