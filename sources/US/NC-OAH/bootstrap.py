#!/usr/bin/env python3
"""
US/NC-OAH -- North Carolina Office of Administrative Hearings
(ALJ Contested Case Decisions — archived, pre-2017)

Fetches the full text of every published Administrative Law Judge (ALJ)
contested-case decision in the North Carolina Office of Administrative
Hearings (OAH) online archive. OAH is North Carolina's central,
independent quasi-judicial tribunal: an ALJ hears a contested case
between a person and a State agency and issues a Final Decision that
resolves that specific case = case_law. Decisions are official North
Carolina state-government works in the public domain (government
edicts, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The archive index at
      https://www.oah.nc.gov/administrative-law-judge-decisions
  is a Drupal page that links out to per-agency, per-year listing pages
  named  /{agency-slug}-decisions-{YEAR}  (e.g.
  /abc-alcoholic-beverage-control-commission-decisions-2012). Each
  listing page server-renders links to the decision documents, which
  are served as born-digital text-layer PDFs at deterministic URLs:

      https://www.oah.nc.gov/documents/files/alj/{doc-slug}/download

  where {doc-slug} is usually the OAH case number (e.g. "16-abc-03310")
  and occasionally a combined per-agency-year bundle
  (e.g. "bme-board-medical-examiners-decisions-2004"). A literal
  "blank" slug marks a placeholder with no document and is skipped.

Strategy:
  1. GET the archive index → collect the /{...}-decisions-{YEAR} pages.
  2. GET each listing page → collect the alj/{doc-slug}/download links
     (deduped, "blank" excluded).
  3. Download each PDF (curl, browser UA, ~1 req/s), extract its text
     via common.pdf_extract, and normalize into the case_law schema.

Coverage note:
  This covers the OAH public *archive* (chiefly ABC contested cases,
  2000-2016, ~160 decisions). Decisions from 2017-present live only in
  the ASP.NET case-management search portal
  (https://www.encoah.oah.state.nc.us/publicsite/search.aspx), a
  ViewState/AJAX form not reachable by a plain GET; harvesting it is a
  documented follow-up (see README / manifest notes).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import io
import sys
import json
import logging
import re
import struct
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NC-OAH")

BASE_URL = "https://www.oah.nc.gov"
INDEX_PATH = "/administrative-law-judge-decisions"
DOC_TEMPLATE = "https://www.oah.nc.gov/documents/files/alj/{slug}/download"

CAT_PAGE_RE = re.compile(r'href="(/[a-z0-9-]+-decisions-(20\d\d))"', re.I)
DOC_SLUG_RE = re.compile(r'/documents/files/alj/([^/"]+)/(?:download|open)', re.I)
# OAH case number embedded in a doc slug: YY-AGENCY-NNNNN
CASE_NO_RE = re.compile(r"^(\d{2})-([a-z]{2,5})-(\d+)$", re.I)
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
# leading agency abbreviation in a case number -> readable agency name
AGENCY_NAMES = {
    "ABC": "Alcoholic Beverage Control Commission",
    "BME": "Board of Medical Examiners",
    "BDA": "Board of Dental Examiners",
    "BCA": "Board of Cosmetic Arts Examiners",
    "BBE": "Board of Barber Examiners",
    "BLA": "Board of Landscape Architects",
    "BAR": "Board of Law Examiners",
}


class NCOAHScraper(BaseScraper):

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
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_text(self, url: str) -> str | None:
        b = self._curl_bytes(url)
        return b.decode("utf-8", "replace") if b else None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _slugify(slug: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "-", slug).strip("-")[:80]

    @staticmethod
    def _case_number(slug: str) -> str | None:
        m = CASE_NO_RE.match(slug)
        if not m:
            return None
        return f"{m.group(1)}-{m.group(2).upper()}-{m.group(3)}"

    @staticmethod
    def _agency_from_slug(slug: str) -> str | None:
        m = CASE_NO_RE.match(slug)
        if m:
            return AGENCY_NAMES.get(m.group(2).upper(), m.group(2).upper())
        return None

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> list[dict]:
        idx = self._curl_text(BASE_URL + INDEX_PATH)
        if not idx:
            logger.error("Could not fetch archive index")
            return []
        cats = []
        seen_cat = set()
        for path, year in CAT_PAGE_RE.findall(idx):
            if path not in seen_cat:
                seen_cat.add(path)
                cats.append((path, year))
        logger.info(f"Archive index: {len(cats)} agency-year listing pages")

        out: list[dict] = []
        seen_doc: set[str] = set()
        for path, year in cats:
            html = self._curl_text(BASE_URL + path)
            if not html:
                logger.warning(f"Failed to fetch listing page {path}")
                continue
            new_on_page = 0
            for slug in DOC_SLUG_RE.findall(html):
                if slug.lower() == "blank" or slug in seen_doc:
                    continue
                seen_doc.add(slug)
                out.append({
                    "slug": slug,
                    "safe_slug": self._slugify(slug),
                    "doc_url": DOC_TEMPLATE.format(slug=slug),
                    "case_number": self._case_number(slug),
                    "agency": self._agency_from_slug(slug),
                    "listing_year": year,
                    "listing_page": BASE_URL + path,
                })
                new_on_page += 1
            logger.info(f"  {path}: {new_on_page} documents (total {len(out)})")
            if sample and len(out) >= 16:
                break
        # newest cases first
        out.sort(key=lambda r: r["slug"], reverse=True)
        logger.info(f"Discovered {len(out)} NC OAH decision documents")
        return out

    # -------------------------------------------------- legacy .doc (OLE)
    @staticmethod
    def _doc_to_text(data: bytes) -> str | None:
        """Extract text from a legacy Word 97-2003 (.doc / OLE) file using
        the piece table in the WordDocument + table stream. Pure-python
        (olefile) so it runs anywhere the package is installed."""
        try:
            import olefile
        except Exception:
            logger.warning("olefile not available — cannot extract .doc")
            return None
        try:
            ole = olefile.OleFileIO(io.BytesIO(data))
            if not ole.exists("WordDocument"):
                return None
            wds = ole.openstream("WordDocument").read()
            flags = struct.unpack_from("<H", wds, 0x0A)[0]
            which = "1Table" if (flags & 0x0200) else "0Table"
            if not ole.exists(which):
                which = "0Table" if which == "1Table" else "1Table"
            if not ole.exists(which):
                return None
            tbl = ole.openstream(which).read()
            fcClx = struct.unpack_from("<I", wds, 0x01A2)[0]
            lcbClx = struct.unpack_from("<I", wds, 0x01A6)[0]
            clx = tbl[fcClx:fcClx + lcbClx]
            i, pcdt = 0, None
            while i < len(clx):
                if clx[i] == 0x01:  # Prc — skip
                    ln = struct.unpack_from("<H", clx, i + 1)[0]
                    i += 3 + ln
                elif clx[i] == 0x02:  # Pcdt
                    lcb = struct.unpack_from("<I", clx, i + 1)[0]
                    pcdt = clx[i + 5:i + 5 + lcb]
                    break
                else:
                    break
            if not pcdt:
                return None
            n = (len(pcdt) - 4) // 12
            cps = [struct.unpack_from("<I", pcdt, k * 4)[0] for k in range(n + 1)]
            parts = []
            for k in range(n):
                pcd_off = (n + 1) * 4 + k * 8
                fc = struct.unpack_from("<I", pcdt, pcd_off + 2)[0]
                count = cps[k + 1] - cps[k]
                if count <= 0:
                    continue
                if fc & 0x40000000:  # ANSI (cp1252)
                    fc = (fc & ~0x40000000) >> 1
                    parts.append(wds[fc:fc + count].decode("cp1252", "replace"))
                else:                # Unicode (utf-16-le)
                    parts.append(wds[fc:fc + count * 2].decode("utf-16-le", "replace"))
            text = "".join(parts)
            text = (text.replace("\r", "\n").replace("\x07", "\t")
                        .replace("\x0b", "\n").replace("\x0c", "\n"))
            text = re.sub(r"[\x00-\x08\x0e-\x1f]", "", text)
            text = re.sub(r"[ \t]+\n", "\n", text)
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()
        except Exception as e:
            logger.warning(f"olefile .doc extraction failed: {e}")
            return None

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        magic = blob[:4]
        if magic == b"%PDF":
            text = pdf_extract.extract_pdf_markdown(
                "US/NC-OAH", doc["safe_slug"], pdf_bytes=blob,
                table="case_law", force=True,
            )
            fmt = "pdf"
        elif magic == b"\xd0\xcf\x11\xe0":  # OLE / legacy Word .doc
            text = self._doc_to_text(blob)
            fmt = "doc"
        else:
            logger.warning(f"Unsupported document type ({magic!r}): {doc['doc_url']}")
            return None
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["format"] = fmt
        doc["date"] = self._doc_date(text, doc.get("listing_year"))
        doc["parties"] = self._parties(text)
        return doc

    @staticmethod
    def _doc_date(text: str, year: str | None) -> str | None:
        for m in DATE_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d = int(m.group(2))
            y = int(m.group(3))
            if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        if year and re.fullmatch(r"\d{4}", year.strip()):
            return f"{year.strip()}-01-01"
        return None

    _NAME = r"[A-Za-z][A-Za-z0-9 .,&'/\-]{2,80}?"
    _BOILER = ("OFFICE OF", "ADMINISTRATIVE HEARINGS", "STATE OF NORTH",
               "COUNTY OF", "DECISION", "PETITION")

    @classmethod
    def _parties(cls, text: str) -> str | None:
        # OAH captions read "<Petitioner>, ) Petitioner, ) v. <Respondent>, ) Respondent"
        # laid out as a table; flatten ")" and tabs so the name sits next to its label.
        t = re.sub(r"[)\t]+", " ", text)
        t = re.sub(r"[ \t]+", " ", t)
        pm = re.search(rf"({cls._NAME}),?\s*Petitioner", t)
        rm = re.search(rf"v\.?\s+({cls._NAME}),?\s*Respondent", t)
        if not pm or not rm:
            return None
        pet = re.sub(r"\s+", " ", pm.group(1)).strip(" ,.")
        res = re.sub(r"\s+", " ", rm.group(1)).strip(" ,.")
        if not pet or not res:
            return None
        if any(b in pet.upper() or b in res.upper() for b in cls._BOILER):
            return None  # caption boilerplate leaked in — drop rather than pollute
        return f"{pet} v. {res}"[:250]

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NC OAH archive discovery + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number') or raw.get('slug')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cn = raw.get("case_number")
        agency = raw.get("agency")
        parties = raw.get("parties")
        if cn:
            title = f"NC OAH {cn}"
        else:
            title = f"NC OAH ALJ Decisions {raw.get('listing_year') or ''}".strip()
        if parties:
            title = f"{title}: {parties}"
        title = title[:300]
        return {
            "_id": f"US/NC-OAH/{raw['safe_slug']}",
            "_source": "US/NC-OAH",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["safe_slug"],
            "case_number": cn,
            "agency": agency,
            "parties": parties,
            "issuer": "North Carolina Office of Administrative Hearings",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NC",
        }

    # ------------------------------------------------------------- fetch
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
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NC-OAH bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NCOAHScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
