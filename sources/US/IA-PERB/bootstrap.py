#!/usr/bin/env python3
"""
US/IA-PERB -- Iowa Public Employment Relations Board (PERB) Decisions

Fetches the full text of the decisions, orders, and neutral (fact-finding /
interest-arbitration) awards of the Iowa Public Employment Relations Board
(PERB), the independent state agency that administers Iowa's public-sector
collective-bargaining law (Iowa Code chapter 20). PERB and its administrative
law judges decide prohibited-practice complaints, bargaining-unit determination
and representation/certification cases, declaratory-order petitions, negotiability
disputes, and state-employee grievance appeals; the corpus also includes the
Iowa District Court and appellate decisions on judicial review of PERB orders.
Each numbered decision/order resolves a specific contested case = case_law, and
they are official Iowa state-government works in the public domain (government
edicts).

BUILD RECIPE (builds + validates LOCALLY via the Internet Archive):
The PERB website (iowaperb.iowa.gov) was retired -- the domain now 301-redirects
to eab.iowa.gov, and the live searchable decision database moved to
iowa-superb.iowa.gov, which is a Blazor Server (SignalR websocket) application
that cannot be enumerated without a full browser. HOWEVER, the entire born-digital
decision corpus -- ~3,300 decision/order/award PDFs -- was crawled and preserved
by the Internet Archive Wayback Machine under the stable Drupal file path
  https://iowaperb.iowa.gov/sites/default/files/{filename}.pdf
These are official Iowa government works (public domain); the Wayback Machine is a
durable public mirror. We enumerate them with the Wayback CDX API (filter
statuscode:200, mimetype application/pdf, collapse=urlkey) and download each
preserved PDF via the `/web/{timestamp}id_/{original_url}` raw-replay endpoint,
then extract full text with common.pdf_extract (born-digital text layer; the
shared helper falls back to tesseract OCR for the older scanned awards). The case
number is parsed from the filename and body, the decision date from the body. No
auth, no CAPTCHA, no JS challenge (the Wayback Machine serves the preserved bytes).

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
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote, unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IA-PERB")

# Original (now-retired) host whose /sites/default/files decision tree the
# Wayback Machine preserved in full.
ORIG_PREFIX = "iowaperb.iowa.gov/sites/default/files"
CDX_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_RAW = "https://web.archive.org/web/{ts}id_/{url}"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# Path: /sites/default/files/{filename}.pdf
PATH_RE = re.compile(r"/sites/default/files/(.+?)\.pdf$", re.IGNORECASE)

# Non-decision support material uploaded to the same Drupal file tree
# (blank forms, presentations, election-result spreadsheets, voter-list
# images, guides). Excluded by filename/path keyword.
JUNK_PATH_RE = re.compile(
    r"(?:^|/)images/"
    r"|annualreport|fillable|\bform\b|presentation|spreadsheet"
    r"|voter|instruction|agenda|minutes|newsletter|poster|brochure"
    r"|results[\s_-]*reporting|recert[\s_-]*election|template|checklist"
    r"|regional[\s_-]*meeting|reporting[\s_-]*guide|user[\s_-]*guide",
    re.IGNORECASE)

# A record is only kept if its body reads like an actual adjudicative
# decision/order/award: a PERB or court caption AND an adjudication word.
CAPTION_RE = re.compile(
    r"PUBLIC\s+EMPLOYMENT\s+RELATIONS\s+BOARD"
    r"|IOWA\s+DISTRICT\s+COURT"
    r"|COURT\s+OF\s+APPEALS\s+OF\s+IOWA"
    r"|SUPREME\s+COURT\s+OF\s+IOWA"
    r"|(?:INTEREST|GRIEVANCE)\s+ARBITRATION"
    r"|FACT[\s-]*FINDING"
    r"|ARBITRATION\s+AWARD", re.IGNORECASE)
ADJUDICATION_RE = re.compile(
    r"IN\s+THE\s+MATTER\s+OF|CASE\s+NOS?\.|\bPERB\s+\d{3,7}"
    r"|Appellant|Petitioner|Complainant|Respondent"
    r"|PROPOSED\s+DECISION|DECISION\s+AND\s+ORDER|DECLARATORY\s+ORDER"
    r"|ORDER\s+OF\s+CERTIFICATION|FINDINGS\s+OF\s+FACT"
    r"|ARBITRATION\s+AWARD|it\s+is\s+(?:hereby\s+)?ordered",
    re.IGNORECASE)

# Iowa PERB case numbers appear in the body as e.g. "PERB No. 8776",
# "PERB 100024", "CASE NO. 100079", "Case No. 8535". Judicial-review
# decisions carry Iowa district-court numbers like "CVCV056325".
PERB_CASE_RE = re.compile(
    r"\bPERB\s+(?:(?:Case\s+)?No\.?\s*)?([0-9]{3,7}[A-Z\-]*)", re.IGNORECASE)
CASE_NO_RE = re.compile(
    r"\bCase\s+Nos?\.?\s*[:\s]*([0-9]{3,7}[A-Z\-]*)", re.IGNORECASE)
CVCV_RE = re.compile(r"\b((?:[A-Z]{2,4})?CVCV[0-9]{4,7})\b", re.IGNORECASE)

LONGDATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{1,2},?\s+(?:19|20)\d{2}\b")
EFILE_RE = re.compile(r"Electronically\s+Filed\s+((?:19|20)\d{2}-\d{2}-\d{2})",
                      re.IGNORECASE)

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class PERBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.4
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_json(self, url: str, params: dict) -> list | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, params=params, timeout=(15, 180))
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"CDX GET failed attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 180), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _iso_from_longdate(s: str) -> str | None:
        m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", s.strip())
        if not m:
            return None
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            return None
        try:
            d, y = int(m.group(2)), int(m.group(3))
        except ValueError:
            return None
        if 1 <= d <= 31 and 1970 <= y <= 2100:
            return f"{y:04d}-{mon:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug_from_url(orig_url: str) -> tuple[str, str]:
        """Return (record_id, filename) from the /sites/default/files path."""
        path = re.sub(r"^https?://[^/]+", "", orig_url).split("?", 1)[0]
        m = PATH_RE.search(path)
        if m:
            fname = unquote(m.group(1))
        else:
            fname = unquote(path.rsplit("/", 1)[-1])
            fname = re.sub(r"(?i)\.pdf$", "", fname)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", fname).strip("-").lower()
        return slug, fname

    @staticmethod
    def _humanize(fname: str) -> str:
        s = re.sub(r"\.(pdf)$", "", fname, flags=re.IGNORECASE)
        s = re.sub(r"[._]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        rows = self._get_json(CDX_URL, {
            "url": ORIG_PREFIX + "*",
            "output": "json",
            "collapse": "urlkey",
            "filter": ["statuscode:200", "mimetype:application/pdf"],
            "fl": "original,timestamp,mimetype",
            "limit": "50000",
        })
        if not rows or len(rows) < 2:
            logger.error("CDX returned no decision snapshots")
            return
        header = rows[0]
        ts_i = header.index("timestamp")
        url_i = header.index("original")
        entries = []
        seen: set[str] = set()
        for r in rows[1:]:
            orig = r[url_i]
            pm = PATH_RE.search(orig.split("?", 1)[0])
            if not pm:
                continue
            if not orig.lower().split("?", 1)[0].endswith(".pdf"):
                continue
            # Drop obvious non-decision support material by path/filename.
            if JUNK_PATH_RE.search(unquote(pm.group(1))):
                continue
            rid, fname = self._slug_from_url(orig)
            if rid in seen:
                continue
            seen.add(rid)
            entries.append({"ts": r[ts_i], "orig": orig,
                            "record_id": rid, "fname": fname})
        # newest snapshots first so samples surface modern born-digital decisions
        entries.sort(key=lambda e: e["ts"], reverse=True)
        logger.info(f"CDX: {len(entries)} unique Iowa PERB decision PDFs")
        for e in entries:
            yield e

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        source_id = entry["record_id"]
        if source_id in self._existing:
            return None
        raw_url = WAYBACK_RAW.format(ts=entry["ts"],
                                     url=quote(entry["orig"], safe=":/?&=%"))
        pdf_bytes = self._get_bytes(raw_url)
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/IA-PERB", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {entry['fname']} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()

        # Content gate: keep only genuine adjudicative decisions/orders/awards
        # (a PERB or court caption AND an adjudication indicator), which drops
        # any non-decision material that slipped past the path filter.
        head = text[:3000]
        if not (CAPTION_RE.search(head) and ADJUDICATION_RE.search(head)):
            logger.info(f"Not a decision (no caption/adjudication marker): "
                        f"{entry['fname']} — skipping")
            return None

        # Case number: prefer explicit "PERB No." / "Case No." in body, else a
        # district-court CVCV number (judicial review), else a numeric filename.
        case_number = None
        for rx in (PERB_CASE_RE, CASE_NO_RE):
            m = rx.search(text[:4000])
            if m:
                case_number = m.group(1).upper().strip("-")
                break
        if not case_number:
            m = CVCV_RE.search(text[:4000])
            if m:
                case_number = m.group(1).upper()
        if not case_number:
            fm = re.match(r"([0-9]{3,7})", entry["fname"])
            if fm:
                case_number = fm.group(1)

        # Issuer: judicial-review rulings come from the Iowa courts.
        head = text[:600].upper()
        if "IOWA DISTRICT COURT" in head:
            issuer = "Iowa District Court (judicial review of PERB)"
        elif "SUPREME COURT" in head or "COURT OF APPEALS" in head:
            issuer = "Iowa appellate courts (judicial review of PERB)"
        else:
            issuer = "Iowa Public Employment Relations Board (PERB)"

        # Date: explicit e-file stamp, else the last long-date in the body
        # (issuance date typically at the end of Board orders).
        date = None
        em = EFILE_RE.search(text)
        if em:
            date = em.group(1)
        if not date:
            spans = [mm.group(0) for mm in LONGDATE_RE.finditer(text)]
            if spans:
                date = self._iso_from_longdate(spans[-1])
        if not date:
            spans = [mm.group(0) for mm in LONGDATE_RE.finditer(text[:1500])]
            if spans:
                date = self._iso_from_longdate(spans[0])

        # Title: a party caption if we can find one, else the humanized filename.
        party = None
        # The lead party normally sits just before its role word, right after
        # the "...RELATIONS BOARD" / "IN THE MATTER OF" header line.
        pm = re.search(
            r"(?:RELATIONS\s+BOARD|IN\s+THE\s+MATTER\s+OF)[\s:)]*"
            r"([A-Z][A-Za-z0-9 .,'&/()-]{3,90}?)\s*,?\s+"
            r"(?:Appellant|Petitioner|Complainant|Charging\s+Party|"
            r"Public\s+Employer|Employer|Union|Grievant)\b",
            text[:1600], re.IGNORECASE)
        if not pm:
            pm = re.search(
                r"(?:IN\s+THE\s+MATTER\s+OF|IN\s+RE)\s+(.+?)"
                r"(?:\n|,?\s+(?:Appellant|Petitioner|Complainant|Respondent|Case|PERB))",
                text[:900], re.IGNORECASE | re.DOTALL)
        if pm:
            party = re.sub(r"[\s)(]+", " ", pm.group(1)).strip(" .,-")[:140]
            if party and not re.search(r"[A-Za-z]", party):
                party = None
        bits = ["Iowa PERB"]
        if case_number:
            bits.append(f"Case No. {case_number}")
        bits.append(party or self._humanize(entry["fname"])[:140])
        title = " — ".join(b for b in bits if b)

        return {
            "record_id": source_id,
            "case_number": case_number,
            "issuer": issuer,
            "title": _html.unescape(title)[:500],
            "text": text,
            "date": date,
            "url": entry["orig"],
            "archive_url": WAYBACK_RAW.format(ts=entry["ts"], url=entry["orig"]),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Iowa PERB decisions (via Wayback CDX)...")
        try:
            gen = self.discover(sample=True)
            raw = None
            tried = 0
            for e in gen:
                raw = self._build_raw(e)
                tried += 1
                if raw:
                    break
                if tried >= 12:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} [{raw.get('date')}]")
                logger.info("API test PASSED")
                return True
            logger.error("  Text extraction failed")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/IA-PERB/{raw['record_id']}",
            "_source": "US/IA-PERB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "case_number": raw.get("case_number") or None,
            "issuer": raw.get("issuer") or "Iowa Public Employment Relations Board (PERB)",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "archive_url": raw.get("archive_url"),
            "date": raw.get("date") or None,
            "jurisdiction": "US-IA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/IA-PERB", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover(sample=sample):
            raw = self._build_raw(entry)
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

    parser = argparse.ArgumentParser(description="US/IA-PERB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PERBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
