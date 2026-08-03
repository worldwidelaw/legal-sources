#!/usr/bin/env python3
"""
US/IN-CADDNAR -- Indiana Natural Resources Commission decisions (CADDNAR)

Fetches the full text of every published administrative decision of the
Indiana Natural Resources Commission (NRC), collected in CADDNAR ("Cite As
Decisions of the Department of Natural Resources / Natural Resources
Commission"), the NRC's official reporter. An Administrative Law Judge of
the Office of Administrative Law Proceedings (OALP, formerly the Division
of Hearings) hears an administrative appeal of a Department of Natural
Resources action (floodway/construction-in-a-floodway permits under the
Flood Control Act, surface-coal-mining reclamation, lake-and-river
"public freshwater lake" disputes, oil & gas, fish & wildlife, boating,
state-park and cemetery matters, etc.) and issues Findings of Fact,
Conclusions of Law and a Final Order resolving that specific contested
case = case_law. These are official Indiana state-government works in the
public domain (government edicts).

Sibling to US/IN-OEA (Indiana Office of Environmental Adjudication / IDEM
final orders) — same publisher (OALP) but a distinct tribunal/corpus. This
source is the "FOLLOW-UP: sibling DNR/CADDNAR" flagged in the IN-OEA note.

Access (no CAPTCHA, no auth, reachable locally; in.gov generally serves
datacenter IPs):
  1. The CADDNAR Citation Index PDF enumerates every decision as a table
     row "CAUSE#  CAPTION  YEAR  ALJ  VOLUME  CITE  LAST-PG":
        https://www.in.gov/nrc/files/caddnar_index.pdf
     (~761 rows, Volumes 1-17, 1977-2024; born-digital text-layer PDF).
  2. Each decision's full text is a standalone born-digital HTML document
     (a Word-to-HTML export, NOT wrapped in the site template) at
        https://www.in.gov/oalp/files/decisions/dnr/{cause}.v{vol}.html
     where {cause} is the lower-cased cause number (e.g. 77-003w) and
     {vol} the CADDNAR volume number. Some files are served as .htm
     (Apache content negotiation returns HTTP 300 for the .html name);
     .htm variants are Windows-1252 encoded. The body opens
        CADDNAR [CITE: <caption>, <v> CADDNAR <pg> (<year>)]
        [VOLUME <v>, PAGE <pg>]  Cause #: <cause>  Caption: ...
        Administrative Law Judge: ...  Date: <Month D, YYYY>  ... ORDER ...

Strategy:
  1. Download + extract the Citation Index PDF; parse every row into
     {cause, caption, year, alj, volume}. Dedup by (cause, volume).
  2. For each: fetch {cause}.v{vol}.html, falling back to .htm on a
     non-200 (content-negotiation 300); decode utf-8 -> cp1252.
  3. Strip HTML, slice from the "CADDNAR" citation marker (drops the
     Word-export preamble), parse the citation + decision date from the
     body, and normalize into the case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (~760 decisions)
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
import html as _html
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
logger = logging.getLogger("legal-data-hunter.US.IN-CADDNAR")

BASE_URL = "https://www.in.gov"
INDEX_PDF = BASE_URL + "/nrc/files/caddnar_index.pdf"
DECISION_TMPL = BASE_URL + "/oalp/files/decisions/dnr/{cause}.v{vol}{ext}"

# One index row: CAUSE# CAPTION YEAR ALJ VOLUME CITE LAST-PG
# e.g. "04-123W A. Lusher v. DNR 2007 Lucas 11 124 136"
ROW_RE = re.compile(
    r"(?P<cause>\d{2}-\d{3}[A-Za-z])\s+"
    r"(?P<caption>.+?)\s+"
    r"(?P<year>(?:19|20)\d{2})\s+"
    r"(?P<alj>[A-Z][A-Za-z'\-]+)\s+"
    r"(?P<vol>\d{1,2})\b"
)
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
CITE_RE = re.compile(r"\[CITE:\s*(.+?)\]", re.S)
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


class INCADDNARScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl(self, url: str) -> tuple[int, bytes]:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: text/html,application/pdf,*/*",
                     "-w", "\n%{http_code}", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    body, _, code = out.stdout.rpartition(b"\n")
                    try:
                        return int(code.decode().strip()), body
                    except ValueError:
                        return 0, out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return 0, b""

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _decode(blob: bytes) -> str:
        try:
            return blob.decode("utf-8")
        except UnicodeDecodeError:
            return blob.decode("cp1252", "replace")

    @classmethod
    def _clean_body(cls, raw_html: str) -> str | None:
        t = SCRIPT_RE.sub(" ", raw_html)
        t = TAG_RE.sub(" ", t)
        t = _html.unescape(t)
        # collapse the Word cp1252 non-breaking-space runs + whitespace
        t = t.replace("\xa0", " ").replace("�", " ")
        t = re.sub(r"[ \t]+", " ", t)
        t = re.sub(r"\s*\n\s*", "\n", t)
        t = re.sub(r"\n{2,}", "\n", t).strip()
        # the decision body starts at the "CADDNAR" citation marker; the
        # preamble before it is Word-export junk (mso styles, EN-US, ...)
        idx = t.find("CADDNAR")
        if idx > 0:
            t = t[idx:].strip()
        return t or None

    @staticmethod
    def _decision_date(text: str) -> str | None:
        m = DATE_RE.search(text)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d, y = int(m.group(2)), int(m.group(3))
        if mo and 1 <= d <= 31 and 1970 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> list[dict]:
        code, blob = self._curl(INDEX_PDF)
        if not blob or blob[:4] != b"%PDF":
            logger.error(f"Index PDF fetch failed (http {code}, {len(blob)} bytes)")
            return []
        text = pdf_extract.extract_pdf_markdown(
            "US/IN-CADDNAR", "caddnar_index", pdf_bytes=blob,
            table="case_law", force=True,
        ) or ""
        out: list[dict] = []
        seen: set[tuple] = set()
        for m in ROW_RE.finditer(text):
            cause = m.group("cause").upper()
            vol = m.group("vol")
            key = (cause, vol)
            if key in seen:
                continue
            seen.add(key)
            caption = re.sub(r"\s+", " ", m.group("caption")).strip(" .,")
            out.append({
                "cause": cause,
                "caption": caption,
                "year": m.group("year"),
                "alj": m.group("alj"),
                "volume": vol,
            })
            if sample and len(out) >= 16:
                break
        logger.info(f"Discovered {len(out)} CADDNAR decisions from the citation index")
        return out

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        cause_l = doc["cause"].lower()
        vol = doc["volume"]
        body = None
        used_url = None
        for ext in (".html", ".htm"):
            url = DECISION_TMPL.format(cause=cause_l, vol=vol, ext=ext)
            code, blob = self._curl(url)
            if code == 200 and blob:
                cleaned = self._clean_body(self._decode(blob))
                if cleaned and len(cleaned) >= 200:
                    body = cleaned
                    used_url = url
                    break
        if not body:
            logger.warning(f"No usable decision text for {doc['cause']} v{vol}")
            return None
        cite_m = CITE_RE.search(body)
        doc = dict(doc)
        doc["text"] = body
        doc["url"] = used_url
        doc["cite"] = re.sub(r"\s+", " ", cite_m.group(1)).strip() if cite_m else None
        doc["date"] = self._decision_date(body) or f"{doc['year']}-01-01"
        return doc

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing IN CADDNAR citation-index + decision fetch...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered from index")
                return False
            logger.info(f"  Discovered {len(docs)} rows (sample)")
            raw = None
            for d in docs:
                raw = self._build_raw(d)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  Decision fetch OK ({len(raw['text'])} chars) — "
                            f"{raw['cause']} v{raw['volume']} [{raw.get('date')}] "
                            f"{raw.get('caption')}")
            else:
                logger.error("  Decision fetch failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cause = raw["cause"]
        vol = raw["volume"]
        caption = raw.get("caption") or ""
        title = f"{caption} ({cause})" if caption else f"CADDNAR Cause {cause}"
        return {
            "_id": f"US/IN-CADDNAR/{cause}-v{vol}",
            "_source": "US/IN-CADDNAR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "cause_number": cause,
            "caption": caption or None,
            "citation": raw.get("cite"),
            "volume": vol,
            "administrative_law_judge": raw.get("alj"),
            "issuer": "Indiana Natural Resources Commission (CADDNAR)",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-IN",
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

    parser = argparse.ArgumentParser(description="US/IN-CADDNAR bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = INCADDNARScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
