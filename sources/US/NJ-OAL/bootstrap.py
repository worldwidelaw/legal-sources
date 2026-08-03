#!/usr/bin/env python3
"""
US/NJ-OAL -- New Jersey Office of Administrative Law
(Administrative Law Judge Initial & Final Decisions)

Fetches the full text of published Administrative Law Judge (ALJ)
decisions of the New Jersey Office of Administrative Law (OAL). OAL is
New Jersey's central, independent tribunal: an ALJ hears a contested
case between a person and a State agency and issues an Initial Decision
(and, where the agency head adopts/modifies it, a Final Decision) that
resolves that specific case = case_law. The decisions are official New
Jersey state-government works in the public domain (government edicts,
17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The searchable OAL decision archive is hosted by Rutgers Law at
      https://njlaw.rutgers.edu/OAL/
  Its full-text search backend honours an undocumented `limit` param
  that removes the default result cap:
      GET https://njlaw.rutgers.edu/OAL/search.php?q={TERM}&limit=5000
  The HTML response server-renders one <a> per matching decision whose
  href is a deterministic born-digital PDF:
      /OAL/NJLAW-pdfs/initial/{docket}.pdf   (ALJ initial decision)
      /OAL/NJLAW-pdfs/final/{docket}.pdf     (adopted final decision)
  where {docket} is the OAL docket, e.g. "edu00255-21_1", "hma03894-16_1"
  or the unsuffixed "edu13804-24". The first 2-4 letters encode the
  transmitting agency (EDU = Education, HMA = Medical Assistance/Medicaid,
  CSV = Civil Service, ...). `q` matches decision body text and docket
  codes, so querying every agency code plus a handful of broad legal
  terms and unioning the hrefs enumerates the whole corpus (10k+ docs,
  Oct 1997-present).

Strategy:
  1. For each search term (agency codes + broad terms), GET the search
     endpoint with limit=5000 and harvest the NJLAW-pdfs/*/*.pdf hrefs.
  2. Union + dedup on (subdir, filename).
  3. Download each PDF (curl, browser UA, ~1 req/s), extract text via
     common.pdf_extract, and normalize into the case_law schema.

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
logger = logging.getLogger("legal-data-hunter.US.NJ-OAL")

BASE_URL = "https://njlaw.rutgers.edu"
SEARCH_TEMPLATE = BASE_URL + "/OAL/search.php?q={q}&limit=5000"
DOC_TEMPLATE = BASE_URL + "/OAL/NJLAW-pdfs/{sub}/{name}"

# Search href: /OAL/NJLAW-pdfs/{initial|final}/{docket}.pdf
DOC_HREF_RE = re.compile(
    r'/OAL/NJLAW-pdfs/(initial|final)/([A-Za-z0-9_.-]+\.pdf)', re.I
)
# docket stem -> agency code + number-year (+ optional _seq)
DOCKET_RE = re.compile(r'^([a-z]{2,4})(\d+)-(\d{2})(?:_(\d+))?$', re.I)
DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
DECIDED_RE = re.compile(
    r"Decided?\s*:?\s*"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

# Leading OAL transmittal codes -> transmitting State agency / subject.
# Fallback is the uppercased code for anything not listed here.
AGENCY_NAMES = {
    "EDU": "Department of Education",
    "EDS": "Department of Education (Special Education)",
    "ECE": "Department of Education (Special Education)",
    "EDE": "Department of Education (Special Education)",
    "ESA": "Department of Education (Special Education)",
    "ESW": "Department of Education (Special Education)",
    "ESR": "Department of Education (Special Education)",
    "ESF": "Department of Education (Special Education)",
    "EEQ": "Department of Education (Special Education)",
    "EEC": "Department of Education (Special Education)",
    "EER": "Department of Education (Special Education)",
    "HMA": "Department of Human Services — Medical Assistance (Medicaid)",
    "HPW": "Department of Human Services",
    "HEA": "Department of Health",
    "HLT": "Department of Health",
    "HCB": "Department of Human Services",
    "CSV": "Civil Service Commission",
    "CSR": "Civil Service Commission",
    "CAF": "Department of Children and Families",
    "PUC": "Board of Public Utilities",
    "BKI": "Department of Banking and Insurance",
    "LID": "Department of Banking and Insurance",
    "ELU": "Department of Labor and Workforce Development",
    "EWR": "Department of Labor and Workforce Development",
    "EHW": "Department of Labor and Workforce Development",
    "MVH": "Motor Vehicle Commission",
    "POL": "Department of Law and Public Safety",
    "CMA": "Department of Community Affairs",
    "ENV": "Department of Environmental Protection",
    "TAX": "Division of Taxation",
    "ABC": "Division of Alcoholic Beverage Control",
    "AGR": "Department of Agriculture",
    "CRT": "Casino Control / Racing",
    "RAC": "Racing Commission",
}

# Search terms that enumerate the corpus: agency transmittal codes seen in
# the archive plus broad legal terms as a backstop for anything missed.
SEARCH_TERMS = [
    # agency / subject codes (each returns that agency's decisions)
    "edu", "eds", "ece", "ede", "esa", "esw", "esr", "esf", "eeq", "eec",
    "eer", "elu", "ewr", "ehw", "hma", "hpw", "hea", "hlt", "hcb", "csv",
    "csr", "caf", "puc", "bki", "lid", "mvh", "pol", "cma", "env", "tax",
    "abc", "agr", "typ", "das", "adc", "mua", "fgc", "bds", "trp", "epc",
    "enh", "hbe", "efg", "crt", "rac",
    # broad legal terms (backstop)
    "medicaid", "education", "license", "insurance", "civil", "labor",
    "environmental", "health", "children", "petitioner", "respondent",
    "appeal", "board", "commission", "penalty", "benefits",
]


class NJOALScraper(BaseScraper):

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
    def _stem(name: str) -> str:
        return re.sub(r"\.pdf$", "", name, flags=re.I)

    @classmethod
    def _agency_code(cls, stem: str) -> str | None:
        m = DOCKET_RE.match(stem)
        return m.group(1).upper() if m else None

    @classmethod
    def _case_number(cls, stem: str) -> str | None:
        m = DOCKET_RE.match(stem)
        if not m:
            return None
        agency, num, yy = m.group(1).upper(), m.group(2), m.group(3)
        return f"{agency} {num}-{yy}"

    @classmethod
    def _agency_name(cls, stem: str) -> str | None:
        code = cls._agency_code(stem)
        if not code:
            return None
        return AGENCY_NAMES.get(code, code)

    @classmethod
    def _year_from_stem(cls, stem: str) -> str | None:
        m = DOCKET_RE.match(stem)
        if not m:
            return None
        yy = int(m.group(3))
        year = 1900 + yy if yy >= 90 else 2000 + yy
        return str(year)

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> list[dict]:
        terms = ["edu"] if sample else SEARCH_TERMS
        seen: set[tuple[str, str]] = set()
        out: list[dict] = []
        for term in terms:
            html = self._curl_text(SEARCH_TEMPLATE.format(q=term))
            if not html:
                logger.warning(f"search q={term}: no response")
                continue
            new_on_term = 0
            for sub, name in DOC_HREF_RE.findall(html):
                sub = sub.lower()
                key = (sub, name.lower())
                if key in seen:
                    continue
                seen.add(key)
                stem = self._stem(name)
                out.append({
                    "sub": sub,
                    "name": name,
                    "stem": stem,
                    "safe_slug": f"{sub}-{re.sub(r'[^A-Za-z0-9._-]+', '-', stem)}"[:90],
                    "doc_url": DOC_TEMPLATE.format(sub=sub, name=name),
                    "case_number": self._case_number(stem),
                    "agency": self._agency_name(stem),
                    "kind": "initial" if sub == "initial" else "final",
                    "stem_year": self._year_from_stem(stem),
                })
                new_on_term += 1
            logger.info(f"  q={term}: +{new_on_term} new (total {len(out)})")
            if sample and len(out) >= 16:
                break
        # newest dockets first (year segment sorts reasonably by stem)
        out.sort(key=lambda r: r["stem"], reverse=True)
        logger.info(f"Discovered {len(out)} NJ OAL decision documents")
        return out

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/NJ-OAL", doc["safe_slug"], pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._doc_date(text, doc.get("stem_year"))
        return doc

    @staticmethod
    def _doc_date(text: str, year: str | None) -> str | None:
        # Prefer an explicit "Decided: <date>"; else first plausible date.
        head = text[:8000]
        m = DECIDED_RE.search(head) or DATE_RE.search(head)
        if m:
            mo = MONTHS.get(m.group(1).lower())
            d = int(m.group(2))
            y = int(m.group(3))
            if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        if year and re.fullmatch(r"\d{4}", year.strip()):
            return f"{year.strip()}-01-01"
        return None

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NJ OAL search discovery + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number') or raw.get('stem')}")
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
        kind = raw.get("kind", "initial")
        label = "Initial Decision" if kind == "initial" else "Final Decision"
        if cn:
            title = f"NJ OAL {label} — OAL DKT. NO. {cn}"
        else:
            title = f"NJ OAL {label} — {raw.get('stem')}"
        title = title[:300]
        return {
            "_id": f"US/NJ-OAL/{raw['safe_slug']}",
            "_source": "US/NJ-OAL",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["safe_slug"],
            "case_number": cn,
            "agency": raw.get("agency"),
            "decision_kind": kind,
            "issuer": "New Jersey Office of Administrative Law",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NJ",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_text: set[str] = set()
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if not raw:
                continue
            # Some dockets are re-uploaded under _1/_2/_3 filename variants
            # that carry byte-identical bodies; collapse those to one record.
            import hashlib
            h = hashlib.sha1(raw["text"].encode("utf-8", "replace")).hexdigest()
            if h in seen_text:
                logger.info(f"  skip duplicate body: {doc['doc_url']}")
                continue
            seen_text.add(h)
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

    parser = argparse.ArgumentParser(description="US/NJ-OAL bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJOALScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
