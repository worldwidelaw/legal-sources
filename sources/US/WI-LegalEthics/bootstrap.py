#!/usr/bin/env python3
"""
US/WI-LegalEthics -- State Bar of Wisconsin — Professional Ethics Opinions

Fetches the full text of the ethics opinions issued by the State Bar of
Wisconsin's Standing Committee on Professional Ethics. Each opinion is the
Committee's written interpretation of the Wisconsin Supreme Court Rules of
Professional Conduct for Attorneys (SCR ch. 20) advising LAWYERS = doctrine
(advisory). The published corpus (~415 documents) spans several historical
series:
  - E-YY-NN     Formal opinions (e.g. E-00-01, E-74-1)
  - EF-YY-NN    Formal opinions, current numbering (e.g. EF-16-03)
  - EI-/IE-YY-NN Informal opinions (e.g. EI-11-01, IE-16-01)
  - EM-N-YY     Ethics memoranda
  - M-N-YY / Memo N-YY  Memorandum opinions
  - I-N-YY      older Informal opinions

Distinct from US/WI-EthicsOpinions (the executive Wisconsin Ethics Commission —
public officials / campaign finance) and US/WI-Courts / US/WI-Legislation. This
is the attorney professional-conduct opinion series that in other states we
build as US/{ST}-LegalEthics (lawyers).

Access (no JavaScript, no CAPTCHA, no auth):
  1. The opinions live in a public SharePoint document library on wisbar.org.
     The library folder is enumerated anonymously via the SharePoint REST API:
       /formembers/ethics/_api/web/GetFolderByServerRelativeUrl(
           '/formembers/ethics/Ethics Opinions')/Files?$top=3000
     which returns every opinion PDF's Name + ServerRelativeUrl.
  2. Each PDF is a born-digital (text-layer) document — extracted with PyMuPDF,
     NO OCR needed. The first line is typically
     "Wisconsin Formal Ethics Opinion {code}: {title}".

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

import requests

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WI-LegalEthics")

BASE = "https://www.wisbar.org"
FOLDER = "/formembers/ethics/Ethics Opinions"
LIST_URL = (
    BASE + "/formembers/ethics/_api/web/GetFolderByServerRelativeUrl('"
    + quote(FOLDER) + "')/Files?$top=3000"
    "&$select=Name,ServerRelativeUrl,TimeCreated,TimeLastModified"
)

# Opinion code somewhere in the filename, e.g. E-00-01, EF-16-03, EI-11-01,
# IE-16-01, EM-6-70, M-8-75, I-1-71 (optional trailing letter, e.g. M-8-75 C).
# Base code, plus an optional STANDALONE suffix letter (e.g. "M-8-75 C") — the
# letter is only captured when it is an isolated token (followed by space, dot
# or end) so it is not confused with the first letter of a descriptive title
# (e.g. "IE-16-01 Advertising").
CODE_RE = re.compile(
    r"\b((?:EF|EI|IE|EM|E|M|I)-\d{1,3}-\d{1,3})(?:\s+([A-Z])(?=\s|\.|$))?", re.I)
MEMO_RE = re.compile(
    r"\b(Memo)\s+(\d{1,3}-\d{1,3})(?:\s+([A-Z])(?=\s|\.|$))?", re.I)
# Year-first series vs year-last series (see module docstring).
YEAR_FIRST = ("E", "EF", "EI", "IE")


class WILegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "application/json;odata=nometadata,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str, json_accept: bool = False) -> requests.Response | None:
        headers = {}
        if json_accept:
            headers["Accept"] = "application/json;odata=nometadata"
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, headers=headers, timeout=90)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url[:90]} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _parse_code(name: str) -> tuple[str, str]:
        """Return (opinion_code, series_prefix) parsed from a filename.
        Falls back to a sanitised stem when no code pattern matches."""
        stem = re.sub(r"\.pdf$", "", name, flags=re.I)
        m = CODE_RE.search(stem)
        if m:
            code = m.group(1).upper()
            if m.group(2):
                code = f"{code}-{m.group(2).upper()}"
            prefix = re.match(r"[A-Z]+", code).group(0)
            return code, prefix
        m = MEMO_RE.search(stem)
        if m:
            code = f"MEMO-{m.group(2)}"
            if m.group(3):
                code = f"{code}-{m.group(3).upper()}"
            return code, "MEMO"
        # fallback: collapse the stem to a compact id
        clean = re.sub(r"\s+", "-", stem.strip())
        clean = re.sub(r"[^A-Za-z0-9-]", "", clean)
        return clean[:60] or name, "?"

    @staticmethod
    def _year_from_code(code: str, prefix: str) -> int | None:
        nums = re.findall(r"\d{1,3}", code)
        if not nums:
            return None
        if prefix in YEAR_FIRST:
            yy = int(nums[0])
        else:  # M / EM / I / MEMO series: year is the last component
            yy = int(nums[-1])
        if yy > 99:  # not a 2-digit year token
            return None
        return 2000 + yy if yy <= 25 else 1900 + yy

    def _list_opinions(self) -> list[dict]:
        """Return [{code, prefix, name, url}], de-duplicated on code."""
        r = self._get(LIST_URL, json_accept=True)
        if not r:
            return []
        try:
            files = r.json().get("value", [])
        except Exception as e:
            logger.error(f"REST list parse failed: {e}")
            return []
        out: dict[str, dict] = {}
        for f in files:
            name = f.get("Name", "")
            if not name.lower().endswith(".pdf"):
                continue
            srv = f.get("ServerRelativeUrl") or (FOLDER + "/" + name)
            code, prefix = self._parse_code(name)
            if code in out:
                continue
            out[code] = {
                "code": code,
                "prefix": prefix,
                "name": name,
                "url": BASE + quote(srv),
            }
        result = sorted(out.values(), key=lambda x: x["code"])
        logger.info(f"  discovered {len(result)} unique ethics opinions")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _pdf_text(content: bytes) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for PDF extraction")
        doc = fitz.open(stream=content, filetype="pdf")
        try:
            return "".join(page.get_text() for page in doc)
        finally:
            doc.close()

    def _make_title(self, code: str, name: str, text: str) -> str:
        # Prefer the PDF's leading title line, e.g.
        # "Wisconsin Formal Ethics Opinion E-00-01: Dividing Fees ..."
        for line in text.split("\n")[:6]:
            ln = line.strip()
            if len(ln) >= 12 and re.search(r"[A-Za-z]{4}", ln) and "opinion" in ln.lower():
                return ln[:300]
        # else derive from the filename (strip the code + noise words)
        stem = re.sub(r"\.pdf$", "", name, flags=re.I)
        stem = re.sub(r"\b(revised|final|part\s*\d+)\b", "", stem, flags=re.I)
        stem = re.sub(r"\s+", " ", stem).strip(" -")
        return stem[:300] if stem else f"Wisconsin Ethics Opinion {code}"

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(op["url"])
        if not r or not r.content:
            return None
        if r.content[:4] != b"%PDF" and "pdf" not in r.headers.get("content-type", "").lower():
            logger.warning(f"  {op['code']}: not a PDF")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {op['code']}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            logger.warning(f"  {op['code']}: insufficient text ({len(text)} chars)")
            return None

        # WI ethics opinions are dated by year (encoded in the opinion number).
        # Body text carries cited dates from other opinions/cases, so scanning
        # it mis-attributes the issue date; use the deterministic code-year.
        year = self._year_from_code(op["code"], op["prefix"])
        date = f"{year}-01-01" if year is not None else None

        return {
            "opinion_number": op["code"],
            "title": self._make_title(op["code"], op["name"], text),
            "text": text,
            "date": date,
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing State Bar of Wisconsin professional ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['url']})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/WI-LegalEthics/{num}",
            "_source": "US/WI-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("State Bar of Wisconsin — Standing Committee on "
                       "Professional Ethics"),
            "title": raw.get("title") or f"Wisconsin Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-WI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
                logger.warning(f"  no text for {op['code']}, skipping")
                continue
            yield rec
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

    parser = argparse.ArgumentParser(description="US/WI-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WILegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
