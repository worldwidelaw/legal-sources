#!/usr/bin/env python3
"""
US/NJ-LegalEthics -- New Jersey Supreme Court ethics advisory opinions
(ACPE / Committee on Attorney Advertising / Committee on the Unauthorized
Practice of Law), digitized by the Rutgers Law Library.

Fetches the full text of the three series of advisory opinions issued by the
New Jersey Supreme Court's standing ethics committees, each of which advises
LAWYERS on the New Jersey Rules of Professional Conduct and related rules =
doctrine (advisory):

  * ACPE -- Advisory Committee on Professional Ethics (file prefix "acp")
  * CAA  -- Committee on Attorney Advertising          (file prefix "caa")
  * UPL  -- Committee on the Unauthorized Practice of Law (file prefix "cua")

All three committees are appointed by, and act under the authority of, the
Supreme Court of New Jersey (R. 1:19, R. 1:19A, R. 1:22), so the opinion texts
are the work of a government-authorized body -- treated as public domain under
the 17 U.S.C. § 105 government-edicts rationale, consistent with the other
state ethics-committee legal-ethics sources. Published free to the public by
the Rutgers Law Library digital collection, no login/paywall/reuse terms.

Access (no JavaScript, no CAPTCHA, no auth, browser UA):
  The collection at njlaw.rutgers.edu/ethics/ stores every opinion as a
  born-digital PDF under /ethics/pdfs/, and that directory has an OPEN index
  listing all ~902 files. The scraper parses the directory listing, groups the
  files into opinions by (series, number) -- a single opinion may span several
  PDFs ("{prefix}{N}_1.pdf", "{prefix}{N}_2.pdf", ...) which are concatenated in
  order -- and extracts each PDF's text with PyMuPDF, NO OCR.

  Number: from the filename ("acp724" -> ACPE Opinion 724). The opinion header
  in the body echoes it as "OPINION {N}".
  Title: the subject caption printed immediately after the "OPINION {N}" header.
  Date: the first "Month DD, YYYY" in the body (the opinion's issue date, printed
  next to its N.J.L.J. citation); falls back to null.

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
logger = logging.getLogger("legal-data-hunter.US.NJ-LegalEthics")

BASE = "https://njlaw.rutgers.edu/ethics"
DIR_URL = BASE + "/pdfs/"
PDF_URL = BASE + "/pdfs/{fname}"

CURRENT_YEAR = 2026

MONTHS = (r"(?:January|February|March|April|May|June|July|August|"
          r"September|October|November|December)")

# file prefix -> (series code, committee full name)
SERIES = {
    "acp": ("ACPE", "Advisory Committee on Professional Ethics"),
    "caa": ("CAA", "Committee on Attorney Advertising"),
    "cua": ("UPL", "Committee on the Unauthorized Practice of Law"),
}
# order for sampling / listing
SERIES_ORDER = ["acp", "caa", "cua"]


class NJLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.3
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/16.0 Safari/605.1.15"
            ),
            "Accept": "application/pdf,text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(3):
            try:
                r = self._session.get(url, timeout=90)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _part_key(part: str):
        """Natural sort key for a part token like '1', '2', '3_2'."""
        return [int(x) if x.isdigit() else x for x in part.split("_")]

    def _list_opinions(self) -> list[dict]:
        """Parse the open /ethics/pdfs/ directory index and group files into
        opinions. Returns a list of dicts:
          {series, code, committee, number, files:[fname,...]} ordered by
          series (ACPE, CAA, UPL) then numeric opinion number."""
        r = self._get(DIR_URL)
        if not r:
            logger.error("could not fetch PDF directory index")
            return []
        fnames = re.findall(r'href="([A-Za-z]+\d+_[^"]+\.pdf)"', r.text)
        groups: dict[tuple, dict] = {}
        for fn in fnames:
            m = re.match(r"([A-Za-z]+)(\d+)_(.+)\.pdf$", fn)
            if not m:
                continue
            prefix, num, part = m.group(1).lower(), int(m.group(2)), m.group(3)
            if prefix not in SERIES:
                continue
            key = (prefix, num)
            g = groups.setdefault(key, {
                "series": prefix,
                "code": SERIES[prefix][0],
                "committee": SERIES[prefix][1],
                "number": num,
                "_parts": [],
            })
            g["_parts"].append((part, fn))
        opinions = []
        for g in groups.values():
            g["_parts"].sort(key=lambda pf: self._part_key(pf[0]))
            g["files"] = [fn for _, fn in g["_parts"]]
            del g["_parts"]
            opinions.append(g)
        opinions.sort(key=lambda g: (SERIES_ORDER.index(g["series"]), g["number"]))
        logger.info(f"  directory index yields {len(fnames)} files -> "
                    f"{len(opinions)} opinions")
        return opinions

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        # drop the digitization breadcrumb the collection prepends
        text = re.sub(r"Link to original WordPerfect Document\s*", "", text)
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

    def _fetch_pdf_text(self, fname: str) -> str | None:
        r = self._get(PDF_URL.format(fname=fname))
        if not r or not r.content:
            return None
        if r.content[:4] != b"%PDF" and \
                "pdf" not in r.headers.get("content-type", "").lower():
            logger.warning(f"  {fname}: not a PDF")
            return None
        try:
            return self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {fname}: PDF extract failed: {e}")
            return None

    @staticmethod
    def _make_title(code: str, num: int, text: str) -> str:
        """The subject caption is printed on the short lines immediately after
        the 'OPINION {N}' header line, before the first body paragraph."""
        # match the real header line ("OPINION 24" in caps at line start), not a
        # mixed-case "... Opinion No. 24 ..." citation elsewhere in the body
        m = re.search(r"^OPINION\s+(?:No\.?\s*)?%d\b" % num, text, re.M)
        if m:
            after = text[m.end():]
            caption = []
            for line in after.splitlines():
                line = line.strip()
                if not line:
                    if caption:
                        break
                    continue
                # a body paragraph line is long or ends a sentence
                if len(line) >= 90 or (caption and line.endswith(".")):
                    break
                if re.match(r"(Appointed by|Supersedes|See footnote|By the )",
                            line, re.I):
                    continue
                caption.append(line)
                if len(caption) >= 6 or sum(len(c) for c in caption) > 220:
                    break
            title = re.sub(r"\s+", " ", " ".join(caption)).strip(" .:-")
            title = re.sub(r"See footnote.*$", "", title).strip()
            if 3 < len(title) < 260:
                return title.title() if title.isupper() else title
        return f"{code} Opinion {num}"

    @staticmethod
    def _parse_date(ds: str):
        try:
            # some headers omit the space after the comma ("July 25,1963")
            norm = re.sub(r"[,\s]+", " ", ds).strip()
            return datetime.strptime(norm, "%B %d %Y").date()
        except Exception:
            return None

    @classmethod
    def _issue_date(cls, text: str) -> str | None:
        # first in-body "Month DD, YYYY" is the opinion's issue date (printed
        # beside its N.J.L.J. citation in the header); allow a missing space
        # after the comma ("July 25,1963")
        for mm in re.finditer(
                r"(" + MONTHS + r"\s+\d{1,2}\s*,?\s*\d{4})", text):
            d = cls._parse_date(mm.group(1))
            if d and 1955 <= d.year <= CURRENT_YEAR + 1:
                return d.isoformat()
        return None

    def _fetch_one(self, op: dict) -> dict | None:
        parts = []
        for fn in op["files"]:
            t = self._fetch_pdf_text(fn)
            if t:
                parts.append(t)
            time.sleep(self.delay)
        if not parts:
            return None
        text = self._clean("\n".join(parts))
        if len(text) < 150:
            logger.warning(f"  {op['code']}-{op['number']}: insufficient text "
                           f"({len(text)})")
            return None
        return {
            "series": op["code"],
            "committee": op["committee"],
            "number": op["number"],
            "opinion_number": f"{op['code']}-{op['number']}",
            "title": self._make_title(op["code"], op["number"], text),
            "text": text,
            "date": self._issue_date(text),
            "url": PDF_URL.format(fname=op["files"][0]),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NJ Rutgers ethics-opinions collection...")
        samples = [
            {"code": "ACPE", "committee": SERIES["acp"][1], "number": 724,
             "files": ["acp724_1.pdf"]},
            {"code": "CAA", "committee": SERIES["caa"][1], "number": 16,
             "files": ["caa16_1.pdf"]},
            {"code": "UPL", "committee": SERIES["cua"][1], "number": 24,
             "files": ["cua24_1.pdf"]},
        ]
        ok = 0
        for op in samples:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']} "
                            f"title={rec['title'][:50]!r}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['code']}-{op['number']})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/NJ-LegalEthics/{raw['opinion_number']}",
            "_source": "US/NJ-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": raw["opinion_number"],
            "series": raw["series"],
            "issuer": f"Supreme Court of New Jersey — {raw['committee']}",
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NJ",
        }

    @staticmethod
    def _interleave(opinions: list[dict]) -> list[dict]:
        """Round-robin across the three series so any prefix of the stream
        (e.g. the first-12 sample) is representative of all committees."""
        buckets = {s: [] for s in SERIES_ORDER}
        for op in opinions:
            buckets[op["series"]].append(op)
        out, i = [], 0
        while any(buckets[s] for s in SERIES_ORDER):
            for s in SERIES_ORDER:
                if i < len(buckets[s]):
                    out.append(buckets[s][i])
            i += 1
        return out

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        opinions = self._interleave(self._list_opinions())
        emitted = 0
        for op in opinions:
            rec = self._fetch_one(op)
            if not rec:
                logger.warning(f"  no text for {op['code']}-{op['number']}, "
                               f"skipping")
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

    parser = argparse.ArgumentParser(description="US/NJ-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
