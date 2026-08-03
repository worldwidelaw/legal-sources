#!/usr/bin/env python3
"""
US/NV-LegalEthics -- State Bar of Nevada — Standing Committee on Ethics and
Professional Responsibility, Formal Opinions.

Fetches the full text of the formal ethics opinions issued by the State Bar of
Nevada's Standing Committee on Ethics and Professional Responsibility. Each
opinion interprets the Nevada Rules of Professional Conduct (and, for older
opinions, the predecessor Rules) in response to a stated question, to advise
LAWYERS = doctrine (advisory). One continuous globally-numbered "Formal Opinion
No. N" series running No. 1 (1986) -> No. 61 (2025, present).

The State Bar of Nevada is the state's INTEGRATED (mandatory) bar, unified under
and regulated by the Supreme Court of Nevada (SCR); its ethics opinions are the
work of a government-authorized body -> pd-us (17 U.S.C. § 105 government-edicts
rationale, consistent with the other state-bar legal-ethics sources).

Distinct from US/NV-EthicsOpinions (executive Nevada Commission on Ethics —
public officials), US/NV-Courts, US/NV-Legislation and US/NV-AGOpinions.

Access (no JavaScript execution, no CAPTCHA, no auth):
  1. The published corpus is indexed on a single listing page,
       https://nvbar.org/for-lawyers/ethics-discipline/ethics-opinions/
     which links every opinion PDF directly under /wp-content/uploads/. The
     filenames are irregular (opinion_41.pdf, Ethics_Op_50.pdf,
     NV-Ethics-Opinion-No.-53.pdf, Finalized-Opinion-Rule-4.2-No-Contact-
     Rule_1.13.25-1.pdf, ...), so we take the href verbatim and parse the
     opinion NUMBER primarily from the anchor text ("OPINION 61") and, when the
     anchor text is generic ("complete opinion, PDF"), from the filename.
  2. Each opinion PDF is born-digital (text layer) -> extracted with PyMuPDF,
     NO OCR. A handful of the oldest opinions survive only as scanned images
     with no text layer and are correctly skipped (< 150 chars).

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
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

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
logger = logging.getLogger("legal-data-hunter.US.NV-LegalEthics")

BASE = "https://nvbar.org"
LIST_URL = BASE + "/for-lawyers/ethics-discipline/ethics-opinions/"

# Opinion number from the anchor text ("OPINION 61", "Opinion No. 54").
ANCHOR_NUM_RE = re.compile(r"\bopinion\s+(?:no\.?\s*)?(\d{1,3})\b", re.I)
# Opinion number from the (url-decoded) filename: an "op(inion)" token, an
# optional "No.", then the number.  "opinion_41", "Ethics_Op_50",
# "NV-Ethics-Opinion-No.-53", "Opinion-18_10-29-94", "Opinion 42".
FILE_NUM_RE = re.compile(r"op(?:inion)?[\s._%-]*(?:no[.\s_%-]*)?0*(\d{1,3})", re.I)

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
# "April 29, 1994", "April 23, 1986", "January __, 2025" (blank day).
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(?:(\d{1,2})|_+)?,?\s*(\d{4})", re.I)


class NVLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.0 Safari/605.1.15"
            ),
            "Accept": "text/html,application/pdf,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url[:90]} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------- number parse
    @staticmethod
    def _opinion_number(anchor_text: str, href: str) -> int | None:
        m = ANCHOR_NUM_RE.search(anchor_text or "")
        if m:
            return int(m.group(1))
        fname = unquote(href.rsplit("/", 1)[-1])
        m = FILE_NUM_RE.search(fname)
        if m:
            return int(m.group(1))
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> list[dict]:
        """Return [{number, url}] parsed from the listing page, de-duped.

        Anchors appear in document order: the canonical modern list first,
        then a legacy duplicate list and "... Summary.pdf" digests.  We keep
        the FIRST link seen per opinion number and skip summaries, so we take
        the full opinion, not its summary.
        """
        r = self._get(LIST_URL)
        if not r:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[int, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if ".pdf" not in low or "/uploads/" not in low:
                continue
            text = " ".join(a.get_text(" ", strip=True).split())
            if "summary" in low or "summary" in text.lower():
                continue
            num = self._opinion_number(text, href)
            if num is None or not (1 <= num <= 999):
                continue
            if num in out:
                continue
            full = urljoin(BASE + "/", href).replace(" ", "%20")
            out[num] = {"number": num, "url": full}
        result = sorted(out.values(), key=lambda x: x["number"])
        logger.info(f"  discovered {len(result)} published ethics opinions")
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

    @staticmethod
    def _issue_date(text: str) -> str | None:
        m = DATE_RE.search(text[:2500])
        if not m:
            return None
        mon = MONTHS.get(m.group(1).lower())
        day = int(m.group(2)) if m.group(2) else 1
        yr = int(m.group(3))
        if mon and 1 <= day <= 31 and 1980 <= yr <= 2100:
            return f"{yr:04d}-{mon:02d}-{day:02d}"
        return None

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(op["url"])
        if not r or not r.content:
            return None
        if r.content[:4] != b"%PDF" and \
                "pdf" not in r.headers.get("content-type", "").lower():
            logger.warning(f"  No. {op['number']}: not a PDF")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  No. {op['number']}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            logger.warning(
                f"  No. {op['number']}: insufficient text ({len(text)}) "
                f"- likely scanned, skipping")
            return None
        return {
            "number": op["number"],
            "text": text,
            "date": self._issue_date(text),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing State Bar of Nevada ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  No. {rec['number']} OK ({len(rec['text'])} "
                            f"chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['url']})")
        if ok >= 2:
            logger.info(f"API test PASSED ({len(ops)} opinions available)")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["number"]
        return {
            "_id": f"US/NV-LegalEthics/{num}",
            "_source": "US/NV-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": f"Formal Opinion No. {num}",
            "issuer": ("State Bar of Nevada — Standing Committee on Ethics "
                       "and Professional Responsibility"),
            "title": f"State Bar of Nevada Formal Ethics Opinion No. {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NV",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
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

    parser = argparse.ArgumentParser(description="US/NV-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NVLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
