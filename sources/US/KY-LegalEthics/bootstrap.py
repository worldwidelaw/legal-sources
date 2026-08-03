#!/usr/bin/env python3
"""
US/KY-LegalEthics -- Kentucky Bar Association — Ethics Opinions

Fetches the full text of the legal-ethics opinions issued by the Kentucky Bar
Association (KBA) interpreting the Kentucky Rules of Professional Conduct (SCR
3.130), and, for the older opinions, the predecessor Canons of Professional
Ethics, to advise LAWYERS. One continuous "KBA E-" series (E-1, 1962 -> E-458+,
present). The KBA is an independent agency of the Supreme Court of Kentucky with
authority to regulate the legal profession (SCR), so the opinions are the work
of a government-authorized body -> pd-us. = doctrine (advisory).

Distinct from US/KY-Courts (Kentucky appellate courts), US/KY-Legislation
(KRS / legislature.ky.gov) and any executive ethics commission.

Access (no JavaScript execution, no CAPTCHA, no auth):
  1. The published corpus is indexed on a single listing page,
       https://kybar.org/For-Members/Rules-Ethics-Information/Ethics-Opinions
     which links every opinion PDF directly as
       /Portals/0/Admin/Ethics Opinions/KBA_E-{NNN}.pdf
     (~381 published opinions; some numbers in the 1..458 range are reserved /
     withdrawn and simply not linked).
  2. Each PDF is born-digital (text layer) -> extracted with PyMuPDF, NO OCR.
     The header reads "KENTUCKY BAR ASSOCIATION / Ethics Opinion KBA E-{N} /
     Issued: {Month} {Year}", followed by a rules-amendment disclaimer and a
     "Question:" / "Subject:" block.

  The opinion NUMBER is taken from the PDF filename (the PDF body sometimes
  renders it with a font/OCR artifact, e.g. "KBA E-l" for E-1), so we never
  parse the number from the body.

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
from urllib.parse import quote, urljoin

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
logger = logging.getLogger("legal-data-hunter.US.KY-LegalEthics")

BASE = "https://kybar.org"
LIST_URL = BASE + "/For-Members/Rules-Ethics-Information/Ethics-Opinions"
PDF_HREF_RE = re.compile(r"Ethics.{0,4}Opinions?/KBA[_ ]E-?(\d{1,3})[a-z]?\.pdf", re.I)

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
# "Issued: March 1962" (older) and "Issued: March 15, 2024" (newer, with day).
ISSUED_RE = re.compile(
    r"Issued:\s*([A-Za-z]+)\s+(?:(\d{1,2}),\s*)?(\d{4})", re.I)


class KYLegalEthicsScraper(BaseScraper):

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

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> list[dict]:
        """Return [{number, url}] parsed from the listing page, de-duped."""
        r = self._get(LIST_URL)
        if not r:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = PDF_HREF_RE.search(href)
            if not m:
                continue
            num = f"E-{int(m.group(1))}"          # strip zero-pad: E-001 -> E-1
            if num in out:
                continue
            full = urljoin(BASE + "/", href)
            # percent-encode spaces in the path but keep an already-encoded URL
            full = full.replace(" ", "%20")
            out[num] = {"number": num, "url": full}
        result = sorted(out.values(),
                        key=lambda x: int(x["number"].split("-")[1]))
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
        m = ISSUED_RE.search(text[:1200])
        if not m:
            return None
        mon = MONTHS.get(m.group(1).lower())
        day = int(m.group(2)) if m.group(2) else 1
        yr = int(m.group(3))
        if mon and 1 <= day <= 31 and 1900 <= yr <= 2100:
            return f"{yr:04d}-{mon:02d}-{day:02d}"
        return None

    @staticmethod
    def _make_title(number: str, text: str) -> str:
        # The subject/question follows a "Subject:" or "Question[.:]" label
        # after the header + amendment disclaimer.
        m = re.search(r"\n(?:Subject|Question)\s*[.:]\s*\n?(.+)", text, re.I)
        if m:
            subject = m.group(1).strip()
            # take the first sentence / line only
            subject = re.split(r"(?<=[.?])\s+(?=[A-Z])", subject)[0]
            subject = subject.split("\n")[0].strip()
            if len(subject) > 8:
                return f"KBA {number}: {subject}"[:300]
        return f"Kentucky Bar Association Ethics Opinion KBA {number}"

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(op["url"])
        if not r or not r.content:
            return None
        if r.content[:4] != b"%PDF" and \
                "pdf" not in r.headers.get("content-type", "").lower():
            logger.warning(f"  {op['number']}: not a PDF")
            return None
        try:
            raw = self._pdf_text(r.content)
        except Exception as e:
            logger.warning(f"  {op['number']}: PDF extract failed: {e}")
            return None
        text = self._clean(raw)
        if len(text) < 150:
            logger.warning(f"  {op['number']}: insufficient text ({len(text)})")
            return None
        return {
            "number": op["number"],
            "title": self._make_title(op["number"], text),
            "text": text,
            "date": self._issue_date(text),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Kentucky Bar Association ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']}")
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
            "_id": f"US/KY-LegalEthics/{num}",
            "_source": "US/KY-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Kentucky Bar Association — Ethics Committee",
            "title": raw.get("title") or f"Kentucky Ethics Opinion KBA {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-KY",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
                logger.warning(f"  no text for {op['number']}, skipping")
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

    parser = argparse.ArgumentParser(description="US/KY-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KYLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
