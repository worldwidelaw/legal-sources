#!/usr/bin/env python3
"""
EU/EP-Questions — European Parliament parliamentary questions (written questions
to the Commission / Council / ECB, with their official answers).

Members of the European Parliament put thousands of *written questions* to the
other EU institutions every year (Rules of Procedure, Rule 144 and predecessors).
Each question — and the institution's written answer — is an official EU document
published by the Parliament. Together they form a very large, continuously growing
full-text corpus of EU regulatory/policy Q&A (well over 100 000 documents from the
7th parliamentary term onward).

Unlike EU legislation and adopted texts, parliamentary questions are **not** given
a downloadable body in CELLAR (the ``9{YYYY}E{NNNN}`` CELEX works are metadata-only
there — every content-negotiation manifestation 404s). The full text lives instead
in the **European Parliament Open Data Portal**, which exposes each question, and
its answer, as a born-digital PDF distribution.

Distinct from EU/EuroParl, which covers the Parliament's *adopted texts*
(resolutions, legislative positions) via the ``/adopted-texts`` endpoint. This
source uses the separate ``/parliamentary-questions`` endpoint; there is no
document overlap, and the loader dedups on ``_id`` regardless.

Data flow
---------
1. Enumerate questions from the EP Open Data Portal, scoped by year (the portal
   covers term 8 onward, i.e. ~2014-present):
       GET /api/v2/parliamentary-questions?year={YYYY}&offset={N}&limit={K}
   returning work stubs (``identifier`` = e.g. ``E-10-2024-001357``).
2. For each work, fetch its detail record:
       GET /api/v2/parliamentary-questions/{identifier}?language=en
   which embeds the English expression's PDF manifestation *and* — via
   ``inverse_answers_to`` — the answer document's PDF manifestation, both as
   ``is_exemplified_by`` distribution paths.
3. Download each PDF from ``https://data.europarl.europa.eu/{path}`` and extract
   text with PyMuPDF. The question text and every answer are concatenated.
4. Normalize to the standard schema (doctrine).

The Open Data Portal (data.europarl.europa.eu) serves the JSON API and the PDF
distributions directly (redirecting to redmapl3.europarl.europa.eu media), and is
not behind the AWS-WAF that 202-challenges the www.europarl.europa.eu doceo pages,
so it is fleet-safe.
"""

import sys
import re
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logger = logging.getLogger("legal-data-hunter")

API_BASE = "https://data.europarl.europa.eu/api/v2/parliamentary-questions"
DL_BASE = "https://data.europarl.europa.eu/"

# The Open Data Portal covers the 8th parliamentary term onward.
MIN_YEAR = 2014


class EPQuestionsScraper(BaseScraper):
    """Scraper for European Parliament written questions + answers."""

    PAGE_SIZE = 100

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (+https://github.com/ZachLaik) legal-open-data",
        })

    # ---- HTTP helpers -----------------------------------------------------

    def _get(self, url, *, headers=None, params=None, max_retries=4, timeout=60):
        last = None
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, headers=headers, params=params,
                                     timeout=timeout, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code == 204:
                    return r  # no content for this query (e.g. empty year)
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt, 30))
                    continue
                return r
            except requests.RequestException as e:
                last = e
                time.sleep(min(2 ** attempt, 30))
        if last:
            raise last
        return None

    def _list_year(self, year: int, offset: int) -> list:
        r = self._get(API_BASE, headers={"Accept": "application/ld+json"},
                      params={"year": year, "offset": offset, "limit": self.PAGE_SIZE,
                              "format": "application/ld+json"}, timeout=90)
        if r is None or r.status_code != 200:
            return []
        try:
            return r.json().get("data", []) or []
        except ValueError:
            return []

    @staticmethod
    def _eng_pdf_path(expressions: list) -> Optional[str]:
        """Return the English-expression PDF distribution path, if any."""
        for exp in expressions or []:
            if not str(exp.get("id", "")).endswith("/en"):
                continue
            for man in exp.get("is_embodied_by", []) or []:
                if "pdf" in str(man.get("id", "")):
                    return man.get("is_exemplified_by")
        return None

    @staticmethod
    def _eng_title(expressions: list) -> Optional[str]:
        for exp in expressions or []:
            if not str(exp.get("id", "")).endswith("/en"):
                continue
            t = exp.get("title")
            if isinstance(t, dict):
                return t.get("en")
            if isinstance(t, str):
                return t
        return None

    def _pdf_text(self, path: str) -> str:
        if not path or fitz is None:
            return ""
        r = self._get(DL_BASE + path.lstrip("/"),
                      headers={"User-Agent": "Mozilla/5.0 LegalDataHunter/1.0"}, timeout=90)
        if r is None or r.status_code != 200 or r.content[:5] != b"%PDF-":
            return ""
        try:
            doc = fitz.open(stream=r.content, filetype="pdf")
            text = "\n".join(p.get_text() for p in doc)
            doc.close()
        except Exception as e:  # pragma: no cover
            logger.debug("PDF extract failed for %s: %s", path, e)
            return ""
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\s*\n\s*", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _build_record(self, identifier: str) -> Optional[dict]:
        """Fetch detail + PDFs for one question, returning a raw dict or None."""
        r = self._get(f"{API_BASE}/{identifier}", headers={"Accept": "application/ld+json"},
                      params={"format": "application/ld+json", "language": "en"}, timeout=90)
        if r is None or r.status_code != 200:
            return None
        try:
            data = r.json().get("data", [])
        except ValueError:
            return None
        if not data:
            return None
        work = data[0]

        title = self._eng_title(work.get("is_realized_by", []))
        q_path = self._eng_pdf_path(work.get("is_realized_by", []))
        q_text = self._pdf_text(q_path) if q_path else ""

        answers = []
        for ans in work.get("inverse_answers_to", []) or []:
            a_path = self._eng_pdf_path(ans.get("is_realized_by", []))
            if a_path:
                a_text = self._pdf_text(a_path)
                if a_text:
                    answers.append(a_text)
            time.sleep(0.5)

        parts = []
        if q_text:
            parts.append("QUESTION\n\n" + q_text)
        for i, a in enumerate(answers, 1):
            label = "ANSWER" if len(answers) == 1 else f"ANSWER {i}"
            parts.append(f"--- {label} ---\n\n" + a)
        text = "\n\n".join(parts).strip()
        if len(text) < 100:
            return None

        return {
            "identifier": identifier,
            "date": work.get("document_date"),
            "title": title,
            "text": text,
        }

    # ---- BaseScraper contract --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        max_year = datetime.now(timezone.utc).year
        for year in range(max_year, MIN_YEAR - 1, -1):
            offset = 0
            while True:
                stubs = self._list_year(year, offset)
                if not stubs:
                    break
                for stub in stubs:
                    ident = stub.get("identifier")
                    if not ident:
                        continue
                    rec = self._build_record(ident)
                    time.sleep(1)
                    if rec:
                        yield rec
                if len(stubs) < self.PAGE_SIZE:
                    break
                offset += self.PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        cutoff = since.date().isoformat() if isinstance(since, datetime) else str(since)
        since_year = int(cutoff[:4])
        max_year = datetime.now(timezone.utc).year
        for year in range(max_year, max(since_year, MIN_YEAR) - 1, -1):
            offset = 0
            while True:
                stubs = self._list_year(year, offset)
                if not stubs:
                    break
                for stub in stubs:
                    ident = stub.get("identifier")
                    if not ident:
                        continue
                    rec = self._build_record(ident)
                    time.sleep(1)
                    if rec and (not rec.get("date") or rec["date"] >= cutoff):
                        yield rec
                if len(stubs) < self.PAGE_SIZE:
                    break
                offset += self.PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        ident = raw["identifier"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"European Parliament written question {ident}"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": ident,
            "_source": "EU/EP-Questions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "identifier": ident,
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://data.europarl.europa.eu/eli/dl/doc/{ident}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = EPQuestionsScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        stubs = scraper._list_year(datetime.now(timezone.utc).year, 0)
        print(f"List returned {len(stubs)} questions on page 1")
        if stubs:
            ident = stubs[0]["identifier"]
            rec = scraper._build_record(ident)
            if rec:
                print(f"  {ident}: {(rec.get('title') or '')[:70]}")
                print(f"  full text length: {len(rec['text'])} chars")
            else:
                print(f"  {ident}: no full text")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
