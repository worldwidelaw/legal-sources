#!/usr/bin/env python3
"""
US/DE-TaxAppeals -- Delaware State Tax Appeal Board
Opinions / Decisions & Orders (case_law)

Fetches the full text of the Delaware State Tax Appeal Board's published
opinions. The Tax Appeal Board (29 Del. C. sec. 8306; 30 Del. C. ch. 3
subch. II) is the independent state tribunal that hears appeals from
determinations of the Delaware Division of Revenue (personal income,
corporate income, franchise, gross-receipts, and other state taxes) and
from the State Escheator's unclaimed-property determinations. Every
published document adjudicates a specific contested case -> case_law.

Access (no JavaScript, no CAPTCHA, no auth):
  The Board publishes its final opinions on one server-rendered page,
  listed by docket number in PDF form:
    https://finance.delaware.gov/state-tax-appeal-board/opinions-of-the-tax-appeal-board/
  Each <a href> points at a born-image PDF hosted on the Department of
  Finance file server:
    https://financefiles.delaware.gov/TAB/{filename}.pdf
  The filename encodes the docket number(s) and, for most opinions, the
  party name and/or the decision date (e.g. "1815 Parsons TAB Decision
  and Order dated 10.28.2024.pdf", "422 423 424 Heisler.pdf").

  Text layer: the Board's opinions are SCANNED IMAGES with no embedded
  text layer (both the oldest 1971 opinions and the newest 2024/2025
  ones). common.pdf_extract falls back to OCR (PyMuPDF -> pytesseract)
  when the born-digital backends return empty, which is gated on the
  `tesseract` binary being installed. Run on a vantage that has tesseract
  to obtain full text; budget for OCR slowness (~237 opinions).

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
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DE-TaxAppeals")

INDEX_URL = (
    "https://finance.delaware.gov/state-tax-appeal-board/"
    "opinions-of-the-tax-appeal-board/"
)

MIN_TEXT_CHARS = 200

# Absolute PDF links on the Department of Finance file server.
PDF_HREF_RE = re.compile(
    r'href="(?P<url>https://financefiles\.delaware\.gov/TAB/[^"]+\.pdf)"',
    re.I,
)
TAG_RE = re.compile(r"<[^>]+>")

# Dotted date embedded in the filename (M.D.YYYY or M.D.YY).
FILE_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})")
# Long-form date in the OCR'd opinion body ("January 5, 2024" / "Jan. 5, 2024").
BODY_DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\.?\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# Filename tokens that are boilerplate, not party names.
_BOILERPLATE = {
    "tab", "decision", "order", "and", "by", "dated", "board", "the", "of",
    "opinion", "opinions", "final", "amended", "corrected", "re", "in",
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _slugify(stem: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-").lower()
    return re.sub(r"-{2,}", "-", s)


def _iso_from_dotted(m: re.Match) -> str | None:
    mo, day, yr = m.group(1), m.group(2), m.group(3)
    if len(yr) == 2:
        yr = ("19" + yr) if int(yr) >= 40 else ("20" + yr)
    try:
        return datetime(int(yr), int(mo), int(day)).date().isoformat()
    except ValueError:
        return None


def _parse_filename(filename: str) -> dict:
    """Parse docket number(s), party, and date out of a TAB PDF filename."""
    stem = filename[:-4] if filename.lower().endswith(".pdf") else filename
    slug = _slugify(stem)
    # Normalise separators to spaces for token parsing.
    norm = re.sub(r"[_]+", " ", stem)

    # Filename-embedded date (if any), then strip it out of the token stream.
    date = None
    dm = FILE_DATE_RE.search(norm)
    if dm:
        date = _iso_from_dotted(dm)
        norm = norm[: dm.start()] + " " + norm[dm.end():]

    tokens = norm.split()
    # Leading pure-integer tokens are docket numbers.
    dockets: list[str] = []
    i = 0
    while i < len(tokens) and re.fullmatch(r"\d{1,6}[A-C]?", tokens[i]):
        dockets.append(tokens[i])
        i += 1
    rest = tokens[i:]

    party_tokens = [t for t in rest if t.lower() not in _BOILERPLATE]
    party = " ".join(party_tokens).strip(" -,.") or None

    docket_number = ", ".join(dockets) if dockets else None
    return {
        "slug": slug,
        "docket_number": docket_number,
        "party": party,
        "date": date,
    }


class DETaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def discover_documents(self, sample: bool = False) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        page = self._get_text(INDEX_URL)
        if not page:
            logger.error("Failed to fetch the Delaware Tax Appeal Board opinions index")
            return docs
        for m in PDF_HREF_RE.finditer(page):
            url = html_module.unescape(m.group("url"))
            filename = unquote(url.rsplit("/TAB/", 1)[-1])
            if url in seen:
                continue
            seen.add(url)
            meta = _parse_filename(filename)
            if meta["slug"] in {d["slug"] for d in docs}:
                continue
            party = meta["party"]
            docket = meta["docket_number"]
            if docket and party:
                title = f"Delaware Tax Appeal Board — Docket {docket} ({party})"
            elif party:
                title = f"Delaware Tax Appeal Board — {party}"
            elif docket:
                title = f"Delaware Tax Appeal Board — Docket {docket}"
            else:
                title = f"Delaware Tax Appeal Board opinion ({meta['slug']})"
            docs.append({
                "slug": meta["slug"],
                "docket_number": docket,
                "party": party,
                "title": title,
                "date": meta["date"],
                "pdf_url": url,
                "filename": filename,
            })
        logger.info(f"Discovered {len(docs)} Delaware Tax Appeal Board opinions")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/DE-TaxAppeals",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars) — scanned PDF, "
                           f"OCR (tesseract) required: {doc['slug']}")
            return None
        # Backfill the decision date from the opinion body when the filename
        # carried none.
        if not doc.get("date"):
            bm = BODY_DATE_RE.search(text)
            if bm:
                try:
                    doc = dict(doc)
                    doc["date"] = datetime(
                        int(bm.group(3)), _MONTHS[bm.group(1).lower()], int(bm.group(2))
                    ).date().isoformat()
                except (ValueError, KeyError):
                    pass
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Delaware State Tax Appeal Board opinions...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            for doc in docs[-5:]:  # newest opinions are last in the list
                raw = self._build_raw(doc)
                if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                    logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                                f"{raw.get('docket_number')}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  Text extraction failed — the PDFs are scanned; OCR "
                         "(tesseract) must be available on this host")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        title = raw.get("title") or "Delaware Tax Appeal Board opinion"
        return {
            "_id": f"US/DE-TaxAppeals/{raw['slug']}",
            "_source": "US/DE-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": raw.get("docket_number"),
            "court": "Delaware State Tax Appeal Board",
            "title": title[:300],
            "party": raw.get("party"),
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-DE",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        docs = self.discover_documents(sample=sample)
        # Newest opinions (bottom of the list) first in sample mode so the
        # sample is representative of the current corpus.
        if sample:
            docs = list(reversed(docs))
        for doc in docs:
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
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

    parser = argparse.ArgumentParser(description="US/DE-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DETaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
