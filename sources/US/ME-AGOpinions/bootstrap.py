#!/usr/bin/env python3
"""
US/ME-AGOpinions -- Maine Attorney General Opinions

Fetches the full text of formal opinions and memoranda issued by the
Maine Attorney General. Each opinion is an authoritative interpretation
of Maine law issued at the request of a public official -- classified
as doctrine.

Strategy:
  The Maine Law and Legislative Digital Library publishes a single
  server-rendered "master" index page that lists EVERY AG opinion
  (1874-present) in one HTML <table>. Each row carries the issue date,
  opinion number, a one-line subject (the <a> link text), the issuing
  Attorney General, the requestor, and statutory/case citations. The
  link points at a direct, public, text-recognised PDF on
  lldc.mainelegislature.org. No pagination, no JS, no CAPTCHA.

  1. GET the master index (one request) and parse every <tr>.
  2. Keep rows whose link is /Open/AG/Opinions/YYYY/ag_*.pdf.
  3. Download each PDF and extract its OCR text layer via
     common.pdf_extract (the library skips the boilerplate cover sheet
     content naturally; we keep all pages).
  4. Normalize into the standard doctrine schema (text = PDF body).

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import html as ihtml
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ME-AGOpinions")

INDEX_URL = "https://www.maine.gov/legis/lawlib/lldl/agops/agmaster.html"
# AG opinion PDFs live on the digital-library host referenced by the index.
OPINION_PATH = "/Open/AG/Opinions/"

_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)
_A_RE = re.compile(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.I | re.S)


def strip_tags(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", raw)
    text = re.sub(r"<[^>]+>", " ", text)
    text = ihtml.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_us_date(raw: str) -> str | None:
    """MM/DD/YYYY -> YYYY-MM-DD."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw or "")
    if not m:
        return None
    mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mm <= 12 and 1 <= dd <= 31 and 1700 < yyyy < 2100):
        return None
    return f"{yyyy:04d}-{mm:02d}-{dd:02d}"


def date_from_filename(pdf_url: str) -> str | None:
    """ag_YYYYMMDD[a].pdf -> YYYY-MM-DD (fallback when the cell date is blank)."""
    m = re.search(r"ag_(\d{4})(\d{2})(\d{2})", pdf_url)
    if not m:
        return None
    yyyy, mm, dd = m.group(1), int(m.group(2)), int(m.group(3))
    if not (1 <= mm <= 12 and 1 <= dd <= 31):
        return f"{yyyy}-01-01"
    return f"{yyyy}-{mm:02d}-{dd:02d}"


class MEAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ----- index discovery -------------------------------------------------
    def _fetch_index(self) -> str:
        time.sleep(self.delay)
        resp = self.http.get(INDEX_URL)
        if resp.status_code != 200:
            raise RuntimeError(f"Index HTTP {resp.status_code}")
        # The master page is latin-1 encoded.
        try:
            return resp.content.decode("latin-1")
        except Exception:
            return resp.text

    def discover_opinions(self, sample: bool = False) -> list[dict]:
        """Parse the master index table into per-opinion metadata rows."""
        html_doc = self._fetch_index()
        out: list[dict] = []
        seen: set[str] = set()
        for tr in _TR_RE.findall(html_doc):
            am = _A_RE.search(tr)
            if not am:
                continue
            href = am.group(1).strip()
            if OPINION_PATH not in href:
                continue
            if not href.lower().endswith(".pdf"):
                continue
            pdf_url = href if href.startswith("http") else (
                "https://lldc.mainelegislature.org" + href
            )
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            cells = [strip_tags(c) for c in _TD_RE.findall(tr)]
            cell_date = cells[0] if len(cells) > 0 else ""
            number = cells[1] if len(cells) > 1 else ""
            subject = strip_tags(am.group(2))
            ag = cells[3] if len(cells) > 3 else ""
            requestor = cells[4] if len(cells) > 4 else ""
            citations = " ; ".join(
                c for c in cells[5:] if c
            ) if len(cells) > 5 else ""

            date_iso = parse_us_date(cell_date) or date_from_filename(pdf_url)
            stem = re.sub(r"\.pdf$", "", pdf_url.rsplit("/", 1)[-1], flags=re.I)
            opinion_key = (number.strip() or stem)
            opinion_key = re.sub(r"\s+", "", opinion_key)

            out.append({
                "opinion_key": opinion_key,
                "opinion_number": number.strip() or None,
                "subject": subject or None,
                "attorney_general": ag or None,
                "requestor": requestor or None,
                "citations": citations or None,
                "date": date_iso,
                "pdf_url": pdf_url,
                "_stem": stem,
            })
        logger.info(f"Discovered {len(out)} AG opinion rows from the index")
        return out

    # ----- per-opinion text ------------------------------------------------
    def _build_raw(self, meta: dict) -> dict | None:
        pdf_url = meta["pdf_url"]
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/ME-AGOpinions", pdf_url=pdf_url,
            table="doctrine", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(
                f"No usable text (image-only scan?) for {pdf_url} "
                f"({len(text) if text else 0} chars)"
            )
            return None
        raw = dict(meta)
        raw["text"] = text.strip()
        return raw

    def test_api(self) -> bool:
        logger.info("Testing Maine AG opinions index + PDF extraction...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions (showing newest first)")
            raw = None
            # Walk newest-first (index is oldest-first) for a text-layer PDF.
            for meta in reversed(ops):
                raw = self._build_raw(meta)
                if raw:
                    break
            if raw and len(raw["text"]) > 150:
                logger.info(
                    f"  PDF text extraction OK ({len(raw['text'])} chars) "
                    f"[{raw['pdf_url']}]"
                )
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # ----- iteration -------------------------------------------------------
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        ops = self.discover_opinions(sample=sample)
        # Newest-first so sample pulls modern, clean text-layer PDFs.
        ops = list(reversed(ops))
        emitted = 0
        for meta in ops:
            raw = self._build_raw(meta)
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

    # ----- normalize -------------------------------------------------------
    def normalize(self, raw: dict) -> dict:
        key = raw["opinion_key"]
        number = raw.get("opinion_number")
        subject = raw.get("subject")
        if number and subject:
            title = f"Maine Attorney General Opinion {number} — {subject}"
        elif subject:
            title = f"Maine Attorney General Opinion — {subject}"
        elif number:
            title = f"Maine Attorney General Opinion {number}"
        else:
            title = f"Maine Attorney General Opinion {key}"
        return {
            "_id": f"US/ME-AGOpinions/{key}",
            "_source": "US/ME-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "subject": subject,
            "attorney_general": raw.get("attorney_general"),
            "requestor": raw.get("requestor"),
            "citations": raw.get("citations"),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date") or None,
            "url": raw["pdf_url"],
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ME-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MEAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
