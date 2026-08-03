#!/usr/bin/env python3
"""
US/MT-AGOpinions -- Montana Attorney General Opinions

Fetches the full text of formal opinions issued by the Montana Attorney
General. Each opinion is an authoritative interpretation of Montana law
issued at the request of a public official -- classified as doctrine.

Strategy:
  The Montana Judicial Branch State Law Library hosts the historical
  Opinions of the Attorney General (Volumes 1-44, covering 1899-1992) as
  clean, text-layer PDFs. The index is two shallow levels:

    /library/mr/agopinions/          -> links to each volume page (vol{N})
    /library/mr/agopinions/vol{N}    -> one HTML <table> of opinions, each
                                        row = [opinion no. (links the PDF),
                                        "Held" summary, date]

  The PDFs live at /external/ag-opinions/{vol}/{num}.pdf and carry a real
  text layer (no OCR needed).

  1. GET the index, collect every vol{N} link.
  2. For each volume page, parse every row -> (number, held, date, pdf_url).
  3. Download each PDF and extract its text via common.pdf_extract.
  4. Normalize into the standard doctrine schema (text = PDF body).

  NOTE: Opinions from 1993-present are published on dojmt.gov, which is
  WAF/Cloudflare-gated (HTTP 403 to non-browser clients). Only the
  historical 1899-1992 corpus on courts.mt.gov is built here; the recent
  set needs browser automation / a VPS pass.

Usage:
  python bootstrap.py bootstrap            # Full pull (all volumes)
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
logger = logging.getLogger("legal-data-hunter.US.MT-AGOpinions")

BASE_URL = "https://courts.mt.gov"
INDEX_URL = f"{BASE_URL}/library/mr/agopinions/"

_VOL_RE = re.compile(r'href="(?:[^"]*/)?vol(\d+)"', re.I)
_ROW_RE = re.compile(
    r'<tr[^>]*>\s*'
    r'<td[^>]*>\s*<a[^>]+href="([^"]+\.pdf)"[^>]*>(.*?)</a>\s*</td>\s*'
    r'<td[^>]*>(.*?)</td>\s*'
    r'<td[^>]*>(.*?)</td>',
    re.I | re.S,
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def strip_tags(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", raw)
    text = re.sub(r"<[^>]+>", " ", text)
    text = ihtml.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_long_date(raw: str) -> str | None:
    """'January 18, 1949' -> '1949-01-18'."""
    raw = strip_tags(raw)
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", raw)
    if m:
        mon = _MONTHS.get(m.group(1).lower())
        if mon:
            return f"{int(m.group(3)):04d}-{mon:02d}-{int(m.group(2)):02d}"
    m = re.search(r"\b(\d{4})\b", raw)
    if m:
        return f"{int(m.group(1)):04d}-01-01"
    return None


class MTAGOpinionsScraper(BaseScraper):

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

    def _fetch(self, url: str) -> str:
        time.sleep(self.delay)
        resp = self.http.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} for {url}")
        return resp.text

    def _volume_urls(self) -> list[str]:
        html_doc = self._fetch(INDEX_URL)
        vols = sorted({int(n) for n in _VOL_RE.findall(html_doc)})
        # Newest volumes first so a sample pulls the most recent opinions.
        return [f"{BASE_URL}/library/mr/agopinions/vol{n}" for n in reversed(vols)]

    def discover_opinions(self, sample: bool = False) -> list[dict]:
        """Walk volume pages -> per-opinion metadata rows."""
        out: list[dict] = []
        seen: set[str] = set()
        for vol_url in self._volume_urls():
            try:
                html_doc = self._fetch(vol_url)
            except Exception as e:
                logger.warning(f"Volume fetch failed {vol_url}: {e}")
                continue
            rows = _ROW_RE.findall(html_doc)
            logger.info(f"{vol_url.rsplit('/', 1)[-1]}: {len(rows)} opinion rows")
            for href, num_cell, held_cell, date_cell in rows:
                pdf_url = href if href.startswith("http") else (BASE_URL + href)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                number = re.sub(r"\s+", "", strip_tags(num_cell))
                held = strip_tags(held_cell)
                date_iso = parse_long_date(date_cell)
                # Stable key from the PDF path: vol/num e.g. 23/1 -> 23-1
                pm = re.search(r"/ag-opinions/(\d+)/(\d+)\.pdf", pdf_url)
                key = f"{pm.group(1)}-{pm.group(2)}" if pm else (
                    number or pdf_url.rsplit("/", 1)[-1]
                )
                out.append({
                    "opinion_key": key,
                    "opinion_number": number or None,
                    "held": held or None,
                    "date": date_iso,
                    "pdf_url": pdf_url,
                })
            if sample and len(out) >= 60:
                break
        logger.info(f"Discovered {len(out)} AG opinion rows total")
        return out

    def _build_raw(self, meta: dict) -> dict | None:
        pdf_url = meta["pdf_url"]
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/MT-AGOpinions", pdf_url=pdf_url,
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
        logger.info("Testing Montana AG opinions index + PDF extraction...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            raw = None
            for meta in ops:
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

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for meta in self.discover_opinions(sample=sample):
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

    def normalize(self, raw: dict) -> dict:
        key = raw["opinion_key"]
        number = raw.get("opinion_number")
        held = raw.get("held")
        if number and held:
            title = f"Montana Attorney General Opinion {number} — {held[:160]}"
        elif number:
            title = f"Montana Attorney General Opinion {number}"
        else:
            title = f"Montana Attorney General Opinion {key}"
        return {
            "_id": f"US/MT-AGOpinions/{key}",
            "_source": "US/MT-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "held": held,
            "title": title,
            "text": raw["text"],
            "date": raw.get("date") or None,
            "url": raw["pdf_url"],
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MT-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MTAGOpinionsScraper()

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
