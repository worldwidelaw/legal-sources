#!/usr/bin/env python3
"""
US/WI-EthicsOpinions -- Wisconsin Ethics Commission — Formal Advisory Opinions

Fetches the full text of the formal advisory opinions published by the Wisconsin
Ethics Commission and its statutory predecessors (the Wisconsin Ethics Board and
the Government Accountability Board / GAB). Under Wis. Stat. § 19.46(2) the
Commission issues formal advisory opinions construing the state Code of Ethics
for public officials (Wis. Stat. ch. 19, subch. III), the lobbying law (ch. 13,
subch. III) and the campaign-finance law (ch. 11). Formal advisory opinions are
public record and are published on the Commission's website; they are the
Commission's official written interpretation of the ethics statutes = doctrine.

Access (no JavaScript, no CAPTCHA, no auth):
  The Commission's "Resources" SharePoint document library is rendered as a
  single server-side HTML table at

      https://ethics.wi.gov/Pages/Resources/ResourcesOverview.aspx

  Every document is one <tr> whose <td> cells give the modified date, audience,
  document type, topic, the PDF file name and a title. Rows whose document-type
  cell is "Opinion" are the formal advisory opinions. Each file name resolves to
  a born-digital PDF (real text layer, no OCR) hosted directly at

      https://ethics.wi.gov/Resources/{filename}

Strategy:
  GET the ResourcesOverview page, parse every table row, keep the rows whose
  type cell == "Opinion", build (opinion_number, title, topic, audience,
  pdf_url), download each PDF, extract its text layer and normalize. The opinion
  number is parsed from the leading token of the title (e.g. "00-02",
  "2008 GAB 03", "04 Op. Eth Bd 103 (1981)"); the decision date is parsed from
  the opinion body ("Month DD, YYYY") with a fallback to the year embedded in
  the opinion number.

Usage:
  python bootstrap.py bootstrap            # Full pull (all formal opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
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
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WI-EthicsOpinions")

BASE_URL = "https://ethics.wi.gov"
RESOURCES_URL = "https://ethics.wi.gov/Resources/"
LISTING_URL = "https://ethics.wi.gov/Pages/Resources/ResourcesOverview.aspx"

# Each document is one table row of <td class="ms-vb2"> cells.
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r'<td class="ms-vb2">(.*?)</td>', re.S | re.I)

MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
DATE_RE = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}),\s+((?:19|20)\d{{2}})\b")
_MONTH_IDX = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}

# Opinion-number token at the start of a title, tried most-specific first.
NUM_CITATION_RE = re.compile(r"^(\d{1,2}\s+Op\.?\s*Eth\.?\s*Bd\.?\s+\d+\s*\((\d{4})\))", re.I)
NUM_GAB_RE = re.compile(r"^((?:19|20)\d{2})\s+GAB\s+(\d+)", re.I)
NUM_YY_RE = re.compile(r"^(\d{2})[-_]\s?(\d{1,3})")


def _clean(fragment: str) -> str:
    """Strip HTML tags/entities from a small HTML fragment."""
    txt = re.sub(r"<[^>]+>", " ", fragment or "")
    txt = _html.unescape(txt)
    # Some SharePoint captions carry a stray zero-width space.
    txt = txt.replace("​", "")
    return re.sub(r"\s+", " ", txt).strip()


def _date_from_text(text: str) -> str | None:
    """First 'Month DD, YYYY' in the opinion body -> ISO date."""
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = _MONTH_IDX.get(m.group(1).capitalize())
    d, y = int(m.group(2)), int(m.group(3))
    if mo and 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class WIEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        # NOTE: ethics.wi.gov serves the full server-rendered SharePoint list
        # table only to non-browser clients; a Mozilla/browser UA gets a
        # JavaScript shell with no rows. Use a plain UA (like mass.gov Akamai).
        self._ua = "python-requests/2.31 (legal-data-hunter)"

    # ---------------------------------------------------------------- http
    def _curl(self, url: str, binary: bool = False):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout if binary else out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _pdf_url(filename: str) -> str:
        """Absolute, percent-encoded URL for a Resources-library file name."""
        filename = _html.unescape(filename).strip()
        return RESOURCES_URL + quote(filename, safe="")

    @staticmethod
    def _opinion_number(title: str) -> str | None:
        """Return a normalized opinion number from the leading title token."""
        t = (title or "").strip()
        m = NUM_CITATION_RE.match(t)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()
        m = NUM_GAB_RE.match(t)
        if m:
            return f"{m.group(1)} GAB {m.group(2)}"
        m = NUM_YY_RE.match(t)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}"
        return None

    @staticmethod
    def _year_from_number(number: str | None, title: str) -> str | None:
        """Best-effort issue year (ISO 'YYYY-01-01') from the number/title."""
        for src in (number, title):
            if not src:
                continue
            m = re.search(r"\((\d{4})\)", src)  # citation year
            if m:
                return f"{m.group(1)}-01-01"
            m = re.match(r"^((?:19|20)\d{2})\s+GAB", src)  # GAB year
            if m:
                return f"{m.group(1)}-01-01"
            m = re.match(r"^(\d{2})[-_ ]", src)  # NN- prefix
            if m:
                yy = int(m.group(1))
                year = 1900 + yy if yy >= 50 else 2000 + yy
                return f"{year}-01-01"
        return None

    @staticmethod
    def _slug(filename: str) -> str:
        fname = _html.unescape(filename).rsplit("/", 1)[-1]
        fname = re.sub(r"\.pdf$", "", fname, flags=re.I)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", fname).strip("-")
        return slug[:100] or "opinion"

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{opinion_number, title, topic, audience, filename, date, pdf_url}]."""
        html = self._curl(LISTING_URL)
        if not html:
            logger.error("could not fetch the Resources listing page")
            return []
        seen: dict[str, dict] = {}
        for rm in ROW_RE.finditer(html):
            cells = [_clean(c) for c in CELL_RE.findall(rm.group(1))]
            if len(cells) < 5:
                continue
            # Document-type cell must be exactly "Opinion".
            if not any(c == "Opinion" for c in cells):
                continue
            filename = next((c for c in cells if c.lower().endswith(".pdf")), None)
            if not filename:
                continue
            idx = cells.index(filename)
            title = cells[idx + 1] if idx + 1 < len(cells) else \
                re.sub(r"\.pdf$", "", filename, flags=re.I)
            topic = cells[3] if len(cells) > 3 else ""
            audience = cells[1] if len(cells) > 1 else ""
            slug = self._slug(filename)
            if slug in seen:
                continue
            number = self._opinion_number(title)
            seen[slug] = {
                "_slug": slug,
                "opinion_number": number,
                "title": title,
                "topic": topic,
                "audience": audience,
                "filename": filename,
                "date": self._year_from_number(number, title),
                "pdf_url": self._pdf_url(filename),
            }
        return list(seen.values())

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing WI Ethics Commission opinions + PDF extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no opinions found on page")
            return False
        logger.info(f"  discovered {len(items)} formal advisory opinions")
        ok = 0
        for it in items[:5]:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['_slug']}")
                continue
            text = _pdf_extract_bytes(pdf)
            if text and len(text) > 400:
                logger.info(f"  {it['opinion_number'] or it['_slug']} OK "
                            f"({len(text)} chars) date={_date_from_text(text) or it['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("opinion_number")
        caption = raw.get("title") or raw.get("_slug")
        title = caption
        if number and not caption.startswith(number):
            title = f"Formal Advisory Opinion {number}: {caption}"
        # The filename slug is the only guaranteed-unique key: opinion numbers
        # (NN-NN) are reused across the Ethics Board and Elections Board series.
        _id = f"US/WI-EthicsOpinions/{raw.get('_slug')}"
        return {
            "_id": _id,
            "_source": "US/WI-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Wisconsin Ethics Commission",
            "title": title,
            "topic": raw.get("topic") or None,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-WI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it.get('_slug')}")
                continue
            try:
                text = _pdf_extract_bytes(pdf)
            except Exception as e:
                logger.warning(f"  extract failed for {it.get('_slug')}: {e}")
                continue
            if not text or len(text) < 400:
                logger.warning(f"  {it.get('_slug')}: insufficient text "
                               f"({len(text) if text else 0} chars), skipping")
                continue
            date = _date_from_text(text) or it["date"]
            yield {**it, "text": text, "date": date}
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

    parser = argparse.ArgumentParser(description="US/WI-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WIEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
