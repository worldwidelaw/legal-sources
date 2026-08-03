#!/usr/bin/env python3
"""
US/CT-JudicialEthics -- Connecticut Committee on Judicial Ethics
                        — Informal Opinion Summaries

Fetches the full text of the Informal Opinion Summaries issued by the
Connecticut Committee on Judicial Ethics. The Committee, established by the
Connecticut Judicial Branch, issues written advisory opinions construing the
Connecticut Code of Judicial Conduct as applied to specific inquiries from
judicial officials = doctrine (the Committee's official written interpretation
of the judicial-conduct rules).

Access (no JavaScript, no CAPTCHA, no auth):
  The Committee publishes a "Summaries of Advisory Opinions" index whose links
  point to per-opinion documents:

      https://www.jud.ct.gov/committees/ethics/summaries.htm
      https://www.jud.ct.gov/committees/ethics/sum/{YYYY-NN}.htm   (older)
      https://www.jud.ct.gov/committees/ethics/sum/{YYYY-NN}.pdf   (newer)

  Each opinion (numbered "YYYY-NN") is born-digital: the older ones are HTML
  pages (cp1252-encoded), the newer ones are PDFs with a real text layer. The
  index lists the correct extension per opinion; the scraper fetches that form
  and, if it comes back empty, retries the other extension.

Strategy:
  GET the summaries index, enumerate every "sum/YYYY-NN.(htm|pdf)" link, dedup
  by opinion number, then fetch each opinion's full text (HTML de-tagged with a
  utf-8->cp1252 fallback, or PDF via the shared text extractor). The issue date
  is parsed from the "(Month DD, YYYY)" header, falling back to the opinion
  number's year. ~300+ opinions from 2008 to present.

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
import subprocess
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CT-JudicialEthics")

BASE = "https://www.jud.ct.gov/committees/ethics/"
INDEX_URL = BASE + "summaries.htm"
LINK_RE = re.compile(r"sum/(\d{4}-\d+)\.(htm|pdf)", re.I)
MONTHS = ("January February March April May June July August September "
          "October November December").split()
DATE_RE = re.compile(
    r"\(\s*(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})\s*\)", re.I)


def _detag_html(raw: bytes) -> str:
    """Decode (utf-8 -> cp1252 fallback) and de-tag an HTML opinion page."""
    try:
        h = raw.decode("utf-8")
    except UnicodeDecodeError:
        h = raw.decode("cp1252", "replace")
    b = re.sub(r"(?is)<script.*?</script>|<style.*?</style>|<!--.*?-->", "", h)
    b = re.sub(r"(?i)<(br|/p|/div|/td|/tr|/li|/h[1-6])[^>]*>", "\n", b)
    b = re.sub(r"<[^>]+>", " ", b)
    b = _html.unescape(b).replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in b.splitlines()]
    return "\n".join(ln for ln in lines if ln).strip()


def _parse_date(text: str, number: str) -> str | None:
    m = DATE_RE.search(text)
    if m:
        mon = MONTHS.index(m.group(1).capitalize()) + 1
        day = int(m.group(2))
        year = int(m.group(3))
        if 1 <= day <= 31:
            return f"{year:04d}-{mon:02d}-{day:02d}"
    ym = re.match(r"(\d{4})-", number)
    return f"{ym.group(1)}-01-01" if ym else None


class CTJudicialEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> list[tuple[str, str]]:
        """Return [(number, ext)] in index order, deduped by number."""
        raw = self._curl_bytes(INDEX_URL)
        if not raw:
            logger.error("could not fetch the summaries index")
            return []
        html = raw.decode("utf-8", "replace")
        seen: dict[str, str] = {}
        for m in LINK_RE.finditer(html):
            number, ext = m.group(1), m.group(2).lower()
            seen.setdefault(number, ext)  # keep first (index-listed) extension
        return list(seen.items())

    # -------------------------------------------------------- extraction
    def _fetch_text(self, number: str, ext: str) -> str:
        """Fetch opinion body, trying the listed extension then the other."""
        for e in (ext, "pdf" if ext == "htm" else "htm"):
            url = f"{BASE}sum/{number}.{e}"
            raw = self._curl_bytes(url)
            if not raw:
                continue
            if e == "pdf":
                if raw[:4] != b"%PDF":
                    continue  # HTML 404 page served with .pdf name
                md = extract_pdf_markdown(
                    url, f"US/CT-JudicialEthics/{number}",
                    pdf_bytes=raw, table="case_law", force=True)
                if md and len(md.strip()) > 100:
                    return md.strip(), url
            else:
                txt = _detag_html(raw)
                # Reject the CT "404 - File or Directory Not Found" template.
                if "File or Directory Not Found" in txt:
                    continue
                if len(txt) > 100:
                    return txt, url
        return "", f"{BASE}sum/{number}.{ext}"

    def _fetch_one(self, number: str, ext: str) -> dict | None:
        text, url = self._fetch_text(number, ext)
        if not text or len(text) < 100:
            return None
        date = _parse_date(text, number)
        return {"number": number, "text": text, "date": date, "url": url}

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CT Committee on Judicial Ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinion links found on index")
            return False
        logger.info(f"  discovered {len(ops)} opinion numbers")
        # Probe a mix of old (htm) and new (pdf) opinions.
        probe = ops[:3] + ops[-3:]
        ok = 0
        for number, ext in probe:
            rec = self._fetch_one(number, ext)
            if rec and len(rec["text"]) > 150:
                logger.info(f"  Opinion {rec['number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        return {
            "_id": f"US/CT-JudicialEthics/{number}",
            "_source": "US/CT-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Connecticut Committee on Judicial Ethics",
            "title": f"Connecticut Judicial Ethics Informal Opinion {number}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-CT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        ops = self._list_opinions()
        emitted = 0
        for number, ext in ops:
            rec = self._fetch_one(number, ext)
            if not rec:
                logger.warning(f"  no text for {number} ({ext}), skipping")
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

    parser = argparse.ArgumentParser(description="US/CT-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CTJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
