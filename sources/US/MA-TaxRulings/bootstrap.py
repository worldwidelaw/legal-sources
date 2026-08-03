#!/usr/bin/env python3
"""
US/MA-TaxRulings -- Massachusetts Department of Revenue — Letter Rulings

Fetches the full text of the Letter Rulings issued by the Massachusetts
Department of Revenue (DOR). A Letter Ruling is the Commissioner's written
statement, in response to a specific taxpayer's request, applying the tax
statutes and regulations to that taxpayer's stated facts. It is official DOR
guidance on the meaning of Massachusetts tax law = doctrine.

Access (no JavaScript, no CAPTCHA, no auth):
  The rulings are indexed on a single Mass.gov list page:

      https://www.mass.gov/lists/dor-letter-rulings

  which server-renders an <a href="/letter-ruling/{slug}">Letter Ruling NN-N:
  {subject}</a> link for every ruling (~995). Each ruling's own page carries the
  full body text (a "Date:" table row + the ruling text in ma__rich-text blocks).

  IMPORTANT — Akamai UA note: www.mass.gov is behind Akamai Bot Manager, which
  flags browser-*claiming* User-Agents (Mozilla/...) that fail its JS/TLS
  fingerprint challenge and returns HTTP 403. A plain, honest non-browser UA
  (python-requests/curl token) passes with 200 for BOTH the list page and the
  individual ruling pages. Do NOT set a Mozilla UA here.

Strategy:
  GET the list page, parse each /letter-ruling/{slug} anchor to (url, number,
  title), GET each ruling page, extract the "Date:" field and the ma__rich-text
  body text, and normalize into the doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all letter rulings)
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MA-TaxRulings")

BASE_URL = "https://www.mass.gov"
LIST_URL = "https://www.mass.gov/lists/dor-letter-rulings"

# Each ruling link on the list page.
LINK_RE = re.compile(
    r'<a[^>]*href="(?P<href>/letter-ruling/[^"#]+)"[^>]*>(?P<text>.*?)</a>',
    re.S | re.I,
)
# Ruling number at the start of the title, e.g. "Letter Ruling 24-1:".
NUMBER_RE = re.compile(r"Letter\s+Ruling\s+(\d{2,4}-\d{1,3})", re.I)
# "Date:" table row -> MM/DD/YYYY.
DATE_RE = re.compile(r"Date:\s*.*?(\d{1,2}/\d{1,2}/\d{4})", re.S)


def _clean(fragment: str) -> str:
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", fragment or "", flags=re.S | re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    return re.sub(r"\s+", " ", _html.unescape(txt)).strip()


def _extract_body(html: str) -> str:
    """Concatenate the cleaned text of every ma__rich-text block (balanced divs)."""
    parts: list[str] = []
    for mo in re.finditer(r'<div class="ma__rich-text', html):
        i = mo.start()
        depth = 0
        end = len(html)
        for dm in re.finditer(r"<div\b|</div>", html[i:]):
            if dm.group(0) == "<div":
                depth += 1
            else:
                depth -= 1
                if depth == 0:
                    end = i + dm.end()
                    break
        frag = _clean(html[i:end])
        if len(frag) > 40:
            parts.append(frag)
    return "\n\n".join(parts)


class MATaxRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        # See module docstring: a plain non-browser UA passes Akamai; a Mozilla
        # UA gets 403-fingerprint-blocked. Do NOT change this to a browser UA.
        self._ua = "legal-data-hunter (+https://github.com/ZachLaik) python-requests/2.31"

    # ---------------------------------------------------------------- http
    def _curl(self, url: str) -> str | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _norm_date(html: str, number: str) -> str | None:
        m = DATE_RE.search(html)
        if m:
            mm, dd, yyyy = m.group(1).split("/")
            mm, dd, yyyy = int(mm), int(dd), int(yyyy)
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1970 <= yyyy <= 2035:
                return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
        # Fallback: two/four-digit year prefix in the ruling number (24-1 / 1985-3).
        m = re.match(r"(\d{2,4})-", number)
        if m:
            y = int(m.group(1))
            if y < 100:
                y = 1900 + y if y >= 70 else 2000 + y
            return f"{y:04d}-01-01"
        return None

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        html = self._curl(LIST_URL)
        if not html:
            logger.error("could not fetch the DOR Letter Rulings list")
            return []
        seen: dict[str, dict] = {}
        for m in LINK_RE.finditer(html):
            href = _html.unescape(m.group("href")).split("#", 1)[0]
            title = _clean(m.group("text"))
            if not title or not title.lower().startswith("letter ruling"):
                continue
            nm = NUMBER_RE.search(title)
            number = nm.group(1) if nm else href.rsplit("/", 1)[-1][:40]
            if href in seen:
                continue
            seen[href] = {
                "number": number,
                "title": title,
                "url": BASE_URL + href,
            }
        return list(seen.values())

    def _fetch_doc(self, item: dict) -> dict | None:
        html = self._curl(item["url"])
        if not html:
            logger.warning(f"  no page for {item['number']}")
            return None
        body = _extract_body(html)
        if not body or len(body) < 300:
            logger.warning(f"  {item['number']}: insufficient text "
                           f"({len(body)} chars), skipping")
            return None
        return {
            **item,
            "text": body,
            "date": self._norm_date(html, item["number"]),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing MA DOR Letter Rulings list + body extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no rulings found in list")
            return False
        logger.info(f"  discovered {len(items)} letter rulings")
        ok = 0
        for it in items[:5]:
            doc = self._fetch_doc(it)
            if doc:
                logger.info(f"  {doc['number']} OK ({len(doc['text'])} chars) "
                            f"date={doc['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        title = raw.get("title") or f"Letter Ruling {number}"
        return {
            "_id": f"US/MA-TaxRulings/{number}",
            "_source": "US/MA-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "document_number": number,
            "issuer": "Massachusetts Department of Revenue",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            doc = self._fetch_doc(it)
            if not doc:
                continue
            yield doc
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

    parser = argparse.ArgumentParser(description="US/MA-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MATaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
