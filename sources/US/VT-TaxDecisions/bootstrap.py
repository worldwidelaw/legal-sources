#!/usr/bin/env python3
"""
US/VT-TaxDecisions -- Vermont Department of Taxes: Formal Rulings

Fetches the full text of the Vermont Department of Taxes' published FORMAL
RULINGS -- redacted interpretive rulings issued to a requesting taxpayer and
published for general guidance (interpretive doctrine). Each ruling states the
governing law reference, the date, the authoring/approving officials and the
Department's interpretive conclusion on the taxpayer's question.

These are public Vermont state-government works (public domain,
government-edicts doctrine).

NOTE (2026-07-02): the Department's separate "Determinations" collection
(formerly /tax-law-and-guidance/determinations) has been removed from the site
(the path and its taxonomy term page now 404 / render empty), and the master
document library /documents does NOT contain the Determination or Formal
Ruling categories. The Formal Rulings section, however, is published
server-side and remains fully available, so this source captures that
collection.

Site: tax.vermont.gov (Drupal). The listing page

  https://tax.vermont.gov/tax-law-and-guidance/formal-rulings

renders every ruling link SERVER-SIDE (paths /tax-law-and-guidance/
formal-rulings/fr-YYYY-NN and a few legacy /content/formal-ruling-YYYY-NN).
Each ruling node page renders its FULL TEXT inline in the body field
(<div property="content:encoded" class="... field--name-body ...">), so no PDF
download is required. No JavaScript, no CAPTCHA, no auth.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample docs
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import html as html_lib
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
logger = logging.getLogger("legal-data-hunter.US.VT-TaxDecisions")

BASE = "https://tax.vermont.gov"
LISTING_URL = BASE + "/tax-law-and-guidance/formal-rulings"

RULING_HREF_RE = re.compile(
    r'href="([^"]*(?:/tax-law-and-guidance/formal-rulings/fr-|/content/formal-ruling-)[^"#?]*)"',
    re.I,
)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)
BODY_OPEN_RE = re.compile(
    r'<div[^>]*\bproperty="content:encoded"[^>]*>', re.I)
TAG_RE = re.compile(r"<[^>]+>")

MON = ["January", "February", "March", "April", "May", "June", "July",
       "August", "September", "October", "November", "December"]
DATE_RE = re.compile(r"\b(" + "|".join(MON) + r")\s+(\d{1,2}),?\s+(\d{4})")
NDATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
FR_NUM_RE = re.compile(r"(?:fr-|formal-ruling-)(\d{4})-(\d{1,3})", re.I)


class VTTaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---- HTTP helper -----------------------------------------------------

    def _curl_get(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl GET failed for {url} (try {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---- parsing ---------------------------------------------------------

    @staticmethod
    def _clean(fragment: str) -> str:
        return re.sub(r"\s+", " ",
                      html_lib.unescape(TAG_RE.sub(" ", fragment))).strip()

    @staticmethod
    def _abs(href: str) -> str:
        href = href.replace("http://tax.vermont.gov", "https://tax.vermont.gov")
        if href.lower().startswith("http"):
            return href
        return BASE + "/" + href.lstrip("/")

    @staticmethod
    def _slug(url: str) -> str:
        name = url.rstrip("/").rsplit("/", 1)[-1]
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return (slug or "formal-ruling")[:120]

    @staticmethod
    def _extract_body(html: str) -> str:
        """Return the plain text of the content:encoded body div, handling
        nested <div> tags by tracking depth."""
        m = BODY_OPEN_RE.search(html)
        if not m:
            return ""
        start = m.end()
        depth = 1
        i = start
        for tok in re.finditer(r"<(/?)div\b[^>]*>", html[start:], re.I):
            if tok.group(1):  # closing
                depth -= 1
            else:
                depth += 1
            if depth == 0:
                inner = html[start:start + tok.start()]
                # convert block breaks to newlines for readability
                inner = re.sub(r"</p>|<br\s*/?>", "\n", inner, flags=re.I)
                text = html_lib.unescape(TAG_RE.sub(" ", inner))
                text = re.sub(r"[ \t]+", " ", text)
                text = re.sub(r"\n\s*\n\s*", "\n\n", text)
                return text.strip()
        return ""

    @classmethod
    def _date_iso(cls, text: str, url: str) -> str | None:
        for mo, d, y in DATE_RE.findall(text):
            try:
                yi = int(y)
                if 1970 <= yi <= 2035:
                    return f"{yi:04d}-{MON.index(mo)+1:02d}-{int(d):02d}"
            except ValueError:
                continue
        for mm, dd, y in NDATE_RE.findall(text):
            try:
                yi, mi, di = int(y), int(mm), int(dd)
                if 1970 <= yi <= 2035 and 1 <= mi <= 12 and 1 <= di <= 31:
                    return f"{yi:04d}-{mi:02d}-{di:02d}"
            except ValueError:
                continue
        fm = FR_NUM_RE.search(url)
        if fm:
            yi = int(fm.group(1))
            if 1970 <= yi <= 2035:
                return f"{yi:04d}-01-01"
        return None

    def discover_documents(self, sample: bool = False) -> list[dict]:
        html = self._curl_get(LISTING_URL)
        if not html:
            logger.error("Failed to fetch formal-rulings listing")
            return []
        seen: set[str] = set()
        out: list[dict] = []
        for href in RULING_HREF_RE.findall(html):
            url = self._abs(href)
            # normalise trailing artefacts
            if url in seen or url.rstrip("/") == LISTING_URL:
                continue
            seen.add(url)
            out.append({"url": url, "slug": self._slug(url)})
        # stable order (newest fr number last -> keep as discovered)
        logger.info(f"Discovered {len(out)} formal rulings")
        if sample:
            return out[:20]
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        html = self._curl_get(doc["url"])
        if not html:
            return None
        h1 = H1_RE.search(html)
        title = self._clean(h1.group(1)) if h1 else doc["slug"]
        body = self._extract_body(html)
        if not body or len(body) < 150:
            logger.warning(f"No usable body for {doc['url']} "
                           f"({len(body)} chars)")
            return None
        doc = dict(doc)
        doc["title"] = title
        doc["text"] = body
        doc["date"] = self._date_iso(body, doc["url"])
        return doc

    def test_api(self) -> bool:
        logger.info("Testing VT formal-rulings listing + node extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No rulings discovered")
                return False
            logger.info(f"  Discovered {len(docs)} rulings (sample)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  Body text OK ({len(raw['text'])} chars) — "
                            f"{raw.get('title')} [{raw.get('date')}]")
            else:
                logger.error("  Body extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title") or raw["slug"]
        if not re.search(r"\bruling\b", title, re.I):
            title = f"Vermont Formal Ruling: {title}"
        return {
            "_id": f"US/VT-TaxDecisions/{raw['slug']}",
            "_source": "US/VT-TaxDecisions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "category": "Formal Ruling",
            "issuer": "Vermont Department of Taxes",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-VT",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
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


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/VT-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VTTaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
