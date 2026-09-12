#!/usr/bin/env python3
"""
OM/SJC-CaseLaw -- Oman Supreme Court Legal Principles Fetcher

Fetches the official compilations of legal principles (المبادئ القانونية)
endorsed by the Supreme Court of Oman, published as born-digital PDFs by the
Technical Bureau of the Supreme Judiciary Council (sjc.gov.om).

Strategy:
  - Fetch the Technical Bureau "Legal Principles" page (server-side HTML).
  - Parse the anchor links to the PDF volumes under /userupload/ Legal principles/
    (dedup by href, keep the longest human-readable anchor title).
  - Download each PDF (exact href bytes, URL-encoded) and extract full text with
    PyMuPDF (fitz), normalising Arabic presentation-forms to standard Arabic (NFKC).
  - One normalized record per published volume.

Website: https://www.sjc.gov.om/InnerPage.aspx?ID=2095bd1d-1eca-4996-90e0-76652101f3c3

Usage:
  python bootstrap.py bootstrap           # Full pull
  python bootstrap.py bootstrap --sample   # Sample records
  python bootstrap.py bootstrap-fast       # Alias for full pull (fleet)
  python bootstrap.py update               # Incremental (re-pull; small corpus)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

import sys
import re
import ssl
import json
import time
import hashlib
import logging
import unicodedata
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.OM.SJC-CaseLaw")

BASE = "https://www.sjc.gov.om"
LIST_URL = (
    "https://www.sjc.gov.om/InnerPage.aspx"
    "?ID=2095bd1d-1eca-4996-90e0-76652101f3c3&culture=ar"
)
UA = "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +open-data-research)"

# Tolerate the host's occasional TLS/cert quirks (self-signed intermediate).
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# Division inference from Arabic title keywords.
_DIVISIONS = [
    ("جزائي", "criminal"),
    ("العمالية", "labour-commercial-rental"),
    ("التجارية", "labour-commercial-rental"),
    ("الإيجار", "labour-commercial-rental"),
    ("الشرعية", "personal-status-civil"),
    ("المدنية", "personal-status-civil"),
    ("الأحوال", "personal-status-civil"),
    ("الديات", "blood-money-compensation"),
    ("الأروش", "blood-money-compensation"),
    ("المترجم", "translated-principles"),
    ("Translated", "translated-principles"),
    ("Selected Collection", "translated-principles"),
]


def _http_get(url: str, tries: int = 5, timeout: int = 90) -> bytes:
    """GET raw bytes with retry/backoff (host returns intermittent 404/timeouts)."""
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            return urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX).read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")


def _clean_text(t: str) -> str:
    """Normalise Arabic presentation-forms to standard Arabic and tidy whitespace."""
    if not t:
        return ""
    # NFKC maps Arabic Presentation Forms A/B -> base Arabic letters.
    t = unicodedata.normalize("NFKC", t)
    # Strip bidi/zero-width control characters.
    t = re.sub("[​-‏‪-‮⁦-⁩﻿]", "", t)
    # Collapse runs of blank lines / trailing spaces.
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _parse_year(title: str) -> tuple[Optional[str], str]:
    """Return (ISO date or None, raw period string) from a title's year(s)."""
    years = re.findall(r"(19|20)\d{2}", title)
    if not years:
        return None, ""
    full = re.findall(r"(?:19|20)\d{2}", title)
    period = "-".join(full) if len(full) > 1 else full[0]
    # Use the earliest year of the covered period as the temporal key.
    start = min(full)
    return f"{start}-01-01", period


class OmanSJCPrinciplesScraper(BaseScraper):
    """Scraper for OM/SJC-CaseLaw — Oman Supreme Court legal principles."""

    def __init__(self):
        super().__init__(Path(__file__).parent)

    # -- discovery ----------------------------------------------------------

    def _discover(self) -> list[dict]:
        """Return [{href, title}] for each unique PDF volume (best title kept)."""
        html = _http_get(LIST_URL).decode("utf-8", "replace")
        best: dict[str, str] = {}
        for m in re.finditer(
            r'<a[^>]+href="(/userupload/[^"]+\.pdf)"[^>]*>(.*?)</a>',
            html, re.S | re.I,
        ):
            href = m.group(1)
            title = unescape(re.sub(r"<[^>]+>", "", m.group(2)))
            title = re.sub(r"\s+", " ", title).replace("\xa0", " ").strip()
            # Keep the longest (most descriptive) title seen for this href.
            if len(title) > len(best.get(href, "")):
                best[href] = title
        items = [{"href": h, "title": best[h]} for h in sorted(best)]
        logger.info("Discovered %d principle volumes", len(items))
        return items

    # -- abstract methods ---------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        for item in self._discover():
            yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # Small, slowly-changing corpus of yearly volumes — re-pull everything;
        # the loader dedups on _id.
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        href = raw["href"]
        title = raw.get("title", "").strip()
        url = BASE + urllib.parse.quote(href)

        data = _http_get(url)
        if data[:5] != b"%PDF-":
            logger.warning("Not a PDF (skipping): %s", href)
            return {}

        import fitz  # PyMuPDF
        doc = fitz.open(stream=data, filetype="pdf")
        text = _clean_text("\n".join(p.get_text() for p in doc))
        pages = doc.page_count
        doc.close()

        filename = urllib.parse.unquote(href.split("/")[-1])
        if not title:
            title = filename.rsplit(".", 1)[0].strip()

        date, period = _parse_year(title)
        division = "general"
        for kw, dv in _DIVISIONS:
            if kw in title:
                division = dv
                break
        language = "ar-en" if division == "translated-principles" else "ar"

        _id = "OM-SJC-" + hashlib.md5(href.encode("utf-8")).hexdigest()[:12]

        return {
            "_id": _id,
            "id": _id,
            "_source": "OM/SJC-CaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "period": period,
            "division": division,
            "language": language,
            "pages": pages,
            "url": url,
            "filename": filename,
            "country": "OM",
            "court": "Supreme Court of Oman (المحكمة العليا)",
        }

    # -- custom command -----------------------------------------------------

    def test_api(self):
        print("Testing Oman SJC legal-principles page...")
        items = self._discover()
        print(f"  Volumes discovered: {len(items)}")
        if items:
            rec = self.normalize(items[0])
            print(f"  First volume: {rec.get('title', '')[:70]}")
            print(f"    id={rec.get('_id')} date={rec.get('date')} "
                  f"division={rec.get('division')} pages={rec.get('pages')}")
            txt = rec.get("text", "")
            print(f"    text: {len(txt)} chars")
            print(f"    sample: {txt[:160]!r}")
        print("Test completed.")


def main():
    scraper = OmanSJCPrinciplesScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        sample_size = int(sys.argv[sys.argv.index("--sample-size") + 1])

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: "
                  f"{stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2, default=str))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
