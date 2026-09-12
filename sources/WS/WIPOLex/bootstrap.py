#!/usr/bin/env python3
"""
WS/WIPOLex -- Samoa legislation via the WIPO Lex database.

WIPO Lex (https://www.wipo.int/wipolex/) is WIPO's free, open gateway to the
intellectual-property and IP-related laws of ~200 jurisdictions. For the
Independent State of Samoa it holds the full text of core statutes -- the
Intellectual Property Act and Regulations, the Copyright Act, the Trademarks
provisions, broadcasting and related commercial acts -- as machine-readable PDF
documents in English (Samoa's legislative language; a few texts also in Samoan).

Samoa's consolidated statutes live behind the SamLII / PacLII platform, which
blocks datacenter IPs on its document pages. WIPO Lex publishes machine-readable
Samoan statute text without authentication and is a stable external full-text
source of the country's law.

Strategy:
  1. Fetch the WIPO Lex Samoa member profile
     (/wipolex/en/members/profile/WS). It is server-rendered HTML listing each
     legal text as a table row: adoption date, title and a link to the
     legislation detail page (/wipolex/en/legislation/details/{id}).
  2. For each detail page, extract the CloudFront-signed PDF download URL(s)
     (https://wipolex-res.wipo.int/edocs/lexdocs/laws/{lang}/ws/{code}.pdf).
     The signed `?last-modified=...` query string is required -- the bare URL
     returns an HTML error page. English is preferred (Samoa's legislative
     language), then Samoan, then French.
  3. Download the PDF and extract full text via the shared pdf_extract backend
     (pdfplumber / pypdf / fitz, with OCR fallback). Any older originals that
     are scanned image-only PDFs with no text layer are skipped.

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import html
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
import urllib3

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.WS.WIPOLex")

HOST = "https://www.wipo.int"
COUNTRY = "WS"
COUNTRY_NAME = "Samoa"
PROFILE_PATH = f"/wipolex/en/members/profile/{COUNTRY}"
SOURCE_ID = "WS/WIPOLex"
DELAY = 1.5

UA = {"User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +legal-data-hunter)"}

DETAIL_LINK_RE = re.compile(
    r'href="/wipolex/en/legislation/details/(\d+)">\s*([^<]+?)\s*</a>', re.S
)
DATE_DIV_RE = re.compile(r'class="black">\s*([^<]+?)\s*</div>')
PDF_HREF_RE = re.compile(
    r'href="(https://wipolex-res\.wipo\.int/edocs/lexdocs/laws/[^"]+\.pdf[^"]*)"'
)
# Language code sits between /laws/ and the 2-letter country segment, e.g.
# .../edocs/lexdocs/laws/en/ws/ws001en.pdf
LANG_RE = re.compile(r"/edocs/lexdocs/laws/([a-z]{2})/[a-z]{2}/")

# Language preference: English first (Samoa's legislative language), then Samoan,
# then French.
LANG_PRIORITY = {"en": 0, "sm": 1, "fr": 2}
LANG_NAMES = {"en": "English", "sm": "Samoan", "fr": "French"}


def _norm_ws(text: str) -> str:
    """Collapse whitespace (incl. non-breaking spaces) in a title."""
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _get(url: str, retries: int = 3) -> Optional[str]:
    """HTTP GET returning decoded text, or None on failure."""
    for attempt in range(retries):
        try:
            time.sleep(DELAY)
            r = requests.get(url, headers=UA, timeout=45, verify=False)
            if r.status_code == 200:
                return r.text
            logger.warning("GET %s -> HTTP %d (attempt %d)", url, r.status_code, attempt + 1)
        except Exception as e:
            logger.warning("GET %s failed: %s (attempt %d)", url, e, attempt + 1)
        if attempt < retries - 1:
            time.sleep(3)
    return None


def _parse_iso_date(raw: str) -> Optional[str]:
    """Parse a WIPO Lex date string ('January 4, 2004') into ISO 8601."""
    raw = (raw or "").strip()
    for fmt in ("%B %d, %Y", "%B %Y", "%Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _parse_profile(html_text: str) -> List[Tuple[str, str, Optional[str]]]:
    """Return [(detail_id, title, iso_date)] for every Samoan legal text.

    Each table row renders the adoption date in a ``<div class="black">`` cell
    immediately before the title cell's detail link, so each link is paired with
    the nearest preceding date div by position.
    """
    dates = [(m.start(), m.group(1).strip()) for m in DATE_DIV_RE.finditer(html_text)]
    links = [
        (m.start(), m.group(1), _norm_ws(html.unescape(m.group(2))))
        for m in DETAIL_LINK_RE.finditer(html_text)
    ]

    def nearest_date(pos: int) -> Optional[str]:
        prior = [d for p, d in dates if p < pos]
        return prior[-1] if prior else None

    rows: List[Tuple[str, str, Optional[str]]] = []
    seen: set = set()
    for pos, det_id, title in links:
        if det_id in seen:
            continue
        seen.add(det_id)
        rows.append((det_id, title, _parse_iso_date(nearest_date(pos))))
    return rows


def _detail_pdf_urls(detail_id: str) -> List[str]:
    """Signed PDF download URLs for a detail page, English first."""
    page = _get(f"{HOST}/wipolex/en/legislation/details/{detail_id}")
    if not page:
        return []
    urls = [html.unescape(u) for u in PDF_HREF_RE.findall(page)]
    # De-dup while keeping order, then sort by language preference.
    unique = list(dict.fromkeys(urls))

    def lang_key(u: str) -> int:
        m = LANG_RE.search(u)
        return LANG_PRIORITY.get(m.group(1) if m else "", 9)

    return sorted(unique, key=lang_key)


class WIPOLexWSScraper(BaseScraper):
    """Scraper for WS/WIPOLex."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir or str(Path(__file__).parent))

    def _iter_docs(self) -> Generator[Tuple[str, str, Optional[str]], None, None]:
        profile = _get(f"{HOST}{PROFILE_PATH}")
        if not profile:
            logger.error("Could not fetch %s member profile", COUNTRY)
            return
        rows = _parse_profile(profile)
        logger.info("%s member profile lists %d legal texts", COUNTRY, len(rows))
        yield from rows

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        for detail_id, title, date in self._iter_docs():
            urls = _detail_pdf_urls(detail_id)
            if not urls:
                logger.info("No PDF attached, skipping: %s", title[:60])
                continue

            doc_id = f"WS_WIPOLEX_{detail_id}"
            text = None
            chosen_url = None
            language = None
            for url in urls:
                m = LANG_RE.search(url)
                lang = m.group(1) if m else None
                try:
                    extracted = extract_pdf_markdown(
                        source=SOURCE_ID,
                        source_id=doc_id,
                        pdf_url=url,
                        table="legislation",
                        # force: without it, extract_pdf_markdown returns None for any doc
                        # already in Neon with text, which this loop cannot tell apart from a
                        # scanned PDF. Once the (small, born-digital) WIPO Lex corpus is
                        # ingested, every later crawl reported all of it as scanned and emitted
                        # 0 records, so the fleet fell back to the bundled samples (#1520).
                        # Re-extraction is cheap here and a full run has to emit every record.
                        force=True,
                    )
                except Exception as e:
                    logger.warning("PDF extraction failed for %s: %s", url, e)
                    extracted = None
                if extracted and len(extracted.strip()) >= 200:
                    text = extracted
                    chosen_url = url
                    language = lang
                    break

            if not text:
                logger.warning("No extractable full text (scanned?): %s", title[:60])
                continue

            count += 1
            logger.info("Full text [%d]: %s (%s, %d chars)",
                        count, title[:60], language or "?", len(text))
            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": f"{HOST}/wipolex/en/legislation/details/{detail_id}",
                "pdf_url": chosen_url,
                "language": language,
            }

        logger.info("Completed: %d documents with full text", count)

    def fetch_updates(self, since: Any = None) -> Generator[Dict[str, Any], None, None]:
        # WIPO Lex has no incremental feed; re-scan the (small) WS corpus.
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        lang = raw.get("language")
        return {
            "_id": raw["doc_id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "language": lang,
            "language_name": LANG_NAMES.get(lang, lang),
            "country": COUNTRY,
            "jurisdiction": COUNTRY_NAME,
        }

    def test(self) -> bool:
        logger.info("Testing WS/WIPOLex profile + detail + PDF extraction...")
        profile = _get(f"{HOST}{PROFILE_PATH}")
        if not profile:
            logger.error("Profile fetch failed")
            return False
        rows = _parse_profile(profile)
        logger.info("Parsed %d WS legal texts", len(rows))
        if len(rows) < 10:
            logger.error("Expected >=10 WS texts, got %d", len(rows))
            return False
        det_id, title, date = rows[0]
        urls = _detail_pdf_urls(det_id)
        logger.info("First text: %s (%s) | %d PDF url(s)", title[:50], date, len(urls))
        return bool(urls)


def main():
    parser = argparse.ArgumentParser(description="WS/WIPOLex data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WIPOLexWSScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        scraper.bootstrap(sample_mode=args.sample, sample_size=25)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    main()
