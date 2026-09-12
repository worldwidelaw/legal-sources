#!/usr/bin/env python3
"""
TJ/SupremeCourt-Plenum -- Қарорҳои Пленуми Суди Олии Ҷумҳурии Тоҷикистон
(Plenum resolutions of the Supreme Court of Tajikistan).

The Plenum of the Supreme Court issues resolutions that give binding
interpretive guidance to the lower courts on how to apply the law — the
post-Soviet equivalent of a practice direction with the force of settled
doctrine. They are published at sud.tj in three subject sections and are
the closest thing Tajikistan has to a public body of authoritative case
law, since individual judgments are not published.

Strategy (static Bitrix HTML, no API, no auth):

  GET /sanadho/karorhoi-plenumi-sudi-oli/{pgo|pj|mfs}/
      -> 200. Each section page is a flat list of

           <a href="/upload/documents/plenum/{dir}/{file}.pdf">
             Қарори Пленуми Суди Олӣ аз {DD.MM.YYYY} №{N} {subject}
           </a>

         so the date, the resolution number and the subject all come off
         the link text, and the section <h1> gives the subject area.

  The PDFs are born-digital and extract cleanly with the shared
  ``common/pdf_extract`` backends.

GOTCHAS:
  - The PDF filenames are Tajik Cyrillic WITH SPACES. The hrefs in the
    HTML are already percent-encoded, so they are used byte-for-byte;
    retyping a filename risks an NFC/NFD mismatch and a 404 (the same trap
    as OM/SJC-CaseLaw).
  - Every section page also links the Judicial Code of Conduct
    (/upload/documents/Кодекси_одоби_судя.pdf) from its sidebar. Only
    hrefs under /plenum/ are collected.
  - Dates in the link text are written both ``29.05.2003`` and
    ``29.09. 2014`` (stray space); both are parsed.
  - A resolution is amended by later Plenum resolutions rather than
    replaced, so the published PDF is the consolidated text and its head
    lists the amending resolutions.

Distinct from TJ/SupremeCourt, which scrapes only /nashriyai-sudi-oli/
(the Supreme Court bulletin) — different path, no overlap.

Usage:
  python bootstrap.py bootstrap            # Full pull (all resolutions)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample resolutions
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import _extract as extract_pdf_text  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TJ.SupremeCourt-Plenum")

SOURCE_ID = "TJ/SupremeCourt-Plenum"
BASE_URL = "https://sud.tj"
SECTION_URL = f"{BASE_URL}/sanadho/karorhoi-plenumi-sudi-oli/{{slug}}/"

# slug -> (section key, English gloss of the section)
SECTIONS = {
    "pgo": ("civil", "Civil and family cases"),
    "pj": ("criminal", "Criminal cases"),
    "mfs": ("judicial_practice", "Questions of judicial activity"),
}

USER_AGENT = (
    "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://legaldatahunter.com)"
)
REQUEST_DELAY = 1.0
MAX_RETRIES = 5
MIN_TEXT_CHARS = 500

TAG_RE = re.compile(r"<[^>]+>")
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)
LINK_RE = re.compile(
    r'<a[^>]*href="([^"]*/plenum/[^"]+\.pdf)"[^>]*>(.*?)</a>', re.S | re.I
)
# "аз 29.05.2003" and the stray-space variant "аз 29.09. 2014"
DATE_RE = re.compile(r"аз\s*(\d{1,2})[.\s]+(\d{1,2})[.\s]+((?:19|20)\d{2})")
NUMBER_RE = re.compile(r"№\s*(\d+)")


def strip_html(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", unescape(TAG_RE.sub(" ", value))).strip()


def clean_pdf_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_date(title: str) -> Optional[str]:
    m = DATE_RE.search(title)
    if not m:
        return None
    day, month, year = (int(g) for g in m.groups())
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def parse_number(title: str) -> Optional[str]:
    m = NUMBER_RE.search(title)
    return m.group(1) if m else None


class TJPlenumScraper(BaseScraper):
    """Scraper for the Plenum resolutions of the Supreme Court of Tajikistan."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/pdf,*/*;q=0.8",
                "Accept-Language": "tg,ru;q=0.8,en;q=0.6",
            }
        )
        self._short_text: list[str] = []

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str) -> requests.Response:
        delay = 2.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=180)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        "HTTP %s for %s — retry %s/%s in %.0fs",
                        resp.status_code, url, attempt, MAX_RETRIES, wait,
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                if getattr(exc.response, "status_code", None) == 404:
                    raise
                last_exc = exc
                logger.warning(
                    "Request error for %s — retry %s/%s in %.0fs (%s)",
                    url, attempt, MAX_RETRIES, delay, exc,
                )
                time.sleep(delay)
                delay = min(delay * 2, 120)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Exhausted retries for {url}")

    # --------------------------------------------------------------- listing

    def _section_items(self, slug: str) -> list[dict]:
        section_key, section_en = SECTIONS[slug]
        url = SECTION_URL.format(slug=slug)
        html = self._get(url).text

        h1 = H1_RE.search(html)
        section_tj = strip_html(h1.group(1)) if h1 else section_en

        items: list[dict] = []
        seen: set[str] = set()
        for href, anchor in LINK_RE.findall(html):
            # The hrefs are already percent-encoded; the Cyrillic filenames
            # carry spaces, so they are passed through byte-for-byte.
            pdf_url = urljoin(BASE_URL, href)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            title = strip_html(anchor)
            items.append(
                {
                    "pdf_url": pdf_url,
                    "title": title,
                    "section": section_key,
                    "section_tj": section_tj,
                    "section_en": section_en,
                    "section_url": url,
                    "date": parse_date(title),
                    "number": parse_number(title),
                }
            )

        if not items:
            raise RuntimeError(
                f"{url} listed no Plenum PDFs — the page layout changed or "
                "sud.tj is blocking this vantage"
            )
        logger.info("%s (%s): %s resolutions", slug, section_en, len(items))
        return items

    def fetch_all(self) -> Generator[dict, None, None]:
        seen: set[str] = set()
        total = 0
        for slug in SECTIONS:
            for item in self._section_items(slug):
                if item["pdf_url"] in seen:
                    continue
                seen.add(item["pdf_url"])
                total += 1
                yield item
            time.sleep(REQUEST_DELAY)
        logger.info("Plenum resolutions: %s in total", total)

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Resolutions adopted on or after ``since`` — the index is 3 pages."""
        if isinstance(since, datetime):
            since_iso = since.date().isoformat()
        else:
            since_iso = str(since)[:10]
        for item in self.fetch_all():
            if not item["date"] or item["date"] >= since_iso:
                yield item

    # ------------------------------------------------------------- normalize

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url")
        if not pdf_url:
            return None
        try:
            resp = self._get(pdf_url)
        except Exception as exc:  # noqa: BLE001 — logged, record skipped
            logger.warning("PDF download failed for %s: %s", pdf_url, exc)
            return None

        content = resp.content
        if not content.startswith(b"%PDF-"):
            logger.warning(
                "Not a PDF at %s (%s bytes, starts %r)",
                pdf_url, len(content), content[:16],
            )
            return None

        text = clean_pdf_text(extract_pdf_text(content))
        if len(text) < MIN_TEXT_CHARS:
            # A scan with no text layer — no OCR backend, so no full text.
            logger.warning(
                "Insufficient text for %s (%s chars) — skipped",
                raw.get("title"), len(text),
            )
            self._short_text.append(str(raw.get("title")))
            return None

        date, number = raw.get("date"), raw.get("number")
        if date and number:
            doc_id = f"tj-plenum-{date}-{number}"
        else:
            doc_id = "tj-plenum-" + re.sub(
                r"[^a-z0-9]+", "-", pdf_url.rsplit("/", 1)[-1].lower()
            ).strip("-")[:80]

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"][:500],
            "text": text,
            "date": date,
            "url": pdf_url,
            "country": "TJ",
            "language": "tg",
            "court": "Суди Олии Ҷумҳурии Тоҷикистон (Supreme Court of Tajikistan)",
            "publisher": "Пленуми Суди Олии Ҷумҳурии Тоҷикистон",
            "resolution_number": number,
            "section": raw.get("section"),
            "section_tj": raw.get("section_tj"),
            "section_en": raw.get("section_en"),
            "index_url": raw.get("section_url"),
        }

    # ------------------------------------------------------------------ test

    def test_api(self) -> bool:
        logger.info("Testing sud.tj Plenum sections ...")
        try:
            items = self._section_items("pgo")
            for item in items:
                rec = self.normalize(item)
                if rec and rec.get("text"):
                    logger.info(
                        "  %s OK — %s chars, date=%s, №%s",
                        rec["_id"], len(rec["text"]), rec["date"],
                        rec["resolution_number"],
                    )
                    logger.info("API test PASSED")
                    return True
            logger.error("  no resolution yielded full text")
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("API test FAILED: %s", exc)
            return False


def main():
    parser = argparse.ArgumentParser(description="TJ/SupremeCourt-Plenum bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api", "updates"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = TJPlenumScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        if scraper._short_text:
            logger.info("Resolutions with no text layer: %s", scraper._short_text)
        return

    if args.command == "updates":
        since = args.since or datetime.now(timezone.utc).date().isoformat()
        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                print(json.dumps(rec, ensure_ascii=False))
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")
    if scraper._short_text:
        logger.info("Resolutions with no text layer: %s", scraper._short_text)


if __name__ == "__main__":
    main()
