#!/usr/bin/env python3
"""
KZ/Adilet -- Kazakhstan Legal Information System "Әділет" (Ministry of Justice)

Consolidated Kazakh legislation (codes, laws, presidential decrees, government
resolutions, ministerial orders, akimat decisions, Constitutional Court
normative rulings) with full consolidated text.

Strategy (rebuilt 2026-08-21, issue #1471):
  The zan.gov.kz REST API (POST /api/documents/search, GET /api/documents/{id}/rus)
  now returns nginx 403 to every request from every vantage we have, so the old
  JSON path yielded 0 records. The public portal host adilet.zan.kz is NOT
  blocked, so discovery and full text are both read from it instead.

  - Discovery: the date-browse index exposes an RSS rendering,
      /rus/index/docs/dt={YEAR}-&rss=true&page={N}
    10 items per page, paginated to the end of the year, with the total count in
    <description>. This path is allowed by robots.txt (only /rus/search/,
    /rus/list/docs/ and /rus/archive/ are disallowed).
  - Full text: GET /rus/docs/{code} returns the whole consolidated act as HTML in
    a <div class="container_gamma text ..."> block (886K chars for the Criminal
    Code). Extracted with a balanced-div scan + tag strip.

Endpoints:
  - Year index (RSS): https://adilet.zan.kz/rus/index/docs/dt={year}-&rss=true&page={n}
  - Document:         https://adilet.zan.kz/rus/docs/{code}

Data:
  - ~206,000 documents, 1947-present. Language: Russian (Kazakh at /kaz/docs/{code}).

Usage:
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap --full   # Full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast     # Full corpus, concurrent full-text
  python bootstrap.py update             # Incremental (current + previous year)
  python bootstrap.py test               # Connectivity check
"""

import argparse
import html as html_module
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from xml.etree import ElementTree

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KZ.Adilet")

BASE_URL = "https://adilet.zan.kz"
INDEX_URL = BASE_URL + "/rus/index/docs/dt={year}-&rss=true&page={page}"
DOC_URL = BASE_URL + "/rus/docs/{code}"

# The date index starts in 1947; keep a floor well below it so a widened
# upstream archive is still picked up. Years with no documents cost one request.
FIRST_YEAR = 1940
MIN_TEXT_CHARS = 50

MONTHS_EN = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# Genitive forms as they appear in the requisites line ("от 29 декабря 1995 г.").
MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11,
    "декабря": 12,
}


class AdiletScraper(BaseScraper):
    """Kazakhstan legislation from adilet.zan.kz."""

    def __init__(self, source_dir: Optional[str] = None):
        # source_dir optional so the VPS bootstrap-fast wrapper can construct
        # this class by introspection.
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru,kk;q=0.8,en;q=0.5",
            },
            timeout=90,
        )

    # ── HTTP helpers (fail loud) ──────────────────────────────────────

    def _get(self, url: str, expect: str = "") -> str:
        """GET a URL, asserting status and content-type before returning body.

        The old scraper called .json() on whatever came back, so a 403 HTML
        error page looked like a transient parse failure and the run silently
        finished with 0 records (issue #1471). Everything here raises instead.
        """
        resp = self.client.get(url)
        if resp.status_code != 200:
            raise RuntimeError(
                f"HTTP {resp.status_code} from {url} "
                f"(body starts: {resp.text[:120]!r})"
            )
        ctype = resp.headers.get("Content-Type", "")
        if expect and expect not in ctype:
            raise RuntimeError(
                f"Unexpected Content-Type {ctype!r} from {url} "
                f"(body starts: {resp.text[:120]!r})"
            )
        return resp.text

    # ── Discovery ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_pubdate(value: str) -> Optional[str]:
        """'Fri, 27 Dec 2024 00:00:00 +0600' -> '2024-12-27'."""
        if not value:
            return None
        m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})", value)
        if not m:
            return None
        day, mon, year = int(m.group(1)), MONTHS_EN.get(m.group(2)), int(m.group(3))
        if not mon:
            return None
        return f"{year:04d}-{mon:02d}-{day:02d}"

    @staticmethod
    def _parse_adoption_date(requisites: str) -> Optional[str]:
        """Adoption date from the requisites line ('... от 29 декабря 1995 г. N 2737').

        <pubDate> in the RSS index carries the *last revision* date, so the 1995
        Constitution comes back stamped 2026. The requisites line carries the
        date the act was actually adopted, which is what `date` should hold.
        """
        if not requisites:
            return None
        m = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})", requisites, re.IGNORECASE)
        if not m:
            return None
        mon = MONTHS_RU.get(m.group(2).lower())
        if not mon:
            return None
        day, year = int(m.group(1)), int(m.group(3))
        if not (1 <= day <= 31 and 1900 <= year <= 2100):
            return None
        return f"{year:04d}-{mon:02d}-{day:02d}"

    def _index_page(self, year: int, page: int) -> Dict[str, Any]:
        """Fetch one RSS index page. Returns {'total': int, 'items': [...]}."""
        url = INDEX_URL.format(year=year, page=page)
        self.rate_limiter.wait()
        body = self._get(url)

        total = None
        m = re.search(r"Найдено\s+(\d+)\s+документ", body)
        if m:
            total = int(m.group(1))

        items: List[Dict[str, Any]] = []
        try:
            root = ElementTree.fromstring(body.encode("utf-8", "surrogatepass"))
        except ElementTree.ParseError as exc:
            raise RuntimeError(f"Index page {url} is not parseable RSS: {exc}")

        for item in root.iter("item"):
            link = (item.findtext("link") or "").strip()
            code_m = re.search(r"/docs/([A-Za-z0-9_]+)", link)
            if not code_m:
                continue
            items.append({
                "code": code_m.group(1),
                "title": html_module.unescape((item.findtext("title") or "").strip()),
                "summary": html_module.unescape((item.findtext("description") or "").strip()),
                "date": self._parse_pubdate(item.findtext("pubDate") or ""),
                "url": link or DOC_URL.format(code=code_m.group(1)),
                "year": year,
            })

        return {"total": total, "items": items}

    def _iter_year(self, year: int, skip_seen: bool = True) -> Generator[dict, None, None]:
        """Yield index entries for one calendar year, newest first."""
        first = self._index_page(year, 1)
        total = first["total"] or 0
        if total == 0:
            logger.debug("%d: no documents", year)
            return
        logger.info("%d: %d documents in index", year, total)

        page = 1
        seen_codes = set()
        emitted = 0
        payload = first
        while True:
            items = payload["items"]
            if not items:
                break
            fresh = [it for it in items if it["code"] not in seen_codes]
            if not fresh:
                # Pagination stopped advancing — stop rather than loop forever.
                logger.warning("%d: page %d repeated known codes, stopping", year, page)
                break
            for it in fresh:
                seen_codes.add(it["code"])
                emitted += 1
                if skip_seen and self.storage.exists(it["code"]):
                    continue
                yield it
            page += 1
            payload = self._index_page(year, page)

        if emitted < total:
            self.record_coverage_gap(
                str(year),
                "index_pagination_short",
                expected=total,
                got=emitted,
            )
            logger.warning("%d: index yielded %d of %d documents", year, emitted, total)

    def _years(self) -> List[int]:
        """Years present in the date index, newest first."""
        this_year = datetime.now(timezone.utc).year
        return list(range(this_year + 1, FIRST_YEAR - 1, -1))

    def fetch_all(self) -> Generator[dict, None, None]:
        """Walk the whole date index. Full text is downloaded in normalize()
        so bootstrap_fast's worker pool can overlap the per-document fetches."""
        found_any = False
        for year in self._years():
            for entry in self._iter_year(year):
                found_any = True
                yield entry
        if not found_any and self.storage.count() == 0:
            raise RuntimeError(
                "Date index returned no documents for any year — adilet.zan.kz "
                "layout changed or this vantage is blocked. Refusing to report success."
            )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Documents adopted on or after `since` (walks current + prior years)."""
        since_str = since.strftime("%Y-%m-%d")
        for year in range(datetime.now(timezone.utc).year + 1, since.year - 1, -1):
            for entry in self._iter_year(year, skip_seen=False):
                if entry["date"] and entry["date"] < since_str:
                    continue
                yield entry

    # ── Full text ─────────────────────────────────────────────────────

    @staticmethod
    def _extract_body(page_html: str) -> str:
        """Pull the act text out of <div class="container_gamma text ...">."""
        m = re.search(r'<div[^>]*class="[^"]*container_gamma\s+text[^"]*"[^>]*>', page_html)
        if not m:
            return ""
        start = m.end()
        depth = 1
        end = len(page_html)
        for tag in re.finditer(r"<(/?)div\b", page_html[start:]):
            depth += -1 if tag.group(1) else 1
            if depth == 0:
                end = start + tag.start()
                break
        body = page_html[start:end]

        body = re.sub(r"<(script|style)\b.*?</\1>", "", body, flags=re.S | re.I)
        body = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
        body = re.sub(r"</(p|div|tr|h[1-6]|li|table)>", "\n", body, flags=re.I)
        text = html_module.unescape(re.sub(r"<[^>]+>", "", body))
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    _TYPE_HINTS = (
        ("Конституция", "constitution"),
        ("Кодекс", "code"),
        ("Конституционный закон", "constitutional_law"),
        ("Закон", "law"),
        ("Указ", "presidential_decree"),
        ("Постановление Правительства", "government_resolution"),
        ("Нормативное постановление", "normative_ruling"),
        ("Постановление", "resolution"),
        ("Приказ", "order"),
        ("Распоряжение", "directive"),
        ("Решение", "decision"),
        ("Правила", "rules"),
    )

    @classmethod
    def _act_type(cls, summary: str, title: str) -> str:
        """Classify from the requisites line, which names the act's own type.

        The title must not be searched first: a Constitutional Court ruling
        titled "О рассмотрении на соответствие Конституции ... Кодекса" would
        otherwise be labelled a code.
        """
        for haystack in (summary, title):
            for needle, label in cls._TYPE_HINTS:
                if needle in haystack:
                    return label
        return "other"

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download the document page and build the standard record.

        Runs in bootstrap_fast worker threads, so the (slow) per-document fetch
        lives here rather than in fetch_all().
        """
        code = raw["code"]
        url = raw.get("url") or DOC_URL.format(code=code)
        self.rate_limiter.wait()
        page_html = self._get(DOC_URL.format(code=code), expect="text/html")
        text = self._extract_body(page_html)

        if len(text) < MIN_TEXT_CHARS:
            logger.warning("%s: text too short (%d chars), skipping", code, len(text))
            return None

        summary = raw.get("summary", "")
        num_m = re.search(r"[№N]\s*([^\s.,;]+(?:\s*-\s*[A-ZА-Я]+)?)", summary)
        adopted = self._parse_adoption_date(summary)

        return {
            "_id": code,
            "_source": "KZ/Adilet",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or summary or code,
            "text": text,
            "date": adopted or raw.get("date"),
            "revision_date": raw.get("date"),
            "url": url,
            "language": "ru",
            "code": code,
            "requisites": summary,
            "document_number": num_m.group(1).strip() if num_m else "",
            "act_type": self._act_type(summary, raw.get("title", "")),
            "year": raw.get("year"),
        }


def main():
    parser = argparse.ArgumentParser(description="KZ/Adilet fetcher")
    parser.add_argument("command", nargs="?", default="bootstrap",
                        choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch 15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch the whole corpus")
    parser.add_argument("--since", help="update: ISO date to fetch from")
    args = parser.parse_args()

    scraper = AdiletScraper(str(Path(__file__).parent))

    if args.command == "test":
        payload = scraper._index_page(datetime.now(timezone.utc).year, 1)
        logger.info("Index OK — %s documents this year, %d items on page 1",
                    payload["total"], len(payload["items"]))
        if not payload["items"]:
            sys.exit(1)
        rec = scraper.normalize(payload["items"][0])
        logger.info("Document OK — %s (%d chars)", rec["_id"], len(rec["text"]))

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info("Bootstrap-fast complete: %s", json.dumps(stats, indent=2, default=str))

    elif args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2, default=str))

    elif args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
