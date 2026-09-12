#!/usr/bin/env python3
"""
RU/Rostechnadzor — Federal Environmental, Industrial and Nuclear Supervision
Service (Федеральная служба по экологическому, технологическому и атомному
надзору, Ростехнадзор).

WHY NOT gosnadzor.ru
--------------------
The service's own site (https://www.gosnadzor.ru/) answers HTTP 403 to every
request from this vantage (bare `<h1>Forbidden</h1>` + a Request ID, i.e. an
edge WAF rule, not a page-level restriction).  publication.pravo.gov.ru — the
official publication portal — does carry the service's acts and is reachable,
but every act there is a scanned signature-page PDF with no text layer, so it
needs OCR.

WHAT THIS USES INSTEAD
----------------------
"Законодательство России" (the state's official legal-information retrieval
system, ИПС), served at http://pravo.gov.ru/proxy/ips/.  It publishes the
*consolidated* text of each act in HTML — the operative wording as currently
in force, with the amendment chain stated in the preamble — which is a
strictly better artefact than the signed PDF of the original act.

ACCESS PATH
-----------
Everything is GET, windows-1251, on http://pravo.gov.ru/proxy/ips/:

  1. Issuing-body classifier ids come from the card-search autocomplete
     (POST ?autocomplete&bpa=cd00000&nclassif=6&area=110, body `query=<prefix>`,
     answering `<label>\t<id>`; it matches on prefix only).  Three ids cover
     the whole Rostechnadzor lineage — see AUTHORITIES.

  2. Listing: ?list_itself=&bpas=cd00000&a6=<id>&a6type=1&a6value=<label>
              &flagFind=0&sort=7&start=<n>     (sort=7 == newest signature first)
     `top.listSize = N` in the response carries the hit count; rows are
     <table class="list_elem ..."> blocks carrying the ИПС document id (`nd`),
     the in-force state, the full act heading (type + authority + date +
     number), the subject line and the official-publication reference.
     `docs_per_page` is ignored — the page is always 20 rows, so paging walks
     `start` in steps of 20.

  3. Full text: ?doc_itself=&nd=<nd>&page=<k>.  Omitting `rdk` (the edition
     index) serves the current consolidated edition.  Long acts are split, so
     pages are walked until one repeats or comes back empty.

Everything is anonymous; no key, no cookie, no auth.

Usage:
  python bootstrap.py bootstrap --sample     # 15 sample records
  python bootstrap.py bootstrap --full       # full corpus
  python bootstrap.py bootstrap-fast         # high-throughput full pull (VPS)
  python bootstrap.py test-api               # connectivity + full-text check
"""

from __future__ import annotations

import html as html_mod
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RU.Rostechnadzor")

IPS = "http://pravo.gov.ru/proxy/ips/"
BPAS = "cd00000"  # "Законодательство России" collection
PAGE_SIZE = 20  # server-fixed; docs_per_page is ignored

# Issuing-body classifier ids (nclassif=6) for the Rostechnadzor lineage.
AUTHORITIES = [
    (
        "102000224",
        "Федеральная служба по экологическому, технологическому и атомному надзору",
    ),
    # Gosgortekhnadzor of Russia — the industrial/mining-safety predecessor
    # merged into Rostechnadzor in 2004.
    ("102000228", "Федеральный горный и промышленный надзор"),
    # Gosatomnadzor of Russia — the nuclear and radiation safety predecessor,
    # merged into Rostechnadzor in 2004.
    ("102000229", "Федеральный надзор по ядерной и радиационной безопасности"),
]

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# Rows are delimited by this comment. Splitting on it rather than on
# <table class="list_elem"> matters: each row nests a <table class="l_name">,
# so a non-greedy match to the first </table> stops before the row's footer
# (where the official-publication reference lives).
ROW_DELIM = "<!-- BEGIN элемент списка -->"
ND_RE = re.compile(r"[?&]nd=(\d+)")
STATE_RE = re.compile(
    r'<span class="tiny_italic_bold">\s*(?P<state>[^<]+?)\s*</span>', re.S
)
HEADING_RE = re.compile(
    r'<a id="link_\d+"[^>]*>\s*(?P<heading>.*?)\s*</a>', re.S
)
SUBJECT_RE = re.compile(r'<span class="bold">\s*(?P<subject>.*?)\s*</span>', re.S)
PUBLICATION_RE = re.compile(r"<li class='tiny'>\s*(?P<pub>.*?)\s*</li>", re.S)
LISTSIZE_RE = re.compile(r"top\.listSize\s*=\s*(\d+)")

# "Приказ Федеральной службы ... от 13.09.2004 № 28" / "... от 27.10.1960 б/н"
HEADING_PARSE_RE = re.compile(
    r"^(?P<kind>[А-ЯЁA-Z][^ ]*)\s+(?P<authority>.*?)\s+от\s+"
    r"(?P<date>\d{2}\.\d{2}\.\d{4})\s*(?:№\s*(?P<number>\S.*?)|б/н)?\s*$",
    re.S,
)

# Acts that merely approve a fee schedule or an internal staffing table are
# still normative, so nothing is filtered out on kind; the field is kept as
# metadata only.
DOCTRINE_KINDS = {"письмо", "информация", "разъяснение", "рекомендации"}


def _strip_tags(fragment: str) -> str:
    """Collapse an HTML fragment to plain text (entities decoded, tags gone)."""
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", fragment, flags=re.S)
    # ИПС nests a complete Word-exported HTML document inside #text_content, so
    # the act's own <head> is in the middle of the page. Left in, its <title>
    # ("Complex") and the `<!--[if gte mso 9]><xml><w:WordDocument>…` settings
    # block prepend "Complex Print false false false MicrosoftInternetExplorer4"
    # to the text of every act.
    text = re.sub(r"<head\b[^>]*>.*?</head\s*>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
    text = re.sub(r"<xml\b[^>]*>.*?</xml\s*>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"</(p|div|tr|h[1-6]|td|li)>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = text.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _inline(fragment: str) -> str:
    """Same as _strip_tags but for one-line values (headings, subjects)."""
    return re.sub(r"\s+", " ", _strip_tags(fragment)).strip()


class RostechnadzorScraper(BaseScraper):
    """Rostechnadzor normative acts, full consolidated text from ИПС."""

    def __init__(self, source_dir: str | None = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9",
            }
        )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    def _get(self, params: str, attempts: int = 4) -> str | None:
        """GET an ИПС query string and decode the windows-1251 body.

        The frame endpoints answer 500 while still returning a complete page,
        so the body is used whenever it is non-trivial regardless of status.
        """
        url = IPS + "?" + params
        for attempt in range(1, attempts + 1):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 204 or not resp.content:
                    return ""
                body = resp.content.decode("cp1251", "replace")
                if len(body) > 500 or resp.status_code == 200:
                    return body
                logger.warning(
                    f"Thin body ({resp.status_code}, {len(body)} chars) for {url}"
                )
            except Exception as exc:  # network flake / read timeout
                logger.warning(f"Error fetching {url} (attempt {attempt}): {exc}")
            if attempt < attempts:
                time.sleep(2 * attempt)
        return None

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------
    @staticmethod
    def _list_params(classifier_id: str, label: str, start: int) -> str:
        encoded_label = quote(label.encode("cp1251"))
        params = (
            f"list_itself=&bpas={BPAS}&a6={classifier_id}&a6type=1"
            f"&a6value={encoded_label}&flagFind=0&sort=7"
        )
        return params + (f"&start={start}" if start else "&page=first")

    def _parse_rows(self, html: str, classifier_id: str, label: str) -> list[dict]:
        rows = []
        for block in html.split(ROW_DELIM)[1:]:
            nd_match = ND_RE.search(block)
            if not nd_match:
                continue
            heading_match = HEADING_RE.search(block)
            heading = _inline(heading_match.group("heading")) if heading_match else ""
            # The subject line is the <span class="bold"> that follows the link.
            subject = ""
            tail = block[heading_match.end():] if heading_match else block
            subject_match = SUBJECT_RE.search(tail)
            if subject_match:
                subject = _inline(subject_match.group("subject"))
            state_match = STATE_RE.search(block)
            publication_match = PUBLICATION_RE.search(block)
            rows.append(
                {
                    "nd": nd_match.group(1),
                    "heading": heading,
                    "subject": subject,
                    "state": _inline(state_match.group("state")) if state_match else "",
                    "publication": (
                        _inline(publication_match.group("pub"))
                        if publication_match
                        else ""
                    ),
                    "classifier_id": classifier_id,
                    "classifier_label": label,
                }
            )
        return rows

    def _iter_listing(self) -> Generator[dict, None, None]:
        """Walk every issuing body's hit list, newest act first."""
        seen: set[str] = set()
        for classifier_id, label in AUTHORITIES:
            html = self._get(self._list_params(classifier_id, label, 0))
            if html is None:
                logger.error(f"Listing unavailable for {label} — skipping")
                continue
            size_match = LISTSIZE_RE.search(html)
            total = int(size_match.group(1)) if size_match else 0
            logger.info(f"{label}: {total} acts")
            start = 0
            while start < total:
                if start:
                    html = self._get(self._list_params(classifier_id, label, start))
                    if html is None:
                        logger.error(f"{label}: listing page start={start} failed")
                        break
                rows = self._parse_rows(html, classifier_id, label)
                if not rows:
                    logger.warning(f"{label}: no rows at start={start} — stopping")
                    break
                for row in rows:
                    if row["nd"] in seen:
                        continue
                    seen.add(row["nd"])
                    yield row
                start += PAGE_SIZE

    # ------------------------------------------------------------------
    # Full text
    # ------------------------------------------------------------------
    def _fetch_text(self, nd: str, max_pages: int = 60) -> str:
        """Consolidated text of act `nd`, concatenated across its pages."""
        chunks: list[str] = []
        seen_pages: set[str] = set()
        for page in range(1, max_pages + 1):
            html = self._get(f"doc_itself=&nd={nd}&page={page}")
            if not html:
                break
            body = html
            start = body.find("<body")
            if start >= 0:
                body = body[start:]
            text = _strip_tags(body)
            if not text:
                break
            fingerprint = text[:400]
            if fingerprint in seen_pages:
                break  # server clamps out-of-range pages to the last one
            seen_pages.add(fingerprint)
            chunks.append(text)
            # A short page means the act fits on it; nothing further to walk.
            if len(text) < 2000:
                break
        return "\n\n".join(chunks).strip()

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------
    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_listing()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Listings are sorted newest-signature-first, so stop once past `since`."""
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        stale = 0
        for row in self._iter_listing():
            date = self._parse_heading(row["heading"]).get("date")
            if date:
                signed = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
                if signed < since:
                    stale += 1
                    # Each authority restarts the sort, so tolerate a run of
                    # older acts before giving up on the whole sweep.
                    if stale > 200:
                        return
                    continue
            stale = 0
            yield row

    @staticmethod
    def _parse_heading(heading: str) -> dict:
        """Split "Приказ <authority> от DD.MM.YYYY № N" into its parts."""
        parsed: dict = {"kind": None, "authority": None, "date": None, "number": None}
        match = HEADING_PARSE_RE.match(heading.strip())
        if not match:
            return parsed
        parsed["kind"] = match.group("kind")
        parsed["authority"] = (match.group("authority") or "").strip()
        day, month, year = match.group("date").split(".")
        parsed["date"] = f"{year}-{month}-{day}"
        number = match.group("number")
        parsed["number"] = number.strip() if number else None
        return parsed

    def normalize(self, raw: dict) -> dict | None:
        nd = raw["nd"]
        text = self._fetch_text(nd)
        if len(text) < 400:
            logger.warning(f"nd={nd}: full text too short ({len(text)} chars) — skipped")
            return None

        parsed = self._parse_heading(raw["heading"])
        title = raw["subject"] or raw["heading"]
        kind = (parsed["kind"] or "").lower().rstrip("ы")
        doc_type = "doctrine" if kind in DOCTRINE_KINDS else "legislation"

        return {
            "_id": f"ips-{nd}",
            "_source": "RU/Rostechnadzor",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": parsed["date"],
            "url": f"{IPS}?docbody=&nd={nd}",
            "language": "ru",
            "country": "RU",
            "authority": parsed["authority"] or raw["classifier_label"],
            "issuing_body": raw["classifier_label"],
            "instrument_type": parsed["kind"],
            "instrument_number": parsed["number"],
            "heading": raw["heading"],
            "in_force_state": raw["state"],
            "official_publication": raw["publication"],
            "ips_id": nd,
        }

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    def test_api(self) -> bool:
        logger.info("Testing pravo.gov.ru ИПС access...")
        try:
            checked = 0
            for row in self._iter_listing():
                record = self.normalize(row)
                if record:
                    logger.info(
                        f"  {record['heading'][:70]!r} "
                        f"({len(record['text'])} chars, date={record['date']})"
                    )
                    checked += 1
                if checked >= 3:
                    break
            if checked < 3:
                logger.error("  Full-text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as exc:
            logger.error(f"API test FAILED: {exc}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="RU/Rostechnadzor bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = RostechnadzorScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
