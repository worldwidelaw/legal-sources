#!/usr/bin/env python3
"""
INTL/WENRA -- Western European Nuclear Regulators Association

Fetches the full text of the doctrine published by WENRA, the association of
the heads of the nuclear safety regulatory authorities of the European
countries with nuclear power plants (plus associated members). WENRA writes
the **Safety Reference Levels** that national regulators then transpose into
their own binding frameworks, so its output is the de-facto harmonised
European nuclear-safety rulebook:

  * Safety Reference Levels (SRLs) for existing reactors, research reactors,
    waste storage, disposal and decommissioning, their successive revisions
    (2008, 2014, 2020 …) and the periodic "Status of the implementation of
    the SRLs in national regulatory frameworks" reports, which record
    country by country how far each member has transposed them.
  * Working-group reports and guidance — RHWG (Reactor Harmonisation),
    WGWD (Waste and Decommissioning), WGRR (Research Reactors), WGRSM
    (Radiation Sources and Materials), WIG (Inspection) — including the
    safety objectives for new NPP designs, periodic safety review, design
    extension conditions, fuel licensing, and the guidelines on external
    hazards (seismic, flooding, aircraft crash).
  * WENRA position papers, statements and recommendations, including the
    ENSREG-WENRA, ENSRA-WENRA and HERCA-WENRA joint statements and the
    series of positions on the safety of Ukrainian nuclear installations.
  * Governance instruments: Terms of Reference, strategy documents and
    Topical Peer Review specifications.
  * The news items in which WENRA announces and explains those positions —
    frequently the only full-text form a statement takes.

Strategy — three complementary streams, de-duplicated on the PDF filename:

  1. **Publications view** (``https://wenra.eu/publications``). A Drupal 9
     view listing every published document on a single page (no pager) with
     its title, the issuing working group and the publication timestamp.
     Each row links a born-digital PDF under ``/sites/default/files/``;
     text is extracted with the shared ``common.pdf_extract`` helper. The
     thematic pages (/ukraine, /workinggroups, /about) are swept for the
     handful of PDFs the view does not list.

  2. **News nodes** (``https://wenra.eu/news-archive`` → ``/node/{id}``).
     The body of each node carries the full text of the announcement or
     position; it is read from the ``node__content`` container.

  3. **Internet Archive backfill**. WENRA moved from wenra.org to wenra.eu
     and the old document tree is gone, so historical material the current
     view no longer lists (the 2007/2008 lists of Reference Levels, the
     2009/2011/2013/2014 RHWG reports, older harmonisation reports) and
     documents since withdrawn from wenra.eu survive only in the Wayback
     Machine. Both domains are enumerated via the CDX API and replayed with
     ``/web/{ts}id_/``. Rows whose archived status is 4xx/5xx are dropped
     while revisit rows (statuscode "-") are KEPT — a CDX ``statuscode:200``
     filter silently hides revisits and would truncate the corpus.

  Everything WENRA publishes is guidance addressed to regulators rather than
  binding law or adjudication, so every record is typed ``doctrine``.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import html as html_mod
import json
import logging
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WENRA")

SOURCE_ID = "INTL/WENRA"
BASE_URL = "https://wenra.eu"
PUBLICATIONS_URL = f"{BASE_URL}/publications"
NEWS_URL = f"{BASE_URL}/news-archive"
EXTRA_PAGES = ("/ukraine", "/workinggroups", "/about")

CDX_URL = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "https://web.archive.org/web/{ts}id_/{url}"
ARCHIVE_DOMAINS = ("wenra.org", "wenra.eu")

MIN_PDF_TEXT = 400   # below this a PDF is a cover sheet or a scanned logo
MIN_NEWS_TEXT = 300  # below this a news node is a one-line pointer

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# "26 May 2026, 12:47 pm" (publications view) / "18 Jun 2026 | 07:23 am" (news)
DMY_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\.?\s+(\d{4})")
# Dates encoded in a PDF filename: 2026-03-31, 20260331, 10_August_2022, 2014.
FILE_ISO_RE = re.compile(r"(19\d{2}|20\d{2})[-_](\d{2})[-_](\d{2})")
FILE_COMPACT_RE = re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)")
FILE_MONTH_RE = re.compile(
    r"(?:(\d{1,2})[-_ ])?"
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[-_ ]?"
    r"(19\d{2}|20\d{2})",
    re.I,
)
FILE_YEAR_RE = re.compile(r"(?<!\d)(19[89]\d|20[0-4]\d)(?!\d)")

# Working group inferred from the filename, for archived documents that carry
# no view metadata.
WG_FROM_NAME = (
    (re.compile(r"\brhwg\b", re.I), "Reactor Harmonisation Working Group (RHWG)"),
    (re.compile(r"\bwgwd\b", re.I), "Working Group on Waste and Decommissioning (WGWD)"),
    (re.compile(r"\bwgrr\b", re.I), "Working Group on Research Reactors (WGRR)"),
    (re.compile(r"\bwgrsm\b", re.I),
     "Working Group on Radiation Sources and Materials (WGRSM)"),
    (re.compile(r"\bwg?ip?\b|\binspection\b", re.I),
     "Working Group on Inspection (WIG)"),
)

# Site furniture that must not end up in a news record's text.
NEWS_CHROME_RE = re.compile(
    r"^(to news archive|to the publication|to the document|back|read more|"
    r"more|\(© wenra\)|© wenra)$",
    re.I,
)
# CVs, presentations and meeting-logistics PDFs from the legacy site.
ARCHIVE_SKIP_RE = re.compile(
    r"(^cv[_%\-]|presentation|agenda|invitation|registration|"
    r"participants?[-_ ]?list|hotel|logo)",
    re.I,
)

TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
BLOCK_END_RE = re.compile(r"</(p|div|li|h[1-6]|tr)>", re.I)


def strip_html(fragment: str) -> str:
    """Visible text of an HTML fragment, keeping block boundaries as newlines."""
    fragment = SCRIPT_RE.sub(" ", fragment)
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    fragment = BLOCK_END_RE.sub("\n", fragment)
    text = html_mod.unescape(TAG_RE.sub(" ", fragment))
    text = text.replace("\xa0", " ").replace("­", "")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.split("\n")]
    return "\n".join(ln for ln in lines if ln)


def clean_text(text: str) -> str:
    text = (text or "").replace("\x00", " ").replace("­", "")
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_dmy(value: str) -> str | None:
    m = DMY_RE.search(value or "")
    if not m:
        return None
    day, mon, year = int(m.group(1)), MONTHS.get(m.group(2).lower()), int(m.group(3))
    if not mon or not 1 <= day <= 31 or not 1990 <= year <= 2100:
        return None
    return f"{year:04d}-{mon:02d}-{day:02d}"


def filename_date(name: str) -> str | None:
    """Publication date encoded in a PDF filename, if any."""
    name = urllib.parse.unquote(name)
    m = FILE_ISO_RE.search(name)
    if m and 1 <= int(m.group(2)) <= 12 and 1 <= int(m.group(3)) <= 31:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = FILE_COMPACT_RE.search(name)
    if m and 1 <= int(m.group(2)) <= 12 and 1 <= int(m.group(3)) <= 31:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = FILE_MONTH_RE.search(name)
    if m:
        day = int(m.group(1)) if m.group(1) else 1
        if 1 <= day <= 31:
            return f"{int(m.group(3)):04d}-{MONTHS[m.group(2).lower()[:3]]:02d}-{day:02d}"
    m = FILE_YEAR_RE.search(name)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def wayback_date(ts: str) -> str | None:
    if ts and len(ts) >= 8 and ts[:8].isdigit():
        return f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
    return None


def pdf_filename(url: str) -> str:
    return urllib.parse.unquote(url.split("?")[0]).rsplit("/", 1)[-1]


def pdf_key(url: str) -> str:
    """De-duplication key for a PDF: its decoded filename stem, alphanumeric only."""
    stem = re.sub(r"\.pdf.*$", "", pdf_filename(url), flags=re.I)
    return re.sub(r"[^a-z0-9]+", "", stem.lower())


ACRONYM_RE = re.compile(
    r"\b(wenra|rhwg|wgwd|wgrr|wgrsm|wig|ensreg|ensra|herca|iaea|tpr|npp|npps|"
    r"srl|srls|rl|rls|psr|psa|smr|smrs|ltn|rpv|eu|nw)\b",
    re.I,
)


def prettify(url: str) -> str:
    """Readable title from a PDF filename, for documents with no view metadata."""
    stem = re.sub(r"\.pdf.*$", "", pdf_filename(url), flags=re.I)
    stem = re.sub(r"[_\-%]+", " ", stem)
    # Leading/trailing date stamps ("2016 10 28 tpr …", "… final 2014 12 19").
    stem = re.sub(r"^\s*(19|20)\d{2}([ _-]\d{1,2}){0,2}\s+", "", stem)
    stem = re.sub(r"\s+(19|20)\d{2}([ _-]\d{1,2}){0,2}\s*$", "", stem)
    stem = re.sub(r"\s+", " ", stem).strip(" -–—_")
    if not stem:
        return pdf_filename(url)
    stem = stem[0].upper() + stem[1:]
    stem = ACRONYM_RE.sub(lambda m: m.group(0).upper(), stem)
    return stem[:300]


def working_group_from_name(name: str) -> str | None:
    for pattern, label in WG_FROM_NAME:
        if pattern.search(name):
            return label
    return None


def slugify(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")[:180]


class WENRAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 0.5
        # Filled by _enumerate(); a news node that merely announces a PDF
        # already in the corpus is dropped in favour of the PDF itself.
        self._publication_keys: set[str] = set()

    # ---- low-level fetch ----------------------------------------------------

    def _get(self, url: str, retries: int = 3, timeout: int = 90) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code in (403, 404, 410):
                    logger.debug(f"HTTP {resp.status_code} for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_html(self, url: str) -> str | None:
        raw = self._get(url)
        return raw.decode("utf-8", "replace") if raw else None

    # ---- stream 1: the publications view ------------------------------------

    def _list_publications(self) -> list[dict]:
        page = self._get_html(PUBLICATIONS_URL)
        if not page:
            raise RuntimeError(
                f"{PUBLICATIONS_URL} unreachable — cannot enumerate WENRA publications"
            )
        rows = re.split(r"views-row clearfix row-\d+", page)[1:]
        out = []
        for row in rows:
            link = re.search(
                r'views-field-field-document.*?<a href="([^"]+\.pdf[^"]*)"',
                row,
                re.S | re.I,
            )
            if not link:
                continue
            title_m = re.search(
                r'views-field-title"><h2 class="field-content">(.*?)</h2>', row, re.S
            )
            wg_m = re.search(
                r'views-field-field-working-group.*?'
                r'<span class="field-content">(.*?)</span>',
                row,
                re.S,
            )
            date_m = re.search(
                r'views-field-field-published-on.*?'
                r'<span class="field-content">(.*?)</span>',
                row,
                re.S,
            )
            url = urllib.parse.urljoin(BASE_URL, html_mod.unescape(link.group(1)))
            title = strip_html(title_m.group(1)) if title_m else prettify(url)
            working_group = strip_html(wg_m.group(1)) if wg_m else ""
            out.append(
                {
                    "kind": "publication",
                    "pdf_url": url,
                    "title": re.sub(r"\s+", " ", title).strip()[:500],
                    "working_group": working_group or working_group_from_name(url),
                    "date": parse_dmy(strip_html(date_m.group(1)) if date_m else ""),
                    "key": pdf_key(url),
                }
            )
        if not out:
            raise RuntimeError(
                "WENRA publications view returned no document rows — "
                "layout changed or the site is blocking this vantage"
            )
        logger.info(f"{len(out)} documents listed on the WENRA publications view")
        return out

    def _extra_page_pdfs(self, seen: set[str]) -> list[dict]:
        """PDFs linked from the thematic pages but absent from the view."""
        out = []
        for path in EXTRA_PAGES:
            page = self._get_html(BASE_URL + path)
            if not page:
                continue
            for href in re.findall(r'href="([^"]+\.pdf[^"]*)"', page, re.I):
                url = urllib.parse.urljoin(BASE_URL, html_mod.unescape(href))
                if "/sites/default/files/" not in url:
                    continue
                key = pdf_key(url)
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "kind": "publication",
                        "pdf_url": url,
                        "title": prettify(url),
                        "working_group": working_group_from_name(url),
                        "date": filename_date(pdf_filename(url)),
                        "key": key,
                    }
                )
        if out:
            logger.info(f"{len(out)} additional PDFs linked from the thematic pages")
        return out

    # ---- stream 2: news nodes -----------------------------------------------

    def _list_news(self) -> list[dict]:
        page = self._get_html(NEWS_URL)
        if not page:
            logger.warning(f"{NEWS_URL} unreachable — skipping the news stream")
            return []
        rows = re.split(r"views-row clearfix row-\d+", page)[1:]
        out, seen_nodes = [], set()
        for row in rows:
            link = re.search(
                r'views-field-title".*?<a href="(/node/\d+)"[^>]*>(.*?)</a>', row, re.S
            )
            if not link or link.group(1) in seen_nodes:
                continue
            seen_nodes.add(link.group(1))
            date_m = re.search(r'page-postdate-news">(.*?)</div>', row, re.S)
            out.append(
                {
                    "kind": "news",
                    "node": link.group(1),
                    "url": BASE_URL + link.group(1),
                    "title": re.sub(r"\s+", " ", strip_html(link.group(2))).strip()[:500],
                    "date": parse_dmy(strip_html(date_m.group(1)) if date_m else ""),
                    "working_group": None,
                }
            )
        logger.info(f"{len(out)} news items listed in the WENRA news archive")
        return out

    # ---- stream 3: Wayback backfill -----------------------------------------

    def _list_archive(self, seen: set[str]) -> list[dict]:
        best: dict[str, dict] = {}
        for domain in ARCHIVE_DOMAINS:
            params = {
                "url": domain,
                "matchType": "domain",
                "filter": "urlkey:.*\\.pdf",
                "collapse": "urlkey",
                "fl": "original,timestamp,statuscode",
                "output": "text",
                "limit": "20000",
            }
            try:
                resp = self.session.get(CDX_URL, params=params, timeout=180)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Wayback CDX unavailable for {domain}: {e}")
                continue
            for line in resp.text.splitlines():
                parts = line.split()
                if len(parts) < 3:
                    continue
                orig, ts, status = parts[0], parts[1], parts[2]
                # Keep 200 and revisit rows ("-"); a statuscode:200 CDX filter
                # hides revisits and silently truncates the corpus.
                if status not in ("200", "-"):
                    continue
                name = pdf_filename(orig)
                if not re.search(r"\.pdf$", name, re.I) or ARCHIVE_SKIP_RE.search(name):
                    continue
                key = pdf_key(orig)
                if not key or key in seen:
                    continue
                cur = best.get(key)
                if cur is None:
                    best[key] = {"orig": orig, "ts": ts, "first_ts": ts}
                else:
                    if ts > cur["ts"]:
                        cur["orig"], cur["ts"] = orig, ts
                    cur["first_ts"] = min(cur["first_ts"], ts)

        out = []
        for key, rec in sorted(best.items()):
            out.append(
                {
                    "kind": "archive",
                    "orig": rec["orig"],
                    "ts": rec["ts"],
                    "first_ts": rec["first_ts"],
                    "title": prettify(rec["orig"]),
                    "working_group": working_group_from_name(rec["orig"]),
                    "date": filename_date(pdf_filename(rec["orig"]))
                    or wayback_date(rec["first_ts"]),
                    "key": key,
                }
            )
        logger.info(
            f"{len(out)} withdrawn/legacy PDFs recovered from the Internet Archive"
        )
        return out

    # ---- enumeration --------------------------------------------------------

    def _enumerate(self) -> list[dict]:
        publications = self._list_publications()
        seen = {p["key"] for p in publications}
        publications += self._extra_page_pdfs(seen)
        self._publication_keys = set(seen)
        news = self._list_news()
        archive = self._list_archive(seen)

        # Round-robin the three streams so a truncated run — or the 12-record
        # sample — spans current publications, news statements and the
        # historical archive instead of draining one stream.
        streams = [s for s in (publications, news, archive) if s]
        records: list[dict] = []
        for i in range(max((len(s) for s in streams), default=0)):
            for stream in streams:
                if i < len(stream):
                    records.append(stream[i])
        logger.info(f"{len(records)} WENRA documents to fetch")
        return records

    # ---- text extraction ----------------------------------------------------

    def _pdf_text(self, pdf_bytes: bytes, doc_id: str) -> str:
        try:
            text = extract_pdf_markdown(
                SOURCE_ID, doc_id, pdf_bytes=pdf_bytes, table="doctrine", force=True
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for {doc_id}: {e}")
            return ""
        return clean_text(text)

    @staticmethod
    def _node_body(page: str) -> tuple[str, str | None, str | None]:
        """(body text, headline, postdate) from a WENRA news node page."""
        m = re.search(r'<div class="node__content">(.*?)</article>', page, re.S)
        if not m:
            return "", None, None
        content = m.group(1)
        headline_m = re.search(r'<div class="headline">(.*?)</div>', content, re.S)
        postdate_m = re.search(r'<div class="postdate">(.*?)</div>', content, re.S)
        body = re.sub(
            r'<div class="(headline|postdate)">.*?</div>', "", content, flags=re.S
        )
        lines = [
            ln for ln in strip_html(body).split("\n") if not NEWS_CHROME_RE.match(ln)
        ]
        return (
            clean_text("\n".join(lines)),
            strip_html(headline_m.group(1)).strip() if headline_m else None,
            strip_html(postdate_m.group(1)).strip() if postdate_m else None,
        )

    # ---- normalize ----------------------------------------------------------

    def _record(self, raw: dict, doc_id: str, text: str, url: str, **extra) -> dict:
        rec = {
            "_id": f"{SOURCE_ID}/{doc_id}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": (raw.get("title") or prettify(url))[:300],
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "working_group": raw.get("working_group") or None,
            "document_kind": raw["kind"],
            "language": "en",
            "authority": "Western European Nuclear Regulators Association (WENRA)",
            "archive_url": None,
        }
        rec.update(extra)
        return rec

    def normalize(self, raw: dict) -> dict | None:
        kind = raw.get("kind")

        if kind == "publication":
            url = raw["pdf_url"]
            doc_id = slugify(raw["key"] or url)
            pdf = self._get(url)
            if not pdf or pdf[:5] != b"%PDF-":
                logger.debug(f"No PDF bytes for {url}")
                return None
            text = self._pdf_text(pdf, doc_id)
            if len(text) < MIN_PDF_TEXT:
                logger.debug(f"Short/empty text for {url} ({len(text)} chars)")
                return None
            raw = dict(raw)
            raw["date"] = raw.get("date") or filename_date(pdf_filename(url))
            return self._record(raw, doc_id, text, url, format="pdf")

        if kind == "news":
            page = self._get_html(raw["url"])
            if not page:
                return None
            # Most statements are announced as a news item and published as a
            # PDF; keep only the PDF so the corpus carries one copy.
            linked = {
                pdf_key(href)
                for href in re.findall(r'href="([^"]+\.pdf[^"]*)"', page, re.I)
            }
            if linked & self._publication_keys:
                logger.debug(f"Skipping {raw['url']} — covered by its publication PDF")
                return None
            text, headline, postdate = self._node_body(page)
            if len(text) < MIN_NEWS_TEXT:
                logger.debug(f"Short news node {raw['url']} ({len(text)} chars)")
                return None
            raw = dict(raw)
            raw["title"] = headline or raw["title"]
            raw["date"] = raw.get("date") or parse_dmy(postdate or "")
            doc_id = slugify(f"news_{raw['node'].rsplit('/', 1)[-1]}")
            return self._record(raw, doc_id, text, raw["url"], format="html")

        if kind == "archive":
            archive_url = WB_RAW.format(ts=raw["ts"], url=raw["orig"])
            doc_id = slugify(f"legacy_{raw['key']}")
            pdf = self._get(archive_url)
            if not pdf or pdf[:5] != b"%PDF-":
                logger.debug(f"No archived PDF bytes for {raw['orig']}")
                return None
            text = self._pdf_text(pdf, doc_id)
            if len(text) < MIN_PDF_TEXT:
                logger.debug(f"Short/empty archived text for {raw['orig']}")
                return None
            return self._record(
                raw, doc_id, text, raw["orig"], format="pdf", archive_url=archive_url
            )

        return None

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing WENRA enumeration and full-text extraction…")
        try:
            records = self._enumerate()
            if len(records) < 50:
                logger.error(f"  Too few documents enumerated: {len(records)}")
                return False
            got: dict[str, dict] = {}
            for raw in records:
                if len(got) >= 3:
                    break
                if raw["kind"] in got:
                    continue
                rec = self.normalize(raw)
                if rec:
                    got[raw["kind"]] = rec
                    logger.info(
                        f"  {raw['kind']}: {rec['title'][:70]!r} "
                        f"({len(rec['text'])} chars, date={rec['date']})"
                    )
            if not got:
                logger.error("  Full-text extraction failed for every stream")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._enumerate()

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        cutoff = since.strftime("%Y-%m-%d") if hasattr(since, "strftime") else since
        for raw in self._enumerate():
            date = raw.get("date")
            if not cutoff or not date or date >= cutoff:
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WENRA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WENRAScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
