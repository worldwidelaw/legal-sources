#!/usr/bin/env python3
"""
US/MN-WCCA -- Minnesota Workers' Compensation Court of Appeals decisions.

The WCCA is a standing appellate court of record (Minn. Stat. s. 175A) that
reviews the decisions of compensation judges; its own decisions are reviewable
only by the Minnesota Supreme Court on certiorari. The court publishes its
opinions -- and the Supreme Court's workers' compensation opinions -- as static
documents under https://mn.gov/workcomp-stat/.

Enumeration uses the server's own recursive directory index,
`/workcomp-stat/.doclist.html`, which lists every published document (~4,250
files: year directories 1988-present plus a `sup/` directory of Supreme Court
opinions). For 1998-2001 the same decision is published twice -- as HTML and as
a redacted PDF -- so same-stem duplicates collapse to one record, preferring
HTML.

ACCESS NOTE: mn.gov sits behind Radware Bot Manager. A bare `requests`
User-Agent is answered with a validate.perfdrive.com JS interstitial (HTTP 200,
~21 KB, no decision content); a complete browser header set (UA + Accept +
Accept-Language + Sec-Fetch-* + sec-ch-ua) is served the real document. The
scraper always sends that header set and raises loudly if the interstitial
comes back anyway, so an IP-reputation block fails visibly instead of silently
yielding zero documents.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap          # sequential full pull
  python bootstrap.py bootstrap-fast     # concurrent full pull (VPS wrapper)
"""

import sys
import re
import html as html_lib
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MN-WCCA")

BASE_URL = "https://mn.gov/"
DOCLIST_URL = "https://mn.gov/workcomp-stat/.doclist.html"
MAX_ATTEMPTS = 6

# Internet Archive fallback. Radware's ban is per-egress-IP and sticky, so a
# blocked vantage reads whatever the Wayback Machine holds instead of returning
# nothing. The archive covers ~420 of the ~3,600 decisions, so this is a
# degraded mode, not a substitute for an unblocked vantage.
CDX_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_URL = "https://web.archive.org/web/{timestamp}id_/{url}"
# Consecutive live challenges, with no live success at all, before the scraper
# stops paying the retry cost on every single document.
LIVE_FAILURE_LATCH = 3

# Radware serves this JS interstitial instead of the document to clients it does
# not like; it is HTTP 200, so it has to be detected by content. Match on the
# <title> only: every genuine mn.gov page also carries a stormcaster.js snippet
# that mentions validate.perfdrive.com, so a body-wide match false-positives.
BOT_INTERSTITIAL_RE = re.compile(
    r"<title[^>]*>\s*Radware\b", re.I
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

HREF_RE = re.compile(r'href="([^"]+)"', re.I)
DOC_EXT_RE = re.compile(r"\.(html?|pdf)$", re.I)
# " - REDACTED", "-redacted", " redacted" -- the 1998-2001 PDF twins.
REDACTED_SUFFIX_RE = re.compile(r"[\s_-]*redacted[\s_-]*$", re.I)
META_DATE_RE = re.compile(
    r'<meta\s+name="(?:date|DC\.Date)"[^>]*content="(\d{4}-\d{2}-\d{2})"', re.I
)
META_DESC_RE = re.compile(
    r'<meta\s+name="(?:description|DC\.Description)"[^>]*content="([^"]*)"', re.I
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
H_RE = re.compile(r"<h([12])[^>]*>(.*?)</h\1>", re.I | re.S)
# Docket numbers: WCCA uses "WC18-6160" / "WC 18-6160"; the Supreme Court's
# workers' compensation files use the appellate "A19-0806" series, and older
# WCCA files use bare five-digit numbers.
DOCKET_RE = re.compile(r"\b(WC\s?\d{2}-\d{3,5}|A\d{2}-\d{3,5}|C\d-\d{2}-\d{3,6})\b", re.I)
# Dates baked into the filename: Name-01-04-19, Name - 07.15.24, Name-sup-09.
FILE_DATE_RE = re.compile(r"(\d{1,2})[.\-](\d{1,2})[.\-](\d{2}(?:\d{2})?)\b")
LONG_DATE_RE = re.compile(
    r"\b(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|"
    r"NOVEMBER|DECEMBER)\s+(\d{1,2}),?\s+(\d{4})\b", re.I
)
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}

# Decisions converted from WordPerfect keep their typographic punctuation as
# ASCII inside <span style='font-family:"WP TypographicSymbols"'>, so an
# apostrophe reaches the page as "=" and a section sign as "\'". Left as-is the
# text reads "the employee = s claim ... Minn. Stat. ' 176.141". Only the four
# glyphs observed across the corpus are mapped; anything else is left alone.
WP_SYMBOL_SPAN_RE = re.compile(
    r'<span[^>]*WP TypographicSymbols[^>]*>(.*?)</span>', re.I | re.S
)
WP_SYMBOLS = {"=": "\u2019", "A": "\u201c", "@": "\u201d", "'": "\u00a7"}

# The court's own banner line, which separates the case caption above it from
# the decision date and headnotes below.
COURT_BANNER_RE = re.compile(
    r"(WORKERS\S?\s*COMPENSATION COURT OF APPEALS|SUPREME COURT)", re.I
)

SUPREME_COURT = "Minnesota Supreme Court"
WCCA = "Minnesota Workers' Compensation Court of Appeals"


def decode_wp_symbols(page: str) -> str:
    """Replace WordPerfect symbol-font spans with the characters they stand for."""
    def replace(match: "re.Match") -> str:
        content = html_lib.unescape(re.sub(r"<[^>]+>", "", match.group(1)))
        return "".join(WP_SYMBOLS.get(ch, ch) for ch in content)
    return WP_SYMBOL_SPAN_RE.sub(replace, page)


def caption_from_text(text: str) -> Optional[str]:
    """Recover the case caption: the lines above the court's banner line."""
    lines = []
    for line in text.split("\n")[:12]:
        if COURT_BANNER_RE.search(line):
            break
        lines.append(line)
    caption = re.sub(r"\s+", " ", " ".join(lines)).strip()
    return caption[:600] or None


def strip_tags(fragment: str) -> str:
    """Collapse an HTML fragment to clean single-spaced text."""
    text = re.sub(r"<br\s*/?>", " ", fragment, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def html_to_text(page: str) -> str:
    """Extract the decision body from a workcomp-stat opinion page.

    The opinions are hand-authored XHTML: everything of interest is inside
    <body>, wrapped in <p>/<h1>/<h2>/<table> with no navigation chrome beyond
    the analytics and bot-manager <script> blocks in <head>.
    """
    page = decode_wp_symbols(page)
    body_match = re.search(r"<body[^>]*>(.*?)</body>", page, re.I | re.S)
    body = body_match.group(1) if body_match else page
    body = re.sub(r"<script\b[^>]*>.*?</script>", " ", body, flags=re.I | re.S)
    body = re.sub(r"<style\b[^>]*>.*?</style>", " ", body, flags=re.I | re.S)
    body = re.sub(r"<!--.*?-->", " ", body, flags=re.S)
    # Block-level tags become paragraph breaks so the text keeps its structure.
    body = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
    body = re.sub(
        r"</?(p|div|h[1-6]|tr|li|ul|ol|table|blockquote|hr)\b[^>]*>",
        "\n", body, flags=re.I,
    )
    body = re.sub(r"</?t[dh]\b[^>]*>", " ", body, flags=re.I)
    body = re.sub(r"<[^>]+>", " ", body)
    body = html_lib.unescape(body).replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in body.split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(ln for ln in lines if ln)).strip()


def parse_long_date(text: str) -> Optional[str]:
    match = LONG_DATE_RE.search(text)
    if not match:
        return None
    month = MONTHS[match.group(1).lower()]
    return f"{int(match.group(3)):04d}-{month:02d}-{int(match.group(2)):02d}"


def parse_file_date(filename: str) -> Optional[str]:
    """Recover an ISO date from names like `Franzen-Derrick-01-04-19.html`."""
    for month, day, year in FILE_DATE_RE.findall(filename):
        month, day = int(month), int(day)
        if not (1 <= month <= 12 and 1 <= day <= 31):
            continue
        year = int(year)
        if year < 100:
            # The archive starts in 1988, so a two-digit year above 87 is 19xx.
            year += 1900 if year >= 88 else 2000
        if 1970 <= year <= datetime.now(timezone.utc).year + 1:
            return f"{year:04d}-{month:02d}-{day:02d}"
    return None


def group_key(path: str) -> str:
    """Key that collapses an HTML opinion and its redacted-PDF twin."""
    directory, name = unquote(path).rsplit("/", 1)
    stem = DOC_EXT_RE.sub("", name)
    stem = REDACTED_SUFFIX_RE.sub("", stem)
    return directory.lower() + "/" + re.sub(r"[^a-z0-9]+", "", stem.lower())


def format_rank(path: str) -> int:
    """Prefer HTML over the scanned/redacted PDF of the same decision."""
    lower = path.lower()
    if lower.endswith(".html"):
        return 0
    if lower.endswith(".htm"):
        return 1
    return 2


class MNWCCAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(BROWSER_HEADERS)
        self.live_failures = 0
        self.live_successes = 0
        self.archive_index = {}

    @property
    def live_blocked(self) -> bool:
        return self.live_successes == 0 and self.live_failures >= LIVE_FAILURE_LATCH

    def _get_live(self, url: str, attempts: int = MAX_ATTEMPTS) -> Optional[requests.Response]:
        """GET mn.gov with backoff. Radware throttles bursts, so a challenged
        response is retried more patiently than a transport error."""
        delay = 5.0
        for attempt in range(attempts):
            challenged = False
            try:
                resp = self.session.get(url, timeout=90)
                resp.raise_for_status()
                if BOT_INTERSTITIAL_RE.search(resp.text[:2000]):
                    challenged = True
                    raise RuntimeError(
                        "mn.gov answered with the Radware bot-manager interstitial "
                        "instead of the document"
                    )
                self.live_successes += 1
                return resp
            except Exception as exc:
                logger.warning(f"GET {url} failed (attempt {attempt + 1}/{attempts}): {exc}")
            if attempt < attempts - 1:
                time.sleep(delay if challenged else 2.0)
                delay = min(delay * 2, 120.0)
        self.live_failures += 1
        return None

    def _cdx(self, url: str, match_type: str = "exact") -> list:
        params = {
            "url": url,
            "fl": "original,timestamp",
            "collapse": "urlkey",
            "filter": "statuscode:200",
            "limit": "100000",
        }
        if match_type != "exact":
            params["matchType"] = match_type
        for attempt in range(3):
            try:
                resp = requests.get(CDX_URL, params=params, timeout=180)
                resp.raise_for_status()
                return [line.split(" ") for line in resp.text.splitlines() if line.strip()]
            except Exception as exc:
                logger.warning(f"CDX {url} failed (attempt {attempt + 1}/3): {exc}")
                time.sleep(5)
        return []

    def _get_archived(self, url: str) -> Optional[requests.Response]:
        """Replay the newest successful Internet Archive capture of `url`."""
        timestamp = self.archive_index.get(url)
        if timestamp is None:
            rows = self._cdx(url)
            if not rows:
                return None
            timestamp = rows[-1][1]
            self.archive_index[url] = timestamp
        try:
            resp = requests.get(
                WAYBACK_URL.format(timestamp=timestamp, url=url), timeout=120
            )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            logger.warning(f"Wayback replay failed for {url}: {exc}")
            return None

    def _get(self, url: str) -> Optional[requests.Response]:
        """Live mn.gov first; the Internet Archive once the vantage is banned."""
        if not self.live_blocked:
            resp = self._get_live(url, attempts=2 if self.live_successes else MAX_ATTEMPTS)
            if resp is not None:
                return resp
            if not self.live_blocked:
                return None
            logger.warning(
                "mn.gov has blocked this egress IP; falling back to the Internet "
                "Archive, which holds only a fraction of the corpus. Re-run from a "
                "US residential vantage for the full ~3,600 decisions."
            )
        return self._get_archived(url)

    def _archived_urls(self) -> list:
        """Enumerate the decisions the Internet Archive actually holds.

        Replaying the archived copy of `.doclist.html` would name thousands of
        documents the archive never captured, so the CDX sweep is the index in
        degraded mode: every URL it returns is known-fetchable.
        """
        urls = []
        for row in self._cdx("mn.gov/workcomp-stat/", match_type="prefix"):
            url, timestamp = row[0], row[1]
            if "?" in url or not DOC_EXT_RE.search(url):
                continue
            if url.lower().endswith(".doclist.html"):
                continue
            self.archive_index[url] = timestamp
            urls.append(url)
        return urls

    def _list_documents(self) -> list:
        """Read the server-side directory index into one entry per decision."""
        resp = self._get_live(DOCLIST_URL)
        if resp is not None:
            hrefs = [urljoin(BASE_URL, h) for h in HREF_RE.findall(resp.text)]
        else:
            logger.warning(
                "The live decision index is unreachable (Radware). Falling back to "
                "the Internet Archive, which holds only a fraction of the corpus."
            )
            hrefs = self._archived_urls()
            if not hrefs:
                raise RuntimeError(
                    f"Could not read the decision index {DOCLIST_URL} live, and the "
                    f"Internet Archive returned nothing either. mn.gov serves the "
                    f"index to browser-shaped requests from unblocked vantages."
                )

        best = {}
        for href in hrefs:
            if not DOC_EXT_RE.search(href) or "/workcomp-stat/" not in href:
                continue
            if href.lower().endswith(".doclist.html"):
                continue
            key = group_key(href)
            if key not in best or format_rank(href) < format_rank(best[key]):
                best[key] = href

        documents = []
        for href in sorted(best.values()):
            path = unquote(href.split("/workcomp-stat/", 1)[1])
            name = path.rsplit("/", 1)[-1]
            directory = path.rsplit("/", 1)[0] if "/" in path else ""
            documents.append({
                "url": href,
                "path": path,
                "filename": name,
                "directory": directory,
                "is_pdf": href.lower().endswith(".pdf"),
            })
        logger.info(f"Decision index: {len(documents)} unique decisions")
        return documents

    def fetch_all(self) -> Generator[dict, None, None]:
        for entry in self._list_documents():
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Recent decisions only -- the index is cheap, the documents are not."""
        cutoff = since.strftime("%Y-%m-%d")
        for entry in self._list_documents():
            date = parse_file_date(entry["filename"])
            if date is None or date >= cutoff:
                yield entry

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["url"]
        path = raw["path"]
        filename = raw["filename"]

        title = None
        date = None
        summary = None
        parties = None
        heading = ""

        resp = self._get(url)
        if resp is None:
            logger.warning(f"Skipping unreachable decision: {url}")
            return None

        if raw["is_pdf"]:
            # The bytes are already in hand (possibly via the archive), so the
            # extractor must not re-download them from the blocked origin.
            text = extract_pdf_markdown(
                "US/MN-WCCA", raw["path"], pdf_bytes=resp.content
            ) or ""
        else:
            resp.encoding = resp.apparent_encoding or "utf-8"
            page = resp.text
            text = html_to_text(page)

            title_match = TITLE_RE.search(page)
            if title_match:
                title = strip_tags(title_match.group(1)) or None
            date_match = META_DATE_RE.search(page)
            if date_match:
                date = date_match.group(1)
            desc_match = META_DESC_RE.search(page)
            if desc_match:
                summary = html_lib.unescape(desc_match.group(1)).strip() or None
            for level, body in H_RE.findall(decode_wp_symbols(page)):
                cleaned = strip_tags(body)
                if not cleaned:
                    continue
                if level == "1" and parties is None:
                    parties = cleaned
                elif level == "2" and not heading:
                    heading = cleaned

        if len(text) < 400:
            logger.warning(f"Insufficient text ({len(text)} chars): {url}")
            return None

        head = text[:1500]
        if parties is None:
            # Pre-2007 opinions are WordPerfect exports with no <h1>; the
            # caption is plain <p> text above the court's banner line.
            parties = caption_from_text(text)
        if date is None:
            date = parse_file_date(filename) or parse_long_date(head)
        if title is None:
            title = (parties or filename.rsplit(".", 1)[0]).strip()

        # `sup/` holds the Supreme Court's certiorari decisions in workers'
        # compensation matters; everything else is the WCCA's own output.
        court = SUPREME_COURT if raw["directory"].lower() == "sup" else WCCA
        if re.search(r"SUPREME\s+COURT", (heading or head[:600]), re.I):
            court = SUPREME_COURT

        docket = None
        docket_match = DOCKET_RE.search(heading or "") or DOCKET_RE.search(head)
        if docket_match:
            docket = re.sub(r"\s+", "", docket_match.group(1)).upper()

        doc_id = re.sub(r"[^A-Za-z0-9]+", "-", DOC_EXT_RE.sub("", path)).strip("-")

        return {
            "_id": f"MN-WCCA-{doc_id}",
            "_source": "US/MN-WCCA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": court,
            "jurisdiction": "US-MN",
            "docket_number": docket,
            "parties": parties,
            "summary": summary,
            "headnote": summary,
            "year": int(date[:4]) if date else None,
            "source_format": "pdf" if raw["is_pdf"] else "html",
            "language": "en",
        }

    def test_api(self) -> bool:
        try:
            documents = self._list_documents()
        except Exception as exc:
            logger.error(f"Index unreachable: {exc}")
            return False
        if len(documents) < 500:
            logger.error(f"Index too small ({len(documents)} documents)")
            return False
        probe = next((d for d in documents if not d["is_pdf"]), documents[0])
        record = self.normalize(probe)
        if not record or not record.get("text"):
            logger.error(f"No full text from probe document {probe['url']}")
            return False
        logger.info(
            f"OK: {len(documents)} decisions; probe {record['_id']} "
            f"({record['date']}) {len(record['text'])} chars"
        )
        return True

    def run_curated_sample(self, size: int = 15) -> int:
        """Sample across the whole date range, not just the newest directory."""
        logger.info(f"=== SAMPLE MODE: {size} decisions ===")
        documents = self._list_documents()
        by_bucket = {}
        for entry in documents:
            bucket = entry["directory"].split("/")[-1] or "root"
            by_bucket.setdefault(bucket, []).append(entry)

        buckets = sorted(by_bucket)
        if len(buckets) > size:
            # More year directories than sample slots: spread the picks evenly
            # across the range instead of exhausting the earliest years.
            step = len(buckets) / size
            buckets = [buckets[int(i * step)] for i in range(size)]
        picks = []
        round_index = 0
        while len(picks) < size * 2 and any(
            len(by_bucket[b]) > round_index for b in buckets
        ):
            for bucket in buckets:
                if len(by_bucket[bucket]) > round_index:
                    picks.append(by_bucket[bucket][round_index])
            round_index += 1

        records = []
        for entry in picks:
            if len(records) >= size:
                break
            try:
                record = self.normalize(entry)
            except Exception as exc:
                logger.warning(f"normalize failed for {entry['url']}: {exc}")
                continue
            if record:
                records.append(record)
                logger.info(f"  {record['_id']}: {len(record['text'])} chars")

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for record in records:
            path = sample_dir / f"{record['_id']}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"=== Sample complete: {len(records)} records ===")
        return len(records)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/MN-WCCA bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = MNWCCAScraper()
    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)
    if args.sample:
        sys.exit(0 if scraper.run_curated_sample() > 0 else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap()
    logger.info(
        f"{args.command} complete: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    sys.exit(0 if stats.get("records_fetched", 0) > 0 else 1)


if __name__ == "__main__":
    main()
