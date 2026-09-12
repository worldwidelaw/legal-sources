#!/usr/bin/env python3
"""
TJ/SupremeCourt -- Supreme Court of the Republic of Tajikistan

Quarterly publications of Tajikistan's Supreme Court (Суди Олии Ҷумҳурии
Тоҷикистон). Each issue is a multi-decision compendium: cassation rulings,
Plenum interpretations of legislation, doctrinal commentary on code
amendments, and practical guidance for lower courts. Two consecutive series
are published:

  * "НАШРИЯИ Суди Олии Ҷумҳурии Тоҷикистон" -- 2020 onward, quarterly
  * "МИЗОНИ ҚОНУН" -- 2025 onward, the renamed successor

Data source: https://sud.tj/nashriyai-sudi-oli/
License: Open Government Data (official Supreme Court publication, free
distribution declared on the cover)

Strategy
--------
  1. GET the publications listing page. Every issue is rendered as an
     ``<a href="...pdf">TITLE</a>`` link inside a year-keyed tab pane.
  2. For each PDF: download, extract text with pdfplumber (page-by-page),
     concatenate with form-feed separators for paragraph reading order.
  3. Parse the title ("НАШРИЯИ №3_2022", "МИЗОНИ ҚОНУН №1_2026") to derive
     the series, issue number and year. Use the year as ``date`` (the
     Supreme Court publishes the precise day only on the cover, in Tajik).

The PDFs contain extractable text layers (no OCR needed). Scanned-only
issues without a text layer are skipped after a length threshold check.

Usage
-----
  python bootstrap.py bootstrap --sample   # 12 issues for validation
  python bootstrap.py bootstrap            # all available issues
  python bootstrap.py test-api             # connectivity / parse probe
"""

import argparse
import hashlib
import html as html_lib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote, urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

SOURCE_ID = "TJ/SupremeCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TJ.SupremeCourt")

BASE_URL = "https://sud.tj"
LISTING_URL = f"{BASE_URL}/nashriyai-sudi-oli/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tg,ru;q=0.9,en;q=0.8",
}

RATE_LIMIT = 1.5
MIN_TEXT_CHARS = 800
SAMPLE_TARGET = 12

# Match the title strings the Supreme Court uses for its serial publications.
# Latin transliterations are tolerated too because they sometimes appear in
# filenames (e.g. "Нашрия_2_2024").
_TITLE_RE = re.compile(
    r"(?P<series>(?:НАШРИЯИ?|Нашрия|НАШРИЯ|МИЗОНИ\s+ҚОНУН|Мизони\s+Қонун))"
    r"\s*(?:№|N)?\s*(?P<issue>\d+)\s*[_\s]\s*(?P<year>\d{4})",
    re.IGNORECASE,
)


def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", html_lib.unescape(text)).strip()


def _normalize_series(raw: str) -> str:
    lowered = raw.lower().replace(" ", "")
    if "мизон" in lowered:
        return "Мизони Қонун"
    return "Нашрияи Суди Олии Ҷумҳурии Тоҷикистон"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _fetch(session: requests.Session, url: str, *, binary: bool = False,
           tries: int = 3) -> Optional[bytes | str]:
    last_exc = None
    for attempt in range(1, tries + 1):
        try:
            resp = session.get(url, timeout=(20, 120))
            resp.raise_for_status()
            if binary:
                return resp.content
            resp.encoding = resp.encoding or "utf-8"
            return resp.text
        except Exception as e:
            last_exc = e
            logger.warning("GET %s failed (attempt %d/%d): %s", url, attempt, tries, e)
            time.sleep(RATE_LIMIT * attempt)
    logger.error("Giving up on %s: %s", url, last_exc)
    return None


def discover_issues(session: requests.Session) -> list[dict]:
    """Scrape the publications page for every linked bulletin PDF.

    Returns a list of {url, title, series, issue, year, raw_filename} dicts,
    deduped by URL and ordered newest-first.
    """
    html = _fetch(session, LISTING_URL)
    if not html:
        return []

    # Match an <a> link whose href ends in .pdf and capture its visible text.
    issues: dict[str, dict] = {}
    for m in re.finditer(
        r'href="(?P<href>https?://[^"]+\.pdf|/[^"]+\.pdf)"[^>]*>(?P<label>[^<]+)</a>',
        html,
    ):
        href = m.group("href")
        label = _clean(m.group("label"))
        url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # Skip obvious non-bulletin docs (Code of Judges' Ethics, etc.).
        if "одоби" in label.lower() or "ethics" in label.lower():
            continue

        title_match = _TITLE_RE.search(label) or _TITLE_RE.search(unquote(href))
        if not title_match:
            # Try the filename as a last resort -- some links carry generic text.
            fname = unquote(href.rsplit("/", 1)[-1])
            title_match = _TITLE_RE.search(fname)
            if not title_match:
                continue

        issue_no = int(title_match.group("issue"))
        year = int(title_match.group("year"))
        series = _normalize_series(title_match.group("series"))

        if url in issues:
            continue
        issues[url] = {
            "url": url,
            "label": label or unquote(href.rsplit("/", 1)[-1]),
            "series": series,
            "issue": issue_no,
            "year": year,
        }

    ordered = sorted(
        issues.values(),
        key=lambda d: (d["year"], d["issue"]),
        reverse=True,
    )
    logger.info("Discovered %d Supreme Court bulletins", len(ordered))
    return ordered


def _extract_pdf_text(blob: bytes) -> str:
    """Extract text from a downloaded PDF. Returns '' on failure."""
    import io
    pages: list[str] = []
    try:
        with pdfplumber.open(io.BytesIO(blob)) as pdf:
            for page in pdf.pages:
                ptxt = page.extract_text() or ""
                if ptxt.strip():
                    pages.append(ptxt.strip())
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        logger.warning("pdfplumber failed: %s", e)
        return ""
    return "\n\n".join(pages).strip()


def normalize(meta: dict, text: str) -> dict:
    title = f"{meta['series']} №{meta['issue']}/{meta['year']}"
    # Derive a stable _id from the URL hash so re-runs are idempotent
    # even when the CMS shuffles its medialibrary paths.
    digest = hashlib.sha1(meta["url"].encode("utf-8")).hexdigest()[:10]
    rec_id = f"tj-supremecourt-{meta['year']}-{meta['issue']}-{digest}"
    return {
        "_id": rec_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": f"{meta['year']}-01-01",
        "url": meta["url"],
        "issue": meta["issue"],
        "year": meta["year"],
        "series": meta["series"],
        "language": "tg",
        "country": "TJ",
        "raw_label": meta["label"],
    }


def fetch_all(session: Optional[requests.Session] = None) -> Generator[dict, None, None]:
    session = session or _session()
    for meta in discover_issues(session):
        logger.info("Fetching %s №%d/%d -- %s", meta["series"], meta["issue"], meta["year"], meta["url"])
        blob = _fetch(session, meta["url"], binary=True)
        if not blob or not blob.startswith(b"%PDF"):
            logger.warning("Skipping (no PDF bytes): %s", meta["url"])
            continue
        text = _extract_pdf_text(blob)
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(
                "Skipping (text too short, %d chars, likely scanned): %s",
                len(text), meta["url"],
            )
            continue
        rec = normalize(meta, text)
        yield rec
        time.sleep(RATE_LIMIT)


def fetch_updates(since: Optional[str] = None) -> Generator[dict, None, None]:
    """The portal has no per-issue last-modified; updates require a full
    sweep. Downstream consumers should dedupe by ``_id``."""
    yield from fetch_all()


def _bootstrap(sample: bool) -> int:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    saved = 0
    target = SAMPLE_TARGET if sample else None
    for rec in fetch_all():
        out = SAMPLE_DIR / f"{rec['_id']}.json"
        with out.open("w", encoding="utf-8") as fh:
            json.dump(rec, fh, ensure_ascii=False, indent=2)
        saved += 1
        logger.info(
            "Saved %s (%d chars) -> %s",
            rec["_id"], len(rec["text"]), out.name,
        )
        if target is not None and saved >= target:
            break
    logger.info("Bootstrap complete: %d records", saved)
    return saved


def _test_api() -> int:
    session = _session()
    issues = discover_issues(session)
    if not issues:
        print("FAIL: no issues discovered")
        return 1
    print(f"Listing OK: {len(issues)} issues discovered")
    probe = issues[0]
    print(f"Probe: {probe['series']} №{probe['issue']}/{probe['year']}  {probe['url']}")
    blob = _fetch(session, probe["url"], binary=True)
    if not blob:
        print("FAIL: probe PDF download failed")
        return 1
    text = _extract_pdf_text(blob)
    print(f"Probe text: {len(text)} chars")
    return 0 if len(text) >= MIN_TEXT_CHARS else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="TJ/SupremeCourt data fetcher")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_boot = sub.add_parser("bootstrap")
    p_boot.add_argument("--sample", action="store_true")
    sub.add_parser("test-api")
    args = parser.parse_args()

    if args.cmd == "bootstrap":
        n = _bootstrap(sample=args.sample)
        return 0 if n > 0 else 1
    if args.cmd == "test-api":
        return _test_api()
    return 1


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    sys.exit(main())
