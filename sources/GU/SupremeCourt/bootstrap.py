#!/usr/bin/env python3
"""
GU/SupremeCourt -- Supreme Court of Guam Opinions Data Fetcher

The Judiciary of Guam rebuilt guamcourts.gov on Drupal (issue #1335). The old
/Supreme-Court-Opinions/Supreme-Court-Opinions.asp year-POST form is gone, and
the corpus is now split in two:

  - current year: /courts-council/supreme-court/opinions (static Drupal page)
  - 2025 and prior: /legacydata/supreme-court-opinions, a jQuery archive whose
    year dropdown fires
    GET ?action=get_items&type=SPRMOP&year=YYYY -> an HTML fragment

Opinions run 1996-present (~800), each a born-digital PDF read with pdfplumber.

Usage:
  python bootstrap.py bootstrap --sample     # ~15 sample records
  python bootstrap.py bootstrap --full       # full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast --full  # alias used by the fleet wrapper
  python bootstrap.py test-api               # quick connectivity test
"""

from __future__ import annotations

import argparse
import html as html_mod
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

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

SOURCE_ID = "GU/SupremeCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GU.SupremeCourt")

BASE_URL = "https://guamcourts.gov"
CURRENT_URL = f"{BASE_URL}/courts-council/supreme-court/opinions"
LEGACY_URL = f"{BASE_URL}/legacydata/supreme-court-opinions"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# One anchor to a PDF plus the trailing metadata text that follows it.
ITEM_RE = re.compile(
    r'<a\s+[^>]*href="([^"]+\.[Pp][Dd][Ff])"[^>]*>(.*?)</a>(.{0,400}?)(?=<a\s|</p>|</div>)',
    re.IGNORECASE | re.DOTALL,
)
CITATION_RE = re.compile(r"(\d{4})\s*Guam\s*(\d+)", re.IGNORECASE)
FILE_CITATION_RE = re.compile(r"(\d{4})\s*Guam\s*0*(\d+)", re.IGNORECASE)
DOCKET_RE = re.compile(r"\b([A-Z]{2,5}\d{2}-\d{3}[A-Z]?)\b")
LONG_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),\s*(\d{4})",
    re.IGNORECASE,
)
POSTED_RE = re.compile(r"Posted:\s*([A-Za-z0-9/,\s]{6,20}?)(?:<|$)", re.IGNORECASE)
FILED_RE = re.compile(r"Filed:?\s*([A-Z][a-z]+\s+\d{1,2},\s*\d{4})")
PDF_DOCKET_RE = re.compile(
    r"Supreme Court Case No\.?\s*:?\s*([A-Z]{2,5}[\s-]?\d{2}-?\d{3}[A-Z]?)",
    re.IGNORECASE,
)


def strip_tags(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", " ", fragment)
    return re.sub(r"\s+", " ", html_mod.unescape(text)).strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber (page cache flushed per page)."""
    text_parts = []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
    return "\n\n".join(text_parts)


def parse_long_date(text: str) -> str | None:
    m = LONG_DATE_RE.search(text)
    if not m:
        return None
    month = MONTHS.get(m.group(1).lower())
    if not month:
        return None
    return f"{int(m.group(3)):04d}-{month:02d}-{int(m.group(2)):02d}"


def parse_posted_date(fragment: str) -> str | None:
    m = POSTED_RE.search(fragment)
    if not m:
        return None
    raw = m.group(1).strip().rstrip(",")
    for fmt in ("%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_items(html: str, page_url: str, expect_year: int | None = None) -> list[dict]:
    """Parse opinion entries out of a listing page or legacy AJAX fragment."""
    opinions = []
    seen = set()

    for m in ITEM_RE.finditer(html):
        href, name_html, tail_html = m.group(1), m.group(2), m.group(3)
        case_name = strip_tags(name_html)
        tail = strip_tags(tail_html)
        pdf_url = urljoin(page_url, href)

        # Citation comes from the metadata line, else from the file name
        # ("2026 Guam 7.pdf", "Opinion (2026 Guam 4).pdf", "1996Guam06.pdf").
        cm = CITATION_RE.search(tail) or FILE_CITATION_RE.search(
            pdf_url.rsplit("/", 1)[-1].replace("%20", " ")
        )
        if not cm:
            logger.warning(f"No citation for {pdf_url} — skipping")
            continue
        year, number = int(cm.group(1)), int(cm.group(2))
        if expect_year is not None and year != expect_year:
            continue

        citation = f"{year} Guam {number}"
        if citation in seen:
            continue
        seen.add(citation)

        # Everything before this anchor in the same entry holds "Posted: ...".
        preamble = html[max(0, m.start() - 400):m.start()]

        docket = DOCKET_RE.search(tail)
        opinions.append({
            "year": year,
            "number": number,
            "citation": citation,
            "case_name": case_name,
            "docket": docket.group(1) if docket else "",
            "date": parse_long_date(tail) or parse_posted_date(preamble),
            "pdf_url": pdf_url,
        })

    opinions.sort(key=lambda o: o["number"])
    return opinions


def get_legacy_years() -> list[int]:
    resp = session.get(LEGACY_URL, timeout=45)
    resp.raise_for_status()
    years = sorted({int(y) for y in re.findall(r'<option value="(\d{4})"', resp.text)},
                   reverse=True)
    if not years:
        raise RuntimeError(
            f"{LEGACY_URL} exposed no year options — the legacy archive changed "
            "or this vantage is blocked."
        )
    return years


def fetch_current_year() -> list[dict]:
    resp = session.get(CURRENT_URL, timeout=45)
    resp.raise_for_status()
    ops = parse_items(resp.text, CURRENT_URL)
    logger.info(f"Current-year page: {len(ops)} opinions")
    return ops


def fetch_legacy_year(year: int) -> list[dict]:
    resp = session.get(
        LEGACY_URL,
        params={"action": "get_items", "type": "SPRMOP", "year": str(year)},
        timeout=45,
    )
    resp.raise_for_status()
    ops = parse_items(resp.text, LEGACY_URL, expect_year=year)
    logger.info(f"Year {year}: {len(ops)} opinions")
    return ops


def discover() -> list[dict]:
    """All opinions, newest first, across the current page and the legacy archive."""
    opinions = list(fetch_current_year())
    seen = {o["citation"] for o in opinions}

    for year in get_legacy_years():
        time.sleep(1)
        try:
            for op in fetch_legacy_year(year):
                if op["citation"] not in seen:
                    seen.add(op["citation"])
                    opinions.append(op)
        except Exception as e:
            logger.error(f"Failed to list year {year}: {e}")

    if not opinions:
        raise RuntimeError(
            "No opinions discovered — guamcourts.gov layout changed again or "
            "this vantage is blocked."
        )
    logger.info(f"Discovered {len(opinions)} opinions")
    return opinions


def enrich_from_text(op: dict) -> None:
    """Prefer the opinion's own caption over the listing metadata.

    Listings before ~2010 carry only a bulk "Posted:" date (often years after the
    decision), and no docket at all — both are printed on the PDF's first page.
    """
    head = op.get("text", "")[:3000]
    filed = FILED_RE.search(head)
    if filed:
        parsed = parse_long_date(filed.group(1))
        if parsed:
            op["date"] = parsed
    if not op.get("docket"):
        docket = PDF_DOCKET_RE.search(head)
        if docket:
            op["docket"] = re.sub(r"\s+", " ", docket.group(1)).strip()


def normalize(raw: dict) -> dict:
    title = raw.get("case_name", "") or raw.get("citation", "")
    return {
        "_id": f"GU-SC-{raw['year']}-{raw['number']:02d}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("pdf_url", ""),
        "citation": raw.get("citation", ""),
        "docket": raw.get("docket", ""),
        "year": raw.get("year"),
        "court": "Supreme Court of Guam",
        "jurisdiction": "GU",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    opinions = discover()
    limit = None
    if sample:
        # Stride so the sample spans 1996-present rather than one recent year.
        limit = 15
        stride = max(1, len(opinions) // limit)
        opinions = opinions[::stride]

    count = 0
    errors = 0
    for op in opinions:
        if limit and count >= limit:
            break
        try:
            resp = session.get(op["pdf_url"], timeout=90)
            resp.raise_for_status()
            text = extract_pdf_text(resp.content)
        except Exception as e:
            errors += 1
            logger.error(f"Failed {op['citation']}: {e}")
            time.sleep(1)
            continue

        if not text.strip():
            logger.warning(f"No text from {op['pdf_url']}")
            time.sleep(1)
            continue

        op["text"] = text
        enrich_from_text(op)
        yield normalize(op)
        count += 1
        if count % 25 == 0:
            logger.info(f"{count} opinions fetched")
        time.sleep(1)

    logger.info(f"Total records yielded: {count} ({errors} errors)")


def fetch_updates(since: str | None = None) -> Generator[dict, None, None]:
    """New opinions land on the current-year page; refetch it."""
    for op in fetch_current_year():
        if since and op.get("date") and op["date"] < since:
            continue
        try:
            resp = session.get(op["pdf_url"], timeout=90)
            resp.raise_for_status()
            text = extract_pdf_text(resp.content)
        except Exception as e:
            logger.error(f"Failed {op['citation']}: {e}")
            continue
        if text.strip():
            op["text"] = text
            yield normalize(op)
        time.sleep(1)


def cmd_test_api():
    print(f"Testing {CURRENT_URL} ...")
    current = fetch_current_year()
    print(f"Current-year opinions: {len(current)}")
    years = get_legacy_years()
    print(f"Legacy years: {years[0]}..{years[-1]} ({len(years)})")
    probe_year = years[0]
    legacy = fetch_legacy_year(probe_year)
    print(f"Year {probe_year}: {len(legacy)} opinions")

    op = (current or legacy)[0]
    print(f"Testing PDF: {op['pdf_url']}")
    resp = session.get(op["pdf_url"], timeout=90)
    print(f"PDF status: {resp.status_code}, size: {len(resp.content)} bytes")
    text = extract_pdf_text(resp.content)
    print(f"Extracted text: {len(text)} chars")
    print(f"Citation: {op['citation']} | docket: {op['docket']} | date: {op['date']}")
    print(f"First 200 chars: {text[:200]}")
    print("\nConnectivity test PASSED")


def cmd_bootstrap(sample: bool = False):
    mode = "sample" if sample else "full"
    logger.info(f"Starting bootstrap in {mode} mode")

    if sample:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        records = list(fetch_all(sample=True))
        for rec in records:
            with open(SAMPLE_DIR / f"{rec['_id']}.json", "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)
        written = len(records)
        with_text = sum(1 for r in records if r.get("text", "").strip())
        output = SAMPLE_DIR
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        output = DATA_DIR / "records.jsonl"
        written = 0
        with_text = 0
        with open(output, "w", encoding="utf-8") as f:
            for rec in fetch_all(sample=False):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
                if rec.get("text", "").strip():
                    with_text += 1

    print(f"\n{'=' * 60}")
    print(f"bootstrap_fast complete: {written} fetched, {written} written")
    print(f"With full text: {with_text}/{written}")
    print(f"Output: {output}")
    print(f"{'=' * 60}")

    if written == 0:
        logger.error("No records fetched!")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="GU/SupremeCourt bootstrapper")
    sub = parser.add_subparsers(dest="command")

    for name in ("bootstrap", "bootstrap-fast"):
        p = sub.add_parser(name, help="Bootstrap data")
        p.add_argument("--sample", action="store_true", help="Sample mode (~15 records)")
        p.add_argument("--full", action="store_true", help="Full bootstrap")

    sub.add_parser("test-api", help="Test connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        cmd_test_api()
    elif args.command in ("bootstrap", "bootstrap-fast"):
        cmd_bootstrap(sample=args.sample and not args.full)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
