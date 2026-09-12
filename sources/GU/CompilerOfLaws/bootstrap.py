#!/usr/bin/env python3
"""
GU/CompilerOfLaws -- Guam Code Annotated & Administrative Rules Data Fetcher

The Compiler of Laws (Judiciary of Guam) moved off guamcourts.gov/CompilerofLaws
onto its own Drupal site, col.guamcourts.gov (issue #1335). Both corpora are now
published as a single accordion page each, with every chapter served as a PDF
under /sites/default/files/.

Covers:
  - Organic Act of Guam note
  - Guam Code Annotated (GCA): 22 titles, ~860 chapter PDFs
  - Guam Administrative Rules & Regulations (GAR): ~250 chapter PDFs

Usage:
  python bootstrap.py bootstrap --sample    # ~20 sample records
  python bootstrap.py bootstrap --full      # full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast --full # alias used by the fleet wrapper
  python bootstrap.py test-api              # quick connectivity test
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

SOURCE_ID = "GU/CompilerOfLaws"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GU.CompilerOfLaws")

BASE_URL = "https://col.guamcourts.gov/"
GCA_INDEX_URL = urljoin(BASE_URL, "/guam-code-annotated/guam-code-annotated")
GAR_INDEX_URL = urljoin(
    BASE_URL,
    "/guam-administrative-rules-regulations/guam-administrative-rules-and-regulations",
)

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

# <div class="panel-heading">...<a href="#faqN">Title 1: General Provisions</a>
PANEL_RE = re.compile(
    r'<a\s+href="#faq(\d+)"[^>]*>(.*?)</a>.*?'
    r'<div class="panel-collapse"\s+id="faq\1">(.*?)(?=<!--\s*\d+\s*-->|</div>\s*</div>\s*</div>)',
    re.IGNORECASE | re.DOTALL,
)
LINK_RE = re.compile(
    r'<a\s+[^>]*href="([^"]+\.[Pp][Dd][Ff])"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TITLE_HEAD_RE = re.compile(r"Title\s+(\d+)\s*[:\-.]?\s*(.*)", re.IGNORECASE)
CHAPTER_CODE_RE = re.compile(
    r"^\s*Chapters?\s+([0-9]+)(?:\.([0-9]+))?([A-Za-z]?)", re.IGNORECASE
)
PART_RE = re.compile(r"\bPart\s+([0-9]+)", re.IGNORECASE)


def strip_tags(fragment: str) -> str:
    """Turn an HTML fragment into clean plain text."""
    text = re.sub(r"<[^>]+>", "", fragment)
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


def get_html(url: str) -> str:
    resp = session.get(url, timeout=45)
    resp.raise_for_status()
    if len(resp.text) < 5000:
        raise RuntimeError(
            f"{url} returned only {len(resp.text)} bytes — the Compiler of Laws "
            "site is unreachable or blocking this vantage."
        )
    return resp.text


def parse_edition_date(html: str) -> str | None:
    """'Updated through P.L. 38-133 (June 4, 2026)' -> '2026-06-04'."""
    m = re.search(
        r"Updated through[^(]*\((\w+)\s+(\d{1,2}),\s*(\d{4})\)", html, re.IGNORECASE
    )
    if not m:
        return None
    month = MONTHS.get(m.group(1).lower())
    if not month:
        return None
    return f"{int(m.group(3)):04d}-{month:02d}-{int(m.group(2)):02d}"


def chapter_code(link_text: str) -> str:
    """Stable chapter code from the visible label: '004', '066A', '009-5', '012-P1'.

    Returns "" for links that are not plain chapters (divisions, attachments);
    those fall back to the PDF file name, which is collision-free.
    """
    m = CHAPTER_CODE_RE.match(link_text)
    if not m:
        return ""
    code = f"{int(m.group(1)):03d}{m.group(3).upper()}"
    if m.group(2):
        code = f"{code}-{m.group(2)}"
    part = PART_RE.search(link_text)
    if part:
        code = f"{code}-P{part.group(1)}"
    return code


def parse_accordion(html: str, index_url: str, collection: str) -> list[dict]:
    """Parse a Compiler of Laws accordion page into chapter descriptors."""
    docs = []
    seen = set()
    for panel in PANEL_RE.finditer(html):
        heading = strip_tags(panel.group(2))
        body = panel.group(3)

        th = TITLE_HEAD_RE.search(heading)
        title_number = int(th.group(1)) if th else 0
        title_name = (th.group(2).strip() if th else heading) or f"Title {title_number}"

        division = ""
        for link in LINK_RE.finditer(body):
            href = link.group(1)
            label = strip_tags(link.group(2))
            pdf_url = urljoin(index_url, href)
            fname = pdf_url.rsplit("/", 1)[-1]

            # A "Division N" link sets the context for the chapters beneath it,
            # but is itself a substantive PDF wherever a division has no chapter
            # children (all of GCA Title 13, several GAR titles) — so keep it.
            if re.match(r"division", label, re.IGNORECASE):
                division = label

            if re.search(r"toc", fname, re.IGNORECASE) or re.search(
                r"^table of contents", label, re.IGNORECASE
            ):
                continue

            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            docs.append({
                "collection": collection,
                "title_number": title_number,
                "title_name": title_name,
                "division": division,
                "chapter_code": chapter_code(label),
                "chapter_name": label,
                "pdf_url": pdf_url,
            })

    logger.info(f"{collection}: parsed {len(docs)} chapter PDFs")
    return docs


def discover() -> tuple[list[dict], str | None]:
    """Return (documents, edition_date) across the Organic Act, GCA and GAR."""
    gca_html = get_html(GCA_INDEX_URL)
    edition_date = parse_edition_date(gca_html)

    docs = []

    organic = re.search(
        r'href="([^"]*OrganicAct[^"]*\.pdf)"', gca_html, re.IGNORECASE
    )
    if organic:
        docs.append({
            "collection": "ORGANIC",
            "title_number": 0,
            "title_name": "Organic Act of Guam",
            "division": "",
            "chapter_code": "",
            "chapter_name": "Organic Act of Guam",
            "pdf_url": urljoin(GCA_INDEX_URL, organic.group(1)),
        })

    docs += parse_accordion(gca_html, GCA_INDEX_URL, "GCA")

    try:
        gar_html = get_html(GAR_INDEX_URL)
        docs += parse_accordion(gar_html, GAR_INDEX_URL, "GAR")
    except Exception as e:
        logger.error(f"Failed to read the GAR index: {e}")

    if not docs:
        raise RuntimeError(
            "No chapter PDFs discovered — col.guamcourts.gov layout changed again."
        )
    logger.info(f"Discovered {len(docs)} documents (edition {edition_date})")
    return docs, edition_date


def normalize(raw: dict) -> dict:
    collection = raw.get("collection", "GCA")
    title_num = raw.get("title_number", 0)
    code = raw.get("chapter_code") or ""
    chap_name = raw.get("chapter_name", "")

    file_slug = re.sub(
        r"[^A-Za-z0-9]+", "", raw["pdf_url"].rsplit("/", 1)[-1][:-4]
    ).upper()

    if collection == "ORGANIC":
        doc_id = "GU-GCA-OrganicAct"
        title = "Organic Act of Guam"
    else:
        if collection == "GCA":
            # Keep the historical GU-GCA-T{tt}-CH{nnn} id so re-ingests dedupe
            # against records landed from the old guamcourts.gov layout.
            doc_id = f"GU-GCA-T{title_num:02d}-CH{code or file_slug}"
        else:
            # GAR chapter numbers restart inside every division, so the file
            # name is the only collision-free key.
            doc_id = f"GU-GAR-T{title_num:02d}-{file_slug}"
        label = "GCA" if collection == "GCA" else "GAR"
        title = f"{label} Title {title_num}"
        if raw.get("division"):
            title = f"{title}, {raw['division'].rstrip('.')}"
        title = f"{title}, {chap_name}" if chap_name else title

    # numeric chapter number kept for backwards compatibility with earlier records
    num_match = re.match(r"(\d+)", code)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("pdf_url", ""),
        "collection": collection,
        "title_number": title_num,
        "title_name": raw.get("title_name", ""),
        "division": raw.get("division", ""),
        "chapter_number": int(num_match.group(1)) if num_match else 0,
        "chapter_code": code,
        "chapter_name": chap_name,
        "jurisdiction": "GU",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    docs, edition_date = discover()
    limit = None
    if sample:
        # Stride the corpus so the sample spans the Organic Act, every GCA title
        # and the GAR rather than just the first title's chapters.
        limit = 20
        stride = max(1, len(docs) // limit)
        docs = docs[::stride]
    count = 0
    errors = 0

    for doc in docs:
        if limit and count >= limit:
            break
        try:
            resp = session.get(doc["pdf_url"], timeout=90)
            resp.raise_for_status()
            text = extract_pdf_text(resp.content)
        except Exception as e:
            errors += 1
            logger.error(f"Failed {doc['pdf_url']}: {e}")
            time.sleep(1)
            continue

        if not text.strip():
            logger.warning(f"No text extracted from {doc['pdf_url']}")
            time.sleep(1)
            continue

        doc["text"] = text
        doc["date"] = edition_date
        yield normalize(doc)
        count += 1
        if count % 25 == 0:
            logger.info(f"{count}/{len(docs)} fetched")
        time.sleep(1)

    logger.info(f"Total records yielded: {count} ({errors} errors)")


def fetch_updates(since: str | None = None) -> Generator[dict, None, None]:
    """The GCA/GAR are consolidated codes republished in place — refetch all."""
    yield from fetch_all(sample=False)


def cmd_test_api():
    print(f"Testing {GCA_INDEX_URL} ...")
    docs, edition = discover()
    print(f"Documents discovered: {len(docs)} (edition {edition})")
    by_collection = {}
    for d in docs:
        by_collection[d["collection"]] = by_collection.get(d["collection"], 0) + 1
    print(f"By collection: {by_collection}")

    probe = docs[1] if len(docs) > 1 else docs[0]
    print(f"Testing PDF: {probe['pdf_url']}")
    resp = session.get(probe["pdf_url"], timeout=60)
    print(f"PDF status: {resp.status_code}, size: {len(resp.content)} bytes")
    text = extract_pdf_text(resp.content)
    print(f"Extracted text: {len(text)} chars")
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
    parser = argparse.ArgumentParser(description="GU/CompilerOfLaws bootstrapper")
    sub = parser.add_subparsers(dest="command")

    for name in ("bootstrap", "bootstrap-fast"):
        p = sub.add_parser(name, help="Bootstrap data")
        p.add_argument("--sample", action="store_true", help="Sample mode (~20 records)")
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
