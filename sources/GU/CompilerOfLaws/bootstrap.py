#!/usr/bin/env python3
"""
GU/CompilerOfLaws -- Guam Code Annotated Data Fetcher

Fetches full text of Guam Code Annotated (GCA) chapters from
guamcourts.gov/CompilerofLaws. Each GCA title page lists chapter-level PDFs;
we parse those pages, download PDFs, and extract text via pdfplumber.

Covers:
  - Organic Act of Guam
  - 22 GCA Titles (General Provisions through Business Regulation)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~20 sample records
  python bootstrap.py bootstrap --full     # Full bootstrap (~862 chapters)
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import tempfile
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

# Setup
SOURCE_ID = "GU/CompilerOfLaws"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GU.CompilerOfLaws")

BASE_URL = "https://guamcourts.gov/CompilerofLaws/"
GCA_INDEX_URL = f"{BASE_URL}gca.html"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    text_parts = []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
    return "\n\n".join(text_parts)


def fetch_title_pages() -> list[dict]:
    """Parse the GCA index page to get all title page URLs."""
    resp = session.get(GCA_INDEX_URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    titles = []

    # Find the Organic Act PDF link
    organic_match = re.search(
        r'href="(GCA/[^"]*OrganicAct[^"]*\.pdf)"',
        html, re.IGNORECASE
    )
    if organic_match:
        titles.append({
            "title_number": 0,
            "title_name": "Organic Act of Guam",
            "url": urljoin(GCA_INDEX_URL, organic_match.group(1)),
            "is_pdf": True,
        })

    # Find title page links: GCA/title{N}.html
    for m in re.finditer(
        r'href="(GCA/title(\d+)\.html)"[^>]*>([^<]*)',
        html, re.IGNORECASE
    ):
        href, num, name = m.group(1), int(m.group(2)), m.group(3).strip()
        titles.append({
            "title_number": num,
            "title_name": name if name else f"Title {num}",
            "url": urljoin(GCA_INDEX_URL, href),
            "is_pdf": False,
        })

    logger.info(f"Found {len(titles)} GCA titles")
    return titles


def fetch_chapter_pdfs(title_url: str, title_number: int, title_name: str) -> list[dict]:
    """Parse a title page to get all chapter PDF URLs."""
    resp = session.get(title_url, timeout=30)
    resp.raise_for_status()
    html = resp.text

    chapters = []
    seen_chapters = set()
    # Match PDF links like: 01gca/1gc001.PDF or 09gca/9gc070.pdf
    for m in re.finditer(
        r'href="([^"]*?gc(\d+)\.pdf)"[^>]*>([^<]*)',
        html, re.IGNORECASE
    ):
        href, chap_num, link_text = m.group(1), m.group(2), m.group(3).strip()
        chap_int = int(chap_num)
        if chap_int in seen_chapters:
            continue
        seen_chapters.add(chap_int)
        pdf_url = urljoin(title_url, href)
        chapters.append({
            "chapter_number": chap_int,
            "chapter_name": link_text if link_text else f"Chapter {chap_int}",
            "pdf_url": pdf_url,
            "title_number": title_number,
            "title_name": title_name,
        })

    # Also get Table of Contents PDF if present
    toc_match = re.search(r'href="([^"]*_TOC\.pdf)"', html, re.IGNORECASE)
    if toc_match:
        logger.debug(f"Title {title_number} has TOC PDF (skipping)")

    logger.info(f"Title {title_number}: found {len(chapters)} chapter PDFs")
    return chapters


def download_and_extract(pdf_url: str) -> str:
    """Download a PDF and extract its text."""
    resp = session.get(pdf_url, timeout=60)
    resp.raise_for_status()
    return extract_pdf_text(resp.content)


def normalize(raw: dict) -> dict:
    """Normalize a raw chapter record into standard schema."""
    title_num = raw.get("title_number", 0)
    chap_num = raw.get("chapter_number", 0)

    if title_num == 0:
        doc_id = "GU-GCA-OrganicAct"
        title = "Organic Act of Guam"
    else:
        doc_id = f"GU-GCA-T{title_num:02d}-CH{chap_num:03d}"
        title = f"GCA Title {title_num}, Chapter {chap_num}"
        chap_name = raw.get("chapter_name", "")
        if chap_name and chap_name != f"Chapter {chap_num}":
            title = f"{title}: {chap_name}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": None,
        "url": raw.get("pdf_url", ""),
        "title_number": title_num,
        "title_name": raw.get("title_name", ""),
        "chapter_number": chap_num,
        "chapter_name": raw.get("chapter_name", ""),
        "jurisdiction": "GU",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records for all GCA chapters."""
    titles = fetch_title_pages()
    count = 0
    sample_limit = 20 if sample else None

    for title_info in titles:
        if sample_limit and count >= sample_limit:
            break

        title_num = title_info["title_number"]
        title_name = title_info["title_name"]

        # Organic Act is a single PDF
        if title_info.get("is_pdf"):
            logger.info(f"Downloading Organic Act PDF...")
            try:
                text = download_and_extract(title_info["url"])
                if text.strip():
                    raw = {
                        "title_number": 0,
                        "chapter_number": 0,
                        "title_name": "Organic Act of Guam",
                        "chapter_name": "Organic Act of Guam",
                        "text": text,
                        "pdf_url": title_info["url"],
                    }
                    yield normalize(raw)
                    count += 1
                else:
                    logger.warning("Organic Act PDF yielded no text")
            except Exception as e:
                logger.error(f"Failed to fetch Organic Act: {e}")
            time.sleep(1)
            continue

        # Regular title: parse the title page for chapter PDFs
        try:
            chapters = fetch_chapter_pdfs(
                title_info["url"], title_num, title_name
            )
        except Exception as e:
            logger.error(f"Failed to parse Title {title_num}: {e}")
            continue

        for chap in chapters:
            if sample_limit and count >= sample_limit:
                break

            pdf_url = chap["pdf_url"]
            logger.info(
                f"Downloading Title {title_num}, Chapter {chap['chapter_number']}..."
            )
            try:
                text = download_and_extract(pdf_url)
                if not text.strip():
                    logger.warning(f"No text extracted from {pdf_url}")
                    continue
                chap["text"] = text
                yield normalize(chap)
                count += 1
            except Exception as e:
                logger.error(f"Failed to fetch {pdf_url}: {e}")

            time.sleep(1)

    logger.info(f"Total records yielded: {count}")


def save_records(records: list[dict], output_dir: Path) -> int:
    """Save records as individual JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    for rec in records:
        fname = f"{rec['_id']}.json"
        fpath = output_dir / fname
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1
    return saved


def cmd_test_api():
    """Quick connectivity test."""
    print(f"Testing connection to {GCA_INDEX_URL}...")
    resp = session.get(GCA_INDEX_URL, timeout=15)
    print(f"Status: {resp.status_code}")
    print(f"Content length: {len(resp.text)} bytes")

    titles = fetch_title_pages()
    print(f"Found {len(titles)} titles")

    if titles:
        # Test first title page
        first = [t for t in titles if not t.get("is_pdf")]
        if first:
            t = first[0]
            chapters = fetch_chapter_pdfs(t["url"], t["title_number"], t["title_name"])
            print(f"Title {t['title_number']} has {len(chapters)} chapters")

            if chapters:
                # Test downloading first chapter PDF
                pdf_url = chapters[0]["pdf_url"]
                print(f"Testing PDF download: {pdf_url}")
                resp = session.get(pdf_url, timeout=30)
                print(f"PDF status: {resp.status_code}, size: {len(resp.content)} bytes")
                text = extract_pdf_text(resp.content)
                print(f"Extracted text length: {len(text)} chars")
                if text:
                    print(f"First 200 chars: {text[:200]}")

    print("\nConnectivity test PASSED")


def cmd_bootstrap(sample: bool = False, full: bool = False):
    """Bootstrap the data source."""
    mode = "sample" if sample else "full"
    logger.info(f"Starting bootstrap in {mode} mode")

    records = list(fetch_all(sample=sample))
    logger.info(f"Fetched {len(records)} records")

    if not records:
        logger.error("No records fetched!")
        sys.exit(1)

    output_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    saved = save_records(records, output_dir)
    logger.info(f"Saved {saved} records to {output_dir}")

    # Validate
    texts_ok = sum(1 for r in records if r.get("text", "").strip())
    print(f"\n{'='*60}")
    print(f"Bootstrap complete ({mode} mode)")
    print(f"Records: {len(records)}")
    print(f"With full text: {texts_ok}/{len(records)}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}")

    if texts_ok < len(records):
        logger.warning(f"{len(records) - texts_ok} records missing text!")


def main():
    parser = argparse.ArgumentParser(description="GU/CompilerOfLaws bootstrapper")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Bootstrap data")
    boot.add_argument("--sample", action="store_true", help="Sample mode (~20 records)")
    boot.add_argument("--full", action="store_true", help="Full bootstrap")

    sub.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        cmd_test_api()
    elif args.command == "bootstrap":
        cmd_bootstrap(sample=args.sample, full=args.full)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
