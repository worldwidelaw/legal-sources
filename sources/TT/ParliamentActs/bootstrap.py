#!/usr/bin/env python3
"""
TT/ParliamentActs — Trinidad & Tobago Parliament Acts of Parliament

Fetches full-text Acts from the Trinidad & Tobago Parliament website
(ttparliament.org). ~600+ Acts from 1999 to present as PDF documents.

Strategy:
  - Parse WordPress sitemaps to discover all publication URLs
  - Filter for act-related URLs (containing "act" in the slug)
  - Visit each page to extract title and PDF download link
  - Download PDF and extract text via pdfminer

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import ssl
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "TT/ParliamentActs"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://www.ttparliament.org"
SITEMAP_URLS = [
    f"{BASE_URL}/wp-sitemap-posts-publication-{i}.xml" for i in range(1, 4)
]

# SSL context that ignores certificate verification (site has cert issues)
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


def http_get(url: str, retries: int = 3, binary: bool = False):
    """Fetch URL with retries, ignoring SSL cert issues."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={
                "User-Agent": "LegalDataHunter/1.0",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
            })
            with urlopen(req, timeout=60, context=SSL_CTX) as resp:
                data = resp.read()
                if binary:
                    return data
                return data.decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Failed to fetch {url}: {e}")


def get_act_urls_from_sitemaps() -> list[str]:
    """Parse WordPress sitemaps to find all act publication URLs."""
    act_urls = []
    for sitemap_url in SITEMAP_URLS:
        try:
            xml = http_get(sitemap_url)
        except Exception as e:
            print(f"  Warning: Could not fetch {sitemap_url}: {e}")
            continue
        urls = re.findall(r'<loc>(https://www\.ttparliament\.org/publication/[^<]+)</loc>', xml)
        for url in urls:
            slug = url.rstrip('/').split('/')[-1]
            if 'act' in slug.lower():
                act_urls.append(url)
    return act_urls


def extract_page_metadata(url: str) -> Optional[dict]:
    """Visit a publication page and extract title, PDF link, and metadata."""
    try:
        html = http_get(url)
    except Exception as e:
        print(f"    Failed to fetch page: {e}")
        return None

    # Extract title from <title> tag
    title_match = re.search(r'<title>([^<]+)</title>', html)
    raw_title = title_match.group(1).strip() if title_match else ""
    # Decode HTML entities first
    raw_title = raw_title.replace('&#8211;', '–').replace('&#8217;', "'").replace('&amp;', '&')
    # Remove " – Parliament" suffix
    title = re.sub(r'\s*[–—\-]+\s*Parliament\s*$', '', raw_title)

    # Find PDF link
    pdf_match = re.search(r'href=["\x27](/wp-content/uploads/[^"\x27]*\.pdf)', html)
    if not pdf_match:
        pdf_match = re.search(r'href=["\x27](https?://[^"\x27]*\.pdf)', html)
    if not pdf_match:
        return None

    pdf_path = pdf_match.group(1)
    if pdf_path.startswith('/'):
        pdf_url = BASE_URL + pdf_path
    else:
        pdf_url = pdf_path

    # Try to extract act number from PDF filename (e.g., a2004-05.pdf)
    act_num_match = re.search(r'a(\d{4})-(\d+)\.pdf', pdf_path)
    act_number = None
    year = None
    if act_num_match:
        year = act_num_match.group(1)
        act_number = f"Act No. {int(act_num_match.group(2))} of {year}"
    else:
        # Try to extract year from title
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', title)
        if year_match:
            year = year_match.group(1)

    slug = url.rstrip('/').split('/')[-1]

    return {
        "title": title,
        "pdf_url": pdf_url,
        "act_number": act_number,
        "year": year,
        "url": url,
        "slug": slug,
    }


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source=SOURCE_ID,
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""


def normalize(meta: dict, text: str) -> dict:
    """Normalize an act into standard schema."""
    date = f"{meta['year']}-01-01" if meta.get("year") else None
    slug = meta.get("slug", "unknown")
    _id = f"TT_PA_{slug}"

    return {
        "_id": _id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": (meta.get("title") or f"TT Act {slug}")[:300],
        "date": date,
        "act_number": meta.get("act_number"),
        "text": text,
        "url": meta.get("url", ""),
        "pdf_url": meta.get("pdf_url", ""),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all acts from TT Parliament website."""
    print("  Discovering act URLs from sitemaps...")
    act_urls = get_act_urls_from_sitemaps()
    print(f"  Found {len(act_urls)} act URLs")

    if not act_urls:
        print("  ERROR: No act URLs found in sitemaps")
        return

    total_yielded = 0
    total_failed = 0
    limit = 15 if sample else len(act_urls)

    for i, url in enumerate(act_urls):
        if total_yielded >= limit:
            break

        slug = url.rstrip('/').split('/')[-1]
        print(f"  [{i+1}/{len(act_urls)}] {slug[:60]}...")
        time.sleep(1)

        meta = extract_page_metadata(url)
        if not meta:
            print(f"    Skipped: no PDF link found")
            total_failed += 1
            continue

        print(f"    Downloading PDF: {meta['pdf_url'].split('/')[-1]}")
        time.sleep(1)

        try:
            pdf_bytes = http_get(meta["pdf_url"], binary=True)
        except Exception as e:
            print(f"    PDF download failed: {e}")
            total_failed += 1
            continue

        text = extract_text_from_pdf(pdf_bytes)
        if not text or len(text.strip()) < 50:
            print(f"    No text extracted from PDF")
            total_failed += 1
            continue

        record = normalize(meta, text)
        yield record
        total_yielded += 1
        print(f"    OK: {len(text)} chars | {meta.get('title', '')[:50]}")

    print(f"\n  Total acts with text: {total_yielded}, failed: {total_failed}")


def test_connection():
    """Test connectivity to ttparliament.org."""
    print("Testing TT/ParliamentActs connectivity...")

    print("\n1. Fetching sitemap...")
    try:
        xml = http_get(SITEMAP_URLS[0])
        urls = re.findall(r'<loc>(https://www\.ttparliament\.org/publication/[^<]+)</loc>', xml)
        act_urls = [u for u in urls if 'act' in u.rstrip('/').split('/')[-1].lower()]
        print(f"   OK: {len(urls)} publications, {len(act_urls)} acts")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    if not act_urls:
        print("   FAIL: No act URLs found")
        return False

    print(f"\n2. Fetching act page: {act_urls[0][:60]}...")
    meta = extract_page_metadata(act_urls[0])
    if meta:
        print(f"   OK: {meta.get('title', '')[:60]}")
        print(f"   PDF: {meta.get('pdf_url', '')}")
    else:
        print("   FAIL: Could not extract page metadata")
        return False

    print("\n3. Downloading PDF...")
    try:
        pdf_bytes = http_get(meta["pdf_url"], binary=True)
        print(f"   OK: {len(pdf_bytes)} bytes")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n4. Extracting text...")
    text = extract_text_from_pdf(pdf_bytes)
    if text and len(text) > 50:
        print(f"   OK: {len(text)} chars")
        print(f"   Preview: {text[:200]}...")
    else:
        print("   FAIL: No text extracted")
        return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="TT Parliament Acts Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            filename = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  Saved: {filepath.name} ({len(record['text'])} chars)")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
