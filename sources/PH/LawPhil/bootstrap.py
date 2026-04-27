#!/usr/bin/env python3
"""
PH/LawPhil -- Philippine Legal Data from LawPhil Project

Fetches Philippine legislation (Republic Acts) and Supreme Court decisions
from lawphil.net, a project of the Arellano Law Foundation.

Strategy:
  - Parse year/month index pages to discover document links
  - Fetch individual HTML pages for full text
  - Extract text from <blockquote> content

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

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

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "PH/LawPhil"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PH.LawPhil")

BASE_URL = "https://lawphil.net"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Month abbreviations used in jurisprudence URLs
MONTHS = ["jan", "feb", "mar", "apr", "may", "jun",
          "jul", "aug", "sep", "oct", "nov", "dec"]


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</blockquote>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fetch_page(url: str) -> Optional[str]:
    """Fetch an HTML page with retry logic."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            resp.encoding = "windows-1252"
            return resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
            else:
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None


def extract_full_text(html: str) -> str:
    """Extract full text from a LawPhil document page."""
    # Primary: extract from <blockquote> tags
    blocks = re.findall(r'<blockquote[^>]*>(.*?)</blockquote>', html, re.DOTALL | re.IGNORECASE)
    if blocks:
        combined = "\n\n".join(blocks)
        return clean_html(combined)

    # Fallback: extract from body, excluding navigation
    body = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL | re.IGNORECASE)
    if body:
        content = body.group(1)
        # Remove navigation tables (usually the first table)
        content = re.sub(r'<table[^>]*id="s-menu"[^>]*>.*?</table>', '', content, flags=re.DOTALL | re.IGNORECASE)
        return clean_html(content)

    return ""


def extract_title_from_html(html: str) -> str:
    """Extract document title from HTML."""
    # Try <title> tag
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL | re.IGNORECASE)
    if title_match:
        title = clean_html(title_match.group(1)).strip()
        # Remove "The LawPhil Project" suffix
        title = re.sub(r'\s*[-|]\s*The LawPhil Project.*$', '', title, flags=re.IGNORECASE)
        if title and len(title) > 5:
            return title

    # Try first centered bold element
    cb = re.search(r'class="cb"[^>]*>(.*?)<', html, re.DOTALL | re.IGNORECASE)
    if cb:
        return clean_html(cb.group(1)).strip()

    return "Untitled Document"


def parse_index_links(html: str, base_url: str) -> list:
    """Parse an index page to extract document links."""
    links = []
    # Find links in table rows with class "xy" or general <a> tags
    for match in re.finditer(
        r'<a\s+[^>]*href="([^"]+\.html)"[^>]*>(.*?)</a>',
        html, re.DOTALL | re.IGNORECASE
    ):
        href = match.group(1)
        link_text = clean_html(match.group(2)).strip()
        # Skip index/navigation links
        if any(skip in href.lower() for skip in ['sitemap', 'index', 'about', 'contact']):
            continue
        # Skip "not yet available" links
        if 'nya' in href.lower() or not href.strip():
            continue
        full_url = urljoin(base_url, href)
        links.append({"url": full_url, "link_text": link_text})
    return links


def get_legislation_year_urls() -> list:
    """Get URLs for Republic Acts year index pages."""
    urls = []
    # Republic Acts: 1946-present
    current_year = datetime.now().year
    for year in range(current_year, 1945, -1):
        url = f"{BASE_URL}/statutes/repacts/ra{year}/ra{year}.html"
        urls.append(("legislation", year, url))
    return urls


def get_jurisprudence_month_urls(year: int) -> list:
    """Get URLs for jurisprudence monthly index pages."""
    urls = []
    for month in MONTHS:
        url = f"{BASE_URL}/judjuris/juri{year}/{month}{year}/{month}{year}.html"
        urls.append(url)
    return urls


def fetch_legislation_from_year(year: int, limit: int = 0) -> list:
    """Fetch legislation documents from a year index page."""
    index_url = f"{BASE_URL}/statutes/repacts/ra{year}/ra{year}.html"
    html = fetch_page(index_url)
    if not html:
        return []

    links = parse_index_links(html, index_url)
    # Filter to actual RA documents (ra_NNNN_YYYY.html pattern)
    doc_links = [l for l in links if re.search(r'ra_\d+', l["url"])]

    records = []
    for link_info in doc_links:
        if limit and len(records) >= limit:
            break

        time.sleep(1.5)
        doc_html = fetch_page(link_info["url"])
        if not doc_html:
            continue

        text = extract_full_text(doc_html)
        if not text or len(text) < 100:
            continue

        title = extract_title_from_html(doc_html)
        # Extract RA number from URL
        ra_match = re.search(r'ra_(\d+)', link_info["url"])
        ra_num = ra_match.group(1) if ra_match else ""

        records.append({
            "_id": f"PH/LawPhil/RA-{ra_num}" if ra_num else f"PH/LawPhil/{link_info['url'].split('/')[-1]}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": f"{year}-01-01",
            "url": link_info["url"],
            "document_number": f"RA {ra_num}" if ra_num else "",
            "document_type": "Republic Act",
            "year": year,
        })
        logger.info(f"  [{len(records)}] RA {ra_num}: {title[:60]}... ({len(text)} chars)")

    return records


def fetch_jurisprudence_from_month(year: int, month_idx: int, limit: int = 0) -> list:
    """Fetch court decisions from a monthly index page."""
    month = MONTHS[month_idx]
    index_url = f"{BASE_URL}/judjuris/juri{year}/{month}{year}/{month}{year}.html"
    html = fetch_page(index_url)
    if not html:
        return []

    links = parse_index_links(html, index_url)
    # Filter to actual decision documents (gr_ pattern or similar)
    doc_links = [l for l in links if re.search(r'(gr_|am_|ac_|oc_)\d+', l["url"])]

    records = []
    for link_info in doc_links:
        if limit and len(records) >= limit:
            break

        time.sleep(1.5)
        doc_html = fetch_page(link_info["url"])
        if not doc_html:
            continue

        text = extract_full_text(doc_html)
        if not text or len(text) < 100:
            continue

        title = extract_title_from_html(doc_html)
        # Extract GR number from URL
        gr_match = re.search(r'(gr|am|ac|oc)_(\d+)', link_info["url"])
        doc_num = f"{gr_match.group(1).upper()} {gr_match.group(2)}" if gr_match else ""

        date = f"{year}-{month_idx + 1:02d}-01"

        records.append({
            "_id": f"PH/LawPhil/{doc_num.replace(' ', '-')}" if doc_num else f"PH/LawPhil/{link_info['url'].split('/')[-1]}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": link_info["url"],
            "document_number": doc_num,
            "document_type": "Supreme Court Decision",
            "year": year,
            "month": month,
        })
        logger.info(f"  [{len(records)}] {doc_num}: {title[:60]}... ({len(text)} chars)")

    return records


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records: mix of legislation and case law."""
    records = []
    current_year = datetime.now().year

    # Get ~8 legislation records from recent years
    logger.info("Fetching legislation samples (Republic Acts)...")
    for year in range(current_year, current_year - 5, -1):
        if len(records) >= 8:
            break
        logger.info(f"  Checking RA year {year}...")
        year_records = fetch_legislation_from_year(year, limit=8 - len(records))
        records.extend(year_records)
        time.sleep(1)

    # Get ~7 case law records from recent months
    logger.info("Fetching jurisprudence samples (Supreme Court decisions)...")
    case_count = 0
    for year in range(current_year, current_year - 3, -1):
        if case_count >= 7:
            break
        for month_idx in range(11, -1, -1):
            if case_count >= 7:
                break
            logger.info(f"  Checking {MONTHS[month_idx]} {year}...")
            month_records = fetch_jurisprudence_from_month(year, month_idx, limit=7 - case_count)
            records.extend(month_records)
            case_count += len(month_records)
            if month_records:
                break  # Got records from this month, try next year
        time.sleep(1)

    return records[:count]


def fetch_all() -> Generator[dict, None, None]:
    """Yield all documents from legislation and jurisprudence."""
    current_year = datetime.now().year

    # Legislation: Republic Acts
    logger.info("Fetching all Republic Acts...")
    for year in range(current_year, 1945, -1):
        records = fetch_legislation_from_year(year)
        for r in records:
            yield r
        if records:
            logger.info(f"  Year {year}: {len(records)} acts")
        time.sleep(1)

    # Jurisprudence: Supreme Court decisions
    logger.info("Fetching all Supreme Court decisions...")
    for year in range(current_year, 1900, -1):
        for month_idx in range(11, -1, -1):
            records = fetch_jurisprudence_from_month(year, month_idx)
            for r in records:
                yield r
            time.sleep(0.5)
        logger.info(f"  Year {year} complete")


def test_api():
    """Test connectivity to LawPhil."""
    logger.info("Testing LawPhil connectivity...")

    # Test homepage
    html = fetch_page(BASE_URL)
    if html:
        logger.info(f"Homepage OK - {len(html)} bytes")
    else:
        logger.error("Homepage unreachable")
        return False

    # Test a Republic Acts index
    ra_url = f"{BASE_URL}/statutes/repacts/ra2024/ra2024.html"
    html = fetch_page(ra_url)
    if html:
        links = parse_index_links(html, ra_url)
        doc_links = [l for l in links if re.search(r'ra_\d+', l["url"])]
        logger.info(f"RA 2024 index OK - {len(doc_links)} documents")
    else:
        logger.warning("RA 2024 index not available")

    # Test a specific document
    test_url = f"{BASE_URL}/statutes/repacts/ra2024/ra_12066_2024.html"
    html = fetch_page(test_url)
    if html:
        text = extract_full_text(html)
        title = extract_title_from_html(html)
        logger.info(f"Document OK - '{title[:60]}' - {len(text)} chars")
        logger.info(f"Preview: {text[:200]}...")
        return True
    else:
        logger.error("Could not fetch test document")
        return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    types = set(r.get("_type", "") for r in records)
    logger.info(f"  - Data types: {', '.join(sorted(types))}")

    doc_types = set(r.get("document_type", "") for r in records)
    logger.info(f"  - Document types: {', '.join(sorted(doc_types))}")

    return len(records) >= 10 and avg_text > 200


def main():
    parser = argparse.ArgumentParser(description="PH/LawPhil Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode")
            count = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all():
                count += 1
                safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
