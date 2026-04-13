#!/usr/bin/env python3
"""
US/DOL -- Department of Labor Administrative Review Board (ARB) Decisions

Fetches ARB decisions via monthly caselist HTML pages on oalj.dol.gov and dol.gov.
Decisions are published as PDF files; full text is extracted via pdfplumber.

Data access:
  - Monthly caselist index at ARBINDEX.HTM (oalj.dol.gov, 1996-2020)
  - Monthly caselist pages on dol.gov (2020+)
  - PDF decision documents with full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DOL")

OLD_INDEX_BASE = "https://www.oalj.dol.gov/PUBLIC/ARB/REFERENCES/CASELISTS/"
NEW_INDEX_BASE = "https://www.dol.gov/agencies/oalj/PUBLIC/ARB/REFERENCES/CASELISTS/"
OLD_BASE = "https://www.oalj.dol.gov"
NEW_BASE = "https://www.dol.gov"
DELAY = 2.0


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def clean_text(text: str) -> str:
    """Clean extracted text: normalize whitespace, remove junk."""
    if not text:
        return ""
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/DOL",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def extract_htm_text(html: str) -> str:
    """Extract text from HTML decision page."""
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        return clean_text(soup.get_text(separator="\n", strip=True))
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    return clean_text(text)


def parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip().replace(".", "")
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def generate_monthly_urls() -> List[Dict[str, str]]:
    """Generate all monthly caselist URLs from May 1996 to April 2020.

    Note: The old oalj.dol.gov site hosts caselists from 1996-2020 and is
    accessible without bot protection. The newer dol.gov site uses challenge
    validation that blocks programmatic access, so we only use the old site.
    """
    urls = []

    # Old site: May 1996 to April 2020
    # Uses MMYY.HTM for 1996-1999, MM_YYYY.HTM for 2000+
    for year in range(1996, 2021):
        start_month = 5 if year == 1996 else 1
        end_month = 4 if year == 2020 else 12
        for month in range(start_month, end_month + 1):
            if year <= 1999:
                slug = f"{month:02d}{year % 100:02d}.HTM"
            else:
                slug = f"{month:02d}_{year}.HTM"
            urls.append({
                "url": OLD_INDEX_BASE + slug,
                "base": OLD_BASE,
                "era": "old",
                "year": year,
                "month": month,
            })

    return urls


def parse_caselist_page(html: str, base_url: str, page_url: str) -> List[Dict[str, Any]]:
    """Parse a monthly caselist page to extract decision entries."""
    if not HAS_BS4:
        logger.error("BeautifulSoup required for parsing caselists")
        return []

    soup = BeautifulSoup(html, "html.parser")
    seen_urls = set()
    decisions = []

    # Find all links to PDF/HTM decision files
    for link in soup.find_all("a", href=True):
        href = link["href"]
        href_lower = href.lower()

        # Only interested in decision document links
        if not (href_lower.endswith(".pdf") or href_lower.endswith(".htm")):
            continue
        if "DECISIONS" not in href.upper() and "decisions" not in href.lower():
            continue

        # Build absolute URL
        if href.startswith("http"):
            doc_url = href
        elif href.startswith("/"):
            doc_url = base_url + href
        else:
            doc_url = urljoin(page_url, href)

        # Deduplicate: same PDF appears multiple times (citation + summary)
        if doc_url in seen_urls:
            continue
        seen_urls.add(doc_url)

        # Extract surrounding text for metadata
        parent = link.find_parent(["li", "p", "tr", "div"])
        context_text = parent.get_text(separator=" ", strip=True) if parent else ""

        # Parse case name from link text or context
        case_name = link.get_text(strip=True)
        if not case_name or len(case_name) < 3:
            case_name = context_text.split("ARB")[0].strip() if "ARB" in context_text else context_text[:200]

        # Extract ARB number
        arb_match = re.search(r'ARB\s*(?:No\.?\s*)?(\d{4}[-–]\d{3,5})', context_text)
        arb_number = arb_match.group(1) if arb_match else None

        # Extract ALJ number
        alj_match = re.search(r'ALJ\s*(?:No\.?\s*)?(\d{4}[-–][A-Z]{2,5}[-–]\d{3,5})', context_text)
        alj_number = alj_match.group(1) if alj_match else None

        # Extract date from context (e.g., "Apr. 21, 2020" or "April 21, 2020")
        date_match = re.search(
            r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})',
            context_text
        )
        if not date_match:
            # Try abbreviated months: "Apr. 21, 2020"
            date_match = re.search(
                r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4})',
                context_text
            )
        date_str = parse_date(date_match.group(1)) if date_match else None

        # Detect program area from URL path
        program_match = re.search(r'/([A-Z]{2,5})/\d', href.upper())
        program_area = program_match.group(1) if program_match else None

        decisions.append({
            "case_name": case_name,
            "arb_number": arb_number,
            "alj_number": alj_number,
            "date": date_str,
            "program_area": program_area,
            "doc_url": doc_url,
            "context": context_text[:500],
        })

    return decisions


def fetch_decision_text(session: requests.Session, doc_url: str) -> str:
    """Download a decision document and extract full text."""
    try:
        resp = session.get(doc_url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {doc_url}: {e}")
        return ""

    if doc_url.lower().endswith(".pdf"):
        return extract_pdf_text(resp.content)
    else:
        return extract_htm_text(resp.text)


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a raw decision record."""
    case_name = raw.get("case_name", "").strip()
    arb_num = raw.get("arb_number", "")
    doc_url = raw.get("doc_url", "")

    # Generate stable ID from ARB number or URL
    if arb_num:
        _id = f"US-DOL-ARB-{arb_num}"
    else:
        _id = f"US-DOL-{doc_url.split('/')[-1].replace('.', '-')}"

    return {
        "_id": _id,
        "_source": "US/DOL",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": case_name,
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": doc_url,
        "arb_number": arb_num,
        "alj_number": raw.get("alj_number"),
        "program_area": raw.get("program_area"),
        "court": "US Department of Labor Administrative Review Board",
    }


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    """Fetch all ARB decisions from monthly caselists."""
    session = get_session()
    monthly_urls = generate_monthly_urls()

    if sample:
        # For sample, use last 12 months of available data (most recent)
        monthly_urls = monthly_urls[-12:]

    total_yielded = 0
    for entry in reversed(monthly_urls):  # newest first
        url = entry["url"]
        base = entry["base"]
        logger.info(f"Fetching caselist: {url}")

        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404:
                logger.debug(f"No caselist for {entry['year']}-{entry['month']:02d}")
                continue
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch caselist {url}: {e}")
            continue

        decisions = parse_caselist_page(resp.text, base, url)
        logger.info(f"  Found {len(decisions)} decisions")

        for dec in decisions:
            time.sleep(DELAY)
            logger.info(f"  Fetching: {dec['case_name'][:60]}...")
            text = fetch_decision_text(session, dec["doc_url"])
            if not text:
                logger.warning(f"  No text extracted for {dec['doc_url']}")
                continue

            dec["text"] = text
            record = normalize(dec)

            if record["text"] and len(record["text"]) > 100:
                yield record
                total_yielded += 1
                logger.info(f"  Record {total_yielded}: {record['title'][:60]} ({len(record['text'])} chars)")

                if sample and total_yielded >= 12:
                    logger.info(f"Sample complete: {total_yielded} records")
                    return

        time.sleep(DELAY)

    logger.info(f"Total records fetched: {total_yielded}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    """Fetch decisions updated since a given date."""
    since_date = datetime.fromisoformat(since).date()
    for record in fetch_all():
        if record.get("date"):
            try:
                rec_date = datetime.fromisoformat(record["date"]).date()
                if rec_date >= since_date:
                    yield record
                else:
                    return  # Newest first, so we can stop
            except ValueError:
                yield record


def test_connectivity() -> bool:
    """Quick connectivity test."""
    session = get_session()
    try:
        resp = session.get(OLD_INDEX_BASE + "ARBINDEX.HTM", timeout=15)
        resp.raise_for_status()
        logger.info(f"Old site OK: {resp.status_code}")
    except Exception as e:
        logger.error(f"Old site failed: {e}")
        return False

    try:
        resp = session.get(NEW_INDEX_BASE + "ARBINDEX", timeout=15)
        if resp.status_code < 400:
            logger.info(f"New site OK: {resp.status_code}")
    except Exception as e:
        logger.warning(f"New site check: {e}")

    return True


def main():
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    if command == "bootstrap":
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_all(sample=sample):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  [{count}] {record['title'][:70]}")
        print(f"\nDone: {count} records saved to {sample_dir}/")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in fetch_updates(since):
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Updated: {count} records since {since}")


if __name__ == "__main__":
    main()
