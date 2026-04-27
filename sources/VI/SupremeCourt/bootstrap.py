#!/usr/bin/env python3
"""
VI/SupremeCourt -- US Virgin Islands Supreme Court Opinions

Fetches published and unpublished opinions from supreme.vicourts.org.

Strategy:
  - Parse the published and unpublished opinions pages for PDF links
  - Each opinion is in a <tr> with columns: title, date, case number,
    author, citation, and summary link
  - Download PDFs and extract full text via PyMuPDF (fitz)
  - ~600 opinions total (492 published + 104 unpublished)

Data Coverage:
  - Published opinions from 2007 to present
  - Unpublished opinions
  - Language: English
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Quick connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from common.pdf_extract import extract_pdf_markdown
except ImportError:
    extract_pdf_markdown = None

SOURCE_ID = "VI/SupremeCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VI.SupremeCourt")

BASE_URL = "https://supreme.vicourts.org"
PUBLISHED_PATH = "/court_opinions/published_opinions"
UNPUBLISHED_PATH = "/court_opinions/unpublished_opinions"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# Pattern to extract table rows with opinion data
# Columns: title (link to PDF), date, case number, author, citation (link to PDF), summary link
ROW_PATTERN = re.compile(
    r'<tr>\s*'
    r'<td>\s*<a\s+href=["\']([^"\']+\.pdf)["\'][^>]*>([^<]+)</a>\s*</td>\s*'
    r'<td[^>]*>(\d{4}/\d{2}/\d{2})</td>\s*'
    r'<td>([^<]*)</td>\s*'
    r'<td>([^<]*)</td>\s*'
    r'<td>\s*<a\s+href=["\']([^"\']+\.pdf)["\'][^>]*title=["\']([^"\']*)["\'][^>]*>[^<]*</a>\s*</td>',
    re.DOTALL
)

# Simpler fallback pattern - just find PDF links with associated metadata
SIMPLE_ROW = re.compile(
    r'<tr>\s*<td>\s*<a\s+href=["\']([^"\']*\.pdf)["\'][^>]*title=["\']([^"\']*)["\'][^>]*>([^<]*)</a>'
    r'.*?</td>\s*<td[^>]*>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>',
    re.DOTALL
)


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def parse_opinions_page(html: str, opinion_type: str = "published") -> list:
    """Parse an opinions page and extract metadata for each opinion."""
    records = []
    seen_pdfs = set()

    # Try the detailed row pattern first
    for m in ROW_PATTERN.finditer(html):
        pdf_path, title, date_str, case_num, author, _, citation = m.groups()
        pdf_path = pdf_path.strip()
        if pdf_path in seen_pdfs:
            continue
        seen_pdfs.add(pdf_path)
        records.append({
            "pdf_path": pdf_path,
            "title": title.strip(),
            "date": date_str.strip(),
            "case_number": case_num.strip(),
            "author": author.strip(),
            "citation": citation.strip(),
            "opinion_type": opinion_type,
        })

    # Fallback: try simpler pattern
    if not records:
        for m in SIMPLE_ROW.finditer(html):
            pdf_path, title_attr, title_text, date_str, case_num, author = m.groups()
            pdf_path = pdf_path.strip()
            if pdf_path in seen_pdfs:
                continue
            seen_pdfs.add(pdf_path)
            title = title_attr.strip() or title_text.strip()
            records.append({
                "pdf_path": pdf_path,
                "title": title,
                "date": date_str.strip(),
                "case_number": case_num.strip(),
                "author": author.strip(),
                "citation": "",
                "opinion_type": opinion_type,
            })

    # Final fallback: extract all PDF links with whatever metadata we can find
    if not records:
        logger.info("Using fallback PDF extraction")
        rows = re.findall(
            r'<tr>(.*?)</tr>',
            html, re.DOTALL
        )
        for row_html in rows:
            pdf_match = re.search(r'href=["\']([^"\']*\.pdf)["\']', row_html)
            if not pdf_match:
                continue
            pdf_path = pdf_match.group(1)
            if pdf_path in seen_pdfs:
                continue
            seen_pdfs.add(pdf_path)

            # Extract title from link text or title attr
            title_match = re.search(
                r'<a[^>]*href=["\'][^"\']*\.pdf["\'][^>]*title=["\']([^"\']+)["\']',
                row_html
            )
            if not title_match:
                title_match = re.search(
                    r'<a[^>]*href=["\'][^"\']*\.pdf["\'][^>]*>([^<]+)</a>',
                    row_html
                )
            title = title_match.group(1).strip() if title_match else Path(pdf_path).stem

            # Extract date
            date_match = re.search(r'(\d{4}/\d{2}/\d{2})', row_html)
            date_str = date_match.group(1) if date_match else ""

            # Extract case number (SCT-CIV-YYYY-NNNN or SCT-CRM-YYYY-NNNN)
            case_match = re.search(r'(SCT-\w+-\d{4}-\d+)', row_html)
            case_num = case_match.group(1) if case_match else ""

            # Extract author
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
            author = ""
            if len(tds) >= 4:
                author_text = re.sub(r'<[^>]+>', '', tds[3]).strip()
                if author_text and not author_text.startswith('http'):
                    author = author_text

            # Extract citation
            citation = ""
            cite_match = re.search(r'title=["\'](\d{4}\s+VI\s+\d+)', row_html)
            if cite_match:
                citation = cite_match.group(1).strip()

            records.append({
                "pdf_path": pdf_path,
                "title": title,
                "date": date_str,
                "case_number": case_num,
                "author": author,
                "citation": citation,
                "opinion_type": opinion_type,
            })

    return records


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using available backends."""
    # Try common pdf_extract first
    if extract_pdf_markdown:
        try:
            text = extract_pdf_markdown("temp", SOURCE_ID, pdf_bytes=pdf_bytes)
            if text and len(text.strip()) > 100:
                return text.strip()
        except Exception:
            pass

    # Fallback to PyMuPDF directly
    if fitz:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            pages = []
            for page in doc:
                t = page.get_text()
                if t:
                    pages.append(t)
            doc.close()
            text = "\n\n".join(pages)
            if len(text.strip()) > 100:
                return text.strip()
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")

    # Fallback to pdfplumber
    try:
        import pdfplumber
        import io
        pages = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
        text = "\n\n".join(pages)
        if len(text.strip()) > 100:
            return text.strip()
    except Exception:
        pass

    return None


def normalize_date(date_str: str) -> Optional[str]:
    """Convert YYYY/MM/DD to ISO 8601."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%Y/%m/%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def make_id(case_number: str, pdf_path: str) -> str:
    """Generate a unique document ID."""
    if case_number:
        return f"VI_SC_{case_number.replace(' ', '_')}"
    # Fallback to filename
    name = Path(pdf_path).stem
    name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
    return f"VI_SC_{name}"


def fetch_all(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all opinions from the Supreme Court website."""
    pages = [
        (PUBLISHED_PATH, "published"),
        (UNPUBLISHED_PATH, "unpublished"),
    ]

    count = 0
    for path, opinion_type in pages:
        url = f"{BASE_URL}{path}"
        logger.info(f"Fetching {opinion_type} opinions from {url}")

        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            continue

        records = parse_opinions_page(resp.text, opinion_type)
        logger.info(f"Found {len(records)} {opinion_type} opinions")

        for rec in records:
            if sample and count >= 15:
                return

            pdf_url = rec["pdf_path"]
            if not pdf_url.startswith("http"):
                pdf_url = f"{BASE_URL}{pdf_url}"

            # URL-encode spaces in path
            parsed = urllib.parse.urlparse(pdf_url)
            encoded_path = urllib.parse.quote(parsed.path, safe="/")
            pdf_url = urllib.parse.urlunparse(parsed._replace(path=encoded_path))

            logger.info(f"Downloading: {rec['title'][:60]}...")
            time.sleep(2)

            try:
                pdf_resp = session.get(pdf_url, timeout=60)
                pdf_resp.raise_for_status()
                pdf_bytes = pdf_resp.content
            except requests.RequestException as e:
                logger.warning(f"Failed to download PDF {pdf_url}: {e}")
                continue

            if len(pdf_bytes) < 1000:
                logger.warning(f"PDF too small ({len(pdf_bytes)} bytes), skipping")
                continue

            text = extract_text_from_pdf(pdf_bytes)
            if not text:
                logger.warning(f"No text extracted from {pdf_url}")
                continue

            doc_id = make_id(rec["case_number"], rec["pdf_path"])
            iso_date = normalize_date(rec["date"])

            doc = {
                "_id": doc_id,
                "_source": SOURCE_ID,
                "_type": "case_law",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": rec["title"],
                "text": text,
                "date": iso_date,
                "url": pdf_url,
                "case_number": rec["case_number"] or None,
                "citation": rec["citation"] or None,
                "author": rec["author"] or None,
                "opinion_type": rec["opinion_type"],
                "court": "Supreme Court of the Virgin Islands",
            }

            count += 1
            logger.info(f"[{count}] {doc_id}: {len(text)} chars")
            yield doc

    logger.info(f"Total opinions fetched: {count}")


def save_record(record: dict, sample_dir: Path):
    """Save a record to the sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', record["_id"])
    filepath = sample_dir / f"record_{safe_id}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2, default=str)
    return filepath


def test_api():
    """Test connectivity to the Supreme Court website."""
    session = create_session()
    url = f"{BASE_URL}{PUBLISHED_PATH}"
    try:
        resp = session.get(url, timeout=15)
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
        print(f"Content-Length: {len(resp.content)} bytes")

        records = parse_opinions_page(resp.text, "published")
        print(f"Published opinions found: {len(records)}")
        if records:
            r = records[0]
            print(f"  First: {r['title'][:60]} ({r['date']}) {r['case_number']}")
        return True
    except Exception as e:
        print(f"FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="VI/SupremeCourt data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    args = parser.parse_args()

    if args.command in ("test-api", "test"):
        success = test_api()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        session = create_session()
        count = 0
        for record in fetch_all(session, sample=args.sample):
            filepath = save_record(record, SAMPLE_DIR)
            count += 1
            logger.info(f"Saved {filepath.name}")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")
        if count == 0:
            print("WARNING: No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
