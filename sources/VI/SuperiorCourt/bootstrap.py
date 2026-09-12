#!/usr/bin/env python3
"""
VI/SuperiorCourt -- US Virgin Islands Superior Court Opinions

Fetches published and unpublished opinions from superior.vicourts.org.

Strategy:
  - Parse the published and unpublished opinions pages for PDF links
  - Each opinion is in a <tr> with columns: Case Caption (PDF link), Date,
    Case Number, Judge, Citation (PDF link), Summary link
  - Download PDFs and extract full text via PyMuPDF (fitz) / pdfplumber
  - ~527 opinions total (217 published + 310 unpublished)

Data Coverage:
  - Published opinions from 2001 to present
  - Unpublished opinions
  - Language: English
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Quick connectivity test
"""

import argparse
import html
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

SOURCE_ID = "VI/SuperiorCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VI.SuperiorCourt")

BASE_URL = "https://superior.vicourts.org"
PUBLISHED_PATH = "/court_opinions/published_opinions"
UNPUBLISHED_PATH = "/court_opinions/unpublished_opinions"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def parse_opinions_page(page_html: str, opinion_type: str = "published") -> list:
    """Parse an opinions page and extract metadata for each opinion."""
    records = []
    seen_pdfs = set()

    rows = re.findall(r'<tr>(.*?)</tr>', page_html, re.DOTALL)
    for row_html in rows:
        pdf_match = re.search(r'href=["\x27]([^"\x27]*\.pdf)["\x27]', row_html)
        if not pdf_match:
            continue
        pdf_path = pdf_match.group(1)
        if pdf_path in seen_pdfs:
            continue
        seen_pdfs.add(pdf_path)

        # Extract title from first link's title attribute
        title_match = re.search(
            r'<a[^>]*href=["\x27][^"\x27]*\.pdf["\x27][^>]*title=["\x27]([^"\x27]+)["\x27]',
            row_html
        )
        if not title_match:
            title_match = re.search(
                r'<a[^>]*href=["\x27][^"\x27]*\.pdf["\x27][^>]*>([^<]+)</a>',
                row_html
            )
        title = html.unescape(title_match.group(1).strip()) if title_match else Path(pdf_path).stem

        # Extract columns
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)

        date_str = ""
        case_number = ""
        judge = ""
        citation = ""

        if len(tds) >= 2:
            date_match = re.search(r'(\d{4}/\d{2}/\d{2})', tds[1])
            date_str = date_match.group(1) if date_match else ""

        if len(tds) >= 3:
            case_number = re.sub(r'<[^>]+>', '', tds[2]).strip()

        if len(tds) >= 4:
            judge = re.sub(r'<[^>]+>', '', tds[3]).strip()

        if len(tds) >= 5:
            cite_match = re.search(r'title=["\x27](\d{4}\s+VI\s+Super\s+\d+\s*\w*)["\x27]', tds[4])
            if cite_match:
                citation = cite_match.group(1).strip()

        records.append({
            "pdf_path": pdf_path,
            "title": title,
            "date": date_str,
            "case_number": case_number,
            "judge": judge,
            "citation": citation,
            "opinion_type": opinion_type,
        })

    return records


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using available backends."""
    if extract_pdf_markdown:
        try:
            text = extract_pdf_markdown("temp", SOURCE_ID, pdf_bytes=pdf_bytes)
            if text and len(text.strip()) > 100:
                return text.strip()
        except Exception:
            pass

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

    try:
        import pdfplumber
        import io
        pages = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
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
        return f"VI_Sup_{case_number.replace(' ', '_')}"
    name = Path(pdf_path).stem
    name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
    return f"VI_Sup_{name}"


def fetch_all(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all opinions from the Superior Court website."""
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
                "judge": rec["judge"] or None,
                "opinion_type": rec["opinion_type"],
                "court": "Superior Court of the Virgin Islands",
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
    """Test connectivity to the Superior Court website."""
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
    parser = argparse.ArgumentParser(description="VI/SuperiorCourt data fetcher")
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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
