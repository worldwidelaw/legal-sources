#!/usr/bin/env python3
"""
MY/AGCLaws -- Malaysia Laws of Malaysia (Attorney General's Chambers)

Fetches legislation from the official AGC portal via DataTables JSON endpoints.
Full text is extracted from directly downloadable PDFs.

Strategy:
  - List acts via POST to json-updated-2024.php (consolidated acts, ~879 records)
  - List amendments via POST to json-amendment-2024.php (~404 records)
  - Download PDFs and extract text using PyPDF2
  - No auth required, no anti-bot protection

API:
  - Base: https://lom.agc.gov.my
  - Updated acts: POST /json-updated-2024.php {draw, start, length, language}
  - Amendments: POST /json-amendment-2024.php {draw, start, length, language}
  - PDFs: GET /ilims/upload/portal/akta/...

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import html as htmlmod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MY.AGCLaws")

BASE_URL = "https://lom.agc.gov.my"

# DataTables endpoints
ENDPOINTS = [
    {"path": "/json-updated-2024.php", "label": "Updated Acts", "category": "act"},
    {"path": "/json-amendment-2024.php", "label": "Amendment Acts", "category": "amendment"},
]


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using PyPDF2."""
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text.strip())
        return "\n\n".join(pages)
    except Exception as e:
        logger.debug(f"PyPDF2 failed: {e}")
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            text = page.get_text()
            if text:
                pages.append(text.strip())
        doc.close()
        return "\n\n".join(pages)
    except Exception as e:
        logger.debug(f"PyMuPDF failed: {e}")
    return ""


def clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    return text.strip()


def extract_pdf_url_from_html(html_str: str) -> Optional[str]:
    """Extract PDF URL from an HTML link string."""
    if not html_str:
        return None
    match = re.search(r'href=["\']([^"\']+\.pdf)["\']', html_str, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r'((?:/ilims|/upload)[^\s"\'<>]+\.pdf)', html_str, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def extract_pdf_url_from_json_field(json_str: str) -> Optional[str]:
    """Extract PDF path from a JSON-encoded field."""
    if not json_str:
        return None
    try:
        data = json.loads(json_str)
        if isinstance(data, dict):
            for key in ("path", "url", "pdfPath", "pdfUrl"):
                if key in data and data[key]:
                    return data[key]
            for v in data.values():
                if isinstance(v, str) and v.endswith(".pdf"):
                    return v
    except (json.JSONDecodeError, TypeError):
        pass
    return None


class AGCLawsScraper(BaseScraper):
    """Scraper for MY/AGCLaws -- Malaysia Attorney General's Chambers legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "en-MY,en;q=0.9",
            },
            timeout=120,
        )

    def _fetch_listing(self, endpoint: str, language: str = "BI",
                       start: int = 0, length: int = -1) -> Optional[Dict]:
        """Fetch a listing from a DataTables JSON endpoint."""
        self.rate_limiter.wait()
        try:
            form_data = {
                "draw": "1",
                "start": str(start),
                "length": str(length),
                "search[value]": "",
                "language": language,
            }
            resp = self.client.post(
                endpoint,
                data=form_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if not resp or resp.status_code != 200:
                logger.warning(f"Listing {endpoint}: HTTP {getattr(resp, 'status_code', 'N/A')}")
                return None
            return resp.json()
        except Exception as e:
            logger.warning(f"Error fetching {endpoint}: {e}")
            return None

    def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download a PDF file."""
        self.rate_limiter.wait()
        try:
            # Normalize relative paths like ../../../ilims/...
            if pdf_url.startswith("../") or pdf_url.startswith("./"):
                # Strip all leading ../
                cleaned = re.sub(r'^(?:\.\./)+', '', pdf_url)
                full_url = "/" + cleaned
            elif pdf_url.startswith("/"):
                full_url = pdf_url
                # JSON field returns /upload/... but correct path is /ilims/upload/...
                if full_url.startswith("/upload/"):
                    full_url = "/ilims" + full_url
            elif pdf_url.startswith("http"):
                full_url = pdf_url.replace(BASE_URL, "")
            else:
                full_url = "/" + pdf_url

            resp = self.client.get(full_url, headers={"Accept": "application/pdf"})
            if not resp or resp.status_code != 200:
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and len(resp.content) < 1000:
                return None
            return resp.content
        except Exception as e:
            logger.debug(f"PDF download error {pdf_url}: {e}")
            return None

    def _get_pdf_url_for_record(self, record: Dict, category: str) -> Optional[str]:
        """Extract the best PDF URL from a listing record."""
        # Try JSON-encoded PDF path field first
        for field in ("doc2downloadgeneratepdf",):
            val = record.get(field)
            if val:
                url = extract_pdf_url_from_json_field(val)
                if url:
                    return url

        # Try direct URL fields
        for field in ("URLDOCBI", "URLDOCBM", "DOC2DOWNLOADBI", "DOC2DOWNLOADBM",
                       "doc2download", "DOC2DOWNLOAD"):
            val = record.get(field)
            if val:
                url = extract_pdf_url_from_html(val)
                if url:
                    return url
                if isinstance(val, str) and val.strip().endswith(".pdf"):
                    return val.strip()

        return None

    def _extract_title(self, record: Dict) -> str:
        """Extract clean title from record."""
        for field in ("title", "TajukBI", "titleBI", "TAJUK_BI", "tajukBI"):
            val = record.get(field)
            if val:
                # For updated acts, extract just the first link text (English title)
                first_link = re.search(r'<a[^>]*>([^<]+)</a>', val)
                if first_link:
                    title = first_link.group(1).strip()
                    # Clean up newlines within title
                    title = re.sub(r'\s+', ' ', title)
                    return title
                return clean_html(val).strip()
        for field in ("TajukBM", "titleBM"):
            val = record.get(field)
            if val:
                return clean_html(val).strip()
        return "Unknown"

    def _extract_act_number(self, record: Dict) -> str:
        """Extract act number from record."""
        for field in ("lgt_act_no", "ACTNO_LEGISLATION", "nombor", "ACT_NO", "noPU"):
            val = record.get(field)
            if val:
                return clean_html(str(val)).strip()
        return ""

    def _extract_date(self, record: Dict) -> Optional[str]:
        """Extract and format date from record."""
        # Try explicit date fields first
        for field in ("PUBLICATIONDATE", "publicationDate", "ROYALASSENTDATE",
                       "dateCreated", "lgt_update_date"):
            val = record.get(field)
            if not val:
                continue
            val = clean_html(str(val)).strip()
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(val[:19], fmt)
                    return dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    continue

        # Try extracting date from title HTML (e.g. <i>25-03-2026</i>)
        title_html = record.get("title", "")
        if title_html:
            date_match = re.search(r'(\d{2}-\d{2}-\d{4})', title_html)
            if date_match:
                try:
                    dt = datetime.strptime(date_match.group(1), "%d-%m-%Y")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all legislation records from AGC DataTables endpoints."""
        for ep in ENDPOINTS:
            path = ep["path"]
            label = ep["label"]
            category = ep["category"]

            logger.info(f"Fetching {label} from {path}...")
            data = self._fetch_listing(path)
            if not data:
                logger.warning(f"Cannot access {label}")
                continue

            records = data.get("records", data.get("data", []))
            total = data.get("recordsTotal", len(records))
            logger.info(f"{label}: {total} records returned")

            for record in records:
                yield {
                    "_category": category,
                    "_endpoint": path,
                    "_record": record,
                }

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch all records (no date filtering available in API)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema, downloading and extracting PDF text."""
        record = raw.get("_record", {})
        category = raw.get("_category", "act")

        title = self._extract_title(record)
        act_no = self._extract_act_number(record)
        date_str = self._extract_date(record)

        if not title or title == "Unknown":
            return None

        pdf_url = self._get_pdf_url_for_record(record, category)
        if not pdf_url:
            logger.debug(f"No PDF URL for: {title}")
            return None

        pdf_bytes = self._download_pdf(pdf_url)
        if not pdf_bytes:
            logger.debug(f"Failed to download PDF for: {title}")
            return None

        text = extract_pdf_text(pdf_bytes)
        if not text or len(text) < 50:
            logger.debug(f"No text extracted from PDF for: {title}")
            return None

        doc_id = act_no or title[:50]
        doc_id = re.sub(r'[^a-zA-Z0-9._-]', '_', doc_id)

        web_url = f"{BASE_URL}/act-detail.php?language=BI&act={act_no}" if act_no else BASE_URL

        return {
            "_id": f"MY-AGC-{category}-{doc_id}",
            "_source": "MY/AGCLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{act_no} - {title}" if act_no else title,
            "text": text,
            "date": date_str,
            "url": web_url,
            "act_number": act_no,
            "category": category,
            "language": "en",
            "jurisdiction": "MY",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Malaysia AGC Laws API...")

        for ep in ENDPOINTS:
            path = ep["path"]
            label = ep["label"]
            category = ep["category"]

            print(f"\n--- {label} ({path}) ---")
            data = self._fetch_listing(path, length=3)
            if not data:
                print("  FAILED: No response")
                continue

            records = data.get("records", data.get("data", []))
            total = data.get("recordsTotal", 0)
            print(f"  Total: {total:,} records")

            if records:
                record = records[0]
                title = self._extract_title(record)
                act_no = self._extract_act_number(record)
                print(f"  First: {act_no} - {title[:80]}")

                pdf_url = self._get_pdf_url_for_record(record, category)
                if pdf_url:
                    print(f"  PDF URL: {pdf_url[:100]}")
                    pdf_bytes = self._download_pdf(pdf_url)
                    if pdf_bytes:
                        print(f"  PDF size: {len(pdf_bytes):,} bytes")
                        text = extract_pdf_text(pdf_bytes)
                        print(f"  Extracted text: {len(text)} chars")
                        if text:
                            print(f"  Sample: {text[:200]}...")
                    else:
                        print("  FAILED: Could not download PDF")
                else:
                    print("  FAILED: No PDF URL found")

        print("\nTest complete!")


def main():
    scraper = AGCLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
