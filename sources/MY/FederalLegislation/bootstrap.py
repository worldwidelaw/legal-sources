#!/usr/bin/env python3
"""
MY/FederalLegislation -- Laws of Malaysia Online (Attorney General's Chambers)

Fetches Malaysian federal principal acts from lom.agc.gov.my.

Strategy:
  - List all acts via the DataTable JSON endpoint (json-updated-2024.php)
  - Parse PDF download URLs from the JSON response (English versions preferred)
  - Download PDFs and extract full text using PyPDF2
  - 878+ principal acts available in bilingual PDF format

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote, unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MY.FederalLegislation")

BASE_URL = "https://lom.agc.gov.my"
JSON_ENDPOINT = f"{BASE_URL}/json-updated-2024.php"
PAGE_SIZE = 50


class MYFederalLegislationScraper(BaseScraper):
    """
    Scraper for MY/FederalLegislation -- Laws of Malaysia Online.
    Country: MY
    URL: https://lom.agc.gov.my/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=120,
        )

    def _fetch_act_list(self, start: int = 0, length: int = PAGE_SIZE) -> dict:
        """Fetch act listings from the DataTable JSON endpoint."""
        self.rate_limiter.wait()
        resp = self.client.post(
            JSON_ENDPOINT,
            data={
                "draw": "1",
                "start": str(start),
                "length": str(length),
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_act_record(self, record: dict) -> Optional[dict]:
        """Parse a single act record from the JSON response."""
        act_no = record.get("lgt_act_no", "").strip()
        if not act_no:
            return None

        # Extract English title from the HTML title field
        title_html = record.get("title", "")
        title_en = ""
        date_str = ""

        # English title is in the second <a> link (lang=BI)
        en_matches = re.findall(
            r'<a\s+href="act-detail\.php\?[^"]*lang=BI[^"]*"[^>]*>([^<]+)</a>',
            title_html,
        )
        if en_matches:
            title_en = en_matches[0].strip().replace("\n", " ")
        else:
            # Fallback: try first link
            any_matches = re.findall(r'<a[^>]*>([^<]+)</a>', title_html)
            if any_matches:
                title_en = any_matches[0].strip().replace("\n", " ")

        # Extract date ("As At dd-mm-yyyy")
        date_match = re.search(r'As At\s*</i><i>(\d{2}-\d{2}-\d{4})', title_html)
        if not date_match:
            date_match = re.search(r'Sebagaimana Pada\s*</i><i>(\d{2}-\d{2}-\d{4})', title_html)
        if date_match:
            try:
                dt = datetime.strptime(date_match.group(1), "%d-%m-%Y")
                date_str = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract English PDF URL from doc2downloadgeneratepdf JSON
        pdf_url = None
        pdf_json_str = record.get("doc2downloadgeneratepdf", "")
        if pdf_json_str:
            try:
                pdf_entries = json.loads(pdf_json_str)
                for entry in pdf_entries:
                    icon = entry.get("icon", "")
                    if "en" in icon.lower():
                        path = entry.get("path", "")
                        doc_name = entry.get("docName", "")
                        if path and doc_name:
                            # URL-encode the filename for spaces etc.
                            pdf_url = f"{BASE_URL}/ilims{path}{quote(doc_name)}"
                        break
                # Fallback to first PDF if no English found
                if not pdf_url and pdf_entries:
                    entry = pdf_entries[0]
                    path = entry.get("path", "")
                    doc_name = entry.get("docName", "")
                    if path and doc_name:
                        pdf_url = f"{BASE_URL}/ilims{path}{quote(doc_name)}"
            except (json.JSONDecodeError, KeyError):
                pass

        # Fallback: parse PDF URL from doc2download HTML
        if not pdf_url:
            doc_html = record.get("doc2download", "")
            # Prefer English PDF (pdf-en)
            en_pdf = re.search(
                r'href="(\.\./\.\./\.\./[^"]+)"[^>]*class="event_kira_download[^"]*">\s*<img[^>]*pdf-en',
                doc_html,
            )
            if en_pdf:
                rel_path = en_pdf.group(1).replace("../../../", "/")
                pdf_url = f"{BASE_URL}{rel_path}"
            else:
                any_pdf = re.search(r'href="(\.\./\.\./\.\./[^"]+)"', doc_html)
                if any_pdf:
                    rel_path = any_pdf.group(1).replace("../../../", "/")
                    pdf_url = f"{BASE_URL}{rel_path}"

        return {
            "act_no": act_no,
            "title": title_en or f"Act {act_no}",
            "date": date_str,
            "pdf_url": pdf_url,
            "detail_url": f"{BASE_URL}/act-detail.php?type=principal&lang=BI&act={act_no}",
        }

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MY/FederalLegislation",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url, allow_redirects=True)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.content
            return None
        except Exception as e:
            logger.warning("Failed to download PDF from %s: %s", url, e)
            return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw record into standard schema."""
        return {
            "_id": f"MY_Act_{raw['act_no']}",
            "_source": "MY/FederalLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": raw.get("date") or None,
            "url": raw.get("detail_url", ""),
            "act_number": raw["act_no"],
            "language": "en",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Malaysian federal acts with full text."""
        logger.info("Fetching act list from LOM JSON endpoint...")

        # Get total count first
        initial = self._fetch_act_list(start=0, length=1)
        total = initial.get("recordsTotal", 0)
        logger.info("Total acts available: %d", total)

        start = 0
        fetched = 0
        while start < total:
            data = self._fetch_act_list(start=start, length=PAGE_SIZE)
            records = data.get("records", [])
            if not records:
                break

            for record in records:
                parsed = self._parse_act_record(record)
                if not parsed:
                    continue

                # Download PDF and extract text
                if parsed["pdf_url"]:
                    pdf_bytes = self._download_pdf(parsed["pdf_url"])
                    if pdf_bytes:
                        text = self._extract_pdf_text(pdf_bytes)
                        parsed["text"] = text
                        if text:
                            logger.info(
                                "Act %s: %s (%d chars)",
                                parsed["act_no"],
                                parsed["title"][:60],
                                len(text),
                            )
                        else:
                            logger.warning(
                                "Act %s: PDF downloaded but text extraction empty",
                                parsed["act_no"],
                            )
                    else:
                        parsed["text"] = ""
                        logger.warning("Act %s: PDF download failed", parsed["act_no"])
                else:
                    parsed["text"] = ""
                    logger.warning("Act %s: no PDF URL found", parsed["act_no"])

                if parsed.get("text"):
                    yield parsed
                    fetched += 1

            start += PAGE_SIZE
            logger.info("Progress: %d/%d acts processed", min(start, total), total)

        logger.info("Fetch complete: %d acts with full text", fetched)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updated acts. Same as fetch_all since no date filter on API."""
        yield from self.fetch_all()

    def test_api(self):
        """Quick connectivity test."""
        logger.info("Testing LOM JSON API endpoint...")
        data = self._fetch_act_list(start=0, length=3)
        total = data.get("recordsTotal", 0)
        records = data.get("records", [])
        logger.info("API OK: %d total acts, got %d in test batch", total, len(records))

        if records:
            parsed = self._parse_act_record(records[0])
            if parsed:
                logger.info("First act: %s - %s", parsed["act_no"], parsed["title"])
                if parsed["pdf_url"]:
                    logger.info("PDF URL: %s", parsed["pdf_url"])
                    pdf_bytes = self._download_pdf(parsed["pdf_url"])
                    if pdf_bytes:
                        text = self._extract_pdf_text(pdf_bytes)
                        logger.info(
                            "PDF text extraction: %d chars", len(text)
                        )
                        if text:
                            logger.info("First 200 chars: %s", text[:200])
                    else:
                        logger.error("PDF download failed!")
                else:
                    logger.error("No PDF URL parsed!")
        return True


def main():
    scraper = MYFederalLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample_mode else None

        for record in scraper.fetch_all():
            if sample_mode:
                out_path = sample_dir / f"{record['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info("Saved sample: %s", out_path.name)

            count += 1
            if limit and count >= limit:
                logger.info("Sample limit reached (%d records)", limit)
                break

        logger.info(
            "Done: %d records %s",
            count,
            "(sample mode)" if sample_mode else "",
        )
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
