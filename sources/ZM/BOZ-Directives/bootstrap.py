#!/usr/bin/env python3
"""
ZM/BOZ-Directives -- Bank of Zambia Regulatory Framework

Fetches directives, circulars, regulations, acts, guidelines, and orders
from the Bank of Zambia's Drupal JSON:API.

Strategy:
  - Enumerate regulatory_framework nodes via Drupal JSON:API
  - Include field_regulatory_file to get PDF attachment metadata
  - Download PDFs from /sites/default/files/ and extract full text

Endpoints:
  - Nodes: https://www.boz.zm/jsonapi/node/regulatory_framework
  - Files: https://www.boz.zm/sites/default/files/{path}

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZM.BOZ-Directives")

BASE_URL = "https://www.boz.zm"
API_URL = f"{BASE_URL}/jsonapi/node/regulatory_framework"
PAGE_LIMIT = 50


class BOZDirectivesScraper(BaseScraper):
    """Scraper for ZM/BOZ-Directives."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                "Accept": "application/vnd.api+json",
            },
            timeout=60,
        )

    def _get_json(self, url: str) -> Optional[dict]:
        """GET JSON from Drupal JSON:API with retry."""
        for attempt in range(3):
            try:
                resp = self.http.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:100]}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _build_file_map(self, included: list) -> dict:
        """Build a map of file UUID -> file metadata from included resources."""
        file_map = {}
        for item in included:
            if item.get("type") == "file--file":
                attrs = item.get("attributes", {})
                uri = attrs.get("uri", {})
                file_map[item["id"]] = {
                    "filename": attrs.get("filename", ""),
                    "url": uri.get("url", ""),
                    "filemime": attrs.get("filemime", ""),
                    "filesize": attrs.get("filesize", 0),
                }
        return file_map

    def _build_taxonomy_map(self, included: list) -> dict:
        """Build a map of taxonomy UUID -> name from included resources."""
        tax_map = {}
        for item in included:
            if "taxonomy_term" in item.get("type", ""):
                attrs = item.get("attributes", {})
                tax_map[item["id"]] = attrs.get("name", "")
        return tax_map

    def _extract_pdf_text(self, pdf_path: str, doc_id: str) -> Optional[str]:
        """Download a PDF from BOZ and extract text."""
        pdf_url = f"{BASE_URL}{pdf_path}"
        try:
            text = extract_pdf_markdown(
                "ZM/BOZ-Directives",
                doc_id,
                pdf_url=pdf_url,
                table="doctrine",
                force=True,
            )
            if text and len(text.strip()) > 50:
                return text.strip()
            return None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulatory documents with full text from PDFs."""
        url = f"{API_URL}?page%5Blimit%5D={PAGE_LIMIT}&include=field_regulatory_file,field_regulatory_framework_categ"
        total = 0
        page_num = 0

        while url:
            page_num += 1
            logger.info(f"Fetching page {page_num}...")
            data = self._get_json(url)
            if not data:
                break

            items = data.get("data", [])
            included = data.get("included", [])
            file_map = self._build_file_map(included)
            tax_map = self._build_taxonomy_map(included)

            for item in items:
                attrs = item.get("attributes", {})
                title = attrs.get("title", "")
                nid = str(attrs.get("drupal_internal__nid", ""))
                uuid = item.get("id", "")

                # Get file reference
                file_rel = item.get("relationships", {}).get("field_regulatory_file", {})
                file_data = file_rel.get("data")
                if not file_data:
                    logger.debug(f"No file for: {title}")
                    continue

                file_id = file_data.get("id", "") if isinstance(file_data, dict) else ""
                if not file_id or file_id not in file_map:
                    continue

                file_info = file_map[file_id]
                pdf_path = file_info.get("url", "")
                if not pdf_path or file_info.get("filemime") != "application/pdf":
                    continue

                # Get category
                cat_rel = item.get("relationships", {}).get("field_regulatory_framework_categ", {})
                cat_data = cat_rel.get("data")
                category = ""
                if isinstance(cat_data, dict) and cat_data.get("id") in tax_map:
                    category = tax_map[cat_data["id"]]

                # Extract text from PDF
                logger.info(f"Extracting: {title[:70]}...")
                text = self._extract_pdf_text(pdf_path, nid)
                if not text:
                    logger.debug(f"No text extracted for: {title}")
                    continue

                total += 1
                yield {
                    "id": nid,
                    "uuid": uuid,
                    "title": title,
                    "text": text,
                    "date": attrs.get("field_regulatory_date") or attrs.get("created", ""),
                    "url": f"{BASE_URL}{pdf_path}",
                    "category": category,
                    "filename": file_info.get("filename", ""),
                    "filesize": file_info.get("filesize", 0),
                }
                time.sleep(1.5)

            # Follow pagination
            next_link = data.get("links", {}).get("next")
            if isinstance(next_link, dict):
                url = next_link.get("href")
            elif isinstance(next_link, str):
                url = next_link
            else:
                url = None
            time.sleep(1)

        logger.info(f"Total: {total} documents with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        url = (
            f"{API_URL}?page%5Blimit%5D={PAGE_LIMIT}"
            f"&include=field_regulatory_file,field_regulatory_framework_categ"
            f"&filter%5Bchanged%5D%5Boperator%5D=%3E"
            f"&filter%5Bchanged%5D%5Bvalue%5D={quote(since_iso)}"
            f"&sort=-changed"
        )
        yield from self._fetch_from_url(url)

    def _fetch_from_url(self, url):
        """Helper to paginate and yield documents from a JSON:API URL."""
        while url:
            data = self._get_json(url)
            if not data:
                break
            items = data.get("data", [])
            included = data.get("included", [])
            file_map = self._build_file_map(included)
            tax_map = self._build_taxonomy_map(included)

            for item in items:
                attrs = item.get("attributes", {})
                title = attrs.get("title", "")
                nid = str(attrs.get("drupal_internal__nid", ""))
                file_rel = item.get("relationships", {}).get("field_regulatory_file", {})
                file_data = file_rel.get("data")
                if not file_data:
                    continue
                file_id = file_data.get("id", "") if isinstance(file_data, dict) else ""
                if not file_id or file_id not in file_map:
                    continue
                file_info = file_map[file_id]
                pdf_path = file_info.get("url", "")
                if not pdf_path or file_info.get("filemime") != "application/pdf":
                    continue

                cat_rel = item.get("relationships", {}).get("field_regulatory_framework_categ", {})
                cat_data = cat_rel.get("data")
                category = ""
                if isinstance(cat_data, dict) and cat_data.get("id") in tax_map:
                    category = tax_map[cat_data["id"]]

                text = self._extract_pdf_text(pdf_path, nid)
                if not text:
                    continue
                yield {
                    "id": nid,
                    "uuid": item.get("id", ""),
                    "title": title,
                    "text": text,
                    "date": attrs.get("field_regulatory_date") or attrs.get("created", ""),
                    "url": f"{BASE_URL}{pdf_path}",
                    "category": category,
                    "filename": file_info.get("filename", ""),
                    "filesize": file_info.get("filesize", 0),
                }
                time.sleep(1.5)

            next_link = data.get("links", {}).get("next")
            if isinstance(next_link, dict):
                url = next_link.get("href")
            elif isinstance(next_link, str):
                url = next_link
            else:
                url = None
            time.sleep(1)

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date_str = raw.get("date", "")
        if date_str and "T" in date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        return {
            "_id": raw.get("id", ""),
            "_source": "ZM/BOZ-Directives",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BOZDirectivesScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to BOZ JSON:API...")
        data = scraper._get_json(f"{API_URL}?page%5Blimit%5D=1")
        if data and data.get("data"):
            title = data["data"][0]["attributes"]["title"]
            logger.info(f"OK — got: {title}")
            print("Test passed: Drupal JSON:API accessible")
        else:
            logger.error("Failed to reach BOZ JSON:API")
            sys.exit(1)

    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
