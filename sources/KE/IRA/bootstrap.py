#!/usr/bin/env python3
"""
KE/IRA -- Kenya Insurance Regulatory Authority — Guidelines & Decisions

Fetches circulars, guidelines, legal notices, and regulatory documents
from Kenya's IRA via custom AJAX API with PDF text extraction.

Strategy:
  - AJAX endpoint at assets/includes/ajapp.php returns paginated HTML
    for each section (circulars, guidelines, legal notices)
  - Each resource has a PDF download via lib.html?f={slug}
  - PDFs are downloaded and text extracted via pdfplumber
  - Scanned-image PDFs (0 extractable chars) are skipped

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import time
import logging
import html
import tempfile
import os
import base64
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.IRA")

BASE_URL = "https://www.ira.go.ke"
AJAX_URL = f"{BASE_URL}/assets/includes/ajapp.php"
USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"

MIN_TEXT_LENGTH = 200

# IRA sections with their sec_id and menu_id (data-com) values
SECTIONS = [
    {"name": "Circulars to Insurers", "sec_id": "7", "menu_id": "74", "page_url": "/circulars-to-insurers/", "doc_type": "circular"},
    {"name": "Guidelines for Insurers", "sec_id": "7", "menu_id": "18", "page_url": "/guidelines-for-insurers/", "doc_type": "guideline"},
    {"name": "Legal Notices", "sec_id": "7", "menu_id": "21", "page_url": "/legal-notices/", "doc_type": "legal_notice"},
    {"name": "Circulars to Agents", "sec_id": "7", "menu_id": "76", "page_url": "/circulars-to-agents/", "doc_type": "circular"},
    {"name": "Circulars to Brokers", "sec_id": "7", "menu_id": "77", "page_url": "/circulars-to-brokers/", "doc_type": "circular"},
    {"name": "Circulars to Service Providers", "sec_id": "7", "menu_id": "78", "page_url": "/circulars-to-service-providers/", "doc_type": "circular"},
    {"name": "Circulars to Reinsurers", "sec_id": "7", "menu_id": "79", "page_url": "/circulars-to-reinsurers/", "doc_type": "circular"},
    {"name": "Circulars to Intermediaries", "sec_id": "7", "menu_id": "145", "page_url": "/circulars-to-intermediaries/", "doc_type": "circular"},
]


def strip_html(raw_html: str) -> str:
    """Remove HTML tags, decode entities, collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", raw_html)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def download_pdf_text(url: str, session: requests.Session) -> Optional[str]:
    """Download a PDF and extract text using pdfplumber."""
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "pdf" not in ct and not resp.content[:5] == b"%PDF-":
            return None
        if len(resp.content) > 50_000_000:
            logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
            return None
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(resp.content)
            tmp_path = f.name
        try:
            with pdfplumber.open(tmp_path) as pdf:
                pages_text = []
                for page in pdf.pages[:200]:
                    page_text = page.extract_text()
                    if page_text:
                        pages_text.append(page_text)
                return "\n\n".join(pages_text) if pages_text else None
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        logger.warning(f"PDF extraction failed for {url}: {e}")
        return None


def fetch_section_resources(session: requests.Session, section: dict) -> list:
    """Fetch all resources from a section via AJAX pagination."""
    resources = []
    page_url = f"{BASE_URL}{section['page_url']}"

    # Visit the page first to establish session
    session.get(page_url, timeout=30)
    time.sleep(0.5)

    for pg in range(1, 20):
        params = {
            "fc": "fc_resources",
            "fsec": section["sec_id"],
            "fops": [section["menu_id"]],
            "ffoc": [],
            "fpill": [],
            "fdfrom": "",
            "fdto": "",
            "fcom": section["menu_id"],
            "fpage": pg,
            "fview": "",
            "fstext": "",
            "fstatus": "",
        }
        encoded = base64.b64encode(json.dumps(params).encode()).decode()
        resp = session.post(
            AJAX_URL,
            data={"fdt": encoded},
            headers={"X-Requested-With": "XMLHttpRequest", "Referer": page_url},
            timeout=30,
        )

        if resp.status_code != 200 or not resp.text:
            break

        # Parse resource entries from HTML
        entries = re.findall(
            r'href="(resource/[^"]+)"[^>]*class="linkRes[^"]*"[^>]*data-id="(\d+)"[^>]*>\s*<span[^>]*title="([^"]+)"',
            resp.text,
        )
        if not entries:
            break

        for url_path, rid, title in entries:
            slug = url_path.rstrip("/").split("/")[-1]
            resources.append({
                "id": rid,
                "title": html.unescape(title),
                "slug": slug,
                "url": f"{BASE_URL}/{url_path}",
                "section": section["name"],
                "doc_type": section["doc_type"],
            })

        time.sleep(1.0)

    return resources


class IRAScraper(BaseScraper):
    """
    Scraper for KE/IRA — Kenya Insurance Regulatory Authority.
    Country: KE
    URL: https://www.ira.go.ke/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT

    def _normalize_resource(self, resource: dict, text: str) -> dict:
        """Normalize a resource into standard schema."""
        return {
            "_id": f"ira-ke-{resource['id']}",
            "_source": "KE/IRA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": resource["title"],
            "text": text,
            "date": None,
            "url": resource["url"],
            "document_type": resource["doc_type"],
            "section": resource["section"],
            "pdf_url": f"{BASE_URL}/lib.html?f={resource['slug']}",
        }

    def normalize(self, raw: dict) -> dict:
        return raw

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IRA regulatory documents with PDF text extraction."""
        yielded = 0
        skipped_no_text = 0
        seen_ids = set()

        for section in SECTIONS:
            logger.info(f"Fetching {section['name']}...")
            resources = fetch_section_resources(self._session, section)
            logger.info(f"  Found {len(resources)} resources")

            for resource in resources:
                if resource["id"] in seen_ids:
                    continue
                seen_ids.add(resource["id"])

                pdf_url = f"{BASE_URL}/lib.html?f={resource['slug']}"
                text = download_pdf_text(pdf_url, self._session)

                if not text or len(text) < MIN_TEXT_LENGTH:
                    skipped_no_text += 1
                    logger.debug(f"  Skipped (no text): {resource['title'][:60]}")
                    continue

                record = self._normalize_resource(resource, text)
                yield record
                yielded += 1
                logger.info(f"  [{yielded}] {resource['title'][:60]} ({len(text)} chars)")

                time.sleep(1.0)

        logger.info(f"fetch_all complete: {yielded} records, {skipped_no_text} skipped (scanned PDFs)")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """IRA has no date-based filtering; yields nothing."""
        return
        yield


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="KE/IRA — Kenya Insurance Regulatory Authority"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = IRAScraper()

    if args.command == "test":
        logger.info("Testing IRA connectivity...")
        try:
            sess = requests.Session()
            sess.headers["User-Agent"] = USER_AGENT
            resp = sess.get(f"{BASE_URL}/circulars-to-insurers/", timeout=30)
            logger.info(f"Page status: {resp.status_code}")

            resources = fetch_section_resources(sess, SECTIONS[0])
            logger.info(f"Circulars to Insurers: {len(resources)} resources")

            if resources:
                r = resources[0]
                logger.info(f"Sample: {r['title'][:80]}")
                pdf_url = f"{BASE_URL}/lib.html?f={r['slug']}"
                text = download_pdf_text(pdf_url, sess)
                if text:
                    logger.info(f"PDF text: {len(text)} chars")
                else:
                    logger.info("PDF: scanned image (no extractable text)")

            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
