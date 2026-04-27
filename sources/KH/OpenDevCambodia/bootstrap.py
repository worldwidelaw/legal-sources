#!/usr/bin/env python3
"""
KH/OpenDevCambodia -- Cambodia Laws via Open Development Cambodia CKAN

Fetches Cambodian legislation from the Open Development Cambodia CKAN portal.

Strategy:
  - CKAN v3 package_search API for laws_record type (2,969 packages)
  - Each package has PDF resources (Khmer and/or English)
  - Download PDFs and extract text via common/pdf_extract
  - Rich bilingual metadata (title, date, issuing agency, document type)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import gc
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KH.OpenDevCambodia")

BASE_API = "https://data.opendevelopmentcambodia.net/api/3/action"
SEARCH_FQ = "type:laws_record organization:cambodia-organization"
PAGE_SIZE = 1000


class OpenDevCambodiaScraper(BaseScraper):
    """
    Scraper for KH/OpenDevCambodia -- Cambodia Laws (CKAN).
    Country: KH
    URL: https://data.opendevelopmentcambodia.net/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research project)",
            "Accept": "application/json",
        })

    def _search_packages(self, start: int = 0, rows: int = PAGE_SIZE) -> dict:
        """Search CKAN for laws_record packages."""
        url = f"{BASE_API}/package_search"
        params = {"fq": SEARCH_FQ, "rows": rows, "start": start}
        r = self.session.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"CKAN API error: {data}")
        return data["result"]

    def _get_all_packages(self) -> list[dict]:
        """Paginate through all laws_record packages."""
        first_page = self._search_packages(start=0)
        total = first_page["count"]
        packages = first_page["results"]
        logger.info(f"Total packages: {total}, fetched page 1: {len(packages)}")

        start = PAGE_SIZE
        while start < total:
            page = self._search_packages(start=start)
            packages.extend(page["results"])
            logger.info(f"Fetched page {start // PAGE_SIZE + 1}: {len(page['results'])} (total so far: {len(packages)})")
            start += PAGE_SIZE
            time.sleep(0.5)

        return packages

    def _get_best_pdf_url(self, package: dict) -> Optional[str]:
        """Pick the best PDF resource from a package.

        Prefers English PDFs, then any PDF.
        """
        resources = package.get("resources", [])
        pdfs = [r for r in resources if r.get("format", "").upper() == "PDF" and r.get("url")]

        if not pdfs:
            return None

        # Prefer English
        for r in pdfs:
            lang = r.get("odm_language", "")
            name = (r.get("name") or r.get("description") or "").lower()
            url = r.get("url", "").lower()
            if "en" in str(lang) or "english" in name or "_en" in url or "_eng" in url:
                return r["url"]

        # Fall back to first PDF
        return pdfs[0]["url"]

    def _extract_title(self, package: dict) -> str:
        """Get the best title — prefer English translated title."""
        title_trans = package.get("title_translated", {})
        if isinstance(title_trans, dict):
            en = title_trans.get("en", "").strip()
            if en:
                return en
            km = title_trans.get("km", "").strip()
            if km:
                return km
        return (package.get("title") or package.get("name", "")).strip()

    def _extract_date(self, package: dict) -> Optional[str]:
        """Get the most relevant date in ISO 8601."""
        for field in ("odm_promulgation_date", "odm_effective_date", "metadata_modified"):
            val = package.get(field, "")
            if val and len(val) >= 10:
                return val[:10]
        return None

    def _extract_description(self, package: dict) -> str:
        """Get English description if available."""
        notes_trans = package.get("notes_translated", {})
        if isinstance(notes_trans, dict):
            en = notes_trans.get("en", "").strip()
            if en:
                return en
        return (package.get("notes") or "").strip()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw CKAN package + extracted text into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        pkg_id = raw.get("id", "")
        doc_type = raw.get("odm_document_type", "")

        return {
            "_id": f"KH-ODC-{pkg_id}",
            "_source": "KH/OpenDevCambodia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": f"https://data.opendevelopmentcambodia.net/dataset/{pkg_id}",
            "document_type": doc_type,
            "document_number": raw.get("odm_document_number", ""),
            "issuing_agency": raw.get("odm_laws_issuing_agency_parties", ""),
            "status": raw.get("odm_laws_status", ""),
            "description": raw.get("description", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Cambodia laws_record packages with PDF text."""
        packages = self._get_all_packages()
        logger.info(f"Processing {len(packages)} packages")

        total_docs = 0
        skipped_no_pdf = 0
        skipped_no_text = 0

        for i, pkg in enumerate(packages):
            pkg_id = pkg.get("id", "")
            title = self._extract_title(pkg)

            pdf_url = self._get_best_pdf_url(pkg)
            if not pdf_url:
                skipped_no_pdf += 1
                continue

            if (i + 1) % 100 == 0:
                logger.info(f"[{i+1}/{len(packages)}] Processing: {title[:60]}")

            text = extract_pdf_markdown(
                source="KH/OpenDevCambodia",
                source_id=pkg_id,
                pdf_url=pdf_url,
                table="legislation",
            ) or ""

            # Cap text to prevent OOM
            if text and len(text) > 500_000:
                text = text[:500_000]

            if not text:
                skipped_no_text += 1
                del text
                continue

            # Extract metadata
            issuing = pkg.get("odm_laws_issuing_agency_parties", "")
            if isinstance(issuing, list):
                issuing = ", ".join(issuing)

            yield {
                "id": pkg_id,
                "title": title,
                "text": text,
                "date": self._extract_date(pkg),
                "pdf_url": pdf_url,
                "odm_document_type": pkg.get("odm_document_type", ""),
                "odm_document_number": pkg.get("odm_document_number", ""),
                "odm_laws_issuing_agency_parties": issuing,
                "odm_laws_status": pkg.get("odm_laws_status", ""),
                "description": self._extract_description(pkg),
            }
            total_docs += 1
            del text

            # GC every 10 docs
            if total_docs % 10 == 0:
                gc.collect()

            time.sleep(1)

        logger.info(
            f"Fetch complete: {total_docs} docs, "
            f"{skipped_no_pdf} skipped (no PDF), "
            f"{skipped_no_text} skipped (no text)"
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch packages modified since a given date."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Fetching updates since {since_str}")

        url = f"{BASE_API}/package_search"
        params = {
            "fq": f"{SEARCH_FQ} metadata_modified:[{since_str} TO *]",
            "rows": PAGE_SIZE,
            "start": 0,
        }
        r = self.session.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            return

        packages = data["result"]["results"]
        logger.info(f"Found {data['result']['count']} updated packages")

        for pkg in packages:
            pkg_id = pkg.get("id", "")
            title = self._extract_title(pkg)
            pdf_url = self._get_best_pdf_url(pkg)
            if not pdf_url:
                continue

            text = extract_pdf_markdown(
                source="KH/OpenDevCambodia",
                source_id=pkg_id,
                pdf_url=pdf_url,
                table="legislation",
            ) or ""

            if text:
                issuing = pkg.get("odm_laws_issuing_agency_parties", "")
                if isinstance(issuing, list):
                    issuing = ", ".join(issuing)
                yield {
                    "id": pkg_id,
                    "title": title,
                    "text": text,
                    "date": self._extract_date(pkg),
                    "pdf_url": pdf_url,
                    "odm_document_type": pkg.get("odm_document_type", ""),
                    "odm_document_number": pkg.get("odm_document_number", ""),
                    "odm_laws_issuing_agency_parties": issuing,
                    "odm_laws_status": pkg.get("odm_laws_status", ""),
                    "description": self._extract_description(pkg),
                }
            time.sleep(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="KH/OpenDevCambodia data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = OpenDevCambodiaScraper()

    if args.command == "test":
        logger.info("Testing CKAN API connectivity...")
        try:
            result = scraper._search_packages(start=0, rows=3)
            count = result["count"]
            pkgs = result["results"]
            logger.info(f"OK: {count} total laws_record packages")
            if pkgs:
                pkg = pkgs[0]
                title = scraper._extract_title(pkg)
                pdf_url = scraper._get_best_pdf_url(pkg)
                logger.info(f"Sample: {title[:80]}")
                logger.info(f"PDF URL: {pdf_url}")
                if pdf_url:
                    text = extract_pdf_markdown(
                        source="KH/OpenDevCambodia",
                        source_id=pkg.get("id", "test"),
                        pdf_url=pdf_url,
                        table="legislation",
                    ) or ""
                    logger.info(f"PDF text: {len(text)} chars")
                    if text:
                        logger.info(f"Preview: {text[:200]}")
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
    main()
