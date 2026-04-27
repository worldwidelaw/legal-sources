#!/usr/bin/env python3
"""
MM/OpenDevMyanmar -- Open Development Myanmar Laws Database

Fetches Myanmar legislation via the CKAN API at data.opendevelopmentmyanmar.net.
Downloads PDF resources and extracts full text using common/pdf_extract.

Strategy:
  - Use CKAN package_search API to list all laws_record packages for Myanmar
  - For each package, find English PDF resources (prefer English, fall back to Burmese)
  - Extract full text from PDFs via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MM.OpenDevMyanmar")

BASE_URL = "https://data.opendevelopmentmyanmar.net"
SEARCH_URL = f"{BASE_URL}/api/3/action/package_search"
ROWS_PER_PAGE = 50


class OpenDevMyanmarScraper(BaseScraper):
    """Scraper for MM/OpenDevMyanmar -- Myanmar laws via CKAN."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _api_get(self, url: str, params: dict = None, timeout: int = 60) -> Optional[dict]:
        """CKAN API GET with retry."""
        for attempt in range(3):
            try:
                time.sleep(0.5)
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if data.get("success"):
                    return data.get("result")
                logger.warning(f"API returned success=false: {data}")
                return None
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _pick_pdf_resource(self, resources: list) -> Optional[Dict[str, str]]:
        """Pick the best PDF resource (prefer English)."""
        english_pdfs = []
        other_pdfs = []

        for r in resources:
            url = r.get("url", "")
            fmt = (r.get("format", "") or "").lower()
            name = (r.get("name", "") or "").lower()
            desc = (r.get("description", "") or "").lower()

            if fmt not in ("pdf", "") and not url.lower().endswith(".pdf"):
                continue
            if not url.lower().endswith(".pdf"):
                continue

            info = {"url": url, "name": r.get("name", ""), "language": "unknown"}

            if "english" in name or "english" in desc or "_en" in url.lower() or "english" in url.lower():
                info["language"] = "en"
                english_pdfs.append(info)
            elif "myanmar" in name or "burmese" in name or "myanmar" in desc or "_my" in url.lower():
                info["language"] = "my"
                other_pdfs.append(info)
            else:
                other_pdfs.append(info)

        if english_pdfs:
            return english_pdfs[0]
        if other_pdfs:
            return other_pdfs[0]
        return None

    def _is_myanmar_specific(self, pkg: dict) -> bool:
        """Check if the package is Myanmar-specific."""
        spatial = (pkg.get("spatial_range", "") or "").lower()
        title = (pkg.get("title", "") or "").lower()
        notes = (pkg.get("notes", "") or "").lower()

        if spatial == "mm" or "myanmar" in spatial:
            return True
        if "myanmar" in title or "burma" in title:
            return True
        if "myanmar" in notes:
            return True
        return False

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        pkg_id = raw.get("package_id", "")
        short_id = pkg_id[:8] if len(pkg_id) > 8 else pkg_id
        doc_id = f"MM-ODM-{short_id}"

        date = ""
        for date_field in ("metadata_created", "metadata_modified", "date"):
            val = raw.get(date_field, "")
            if val:
                m = re.search(r"(\d{4}-\d{2}-\d{2})", str(val))
                if m:
                    date = m.group(1)
                    break

        return {
            "_id": doc_id,
            "_source": "MM/OpenDevMyanmar",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": f"{BASE_URL}/dataset/{raw.get('package_name', pkg_id)}",
            "license": raw.get("license_title", ""),
            "language": raw.get("language", ""),
            "spatial": raw.get("spatial_range", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Myanmar laws from CKAN."""
        count = 0
        start = 0

        while True:
            params = {
                "q": "myanmar",
                "fq": "type:laws_record",
                "rows": ROWS_PER_PAGE,
                "start": start,
            }
            result = self._api_get(SEARCH_URL, params=params)
            if result is None:
                logger.error(f"Failed to fetch page at start={start}")
                break

            packages = result.get("results", [])
            total = result.get("count", 0)

            if not packages:
                logger.info(f"No more packages at start={start}")
                break

            logger.info(f"Page start={start}: {len(packages)} packages (total={total})")

            for pkg in packages:
                if not self._is_myanmar_specific(pkg):
                    continue

                pkg_id = pkg.get("id", "")
                title = pkg.get("title", "")
                resources = pkg.get("resources", [])

                pdf_info = self._pick_pdf_resource(resources)
                if not pdf_info:
                    logger.warning(f"No PDF resource for {title}")
                    continue

                pdf_url = pdf_info["url"]
                language = pdf_info.get("language", "unknown")

                logger.info(f"Extracting PDF for: {title[:80]} ({language})")
                try:
                    text = extract_pdf_markdown(
                        source="MM/OpenDevMyanmar",
                        source_id=f"MM-ODM-{pkg_id[:8]}",
                        pdf_url=pdf_url,
                        table="legislation",
                    )
                except Exception as e:
                    logger.warning(f"PDF extraction failed for {title}: {e}")
                    text = None

                if not text or len(text) < 50:
                    logger.warning(f"Insufficient text for {title}: {len(text) if text else 0} chars")
                    continue

                raw = {
                    "package_id": pkg_id,
                    "package_name": pkg.get("name", ""),
                    "title": title,
                    "text": text,
                    "metadata_created": pkg.get("metadata_created", ""),
                    "metadata_modified": pkg.get("metadata_modified", ""),
                    "license_title": pkg.get("license_title", ""),
                    "language": language,
                    "spatial_range": pkg.get("spatial_range", ""),
                }
                count += 1
                yield raw

            start += ROWS_PER_PAGE
            if start >= total:
                break

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently modified packages."""
        params = {
            "q": "myanmar",
            "fq": "type:laws_record",
            "rows": 20,
            "start": 0,
            "sort": "metadata_modified desc",
        }
        result = self._api_get(SEARCH_URL, params=params)
        if result is None:
            return

        count = 0
        for pkg in result.get("results", []):
            if not self._is_myanmar_specific(pkg):
                continue

            resources = pkg.get("resources", [])
            pdf_info = self._pick_pdf_resource(resources)
            if not pdf_info:
                continue

            try:
                text = extract_pdf_markdown(
                    source="MM/OpenDevMyanmar",
                    source_id=f"MM-ODM-{pkg['id'][:8]}",
                    pdf_url=pdf_info["url"],
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed: {e}")
                text = None

            if not text or len(text) < 50:
                continue

            raw = {
                "package_id": pkg["id"],
                "package_name": pkg.get("name", ""),
                "title": pkg.get("title", ""),
                "text": text,
                "metadata_created": pkg.get("metadata_created", ""),
                "metadata_modified": pkg.get("metadata_modified", ""),
                "license_title": pkg.get("license_title", ""),
                "language": pdf_info.get("language", "unknown"),
                "spatial_range": pkg.get("spatial_range", ""),
            }
            count += 1
            yield raw

        logger.info(f"Updates: {count} documents fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        result = self._api_get(SEARCH_URL, params={"q": "myanmar", "fq": "type:laws_record", "rows": 1})
        if result is None:
            logger.error("Cannot reach CKAN API")
            return False

        total = result.get("count", 0)
        packages = result.get("results", [])
        logger.info(f"CKAN API OK: {total} total law records")

        if packages:
            pkg = packages[0]
            resources = pkg.get("resources", [])
            pdf_info = self._pick_pdf_resource(resources)
            if pdf_info:
                logger.info(f"Sample: {pkg.get('title', 'N/A')[:80]} — PDF at {pdf_info['url'][:80]}")
            return True

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MM/OpenDevMyanmar data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OpenDevMyanmarScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
