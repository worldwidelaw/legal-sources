#!/usr/bin/env python3
"""
LU/Competition — Luxembourg Competition Authority Decisions

Fetches competition decisions and opinions from the Autorité de la concurrence
(formerly Conseil de la concurrence) of Luxembourg.

Strategy:
  - Phase 1: Query data.public.lu API for all datasets published by the org
  - Phase 2: Check supplementary dam-assets URLs for newer/older decisions
  - Download PDFs and extract full text via pdfplumber

Endpoints:
  - API: https://data.public.lu/api/1/organizations/{org_id}/datasets/
  - PDFs: https://download.data.public.lu/resources/...
  - Supplementary: https://concurrence.public.lu/dam-assets/fr/decisions/...

Data:
  - ~80-100 decisions + opinions (2012-2024)
  - Language: French (some German)
  - Rate limit: 1 request/second for PDF downloads

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("LU/Competition")

# data.public.lu organization ID for Conseil de la concurrence
ORG_ID = "5c25f1c928c4b25ebffe46c7"
API_BASE = "https://data.public.lu/api/1"

# Supplementary PDF URLs not on data.public.lu (discovered via web search)
SUPPLEMENTARY_PDFS = [
    # 2024
    "https://concurrence.public.lu/dam-assets/fr/decisions/engagements/2024/decision-2024-e-01.pdf",
    # 2023
    "https://concurrence.public.lu/dam-assets/fr/decisions/ententes/2023/decision-2023-d-01.pdf",
    # 2021
    "https://concurrence.public.lu/dam-assets/fr/decisions/classements/Jugement-du-25-janvier-2021-version-publiee.pdf",
    # 2020
    "https://concurrence.public.lu/dam-assets/fr/decisions/amendes-astreintes/2020/d%C3%A9cision-2020-fo-03/2020-11-18-Decision-Bahlsen-Auchan-version-publique.pdf",
    # 2019
    "https://concurrence.public.lu/dam-assets/fr/decisions/classements/2019-12-20-Decision-2019-C-02-version-non-confidentielle.pdf",
    "https://concurrence.public.lu/dam-assets/fr/decisions/mesures-conservatoires/2019/Decision-2019-MC-01-Version-unique-.pdf",
    # 2010
    "https://concurrence.public.lu/dam-assets/fr/decisions/ententes/2010/2010-fo-01/Decision-N_2010-FO-01-du-5-mars-2010.pdf",
]

PDF_DELAY = 1.5  # seconds between PDF downloads
REQUEST_TIMEOUT = 60

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (academic research; contact: github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/pdf,*/*",
}

# Decision number patterns: 2018-FO-03, 2023-D-01, 2019-MC-01, etc.
DECISION_NUM_RE = re.compile(
    r"(\d{4})[_-]([A-Za-z]{1,4})[_-](\d{1,3})", re.IGNORECASE
)


class LuxembourgCompetitionScraper(BaseScraper):
    """Scraper for LU/Competition — Luxembourg Competition Authority decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from data.public.lu API + supplementary PDFs."""
        seen_urls = set()

        # Phase 1: data.public.lu API datasets
        logger.info("Phase 1: Fetching datasets from data.public.lu API")
        datasets = self._get_datasets()
        for ds in datasets:
            title = ds.get("title", "")
            # Skip annual reports and orientation docs
            if "rapport" in title.lower() or "orientation" in title.lower():
                continue
            logger.info(f"  Dataset: {title} ({len(ds.get('resources', []))} resources)")
            for resource in ds.get("resources", []):
                fmt = (resource.get("format") or "").lower()
                url = resource.get("url", "")
                if fmt != "pdf" or not url:
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                raw = self._build_raw_from_resource(resource, title)
                yield raw

        # Phase 2: Supplementary dam-assets PDFs
        logger.info("Phase 2: Checking supplementary PDF URLs")
        for url in SUPPLEMENTARY_PDFS:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            raw = self._build_raw_from_url(url)
            yield raw

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (small corpus, full re-fetch is fast)."""
        yield from self.fetch_all()

    def _get_datasets(self) -> list:
        """Fetch all datasets from the data.public.lu API for the org."""
        url = f"{API_BASE}/organizations/{ORG_ID}/datasets/"
        try:
            resp = self.session.get(url, params={"page_size": 50}, timeout=30)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch datasets from API: {e}")
            return []

    def _build_raw_from_resource(self, resource: dict, dataset_title: str) -> dict:
        """Build a raw record dict from a data.public.lu resource."""
        url = resource.get("url", "")
        filename = unquote(url.split("/")[-1]) if url else ""
        title = resource.get("title") or filename
        return {
            "pdf_url": url,
            "filename": filename,
            "title": title,
            "dataset_title": dataset_title,
            "source_type": "data_public_lu",
        }

    def _build_raw_from_url(self, url: str) -> dict:
        """Build a raw record dict from a supplementary dam-assets URL."""
        filename = unquote(url.split("/")[-1])
        return {
            "pdf_url": url,
            "filename": filename,
            "title": filename.replace(".pdf", "").replace("-", " ").replace("_", " "),
            "dataset_title": "Supplementary",
            "source_type": "dam_assets",
        }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw PDF record into standard schema."""
        pdf_url = raw.get("pdf_url", "")
        filename = raw.get("filename", "")

        if not pdf_url:
            return None

        # Extract decision number from filename
        decision_number = self._extract_decision_number(filename)
        if not decision_number:
            # Try from title
            decision_number = self._extract_decision_number(raw.get("title", ""))
        if not decision_number:
            # Use filename as fallback ID
            decision_number = re.sub(r"\.pdf$", "", filename, flags=re.I)
            decision_number = re.sub(r"[^a-zA-Z0-9_-]", "_", decision_number).strip("_")

        # Extract year from decision number or filename
        year = self._extract_year(decision_number) or self._extract_year(filename)

        # Determine category from URL path
        category = self._extract_category(pdf_url)

        # Download and extract PDF text
        text = self._download_pdf_text(pdf_url)
        if not text or len(text.strip()) < 100:
            logger.warning(f"Insufficient text from PDF: {filename} ({len(text or '')} chars)")
            return None

        # Build title
        title_parts = [decision_number]
        raw_title = raw.get("title", "")
        if raw_title and raw_title != filename and raw_title != decision_number:
            title_parts.append(raw_title)
        title = " — ".join(title_parts)

        # Build a clean ID
        doc_id = re.sub(r"[^a-zA-Z0-9_/-]", "_", decision_number).strip("_")

        # Try to extract date from text or decision number
        iso_date = None
        if year:
            iso_date = f"{year}-01-01"
            # Try to find a more specific date in the filename
            date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", filename)
            if date_match:
                iso_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            else:
                # Try to extract date from the first few lines of text
                date_in_text = re.search(
                    r"(\d{1,2})\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})",
                    text[:2000],
                    re.IGNORECASE,
                )
                if date_in_text:
                    month_map = {
                        "janvier": "01", "février": "02", "mars": "03",
                        "avril": "04", "mai": "05", "juin": "06",
                        "juillet": "07", "août": "08", "septembre": "09",
                        "octobre": "10", "novembre": "11", "décembre": "12",
                    }
                    full_match = re.search(
                        r"(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})",
                        text[:2000],
                        re.IGNORECASE,
                    )
                    if full_match:
                        day = full_match.group(1).zfill(2)
                        month = month_map.get(full_match.group(2).lower(), "01")
                        yr = full_match.group(3)
                        iso_date = f"{yr}-{month}-{day}"

        return {
            "_id": f"LU/Competition/{doc_id}",
            "_source": "LU/Competition",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": pdf_url,
            "decision_number": decision_number,
            "decision_category": category,
            "year": year,
        }

    def _extract_decision_number(self, text: str) -> Optional[str]:
        """Extract decision number like 2018-FO-03 from text."""
        match = DECISION_NUM_RE.search(text)
        if match:
            return f"{match.group(1)}-{match.group(2).upper()}-{match.group(3).zfill(2)}"
        return None

    def _extract_year(self, text: str) -> Optional[int]:
        """Extract a 4-digit year from text."""
        match = re.search(r"(20\d{2})", text)
        if match:
            return int(match.group(1))
        return None

    def _extract_category(self, url: str) -> str:
        """Extract decision category from URL path."""
        url_lower = url.lower()
        categories = {
            "ententes": "ententes",
            "abus-de-position-dominante": "abus_de_position_dominante",
            "amendes-astreintes": "amendes_astreintes",
            "classements": "classements",
            "engagements": "engagements",
            "concentrations": "concentrations",
            "mesures-conservatoires": "mesures_conservatoires",
            "avis": "avis",
        }
        for key, value in categories.items():
            if key in url_lower:
                return value
        # Check dataset title for avis
        return "decision"

    def _download_pdf_text(self, url: str) -> str:
        """Download PDF and extract text using pdfplumber."""
        try:
            time.sleep(PDF_DELAY)
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            if "pdf" not in resp.headers.get("content-type", "").lower() and not url.endswith(".pdf"):
                logger.warning(f"Non-PDF response for {url}")
                return ""

            import pdfplumber

            pdf_bytes = io.BytesIO(resp.content)
            text_parts = []
            with pdfplumber.open(pdf_bytes) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            return "\n\n".join(text_parts)

        except Exception as e:
            logger.error(f"PDF extraction failed for {url}: {e}")
            return ""


def main():
    import argparse

    parser = argparse.ArgumentParser(description="LU/Competition bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    args = parser.parse_args()

    scraper = LuxembourgCompetitionScraper()

    if args.command == "test":
        logger.info("Testing API connectivity...")
        datasets = scraper._get_datasets()
        logger.info(f"Found {len(datasets)} datasets")
        for ds in datasets:
            logger.info(f"  {ds['title']}: {len(ds.get('resources', []))} resources")
        # Test one PDF download
        if datasets:
            for ds in datasets:
                for r in ds.get("resources", []):
                    if (r.get("format") or "").lower() == "pdf" and r.get("url"):
                        logger.info(f"Testing PDF: {r['url'][:80]}...")
                        text = scraper._download_pdf_text(r["url"])
                        logger.info(f"  Extracted {len(text)} chars")
                        return
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        max_records = 15 if args.sample else 999
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            if record is None:
                continue
            count += 1
            # Save sample
            if count <= 15 or not args.sample:
                fname = re.sub(r"[^a-zA-Z0-9_-]", "_", record["decision_number"])[:60]
                sample_file = sample_dir / f"{fname}.json"
                with open(sample_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(
                f"[{count}] {record['decision_number']} — "
                f"{len(record['text'])} chars — {record.get('date', 'no date')}"
            )

            if args.sample and count >= max_records:
                break

        logger.info(f"Done. {count} records saved.")

    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(None):
            record = scraper.normalize(raw)
            if record:
                count += 1
                logger.info(f"[{count}] {record['decision_number']}")
        logger.info(f"Update complete. {count} records.")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
