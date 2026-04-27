#!/usr/bin/env python3
"""
Legal Data Hunter - UK Planning Appeal Decisions Scraper

Fetches planning appeal decisions from the Planning Inspectorate Appeals
Casework Portal (ACP) at acp.planninginspectorate.gov.uk.

Strategy:
  - Iterate sequential case IDs on the ACP portal
  - Parse case HTML for reference, status, decision date, PDF link
  - Download decision PDFs and extract text via common/pdf_extract

Coverage: ~30,000+ decided appeals (case IDs ~3300000–3365000+).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

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
logger = logging.getLogger("UK/PlanningAppeals")

MAX_PDF_SIZE = 30 * 1024 * 1024  # 30MB limit

# Recent case IDs with decisions — iterate backwards from latest
LATEST_CASE_ID = 3365000
EARLIEST_CASE_ID = 3300000


class UKPlanningAppealsScraper(BaseScraper):
    """
    Scraper for UK Planning Inspectorate appeal decisions.

    Uses the Appeals Casework Portal (ACP) to fetch case metadata
    and decision PDFs by iterating sequential case IDs.
    """

    BASE_URL = "https://acp.planninginspectorate.gov.uk"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "text/html",
            },
            timeout=30,
        )

    def _parse_case_page(self, html: str) -> Optional[dict]:
        """Parse an ACP case page and extract metadata + decision PDF link."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "No case found"
        ref_el = soup.find("h4", id="cphMainContent_LabelCaseReference")
        if ref_el and "No case found" in ref_el.get_text():
            return None

        # Extract reference
        reference = ""
        if ref_el:
            ref_text = ref_el.get_text(strip=True)
            ref_match = re.search(r"Reference:\s*(APP/[^\s]+)", ref_text)
            if ref_match:
                reference = ref_match.group(1)

        # Extract status
        status_el = soup.find("span", id="cphMainContent_labStatus")
        status = status_el.get_text(strip=True) if status_el else ""

        # Only process cases with a decision
        if "Decision issued" not in status:
            return None

        # Extract decision date
        date_el = soup.find("span", id="cphMainContent_labDecisionDate")
        decision_date = date_el.get_text(strip=True) if date_el else ""

        # Extract decision PDF link
        decision_link_el = soup.find("span", id="cphMainContent_labDecisionLink")
        pdf_file_id = None
        if decision_link_el:
            link = decision_link_el.find("a", href=True)
            if link:
                href = link["href"]
                fid_match = re.search(r"fileid=(\d+)", href)
                if fid_match:
                    pdf_file_id = fid_match.group(1)

        if not pdf_file_id:
            return None

        # Extract site address
        address_el = soup.find("span", id="cphMainContent_labAddress")
        address = address_el.get_text(strip=True) if address_el else ""

        # Extract appeal type
        type_el = soup.find("span", id="cphMainContent_labAppealType")
        appeal_type = type_el.get_text(strip=True) if type_el else ""

        # Extract LPA (Local Planning Authority)
        lpa_el = soup.find("span", id="cphMainContent_labLPA")
        lpa = lpa_el.get_text(strip=True) if lpa_el else ""

        return {
            "reference": reference,
            "decision_date": decision_date,
            "pdf_file_id": pdf_file_id,
            "address": address,
            "appeal_type": appeal_type,
            "lpa": lpa,
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse UK date format (e.g. '06 Aug 2024') to ISO format."""
        if not date_str:
            return None
        for fmt in ("%d %b %Y", "%d %B %Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _download_decision_pdf(self, file_id: str) -> Optional[bytes]:
        """Download a decision PDF from the ACP portal."""
        url = f"/ViewDocument.aspx?fileid={file_id}"
        self.rate_limiter.wait()
        try:
            import requests
            full_url = f"{self.BASE_URL}{url}"
            resp = requests.get(full_url, headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
            }, timeout=60, stream=True)
            if resp.status_code != 200:
                logger.warning(f"PDF download returned {resp.status_code}: {full_url}")
                return None
            content_length = int(resp.headers.get("content-length", 0))
            if content_length > MAX_PDF_SIZE:
                logger.warning(f"PDF too large ({content_length} bytes): file {file_id}")
                return None
            data = resp.content
            if len(data) > MAX_PDF_SIZE:
                logger.warning(f"PDF too large ({len(data)} bytes): file {file_id}")
                return None
            return data
        except Exception as e:
            logger.warning(f"PDF download failed for file {file_id}: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, case_id: str) -> str:
        """Extract text from decision PDF."""
        return extract_pdf_markdown(
            source="UK/PlanningAppeals",
            source_id=case_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all planning appeal decisions with full text."""
        count = 0
        skipped = 0
        not_found = 0
        consecutive_not_found = 0

        # Iterate from latest backwards
        case_id = LATEST_CASE_ID
        while case_id >= EARLIEST_CASE_ID:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/ViewCase.aspx?caseid={case_id}")
                if resp.status_code != 200:
                    skipped += 1
                    case_id -= 1
                    continue
            except Exception as e:
                logger.warning(f"Failed to fetch case {case_id}: {e}")
                skipped += 1
                case_id -= 1
                continue

            meta = self._parse_case_page(resp.text)
            if meta is None:
                # Case doesn't exist or has no decision
                not_found += 1
                consecutive_not_found += 1
                # Stop if too many consecutive missing
                if consecutive_not_found > 500:
                    logger.info(f"500 consecutive missing cases at {case_id}, stopping")
                    break
                case_id -= 1
                continue

            consecutive_not_found = 0

            # Download and extract PDF
            pdf_bytes = self._download_decision_pdf(meta["pdf_file_id"])
            if not pdf_bytes:
                skipped += 1
                case_id -= 1
                continue

            text = self._extract_text(pdf_bytes, str(case_id))
            if not text or len(text) < 100:
                skipped += 1
                case_id -= 1
                continue

            count += 1
            yield {
                "case_id": str(case_id),
                "reference": meta["reference"],
                "title": f"Planning Appeal {meta['reference']}",
                "text": text,
                "decision_date": meta["decision_date"],
                "address": meta["address"],
                "appeal_type": meta["appeal_type"],
                "lpa": meta["lpa"],
            }

            if count % 50 == 0:
                logger.info(f"  {count} decisions fetched ({skipped} skipped, {not_found} not found)")

            case_id -= 1

        logger.info(f"Total: {count} decisions with text ({skipped} skipped, {not_found} not found)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions issued since a given date."""
        since_str = since.strftime("%Y-%m-%d")

        case_id = LATEST_CASE_ID
        count = 0

        while case_id >= EARLIEST_CASE_ID:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/ViewCase.aspx?caseid={case_id}")
                if resp.status_code != 200:
                    case_id -= 1
                    continue
            except Exception:
                case_id -= 1
                continue

            meta = self._parse_case_page(resp.text)
            if meta is None:
                case_id -= 1
                continue

            date_iso = self._parse_date(meta["decision_date"])
            if date_iso and date_iso < since_str:
                break

            pdf_bytes = self._download_decision_pdf(meta["pdf_file_id"])
            if not pdf_bytes:
                case_id -= 1
                continue

            text = self._extract_text(pdf_bytes, str(case_id))
            if not text or len(text) < 100:
                case_id -= 1
                continue

            count += 1
            yield {
                "case_id": str(case_id),
                "reference": meta["reference"],
                "title": f"Planning Appeal {meta['reference']}",
                "text": text,
                "decision_date": meta["decision_date"],
                "address": meta["address"],
                "appeal_type": meta["appeal_type"],
                "lpa": meta["lpa"],
            }
            case_id -= 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw appeal data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        date_iso = self._parse_date(raw.get("decision_date", ""))

        return {
            "_id": f"UK/PlanningAppeals/{raw.get('case_id', '')}",
            "_source": "UK/PlanningAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": raw.get("case_id", ""),
            "reference": raw.get("reference", ""),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_iso,
            "address": raw.get("address", ""),
            "appeal_type": raw.get("appeal_type", ""),
            "lpa": raw.get("lpa", ""),
            "url": f"{self.BASE_URL}/ViewCase.aspx?caseid={raw.get('case_id', '')}",
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKPlanningAppealsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
