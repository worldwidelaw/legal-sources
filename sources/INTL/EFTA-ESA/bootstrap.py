#!/usr/bin/env python3
"""
INTL/EFTA-ESA -- EFTA Surveillance Authority Decisions

Fetches enforcement decisions from the EFTA Surveillance Authority at eftasurv.int.

Strategy:
  - Paginate the Drupal JSON API (/cms/api/node) for college decisions
  - Download PDF attachments and extract full text via common/pdf_extract

Data Coverage:
  - ~1500+ college decisions from 1994 to present
  - State aid, competition, internal market compliance
  - Iceland, Liechtenstein, Norway (EEA)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EFTA-ESA")

BASE_URL = "https://www.eftasurv.int"
API_URL = f"{BASE_URL}/cms/api/node"
DOC_PATH = "/esa-at-a-glance/publications/public-access-to-documents/public-documents"
PER_PAGE = 50  # Fixed by API
MAX_PDF_BYTES = 50 * 1024 * 1024


class EFTAESAScraper(BaseScraper):
    """Scraper for EFTA Surveillance Authority decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "application/json, text/html",
            "Accept-Language": "en",
        })

    def _fetch_page(self, page: int, doc_type: str = "decision") -> dict:
        """Fetch a page of decisions from the JSON API."""
        search_params = f"type={doc_type}&page={page}"
        params = {
            "url": DOC_PATH,
            "search": search_params,
        }
        resp = self.session.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _list_all_decisions(self) -> list[dict]:
        """Paginate through all college decisions."""
        all_decisions = []
        page = 1
        while True:
            data = self._fetch_page(page)
            listing = data.get("listing", {}).get("data", {})
            nodes = listing.get("nodes", [])
            total = listing.get("nodesCount", 0)

            if not nodes:
                break

            all_decisions.extend(nodes)
            logger.info(f"Listed page {page}: {len(nodes)} decisions (total {len(all_decisions)}/{total})")

            if len(all_decisions) >= total:
                break

            page += 1
            time.sleep(0.5)

        return all_decisions

    def _download_pdf(self, pdf_path: str) -> Optional[bytes]:
        """Download a PDF from the ESA site."""
        if not pdf_path:
            return None
        url = f"{BASE_URL}{pdf_path}" if pdf_path.startswith("/") else pdf_path
        try:
            time.sleep(1)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes), likely error")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        """Extract text from PDF bytes."""
        text = extract_pdf_markdown(
            source="INTL/EFTA-ESA",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text

        # Fallback
        import io
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p)
                if text and len(text.strip()) >= 100:
                    return text
        except Exception:
            pass
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(p for p in pages if p)
            if text and len(text.strip()) >= 100:
                return text
        except Exception:
            pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions with full text from PDFs."""
        decisions = self._list_all_decisions()
        logger.info(f"Total decisions to process: {len(decisions)}")

        for i, dec in enumerate(decisions):
            try:
                title = dec.get("title", "")
                case_number = dec.get("caseNumber", "")
                decision_number = dec.get("collegeDecision", "")
                source_id = decision_number or case_number or str(dec.get("number", i))

                logger.info(f"[{i+1}/{len(decisions)}] Processing {title[:60]} ...")

                attachment = dec.get("attachment", {})
                pdf_path = attachment.get("url", "") if attachment else ""

                if not pdf_path:
                    logger.warning(f"  No PDF attachment, skipping")
                    continue

                pdf_bytes = self._download_pdf(pdf_path)
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, source_id)
                if not text:
                    logger.warning(f"  Insufficient text for {source_id}, skipping")
                    continue

                dec["_extracted_text"] = text
                dec["_pdf_url"] = f"{BASE_URL}{pdf_path}" if pdf_path.startswith("/") else pdf_path
                yield dec

            except Exception as e:
                logger.error(f"  Error processing decision {i}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions from recent years."""
        since_year = since.year
        current_year = datetime.now().year
        for year in range(since_year, current_year + 1):
            search_params = f"type=decision&year={year}&page=1"
            params = {"url": DOC_PATH, "search": search_params}
            try:
                resp = self.session.get(API_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                listing = data.get("listing", {}).get("data", {})
                total = listing.get("nodesCount", 0)
                nodes = listing.get("nodes", [])

                page = 1
                while nodes:
                    for dec in nodes:
                        attachment = dec.get("attachment", {})
                        pdf_path = attachment.get("url", "") if attachment else ""
                        if not pdf_path:
                            continue
                        pdf_bytes = self._download_pdf(pdf_path)
                        if not pdf_bytes:
                            continue
                        source_id = dec.get("collegeDecision") or dec.get("caseNumber") or str(dec.get("number", ""))
                        text = self._extract_text(pdf_bytes, source_id)
                        if not text:
                            continue
                        dec["_extracted_text"] = text
                        dec["_pdf_url"] = f"{BASE_URL}{pdf_path}" if pdf_path.startswith("/") else pdf_path
                        yield dec

                    page += 1
                    if len(nodes) < PER_PAGE:
                        break
                    search_params = f"type=decision&year={year}&page={page}"
                    params = {"url": DOC_PATH, "search": search_params}
                    resp = self.session.get(API_URL, params=params, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    nodes = data.get("listing", {}).get("data", {}).get("nodes", [])
                    time.sleep(0.5)

            except Exception as e:
                logger.error(f"  Error fetching year {year}: {e}")
                continue

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        title = raw.get("title", "")
        case_number = raw.get("caseNumber", "")
        decision_number = raw.get("collegeDecision", "")
        country = raw.get("country", {})
        country_code = country.get("code", "") if isinstance(country, dict) else ""

        # Parse timestamp date
        date_ts = raw.get("date")
        date = None
        if date_ts and isinstance(date_ts, (int, float)):
            try:
                date = datetime.fromtimestamp(date_ts, tz=timezone.utc).strftime("%Y-%m-%d")
            except (OSError, ValueError):
                pass

        # Build unique ID
        uid = decision_number or case_number or str(raw.get("number", ""))
        uid_slug = uid.lower().replace("/", "-").replace(" ", "-")

        return {
            "_id": f"efta-esa-{uid_slug}",
            "_source": "INTL/EFTA-ESA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_extracted_text", ""),
            "date": date,
            "url": raw.get("_pdf_url", ""),
            "case_number": case_number,
            "decision_number": decision_number,
            "country": country_code,
            "doc_type": raw.get("type", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = EFTAESAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        data = scraper._fetch_page(1)
        listing = data.get("listing", {}).get("data", {})
        print(f"Total decisions: {listing.get('nodesCount', 0)}")
        nodes = listing.get("nodes", [])
        if nodes:
            print(f"First: {nodes[0].get('title', '')[:80]}")
        sys.exit(0)

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
