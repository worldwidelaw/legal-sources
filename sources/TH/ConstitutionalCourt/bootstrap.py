#!/usr/bin/env python3
"""
TH/ConstitutionalCourt -- Thailand Constitutional Court Decisions

Fetches constitutional review decisions from the Thai Constitutional Court's
Intelligent Search System (ISS) API. Downloads PDF attachments and extracts
full text using pdfplumber.

The ISS indexes ~12,000 Thai Constitutional Court records. About 1,200+ of
these are ruling summaries with digitally-embedded text (extractable).
The remainder are scanned-image PDFs; records without extractable text are
skipped.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import io
import hashlib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TH.ConstitutionalCourt")

ISS_API = "https://iss.constitutionalcourt.or.th/api/search"
# Thai Constitutional Court organization ID in ISS MongoDB
ORG_ID = "61fc0f3559884c579650d132"
# Thailand country ID in ISS MongoDB
COUNTRY_ID = "61fc0c04c6bd6498e75b6527"

PAGE_SIZE = 20
DELAY = 1.5
MIN_TEXT_LENGTH = 100


def _extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    parts.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(parts) if parts else None
    except Exception as e:
        logger.debug("pdfplumber failed: %s", e)
        return None


class THConstitutionalCourtScraper(BaseScraper):
    """Scraper for Thailand Constitutional Court decisions via ISS API."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

    def _search(self, skip: int = 0, limit: int = PAGE_SIZE,
                query: str = "") -> Dict[str, Any]:
        """Call the ISS search API."""
        payload = {
            "query": query,
            "organization": ORG_ID,
            "country": COUNTRY_ID,
            "skip": skip,
            "limit": limit,
        }
        resp = self.http.post(ISS_API, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download a PDF from the ISS backend."""
        try:
            encoded_url = quote(pdf_url, safe=":/?&=")
            resp = self.http.get(encoded_url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 500:
                return None
            return resp.content
        except Exception as e:
            logger.debug("PDF download failed for %s: %s", pdf_url, e)
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw ISS record."""
        return {
            "_id": raw["_id"],
            "_source": "TH/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "case_id": raw.get("case_id", ""),
            "decision_type": raw.get("decision_type", ""),
            "year": raw.get("year"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Thai Constitutional Court decisions with extractable text."""
        # First get total count
        result = self._search(skip=0, limit=1)
        total = result.get("totalAmount", {}).get("value", 0)
        logger.info("ISS reports %d total Thai Constitutional Court records", total)

        limit = 15 if sample else total
        fetched = 0
        skip = 0
        empty_pages = 0

        while skip < total and fetched < limit:
            batch_size = min(PAGE_SIZE, limit - fetched + 10)  # extra to account for skipped
            logger.info("Fetching page at skip=%d (fetched %d/%d)", skip, fetched, limit)

            try:
                result = self._search(skip=skip, limit=batch_size)
            except Exception as e:
                logger.error("API error at skip=%d: %s", skip, e)
                skip += batch_size
                empty_pages += 1
                if empty_pages > 5:
                    break
                continue

            data = result.get("data", [])
            if not data:
                empty_pages += 1
                if empty_pages > 3:
                    logger.info("No more data after skip=%d", skip)
                    break
                skip += batch_size
                continue

            empty_pages = 0

            for record in data:
                if fetched >= limit:
                    break

                iss_id = record.get("_id", "")
                topic = record.get("topic", "")
                case_id = record.get("caseID", "")
                pub_date = record.get("publicDate", "")
                year = record.get("year")
                rtype = record.get("type", "")
                reference = record.get("reference", "")
                attachments = record.get("attachment", [])

                if not attachments:
                    continue

                pdf_url = attachments[0]
                pdf_bytes = self._download_pdf(pdf_url)
                if not pdf_bytes:
                    continue

                text = _extract_pdf_text(pdf_bytes)
                if not text or len(text.strip()) < MIN_TEXT_LENGTH:
                    continue

                iso_date = None
                if pub_date:
                    try:
                        iso_date = pub_date[:10]
                    except (IndexError, TypeError):
                        pass

                raw = {
                    "_id": f"TH_CC_{iss_id}",
                    "title": topic,
                    "text": text.strip(),
                    "date": iso_date,
                    "url": reference,
                    "pdf_url": pdf_url,
                    "case_id": case_id,
                    "decision_type": rtype,
                    "year": year,
                }

                yield self.normalize(raw)
                fetched += 1
                time.sleep(DELAY)

            skip += len(data)

        logger.info("Fetched %d records with extractable text", fetched)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions updated since a given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        since_date = datetime.fromisoformat(since).date()
        for record in self.fetch_all():
            if record.get("date"):
                try:
                    rec_date = datetime.fromisoformat(record["date"]).date()
                    if rec_date >= since_date:
                        yield record
                except ValueError:
                    yield record
            else:
                yield record


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="TH/ConstitutionalCourt bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = THConstitutionalCourtScraper()

    if args.command == "test":
        result = scraper._search(skip=0, limit=1)
        total = result.get("totalAmount", {}).get("value", 0)
        print(f"OK -- ISS API accessible, {total} Thai CC records")
        if result.get("data"):
            r = result["data"][0]
            print(f"  Latest: {r.get('topic', '')[:80]}...")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
