#!/usr/bin/env python3
"""
SG/IRAS-AdvanceRulings -- Singapore IRAS Advance Ruling Summaries

Fetches published advance ruling summaries from IRAS covering:
  - Corporate Income Tax (main series, ~10-23 per year)
  - Individual Income Tax (small series, ~2 per year)
  - GST (small series, descriptive filenames)

Strategy:
  - Enumerate PDF URLs using known patterns per year/number
  - Try multiple URL variants (dash, underscore, dot separators)
  - Download PDFs and extract full text via common/pdf_extract
  - Parse structured sections (Subject, Facts, Ruling, Reasons)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SG.IRAS-AdvanceRulings")

USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# Base URLs for PDF downloads
CIT_BASE = "https://www.iras.gov.sg/media/docs/default-source/uploadedfiles/pdf"
IIT_BASE = "https://www.iras.gov.sg/docs/default-source/individual-income-tax/advance-ruling-summary"
GST_BASE = "https://www.iras.gov.sg/media/docs/default-source/uploadedfiles/gst"

# Known GST ruling filenames (descriptive, cannot be enumerated by number)
KNOWN_GST_RULINGS = [
    "zero-rating-of-telecommunication-and-related-services-under-section-21(3)(q).pdf",
]

# Year range to enumerate
START_YEAR = 2020
END_YEAR = 2026


def _build_cit_url_variants(year: int, num: int) -> List[str]:
    """Build all known URL variants for a corporate income tax ruling."""
    variants = []
    # Zero-padded and non-padded number
    num_pad = f"{num:02d}"
    num_str = str(num)
    # 2-digit and 4-digit year
    yr2 = str(year)[-2:]
    yr4 = str(year)

    # Pattern: advance-ruling-summary-no-NN-YYYY.pdf (most common 2023+)
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_pad}-{yr4}.pdf")
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_str}-{yr4}.pdf")

    # Pattern: advance-ruling-summary-no-NN_YYYY.pdf (underscore before year)
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_pad}_{yr4}.pdf")
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_str}_{yr4}.pdf")

    # Pattern: advance-ruling-summary-no.N_YYYY.pdf (dot after no, underscore year)
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no.{num_str}_{yr4}.pdf")
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no.{num_pad}_{yr4}.pdf")

    # Pattern with 2-digit year: advance-ruling-summary-no-N-YY.pdf
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_str}-{yr2}.pdf")
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_pad}-{yr2}.pdf")

    # Pattern with 2-digit year: advance-ruling-summary-no-N_YY.pdf
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_str}_{yr2}.pdf")
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no-{num_pad}_{yr2}.pdf")

    # Pattern with dot: advance-ruling-summary-no.N_YY.pdf
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no.{num_str}_{yr2}.pdf")
    variants.append(f"{CIT_BASE}/advance-ruling-summary-no.{num_pad}_{yr2}.pdf")

    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


def _build_iit_url_variants(year: int, num: int) -> List[str]:
    """Build URL variants for individual income tax rulings."""
    variants = []
    num_pad = f"{num:02d}"
    num_str = str(num)
    yr4 = str(year)

    # Pattern: iit_summary_YYYY_NN.pdf
    variants.append(f"{IIT_BASE}/iit_summary_{yr4}_{num_pad}.pdf")
    variants.append(f"{IIT_BASE}/iit_summary_{yr4}_{num_str}.pdf")

    # Pattern with double underscore: iit_summary_YYYY__NN.pdf
    variants.append(f"{IIT_BASE}/iit_summary_{yr4}__{num_pad}.pdf")
    variants.append(f"{IIT_BASE}/iit_summary_{yr4}__{num_str}.pdf")

    # Older format: iit_NN_summary.pdf
    variants.append(f"{IIT_BASE}/iit_{num_pad}_summary.pdf")
    variants.append(f"{IIT_BASE}/iit_{num_str}_summary.pdf")

    seen = set()
    unique = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


def _try_fetch_pdf(urls: List[str], timeout: int = 30) -> Tuple[Optional[bytes], Optional[str]]:
    """Try each URL in sequence, return (pdf_bytes, successful_url) or (None, None)."""
    for url in urls:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            resp = urlopen(req, timeout=timeout)
            if resp.status == 200:
                content_type = resp.headers.get("Content-Type", "")
                data = resp.read()
                # Verify it's actually a PDF (not an HTML error page)
                if b"%PDF" in data[:20] or "pdf" in content_type.lower():
                    return data, url
        except (HTTPError, URLError):
            continue
    return None, None


def _fetch_pdf_direct(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download PDF bytes from a single URL."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        if b"%PDF" in data[:20]:
            return data
        logger.warning(f"Not a PDF (got HTML?) for {url}")
        return None
    except (HTTPError, URLError) as e:
        logger.warning(f"PDF download failed for {url}: {e}")
        return None


def _extract_text(pdf_bytes: bytes, source_id: str) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="SG/IRAS-AdvanceRulings",
        source_id=source_id,
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""


def _parse_ruling_metadata(text: str) -> dict:
    """Extract structured metadata from ruling text."""
    meta = {}

    # Try to extract ruling number from header
    num_match = re.search(
        r"Advance Ruling Summary\s+No\.?\s*(\d+)[/_\s-]+(\d{2,4})",
        text, re.IGNORECASE
    )
    if num_match:
        meta["ruling_number"] = int(num_match.group(1))
        yr = num_match.group(2)
        if len(yr) == 2:
            yr = "20" + yr
        meta["year"] = int(yr)

    # Extract published date
    date_match = re.search(
        r"Published\s+on\s+(\d{1,2}\s+\w+\s+\d{4})",
        text, re.IGNORECASE
    )
    if date_match:
        for fmt in ["%d %B %Y", "%d %b %Y"]:
            try:
                meta["date"] = datetime.strptime(
                    date_match.group(1).strip(), fmt
                ).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # Extract subject
    subj_match = re.search(
        r"(?:1\.?\s*)?Subject\s*:?\s*(.+?)(?:\n\s*\n|\n\s*2\.)",
        text, re.IGNORECASE | re.DOTALL
    )
    if subj_match:
        meta["subject"] = subj_match.group(1).strip()[:500]

    return meta


class SingaporeIRASAdvanceRulingsScraper(BaseScraper):
    """
    Scraper for SG/IRAS-AdvanceRulings.
    Country: SG
    URL: https://www.iras.gov.sg/taxes/corporate-tax/specific-topics/advance-ruling-system

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _enumerate_cit_rulings(self, sample: bool = False) -> Generator[dict, None, None]:
        """Enumerate corporate income tax advance ruling PDFs."""
        max_per_year = 30  # Safety ceiling
        found_count = 0

        for year in range(END_YEAR, START_YEAR - 1, -1):
            consecutive_misses = 0
            for num in range(1, max_per_year + 1):
                if sample and found_count >= 11:
                    return

                urls = _build_cit_url_variants(year, num)
                self.rate_limiter.wait()
                pdf_bytes, found_url = _try_fetch_pdf(urls)

                if pdf_bytes:
                    consecutive_misses = 0
                    found_count += 1
                    source_id = f"CIT-{num:02d}-{year}"
                    text = _extract_text(pdf_bytes, source_id)

                    if not text or len(text) < 100:
                        logger.warning(f"Text too short for CIT {num}/{year}")
                        continue

                    meta = _parse_ruling_metadata(text)
                    yield {
                        "source_id": source_id,
                        "series": "corporate_income_tax",
                        "ruling_number": meta.get("ruling_number", num),
                        "year": meta.get("year", year),
                        "date": meta.get("date"),
                        "subject": meta.get("subject", ""),
                        "text": text,
                        "url": found_url,
                    }
                    logger.info(f"  Found CIT {num}/{year}: {meta.get('subject', '')[:50]}")
                else:
                    consecutive_misses += 1
                    # Skip rest of year if 5 consecutive misses
                    if consecutive_misses >= 5:
                        logger.info(f"  CIT {year}: stopping at {num} (5 consecutive misses)")
                        break

    def _enumerate_iit_rulings(self, sample: bool = False) -> Generator[dict, None, None]:
        """Enumerate individual income tax advance ruling PDFs."""
        found_count = 0

        for year in range(END_YEAR, START_YEAR - 1, -1):
            for num in range(1, 10):  # Small series, max ~5 per year
                if sample and found_count >= 2:
                    return

                urls = _build_iit_url_variants(year, num)
                self.rate_limiter.wait()
                pdf_bytes, found_url = _try_fetch_pdf(urls)

                if pdf_bytes:
                    found_count += 1
                    source_id = f"IIT-{num:02d}-{year}"
                    text = _extract_text(pdf_bytes, source_id)

                    if not text or len(text) < 100:
                        logger.warning(f"Text too short for IIT {num}/{year}")
                        continue

                    meta = _parse_ruling_metadata(text)
                    yield {
                        "source_id": source_id,
                        "series": "individual_income_tax",
                        "ruling_number": meta.get("ruling_number", num),
                        "year": meta.get("year", year),
                        "date": meta.get("date"),
                        "subject": meta.get("subject", ""),
                        "text": text,
                        "url": found_url,
                    }
                    logger.info(f"  Found IIT {num}/{year}: {meta.get('subject', '')[:50]}")

    def _enumerate_gst_rulings(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch known GST advance ruling PDFs."""
        for i, filename in enumerate(KNOWN_GST_RULINGS):
            if sample and i >= 1:
                return

            url = f"{GST_BASE}/{filename}"
            self.rate_limiter.wait()
            pdf_bytes = _fetch_pdf_direct(url)

            if pdf_bytes:
                source_id = f"GST-{filename.replace('.pdf', '')}"
                if len(source_id) > 80:
                    source_id = source_id[:80]
                text = _extract_text(pdf_bytes, source_id)

                if not text or len(text) < 100:
                    logger.warning(f"Text too short for GST {filename}")
                    continue

                meta = _parse_ruling_metadata(text)
                yield {
                    "source_id": source_id,
                    "series": "gst",
                    "ruling_number": meta.get("ruling_number", i + 1),
                    "year": meta.get("year", 2025),
                    "date": meta.get("date"),
                    "subject": meta.get("subject", ""),
                    "text": text,
                    "url": url,
                }
                logger.info(f"  Found GST: {filename}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IRAS advance ruling summaries with full text."""
        logger.info("Starting enumeration of IRAS advance ruling PDFs...")

        logger.info("=== Corporate Income Tax Rulings ===")
        yield from self._enumerate_cit_rulings(sample=False)

        logger.info("=== Individual Income Tax Rulings ===")
        yield from self._enumerate_iit_rulings(sample=False)

        logger.info("=== GST Rulings ===")
        yield from self._enumerate_gst_rulings(sample=False)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Re-fetch all (no incremental API available)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw ruling data into standard schema."""
        title = f"IRAS Advance Ruling Summary No. {raw['ruling_number']}/{raw['year']}"
        if raw.get("subject"):
            title += f" - {raw['subject'][:100]}"

        return {
            "_id": raw["source_id"],
            "_source": "SG/IRAS-AdvanceRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "ruling_number": raw.get("ruling_number"),
            "year": raw.get("year"),
            "series": raw.get("series"),
            "subject": raw.get("subject", ""),
        }


# === CLI entry point ===
if __name__ == "__main__":
    scraper = SingaporeIRASAdvanceRulingsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        # Quick connectivity test: try one known URL
        test_url = f"{CIT_BASE}/advance-ruling-summary-no-01-2025.pdf"
        req = Request(test_url, headers={"User-Agent": USER_AGENT}, method="HEAD")
        try:
            resp = urlopen(req, timeout=15)
            print(f"OK: {test_url} → {resp.status}")
        except Exception as e:
            print(f"FAIL: {test_url} → {e}")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 12 if sample else 999

        if sample:
            # In sample mode, pull from CIT (most reliable), IIT, and GST
            logger.info("=== SAMPLE MODE: fetching ~12 records ===")
            generators = [
                scraper._enumerate_cit_rulings(sample=True),
                scraper._enumerate_iit_rulings(sample=True),
                scraper._enumerate_gst_rulings(sample=True),
            ]
            for gen in generators:
                for raw in gen:
                    record = scraper.normalize(raw)
                    # Save to sample/
                    import json
                    out_file = sample_dir / f"{record['_id']}.json"
                    out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                    count += 1
                    logger.info(f"Saved [{count}]: {record['title'][:70]}")
                    if count >= max_records:
                        break
                if count >= max_records:
                    break
        else:
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                import json
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
