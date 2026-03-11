#!/usr/bin/env python3
"""
BA/ConstitutionalCourt - Constitutional Court of Bosnia and Herzegovina

Fetches case law decisions from the Constitutional Court of Bosnia and Herzegovina.
Uses the official API at ustavnisud.ba and extracts full text from PDF documents.
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests

# Try to import pdfplumber for PDF text extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    try:
        import PyPDF2
        HAS_PYPDF2 = True
    except ImportError:
        HAS_PYPDF2 = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.ustavnisud.ba"
API_ENDPOINT = "/bs/api/odluke"
DEFAULT_DELAY = 1.5  # seconds between requests


class ConstitutionalCourtFetcher:
    """Fetcher for Constitutional Court of Bosnia and Herzegovina decisions."""

    def __init__(self, delay: float = DEFAULT_DELAY):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "WorldWideLaw/1.0 (Legal research project)",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "bs,en;q=0.9",
        })
        self.delay = delay
        self.last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()

    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """Make a rate-limited GET request."""
        self._rate_limit()
        response = self.session.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response

    def fetch_decisions_by_year(self, year: int) -> list[dict]:
        """
        Fetch decisions for a specific year.

        Args:
            year: The year to fetch decisions for (e.g., 2020)

        Returns:
            List of decision metadata dicts
        """
        url = f"{BASE_URL}{API_ENDPOINT}"
        # Date format: DD.MM.YYYY. (with trailing dot)
        params = {
            "dos": f"01.01.{year}.",
            "doe": f"31.12.{year}.",
            "sp": "DatumDesc"
        }

        logger.info(f"Fetching decisions for year {year}")
        response = self._get(url, params)
        data = response.json()

        items = data.get("items", [])
        logger.info(f"Retrieved {len(items)} decisions for {year}")
        return items

    def fetch_all_decisions(self, start_year: int = 1997, end_year: int = None) -> list[dict]:
        """
        Fetch all decisions by iterating through years.

        The API only supports date range filtering, not "fetch all".
        We iterate year by year to get the complete dataset.

        Args:
            start_year: First year to fetch (Constitutional Court founded 1997)
            end_year: Last year to fetch (defaults to current year)

        Returns:
            List of all decision metadata dicts
        """
        if end_year is None:
            end_year = datetime.now().year

        all_items = []
        for year in range(start_year, end_year + 1):
            try:
                items = self.fetch_decisions_by_year(year)
                all_items.extend(items)
                logger.info(f"Cumulative total: {len(all_items)} decisions")
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch year {year}: {e}")
                continue

        logger.info(f"Total decisions fetched: {len(all_items)}")
        return all_items

    def fetch_decisions(self, filter_preset: int = 1) -> list[dict]:
        """
        Fetch decisions from the API.

        DEPRECATED: This only fetches "latest session" decisions (~284 records).
        Use fetch_all_decisions() for the complete dataset.

        Args:
            filter_preset: Predefined filter ID (1 = latest session decisions)

        Returns:
            List of decision metadata dicts
        """
        url = f"{BASE_URL}{API_ENDPOINT}"
        params = {
            "fp": filter_preset,
            "sp": "DatumDesc"  # Sort by date descending
        }

        logger.info(f"Fetching decisions from {url}")
        response = self._get(url, params)
        data = response.json()

        items = data.get("items", [])
        logger.info(f"Retrieved {len(items)} decisions")
        return items

    def download_pdf(self, pdf_path: str) -> Optional[bytes]:
        """
        Download a PDF document.

        Args:
            pdf_path: Relative path to PDF (e.g., /uploads/odluke/xxx.pdf)

        Returns:
            PDF content as bytes, or None if download fails
        """
        if not pdf_path:
            return None

        # Ensure path starts with /
        if not pdf_path.startswith("/"):
            pdf_path = "/" + pdf_path

        url = f"{BASE_URL}{pdf_path}"

        try:
            self._rate_limit()
            response = self.session.get(url, timeout=120)
            response.raise_for_status()

            if response.headers.get("content-type", "").startswith("application/pdf"):
                return response.content
            else:
                logger.warning(f"Expected PDF but got {response.headers.get('content-type')}")
                return None
        except requests.RequestException as e:
            logger.error(f"Failed to download PDF from {url}: {e}")
            return None

    def extract_text_from_pdf(self, pdf_content: bytes) -> Optional[str]:
        """
        Extract text from PDF content.

        Args:
            pdf_content: PDF file content as bytes

        Returns:
            Extracted text, or None if extraction fails
        """
        if not pdf_content:
            return None

        # Write to temporary file
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_content)
            tmp_path = tmp.name

        try:
            text = ""

            if HAS_PDFPLUMBER:
                with pdfplumber.open(tmp_path) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n\n"
            elif HAS_PYPDF2:
                with open(tmp_path, "rb") as f:
                    reader = PyPDF2.PdfReader(f)
                    for page in reader.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n\n"
            else:
                logger.error("No PDF extraction library available (pdfplumber or PyPDF2)")
                return None

            return text.strip() if text else None

        except Exception as e:
            logger.error(f"Failed to extract text from PDF: {e}")
            return None
        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def normalize(self, raw: dict, full_text: Optional[str] = None) -> dict:
        """
        Normalize raw API data into standard schema.

        Args:
            raw: Raw decision data from API
            full_text: Extracted PDF text (optional)

        Returns:
            Normalized record dict
        """
        case_number = raw.get("case_number", "")
        decision_id = raw.get("id", "")

        # Parse date
        date_str = raw.get("date", "")
        if date_str:
            try:
                date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_iso = date_obj.date().isoformat()
            except ValueError:
                date_iso = date_str[:10] if len(date_str) >= 10 else None
        else:
            date_iso = None

        # Build URL to decision page
        decision_url = raw.get("decision_url", "")
        if decision_url and not decision_url.startswith("http"):
            decision_url = f"{BASE_URL}{decision_url}"

        # Extract keywords
        keywords = [kw.get("title", "") for kw in raw.get("keywords", []) if kw.get("title")]

        # Extract violations found/not found
        infringement = [v.get("title", "") for v in raw.get("infringement", []) if v.get("title")]
        not_infringement = [v.get("title", "") for v in raw.get("not_infringement", []) if v.get("title")]

        # Build title
        title = raw.get("title", "")
        if case_number:
            title = f"{case_number}: {title}" if title else case_number

        # Combine conclusion with full text
        conclusion = raw.get("conclusion", "")
        text = full_text if full_text else conclusion

        return {
            "_id": f"BA-CC-{case_number}-{decision_id}".replace("/", "-"),
            "_source": "BA/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case_number,
            "decision_id": decision_id,
            "case_id": raw.get("case_id", ""),
            "title": title,
            "appellant": raw.get("title", ""),  # Original title is appellant name
            "text": text,
            "conclusion": conclusion,
            "date": date_iso,
            "url": decision_url,
            "decision_type": raw.get("decision_type", ""),
            "decision_type_id": raw.get("decision_type_id", ""),
            "case_type": raw.get("item_type", ""),
            "case_type_id": raw.get("case_type_id", ""),
            "disputed_act": raw.get("disputed_act", ""),
            "keywords": keywords,
            "violations_found": infringement,
            "no_violations_found": not_infringement,
            "publications": raw.get("publications", []),
            "pdf_filename": raw.get("filename", ""),
        }

    def fetch_all(
        self,
        with_full_text: bool = True,
        limit: Optional[int] = None,
        start_year: int = 1997,
        end_year: int = None
    ) -> Generator[dict, None, None]:
        """
        Fetch all available decisions with full text.

        Args:
            with_full_text: Whether to download and extract PDF text
            limit: Maximum number of records to fetch (None for all)
            start_year: First year to fetch (Constitutional Court founded 1997)
            end_year: Last year to fetch (defaults to current year)

        Yields:
            Normalized decision records
        """
        decisions = self.fetch_all_decisions(start_year=start_year, end_year=end_year)

        if limit:
            decisions = decisions[:limit]

        for i, decision in enumerate(decisions):
            logger.info(f"Processing decision {i+1}/{len(decisions)}: {decision.get('case_number', 'N/A')}")

            full_text = None
            if with_full_text:
                pdf_path = decision.get("url") or decision.get("decision_url")
                if pdf_path:
                    pdf_content = self.download_pdf(pdf_path)
                    if pdf_content:
                        full_text = self.extract_text_from_pdf(pdf_content)
                        if full_text:
                            logger.info(f"  Extracted {len(full_text)} chars from PDF")
                        else:
                            logger.warning(f"  Failed to extract text from PDF")

            yield self.normalize(decision, full_text)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Fetch decisions updated since a given date.

        Note: The API only provides a "latest session" filter, so this
        returns all recent decisions and filters client-side.

        Args:
            since: Only return decisions from after this date

        Yields:
            Normalized decision records
        """
        for record in self.fetch_all(with_full_text=True):
            if record.get("date"):
                try:
                    record_date = datetime.fromisoformat(record["date"])
                    if record_date >= since.date() if hasattr(since, 'date') else since:
                        yield record
                except ValueError:
                    # If date parsing fails, include the record
                    yield record


def bootstrap_sample(output_dir: Path, sample_size: int = 12):
    """
    Create sample data for testing.

    For samples, we use the "latest session" filter for speed, rather than
    iterating through all years. The full fetch_all() method uses year-by-year
    iteration to get all 26,000+ records.

    Args:
        output_dir: Directory to save sample files
        sample_size: Number of samples to fetch
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    fetcher = ConstitutionalCourtFetcher()

    # For samples, use the fast "latest session" filter instead of all years
    # This returns ~284 recent decisions, enough for a good sample
    decisions = fetcher.fetch_decisions(filter_preset=1)[:sample_size]

    records = []
    for i, decision in enumerate(decisions):
        logger.info(f"Processing decision {i+1}/{len(decisions)}: {decision.get('case_number', 'N/A')}")

        full_text = None
        pdf_path = decision.get("url") or decision.get("decision_url")
        if pdf_path:
            pdf_content = fetcher.download_pdf(pdf_path)
            if pdf_content:
                full_text = fetcher.extract_text_from_pdf(pdf_content)
                if full_text:
                    logger.info(f"  Extracted {len(full_text)} chars from PDF")
                else:
                    logger.warning(f"  Failed to extract text from PDF")

        record = fetcher.normalize(decision, full_text)
        records.append(record)

        # Save individual record
        record_file = output_dir / f"{record['_id']}.json"
        with open(record_file, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved: {record_file.name}")

    # Save summary
    summary = {
        "source": "BA/ConstitutionalCourt",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "total_records": len(records),
        "records_with_text": sum(1 for r in records if r.get("text")),
        "avg_text_length": sum(len(r.get("text", "")) for r in records) / len(records) if records else 0,
        "date_range": {
            "earliest": min((r.get("date") for r in records if r.get("date")), default=None),
            "latest": max((r.get("date") for r in records if r.get("date")), default=None),
        },
        "case_types": list(set(r.get("case_type", "") for r in records)),
        "decision_types": list(set(r.get("decision_type", "") for r in records)),
    }

    summary_file = output_dir / "_summary.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"\n{'='*60}")
    logger.info(f"Bootstrap complete!")
    logger.info(f"Total records: {summary['total_records']}")
    logger.info(f"Records with full text: {summary['records_with_text']}")
    logger.info(f"Average text length: {summary['avg_text_length']:.0f} chars")
    logger.info(f"Date range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
    logger.info(f"Files saved to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="BA/ConstitutionalCourt - Fetch Bosnia and Herzegovina Constitutional Court decisions"
    )
    parser.add_argument(
        "command",
        choices=["bootstrap", "fetch", "test"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample data only (12 records)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "sample",
        help="Output directory for sample data"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of records to fetch"
    )
    parser.add_argument(
        "--no-text",
        action="store_true",
        help="Skip PDF download and text extraction"
    )

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(args.output, sample_size=args.limit or 12)
        else:
            # Full bootstrap
            fetcher = ConstitutionalCourtFetcher()
            output_dir = args.output
            output_dir.mkdir(parents=True, exist_ok=True)

            count = 0
            for record in fetcher.fetch_all(
                with_full_text=not args.no_text,
                limit=args.limit
            ):
                record_file = output_dir / f"{record['_id']}.json"
                with open(record_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(f"Saved: {record_file.name}")

            logger.info(f"Bootstrap complete! Saved {count} records.")

    elif args.command == "fetch":
        fetcher = ConstitutionalCourtFetcher()
        for record in fetcher.fetch_all(
            with_full_text=not args.no_text,
            limit=args.limit or 5
        ):
            print(json.dumps(record, ensure_ascii=False, indent=2))

    elif args.command == "test":
        # Quick connectivity test
        logger.info("Testing API connectivity...")
        fetcher = ConstitutionalCourtFetcher()

        # Test API
        decisions = fetcher.fetch_decisions()
        logger.info(f"API returned {len(decisions)} decisions")

        if decisions:
            # Test PDF download and extraction
            first = decisions[0]
            logger.info(f"Testing PDF extraction for: {first.get('case_number')}")

            pdf_path = first.get("url") or first.get("decision_url")
            if pdf_path:
                pdf_content = fetcher.download_pdf(pdf_path)
                if pdf_content:
                    logger.info(f"Downloaded PDF: {len(pdf_content)} bytes")

                    text = fetcher.extract_text_from_pdf(pdf_content)
                    if text:
                        logger.info(f"Extracted text: {len(text)} chars")
                        logger.info(f"First 500 chars:\n{text[:500]}")
                    else:
                        logger.error("Failed to extract text from PDF")
                else:
                    logger.error("Failed to download PDF")
            else:
                logger.warning("No PDF URL found")

        logger.info("Test complete!")


if __name__ == "__main__":
    main()
