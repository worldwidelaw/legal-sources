#!/usr/bin/env python3
"""
IT/AGCM -- Italian Competition Authority (AGCM) Data Fetcher

Fetches competition and consumer protection decisions from AGCM.

Strategy:
  - Downloads weekly bulletin PDFs from AGCM website
  - Extracts full text using pdfplumber
  - Parses individual decisions from bulletin content
  - URL pattern: https://www.agcm.it/dotcmsdoc/bollettini/{YEAR}/{WEEK}-{YY}.pdf

The bulletins contain full text of all AGCM decisions including:
  - Competition cases (antitrust, cartels, abuse of dominance)
  - Merger notifications and decisions
  - Consumer protection decisions
  - Opinions and signaling

License: Italian Open Data (public domain)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (last 30 days)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import io
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Dict, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# Optional PDF extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    logging.warning("pdfplumber not installed. PDF text extraction will be limited.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.AGCM")

# AGCM bulletin URL pattern
BASE_URL = "https://www.agcm.it"
BULLETIN_URL_PATTERN = "https://www.agcm.it/dotcmsdoc/bollettini/{year}/{week}-{yy}.pdf"

# Decision section headers in bulletins
SECTION_HEADERS = [
    "INTESE E ABUSO DI POSIZIONE DOMINANTE",
    "OPERAZIONI DI CONCENTRAZIONE",
    "INDAGINI CONOSCITIVE",
    "ATTIVITA' DI SEGNALAZIONE E CONSULTIVA",
    "PRATICHE COMMERCIALI SCORRETTE",
    "PUBBLICITA' INGANNEVOLE",
    "CLAUSOLE VESSATORIE",
    "VARIE",
]

# Decision type mappings
SECTION_TO_TYPE = {
    "INTESE E ABUSO DI POSIZIONE DOMINANTE": "competition",
    "OPERAZIONI DI CONCENTRAZIONE": "merger",
    "INDAGINI CONOSCITIVE": "market_study",
    "ATTIVITA' DI SEGNALAZIONE E CONSULTIVA": "opinion",
    "PRATICHE COMMERCIALI SCORRETTE": "consumer_protection",
    "PUBBLICITA' INGANNEVOLE": "advertising",
    "CLAUSOLE VESSATORIE": "unfair_terms",
    "VARIE": "other",
}


class AGCMScraper(BaseScraper):
    """
    Scraper for IT/AGCM -- Italian Competition Authority.
    Country: IT
    URL: https://www.agcm.it

    Fetches decisions from weekly PDF bulletins (Bollettino).
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.temp_dir = tempfile.mkdtemp(prefix="agcm_")

    def _download_pdf_curl(self, url: str) -> Optional[bytes]:
        """Download PDF using curl (bypasses Python SSL issues)."""
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", "120", url],
                capture_output=True,
                timeout=130,
            )
            if result.returncode == 0 and len(result.stdout) > 1000:
                return result.stdout
            return None
        except Exception as e:
            logger.warning(f"curl download failed for {url}: {e}")
            return None

    def _bulletin_exists(self, year: int, week: int) -> bool:
        """Check if a bulletin exists for the given year/week."""
        yy = str(year)[-2:]
        url = BULLETIN_URL_PATTERN.format(year=year, week=week, yy=yy)
        try:
            result = subprocess.run(
                ["curl", "-sI", "--max-time", "10", url],
                capture_output=True,
                timeout=15,
            )
            headers = result.stdout.decode('utf-8', errors='ignore').lower()
            # Must be HTTP 200 AND content-type must be PDF
            is_200 = "200" in headers.split('\n')[0] if headers else False
            is_pdf = "application/pdf" in headers
            return is_200 and is_pdf
        except Exception:
            return False

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF bytes using pdfplumber."""
        if not HAS_PDFPLUMBER:
            logger.warning("pdfplumber not available")
            return None

        try:
            pdf_io = io.BytesIO(pdf_bytes)
            text_parts = []

            with pdfplumber.open(pdf_io) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)

            return "\n\n".join(text_parts)

        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return None

    def _parse_decisions_from_bulletin(
        self, full_text: str, year: int, week: int
    ) -> List[Dict]:
        """
        Parse individual decisions from bulletin text.

        Each decision starts with a pattern like:
        "CASE_REF - CASE_NAME\nProvvedimento n. XXXXX"
        """
        decisions = []

        # Find decision boundaries using "Provvedimento n." pattern
        provv_pattern = re.compile(
            r'([A-Z]+\d+[A-Z]?(?:\s*-\s*[A-Z]+\d+[A-Z]?)*)\s*[-–—]\s*([^\n]+)\n\s*Provvedimento n\.\s*(\d+)',
            re.MULTILINE
        )

        # Also match simpler patterns
        simple_pattern = re.compile(
            r'Provvedimento n\.\s*(\d+)',
            re.MULTILINE
        )

        # Find all provvedimento markers
        provv_markers = list(simple_pattern.finditer(full_text))

        if not provv_markers:
            logger.warning(f"No decisions found in bulletin {year}/{week}")
            return []

        # Extract each decision
        for i, match in enumerate(provv_markers):
            start_pos = match.start()
            end_pos = provv_markers[i + 1].start() if i + 1 < len(provv_markers) else len(full_text)

            decision_text = full_text[start_pos:end_pos].strip()
            provv_num = match.group(1)

            # Look backward from this position to find case ref and name
            lookback_text = full_text[max(0, start_pos - 500):start_pos]

            # Try to extract case reference (e.g., A558, I850, C12659, PS12345)
            case_ref_match = re.search(
                r'([A-Z]+\d+[A-Z]?(?:\s*[-–—]\s*[A-Z]+\d+[A-Z]?)*)\s*[-–—]\s*([^\n]+?)(?:\n|$)',
                lookback_text,
                re.MULTILINE
            )

            if case_ref_match:
                case_ref = case_ref_match.group(1).strip()
                case_name = case_ref_match.group(2).strip()
            else:
                # Try simpler pattern
                simple_ref = re.search(r'([AICP][A-Z]?\d+[A-Z]?)', lookback_text)
                case_ref = simple_ref.group(1) if simple_ref else f"PROVV{provv_num}"
                case_name = ""

            # Determine section/type
            section_type = "other"
            for header, type_name in SECTION_TO_TYPE.items():
                if header in lookback_text or header in decision_text[:500]:
                    section_type = type_name
                    break

            # Clean text
            clean_text = self._clean_text(decision_text)

            if len(clean_text) > 200:  # Only include substantial decisions
                decisions.append({
                    "case_ref": case_ref,
                    "case_name": case_name,
                    "decision_number": provv_num,
                    "decision_type": section_type,
                    "bulletin_year": year,
                    "bulletin_week": week,
                    "text": clean_text,
                })

        return decisions

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        # Remove page headers/footers
        text = re.sub(r'BOLLETTINO N\.\s*\d+\s+DEL\s+\d+\s+\w+\s+\d+\s+\d+', '', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        # Clean up
        text = text.strip()
        return text

    def _parse_date_from_bulletin(self, text: str, year: int) -> Optional[str]:
        """Extract decision date from text."""
        # Look for "NELLA SUA ADUNANZA del DD mese YYYY"
        date_match = re.search(
            r'ADUNANZA del\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
            text,
            re.IGNORECASE
        )

        if date_match:
            day = int(date_match.group(1))
            month_str = date_match.group(2).lower()
            year_found = int(date_match.group(3))

            months = {
                'gennaio': 1, 'febbraio': 2, 'marzo': 3, 'aprile': 4,
                'maggio': 5, 'giugno': 6, 'luglio': 7, 'agosto': 8,
                'settembre': 9, 'ottobre': 10, 'novembre': 11, 'dicembre': 12
            }

            month = months.get(month_str)
            if month:
                return f"{year_found}-{month:02d}-{day:02d}"

        return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        case_ref = raw.get("case_ref", "")
        case_name = raw.get("case_name", "")
        decision_number = raw.get("decision_number", "")
        decision_type = raw.get("decision_type", "other")
        bulletin_year = raw.get("bulletin_year")
        bulletin_week = raw.get("bulletin_week")
        full_text = raw.get("text", "")

        # Extract date from text
        date_iso = self._parse_date_from_bulletin(full_text, bulletin_year)
        if not date_iso and bulletin_year:
            # Approximate date from bulletin week
            try:
                # Week number to approximate date
                jan1 = datetime(bulletin_year, 1, 1)
                delta = timedelta(weeks=bulletin_week - 1)
                approx_date = jan1 + delta
                date_iso = approx_date.strftime("%Y-%m-%d")
            except Exception:
                date_iso = f"{bulletin_year}-01-01"

        # Construct unique ID
        doc_id = f"IT:AGCM:{case_ref}:{decision_number}"

        # Construct title
        title = f"{case_ref}"
        if case_name:
            title += f" - {case_name}"
        if decision_number:
            title += f" (Provv. {decision_number})"

        # Source URL - link to bulletin
        yy = str(bulletin_year)[-2:] if bulletin_year else "00"
        bulletin_url = BULLETIN_URL_PATTERN.format(
            year=bulletin_year or 2024,
            week=bulletin_week or 1,
            yy=yy
        )

        return {
            "_id": doc_id,
            "_source": "IT/AGCM",
            "_type": "regulatory_decision",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": bulletin_url,
            "case_ref": case_ref,
            "case_name": case_name,
            "decision_number": decision_number,
            "decision_type": decision_type,
            "bulletin_year": bulletin_year,
            "bulletin_week": bulletin_week,
            "language": "it",
            "authority": "Autorità Garante della Concorrenza e del Mercato",
            "country": "IT",
        }

    def _fetch_bulletin(self, year: int, week: int) -> List[Dict]:
        """Fetch and parse all decisions from a single bulletin."""
        yy = str(year)[-2:]
        url = BULLETIN_URL_PATTERN.format(year=year, week=week, yy=yy)

        logger.info(f"Fetching bulletin {year}/{week}: {url}")

        # Download PDF
        pdf_bytes = self._download_pdf_curl(url)
        if not pdf_bytes:
            return []

        # Extract text
        full_text = self._extract_text_from_pdf(pdf_bytes)
        if not full_text:
            return []

        # Parse decisions
        decisions = self._parse_decisions_from_bulletin(full_text, year, week)
        logger.info(f"  Found {len(decisions)} decisions")

        return decisions

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all decisions from available bulletins."""
        current_year = datetime.now().year

        # Iterate through years (most recent first)
        for year in range(current_year, 2020, -1):  # Back to 2020
            logger.info(f"Processing year: {year}")

            # Bulletins are numbered 1-52 (approximately)
            for week in range(52, 0, -1):
                if not self._bulletin_exists(year, week):
                    continue

                try:
                    decisions = self._fetch_bulletin(year, week)
                    for decision in decisions:
                        normalized = self.normalize(decision)
                        if normalized.get("text") and len(normalized["text"]) > 500:
                            yield normalized
                except Exception as e:
                    logger.warning(f"Error processing bulletin {year}/{week}: {e}")
                    continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions from bulletins published since a given date."""
        current_date = datetime.now()
        current_year = current_date.year
        since_year = since.year

        for year in range(current_year, since_year - 1, -1):
            # Calculate approximate week range
            if year == current_year:
                end_week = (current_date - datetime(year, 1, 1)).days // 7 + 1
            else:
                end_week = 52

            if year == since_year:
                start_week = (since - datetime(year, 1, 1)).days // 7 + 1
            else:
                start_week = 1

            for week in range(min(end_week, 52), max(start_week - 1, 0), -1):
                if not self._bulletin_exists(year, week):
                    continue

                try:
                    decisions = self._fetch_bulletin(year, week)
                    for decision in decisions:
                        normalized = self.normalize(decision)
                        if normalized.get("text") and len(normalized["text"]) > 500:
                            yield normalized
                except Exception as e:
                    logger.warning(f"Error in update {year}/{week}: {e}")

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch a sample of decisions for validation."""
        samples_collected = 0
        current_year = datetime.now().year

        # Try recent bulletins
        for year in [current_year, current_year - 1]:
            for week in range(52, 0, -1):
                if samples_collected >= count:
                    return

                if not self._bulletin_exists(year, week):
                    continue

                try:
                    decisions = self._fetch_bulletin(year, week)
                    for decision in decisions:
                        if samples_collected >= count:
                            return

                        normalized = self.normalize(decision)
                        if normalized.get("text") and len(normalized["text"]) > 500:
                            yield normalized
                            samples_collected += 1
                            logger.info(
                                f"Sample {samples_collected}: {normalized['case_ref']} "
                                f"({len(normalized['text'])} chars)"
                            )

                except Exception as e:
                    logger.warning(f"Error sampling {year}/{week}: {e}")
                    continue

        logger.info(f"Collected {samples_collected} samples")

    def test_api(self) -> bool:
        """Test API connectivity by checking a recent bulletin."""
        try:
            current_year = datetime.now().year

            # Find a recent bulletin
            for week in range(52, 0, -1):
                if self._bulletin_exists(current_year, week):
                    logger.info(f"Found bulletin: {current_year}/{week}")

                    # Try to download and parse
                    decisions = self._fetch_bulletin(current_year, week)
                    if decisions:
                        logger.info(f"Successfully parsed {len(decisions)} decisions")
                        return True
                    else:
                        logger.warning("Bulletin found but no decisions parsed")
                        return False

            logger.error("No bulletins found for current year")
            return False

        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IT/AGCM data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch only sample records (for validation)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of sample records to fetch"
    )

    args = parser.parse_args()
    scraper = AGCMScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            # Sample mode - save to sample/ directory
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            count = 0
            total_chars = 0

            for record in scraper.fetch_sample(count=args.count):
                filename = f"{record['_id'].replace(':', '_').replace('/', '_')}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                count += 1
                text_len = len(record.get("text", ""))
                total_chars += text_len
                logger.info(f"Saved: {filename} ({text_len} chars)")

            avg_chars = total_chars // count if count > 0 else 0
            logger.info(f"Sample complete: {count} records, avg {avg_chars} chars/doc")

        else:
            # Full bootstrap
            count = 0
            for record in scraper.fetch_all():
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} records")

            logger.info(f"Bootstrap complete: {count} total records")

    elif args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=30)

        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info(f"Update: {record.get('case_ref')}")

        logger.info(f"Update complete: {count} new records")


if __name__ == "__main__":
    main()
