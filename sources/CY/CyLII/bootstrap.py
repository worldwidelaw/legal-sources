#!/usr/bin/env python3
"""
CY/CyLII -- Cyprus Legal Information Institute (CyLaw)

Fetches consolidated legislation and Supreme Court case law from
cylaw.org, the Cyprus Legal Information Institute.

Strategy:
  - Legislation: enumerate directories from Apache listing at
    /nomoi/enop/non-ind/, fetch full.html for each law
  - Case law: enumerate cases from year indexes at
    /apofaseis/aad/index_{year}.html, fetch via cgi-bin/open.pl

Data: ~1700 laws + tens of thousands of court decisions
Content language: Greek
Encoding: windows-1253
License: Open access, attribution required

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape
from urllib.parse import unquote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CY.CyLII")

BASE_URL = "https://www.cylaw.org"

# Greek months for date parsing
GREEK_MONTHS = {
    'Ιανουαρίου': '01', 'Φεβρουαρίου': '02', 'Μαρτίου': '03',
    'Απριλίου': '04', 'Μαΐου': '05', 'Ιουνίου': '06',
    'Ιουλίου': '07', 'Αυγούστου': '08', 'Σεπτεμβρίου': '09',
    'Οκτωβρίου': '10', 'Νοεμβρίου': '11', 'Δεκεμβρίου': '12',
    'Ιανουαρίου,': '01', 'Φεβρουαρίου,': '02', 'Μαρτίου,': '03',
    'Απριλίου,': '04', 'Μαΐου,': '05', 'Ιουνίου,': '06',
    'Ιουλίου,': '07', 'Αυγούστου,': '08', 'Σεπτεμβρίου,': '09',
    'Οκτωβρίου,': '10', 'Νοεμβρίου,': '11', 'Δεκεμβρίου,': '12',
}


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.S | re.I)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.S | re.I)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
    text = re.sub(r'</?(?:p|div)[^>]*>', '\n\n', text, flags=re.I)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = text.replace('&nbsp;', ' ').replace('\xa0', ' ')
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +\n', '\n', text)
    return text.strip()


def decode_response(content: bytes) -> str:
    """Decode response trying windows-1253 first, then utf-8."""
    for enc in ('windows-1253', 'iso-8859-7', 'utf-8'):
        try:
            return content.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return content.decode('utf-8', errors='replace')


def parse_greek_date(text: str) -> Optional[str]:
    """Parse Greek date like '9 Ιανουαρίου, 2024' to ISO format."""
    if not text:
        return None
    m = re.search(r'(\d{1,2})\s+(\S+?),?\s+(\d{4})', text)
    if not m:
        return None
    day = m.group(1).zfill(2)
    month_name = m.group(2).rstrip(',')
    year = m.group(3)
    month = GREEK_MONTHS.get(month_name) or GREEK_MONTHS.get(month_name + ',')
    if not month:
        return None
    return f"{year}-{month}-{day}"


class CYCyLIIScraper(BaseScraper):
    """
    Scraper for CY/CyLII -- Cyprus Legal Information Institute.
    Country: CY
    URL: https://www.cylaw.org

    Data types: legislation, case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _get(self, url: str, max_retries: int = 3) -> Optional[bytes]:
        """GET with retry logic, returns raw bytes."""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 200:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
        return None

    # ── Legislation ──────────────────────────────────────────────

    def _list_law_dirs(self) -> list[str]:
        """List law directory names from Apache index."""
        raw = self._get(f"{BASE_URL}/nomoi/enop/non-ind/")
        if not raw:
            logger.error("Failed to fetch legislation index")
            return []
        html = decode_response(raw)
        # Apache directory listing has href="dirname/"
        dirs = re.findall(r'href="([^"]+)/"', html)
        # Filter out parent directory links
        law_dirs = [d for d in dirs if d and d != '/nomoi/enop/' and not d.startswith('?') and not d.startswith('/')]
        logger.info(f"Found {len(law_dirs)} law directories")
        return law_dirs

    def _fetch_law(self, law_dir: str) -> Optional[dict]:
        """Fetch a single law's full text."""
        url = f"{BASE_URL}/nomoi/enop/non-ind/{law_dir}/full.html"
        raw = self._get(url)
        if not raw:
            return None

        html = decode_response(raw)

        # Extract title
        title_m = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL)
        title = title_m.group(1).strip() if title_m else law_dir

        # Extract body content
        body_m = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL | re.I)
        body_html = body_m.group(1) if body_m else html
        text = strip_html(body_html)

        # Remove navigation elements from text
        for nav_text in ['CyLaw', 'Αναφορικά μ\'εμάς', 'Επικοινωνία',
                         'Κατάλογος Ενοποιημένης Νομοθεσίας',
                         'Περιεχόμενα', 'Πλήρες Κείμενο', 'Εκτύπωση',
                         'Ιστορικό Τροποποιήσεων',
                         'ΠΑΓΚΥΠΡΙΟΣ ΔΙΚΗΓΟΡΙΚΟΣ ΣΥΛΛΟΓΟΣ']:
            text = text.replace(nav_text, '', 1)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        if len(text) < 50:
            logger.warning(f"Too short text for law {law_dir}: {len(text)} chars")
            return None

        # Decode URL-encoded directory name for display
        decoded_dir = unquote(law_dir)

        return {
            "_id": f"law-{law_dir}",
            "_source": "CY/CyLII",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": law_dir,
            "title": title,
            "text": text,
            "date": None,
            "url": url,
            "law_chapter": decoded_dir,
        }

    # ── Case Law ─────────────────────────────────────────────────

    def _list_case_years(self, court: str = "aad") -> list[str]:
        """List available years for a court type."""
        raw = self._get(f"{BASE_URL}/apofaseis/{court}/")
        if not raw:
            return []
        html = decode_response(raw)
        years = re.findall(r'index_(\d{4})\.html', html)
        return sorted(years)

    def _list_cases_for_year(self, court: str, year: str) -> list[str]:
        """List case file paths from a year index page."""
        raw = self._get(f"{BASE_URL}/apofaseis/{court}/index_{year}.html")
        if not raw:
            return []
        html = decode_response(raw)
        # Links like /cgi-bin/open.pl?file=/apofaseis/aad/meros_1/2024/filename.htm
        paths = re.findall(r'open\.pl\?file=([^\s"\']+\.htm)', html)
        return paths

    def _fetch_case(self, file_path: str) -> Optional[dict]:
        """Fetch a single case law document."""
        url = f"{BASE_URL}/cgi-bin/open.pl?file={file_path}"
        raw = self._get(url)
        if not raw:
            return None

        html = decode_response(raw)

        # Extract title
        title_m = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL)
        title = title_m.group(1).strip() if title_m else file_path

        # Extract body content
        body_m = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL | re.I)
        body_html = body_m.group(1) if body_m else html
        text = strip_html(body_html)

        # Remove navigation elements
        for nav_text in ['CyLaw', 'Αναφορικά μ\'εμάς', 'Επικοινωνία',
                         'Όροι χρήσης', 'Έρευνα', 'Κατάλογος Αποφάσεων',
                         'Εμφάνιση Αναφορών (Noteup on)',
                         'Αφαίρεση Υπογραμμίσεων',
                         'ΠΑΓΚΥΠΡΙΟΣ ΔΙΚΗΓΟΡΙΚΟΣ ΣΥΛΛΟΓΟΣ']:
            text = text.replace(nav_text, '', 1)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        if len(text) < 50:
            logger.warning(f"Too short text for case {file_path}: {len(text)} chars")
            return None

        # Parse date from title (e.g., "9/1/2024" or "9 Ιανουαρίου, 2024")
        date = None
        # Try DD/MM/YYYY at end of title
        date_m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})\s*$', title)
        if date_m:
            date = f"{date_m.group(3)}-{date_m.group(2).zfill(2)}-{date_m.group(1).zfill(2)}"
        else:
            date = parse_greek_date(text[:500])

        # Extract case number from file path
        # e.g. /apofaseis/aad/meros_1/2024/1-202401-50-16EDD.htm
        case_id = Path(file_path).stem

        return {
            "_id": f"case-{case_id}",
            "_source": "CY/CyLII",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": case_id,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": "Supreme Court of Cyprus",
            "file_path": file_path,
        }

    # ── BaseScraper interface ────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation and case law documents."""
        # Legislation
        law_dirs = self._list_law_dirs()
        for i, law_dir in enumerate(law_dirs):
            doc = self._fetch_law(law_dir)
            if doc:
                yield doc
            if (i + 1) % 50 == 0:
                logger.info(f"Legislation progress: {i+1}/{len(law_dirs)}")
            time.sleep(1)

        # Case law (Supreme Court)
        years = self._list_case_years("aad")
        for year in years:
            cases = self._list_cases_for_year("aad", year)
            logger.info(f"Year {year}: {len(cases)} cases")
            for case_path in cases:
                doc = self._fetch_case(case_path)
                if doc:
                    yield doc
                time.sleep(1)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Yield documents modified since a date."""
        # Use recent years for updates
        current_year = datetime.now().year
        for year in range(current_year - 1, current_year + 1):
            cases = self._list_cases_for_year("aad", str(year))
            for case_path in cases:
                doc = self._fetch_case(case_path)
                if doc:
                    yield doc
                time.sleep(1)

    def normalize(self, raw: dict) -> dict:
        """Already normalized during fetch."""
        return raw

    def test_api(self) -> dict:
        """Test connectivity to cylaw.org."""
        results = {"legislation": False, "case_law": False}

        # Test legislation
        raw = self._get(f"{BASE_URL}/nomoi/enop/non-ind/")
        if raw:
            html = decode_response(raw)
            dirs = re.findall(r'href="([^"]+)/"', html)
            results["legislation"] = len(dirs) > 100
            logger.info(f"Legislation index: {len(dirs)} directories")

        # Test case law
        raw = self._get(f"{BASE_URL}/apofaseis/aad/")
        if raw:
            html = decode_response(raw)
            years = re.findall(r'index_(\d{4})\.html', html)
            results["case_law"] = len(years) > 10
            logger.info(f"Case law: {len(years)} year indexes")

        return results


# ── CLI ──────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="CY/CyLII scraper")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = CYCyLIIScraper()

    if args.command == "test-api":
        results = scraper.test_api()
        print(json.dumps(results, indent=2))
        sys.exit(0 if all(results.values()) else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        if args.sample:
            # Sample mode: get 8 laws + 7 cases
            logger.info("Sample mode: fetching 8 laws + 7 cases")

            # Legislation sample
            law_dirs = scraper._list_law_dirs()
            for law_dir in law_dirs[:8]:
                doc = scraper._fetch_law(law_dir)
                if doc:
                    out_path = sample_dir / f"{count:04d}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(doc, f, ensure_ascii=False, indent=2)
                    logger.info(f"[{count+1}] {doc['_type']}: {doc['title'][:80]} ({len(doc['text'])} chars)")
                    count += 1
                time.sleep(1)

            # Case law sample - pick recent year
            years = scraper._list_case_years("aad")
            if years:
                # Use the most recent year with cases
                for year in reversed(years):
                    cases = scraper._list_cases_for_year("aad", year)
                    if cases:
                        logger.info(f"Sampling cases from year {year} ({len(cases)} available)")
                        for case_path in cases[:7]:
                            doc = scraper._fetch_case(case_path)
                            if doc:
                                out_path = sample_dir / f"{count:04d}.json"
                                with open(out_path, "w", encoding="utf-8") as f:
                                    json.dump(doc, f, ensure_ascii=False, indent=2)
                                logger.info(f"[{count+1}] {doc['_type']}: {doc['title'][:80]} ({len(doc['text'])} chars)")
                                count += 1
                            time.sleep(1)
                        break
        else:
            # Full bootstrap
            for doc in scraper.fetch_all():
                count += 1
                if count % 100 == 0:
                    logger.info(f"Progress: {count} documents fetched")

        logger.info(f"Done. Total records: {count}")
        sys.exit(0)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
