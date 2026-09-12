#!/usr/bin/env python3
"""
PL/DziennikUrzedowy -- Polish Official Journal Data Fetcher

Fetches Polish legislation from Dziennik Ustaw (Official Journal of the
Republic of Poland) using the ELI API provided by the Sejm.

Strategy:
  - List acts by year: GET /eli/acts/DU/{year} returns JSON with all acts
  - Get act metadata: GET /eli/acts/DU/{year}/{pos} returns detailed metadata
  - Get full text: GET /eli/acts/DU/{year}/{pos}/text.html returns HTML content,
    falling back to /text.pdf — most of the corpus (all of 2025+, most pre-2015)
    is published as PDF only, with textHTML=false

API Documentation:
  - Base URL: https://api.sejm.gov.pl
  - ELI endpoint: /eli/acts/DU/{year}/{position}
  - Full text: /eli/acts/DU/{year}/{position}/text.html
  - Publishers: DU (Dziennik Ustaw), MP (Monitor Polski)

Data Coverage:
  - Dziennik Ustaw from 1918 to present
  - Laws, regulations, treaties, announcements
  - Approximately 2,000 acts per year

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent acts)
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.DziennikUrzedowy")

# API configuration
BASE_URL = "https://api.sejm.gov.pl"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Transient upstream failures worth retrying rather than silently dropping.
RETRY_STATUS = {429, 500, 502, 503, 504}
RETRY_ATTEMPTS = 5

# Dziennik Ustaw runs from 1918 to the present. Only a slice of it is served as
# HTML: 2015-2024 is fully HTML, 2025 onwards is 100% PDF-only (textHTML=false on
# every act), and the pre-2015 archive is mostly PDF too (2005: 309 HTML of 2,260;
# 1980: 12 of 132). So HTML-only crawling can never reach most of the corpus —
# every act does have a PDF, which is what _get_full_text falls back to.
FIRST_YEAR = 1918


def years_to_scrape() -> List[int]:
    """Newest year first, so a truncated run still lands the most useful acts."""
    return list(range(datetime.now().year, FIRST_YEAR - 1, -1))


class DziennikUrzedowyScraper(BaseScraper):
    """
    Scraper for PL/DziennikUrzedowy -- Polish Official Journal.
    Country: PL
    URL: https://www.dziennikustaw.gov.pl

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.checkpoint_path = source_dir / "data" / "years_done.json"

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
            "Accept-Language": "pl,en;q=0.9",
        })

    def _request(self, endpoint: str, timeout: int = 60) -> Optional[requests.Response]:
        """
        GET an API endpoint, retrying transient failures.

        api.sejm.gov.pl intermittently 500s on year listings (seen live on
        /eli/acts/DU/2025). Without a retry that one blip drops a whole year of
        acts and the run still exits 0, so the loss is invisible.
        """
        url = f"{BASE_URL}{endpoint}"
        delay = 2.0
        for attempt in range(RETRY_ATTEMPTS):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code in RETRY_STATUS and attempt < RETRY_ATTEMPTS - 1:
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        f"{endpoint} returned {resp.status_code}, retrying in {wait:.0f}s "
                        f"(attempt {attempt + 1}/{RETRY_ATTEMPTS})"
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                resp.raise_for_status()
                return resp
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    logger.warning(f"API request failed for {endpoint}: {e}")
                    return None
                time.sleep(min(delay, 120))
                delay = min(delay * 2, 120)
            except Exception as e:
                logger.warning(f"API request failed for {endpoint}: {e}")
                return None
        return None

    def _api_get(self, endpoint: str, timeout: int = 60) -> Optional[dict]:
        """Make GET request to API endpoint."""
        resp = self._request(endpoint, timeout=timeout)
        if resp is None:
            return None
        try:
            return resp.json()
        except requests.exceptions.JSONDecodeError:
            # Some endpoints return HTML, not JSON
            return None

    def _api_get_html(self, endpoint: str, timeout: int = 60) -> str:
        """Make GET request and return HTML content."""
        resp = self._request(endpoint, timeout=timeout)
        return resp.text if resp is not None else ""

    def _list_acts_by_year(self, year: int) -> List[Dict[str, Any]]:
        """
        List all acts for a given year.

        Returns list of act metadata dicts.
        """
        endpoint = f"/eli/acts/DU/{year}"
        data = self._api_get(endpoint)
        if data and "items" in data:
            logger.info(f"Found {data.get('count', len(data['items']))} acts for {year}")
            return data["items"]
        return []

    def _get_act_details(self, year: int, pos: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed metadata for a specific act.

        Returns dict with full metadata or None on failure.
        """
        endpoint = f"/eli/acts/DU/{year}/{pos}"
        return self._api_get(endpoint)

    def _get_full_text(self, year: int, pos: int, act: Optional[dict] = None) -> str:
        """
        Fetch the act's full text, preferring HTML and falling back to the PDF.

        Every act is published as a PDF but only some carry an HTML rendering
        (none at all since 2025), so the PDF path is the one that reaches most
        of the corpus. Returns "" when neither format yields text.
        """
        has_html = True if act is None else bool(act.get("textHTML"))

        text = self._get_html_text(year, pos) if has_html else ""
        if text:
            return text

        return self._get_pdf_text(year, pos, act)

    def _get_pdf_text(self, year: int, pos: int, act: Optional[dict] = None) -> str:
        """Extract text from the act's PDF rendering."""
        if act is not None and not act.get("textPDF", True):
            return ""

        url = f"{BASE_URL}/eli/acts/DU/{year}/{pos}/text.pdf"
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"PDF download failed for DU/{year}/{pos}: {e}")
            return ""

        try:
            # force=True: an act only reaches this path because we want its text
            # now — either it is new, or fetch_updates saw its changeDate move.
            # The default skip-if-already-in-Neon guard would return None here
            # and hand the record back with no text at all.
            text = extract_pdf_markdown(
                "PL/DziennikUrzedowy",
                f"DU/{year}/{pos}",
                pdf_bytes=pdf_bytes,
                table="legislation",
                force=True,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for DU/{year}/{pos}: {e}")
            return ""

        return (text or "").strip()

    def _get_html_text(self, year: int, pos: int) -> str:
        """
        Fetch and extract full text from HTML.

        Returns cleaned plain text.
        """
        endpoint = f"/eli/acts/DU/{year}/{pos}/text.html"
        html_content = self._api_get_html(endpoint)

        if not html_content:
            return ""

        # Extract text from HTML
        text_parts = []

        # Remove script and style tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML comments
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

        # Extract text between tags, preserving structure
        # Get headers
        for match in re.findall(r'<h[1-6][^>]*>(.*?)</h[1-6]>', content, re.DOTALL | re.IGNORECASE):
            text = re.sub(r'<[^>]+>', '', match)
            text = html.unescape(text.strip())
            text = re.sub(r'\s+', ' ', text).strip()
            if text:
                text_parts.append(text)

        # Get paragraphs and divs with text content
        for tag in ['div', 'p', 'span']:
            pattern = f'<{tag}[^>]*data-template="xText"[^>]*>(.*?)</{tag}>'
            for match in re.findall(pattern, content, re.DOTALL | re.IGNORECASE):
                text = re.sub(r'<[^>]+>', '', match)
                text = html.unescape(text.strip())
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 5:
                    text_parts.append(text)

        # If xText extraction yielded little, try broader extraction
        if len('\n'.join(text_parts)) < 500:
            text_parts = []
            # Get all visible text between tags
            for match in re.findall(r'>([^<]+)<', content):
                text = match.strip()
                # Filter out noise
                if len(text) < 3:
                    continue
                if text.startswith('{') or text.startswith('function'):
                    continue
                text = html.unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                if text and len(text) > 2:
                    text_parts.append(text)

        full_text = '\n'.join(text_parts)

        # Clean up excessive whitespace while preserving paragraphs
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r' +', ' ', full_text)

        return full_text.strip()

    def _load_checkpoint(self) -> set:
        """Years already crawled to completion, so a re-launch skips them."""
        try:
            with open(self.checkpoint_path, encoding="utf-8") as fh:
                return set(json.load(fh).get("years_done", []))
        except (OSError, ValueError):
            return set()

    def _mark_year_done(self, year: int):
        done = self._load_checkpoint()
        done.add(year)
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"years_done": sorted(done)}, fh)
        tmp.replace(self.checkpoint_path)

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield every act in Dziennik Ustaw, newest year first.

        Acts are fetched as HTML where available and as PDF otherwise — the
        whole 2025+ range and most of the pre-2015 archive is PDF-only, so
        skipping textHTML=false acts (as this used to) hid most of the corpus.
        Acts whose text cannot be extracted in either format are skipped.

        Completed years are checkpointed: at ~100K acts the corpus outlives a
        single fleet slot, and a re-launch resumes at the first unfinished year
        without re-fetching anything.
        """
        logger.info("Starting full Dziennik Ustaw fetch...")
        years_done = self._load_checkpoint()

        for year in years_to_scrape():
            if year in years_done:
                logger.info(f"Skipping {year} — already crawled (checkpoint)")
                continue

            logger.info(f"Fetching acts from {year}...")
            acts = self._list_acts_by_year(year)

            for act in acts:
                pos = act.get("pos")
                if not pos:
                    continue

                # Get detailed metadata
                details = self._get_act_details(year, pos)
                if details:
                    act.update(details)

                # Get full text
                full_text = self._get_full_text(year, pos, act)
                if full_text:
                    act["full_text"] = full_text
                    yield act
                else:
                    logger.warning(f"Could not fetch text for {year}/{pos}, skipping")

            self._mark_year_done(year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield acts updated since the given date.

        Checks recent years for acts with changeDate after since.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        current_year = datetime.now().year
        matched = 0
        textless = 0

        # Check last 2 years
        for year in [current_year, current_year - 1]:
            logger.info(f"Checking {year} for updates since {since_str}...")
            acts = self._list_acts_by_year(year)

            for act in acts:
                change_date = act.get("changeDate", "")
                if not change_date or change_date < since_str:
                    continue
                pos = act.get("pos")
                if not pos:
                    continue

                matched += 1
                details = self._get_act_details(year, pos)
                if details:
                    act.update(details)

                full_text = self._get_full_text(year, pos, act)
                if not full_text:
                    # Never yield a textless record: ingest rejects it anyway and
                    # a batch of them reads as a detail-extraction failure (#1507).
                    textless += 1
                    logger.warning(
                        f"No text for DU/{year}/{pos} ({act.get('displayAddress')}), skipping"
                    )
                    continue

                act["full_text"] = full_text
                yield act

        logger.info(
            f"Update scan: {matched} acts changed since {since_str}, "
            f"{matched - textless} with text, {textless} skipped as textless"
        )

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw API data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        eli = raw.get("ELI", "")
        year = raw.get("year", 0)
        pos = raw.get("pos", 0)

        # Create unique document ID
        doc_id = eli if eli else f"DU/{year}/{pos}"

        # Get dates
        promulgation = raw.get("promulgation", "")
        announcement = raw.get("announcementDate", "")
        date = promulgation or announcement

        # Build URL
        url = f"https://www.dziennikustaw.gov.pl/{year}/{pos}"
        if eli:
            url = f"https://isap.sejm.gov.pl/isap.nsf/DocDetails.xsp?id=W{eli.replace('/', '')}"

        # Get full text
        full_text = raw.get("full_text", "")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "PL/DziennikUrzedowy",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "eli": eli,
            "address": raw.get("address", ""),
            "display_address": raw.get("displayAddress", ""),
            "year": year,
            "pos": pos,
            "doc_type": raw.get("type", ""),
            "status": raw.get("status", ""),
            "in_force": raw.get("inForce", ""),
            "entry_into_force": raw.get("entryIntoForce", ""),
            "keywords": raw.get("keywords", []),
            "released_by": raw.get("releasedBy", []),
            "references": raw.get("references", {}),
            "language": "pl",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Polish Dziennik Ustaw ELI API...")

        # Test year listing
        print("\n1. Testing year listing endpoint...")
        acts = self._list_acts_by_year(2024)
        if acts:
            print(f"   Found {len(acts)} acts for 2024")
            print(f"   First act: {acts[0].get('title', '')[:60]}...")
        else:
            print("   ERROR: No acts returned")
            return

        # Test act details
        print("\n2. Testing act details endpoint...")
        if acts:
            first_pos = acts[0].get("pos")
            details = self._get_act_details(2024, first_pos)
            if details:
                print(f"   Act: {details.get('displayAddress')}")
                print(f"   Type: {details.get('type')}")
                print(f"   Status: {details.get('status')}")
                print(f"   Has HTML text: {details.get('textHTML')}")
            else:
                print("   ERROR: Could not fetch details")

        # Test full text (HTML path)
        print("\n3. Testing full text endpoint (HTML)...")
        if acts:
            first_pos = acts[0].get("pos")
            text = self._get_full_text(2024, first_pos, acts[0])
            if text:
                print(f"   Text length: {len(text)} characters")
                print(f"   Preview: {text[:200]}...")
            else:
                print("   WARNING: Could not fetch full text")

        # Test full text (PDF fallback) — every act since 2025 is PDF-only
        print("\n4. Testing full text fallback (PDF, current year)...")
        this_year = datetime.now().year
        recent = self._list_acts_by_year(this_year)
        if recent:
            act = recent[-1]
            text = self._get_full_text(this_year, act.get("pos"), act)
            print(f"   {act.get('displayAddress')} textHTML={act.get('textHTML')}")
            if text:
                print(f"   Text length: {len(text)} characters")
                print(f"   Preview: {text[:200]}...")
            else:
                print("   WARNING: Could not fetch full text from PDF")

        print("\nAPI test complete!")


def main():
    scraper = DziennikUrzedowyScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
