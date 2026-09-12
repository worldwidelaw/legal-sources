#!/usr/bin/env python3
"""
GE/SupremeCourt -- Georgian Supreme Court Data Fetcher

Fetches Georgian Supreme Court case law from the official website.

Strategy:
  - Search endpoint returns paginated case listings by chamber (palata)
  - Three chambers: 0=Administrative, 1=Civil, 2=Criminal
  - Full text available via /fullcase/{id}/{palata} endpoint
  - HTML content with Georgian text

Endpoints:
  - Search: GET https://www.supremecourt.ge/ka/getCases?palata={0|1|2}&page={n}
    Optional server-side filters (the search form's own params):
      tarigiDan=YYYY-MM-DD  decision date >= this
      tarigiMde=YYYY-MM-DD  decision date <= this
  - Full case: GET https://www.supremecourt.ge/ka/fullcase/{id}/{palata}

Data:
  - ~88,000 decisions across all chambers (27,674 / 33,400 / 27,066)
  - Language: Georgian (KA)
  - Rate limit: 1-2 requests/second (the host answers HTTP 429 above that)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # Same, streamed to data/records.jsonl
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (date-windowed)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import math
import re
import html
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GE.supremecourt")

# Base URL for Georgian Supreme Court
BASE_URL = "https://www.supremecourt.ge"

# Chambers: 0=Administrative, 1=Civil, 2=Criminal
CHAMBERS = {
    0: "administrative",
    1: "civil",
    2: "criminal",
}

# Results per listing page, fixed by the server.
PAGE_SIZE = 30

# How far BEFORE `since` an incremental refresh starts its date window.
#
# `since` is a CRAWL time but `tarigiDan` filters on the DECISION date, and the
# court publishes a decision some weeks after it is handed down — a decision
# dated inside the last refresh's window can therefore appear only afterwards.
# Anchoring the window straight on `since` would skip exactly those. Six months
# is far wider than the observed lag (the newest listed decision was 25 days old
# when this was written) and still costs ~1,457 of 88,140 cases per refresh.
UPDATE_LOOKBACK_DAYS = 180

# Earliest decision date in the corpus; clamps the window so a stale `since`
# cannot ask the server for a nonsensical range.
CORPUS_START_DATE = "2010-01-01"

# Search results are a flat run of per-case cards. Splitting on the card
# wrapper keeps every field lookup inside the case it belongs to.
CARD_SPLIT_RE = re.compile(r'<div class="cases clearfix">')
TOTAL_RE = re.compile(r'მოიძებნა (\d+) გადაწყვეტილება')
CARD_ID_RE = re.compile(r'seeMore\((\d+),(\d+)\)')
CARD_NUMBER_RE = re.compile(r'საქმის ნომერი:</span>\s*([^<]+)<')
CARD_DATE_RE = re.compile(r'თარიღი:</span>\s*([^<]+)<')
CARD_SUBJECT_RE = re.compile(r'დავის საგანი:</span>\s*([^<]*)<')
CARD_RESULT_RE = re.compile(r'შედეგი:</span>\s*([^<]+)<')
CARD_TYPE_RE = re.compile(r'საჩივრის სახე:</span>\s*([^<]+)<')


def _card_field(block: str, pattern: re.Pattern) -> str:
    """Read one labelled field out of a single result card, or '' if absent."""
    match = pattern.search(block)
    return re.sub(r"\s+", " ", match.group(1)).strip() if match else ""


class GeorgianSupremeCourtScraper(BaseScraper):
    """
    Scraper for GE/SupremeCourt -- Georgian Supreme Court.
    Country: GE
    URL: https://www.supremecourt.ge

    Data types: case_law
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ka,en",
            },
            timeout=60,
        )

    def _listing_html(self, palata: int, page: int, since_date: Optional[str]) -> str:
        """Fetch one listing page, raising loudly if the host refuses it.

        The host answers HTTP 429 as soon as requests come faster than ~1/s.
        Swallowing that into an empty list is indistinguishable from "no more
        pages", so a throttling burst used to end a chamber's walk early and
        truncate the corpus silently. HttpClient already retries 429/5xx with
        backoff; if it still fails, that is a real failure and must surface.
        """
        params: Dict[str, Any] = {"palata": palata, "page": page}
        if since_date:
            params["tarigiDan"] = since_date

        last_error: Optional[Exception] = None
        for attempt in range(3):
            self.rate_limiter.wait()
            try:
                resp = self.client.get("/ka/getCases", params=params)
                resp.raise_for_status()
                return resp.text
            except Exception as e:  # noqa: BLE001 - re-raised below
                last_error = e
                logger.warning(
                    f"Chamber {palata} page {page}: listing fetch failed "
                    f"(attempt {attempt + 1}/3): {e}"
                )
                time.sleep(5 * (attempt + 1))

        raise RuntimeError(
            f"Chamber {palata} page {page}: listing unreachable after 3 attempts "
            f"({last_error}) — refusing to treat a fetch failure as end-of-results"
        )

    def _get_cases_page(
        self,
        palata: int,
        page: int = 1,
        since_date: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        """
        Fetch a page of cases from the search endpoint.

        Args:
            palata: chamber id
            page: 1-indexed page number
            since_date: optional YYYY-MM-DD server-side floor on the decision date

        Returns (cases, total) where cases is a list of dicts with
        id, palata, case_number, date, subject, result, type — and total is the
        result count the page reports for the whole query (None if absent).
        """
        cases: List[Dict[str, Any]] = []
        total: Optional[int] = None

        content = self._listing_html(palata, page, since_date)

        # Check if empty/no results
        if "მოიძებნა 0" in content or len(content) < 200:
            return [], 0

        total_match = TOTAL_RE.search(content)
        if total_match:
            total = int(total_match.group(1))
            if page == 1:
                scope = f" since {since_date}" if since_date else ""
                logger.info(
                    f"Chamber {palata} ({CHAMBERS.get(palata, 'unknown')}): "
                    f"{total} decisions{scope}"
                )

        try:
            # Each result is one <div class="cases clearfix"> card holding the
            # metadata first and the seeMore(ID, PALATA) link last. Fields must
            # be read WITHIN a card: a cross-card `.*?` search anchored on
            # seeMore() walks forward into the *next* card's metadata and
            # silently shifts every case number by one row (issue #1474).
            blocks = CARD_SPLIT_RE.split(content)[1:]

            for block in blocks:
                id_match = CARD_ID_RE.search(block)
                if not id_match:
                    continue

                case_number = _card_field(block, CARD_NUMBER_RE)
                if not case_number:
                    logger.warning(
                        f"Chamber {palata} page {page}: card {id_match.group(1)} "
                        f"has no case number — skipping rather than borrowing a neighbour's"
                    )
                    continue

                cases.append({
                    "id": id_match.group(1),
                    "palata": int(id_match.group(2)),
                    "case_number": case_number,
                    "date": _card_field(block, CARD_DATE_RE),
                    "subject": _card_field(block, CARD_SUBJECT_RE),
                    "result": _card_field(block, CARD_RESULT_RE),
                    "type": _card_field(block, CARD_TYPE_RE),
                })

            if len(cases) != len(blocks):
                logger.warning(
                    f"Chamber {palata} page {page}: parsed {len(cases)} of "
                    f"{len(blocks)} result cards"
                )

            logger.debug(f"Page {page} of chamber {palata}: found {len(cases)} cases")
            return cases, total

        except Exception as e:
            # Parse failures are per-page and non-fatal; fetch failures are not
            # and were already raised by _listing_html above.
            logger.error(f"Failed to parse cases page {page} for chamber {palata}: {e}")
            return [], total

    def _fetch_full_case(self, case_id: str, palata: int) -> Optional[Dict[str, Any]]:
        """
        Fetch the full text of a case.

        Returns dict with: title, text, or None on failure.
        """
        try:
            url = f"/ka/fullcase/{case_id}/{palata}"

            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            content = resp.text

            # Extract title from <title> tag
            title_match = re.search(r'<title>([^<]+)</title>', content)
            title = ""
            if title_match:
                title = html.unescape(title_match.group(1)).strip()

            # Extract full text from modalBody div
            # The content is between <div class="case-single mt-5" id="modalBody"> and its closing tag
            modal_start = content.find('id="modalBody">')
            if modal_start == -1:
                logger.warning(f"No modalBody found for case {case_id}")
                return None

            modal_start = content.find('>', modal_start) + 1

            # Find end of case content - typically ends with </div> before the script
            modal_end = content.find('</div>', modal_start)

            # Actually get a larger chunk and clean it
            # Look for closing </div> that matches, accounting for nesting
            depth = 1
            pos = modal_start
            while depth > 0 and pos < len(content):
                next_open = content.find('<div', pos)
                next_close = content.find('</div>', pos)

                if next_close == -1:
                    break

                if next_open != -1 and next_open < next_close:
                    depth += 1
                    pos = next_open + 4
                else:
                    depth -= 1
                    if depth == 0:
                        modal_end = next_close
                    else:
                        pos = next_close + 6

            if modal_end == -1 or modal_end <= modal_start:
                # Fallback: just take a large chunk
                modal_end = min(modal_start + 500000, len(content))

            raw_html = content[modal_start:modal_end]

            # Clean HTML to extract text
            text = self._extract_text(raw_html)

            if not text or len(text) < 50:
                logger.warning(f"Very short text for case {case_id}: {len(text)} chars")

            return {
                "title": title,
                "text": text,
            }

        except Exception as e:
            logger.warning(f"Failed to fetch full case {case_id}/{palata}: {e}")
            return None

    def _extract_text(self, raw_html: str) -> str:
        """Extract clean text from case HTML."""
        # Remove script and style tags
        text = re.sub(r'<script[^>]*>.*?</script>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)

        # Convert breaks and paragraphs to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Decode HTML entities (including numeric entities like &#4321;)
        text = html.unescape(text)

        # Clean up whitespace while preserving paragraph structure
        lines = []
        for line in text.split('\n'):
            line = re.sub(r'\s+', ' ', line).strip()
            if line:
                lines.append(line)

        return '\n'.join(lines)

    def _build_record(self, case_info: Dict[str, Any]) -> Optional[dict]:
        """Attach the full decision text to one listing card, or None to skip."""
        case_id = case_info["id"]
        case_palata = case_info["palata"]

        full_case = self._fetch_full_case(case_id, case_palata)
        if not full_case:
            return None

        text = full_case.get("text") or ""
        if len(text) < 50:
            logger.warning(f"Skipping case {case_id}: no/short text")
            return None

        return {
            "id": case_id,
            "palata": case_palata,
            "chamber": CHAMBERS.get(case_palata, "unknown"),
            "case_number": case_info.get("case_number", ""),
            "date": case_info.get("date", ""),
            "subject": case_info.get("subject", ""),
            "result": case_info.get("result", ""),
            "case_type": case_info.get("type", ""),
            "title": full_case.get("title", ""),
            "text": text,
        }

    def _walk_chamber(
        self,
        palata: int,
        since_date: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """Page a chamber's listing, yielding one raw record per case.

        `since_date` is passed straight to the endpoint's own `tarigiDan`
        filter, so a windowed walk costs only the pages inside the window.
        """
        page = 1
        expected_pages: Optional[int] = None

        while True:
            cases, total = self._get_cases_page(palata, page, since_date)

            if page == 1 and total:
                expected_pages = math.ceil(total / PAGE_SIZE)

            if not cases:
                logger.info(f"Chamber {palata}: no more results after page {page}")
                break

            for case_info in cases:
                record = self._build_record(case_info)
                if record:
                    yield record

            if expected_pages is not None and page >= expected_pages:
                logger.info(
                    f"Chamber {palata}: reached the reported last page "
                    f"({expected_pages})"
                )
                break

            page += 1

            if max_pages is not None and page > max_pages:
                logger.warning(f"Chamber {palata}: reached page limit {max_pages}")
                break

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Georgian Supreme Court.

        Iterates through all chambers and all pages.
        """
        for palata in CHAMBERS.keys():
            logger.info(f"Processing chamber {palata} ({CHAMBERS[palata]})...")
            # ~60,000 cases per chamber max — a backstop, not the stop condition.
            yield from self._walk_chamber(palata, max_pages=2000)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield only the decisions that can plausibly be new since `since`.

        The search form's own `tarigiDan` param filters server-side on the
        decision date, so the refresh asks for a date window instead of walking
        all 88,140 cases (issue #1502). The window opens UPDATE_LOOKBACK_DAYS
        before `since` because `since` is a crawl time while `tarigiDan` matches
        a decision date, and the court publishes weeks after deciding — see the
        constant for the reasoning.
        """
        cutoff = as_date_str(since)
        try:
            window_start = (
                datetime.strptime(cutoff, "%Y-%m-%d")
                - timedelta(days=UPDATE_LOOKBACK_DAYS)
            ).strftime("%Y-%m-%d")
        except ValueError:
            logger.warning(f"Unparseable since={since!r} — falling back to a full walk")
            yield from self.fetch_all()
            return

        window_start = max(window_start, CORPUS_START_DATE)
        logger.info(
            f"Incremental refresh: since={cutoff}, "
            f"requesting decisions dated >= {window_start} "
            f"({UPDATE_LOOKBACK_DAYS}d publication-lag margin)"
        )

        for palata in CHAMBERS.keys():
            logger.info(f"Checking chamber {palata} ({CHAMBERS[palata]}) for updates...")
            yield from self._walk_chamber(palata, since_date=window_start)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        case_id = raw.get("id", "")
        palata = raw.get("palata", 0)
        chamber = raw.get("chamber", CHAMBERS.get(palata, "unknown"))

        # Create unique document ID
        doc_id = f"GE-SC/{case_id}/{palata}"

        case_number = raw.get("case_number", "")
        title = raw.get("title", "") or f"საქმე {case_number}"
        text = raw.get("text", "")
        date_str = raw.get("date", "")
        subject = raw.get("subject", "")
        result = raw.get("result", "")
        case_type = raw.get("case_type", "")

        # Parse date if in Georgian format (YYYY-MM-DD from the HTML)
        # The date appears to already be in YYYY-MM-DD format from the search results

        # Build URL
        url = f"{BASE_URL}/ka/fullcase/{case_id}/{palata}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "GE/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Additional metadata
            "case_number": case_number,
            "chamber": chamber,
            "chamber_id": palata,
            "subject": subject,
            "result": result,
            "case_type": case_type,
            "language": "ka",
            "court": "საქართველოს უზენაესი სასამართლო",
            "court_en": "Supreme Court of Georgia",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Georgian Supreme Court endpoints...")

        # Test homepage
        print("\n1. Testing homepage...")
        try:
            resp = self.client.get("/ka/cases")
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
            if "გადაწყვეტილებები" in resp.text:
                print("   Decisions page found: YES")
            else:
                print("   Decisions page found: NO")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test search for each chamber
        print("\n2. Testing case search...")
        for palata, name in CHAMBERS.items():
            try:
                cases, total = self._get_cases_page(palata, 1)
                print(
                    f"   Chamber {palata} ({name}): {len(cases)} cases on page 1 "
                    f"of {total} total"
                )
                if cases:
                    print(f"      Sample: {cases[0]['case_number']} ({cases[0]['date']})")
            except Exception as e:
                print(f"   Chamber {palata}: ERROR - {e}")

        # Test the incremental date window the refresh lane relies on
        print("\n3. Testing incremental date filter (tarigiDan)...")
        window_start = (
            datetime.now(timezone.utc) - timedelta(days=UPDATE_LOOKBACK_DAYS)
        ).strftime("%Y-%m-%d")
        for palata, name in CHAMBERS.items():
            try:
                cases, total = self._get_cases_page(palata, 1, since_date=window_start)
                oldest = cases[-1]["date"] if cases else "-"
                print(
                    f"   Chamber {palata} ({name}): {total} decisions since "
                    f"{window_start} (page 1 ends at {oldest})"
                )
            except Exception as e:
                print(f"   Chamber {palata}: ERROR - {e}")

        # Test full case fetch
        print("\n4. Testing full case fetch...")
        try:
            cases, _ = self._get_cases_page(0, 1)  # Administrative chamber
            if cases:
                case = cases[0]
                result = self._fetch_full_case(case["id"], case["palata"])
                if result:
                    print(f"   Title: {result['title'][:60]}...")
                    print(f"   Text length: {len(result.get('text', ''))} chars")
                    if result.get('text'):
                        # Show first 150 chars of Georgian text
                        preview = result['text'][:150].replace('\n', ' ')
                        print(f"   Sample text: {preview}...")
                else:
                    print("   ERROR: No result returned")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = GeorgianSupremeCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
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

    elif command == "bootstrap-fast":
        # The fleet wrapper invokes `bootstrap-fast`; without this alias argparse
        # rejected it and the wrapper fell back to re-ingesting sample/ (#1113).
        stats = scraper.bootstrap_fast()
        print(
            f"\nBootstrap-fast complete: {stats.get('records_new', 0)} new, "
            f"{stats.get('records_updated', 0)} updated, "
            f"{stats.get('records_skipped', 0)} skipped"
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
