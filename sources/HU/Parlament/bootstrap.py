#!/usr/bin/env python3
"""
HU/Parlament -- Hungarian Parliament Documents (Országgyűlés)

Fetches parliamentary documents (irományok) from the Hungarian Parliament website.
These include draft legislation (törvényjavaslatok), amendments, committee reports,
and other parliamentary documents.

Strategy:
  - Iterates through document numbers for each parliamentary cycle
  - Downloads PDF documents and extracts full text using pdfplumber
  - Parses PDF content for title, date, submitter, and other metadata

Endpoints:
  - PDF: https://www.parlament.hu/irom{cycle}/{docnum}/{docnum}.pdf
  - Web: https://www.parlament.hu/iromanyok-lekerdezese (query interface)

Data:
  - Document types: T (törvényjavaslat/bill), H (határozat/resolution), etc.
  - Cycles: 38 (2006-2010), 39 (2010-2014), 40 (2014-2018), 41 (2018-2022), 42 (2022-)
  - License: Open Government Data

Note: The official Web API requires registration. This scraper uses publicly
accessible PDF files instead.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent docs only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HU.Parlament")

# Base URL for Hungarian Parliament
BASE_URL = "https://www.parlament.hu"

# Parliamentary cycles with approximate document ranges
# Format: cycle_num: (start_doc, end_doc_estimate, start_year, end_year)
# Note: Document numbers are cycle-specific, typically 1 to ~20,000
CYCLES = {
    42: (1, 13700, 2022, None),    # Current cycle, ongoing (~13,613 as of Feb 2026)
    41: (1, 18500, 2018, 2022),    # Completed cycle
    40: (1, 22000, 2014, 2018),
    39: (1, 14500, 2010, 2014),
    38: (1, 17500, 2006, 2010),
}

# Default to current cycle for sample mode
DEFAULT_CYCLE = 42

# A document that is known to exist, used to preflight the site before a long sweep.
PREFLIGHT_DOC = (DEFAULT_CYCLE, 1)

# www.parlament.hu answers *every* path with HTTP 200 and a ~33 KB image-CAPTCHA
# interstitial when it does not like the client, so a status code alone says nothing
# about whether a document exists. These markers identify that page.
CAPTCHA_MARKERS = (b"captcha_resp", b"captcha.gif", b"CAPTCHA")

# Upper bound on probes in _find_max_doc_number. The upward walk only shrinks its
# step on a miss, so a host that never reports a miss would otherwise loop forever.
MAX_PROBES = 80


class SourceBlockedError(RuntimeError):
    """Raised when the upstream host refuses to serve documents to this vantage."""


def _looks_like_captcha(body: bytes) -> bool:
    """True if a response body is the parlament.hu CAPTCHA interstitial."""
    if body.startswith(b"%PDF"):
        return False
    head = body[:4096]
    return any(marker in head for marker in CAPTCHA_MARKERS)


class ParlamentScraper(BaseScraper):
    """
    Scraper for HU/Parlament -- Hungarian Parliamentary Documents.
    Country: HU
    URL: https://www.parlament.hu

    Data types: legislation, parliamentary_documents
    Auth: none (publicly accessible PDFs)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/pdf,*/*",
            },
            timeout=60,
        )

    def _format_doc_number(self, num: int) -> str:
        """Format document number with leading zeros (5 digits)."""
        return f"{num:05d}"

    def _build_pdf_url(self, cycle: int, doc_num: int) -> str:
        """Build the PDF URL for a given cycle and document number."""
        formatted = self._format_doc_number(doc_num)
        return f"/irom{cycle}/{formatted}/{formatted}.pdf"

    def _check_pdf_exists(self, cycle: int, doc_num: int) -> bool:
        """
        Check whether a document PDF really exists.

        A HEAD request is useless here: the host answers 200 for every path,
        including nonexistent ones, so existence has to be decided on the body.
        Fetch just the first bytes and require the PDF magic number.
        """
        url = self._build_pdf_url(cycle, doc_num)
        full_url = f"{BASE_URL}{url}"
        try:
            import requests
            resp = requests.get(full_url, timeout=15, stream=True, headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Range": "bytes=0-4095",
            })
            head = resp.raw.read(4096, decode_content=True) or b""
            resp.close()
        except Exception:
            return False

        if resp.status_code not in (200, 206):
            return False
        if _looks_like_captcha(head):
            raise SourceBlockedError(
                f"www.parlament.hu served its CAPTCHA interstitial instead of "
                f"{full_url} — the site is anti-bot gated from this vantage"
            )
        return head.startswith(b"%PDF")

    def _parse_metadata_from_text(self, text: str) -> Dict[str, Any]:
        """
        Extract metadata from the document text.

        Hungarian parliamentary documents typically have:
        - "Iromány száma: T/XXXXX." - document number/type
        - "Benyújtás dátuma: YYYY-MM-DD HH:MM" - submission date
        - Title after "Törvényjavaslat címe:" or in header
        - Submitter after "Benyújtó:" or "Előterjesztő:"
        """
        metadata = {
            "title": "",
            "date": "",
            "doc_type": "",
            "submitter": "",
            "official_number": "",
        }

        # Extract document type and official number
        # Pattern: "Iromány száma: T/13608."
        iromany_match = re.search(r'Iromány száma:\s*([A-Z])/(\d+)', text)
        if iromany_match:
            metadata["doc_type"] = iromany_match.group(1)
            metadata["official_number"] = f"{iromany_match.group(1)}/{iromany_match.group(2)}"

        # Extract submission date
        # Pattern: "Benyújtás dátuma: 2026-02-11 14:17"
        date_match = re.search(r'Benyújtás dátuma:\s*(\d{4}-\d{2}-\d{2})', text)
        if date_match:
            metadata["date"] = date_match.group(1)

        # Extract title
        # Pattern: "Törvényjavaslat címe: ..." until end of line/paragraph
        title_match = re.search(r'(?:Törvényjavaslat címe|Címe|Tárgy):\s*(.+?)(?:\n|Benyújtó|Előterjesztő)', text, re.DOTALL)
        if title_match:
            title = title_match.group(1).strip()
            # Clean up multi-line titles
            title = re.sub(r'\s+', ' ', title)
            metadata["title"] = title[:500]  # Limit title length
        else:
            # Try to extract from first meaningful line
            lines = text.split('\n')
            for line in lines[1:10]:  # Skip first line (often header), check next few
                line = line.strip()
                if len(line) > 30 and not re.match(r'^(Iromány|Benyújtás|Parlex|Országgyűlési|Címzett)', line):
                    metadata["title"] = line[:500]
                    break

        # Extract submitter
        # Pattern: "Benyújtó: Name (Party), Name2 (Party2)"
        submitter_match = re.search(r'Benyújtó:\s*(.+?)(?:\n\n|Törvényjavaslat címe|$)', text, re.DOTALL)
        if submitter_match:
            submitter = submitter_match.group(1).strip()
            # Clean up
            submitter = re.sub(r'\s+', ' ', submitter)
            metadata["submitter"] = submitter[:500]
        else:
            # Try "Előterjesztő:" pattern
            submitter_match = re.search(r'Előterjesztő:\s*(.+?)(?:\n|$)', text)
            if submitter_match:
                metadata["submitter"] = submitter_match.group(1).strip()[:500]

        return metadata

    def _fetch_document(self, cycle: int, doc_num: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a single document by cycle and number.

        Returns dict with metadata and full_text, or None if failed/not found.
        """
        url = self._build_pdf_url(cycle, doc_num)

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 404:
                return None

            resp.raise_for_status()

            if _looks_like_captcha(resp.content):
                raise SourceBlockedError(
                    f"www.parlament.hu served its CAPTCHA interstitial instead of "
                    f"{BASE_URL}{url} — the site is anti-bot gated from this vantage"
                )

            # The host returns 200 + an HTML error page for missing documents,
            # so trust the PDF magic number rather than the status code.
            if not resp.content.startswith(b"%PDF"):
                return None

            # Extract text from PDF via centralized extractor
            doc_id_pre = f"{cycle}-{doc_num:05d}"
            full_text = extract_pdf_markdown(
                source="HU/Parlament", source_id=doc_id_pre,
                pdf_bytes=resp.content, table="legislation",
            )
            page_count = 0  # Page count no longer tracked

            if not full_text or len(full_text.strip()) < 50:
                logger.warning(f"Insufficient text content for cycle {cycle} doc {doc_num}: {len(full_text)} chars")
                return None

            # Parse metadata from text
            metadata = self._parse_metadata_from_text(full_text)

            # Build document record
            doc = {
                "cycle": cycle,
                "doc_number": doc_num,
                "doc_type": metadata.get("doc_type", ""),
                "official_number": metadata.get("official_number", ""),
                "title": metadata.get("title", f"Document {cycle}/{doc_num}"),
                "date": metadata.get("date", ""),
                "submitter": metadata.get("submitter", ""),
                "full_text": full_text,
                "pdf_pages": page_count,
                "pdf_size": len(resp.content),
                "url": f"{BASE_URL}{url}",
            }

            return doc

        except SourceBlockedError:
            raise
        except Exception as e:
            logger.warning(f"Failed to fetch document cycle {cycle} doc {doc_num}: {e}")
            return None

    def _find_max_doc_number(self, cycle: int, known_max: int) -> int:
        """
        Find the current maximum document number for a cycle using binary search.

        Starts from known_max and searches around it.
        """
        probes = 0

        def exists(num: int) -> bool:
            nonlocal probes
            probes += 1
            return self._check_pdf_exists(cycle, num)

        # First verify known_max works
        if not exists(known_max):
            # Binary search downward to find any existing document
            low, high = 1, known_max
            found = None

            while low <= high and probes < MAX_PROBES:
                mid = (low + high) // 2
                if exists(mid):
                    found = mid
                    low = mid + 1  # Search higher
                else:
                    high = mid - 1  # Search lower

            if found is None:
                return 1  # Nothing found
            known_max = found

        # Now search upward to find the actual max. The step only shrinks on a
        # miss, so bound the walk: a host that answers "exists" for every number
        # (soft-200 error pages, anti-bot interstitials) would otherwise spin
        # here forever without ever yielding a record.
        step = 100
        while step >= 1 and probes < MAX_PROBES:
            test_num = known_max + step
            if exists(test_num):
                known_max = test_num
            else:
                step = step // 2

        if probes >= MAX_PROBES:
            logger.warning(
                f"Cycle {cycle}: gave up locating the max document number after "
                f"{probes} probes; using {known_max}"
            )

        return known_max

    def _preflight(self) -> None:
        """
        Confirm the host will actually serve a PDF before starting a long sweep.

        Raises SourceBlockedError if it will not, so the fleet records a real
        failure in seconds instead of walking the whole ID space against a
        CAPTCHA wall.
        """
        cycle, doc_num = PREFLIGHT_DOC
        if not self._check_pdf_exists(cycle, doc_num):
            raise SourceBlockedError(
                f"Preflight failed: {BASE_URL}{self._build_pdf_url(cycle, doc_num)} "
                f"did not return a PDF — www.parlament.hu is unreachable or gated "
                f"from this vantage"
            )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from all parliamentary cycles.

        Iterates through cycles from newest to oldest, checking each
        document number sequentially.
        """
        self._preflight()

        for cycle in sorted(CYCLES.keys(), reverse=True):
            start_doc, end_estimate, start_year, end_year = CYCLES[cycle]
            logger.info(f"Processing cycle {cycle} ({start_year}-{end_year or 'present'})...")

            # Find actual max for this cycle
            actual_max = self._find_max_doc_number(cycle, end_estimate)
            logger.info(f"Cycle {cycle}: found {actual_max} documents")

            # Fetch documents in reverse order (newest first)
            consecutive_failures = 0
            for doc_num in range(actual_max, 0, -1):
                doc = self._fetch_document(cycle, doc_num)

                if doc:
                    consecutive_failures = 0
                    yield doc
                else:
                    consecutive_failures += 1
                    # Allow some gaps (some document numbers may be reserved/removed)
                    if consecutive_failures > 50:
                        logger.info(f"50 consecutive failures at doc {doc_num}, likely reached start")
                        break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents added since the given date.

        Only checks the current cycle for new documents.
        """
        self._preflight()

        cycle = DEFAULT_CYCLE
        _, end_estimate, _, _ = CYCLES[cycle]

        # Find current max
        actual_max = self._find_max_doc_number(cycle, end_estimate)
        logger.info(f"Checking cycle {cycle} for updates (max doc: {actual_max})")

        # Fetch recent documents until we hit ones older than 'since'
        for doc_num in range(actual_max, max(1, actual_max - 500), -1):
            doc = self._fetch_document(cycle, doc_num)

            if doc:
                # Check date if available
                doc_date_str = doc.get("date", "")
                if doc_date_str:
                    try:
                        doc_date = datetime.strptime(doc_date_str, "%Y-%m-%d")
                        if doc_date.replace(tzinfo=timezone.utc) < since:
                            logger.info(f"Reached documents from {doc_date_str}, stopping update")
                            break
                    except ValueError:
                        pass

                yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        cycle = raw.get("cycle", 0)
        doc_num = raw.get("doc_number", 0)
        doc_id = f"{cycle}-{doc_num:05d}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "HU/Parlament",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("title", f"Document {cycle}/{doc_num}"),
            "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            # Additional metadata
            "cycle": cycle,
            "doc_number": doc_num,
            "doc_type": raw.get("doc_type", ""),
            "official_number": raw.get("official_number", ""),
            "submitter": raw.get("submitter", ""),
            "pdf_pages": raw.get("pdf_pages", 0),
            "pdf_size": raw.get("pdf_size", 0),
            "language": "hu",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Hungarian Parliament PDF access...")

        # Test current cycle
        cycle = DEFAULT_CYCLE
        print(f"\n1. Testing cycle {cycle} (current)...")

        # Check a known recent document
        test_num = 13608
        print(f"   Checking document {test_num}...")

        exists = self._check_pdf_exists(cycle, test_num)
        print(f"   PDF exists: {exists}")

        if exists:
            print(f"\n2. Fetching document {cycle}/{test_num}...")
            doc = self._fetch_document(cycle, test_num)

            if doc:
                print(f"   Title: {doc.get('title', 'N/A')[:60]}...")
                print(f"   Date: {doc.get('date', 'N/A')}")
                print(f"   Doc type: {doc.get('doc_type', 'N/A')}")
                print(f"   Submitter: {doc.get('submitter', 'N/A')[:50]}...")
                print(f"   Pages: {doc.get('pdf_pages', 'N/A')}")
                print(f"   Text length: {len(doc.get('full_text', ''))} characters")
                print(f"\n   Text preview:\n   {doc.get('full_text', '')[:500]}...")
            else:
                print("   ERROR: Failed to fetch document")

        # Test finding max doc number
        print(f"\n3. Finding current max document for cycle {cycle}...")
        actual_max = self._find_max_doc_number(cycle, 13600)
        print(f"   Current max: {actual_max}")

        print("\nTest complete!")


def main():
    scraper = ParlamentScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
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
        try:
            scraper.test_connection()
        except SourceBlockedError as e:
            print(f"\nERROR: {e}", file=sys.stderr)
            sys.exit(1)

    # "bootstrap-fast" is what the fleet wrapper invokes; route it to the full
    # bootstrap so an unrecognised command can't silently fall back to samples.
    elif command in ("bootstrap", "bootstrap-fast"):
        try:
            if sample_mode:
                stats = scraper.run_sample(n=sample_size)
                print(
                    f"\nSample complete: "
                    f"{stats.get('sample_records_saved', 0)} records saved to sample/"
                )
                written = stats.get("sample_records_saved", 0)
            else:
                stats = scraper.bootstrap()
                print(
                    f"\nBootstrap complete: {stats['records_new']} new, "
                    f"{stats['records_updated']} updated, "
                    f"{stats['records_skipped']} skipped"
                )
                written = stats.get("records_new", 0) + stats.get("records_updated", 0)
        except SourceBlockedError as e:
            print(f"\nERROR: {e}", file=sys.stderr)
            sys.exit(1)

        print(json.dumps(stats, indent=2))
        if stats.get("error_message"):
            print(f"\nERROR: {stats['error_message']}", file=sys.stderr)
            sys.exit(1)
        if not written:
            print("\nERROR: no records written", file=sys.stderr)
            sys.exit(1)

    elif command == "update":
        try:
            stats = scraper.update()
        except SourceBlockedError as e:
            print(f"\nERROR: {e}", file=sys.stderr)
            sys.exit(1)
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))
        # Same fail-loud contract as bootstrap: an update that hit the CAPTCHA
        # wall mid-walk must not exit 0 with a handful of records (#1583).
        if stats.get("error_message"):
            print(f"\nERROR: {stats['error_message']}", file=sys.stderr)
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
