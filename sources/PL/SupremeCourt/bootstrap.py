#!/usr/bin/env python3
"""
PL/SupremeCourt -- Polish Supreme Court Data Fetcher via SAOS API

Fetches Polish Supreme Court (Sąd Najwyższy) case law from SAOS.

Strategy:
  - Bootstrap: Paginates through search API with courtType=SUPREME
  - Update: Uses judgmentDateFrom parameter to fetch recent decisions
  - Sample: Fetches 12+ records for validation
  - Full text: Available in textContent field from the detail API
  - Checkpoint/Resume: Saves progress to checkpoint.json for resuming across sessions

API Documentation: https://www.saos.org.pl/help/index.php/dokumentacja-api

Available court chambers (Izby):
  - Izba Karna (Criminal Chamber)
  - Izba Cywilna (Civil Chamber)
  - Izba Pracy, Ubezpieczeń Społecznych i Spraw Publicznych (Labor/Social/Public)
  - Izba Wojskowa (Military Chamber)

Usage:
  python bootstrap.py bootstrap              # Full initial pull (38K+ records)
  python bootstrap.py bootstrap --sample     # Fetch sample records for validation
  python bootstrap.py bootstrap --no-checkpoint  # Full pull without checkpoint
  python bootstrap.py update                 # Incremental update (recent decisions)
  python bootstrap.py test-api               # Quick API connectivity test
  python bootstrap.py status                 # Show checkpoint status
  python bootstrap.py clear-checkpoint       # Clear checkpoint file
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.SupremeCourt")

# SAOS API endpoints
API_BASE = "https://www.saos.org.pl/api"
SEARCH_URL = f"{API_BASE}/search/judgments"
DETAIL_URL = f"{API_BASE}/judgments"

# Checkpoint file for resuming across sessions
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for PL/SupremeCourt -- Polish Supreme Court via SAOS API.
    Country: PL
    URL: https://www.saos.org.pl

    Data types: case_law
    Auth: none (public API)
    Coverage: 38,000+ Supreme Court judgments
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- Checkpoint helpers -------------------------------------------------

    def _load_checkpoint(self) -> dict:
        """Load checkpoint from file if it exists."""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid checkpoint file, starting fresh")
        return {
            "page": 0,
            "total_fetched": 0,
            "last_judgment_id": None,
            "fetched_ids": [],
            "last_update": None,
        }

    def _save_checkpoint(self, checkpoint: dict):
        """Save checkpoint to file."""
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.debug(f"Checkpoint saved: page={checkpoint['page']}, fetched={checkpoint['total_fetched']}")

    def _clear_checkpoint(self):
        """Clear checkpoint file."""
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    # -- API helpers --------------------------------------------------------

    def _search_judgments(
        self,
        page_number: int = 0,
        page_size: int = 100,
        judgment_date_from: Optional[str] = None,
        judgment_date_to: Optional[str] = None,
    ) -> dict:
        """
        Search for Supreme Court judgments.

        Returns the full API response with links, items, and metadata.
        """
        params = {
            "courtType": "SUPREME",
            "pageNumber": str(page_number),
            "pageSize": str(page_size),
            "sortingField": "JUDGMENT_DATE",
            "sortingDirection": "DESC",
        }

        if judgment_date_from:
            params["judgmentDateFrom"] = judgment_date_from
        if judgment_date_to:
            params["judgmentDateTo"] = judgment_date_to

        self.rate_limiter.wait()

        try:
            resp = self.client.get("/search/judgments", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Search API error on page {page_number}: {e}")
            # Retry once after a pause
            time.sleep(3)
            try:
                resp = self.client.get("/search/judgments", params=params)
                resp.raise_for_status()
                return resp.json()
            except Exception as e2:
                logger.error(f"Retry failed: {e2}")
                return {"items": [], "info": {"totalResults": 0}}

    def _get_judgment_detail(self, judgment_id: int) -> Optional[dict]:
        """
        Fetch full judgment details including complete textContent.

        The search API returns truncated text, so we need to fetch
        the detail endpoint for full text.
        """
        self.rate_limiter.wait()

        try:
            resp = self.client.get(f"/judgments/{judgment_id}")
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", data)
        except Exception as e:
            logger.warning(f"Failed to fetch detail for judgment {judgment_id}: {e}")
            return None

    def _paginate_search(
        self,
        max_pages: Optional[int] = None,
        judgment_date_from: Optional[str] = None,
        judgment_date_to: Optional[str] = None,
        use_checkpoint: bool = False,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through Supreme Court judgments.

        Args:
            max_pages: Maximum number of pages to fetch (None = all)
            judgment_date_from: Start date filter (YYYY-MM-DD)
            judgment_date_to: End date filter (YYYY-MM-DD)
            use_checkpoint: Whether to use checkpoint file for resuming

        Yields full judgment details (with complete textContent).
        """
        page_size = 100

        # Load checkpoint if enabled
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            page = checkpoint.get("page", 0)
            total_fetched = checkpoint.get("total_fetched", 0)
            fetched_ids = set(checkpoint.get("fetched_ids", []))
            if page > 0:
                logger.info(f"Resuming from checkpoint: page={page}, total_fetched={total_fetched}")
        else:
            page = 0
            total_fetched = 0
            fetched_ids = set()

        total_results = None

        while True:
            if max_pages and page >= max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            data = self._search_judgments(
                page_number=page,
                page_size=page_size,
                judgment_date_from=judgment_date_from,
                judgment_date_to=judgment_date_to,
            )

            # Parse total on first page
            if total_results is None:
                info = data.get("info", {})
                total_results = info.get("totalResults", 0)
                logger.info(f"Total Supreme Court judgments: {total_results}")

                if total_results == 0:
                    return

            items = data.get("items", [])
            if not items:
                logger.info(f"No more items on page {page}")
                break

            for item in items:
                judgment_id = item.get("id")
                if not judgment_id:
                    continue

                # Skip already fetched IDs (for checkpoint resume)
                if judgment_id in fetched_ids:
                    continue

                # Fetch full details to get complete textContent
                detail = self._get_judgment_detail(judgment_id)
                if detail:
                    yield detail
                    total_fetched += 1
                    fetched_ids.add(judgment_id)
                else:
                    # Fall back to search result (may have truncated text)
                    yield item
                    total_fetched += 1
                    fetched_ids.add(judgment_id)

            # Check if we've fetched all
            fetched_position = (page + 1) * page_size
            if fetched_position >= total_results:
                logger.info(f"Fetched all {total_results} records")
                break

            page += 1

            # Save checkpoint after each page
            if use_checkpoint:
                # Only keep last 5000 IDs in checkpoint to limit file size
                recent_ids = list(fetched_ids)[-5000:]
                checkpoint = {
                    "page": page,
                    "total_fetched": total_fetched,
                    "fetched_ids": recent_ids,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                }
                self._save_checkpoint(checkpoint)

            if page % 5 == 0:
                logger.info(f"  Page {page} ({total_fetched}/{total_results} fetched)")

        # Clear checkpoint on successful completion
        if use_checkpoint:
            self._clear_checkpoint()
            logger.info("Bootstrap complete - checkpoint cleared")

    # -- Text cleaning helpers ----------------------------------------------

    def _clean_text(self, text: str) -> str:
        """
        Clean judgment text content.

        SAOS returns relatively clean text, but we normalize whitespace
        and handle any HTML entities.
        """
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove any residual HTML tags (shouldn't be present but just in case)
        text = re.sub(r"<[^>]+>", " ", text)

        # Normalize whitespace (preserve paragraph breaks)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self, use_checkpoint: bool = False) -> Generator[dict, None, None]:
        """
        Yield all Supreme Court judgments from SAOS.

        Args:
            use_checkpoint: Whether to use checkpoint file for resuming.
                Defaults to False so bootstrap_fast() starts clean.
                The CLI passes True explicitly for long-running manual runs.

        WARNING: Full fetch is 38K+ records. Use sample mode for testing.
        """
        logger.info("Starting full fetch of Supreme Court judgments...")

        for judgment in self._paginate_search(use_checkpoint=use_checkpoint):
            yield judgment

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield judgments issued since the given date.

        Uses judgmentDateFrom parameter to filter recent decisions.
        """
        date_from = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching Supreme Court judgments since {date_from}")

        for judgment in self._paginate_search(judgment_date_from=date_from):
            yield judgment

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw SAOS API response into standard schema.

        SAOS provides well-structured JSON, so this is straightforward.
        The textContent field contains the full judgment text.
        """
        # Extract basic identifiers
        judgment_id = raw.get("id", "")
        court_type = raw.get("courtType", "SUPREME")

        # Case numbers (may be multiple)
        court_cases = raw.get("courtCases", [])
        case_numbers = [cc.get("caseNumber", "") for cc in court_cases if cc.get("caseNumber")]
        case_number = case_numbers[0] if case_numbers else ""

        # Judgment metadata
        judgment_date = raw.get("judgmentDate", "")
        judgment_type = raw.get("judgmentType", "")
        personnel_type = raw.get("personnelType", "")

        # Court structure (chamber and division)
        division = raw.get("division", {})
        division_name = division.get("name", "")

        # Chamber info can be in different places
        chamber = division.get("chamber", {})
        chamber_name = chamber.get("name", "")

        # Fallback: check chambers array
        if not chamber_name:
            chambers = raw.get("chambers", [])
            if chambers:
                chamber_name = chambers[0].get("name", "")

        # Judges
        judges_raw = raw.get("judges", [])
        judges = []
        for j in judges_raw:
            judge_info = {
                "name": j.get("name", ""),
                "function": j.get("function", ""),
                "roles": j.get("specialRoles", []),
            }
            judges.append(judge_info)

        # Full text content - THIS IS MANDATORY
        text_content = raw.get("textContent", "")
        text = self._clean_text(text_content)

        # Source information
        source = raw.get("source", {})
        source_url = source.get("judgmentUrl", "")
        publication_date = source.get("publicationDate", "")

        # Legal references
        legal_bases = raw.get("legalBases", [])
        referenced_regs = raw.get("referencedRegulations", [])
        referenced_cases = raw.get("referencedCourtCases", [])
        keywords = raw.get("keywords", [])

        # Decision and summary (may be null)
        decision = raw.get("decision", "") or ""
        summary = raw.get("summary", "") or ""

        # Judgment form (e.g., "wyrok SN", "postanowienie SN")
        judgment_form = raw.get("judgmentForm", {})
        if isinstance(judgment_form, dict):
            judgment_form_name = judgment_form.get("name", "")
        else:
            judgment_form_name = str(judgment_form)

        # Build URL
        url = f"https://www.saos.org.pl/judgments/{judgment_id}" if judgment_id else source_url

        # Build title from case number and date
        title = f"{case_number} - {judgment_date}" if case_number else f"SN {judgment_id}"

        return {
            # Required base fields
            "_id": str(judgment_id),
            "_source": "PL/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": judgment_date,
            "url": url,

            # Case identifiers
            "case_number": case_number,
            "case_numbers": case_numbers,
            "judgment_id": judgment_id,

            # Court structure
            "court_type": court_type,
            "chamber": chamber_name,
            "division": division_name,

            # Judgment metadata
            "judgment_type": judgment_type,
            "judgment_form": judgment_form_name,
            "personnel_type": personnel_type,

            # People
            "judges": judges,

            # Legal references
            "legal_bases": legal_bases,
            "referenced_regulations": referenced_regs,
            "referenced_cases": referenced_cases,
            "keywords": keywords,

            # Content
            "decision": decision,
            "summary": summary,

            # Source
            "source_url": source_url,
            "publication_date": publication_date,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing SAOS API for Polish Supreme Court...")

        # Test search endpoint
        data = self._search_judgments(page_number=0, page_size=10)
        total = data.get("info", {}).get("totalResults", 0)
        items = data.get("items", [])
        print(f"  Total Supreme Court judgments: {total}")
        print(f"  Sample page items: {len(items)}")

        if items:
            # Test detail endpoint
            first_id = items[0].get("id")
            detail = self._get_judgment_detail(first_id)
            if detail:
                text = detail.get("textContent", "")
                print(f"  Detail API works: judgment {first_id}")
                print(f"  Text length: {len(text)} chars")
            else:
                print(f"  Detail API failed for judgment {first_id}")

        # Test chambers endpoint
        try:
            resp = self.client.get("/dump/scChambers")
            chambers_data = resp.json()
            chambers = chambers_data.get("items", [])
            print(f"\n  Supreme Court Chambers ({len(chambers)}):")
            for ch in chambers:
                print(f"    - {ch.get('name')}")
        except Exception as e:
            print(f"  Chambers endpoint error: {e}")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PL/SupremeCourt fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api", "status", "clear-checkpoint"],
        help="Command to run",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of concurrent workers for bootstrap-fast",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for bootstrap-fast",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=12,
        help="Number of sample records to fetch",
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpoint/resume functionality",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint before starting bootstrap",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    scraper = SupremeCourtScraper()

    if args.command == "status":
        # Show checkpoint status
        checkpoint = scraper._load_checkpoint()
        print("Checkpoint status:")
        print(f"  Current page: {checkpoint.get('page', 0)}")
        print(f"  Total fetched: {checkpoint.get('total_fetched', 0)}")
        print(f"  Last update: {checkpoint.get('last_update', 'N/A')}")
        print(f"  Tracked IDs: {len(checkpoint.get('fetched_ids', []))}")
        sys.exit(0)

    elif args.command == "clear-checkpoint":
        scraper._clear_checkpoint()
        sys.exit(0)

    elif args.command == "test-api":
        scraper.test_api()

    elif args.command == "bootstrap":
        # Clear checkpoint if requested
        if args.clear_checkpoint:
            scraper._clear_checkpoint()

        if args.sample:
            stats = scraper.run_sample(n=args.sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            # Full bootstrap with checkpoint support
            # We need to bypass the BaseScraper bootstrap() method
            # and call fetch_all directly with checkpoint support
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)

            use_checkpoint = not args.no_checkpoint
            count = 0
            errors = 0

            try:
                for raw in scraper.fetch_all(use_checkpoint=use_checkpoint):
                    try:
                        record = scraper.normalize(raw)
                        filename = data_dir / f"{record['_id']}.json"
                        with open(filename, "w", encoding="utf-8") as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)
                        count += 1
                        if count % 500 == 0:
                            logger.info(f"Saved {count} documents to data/")
                    except Exception as e:
                        logger.warning(f"Error normalizing record: {e}")
                        errors += 1
            except KeyboardInterrupt:
                logger.info(f"Interrupted. Saved {count} documents. Resume with: python bootstrap.py bootstrap")
                sys.exit(1)

            print(f"\nBootstrap complete: {count} documents saved, {errors} errors")
            print(json.dumps({"records": count, "errors": errors}, indent=2))
            return

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast(
            max_workers=args.workers,
            batch_size=args.batch_size,
        )
        print(json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
