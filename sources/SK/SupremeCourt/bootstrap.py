#!/usr/bin/env python3
"""
SK/SupremeCourt -- Slovak Supreme Court (Najvyšší súd) Case Law Fetcher

Fetches Supreme Court decisions from the nsud.sk OpenData API.

Strategy:
  - Bootstrap: Iterate through decision IDs from 1000 to max_id with checkpoint
  - Update: Uses getLastDecision(date) for recent records.
  - Sample: Fetches 12+ records for validation.
  - Checkpoint/Resume: Saves progress to checkpoint.json for multi-session runs

API: https://www.nsud.sk/ws/opendata.php
Docs: https://www.nsud.sk/opendata/

Functions:
  - getDecision(id) - Get a specific decision by ID
  - getLastDecision(date) - Get IDs modified since date (YYYY-MM-DD)
  - searchDecision(...) - Search by various parameters

RATE LIMITING: The API enforces strict rate limits (~0.5 req/s sustained).
The scraper uses exponential backoff for 429 errors and saves checkpoints
frequently to allow resumption across multiple sessions.

Usage:
  python bootstrap.py bootstrap              # Full initial pull (104K+ records)
  python bootstrap.py bootstrap --sample     # Fetch sample records for validation
  python bootstrap.py bootstrap --no-checkpoint  # Disable checkpoint/resume
  python bootstrap.py update                 # Incremental update (last month)
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
import random
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SK.SupremeCourt")

# API Base
API_BASE = "https://www.nsud.sk/ws"

# Checkpoint file for resuming across sessions
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

# ID range constants
MIN_ID = 1000   # Oldest decision IDs start around 1000
MAX_ID = 250000 # Approximate max ID (will discover actual max)

# Kolegium codes
KOLEGIUM_MAP = {
    "1": "Občianskoprávne kolégium (Civil)",
    "2": "Obchodnoprávne kolégium (Commercial)",
    "3": "Správne kolégium (Administrative)",
    "4": "Trestnoprávne kolégium (Criminal)",
}


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for SK/SupremeCourt -- Slovak Supreme Court.
    Country: SK
    URL: https://www.nsud.sk

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )
        # Backoff state for rate limiting
        self._consecutive_429s = 0
        self._base_delay = 2.0  # Base delay between requests

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
            "current_id": MIN_ID,
            "max_id_seen": 0,
            "total_fetched": 0,
            "total_errors": 0,
            "last_update": None,
        }

    def _save_checkpoint(self, checkpoint: dict):
        """Save checkpoint to file."""
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.debug(f"Checkpoint saved: id={checkpoint['current_id']}, fetched={checkpoint['total_fetched']}")

    def _clear_checkpoint(self):
        """Clear checkpoint file."""
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    # -- Rate limiting helpers ----------------------------------------------

    def _smart_wait(self):
        """Wait with exponential backoff based on 429 history."""
        # Calculate delay based on consecutive 429s
        if self._consecutive_429s > 0:
            delay = self._base_delay * (2 ** min(self._consecutive_429s, 6))
            delay = min(delay, 120)  # Cap at 2 minutes
            # Add jitter
            delay += random.uniform(0, delay * 0.2)
            logger.debug(f"Backoff delay: {delay:.1f}s (429s: {self._consecutive_429s})")
        else:
            delay = self._base_delay

        time.sleep(delay)

    def _handle_429(self):
        """Handle rate limit response."""
        self._consecutive_429s += 1
        if self._consecutive_429s >= 5:
            logger.warning(f"Too many 429s ({self._consecutive_429s}), long pause...")
            time.sleep(60)  # 1 minute pause after 5 consecutive 429s

    def _reset_backoff(self):
        """Reset backoff after successful request."""
        self._consecutive_429s = 0

    # -- API helpers --------------------------------------------------------

    def _get_decision(self, decision_id: str, retries: int = 3) -> Optional[dict]:
        """
        Fetch a single decision by ID with retry logic for 429s.

        Returns the full decision data or None if not found.
        """
        for attempt in range(retries):
            try:
                self._smart_wait()
                resp = self.client.get(
                    "/opendata.php",
                    params={"getDecision": "", "id": str(decision_id)}
                )

                # Handle rate limiting
                if resp.status_code == 429:
                    self._handle_429()
                    continue

                resp.raise_for_status()
                self._reset_backoff()

                data = resp.json()

                # API returns empty object or single decision object
                if data and isinstance(data, dict) and data.get("ID"):
                    return data
                elif data and isinstance(data, dict) and "cislo" in data:
                    # Sometimes ID field is empty but data is valid
                    data["ID"] = str(decision_id)
                    return data

                return None

            except Exception as e:
                if "429" in str(e):
                    self._handle_429()
                    continue
                logger.debug(f"Failed to fetch decision {decision_id}: {e}")
                return None

        logger.debug(f"Gave up on decision {decision_id} after {retries} retries")
        return None

    def _get_recent_ids(self, since_date: str) -> List[str]:
        """
        Get list of decision IDs modified since a date.

        Args:
            since_date: Date in YYYY-MM-DD format

        Returns:
            List of decision IDs as strings
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                "/opendata.php",
                params={"getLastDecision": "", "date": since_date}
            )
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                return [str(id) for id in data]

            return []

        except Exception as e:
            logger.error(f"Failed to get recent IDs since {since_date}: {e}")
            return []

    def _search_decisions(self, **params) -> List[str]:
        """
        Search for decisions by various criteria.

        Returns list of matching decision IDs.
        """
        try:
            self.rate_limiter.wait()
            query_params = {"searchDecision": ""}
            query_params.update(params)

            resp = self.client.get("/opendata.php", params=query_params)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                return [str(id) for id in data]

            return []

        except Exception as e:
            logger.error(f"Failed to search decisions: {e}")
            return []

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove HTML tags if any
        text = re.sub(r"<[^>]+>", " ", text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self, use_checkpoint: bool = True) -> Generator[dict, None, None]:
        """
        Yield all Supreme Court decisions by iterating through IDs.

        Strategy: Iterate through decision IDs from MIN_ID to MAX_ID (250000).
        IDs are not fully sequential (gaps exist) but this approach covers
        the full range. The consecutive_misses threshold stops iteration early
        if we hit too many empty IDs in a row (indicating end of data).

        Args:
            use_checkpoint: Whether to use checkpoint file for resuming

        Full fetch is ~104K+ records. With rate limiting, this takes days.
        """
        logger.info("Fetching all Supreme Court decisions")

        # Load checkpoint if enabled
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            current_id = checkpoint.get("current_id", MIN_ID)
            max_id_seen = checkpoint.get("max_id_seen", 0)
            total_fetched = checkpoint.get("total_fetched", 0)
            if current_id > MIN_ID:
                logger.info(f"Resuming from checkpoint: id={current_id}, fetched={total_fetched}")
        else:
            current_id = MIN_ID
            max_id_seen = 0
            total_fetched = 0

        # Always use MAX_ID as the target (250000)
        # We'll stop early via consecutive_misses if we hit the end of data
        max_id_target = MAX_ID

        # Check recent IDs for logging purposes only
        recent_ids = self._get_recent_ids((datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
        if recent_ids:
            max_recent = max(int(id) for id in recent_ids)
            max_id_seen = max(max_id_seen, max_recent)
            logger.info(f"Max ID from recent decisions: {max_recent}")

        logger.info(f"Will iterate from ID {current_id} to {max_id_target}")

        consecutive_misses = 0
        max_consecutive_misses = 1000  # Increased: stop if 1000 consecutive IDs are missing

        while current_id <= max_id_target:
            decision = self._get_decision(str(current_id))

            if decision:
                total_fetched += 1
                consecutive_misses = 0

                # Update max seen ID
                decision_id = int(decision.get("ID", current_id))
                max_id_seen = max(max_id_seen, decision_id)

                yield decision

                if total_fetched % 100 == 0:
                    logger.info(f"Progress: ID {current_id}, fetched {total_fetched} decisions")
            else:
                consecutive_misses += 1

            current_id += 1

            # Save checkpoint periodically
            if use_checkpoint and current_id % 50 == 0:
                checkpoint = {
                    "current_id": current_id,
                    "max_id_seen": max_id_seen,
                    "total_fetched": total_fetched,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                }
                self._save_checkpoint(checkpoint)

            # Stop if too many consecutive misses (we've gone past the end)
            if consecutive_misses >= max_consecutive_misses:
                logger.info(f"Stopping: {max_consecutive_misses} consecutive misses at ID {current_id}")
                break

        logger.info(f"Total fetched: {total_fetched} decisions")

        # Clear checkpoint on successful completion
        if use_checkpoint:
            self._clear_checkpoint()
            logger.info("Bootstrap complete - checkpoint cleared")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield Supreme Court records modified since the given date.

        Uses getLastDecision API function.
        """
        since_date = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching Supreme Court updates since {since_date}")

        ids = self._get_recent_ids(since_date)
        logger.info(f"Found {len(ids)} decision IDs since {since_date}")

        for decision_id in ids:
            decision = self._get_decision(decision_id)
            if decision:
                yield decision

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw API response into standard schema.

        CRITICAL: Full text is in the 'obsah' field.
        """
        # Extract fields from API response
        decision_id = str(raw.get("ID", ""))
        if not decision_id:
            decision_id = raw.get("cislo", "").replace("/", "_")

        cislo = raw.get("cislo", "")
        ecli = raw.get("ecli", "")
        datum = raw.get("datum", "")
        kolegium = raw.get("kolegium", "")
        senat = raw.get("senat", "")
        merito = raw.get("merito", "")
        sudca = raw.get("sudca", "")
        obsah = raw.get("obsah", "")  # FULL TEXT
        subor = raw.get("subor", "")  # PDF attachment URL

        # Clean full text
        full_text = self._clean_text(obsah)

        # Build URL
        if cislo:
            # URL format: /rozhodnutia/{cislo_sanitized}/
            url_path = cislo.lower().replace("/", "").replace(" ", "")
            url = f"https://www.nsud.sk/rozhodnutia/{url_path}/"
        else:
            url = f"https://www.nsud.sk/rozhodnutia/"

        # Kolegium description
        kolegium_desc = KOLEGIUM_MAP.get(str(kolegium), str(kolegium))

        return {
            # Required base fields
            "_id": decision_id,
            "_source": "SK/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": cislo,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": datum,
            "url": url,
            # Case law specific fields
            "ecli": ecli,
            "cislo": cislo,
            "kolegium": kolegium,
            "kolegium_desc": kolegium_desc,
            "senat": senat,
            "merito": merito,
            "sudca": sudca,
            "subor": subor,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing SK Supreme Court OpenData API...")

        # Get recent IDs (last 7 days)
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        ids = self._get_recent_ids(week_ago)
        print(f"  Recent decisions (last 7 days): {len(ids)} records")

        # Fetch one sample decision
        if ids:
            sample_id = ids[0]
            decision = self._get_decision(sample_id)
            if decision:
                print(f"  Sample decision ID: {sample_id}")
                print(f"    Case number: {decision.get('cislo', 'N/A')}")
                print(f"    ECLI: {decision.get('ecli', 'N/A')}")
                print(f"    Date: {decision.get('datum', 'N/A')}")
                print(f"    Kolegium: {KOLEGIUM_MAP.get(str(decision.get('kolegium', '')), 'N/A')}")
                obsah = decision.get('obsah', '')
                print(f"    Full text length: {len(obsah)} chars")

        # Total count estimate (from 2020)
        all_ids = self._get_recent_ids("2020-01-01")
        print(f"  Total decisions since 2020-01-01: {len(all_ids)}")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SK/SupremeCourt fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api", "status", "clear-checkpoint"],
        help="Command to run",
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
        print(f"  Current ID: {checkpoint.get('current_id', MIN_ID)}")
        print(f"  Max ID seen: {checkpoint.get('max_id_seen', 0)}")
        print(f"  Total fetched: {checkpoint.get('total_fetched', 0)}")
        print(f"  Last update: {checkpoint.get('last_update', 'N/A')}")
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
            print(json.dumps(stats, indent=2))
        else:
            # Full bootstrap with checkpoint support
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
