#!/usr/bin/env python3
"""
NL/TweedeKamer -- Dutch House of Representatives Parliamentary Proceedings

Fetches parliamentary transcripts from the Tweede Kamer der Staten-Generaal
using the OData v4 API at gegevensmagazijn.tweedekamer.nl.

Strategy:
  - Bootstrap: Paginates through Verslag (transcript) records
  - Update: Uses ApiGewijzigdOp filter for recently modified records
  - Sample: Fetches 10+ records for validation with full text

API Documentation:
  - Portal: https://opendata.tweedekamer.nl
  - API base: https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0
  - Verslag entity: Debate transcripts in XML format
  - Full content: /Verslag({id})/resource

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent modifications)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.TweedeKamer")

# OData API endpoint
ODATA_BASE = "https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0"


class TweedeKamerScraper(BaseScraper):
    """
    Scraper for NL/TweedeKamer -- Dutch House of Representatives.
    Country: NL
    URL: https://opendata.tweedekamer.nl

    Data types: parliamentary_proceedings
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=ODATA_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- OData API helpers --------------------------------------------------------

    def _odata_query(
        self,
        entity: str,
        filter_expr: str = None,
        top: int = 100,
        skip: int = 0,
        orderby: str = None,
        expand: str = None,
    ) -> Optional[dict]:
        """
        Execute an OData query and return JSON response.

        Args:
            entity: OData entity name (e.g., "Verslag")
            filter_expr: OData $filter expression
            top: Max records to return (default 100, max 250)
            skip: Number of records to skip
            orderby: Sort order (e.g., "Datum desc")
            expand: Related entities to include

        Returns:
            JSON response dict or None on error
        """
        params = {
            "$format": "json",
            "$top": str(min(top, 250)),
            "$skip": str(skip),
        }
        if filter_expr:
            params["$filter"] = filter_expr
        if orderby:
            params["$orderby"] = orderby
        if expand:
            params["$expand"] = expand

        self.rate_limiter.wait()

        try:
            resp = self.client.get(f"/{entity}", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"OData query error for {entity}: {e}")
            return None

    def _fetch_verslag_resource(self, verslag_id: str) -> str:
        """
        Fetch full XML content for a Verslag (transcript).

        Args:
            verslag_id: GUID of the Verslag record

        Returns:
            Extracted plain text from XML, or empty string on error
        """
        url = f"/Verslag({verslag_id})/resource"

        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            resp.raise_for_status()

            # Parse XML and extract text
            root = ET.fromstring(resp.content)
            text_parts = []

            # Extract all text from relevant XML elements
            # Structure includes: <tekst>, <alinea>, <alineaitem>, <nadruk>
            # Also speaker metadata and timestamps

            for elem in root.iter():
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

                # Extract text from alineaitem elements (main speech content)
                if tag == "alineaitem":
                    text = "".join(elem.itertext()).strip()
                    if text:
                        text_parts.append(text)

                # Also get titles and subjects
                elif tag in ["titel", "onderwerp"]:
                    text = "".join(elem.itertext()).strip()
                    if text:
                        text_parts.append(f"[{tag.upper()}] {text}")

            full_text = "\n\n".join(text_parts)

            # Clean up the text
            full_text = html.unescape(full_text)
            # Normalize whitespace while preserving paragraph breaks
            full_text = re.sub(r"[ \t]+", " ", full_text)
            full_text = re.sub(r"\n{3,}", "\n\n", full_text)

            return full_text.strip()

        except Exception as e:
            logger.warning(f"Failed to fetch Verslag resource {verslag_id}: {e}")
            return ""

    def _paginate_verslagen(
        self,
        filter_expr: str = None,
        max_records: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through Verslag records.

        Args:
            filter_expr: Optional OData filter expression
            max_records: Maximum total records to fetch (None for all)

        Yields:
            Individual Verslag metadata dicts
        """
        # Always filter out deleted records
        base_filter = "Verwijderd eq false"
        if filter_expr:
            combined_filter = f"({base_filter}) and ({filter_expr})"
        else:
            combined_filter = base_filter

        skip = 0
        batch_size = 100
        total_fetched = 0

        while True:
            if max_records and total_fetched >= max_records:
                return

            result = self._odata_query(
                entity="Verslag",
                filter_expr=combined_filter,
                top=batch_size,
                skip=skip,
                orderby="GewijzigdOp desc",
            )

            if result is None:
                logger.error("Failed to fetch Verslag batch")
                return

            records = result.get("value", [])
            if not records:
                logger.info(f"No more Verslag records after {total_fetched}")
                return

            for record in records:
                if max_records and total_fetched >= max_records:
                    return
                yield record
                total_fetched += 1

            skip += len(records)
            logger.info(f"  Fetched {total_fetched} Verslag records...")

            # OData pagination check - if we got fewer records than requested, we're done
            if len(records) < batch_size:
                return

    def _fetch_vergadering(self, vergadering_id: str) -> Optional[dict]:
        """Fetch Vergadering (meeting) metadata for a Verslag."""
        result = self._odata_query(
            entity=f"Vergadering({vergadering_id})",
        )
        if result and not isinstance(result.get("value"), list):
            # Single entity response
            return result
        return None

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Verslag (transcript) documents.
        """
        logger.info("Starting full TweedeKamer Verslag fetch...")
        yield from self._paginate_verslagen()

    def run_sample(self, n: int = 12) -> dict:
        """
        Custom sample mode that fetches recent transcripts with full text.
        """
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        sample_records = []
        logger.info(f"Fetching {n} recent TweedeKamer transcripts for sampling...")

        try:
            # Fetch recent Verslagen (transcripts)
            for raw in self._paginate_verslagen(max_records=n * 2):
                try:
                    record = self.normalize(raw)
                except Exception as e:
                    logger.warning(f"Normalization error: {e}")
                    stats["errors"] += 1
                    continue

                stats["records_fetched"] += 1

                # Only include records with actual text content
                if record.get("text"):
                    sample_records.append(record)
                    text_len = len(record.get("text", ""))
                    logger.info(f"  Collected: {record.get('_id')[:8]}... ({text_len} chars)")
                    if len(sample_records) >= n:
                        break
                else:
                    logger.warning(f"  Skipped {record.get('_id')[:8]}...: no full text")

        except Exception as e:
            logger.error(f"Sample error: {e}")
            stats["error_message"] = str(e)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._save_samples(sample_records)
        stats["sample_records_saved"] = len(sample_records)

        return stats

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield Verslag records modified since the given date.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        filter_expr = f"ApiGewijzigdOp ge {since_str}"
        logger.info(f"Fetching TweedeKamer updates since {since_str}...")
        yield from self._paginate_verslagen(filter_expr=filter_expr)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw OData Verslag record into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from /resource endpoint.
        """
        verslag_id = raw.get("Id", "")

        # Fetch full text from /resource endpoint
        full_text = ""
        if verslag_id:
            full_text = self._fetch_verslag_resource(verslag_id)

        # Get Vergadering metadata if available
        vergadering_id = raw.get("Vergadering_Id", "")
        vergadering_titel = ""
        vergaderjaar = ""
        vergaderingnummer = ""
        datum = ""

        if vergadering_id:
            verg = self._fetch_vergadering(vergadering_id)
            if verg:
                vergadering_titel = verg.get("Titel", "")
                vergaderjaar = verg.get("Vergaderjaar", "")
                vergaderingnummer = str(verg.get("Vergaderingnummer", ""))
                datum = verg.get("Datum", "")

        # Parse date from various sources
        if not datum:
            # Try to get date from GewijzigdOp
            gewijzigd = raw.get("GewijzigdOp", "")
            if gewijzigd:
                datum = gewijzigd.split("T")[0]

        # Build title
        soort = raw.get("Soort", "")
        title = vergadering_titel or f"TweedeKamer {soort} {verslag_id[:8]}"

        # Build URL
        url = f"https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0/Verslag({verslag_id})/resource"

        return {
            # Required base fields
            "_id": verslag_id,
            "_source": "NL/TweedeKamer",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": datum,
            "url": url,
            # Source-specific fields
            "soort": soort,
            "status": raw.get("Status", ""),
            "vergadering_id": vergadering_id,
            "vergadering_titel": vergadering_titel,
            "vergaderjaar": vergaderjaar,
            "vergaderingnummer": vergaderingnummer,
            "kamer": 2,  # Tweede Kamer = Second Chamber
            "content_type": raw.get("ContentType", ""),
            "content_length": raw.get("ContentLength", 0),
            "gewijzigd_op": raw.get("GewijzigdOp", ""),
            "api_gewijzigd_op": raw.get("ApiGewijzigdOp", ""),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing NL TweedeKamer OData API...")

        # Test basic entity query
        result = self._odata_query(
            entity="Verslag",
            filter_expr="Verwijderd eq false",
            top=3,
        )

        if result is None:
            print("  ERROR: Failed to connect to OData API")
            return

        records = result.get("value", [])
        print(f"  Found {len(records)} Verslag records in test query")

        if records:
            for i, record in enumerate(records[:3]):
                verslag_id = record.get("Id", "")
                soort = record.get("Soort", "")
                content_length = record.get("ContentLength", 0)
                print(f"\n  Record {i+1}:")
                print(f"    ID: {verslag_id}")
                print(f"    Soort: {soort}")
                print(f"    ContentLength: {content_length}")

                # Test fetching full text for first record
                if i == 0 and verslag_id:
                    print("    Testing full text fetch...")
                    text = self._fetch_verslag_resource(verslag_id)
                    if text:
                        print(f"    Full text length: {len(text)} characters")
                        print(f"    Text preview: {text[:200]}...")
                    else:
                        print("    WARNING: Could not fetch full text")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = TweedeKamerScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12  # Default to 12 for validation
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

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
