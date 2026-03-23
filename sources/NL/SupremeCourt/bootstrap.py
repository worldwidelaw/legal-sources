#!/usr/bin/env python3
"""
NL/SupremeCourt -- Dutch Supreme Court (Hoge Raad) Case Law Fetcher

Fetches case law from the Dutch Supreme Court (Hoge Raad) using the
Open Data API provided by de Rechtspraak (Council for the Judiciary).

The API has 49,000+ Supreme Court decisions available with full text.

Strategy:
  - Bootstrap: Paginates through all Hoge Raad decisions using Atom feed
  - Update: Uses modified date filter to fetch only recently published decisions
  - Sample: Fetches 12+ records for validation with full text

API Documentation:
  - Open Data portal: https://www.rechtspraak.nl/Uitspraken/Paginas/Open-Data.aspx
  - Search endpoint: https://data.rechtspraak.nl/uitspraken/zoeken
  - Content endpoint: https://data.rechtspraak.nl/uitspraken/content?id={ECLI}

Usage:
  python bootstrap.py bootstrap           # Full initial pull (49K+ decisions)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent decisions)
  python bootstrap.py test-api            # Quick API connectivity test
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

import requests
from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.SupremeCourt")

# Checkpoint file for resuming across sessions
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

# API endpoints
SEARCH_ENDPOINT = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_ENDPOINT = "https://data.rechtspraak.nl/uitspraken/content"

# Court identifier for Hoge Raad
HOGE_RAAD_CREATOR = "http://standaarden.overheid.nl/owms/terms/Hoge_Raad_der_Nederlanden"

USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# XML namespaces used in content responses
NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
    "psi": "http://psi.rechtspraak.nl/",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0",
}


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for NL/SupremeCourt -- Dutch Supreme Court Case Law.
    Country: NL
    URL: https://www.rechtspraak.nl

    Data types: case_law
    Auth: none (Open Government Data)

    Data Coverage:
      - 49,000+ Supreme Court decisions
      - All published decisions from 1999 onwards
      - Full text with metadata
      - Daily updates
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def _get_with_retry(self, url: str, params: dict = None, max_retries: int = 4) -> Optional[requests.Response]:
        """
        HTTP GET with retry and exponential backoff for transient errors (403, 429, 5xx).

        Returns Response on success, None after all retries exhausted.
        """
        last_exc = None
        for attempt in range(max_retries + 1):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(url, params=params, timeout=60)
                if resp.status_code in (403, 429, 500, 502, 503, 504):
                    wait = min(2 ** attempt * 3, 120)  # 3s, 6s, 12s, 24s, 48s
                    logger.warning(
                        f"HTTP {resp.status_code} on {url} — retry {attempt+1}/{max_retries} in {wait}s"
                    )
                    if attempt < max_retries:
                        import time
                        time.sleep(wait)
                        continue
                    else:
                        logger.error(f"Exhausted retries on {url} (last status {resp.status_code})")
                        return None
                resp.raise_for_status()
                return resp
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                last_exc = e
                if attempt < max_retries:
                    wait = min(2 ** attempt * 3, 120)
                    logger.warning(f"Connection error on {url} — retry {attempt+1}/{max_retries} in {wait}s: {e}")
                    import time
                    time.sleep(wait)
                else:
                    logger.error(f"Exhausted retries on {url}: {e}")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error on {url}: {e}")
                return None
        return None

    def _search_eclis(
        self,
        max_results: int = 100,
        from_offset: int = 0,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        sort: str = "DESC",
    ) -> list[str]:
        """
        Search for ECLI identifiers from the Hoge Raad.

        Args:
            max_results: Maximum number of results per page
            from_offset: Offset for pagination
            date_from: Start date filter (YYYY-MM-DD)
            date_to: End date filter (YYYY-MM-DD)
            sort: Sort order (DESC = newest first, ASC = oldest first)

        Returns:
            List of ECLI identifiers
        """
        params = {
            "creator": HOGE_RAAD_CREATOR,
            "max": str(max_results),
            "sort": sort,
        }

        if from_offset > 0:
            params["from"] = str(from_offset)

        if date_from and date_to:
            params["date"] = [date_from, date_to]

        resp = self._get_with_retry(SEARCH_ENDPOINT, params=params)
        if resp is None:
            return []

        try:
            # Parse Atom feed
            root = ET.fromstring(resp.content)

            eclis = []
            for entry in root.findall("atom:entry", NAMESPACES):
                ecli_elem = entry.find("atom:id", NAMESPACES)
                if ecli_elem is not None and ecli_elem.text:
                    eclis.append(ecli_elem.text)

            # Get total count from subtitle
            subtitle = root.find("atom:subtitle", NAMESPACES)
            if subtitle is not None and subtitle.text:
                match = re.search(r"(\d+)", subtitle.text)
                if match:
                    total_count = int(match.group(1))
                    logger.info(f"Total decisions available: {total_count}")

            return eclis

        except ET.ParseError as e:
            logger.error(f"XML parse error in search: {e}")
            return []

    def _fetch_document(self, ecli: str) -> Optional[dict]:
        """
        Fetch full document content for an ECLI.

        Args:
            ecli: ECLI identifier

        Returns:
            Dict with document metadata and full text, or None on error
        """
        params = {"id": ecli}

        resp = self._get_with_retry(CONTENT_ENDPOINT, params=params)
        if resp is None:
            return None

        try:
            root = ET.fromstring(resp.content)
            return self._parse_document(root, ecli)
        except ET.ParseError as e:
            logger.error(f"XML parse error for {ecli}: {e}")
            return None

    def _parse_document(self, root: ET.Element, ecli: str) -> dict:
        """
        Parse XML document into normalized format.

        Args:
            root: XML root element
            ecli: ECLI identifier

        Returns:
            Normalized document dict
        """
        doc = {
            "ecli": ecli,
            "title": None,
            "date": None,
            "case_number": None,
            "court": "Hoge Raad",
            "subject_area": None,
            "procedure_type": None,
            "summary": None,
            "text": None,
            "url": f"https://uitspraken.rechtspraak.nl/details?id={ecli}",
            "related_cases": [],
            "citations": [],
        }

        # Find RDF description
        rdf = root.find(".//rdf:Description", NAMESPACES)
        if rdf is not None:
            # ECLI
            identifier = rdf.find("dcterms:identifier", NAMESPACES)
            if identifier is not None and identifier.text:
                doc["ecli"] = identifier.text

            # Decision date
            date_elem = rdf.find("dcterms:date", NAMESPACES)
            if date_elem is not None and date_elem.text:
                doc["date"] = date_elem.text

            # Case number
            zaaknummer = rdf.find("psi:zaaknummer", NAMESPACES)
            if zaaknummer is not None and zaaknummer.text:
                doc["case_number"] = zaaknummer.text

            # Subject area (rechtsgebied)
            subject = rdf.find("dcterms:subject", NAMESPACES)
            if subject is not None:
                label = subject.get(f"{{{NAMESPACES['rdfs']}}}label")
                if label:
                    doc["subject_area"] = label
                elif subject.text:
                    doc["subject_area"] = subject.text

            # Procedure type
            procedure = rdf.find("psi:procedure", NAMESPACES)
            if procedure is not None:
                label = procedure.get(f"{{{NAMESPACES['rdfs']}}}label")
                if label:
                    doc["procedure_type"] = label
                elif procedure.text:
                    doc["procedure_type"] = procedure.text

            # Related cases
            for relation in rdf.findall("dcterms:relation", NAMESPACES):
                related_ecli = relation.get(
                    "ecli:resourceIdentifier",
                    relation.get(f"{{{NAMESPACES.get('ecli', '')}}}resourceIdentifier")
                )
                if not related_ecli:
                    # Try namespace from attribute
                    for attr_name, attr_value in relation.attrib.items():
                        if "resourceIdentifier" in attr_name:
                            related_ecli = attr_value
                            break
                if related_ecli:
                    doc["related_cases"].append(related_ecli)

        # Title from second rdf:Description (HTML version info)
        for desc in root.findall(".//rdf:Description", NAMESPACES):
            title = desc.find("dcterms:title", NAMESPACES)
            if title is not None and title.text:
                doc["title"] = title.text
                break

        # Summary (inhoudsindicatie)
        inhoud = root.find(".//{http://www.rechtspraak.nl/schema/rechtspraak-1.0}inhoudsindicatie", NAMESPACES)
        if inhoud is not None:
            # Extract text from parablock/para elements
            summary_parts = []
            for para in inhoud.iter():
                if para.text:
                    summary_parts.append(para.text.strip())
                if para.tail:
                    summary_parts.append(para.tail.strip())
            doc["summary"] = " ".join(filter(None, summary_parts))

        # Full text (uitspraak)
        uitspraak = root.find(".//{http://www.rechtspraak.nl/schema/rechtspraak-1.0}uitspraak", NAMESPACES)
        if uitspraak is not None:
            # Extract all text content, preserving structure
            text_parts = []
            for elem in uitspraak.iter():
                if elem.text:
                    text_parts.append(elem.text.strip())
                if elem.tail:
                    text_parts.append(elem.tail.strip())
            doc["text"] = "\n".join(filter(None, text_parts))

        # Clean up text
        if doc["text"]:
            # Decode HTML entities
            doc["text"] = html.unescape(doc["text"])
            # Remove excessive whitespace
            doc["text"] = re.sub(r"\n{3,}", "\n\n", doc["text"])
            doc["text"] = re.sub(r"[ \t]+", " ", doc["text"])

        if doc["summary"]:
            doc["summary"] = html.unescape(doc["summary"])
            doc["summary"] = re.sub(r"\s+", " ", doc["summary"]).strip()

        return doc

    def normalize(self, raw: dict) -> dict:
        """
        Normalize raw document to standard schema.

        Args:
            raw: Raw document dict from _fetch_document

        Returns:
            Normalized record dict
        """
        return {
            "_id": raw["ecli"],
            "_source": "NL/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or f"Decision {raw['ecli']}",
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url"),
            "ecli": raw.get("ecli"),
            "case_number": raw.get("case_number"),
            "court": raw.get("court"),
            "subject_area": raw.get("subject_area"),
            "procedure_type": raw.get("procedure_type"),
            "summary": raw.get("summary"),
            "related_cases": raw.get("related_cases", []),
        }

    def _load_checkpoint(self) -> dict:
        """Load checkpoint from file if it exists."""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid checkpoint file, starting fresh")
        return {"current_year": None, "offset": 0, "fetched_eclis": [], "total_fetched": 0}

    def _save_checkpoint(self, checkpoint: dict):
        """Save checkpoint to file."""
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.debug(f"Checkpoint saved: offset={checkpoint['offset']}, fetched={checkpoint['total_fetched']}")

    def _clear_checkpoint(self):
        """Clear checkpoint file."""
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    def fetch_all(self, use_checkpoint: bool = True) -> Generator[dict, None, None]:
        """
        Fetch all Hoge Raad decisions using year-based pagination.

        The API has a hard limit on offset (~42,000). To work around this,
        we iterate through years (1998-present) and paginate within each year.
        Each year has fewer than 42,000 decisions so offset-based pagination works.

        Args:
            use_checkpoint: Whether to use checkpoint file for resuming

        Yields normalized document dicts.
        """
        batch_size = 100
        current_year = datetime.now().year

        # Years to process (API has data from 1998 onwards)
        all_years = list(range(1998, current_year + 1))

        # Load checkpoint if enabled
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            fetched_eclis = set(checkpoint.get("fetched_eclis", []))
            fetched = checkpoint.get("total_fetched", 0)
            start_year = checkpoint.get("current_year")
            start_offset = checkpoint.get("offset", 0)
            if start_year:
                logger.info(f"Resuming from checkpoint: year={start_year}, offset={start_offset}, total_fetched={fetched}")
        else:
            checkpoint = {"current_year": None, "offset": 0, "fetched_eclis": [], "total_fetched": 0}
            fetched_eclis = set()
            fetched = 0
            start_year = None
            start_offset = 0

        # Process each year
        for year in all_years:
            # Skip years before checkpoint
            if start_year and year < start_year:
                continue

            date_from = f"{year}-01-01"
            date_to = f"{year}-12-31"

            # Start offset: use checkpoint offset if this is the checkpoint year, else 0
            offset = start_offset if year == start_year else 0
            start_offset = 0  # Clear after first use

            logger.info(f"Processing year {year} (from offset {offset})...")

            while True:
                eclis = self._search_eclis(
                    max_results=batch_size,
                    from_offset=offset,
                    date_from=date_from,
                    date_to=date_to,
                    sort="ASC",  # Oldest first within year for consistent pagination
                )

                if not eclis:
                    break

                for ecli in eclis:
                    # Skip already fetched ECLIs
                    if ecli in fetched_eclis:
                        continue

                    doc = self._fetch_document(ecli)
                    if doc and doc.get("text"):
                        yield self.normalize(doc)
                        fetched += 1
                        fetched_eclis.add(ecli)

                        if fetched % 100 == 0:
                            logger.info(f"Fetched {fetched} documents with full text (year {year})")

                offset += len(eclis)

                # Save checkpoint after each batch
                if use_checkpoint:
                    # Only keep last 10000 ECLIs in checkpoint to limit file size
                    recent_eclis = list(fetched_eclis)[-10000:]
                    checkpoint = {
                        "current_year": year,
                        "offset": offset,
                        "fetched_eclis": recent_eclis,
                        "total_fetched": fetched,
                        "last_update": datetime.now(timezone.utc).isoformat(),
                    }
                    self._save_checkpoint(checkpoint)

                if len(eclis) < batch_size:
                    break

            logger.info(f"Completed year {year}: total fetched so far = {fetched}")

        logger.info(f"Total documents fetched: {fetched}")

        # Clear checkpoint on successful completion
        if use_checkpoint:
            self._clear_checkpoint()
            logger.info("Bootstrap complete - checkpoint cleared")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Fetch decisions modified since a given date.

        Args:
            since: Datetime to fetch updates from

        Yields normalized document dicts.
        """
        date_from = since.strftime("%Y-%m-%d")
        date_to = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        logger.info(f"Fetching decisions from {date_from} to {date_to}")

        offset = 0
        batch_size = 100

        while True:
            eclis = self._search_eclis(
                max_results=batch_size,
                from_offset=offset,
                date_from=date_from,
                date_to=date_to,
                sort="DESC",
            )

            if not eclis:
                break

            for ecli in eclis:
                doc = self._fetch_document(ecli)
                if doc and doc.get("text"):
                    yield self.normalize(doc)

            offset += len(eclis)

            if len(eclis) < batch_size:
                break

    def fetch_sample(self, count: int = 12) -> list[dict]:
        """
        Fetch sample records for validation.

        Args:
            count: Number of samples to fetch

        Returns:
            List of normalized document dicts
        """
        logger.info(f"Fetching {count} sample records...")

        # Get most recent decisions
        eclis = self._search_eclis(max_results=count, sort="DESC")

        samples = []
        for ecli in eclis:
            doc = self._fetch_document(ecli)
            if doc and doc.get("text"):
                samples.append(self.normalize(doc))
                logger.info(
                    f"Sample {len(samples)}: {ecli} - "
                    f"{len(doc.get('text', ''))} chars"
                )

            if len(samples) >= count:
                break

        return samples

    def test_api(self):
        """Test API connectivity and print sample data."""
        logger.info("Testing API connectivity...")

        # Test search
        eclis = self._search_eclis(max_results=3, sort="DESC")
        if eclis:
            logger.info(f"Search API works. Found ECLIs: {eclis}")
        else:
            logger.error("Search API failed!")
            return False

        # Test content
        if eclis:
            doc = self._fetch_document(eclis[0])
            if doc:
                logger.info(f"Content API works.")
                logger.info(f"  ECLI: {doc.get('ecli')}")
                logger.info(f"  Date: {doc.get('date')}")
                logger.info(f"  Case: {doc.get('case_number')}")
                logger.info(f"  Subject: {doc.get('subject_area')}")
                logger.info(f"  Text length: {len(doc.get('text', ''))} chars")
                if doc.get("text"):
                    logger.info(f"  Text preview: {doc['text'][:200]}...")
                return True
            else:
                logger.error("Content API failed!")
                return False

        return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="NL/SupremeCourt fetcher")
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
        "--since",
        type=str,
        help="Date to fetch updates from (YYYY-MM-DD)",
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

    args = parser.parse_args()

    scraper = SupremeCourtScraper()

    if args.command == "status":
        # Show checkpoint status
        checkpoint = scraper._load_checkpoint()
        logger.info("Checkpoint status:")
        logger.info(f"  Current year: {checkpoint.get('current_year', 'N/A')}")
        logger.info(f"  Offset in year: {checkpoint.get('offset', 0)}")
        logger.info(f"  Total fetched: {checkpoint.get('total_fetched', 0)}")
        logger.info(f"  Last update: {checkpoint.get('last_update', 'N/A')}")
        logger.info(f"  Tracked ECLIs: {len(checkpoint.get('fetched_eclis', []))}")
        sys.exit(0)

    elif args.command == "clear-checkpoint":
        scraper._clear_checkpoint()
        sys.exit(0)

    elif args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        # Clear checkpoint if requested
        if args.clear_checkpoint:
            scraper._clear_checkpoint()

        if args.sample:
            # Fetch samples and save to sample directory
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            samples = scraper.fetch_sample(count=12)

            for i, doc in enumerate(samples, 1):
                filename = sample_dir / f"sample_{i:02d}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                logger.info(f"Saved {filename}")

            # Calculate stats
            if samples:
                total_chars = sum(len(s.get("text", "")) for s in samples)
                avg_chars = total_chars // len(samples)
                logger.info(f"Saved {len(samples)} samples")
                logger.info(f"Average text length: {avg_chars:,} chars")
                logger.info(f"Total text: {total_chars:,} chars")
        else:
            # Full bootstrap
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)

            count = 0
            use_checkpoint = not args.no_checkpoint
            for doc in scraper.fetch_all(use_checkpoint=use_checkpoint):
                filename = data_dir / f"{doc['_id'].replace(':', '_')}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 1000 == 0:
                    logger.info(f"Saved {count} documents")

            logger.info(f"Bootstrap complete. Total: {count} documents")

    elif args.command == "update":
        if args.since:
            since = datetime.fromisoformat(args.since)
        else:
            since = datetime.now(timezone.utc) - timedelta(days=7)

        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)

        count = 0
        for doc in scraper.fetch_updates(since):
            filename = data_dir / f"{doc['_id'].replace(':', '_')}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Update complete. Fetched {count} new documents")


if __name__ == "__main__":
    main()
