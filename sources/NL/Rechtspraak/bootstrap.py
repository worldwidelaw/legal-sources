#!/usr/bin/env python3
"""
NL/Rechtspraak -- Dutch Court Decisions Data Fetcher

Fetches court decisions from de Rechtspraak (Dutch Judiciary) Open Data API.

Strategy:
  - Bootstrap: Paginate through ECLI index using date ranges, fetch full XML for each
  - Update: Uses modified parameter to fetch recently modified decisions
  - Sample: Fetches 10+ recent decisions for validation

API: https://data.rechtspraak.nl/
Docs: https://www.rechtspraak.nl/Uitspraken/Paginas/Open-Data.aspx

Two-step query process:
1. Search ECLI-index: GET /uitspraken/zoeken?{params} -> Atom feed of ECLIs
2. Fetch document: GET /uitspraken/content?id={ECLI} -> Full XML document

Usage:
  python bootstrap.py bootstrap          # Full initial pull (700K+ records)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (last month)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Dict
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.rechtspraak")

# API endpoints
API_BASE = "https://data.rechtspraak.nl"
SEARCH_URL = f"{API_BASE}/uitspraken/zoeken"
CONTENT_URL = f"{API_BASE}/uitspraken/content"

# Namespaces in Rechtspraak XML
NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
    "psi": "http://psi.rechtspraak.nl/",
    "rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
}


class RechtspraakScraper:
    """
    Scraper for NL/Rechtspraak -- Dutch Court Decisions.
    Country: NL
    URL: https://www.rechtspraak.nl

    Data types: case_law
    Auth: none (Open Data)
    """

    def __init__(self, request_delay: float = 1.5):
        """
        Initialize the scraper.

        Args:
            request_delay: Seconds between requests (default 1.5s for VPS safety)
        """
        self.session = requests.Session()

        # Configure retry strategy with longer backoff for VPS environments
        retry_strategy = Retry(
            total=3,  # Fewer retries to avoid hammering the API
            backoff_factor=5,  # Longer backoff: 5s, 10s, 20s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update({
            "User-Agent": "WorldWideLaw/1.0 (Open Data Research; contact@example.com)",
            "Accept": "application/xml, application/atom+xml, text/xml, */*",
        })

        # Rate limiting: More conservative for VPS environments
        # The API is sensitive to request rates from cloud IPs
        self.request_delay = request_delay
        self.last_request_time = 0

        # Track consecutive errors for adaptive backoff
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5

    def _rate_limit(self):
        """Enforce rate limiting between requests with adaptive backoff."""
        # Add extra delay if we've had recent errors (adaptive backoff)
        extra_delay = min(self.consecutive_errors * 2, 30)  # Max 30s extra
        total_delay = self.request_delay + extra_delay

        elapsed = time.time() - self.last_request_time
        if elapsed < total_delay:
            time.sleep(total_delay - elapsed)
        self.last_request_time = time.time()

    def _search_eclis(
        self,
        max_results: int = 1000,
        offset: int = 0,
        sort: str = "DESC",
        modified_since: str = None,
        date_from: str = None,
        date_to: str = None,
    ) -> List[Dict]:
        """
        Search the ECLI index and return a list of ECLI metadata.

        Returns list of dicts with: ecli, title, date, link, summary
        """
        params = {
            "max": str(max_results),
            "from": str(offset),
            "sort": sort,
            "type": "Uitspraak",  # Judgments only (not Conclusie/opinions)
        }

        if modified_since:
            params["modified"] = modified_since
        if date_from:
            params["date"] = date_from if not date_to else f"{date_from}&date={date_to}"

        self._rate_limit()

        try:
            resp = self.session.get(SEARCH_URL, params=params, timeout=60)
            resp.raise_for_status()
            self.consecutive_errors = 0  # Reset on success
        except requests.exceptions.HTTPError as e:
            self.consecutive_errors += 1
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                logger.warning(f"Search failed with HTTP {resp.status_code}, "
                              f"consecutive errors: {self.consecutive_errors}")
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({self.consecutive_errors}), stopping")
                    raise
                # Wait longer before retrying
                time.sleep(self.consecutive_errors * 10)
            raise
        except Exception as e:
            self.consecutive_errors += 1
            logger.error(f"Search failed: {e}")
            raise

        # Parse Atom feed
        results = []
        try:
            root = ET.fromstring(resp.content)

            # Find all entry elements
            for entry in root.findall("atom:entry", NAMESPACES):
                ecli_elem = entry.find("atom:id", NAMESPACES)
                title_elem = entry.find("atom:title", NAMESPACES)
                updated_elem = entry.find("atom:updated", NAMESPACES)
                summary_elem = entry.find("atom:summary", NAMESPACES)
                link_elem = entry.find("atom:link[@type='text/html']", NAMESPACES)

                if ecli_elem is not None and ecli_elem.text:
                    results.append({
                        "ecli": ecli_elem.text,
                        "title": title_elem.text if title_elem is not None else "",
                        "updated": updated_elem.text if updated_elem is not None else "",
                        "summary": summary_elem.text if summary_elem is not None else "",
                        "link": link_elem.get("href") if link_elem is not None else "",
                    })
        except ET.ParseError as e:
            logger.error(f"Failed to parse search results: {e}")
            raise

        logger.info(f"Search returned {len(results)} ECLIs (offset={offset})")
        return results

    def _fetch_document(self, ecli: str) -> Optional[str]:
        """
        Fetch the full XML document for an ECLI.

        Returns the raw XML content as string, or None if failed.
        """
        self._rate_limit()

        try:
            resp = self.session.get(
                CONTENT_URL,
                params={"id": ecli},
                timeout=60
            )
            resp.raise_for_status()
            self.consecutive_errors = 0  # Reset on success
            return resp.text
        except requests.exceptions.HTTPError as e:
            self.consecutive_errors += 1
            status = getattr(resp, 'status_code', 'unknown')
            logger.warning(f"Failed to fetch document {ecli}: HTTP {status}")
            if status in (403, 429):
                # Rate limited - wait longer
                time.sleep(self.consecutive_errors * 10)
            return None
        except Exception as e:
            self.consecutive_errors += 1
            logger.warning(f"Failed to fetch document {ecli}: {e}")
            return None

    def _parse_document(self, xml_content: str) -> Dict:
        """
        Parse a Rechtspraak XML document and extract structured data.

        Returns a dict with all relevant fields including full text.
        """
        try:
            root = ET.fromstring(xml_content.encode("utf-8"))
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return {}

        result = {}

        # RDF metadata (Dublin Core + Rechtspraak-specific)
        rdf = root.find(".//rdf:RDF", NAMESPACES)
        if rdf is not None:
            desc = rdf.find("rdf:Description", NAMESPACES)
            if desc is not None:
                # Core identifiers
                result["ecli"] = self._get_text(desc, "dcterms:identifier")
                result["title"] = self._get_text(desc, "dcterms:title")

                # Dates
                result["date"] = self._get_text(desc, "dcterms:date")
                result["issued"] = self._get_text(desc, "dcterms:issued")
                result["modified"] = self._get_text(desc, "dcterms:modified")

                # Court/creator
                result["creator"] = self._get_text(desc, "dcterms:creator")
                result["publisher"] = self._get_text(desc, "dcterms:publisher")

                # Subject/type
                result["subject"] = self._get_all_texts(desc, "dcterms:subject")
                result["type"] = self._get_text(desc, "dcterms:type")

                # Procedure
                result["procedure"] = self._get_all_texts(desc, "psi:procedure")

                # Relations/references
                result["references"] = self._get_all_texts(desc, "dcterms:references")
                result["relation"] = self._get_all_texts(desc, "dcterms:relation")

        # Extract full text from inhoudsindicatie and uitspraak elements
        # Note: These elements declare their own default namespace on the element itself:
        # <uitspraak xmlns="http://www.rechtspraak.nl/schema/rechtspraak-1.0" ...>
        # So we must use the full namespace URI in braces for find()
        text_parts = []
        rs_ns = "{http://www.rechtspraak.nl/schema/rechtspraak-1.0}"

        # Inhoudsindicatie (summary/headnote)
        inhoud = root.find(f".//{rs_ns}inhoudsindicatie")
        if inhoud is None:
            inhoud = root.find(".//inhoudsindicatie")  # fallback without namespace
        if inhoud is not None:
            inhoud_text = self._extract_text_recursive(inhoud)
            if inhoud_text and inhoud_text != "-":
                text_parts.append("=== INHOUDSINDICATIE ===\n" + inhoud_text)

        # Uitspraak (main judgment text)
        uitspraak = root.find(f".//{rs_ns}uitspraak")
        if uitspraak is None:
            uitspraak = root.find(".//uitspraak")  # fallback without namespace
        if uitspraak is not None:
            uitspraak_text = self._extract_text_recursive(uitspraak)
            if uitspraak_text:
                text_parts.append("=== UITSPRAAK ===\n" + uitspraak_text)

        # Conclusie (for opinions/conclusions)
        conclusie = root.find(f".//{rs_ns}conclusie")
        if conclusie is None:
            conclusie = root.find(".//conclusie")  # fallback without namespace
        if conclusie is not None:
            conclusie_text = self._extract_text_recursive(conclusie)
            if conclusie_text:
                text_parts.append("=== CONCLUSIE ===\n" + conclusie_text)

        result["text"] = "\n\n".join(text_parts)

        return result

    def _get_text(self, elem: ET.Element, tag: str) -> str:
        """Get text content of a child element."""
        child = elem.find(tag, NAMESPACES)
        if child is not None and child.text:
            return child.text.strip()
        return ""

    def _get_all_texts(self, elem: ET.Element, tag: str) -> List[str]:
        """Get text content of all matching child elements."""
        texts = []
        for child in elem.findall(tag, NAMESPACES):
            if child.text:
                texts.append(child.text.strip())
        return texts

    def _extract_text_recursive(self, elem: ET.Element) -> str:
        """Recursively extract all text from an element and its children."""
        texts = []

        # Get element's direct text
        if elem.text:
            texts.append(elem.text.strip())

        # Process children
        for child in elem:
            child_text = self._extract_text_recursive(child)
            if child_text:
                texts.append(child_text)
            # Get tail text (text after child element)
            if child.tail:
                texts.append(child.tail.strip())

        # Join with appropriate whitespace
        full_text = " ".join(t for t in texts if t)

        # Clean up
        full_text = html.unescape(full_text)
        full_text = re.sub(r"\s+", " ", full_text)

        return full_text.strip()

    def fetch_all(self, start_offset: int = 0, checkpoint_file: str = None) -> Generator[dict, None, None]:
        """
        Yield all court decisions from Rechtspraak.

        Paginates through the ECLI index and fetches full documents.
        WARNING: Full fetch is 700K+ records. Use sample mode for testing.

        Args:
            start_offset: Resume from this offset (useful for checkpoint/resume)
            checkpoint_file: Path to save checkpoint state (optional)
        """
        offset = start_offset
        batch_size = 500  # Reduced from 1000 for stability
        total_fetched = 0

        # Load checkpoint if exists
        if checkpoint_file:
            checkpoint_path = Path(checkpoint_file)
            if checkpoint_path.exists():
                try:
                    with open(checkpoint_path, 'r') as f:
                        checkpoint = json.load(f)
                        offset = checkpoint.get('offset', offset)
                        total_fetched = checkpoint.get('total_fetched', 0)
                        logger.info(f"Resuming from checkpoint: offset={offset}, fetched={total_fetched}")
                except Exception as e:
                    logger.warning(f"Failed to load checkpoint: {e}")

        while True:
            # Search for ECLIs
            try:
                results = self._search_eclis(
                    max_results=batch_size,
                    offset=offset,
                    sort="DESC"
                )
            except Exception as e:
                logger.error(f"Search failed at offset {offset}: {e}")
                # Save checkpoint before exiting
                if checkpoint_file:
                    self._save_checkpoint(checkpoint_file, offset, total_fetched)
                break

            if not results:
                logger.info(f"No more results at offset {offset}")
                break

            # Check if we've hit too many consecutive errors
            if self.consecutive_errors >= self.max_consecutive_errors:
                logger.error(f"Too many errors, stopping at offset {offset}")
                if checkpoint_file:
                    self._save_checkpoint(checkpoint_file, offset, total_fetched)
                break

            # Fetch full document for each ECLI
            for item in results:
                ecli = item.get("ecli")
                if not ecli:
                    continue

                xml_content = self._fetch_document(ecli)
                if xml_content:
                    parsed = self._parse_document(xml_content)
                    if parsed and parsed.get("text"):
                        # Add search metadata
                        parsed["_search_title"] = item.get("title", "")
                        parsed["_search_summary"] = item.get("summary", "")
                        yield parsed
                        total_fetched += 1

                # Check for too many consecutive errors
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.warning("Too many consecutive errors in document fetch, pausing...")
                    time.sleep(60)  # Wait 1 minute before continuing
                    self.consecutive_errors = 0  # Reset and try again

            if total_fetched % 100 == 0:
                logger.info(f"Fetched {total_fetched} documents (offset={offset})...")

            offset += batch_size

            # Save checkpoint periodically
            if checkpoint_file and total_fetched % 500 == 0:
                self._save_checkpoint(checkpoint_file, offset, total_fetched)

            # Delay between batches (longer for VPS stability)
            time.sleep(3)

        logger.info(f"Total documents fetched: {total_fetched}")

    def _save_checkpoint(self, checkpoint_file: str, offset: int, total_fetched: int):
        """Save checkpoint state to file."""
        try:
            checkpoint_path = Path(checkpoint_file)
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            with open(checkpoint_path, 'w') as f:
                json.dump({
                    'offset': offset,
                    'total_fetched': total_fetched,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }, f)
            logger.info(f"Checkpoint saved: offset={offset}, fetched={total_fetched}")
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield records modified since the given date.

        Uses the modified parameter to filter by modification timestamp.
        """
        modified_since = since.strftime("%Y-%m-%dT%H:%M:%S")
        offset = 0
        batch_size = 1000

        while True:
            try:
                results = self._search_eclis(
                    max_results=batch_size,
                    offset=offset,
                    sort="DESC",
                    modified_since=modified_since
                )
            except Exception as e:
                logger.error(f"Update search failed: {e}")
                break

            if not results:
                break

            for item in results:
                ecli = item.get("ecli")
                if not ecli:
                    continue

                xml_content = self._fetch_document(ecli)
                if xml_content:
                    parsed = self._parse_document(xml_content)
                    if parsed and parsed.get("text"):
                        parsed["_search_title"] = item.get("title", "")
                        parsed["_search_summary"] = item.get("summary", "")
                        yield parsed

            offset += batch_size
            time.sleep(1)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw Rechtspraak data into standard schema.
        """
        ecli = raw.get("ecli", "")
        date = raw.get("date") or raw.get("issued") or ""

        # Build URL
        url = f"https://uitspraken.rechtspraak.nl/details?id={ecli}" if ecli else ""

        # Get title - use search title or construct from ECLI
        title = raw.get("title") or raw.get("_search_title") or ecli

        # Join subject list
        subjects = raw.get("subject", [])
        if isinstance(subjects, list):
            subjects = ", ".join(subjects)

        # Join procedure list
        procedures = raw.get("procedure", [])
        if isinstance(procedures, list):
            procedures = ", ".join(procedures)

        return {
            # Required base fields
            "_id": ecli,
            "_source": "NL/Rechtspraak",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": url,
            # Case law specific
            "ecli": ecli,
            "court": raw.get("creator", ""),
            "subject": subjects,
            "procedure": procedures,
            "type": raw.get("type", ""),
            "issued": raw.get("issued", ""),
            "modified": raw.get("modified", ""),
            "publisher": raw.get("publisher", ""),
            "summary": raw.get("_search_summary", ""),
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Rechtspraak Open Data API...")

        # Test search
        try:
            results = self._search_eclis(max_results=50)
            print(f"  Search API: OK ({len(results)} results)")

            # Try to find a document with full text (some recent ones may not have it yet)
            for item in results:
                ecli = item.get("ecli")
                if not ecli:
                    continue

                xml = self._fetch_document(ecli)
                if xml:
                    parsed = self._parse_document(xml)
                    text_len = len(parsed.get("text", ""))
                    print(f"  Document fetch: OK")
                    print(f"  Sample ECLI: {ecli}")
                    print(f"  Text length: {text_len} chars")

                    if text_len > 100:
                        print(f"  Preview: {parsed.get('text', '')[:200]}...")
                        break
                    else:
                        print(f"  (No full text yet, trying another...)")
        except Exception as e:
            print(f"  API test failed: {e}")
            return

        print("\nAPI test passed!")

    def run_sample(self, n: int = 10) -> dict:
        """Fetch sample records and save to sample directory."""
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        total_chars = 0

        for raw in self.fetch_all():
            if count >= n:
                break

            normalized = self.normalize(raw)
            text_len = len(normalized.get("text", ""))

            if text_len < 100:
                continue

            # Save to file
            ecli_safe = normalized["_id"].replace(":", "_").replace("/", "-")
            filename = f"{ecli_safe}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"[{count+1}/{n}] Saved: {normalized['_id']} ({text_len} chars)")
            count += 1
            total_chars += text_len

        return {
            "sample_records_saved": count,
            "total_chars": total_chars,
            "avg_chars_per_doc": total_chars // max(count, 1),
            "sample_dir": str(sample_dir),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="NL/Rechtspraak -- Dutch Court Decisions Data Fetcher"
    )
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    parser.add_argument("--sample-size", type=int, default=12,
                        help="Number of sample records to fetch (default: 12)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint if available")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="Seconds between requests (default: 1.5, use higher for VPS)")

    args = parser.parse_args()

    scraper = RechtspraakScraper(request_delay=args.delay)
    checkpoint_file = Path(__file__).parent / ".checkpoint.json"

    if args.command == "test-api":
        scraper.test_api()

    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=args.sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
            print(json.dumps(stats, indent=2))
        else:
            # Full bootstrap with checkpoint support
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            count = 0
            checkpoint = str(checkpoint_file) if args.resume else None
            for raw in scraper.fetch_all(checkpoint_file=checkpoint):
                normalized = scraper.normalize(raw)
                # In full mode, would save to database
                # For now, just count
                count += 1
                if count % 1000 == 0:
                    logger.info(f"Processed {count} records...")

            print(f"\nBootstrap complete: {count} records processed")

            # Clean up checkpoint on successful completion
            if checkpoint_file.exists():
                checkpoint_file.unlink()

    elif args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=30)
        count = 0
        for raw in scraper.fetch_updates(since):
            normalized = scraper.normalize(raw)
            count += 1
        print(f"\nUpdate complete: {count} records fetched")

    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
