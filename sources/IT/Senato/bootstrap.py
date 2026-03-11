#!/usr/bin/env python3
"""
IT/Senato -- Senato della Repubblica Data Fetcher

Fetches Italian Senate bills (Disegni di Legge) with full text from the
Open Data portal via SPARQL + GitHub Akoma Ntoso repository.

Strategy:
  - Uses SPARQL endpoint (dati.senato.it/sparql) to query DDL metadata
  - Downloads full text from GitHub AkomaNtosoBulkData repository
  - Parses Akoma Ntoso XML to extract document body text
  - Supports incremental updates via dataPresentazione filter

Data sources:
  - SPARQL: https://dati.senato.it/sparql
  - GitHub: https://github.com/SenatoDellaRepubblica/AkomaNtosoBulkData

Usage:
  python bootstrap.py bootstrap          # Full initial pull (58K+ records)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent bills)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import html
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.IT.senato")

# Endpoints
SPARQL_ENDPOINT = "https://dati.senato.it/sparql"
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/SenatoDellaRepubblica/AkomaNtosoBulkData/master"

# SPARQL prefixes
SPARQL_PREFIXES = """
PREFIX osr: <http://dati.senato.it/osr/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
"""

# Akoma Ntoso namespace
AKN_NS = {"an": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD03"}

# GitHub repository coverage - idFase ranges per legislature where XML is available
# These ranges are based on actual repository contents
GITHUB_COVERAGE = {
    19: (55177, 58032),  # Leg19: Atto00055177 to Atto00058032
    18: (37946, 54869),  # Leg18 approximate range
    17: (22155, 37727),  # Leg17 approximate range
    16: (12671, 21989),  # Leg16 approximate range
    15: (7952, 12559),   # Leg15 approximate range
    14: (3779, 7936),    # Leg14 approximate range
    13: (1, 3777),       # Leg13 approximate range
}


class SenatoScraper(BaseScraper):
    """
    Scraper for IT/Senato -- Italian Senate Open Data.
    Country: IT
    URL: https://www.senato.it

    Data types: legislation (DDL - Disegni di Legge)
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Use HttpClient without base_url to avoid trailing slash issues
        self.http_client = HttpClient(
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            },
            timeout=60,
        )

    # -- SPARQL helpers --------------------------------------------------------

    def _sparql_query(self, query: str) -> list:
        """Execute a SPARQL query and return bindings as list of dicts."""
        self.rate_limiter.wait()

        try:
            # Make direct request to avoid trailing slash issues
            params = {"query": query.strip(), "format": "json"}
            resp = self.http_client.get(SPARQL_ENDPOINT, params=params)
            resp.raise_for_status()
            data = resp.json()
            bindings = data.get("results", {}).get("bindings", [])
            return bindings
        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            return []

    def _get_ddl_list(
        self,
        legislature: Optional[int] = None,
        since_date: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        id_fase_min: Optional[int] = None,
        id_fase_max: Optional[int] = None,
    ) -> list:
        """
        Query SPARQL for DDL list with metadata.

        Args:
            legislature: Filter by legislature number (e.g., 19)
            since_date: Filter by presentation date >= this date (YYYY-MM-DD)
            limit: Maximum number of results
            offset: Pagination offset
            id_fase_min: Minimum idFase value (for filtering to GitHub range)
            id_fase_max: Maximum idFase value (for filtering to GitHub range)

        Returns:
            List of DDL metadata dicts
        """
        filters = []
        if legislature:
            filters.append(f"?ddl osr:legislatura {legislature} .")
        if since_date:
            filters.append(f'FILTER(?dataPres >= "{since_date}")')
        if id_fase_min is not None:
            filters.append(f"FILTER(?idFase >= {id_fase_min})")
        if id_fase_max is not None:
            filters.append(f"FILTER(?idFase <= {id_fase_max})")

        filter_clause = "\n".join(filters)
        limit_clause = f"LIMIT {limit}" if limit else ""
        offset_clause = f"OFFSET {offset}" if offset > 0 else ""

        query = f"""
        {SPARQL_PREFIXES}
        SELECT ?ddl ?titolo ?dataPres ?leg ?idFase ?idDdl ?fase ?numeroFase
               ?statoDdl ?ramo ?natura ?descrIniziativa
        WHERE {{
            ?ddl a osr:Ddl .
            ?ddl osr:titolo ?titolo .
            OPTIONAL {{ ?ddl osr:dataPresentazione ?dataPres }}
            OPTIONAL {{ ?ddl osr:legislatura ?leg }}
            OPTIONAL {{ ?ddl osr:idFase ?idFase }}
            OPTIONAL {{ ?ddl osr:idDdl ?idDdl }}
            OPTIONAL {{ ?ddl osr:fase ?fase }}
            OPTIONAL {{ ?ddl osr:numeroFase ?numeroFase }}
            OPTIONAL {{ ?ddl osr:statoDdl ?statoDdl }}
            OPTIONAL {{ ?ddl osr:ramo ?ramo }}
            OPTIONAL {{ ?ddl osr:natura ?natura }}
            OPTIONAL {{ ?ddl osr:descrIniziativa ?descrIniziativa }}
            {filter_clause}
        }}
        ORDER BY DESC(?dataPres)
        {limit_clause}
        {offset_clause}
        """

        bindings = self._sparql_query(query)

        results = []
        for b in bindings:
            ddl = {
                "ddl_uri": b.get("ddl", {}).get("value", ""),
                "titolo": b.get("titolo", {}).get("value", ""),
                "dataPresentazione": b.get("dataPres", {}).get("value", ""),
                "legislatura": int(b.get("leg", {}).get("value", "0") or "0"),
                "idFase": int(b.get("idFase", {}).get("value", "0") or "0"),
                "idDdl": int(b.get("idDdl", {}).get("value", "0") or "0"),
                "fase": b.get("fase", {}).get("value", ""),
                "numeroFase": b.get("numeroFase", {}).get("value", ""),
                "statoDdl": b.get("statoDdl", {}).get("value", ""),
                "ramo": b.get("ramo", {}).get("value", ""),
                "natura": b.get("natura", {}).get("value", ""),
                "descrIniziativa": b.get("descrIniziativa", {}).get("value", ""),
            }
            if ddl["idFase"] > 0:  # Only include records with valid idFase
                results.append(ddl)

        return results

    # -- GitHub/Akoma Ntoso helpers --------------------------------------------

    def _get_github_folder_contents(self, path: str) -> list:
        """List contents of a GitHub folder via API."""
        api_url = f"https://api.github.com/repos/SenatoDellaRepubblica/AkomaNtosoBulkData/contents/{path}"

        self.rate_limiter.wait()

        try:
            resp = self.http_client.get(api_url)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug(f"GitHub API error for {path}: {e}")
            return []

    def _download_akn_xml(self, legislature: int, id_fase: int) -> Optional[str]:
        """
        Download Akoma Ntoso XML for a specific DDL.

        Args:
            legislature: Legislature number
            id_fase: Phase ID (used to construct folder path)

        Returns:
            XML content as string, or None if not found
        """
        # Construct the folder path: Leg{N}/Atto{idFase:08d}/ddlpres/
        atto_folder = f"Leg{legislature}/Atto{id_fase:08d}"
        ddlpres_path = f"{atto_folder}/ddlpres"

        # First, list the folder to find the XML file
        contents = self._get_github_folder_contents(ddlpres_path)

        if not contents:
            logger.debug(f"No ddlpres folder for {atto_folder}")
            return None

        # Find .akn.xml files
        xml_files = [f for f in contents if f.get("name", "").endswith(".akn.xml")]

        if not xml_files:
            logger.debug(f"No .akn.xml files in {ddlpres_path}")
            return None

        # Download the first XML file (usually there's only one)
        download_url = xml_files[0].get("download_url", "")
        if not download_url:
            return None

        self.rate_limiter.wait()

        try:
            resp = self.http_client.get(download_url)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to download {download_url}: {e}")
            return None

    def _parse_akn_xml(self, xml_content: str) -> str:
        """
        Parse Akoma Ntoso XML and extract the bill text.

        Args:
            xml_content: Raw XML string

        Returns:
            Extracted text content
        """
        try:
            root = ET.fromstring(xml_content.encode("utf-8"))
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return ""

        text_parts = []

        # Extract from coverPage
        for elem in root.findall(".//an:coverPage", AKN_NS):
            text = "".join(elem.itertext()).strip()
            if text:
                text_parts.append(text)

        # Extract from body (main content)
        for body in root.findall(".//an:body", AKN_NS):
            # Extract articles and paragraphs
            for article in body.findall(".//an:article", AKN_NS):
                article_text = []

                # Article number
                num_elem = article.find("an:num", AKN_NS)
                if num_elem is not None and num_elem.text:
                    article_text.append(f"Articolo {num_elem.text}")

                # Paragraphs within article
                for para in article.findall(".//an:paragraph", AKN_NS):
                    para_num = para.find("an:num", AKN_NS)
                    content = para.find(".//an:content", AKN_NS)
                    if content is not None:
                        para_text = "".join(content.itertext()).strip()
                        if para_num is not None and para_num.text:
                            para_text = f"{para_num.text}. {para_text}"
                        if para_text:
                            article_text.append(para_text)

                if article_text:
                    text_parts.append("\n".join(article_text))

        # Extract from attachments (relazione, etc.)
        for attachment in root.findall(".//an:attachments//an:doc", AKN_NS):
            for body in attachment.findall(".//an:mainBody", AKN_NS):
                body_text = "".join(body.itertext()).strip()
                if body_text:
                    text_parts.append(body_text)

        # Combine all parts
        full_text = "\n\n".join(text_parts)

        # Clean up
        full_text = html.unescape(full_text)
        full_text = re.sub(r"\s+", " ", full_text)
        full_text = re.sub(r" +\n", "\n", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)

        return full_text.strip()

    def _get_doc_id_from_senate_page(self, legislature: int, id_fase: int) -> Optional[str]:
        """
        Scrape the Senate DDL page to find the document ID.

        Args:
            legislature: Legislature number
            id_fase: Phase ID

        Returns:
            Document ID (e.g., "01360967") or None
        """
        # Try the texts and amendments tab
        url = f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl?tab=testiEmendamenti&did={id_fase}"

        self.rate_limiter.wait()

        try:
            resp = self.http_client.get(url)
            resp.raise_for_status()

            # Look for PDFServer/BGT/{doc_id} pattern
            match = re.search(r"PDFServer/BGT/(\d+)", resp.text)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            logger.debug(f"Failed to scrape Senate page for {id_fase}: {e}")
            return None

    def _download_text_from_senate_html(self, doc_id: str) -> str:
        """
        Try to download HTML text from Senate website.

        Args:
            doc_id: Document ID (e.g., "01360967")

        Returns:
            Extracted text or empty string
        """
        # Try raw HTML endpoint first
        html_url = f"https://www.senato.it/service/HTML/PDFServer/BGT/{doc_id}.html"

        self.rate_limiter.wait()

        try:
            resp = self.http_client.get(html_url)
            if resp.status_code == 200:
                # Parse HTML and extract text
                text = self._clean_html_text(resp.text)
                if text and len(text) > 100:
                    return text
        except Exception as e:
            logger.debug(f"HTML fetch failed for {doc_id}: {e}")

        return ""

    def _clean_html_text(self, html_content: str) -> str:
        """
        Extract clean text from HTML content.

        Args:
            html_content: Raw HTML

        Returns:
            Cleaned text
        """
        # Remove script and style elements
        html_content = re.sub(r"<script[^>]*>.*?</script>", "", html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r"<style[^>]*>.*?</style>", "", html_content, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", html_content)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()

    def _download_full_text(self, ddl: dict) -> str:
        """
        Download and parse full text for a DDL.

        Strategy:
        1. Try GitHub Akoma Ntoso XML first (best quality)
        2. Fall back to scraping Senate website for document ID
        3. Try HTML endpoint with document ID

        Args:
            ddl: DDL metadata dict from SPARQL

        Returns:
            Extracted full text
        """
        legislature = ddl.get("legislatura", 0)
        id_fase = ddl.get("idFase", 0)

        if legislature < 13 or id_fase == 0:
            return ""

        # Strategy 1: Try GitHub Akoma Ntoso XML
        xml_content = self._download_akn_xml(legislature, id_fase)
        if xml_content:
            text = self._parse_akn_xml(xml_content)
            if text and len(text) > 100:
                logger.debug(f"Got text from GitHub XML for {id_fase}")
                return text

        # Strategy 2: Fall back to Senate website
        doc_id = self._get_doc_id_from_senate_page(legislature, id_fase)
        if doc_id:
            text = self._download_text_from_senate_html(doc_id)
            if text:
                logger.debug(f"Got text from Senate HTML for {id_fase}")
                return text

        return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self, sample_mode: bool = False) -> Generator[dict, None, None]:
        """
        Yield all DDL records from the Senate.

        WARNING: Full fetch is 58K+ records. Use sample mode for testing.

        Args:
            sample_mode: If True, fetch only records with GitHub XML available
        """
        # Iterate through legislatures 19 down to 13
        for leg in range(19, 12, -1):
            logger.info(f"Fetching DDL for legislature {leg}")

            # Get GitHub coverage range for this legislature
            coverage = GITHUB_COVERAGE.get(leg)
            id_fase_min = coverage[0] if coverage and sample_mode else None
            id_fase_max = coverage[1] if coverage and sample_mode else None

            if sample_mode and coverage:
                logger.info(f"  Sample mode: filtering to idFase {id_fase_min}-{id_fase_max}")

            offset = 0
            page_size = 100

            while True:
                ddl_list = self._get_ddl_list(
                    legislature=leg,
                    limit=page_size,
                    offset=offset,
                    id_fase_min=id_fase_min,
                    id_fase_max=id_fase_max,
                )

                if not ddl_list:
                    logger.info(f"No more DDL for legislature {leg}")
                    break

                for ddl in ddl_list:
                    yield ddl

                if len(ddl_list) < page_size:
                    break

                offset += page_size
                logger.info(f"  Legislature {leg}: fetched {offset} records")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield DDL records presented since the given date.
        """
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching DDL updates since {since_str}")

        offset = 0
        page_size = 100

        while True:
            ddl_list = self._get_ddl_list(
                since_date=since_str,
                limit=page_size,
                offset=offset,
            )

            if not ddl_list:
                break

            for ddl in ddl_list:
                yield ddl

            if len(ddl_list) < page_size:
                break

            offset += page_size

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw DDL metadata into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from Akoma Ntoso XML.
        """
        # Download full text
        full_text = self._download_full_text(raw)

        id_fase = raw.get("idFase", 0)
        legislature = raw.get("legislatura", 0)

        # Construct URL to the bill page
        url = f"http://www.senato.it/leg/{legislature}/BGT/Schede/Ddliter/{id_fase}.htm"

        return {
            # Required base fields
            "_id": str(id_fase),
            "_source": "IT/Senato",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("titolo", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": raw.get("dataPresentazione", ""),
            "url": url,
            # Source-specific fields
            "idFase": id_fase,
            "idDdl": raw.get("idDdl", 0),
            "legislatura": legislature,
            "fase": raw.get("fase", ""),
            "numeroFase": raw.get("numeroFase", ""),
            "statoDdl": raw.get("statoDdl", ""),
            "ramo": raw.get("ramo", ""),
            "natura": raw.get("natura", ""),
            "descrIniziativa": raw.get("descrIniziativa", ""),
            "ddl_uri": raw.get("ddl_uri", ""),
        }

    # -- Override methods ----------------------------------------------------

    def run_sample(self, n: int = 12) -> dict:
        """
        Override run_sample to fetch records with GitHub XML available.

        The default fetch_all() returns most recent DDLs first, but these
        may not have XML in the GitHub repository yet. This override filters
        to records within the known GitHub coverage range.
        """
        from datetime import datetime, timezone

        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        sample_records = []

        # Fetch from GitHub-available range (Leg19)
        logger.info(f"Fetching sample of {n} DDLs with GitHub XML available")

        # Get DDLs from the GitHub-covered range
        coverage = GITHUB_COVERAGE.get(19)
        if not coverage:
            logger.error("No GitHub coverage data for Leg19")
            stats["errors"] = 1
            return stats

        ddl_list = self._get_ddl_list(
            legislature=19,
            id_fase_min=coverage[0],
            id_fase_max=coverage[1],
            limit=n * 2,  # Fetch extra in case some fail
        )

        for ddl in ddl_list:
            if len(sample_records) >= n:
                break

            self.rate_limiter.wait()

            try:
                record = self.normalize(ddl)
            except Exception as e:
                logger.warning(f"Normalization error: {e}")
                stats["errors"] += 1
                continue

            # Validate
            is_valid, errors = self.validator.validate(record)
            if not is_valid:
                logger.warning(f"Validation errors for {record.get('_id', '?')}: {errors}")

            stats["records_fetched"] += 1

            # Only include records with actual full text
            if record.get("text") and len(record.get("text", "")) > 100:
                sample_records.append(record)
                logger.info(f"  Sample {len(sample_records)}/{n}: {record['_id']} - {len(record['text'])} chars")
            else:
                logger.warning(f"  Skipping {record['_id']} - no full text")

        # Save sample records
        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        stats["sample_records_saved"] = len(sample_records)

        if sample_records:
            self._save_samples(sample_records)
            logger.info(f"Saved {len(sample_records)} sample records")

        # Update status
        self._update_status(stats)

        return stats

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing IT/Senato APIs...")

        # Test SPARQL endpoint
        print("\n1. Testing SPARQL endpoint...")
        query = """
        PREFIX osr: <http://dati.senato.it/osr/>
        SELECT (COUNT(*) as ?count) WHERE { ?s a osr:Ddl }
        """
        bindings = self._sparql_query(query)
        if bindings:
            count = bindings[0].get("count", {}).get("value", "0")
            print(f"   Total DDL in SPARQL: {count}")
        else:
            print("   SPARQL query failed!")
            return

        # Test GitHub repository
        print("\n2. Testing GitHub repository...")
        contents = self._get_github_folder_contents("Leg19")
        if contents:
            print(f"   Leg19 folder: {len(contents)} items")
        else:
            print("   GitHub API failed!")
            return

        # Test full fetch for one DDL
        print("\n3. Testing full text download...")
        ddl_list = self._get_ddl_list(legislature=19, limit=1)
        if ddl_list:
            ddl = ddl_list[0]
            print(f"   Sample DDL: {ddl['titolo'][:60]}...")
            text = self._download_full_text(ddl)
            if text:
                print(f"   Full text: {len(text)} characters")
            else:
                print("   WARNING: No full text available")
        else:
            print("   No DDL found!")
            return

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = SenatoScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
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
