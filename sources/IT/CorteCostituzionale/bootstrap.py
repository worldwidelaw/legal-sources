#!/usr/bin/env python3
"""
IT/CorteCostituzionale -- Italian Constitutional Court Data Fetcher

Fetches Constitutional Court decisions (sentenze, ordinanze) from the
official open data portal at dati.cortecostituzionale.it.

Strategy:
  - Bootstrap: Downloads JSON datasets organized by year periods from the
    official download page. Data available from 1956 to present.
  - Sample: Fetches decisions from the SPARQL endpoint for quick validation.
  - Updates: Uses SPARQL queries with date filtering.

Data Portal: https://dati.cortecostituzionale.it
SPARQL Endpoint: https://dati.cortecostituzionale.it/sparql/endpoint
License: CC BY SA 3.0

Usage:
  python bootstrap.py bootstrap          # Full initial pull (~21K decisions)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import html
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from io import BytesIO

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.CorteCostituzionale")

# API endpoints
SPARQL_ENDPOINT = "https://dati.cortecostituzionale.it/sparql/endpoint"
DOWNLOAD_BASE = "https://dati.cortecostituzionale.it/opendata/distribuzione/pronunce/"

# JSON download files (nested ZIPs containing yearly JSON files)
DOWNLOAD_FILES = [
    "P_json2001_oggi.zip",    # 2001-present
    "P_json1981_2000.zip",    # 1981-2000
    "P_json1956_1980.zip",    # 1956-1980
]


class CorteCostituzionaleScraper(BaseScraper):
    """
    Scraper for IT/CorteCostituzionale -- Italian Constitutional Court.
    Country: IT
    URL: https://www.cortecostituzionale.it

    Data types: case_law (Constitutional Court decisions)
    Auth: none (Open Data - CC BY SA 3.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,  # Longer timeout for large downloads
        )

    # -- SPARQL helpers --------------------------------------------------------

    def _sparql_query(self, query: str, limit: int = 100) -> list:
        """
        Execute a SPARQL query against the open data endpoint.
        Returns list of result bindings.
        """
        params = {
            "query": query,
            "output": "json",
        }

        self.rate_limiter.wait()

        try:
            resp = self.client.get(SPARQL_ENDPOINT, params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", {}).get("bindings", [])
        except Exception as e:
            logger.error(f"SPARQL query error: {e}")
            return []

    def _get_recent_decisions_sparql(self, year: int, limit: int = 100) -> list:
        """
        Fetch recent decisions from SPARQL endpoint.
        Returns list of decision metadata (without full text).
        """
        query = f"""
PREFIX dcc: <https://dati.cortecostituzionale.it/ontology/>
PREFIX time: <http://www.w3.org/2006/time#>
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX sioc: <http://rdfs.org/sioc/ns#>

SELECT ?pronuncia ?numero ?anno ?label ?identifier ?contentUrl WHERE {{
  ?pronuncia a dcc:Pronuncia ;
    dbpedia-owl:number ?numero ;
    time:year ?anno ;
    rdfs:label ?label ;
    dc:identifier ?identifier ;
    sioc:content ?contentUrl .
  FILTER(?anno = '{year}')
}}
ORDER BY DESC(?numero)
LIMIT {limit}
"""
        return self._sparql_query(query, limit)

    # -- JSON download helpers -------------------------------------------------

    def _download_and_extract_json(self, zip_url: str) -> Generator[dict, None, None]:
        """
        Download a ZIP file containing yearly JSON files, extract and yield records.

        The ZIP files are nested: outer ZIP contains yearly ZIPs,
        each yearly ZIP contains a single JSON file.
        """
        logger.info(f"Downloading {zip_url}")

        self.rate_limiter.wait()

        try:
            resp = self.client.get(zip_url, stream=True)
            resp.raise_for_status()

            # Load outer ZIP into memory
            outer_zip = zipfile.ZipFile(BytesIO(resp.content))

            for inner_name in outer_zip.namelist():
                if not inner_name.endswith('.zip'):
                    continue

                logger.info(f"  Processing {inner_name}")

                # Extract inner ZIP
                inner_content = outer_zip.read(inner_name)
                inner_zip = zipfile.ZipFile(BytesIO(inner_content))

                for json_name in inner_zip.namelist():
                    if not json_name.endswith('.json'):
                        continue

                    # Read and parse JSON
                    json_content = inner_zip.read(json_name)

                    # Try different encodings
                    for encoding in ['utf-8', 'latin-1', 'cp1252']:
                        try:
                            text = json_content.decode(encoding)
                            data = json.loads(text)
                            break
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                    else:
                        logger.warning(f"Failed to decode {json_name}")
                        continue

                    # Extract decisions from elenco_pronunce
                    decisions = data.get("elenco_pronunce", [])
                    logger.info(f"    Found {len(decisions)} decisions in {json_name}")

                    for decision in decisions:
                        yield decision

        except Exception as e:
            logger.error(f"Error processing {zip_url}: {e}")
            raise

    # -- Text cleaning ---------------------------------------------------------

    def _clean_text(self, text: str) -> str:
        """
        Clean HTML entities and normalize whitespace in text.
        """
        if not text:
            return ""

        # Decode HTML entities (&#13; etc.)
        text = html.unescape(text)

        # Remove HTML tags if any
        text = re.sub(r'<[^>]+>', ' ', text)

        # Normalize whitespace but preserve paragraph breaks
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\r', '\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)

        return text.strip()

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Constitutional Court decisions from JSON downloads.

        Downloads and processes nested ZIP files containing yearly JSON data.
        Total: ~21,000 decisions from 1956 to present.
        """
        for zip_file in DOWNLOAD_FILES:
            zip_url = DOWNLOAD_BASE + zip_file
            logger.info(f"Processing {zip_file}")

            for decision in self._download_and_extract_json(zip_url):
                yield decision

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions from the current and previous year.

        The open data portal updates periodically with new decisions.
        For incremental updates, we re-fetch recent years.
        """
        current_year = datetime.now().year
        years_to_fetch = [current_year, current_year - 1]

        # Download the most recent ZIP file
        zip_url = DOWNLOAD_BASE + "P_json2001_oggi.zip"

        for decision in self._download_and_extract_json(zip_url):
            anno = decision.get("anno_pronuncia", "")
            try:
                year = int(anno)
                if year in years_to_fetch:
                    yield decision
            except (ValueError, TypeError):
                continue

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw JSON decision data into standard schema.

        The JSON structure from dati.cortecostituzionale.it:
        {
            "collegio": "...",
            "numero_pronuncia": "1",
            "anno_pronuncia": "1956",
            "data_decisione": "05/06/1956",
            "epigrafe": "...",
            "relatore_pronuncia": "...",
            "testo": "FULL TEXT HERE",
            "ecli": "ECLI:IT:COST:1956:1",
            "dispositivo": "...",
            "data_deposito": "14/06/1956",
            "redattore_pronuncia": "...",
            "tipologia_pronuncia": "S",
            "presidente": "..."
        }
        """
        numero = raw.get("numero_pronuncia", "")
        anno = raw.get("anno_pronuncia", "")
        ecli = raw.get("ecli", "")
        tipologia = raw.get("tipologia_pronuncia", "")

        # Generate ID from ECLI or numero/anno
        if ecli:
            doc_id = ecli
        else:
            doc_id = f"COST_{anno}_{numero}"

        # Parse dates (DD/MM/YYYY format)
        date_decision = raw.get("data_decisione", "")
        date_deposit = raw.get("data_deposito", "")

        def parse_date(date_str):
            if not date_str:
                return None
            try:
                parts = date_str.split("/")
                if len(parts) == 3:
                    return f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                pass
            return date_str

        # Primary date is decision date
        date_iso = parse_date(date_decision) or parse_date(date_deposit) or (f"{anno}-01-01" if anno else None)

        # Map decision type
        type_map = {
            "S": "Sentenza",
            "O": "Ordinanza",
            "D": "Decreto",
        }
        decision_type = type_map.get(tipologia, tipologia or "Pronuncia")

        # Build title
        title = f"{decision_type} {numero}/{anno}" if numero and anno else raw.get("epigrafe", "")[:100]

        # Full text from testo field
        full_text = self._clean_text(raw.get("testo", ""))

        # Also include epigrafe and dispositivo for completeness
        epigrafe = self._clean_text(raw.get("epigrafe", ""))
        dispositivo = self._clean_text(raw.get("dispositivo", ""))

        # Combine full text with epigrafe and dispositivo if missing from testo
        if epigrafe and epigrafe not in full_text:
            full_text = epigrafe + "\n\n" + full_text
        if dispositivo and dispositivo not in full_text:
            full_text = full_text + "\n\n" + dispositivo

        # URL to decision page
        if ecli:
            url = f"https://www.cortecostituzionale.it/actionSchedaPronuncia.do?param_ecli={ecli}"
        else:
            url = f"https://www.cortecostituzionale.it/actionSchedaPronuncia.do?anno={anno}&numero={numero}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IT/CorteCostituzionale",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard fields
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": url,

            # Constitutional Court specific fields
            "ecli": ecli,
            "numero_pronuncia": numero,
            "anno_pronuncia": anno,
            "tipologia_pronuncia": tipologia,
            "decision_type": decision_type,
            "data_decisione": date_decision,
            "data_deposito": date_deposit,
            "presidente": raw.get("presidente", ""),
            "relatore_pronuncia": raw.get("relatore_pronuncia", ""),
            "redattore_pronuncia": raw.get("redattore_pronuncia", ""),
            "collegio": self._clean_text(raw.get("collegio", "")),
            "epigrafe": epigrafe,
            "dispositivo": dispositivo,
        }

    # -- Custom commands --------------------------------------------------------

    def test_api(self):
        """Quick connectivity and data availability test."""
        print("Testing Italian Constitutional Court Open Data...")

        # Test SPARQL endpoint
        print("\n1. SPARQL Endpoint:")
        query = """
PREFIX dcc: <https://dati.cortecostituzionale.it/ontology/>
SELECT (COUNT(?p) as ?count) WHERE { ?p a dcc:Pronuncia }
"""
        results = self._sparql_query(query)
        if results:
            count = results[0].get("count", {}).get("value", "?")
            print(f"   Total decisions in SPARQL: {count}")
        else:
            print("   SPARQL endpoint unavailable")

        # Test download endpoint
        print("\n2. Download Endpoints:")
        for zip_file in DOWNLOAD_FILES:
            url = DOWNLOAD_BASE + zip_file
            try:
                resp = self.client.head(url)
                size = int(resp.headers.get("content-length", 0)) / 1024 / 1024
                print(f"   {zip_file}: {size:.1f} MB")
            except Exception as e:
                print(f"   {zip_file}: Error - {e}")

        # Test sample decision
        print("\n3. Sample Decision (ECLI:IT:COST:2024:1):")
        query = """
PREFIX dcc: <https://dati.cortecostituzionale.it/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
SELECT ?label ?identifier WHERE {
  <https://dati.cortecostituzionale.it/ontology/Pronuncia/1/2024> rdfs:label ?label ;
    dc:identifier ?identifier .
}
"""
        results = self._sparql_query(query)
        if results:
            label = results[0].get("label", {}).get("value", "?")
            print(f"   Label: {label}")
        else:
            print("   Sample not found (may not exist yet)")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = CorteCostituzionaleScraper()

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
