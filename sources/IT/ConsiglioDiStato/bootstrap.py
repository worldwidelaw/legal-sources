#!/usr/bin/env python3
"""
IT/ConsiglioDiStato -- Italian Council of State Data Fetcher

Fetches administrative court decisions (sentenze, ordinanze, decreti, pareri)
from the OpenGA CKAN portal and mdp endpoint.

Strategy:
  - Uses OpenGA CKAN API to get decision metadata (case numbers, dates, sections)
  - Constructs URLs to the mdp.giustizia-amministrativa.it XML endpoint
  - Extracts full text from the XML response
  - Covers Council of State (CdS), CGA Sicily, and all 29 Regional Administrative Courts (TAR)

Data Portal: https://openga.giustizia-amministrativa.it
Full Text Endpoint: https://mdp.giustizia-amministrativa.it/visualizza/
License: CC BY 4.0 (Licence Ouverte)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
import xml.etree.ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.ConsiglioDiStato")

# API endpoints
CKAN_API = "https://openga.giustizia-amministrativa.it/api/3/action"
MDP_ENDPOINT = "https://mdp.giustizia-amministrativa.it/visualizza/"

# Court schema mappings (dataset prefix -> URL schema)
COURT_SCHEMAS = {
    "cds": "cds",                             # Council of State
    "cga-sicilia": "cga-sicilia",              # CGA Sicily
    "tar-lazio-roma": "tar-lazio-roma",        # TAR Lazio - Rome
    "tar-lombardia-milano": "tar-lombardia-milano",
    "tar-campania-napoli": "tar-campania-napoli",
    # Add more TAR courts as needed
}

# All sentenze datasets available on OpenGA
SENTENZE_DATASETS = [
    "cds-sentenze",
    "cga-sicilia-sentenze",
    "tar-abruzzo-l-aquila-sentenze",
    "tar-abruzzo-pescara-sentenze",
    "tar-basilicata-sentenze",
    "tar-calabria-catanzaro-sentenze",
    "tar-calabria-reggio-calabria-sentenze",
    "tar-campania-napoli-sentenze",
    "tar-campania-salerno-sentenze",
    "tar-emilia-romagna-bologna-sentenze",
    "tar-emilia-romagna-parma-sentenze",
    "tar-friuli-venezia-giulia-sentenze",
    "tar-lazio-latina-sentenze",
    "tar-lazio-roma-sentenze",
    "tar-liguria-sentenze",
    "tar-lombardia-brescia-sentenze",
    "tar-lombardia-milano-sentenze",
    "tar-marche-sentenze",
    "tar-molise-sentenze",
    "tar-piemonte-sentenze",
    "tar-puglia-bari-sentenze",
    "tar-puglia-lecce-sentenze",
    "tar-sardegna-sentenze",
    "tar-sicilia-catania-sentenze",
    "tar-sicilia-palermo-sentenze",
    "tar-toscana-sentenze",
    "tar-umbria-sentenze",
    "tar-valle-d-aosta-sentenze",
    "tar-veneto-sentenze",
    "trga-bolzano-sentenze",
    "trga-trento-sentenze",
]


class ConsiglioDiStatoScraper(BaseScraper):
    """
    Scraper for IT/ConsiglioDiStato -- Italian Administrative Courts.
    Country: IT
    URL: https://www.giustizia-amministrativa.it

    Data types: case_law (Administrative Court decisions)
    Auth: none (Open Data - CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- CKAN API helpers ------------------------------------------------------

    def _get_dataset_resources(self, dataset_name: str) -> list:
        """Get resources (data files) for a dataset."""
        url = f"{CKAN_API}/package_show"
        params = {"id": dataset_name}

        self.rate_limiter.wait()
        resp = self.client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        if data.get("success"):
            return data["result"].get("resources", [])
        return []

    def _get_json_resource_id(self, dataset_name: str, year: int = None) -> Optional[str]:
        """Get the JSON resource ID for a dataset, optionally for a specific year."""
        resources = self._get_dataset_resources(dataset_name)

        for resource in resources:
            if resource.get("format", "").upper() == "JSON":
                # If year specified, try to find matching year resource
                if year:
                    name = resource.get("name", "")
                    if str(year) in name:
                        return resource["id"]
                else:
                    # Return first JSON resource
                    return resource["id"]

        # If no specific year found, return any JSON resource
        for resource in resources:
            if resource.get("format", "").upper() == "JSON":
                return resource["id"]
        return None

    def _query_datastore(self, resource_id: str, limit: int = 100, offset: int = 0) -> dict:
        """Query the CKAN datastore for records."""
        url = f"{CKAN_API}/datastore_search"
        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }

        self.rate_limiter.wait()
        resp = self.client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    # -- Full text extraction --------------------------------------------------

    def _extract_schema_from_dataset(self, dataset_name: str) -> str:
        """Extract the schema (court code) from dataset name."""
        # Remove -sentenze suffix
        schema = dataset_name.replace("-sentenze", "")
        return schema

    def _construct_mdp_url(self, schema: str, nrg: int, num_provv: int) -> str:
        """
        Construct the URL to fetch the full text XML.

        The URL pattern is:
        https://mdp.giustizia-amministrativa.it/visualizza/?nodeRef=&schema={schema}&nrg={nrg}&nomeFile={num_provv}_11.html&subDir=Provvedimenti

        Where:
        - schema: court code (cds, tar-lazio-roma, etc.)
        - nrg: NUMERO_RICORSO (case registration number)
        - num_provv: NUMERO_PROVVEDIMENTO (decision number, used in filename)
        """
        return f"{MDP_ENDPOINT}?nodeRef=&schema={schema}&nrg={nrg}&nomeFile={num_provv}_11.html&subDir=Provvedimenti"

    def _extract_text_from_xml(self, xml_content: str) -> str:
        """Extract full text from the decision XML."""
        try:
            # Remove XML declaration and stylesheet reference
            xml_content = re.sub(r'<\?xml[^>]*\?>', '', xml_content)
            xml_content = re.sub(r'<\?xml-stylesheet[^>]*\?>', '', xml_content)

            # Parse XML
            root = ET.fromstring(xml_content)

            # Extract all text content recursively
            def get_text(element) -> str:
                texts = []
                if element.text:
                    texts.append(element.text.strip())
                for child in element:
                    texts.append(get_text(child))
                    if child.tail:
                        texts.append(child.tail.strip())
                return ' '.join(filter(None, texts))

            text = get_text(root)

            # Clean up the text
            text = re.sub(r'\s+', ' ', text)
            text = html.unescape(text)

            return text.strip()
        except ET.ParseError as e:
            logger.warning(f"Failed to parse XML: {e}")
            # Fallback: strip tags with regex
            text = re.sub(r'<[^>]+>', ' ', xml_content)
            text = re.sub(r'\s+', ' ', text)
            text = html.unescape(text)
            return text.strip()

    def _fetch_full_text(self, schema: str, nrg: int, num_provv: int) -> Optional[str]:
        """Fetch the full text of a decision from the mdp endpoint."""
        url = self._construct_mdp_url(schema, nrg, num_provv)

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 200:
                content = resp.text
                if '<Provvedimento>' in content or '<GA' in content:
                    return self._extract_text_from_xml(content)

            logger.debug(f"No content for {schema}/{nrg}/{num_provv}: HTTP {resp.status_code}")
            return None
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    # -- Normalize -------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw CKAN record into standard schema.

        Expected raw dict contains:
          - CKAN fields (NUMERO_PROVVEDIMENTO, etc.)
          - _text: full text (added by fetch_all)
          - _dataset_name: dataset name (added by fetch_all)
        """
        # Extract key fields
        num_provv = raw.get("NUMERO_PROVVEDIMENTO", "")
        num_ricorso = raw.get("NUMERO_RICORSO", "")
        data_pub = raw.get("DATA_PUBBLICAZIONE", "")
        anno = raw.get("ANNO_PUBBLICAZIONE", "")
        sede = raw.get("NOME_SEDE", "")
        sezione = raw.get("NOME_SEZIONE", "")
        tipo = raw.get("TIPO_PROVVEDIMENTO", "")
        esito = raw.get("ESITO_PROVVEDIMENTO", "")
        oggetto = raw.get("OGGETTO_RICORSO", "")

        # Get text and dataset from enriched raw record
        full_text = raw.get("_text", "")
        dataset_name = raw.get("_dataset_name", "")

        # Construct unique ID
        doc_id = f"IT:GA:{sede.replace(' ', '_')}:{anno}:{num_provv}"

        # Construct ECLI if possible
        ecli = None
        if sede and anno and num_provv:
            # Map sede to ECLI court code
            if "CdS" in sede:
                ecli = f"ECLI:IT:CDS:{anno}:{num_provv}SENT"
            elif "TAR" in sede:
                # Extract region from sede
                court_code = sede.replace(" ", "").replace("-", "").upper()
                ecli = f"ECLI:IT:{court_code}:{anno}:{num_provv}SENT"

        # Construct title
        title = f"{tipo} n. {num_provv}/{anno}"
        if sezione:
            title += f" - {sezione}"
        if oggetto:
            title += f" - {oggetto[:100]}"

        # Construct source URL
        schema = self._extract_schema_from_dataset(dataset_name) if dataset_name else "cds"
        source_url = self._construct_mdp_url(schema, num_ricorso, num_provv)

        return {
            "_id": doc_id,
            "_source": "IT/ConsiglioDiStato",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": data_pub if data_pub else f"{anno}-01-01",
            "url": source_url,
            "ecli": ecli,
            "court": sede,
            "section": sezione,
            "decision_type": tipo,
            "outcome": esito,
            "case_number": str(num_ricorso),
            "decision_number": str(num_provv),
            "year": anno,
            "subject": oggetto,
            "language": "it",
            "license": "CC BY 4.0",
        }

    # -- BaseScraper implementation --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Fetch all decisions from all courts.

        Yields raw records enriched with:
          - _text: full text content
          - _dataset_name: source dataset name

        Records without full text are skipped.
        """
        for dataset_name in SENTENZE_DATASETS:
            logger.info(f"Processing dataset: {dataset_name}")

            try:
                # Get the JSON resource ID for 2024 (most recent full year)
                resource_id = self._get_json_resource_id(dataset_name, year=2024)
                if not resource_id:
                    # Try without year filter
                    resource_id = self._get_json_resource_id(dataset_name)

                if not resource_id:
                    logger.warning(f"No JSON resource found for {dataset_name}")
                    continue

                schema = self._extract_schema_from_dataset(dataset_name)

                # Paginate through all records
                offset = 0
                limit = 100

                while True:
                    result = self._query_datastore(resource_id, limit=limit, offset=offset)

                    if not result.get("success"):
                        logger.error(f"Datastore query failed for {dataset_name}")
                        break

                    records = result.get("result", {}).get("records", [])
                    if not records:
                        break

                    for record in records:
                        try:
                            # Get full text
                            nrg = record.get("NUMERO_RICORSO")
                            num_provv = record.get("NUMERO_PROVVEDIMENTO")

                            if nrg and num_provv:
                                full_text = self._fetch_full_text(schema, nrg, num_provv)

                                if full_text and len(full_text) > 500:
                                    # Enrich raw record with text and metadata
                                    record["_text"] = full_text
                                    record["_dataset_name"] = dataset_name
                                    yield record
                                else:
                                    logger.debug(f"Skipping {nrg}/{num_provv}: no/short full text")
                        except Exception as e:
                            logger.warning(f"Error processing record: {e}")
                            continue

                    offset += limit
                    logger.info(f"Processed {offset} records from {dataset_name}")

            except Exception as e:
                logger.error(f"Error processing {dataset_name}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions published since a given date."""
        # For updates, focus on CdS and major TAR courts
        priority_datasets = [
            "cds-sentenze",
            "tar-lazio-roma-sentenze",
            "tar-lombardia-milano-sentenze",
            "cga-sicilia-sentenze",
        ]

        since_year = since.year

        for dataset_name in priority_datasets:
            try:
                resource_id = self._get_json_resource_id(dataset_name, year=since_year)
                if not resource_id:
                    resource_id = self._get_json_resource_id(dataset_name)

                if not resource_id:
                    continue

                schema = self._extract_schema_from_dataset(dataset_name)

                # Get recent records
                result = self._query_datastore(resource_id, limit=100, offset=0)

                if result.get("success"):
                    for record in result.get("result", {}).get("records", []):
                        pub_date = record.get("DATA_PUBBLICAZIONE", "")
                        if pub_date and pub_date >= since.strftime("%Y-%m-%d"):
                            nrg = record.get("NUMERO_RICORSO")
                            num_provv = record.get("NUMERO_PROVVEDIMENTO")

                            if nrg and num_provv:
                                full_text = self._fetch_full_text(schema, nrg, num_provv)
                                if full_text:
                                    # Enrich raw record with text and metadata
                                    record["_text"] = full_text
                                    record["_dataset_name"] = dataset_name
                                    yield record

            except Exception as e:
                logger.error(f"Error fetching updates from {dataset_name}: {e}")

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch a sample of decisions for validation."""
        samples_per_court = max(3, count // 5)  # Distribute across courts
        total = 0

        # Sample from CdS and a few major TAR courts
        sample_datasets = [
            "cds-sentenze",
            "tar-lazio-roma-sentenze",
            "tar-lombardia-milano-sentenze",
            "cga-sicilia-sentenze",
            "tar-campania-napoli-sentenze",
        ]

        for dataset_name in sample_datasets:
            if total >= count:
                break

            try:
                resource_id = self._get_json_resource_id(dataset_name, year=2024)
                if not resource_id:
                    resource_id = self._get_json_resource_id(dataset_name)

                if not resource_id:
                    logger.warning(f"No resource found for {dataset_name}")
                    continue

                schema = self._extract_schema_from_dataset(dataset_name)

                # Get some records
                result = self._query_datastore(resource_id, limit=samples_per_court * 2, offset=0)

                if not result.get("success"):
                    continue

                court_samples = 0
                for record in result.get("result", {}).get("records", []):
                    if court_samples >= samples_per_court or total >= count:
                        break

                    nrg = record.get("NUMERO_RICORSO")
                    num_provv = record.get("NUMERO_PROVVEDIMENTO")

                    if nrg and num_provv:
                        logger.info(f"Fetching {dataset_name}: {nrg}/{num_provv}")
                        full_text = self._fetch_full_text(schema, nrg, num_provv)

                        if full_text and len(full_text) > 500:
                            # Enrich raw record with text and metadata
                            record["_text"] = full_text
                            record["_dataset_name"] = dataset_name
                            # Return normalized for sample mode
                            normalized = self.normalize(record)
                            yield normalized
                            court_samples += 1
                            total += 1
                            logger.info(f"  -> {len(full_text)} chars")
                        else:
                            logger.debug(f"Skipping {nrg}/{num_provv}: no/short text")

            except Exception as e:
                logger.error(f"Error sampling {dataset_name}: {e}")
                continue

        logger.info(f"Sampled {total} records total")

    def test_api(self) -> bool:
        """Test API connectivity."""
        try:
            # Test CKAN API
            url = f"{CKAN_API}/package_list"
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.error(f"CKAN API failed: HTTP {resp.status_code}")
                return False

            data = resp.json()
            if not data.get("success"):
                logger.error("CKAN API returned failure")
                return False

            # Test mdp endpoint with a known working URL
            test_url = self._construct_mdp_url("cds", 202203542, 202402322)
            resp = self.client.get(test_url)
            if resp.status_code != 200:
                logger.error(f"MDP endpoint failed: HTTP {resp.status_code}")
                return False

            if '<Provvedimento>' not in resp.text:
                logger.error("MDP endpoint did not return expected XML")
                return False

            logger.info("API connectivity test passed")
            return True

        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IT/ConsiglioDiStato data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch only sample records (for validation)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of sample records to fetch"
    )

    args = parser.parse_args()
    scraper = ConsiglioDiStatoScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            # Sample mode - save to sample/ directory
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            count = 0
            total_chars = 0

            for record in scraper.fetch_sample(count=args.count):
                filename = f"{record['_id'].replace(':', '_').replace('/', '_')}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                count += 1
                text_len = len(record.get("text", ""))
                total_chars += text_len
                logger.info(f"Saved: {filename} ({text_len} chars)")

            avg_chars = total_chars // count if count > 0 else 0
            logger.info(f"Sample complete: {count} records, avg {avg_chars} chars/doc")

        else:
            # Full bootstrap
            count = 0
            for record in scraper.fetch_all():
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} records")

            logger.info(f"Bootstrap complete: {count} total records")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)

        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info(f"Update: {record['_id']}")

        logger.info(f"Update complete: {count} new records")


if __name__ == "__main__":
    main()
