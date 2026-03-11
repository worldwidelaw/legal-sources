#!/usr/bin/env python3
"""
PT/SupremeCourt -- Portuguese Supreme Court Case Law Fetcher

Fetches Supreme Court of Justice (Supremo Tribunal de Justiça - STJ) decisions
from the official jurisprudence search portal at juris.stj.pt.

Strategy:
  - Search API: https://juris.stj.pt/api/search returns list of decisions
  - Document API: https://juris.stj.pt/{processo}/{uuid_prefix} returns full text
  - Full text is available via Next.js SSR in the "Texto" field
  - ECLI identifiers are available for some decisions

Endpoints:
  - Search: https://juris.stj.pt/api/search?MinAno=YYYY&MaxAno=YYYY&mustHaveText=true
  - Document: https://juris.stj.pt/{processo_number}/{uuid_prefix}

Data:
  - Case types: Criminal appeals, civil appeals, extradition, habeas corpus, etc.
  - Coverage: 1900 to present (~71,500 decisions)
  - License: Public (open government data)
  - Full text: HTML content with legal arguments, facts, and decision

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import urllib3
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

# Suppress SSL warnings for PT/SupremeCourt (server has incomplete cert chain)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.supremecourt")

# Base URL for the STJ jurisprudence portal
BASE_URL = "https://juris.stj.pt"

# API endpoints
SEARCH_API = "/api/search"


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for PT/SupremeCourt -- Portuguese Supreme Court of Justice.
    Country: PT
    URL: https://juris.stj.pt

    Data types: case_law
    Auth: none (Public government data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Note: verify=False is used because juris.stj.pt has an incomplete SSL
        # certificate chain (missing Let's Encrypt E7 intermediate). The server
        # only sends the leaf certificate, causing verification failures on some
        # systems. This is a server-side configuration issue.
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json,text/html,application/xhtml+xml,*/*",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
            },
            timeout=60,
            verify=False,
        )

    def _search_decisions(
        self, year: int, page: int = 0, must_have_text: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Search for decisions in a specific year.

        Returns list of decision metadata (without full text).
        """
        params = {
            "MinAno": year,
            "MaxAno": year,
        }
        if must_have_text:
            params["mustHaveText"] = "true"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(SEARCH_API, params=params)

            if resp.status_code != 200:
                logger.warning(f"Search failed for year {year}: HTTP {resp.status_code}")
                return []

            results = resp.json()
            return results if isinstance(results, list) else []

        except Exception as e:
            logger.warning(f"Error searching year {year}: {e}")
            return []

    def _fetch_document(
        self, processo: str, uuid: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch full document details including the complete decision text.

        The document is accessed via Next.js SSR and the data is in __NEXT_DATA__.
        """
        # Build URL: /{processo}/{uuid_prefix}
        processo_encoded = quote(processo, safe="")
        uuid_prefix = uuid[:6]  # First 6 chars of UUID

        url = f"/{processo_encoded}/{uuid_prefix}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.debug(f"Document fetch failed: {url} -> HTTP {resp.status_code}")
                return None

            html_content = resp.content.decode("utf-8", errors="replace")

            # Extract __NEXT_DATA__ JSON
            match = re.search(
                r'__NEXT_DATA__[^>]*>(.*?)</script>',
                html_content,
                re.DOTALL
            )

            if not match:
                logger.warning(f"No __NEXT_DATA__ found for {processo}")
                return None

            try:
                next_data = json.loads(match.group(1))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse __NEXT_DATA__ for {processo}: {e}")
                return None

            # Extract document from pageProps
            page_props = next_data.get("props", {}).get("pageProps", {})
            doc = page_props.get("doc")

            if not doc:
                logger.debug(f"No doc in pageProps for {processo}")
                return None

            # Verify we have full text
            texto = doc.get("Texto", "")
            if not texto:
                logger.debug(f"No Texto field for {processo}")
                return None

            return doc

        except Exception as e:
            logger.warning(f"Error fetching document {processo}: {e}")
            return None

    def _clean_html(self, html_text: str) -> str:
        """Strip HTML tags and clean text."""
        if not html_text:
            return ""

        # Remove style and script tags
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert br/p/div to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)

        # Strip lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions from the Portuguese Supreme Court.

        Iterates through years from newest to oldest.
        """
        current_year = datetime.now().year

        for year in range(current_year, 1900, -1):
            logger.info(f"Processing year {year}...")

            results = self._search_decisions(year, must_have_text=True)
            if not results:
                logger.info(f"No results for year {year}")
                continue

            logger.info(f"Found {len(results)} decisions for year {year}")

            for item in results:
                source = item.get("_source", {})
                processo = source.get("Número de Processo", "")
                uuid = source.get("UUID", "")

                if not processo or not uuid:
                    continue

                # Fetch full document
                doc = self._fetch_document(processo, uuid)
                if doc:
                    yield {
                        "raw_doc": doc,
                        "search_meta": source,
                    }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield recent decisions.

        Fetches decisions from the current year and previous year.
        """
        current_year = datetime.now().year
        years_to_check = [current_year, current_year - 1]

        for year in years_to_check:
            logger.info(f"Checking year {year} for updates...")

            results = self._search_decisions(year, must_have_text=True)
            if not results:
                continue

            for item in results:
                source = item.get("_source", {})
                processo = source.get("Número de Processo", "")
                uuid = source.get("UUID", "")

                if not processo or not uuid:
                    continue

                doc = self._fetch_document(processo, uuid)
                if doc:
                    yield {
                        "raw_doc": doc,
                        "search_meta": source,
                    }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc = raw.get("raw_doc", {})
        search_meta = raw.get("search_meta", {})

        # Get identifiers
        processo = doc.get("Número de Processo", search_meta.get("Número de Processo", ""))
        uuid = doc.get("UUID", search_meta.get("UUID", ""))

        # Build document ID
        doc_id = f"STJ-{processo.replace('/', '-')}" if processo else f"STJ-{uuid[:12]}"

        # Extract date (format: DD/MM/YYYY)
        date_str = doc.get("Data", "")
        iso_date = None
        if date_str:
            try:
                parts = date_str.split("/")
                if len(parts) == 3:
                    iso_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                pass

        # Get and clean full text
        texto_html = doc.get("Texto", "")
        full_text = self._clean_html(texto_html)

        # Get summary
        sumario_html = doc.get("Sumário", "")
        sumario = self._clean_html(sumario_html)

        # Build title
        area = doc.get("Área", {})
        area_name = ""
        if isinstance(area, dict):
            area_name = area.get("Show", [""])[0] if area.get("Show") else ""
        elif isinstance(area, str):
            area_name = area

        title = f"Acórdão STJ {processo}"
        if area_name:
            title = f"{title} ({area_name})"

        # Extract section
        seccao = doc.get("Secção", {})
        section_name = ""
        if isinstance(seccao, dict):
            section_name = seccao.get("Show", [""])[0] if seccao.get("Show") else ""
        elif isinstance(seccao, str):
            section_name = seccao

        # Extract rapporteur
        relator = doc.get("Relator Nome Profissional", {})
        rapporteur = ""
        if isinstance(relator, dict):
            rapporteur = relator.get("Show", [""])[0] if relator.get("Show") else ""
        elif isinstance(relator, str):
            rapporteur = relator

        # Extract decision type
        meio = doc.get("Meio Processual", {})
        procedural_type = ""
        if isinstance(meio, dict):
            meio_list = meio.get("Show", [])
            procedural_type = " / ".join([m for m in meio_list if m != "/"])
        elif isinstance(meio, str):
            procedural_type = meio

        # Extract decision outcome
        decisao = doc.get("Decisão", {})
        outcome = ""
        if isinstance(decisao, dict):
            outcome = decisao.get("Show", [""])[0] if decisao.get("Show") else ""
        elif isinstance(decisao, str):
            outcome = decisao

        # Extract descriptors/keywords
        descritores = doc.get("Descritores", {})
        keywords = []
        if isinstance(descritores, dict):
            keywords = descritores.get("Show", [])
        elif isinstance(descritores, list):
            keywords = descritores

        # Build URL
        url = doc.get("URL", f"{BASE_URL}/{quote(processo, safe='')}/{uuid[:6]}" if processo else "")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "PT/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "summary": sumario,
            "date": iso_date,
            "url": url,
            # Case law specific fields
            "case_number": processo,
            "ecli": doc.get("ECLI", ""),
            "rapporteur": rapporteur,
            "section": section_name,
            "area": area_name,
            "procedural_type": procedural_type,
            "outcome": outcome,
            "keywords": keywords,
            # Voting info
            "voting": doc.get("Votação", {}).get("Show", [""])[0] if isinstance(doc.get("Votação"), dict) else "",
            # Source info
            "court": "Supremo Tribunal de Justiça",
            "jurisdiction": "PT",
            "language": "pt",
            "uuid": uuid,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Portuguese Supreme Court (STJ) endpoints...")

        # Test 1: Search API
        print("\n1. Testing search API...")
        current_year = datetime.now().year
        results = self._search_decisions(current_year, must_have_text=True)
        print(f"   Found {len(results)} decisions for {current_year}")

        if results:
            sample = results[0].get("_source", {})
            print(f"   Sample processo: {sample.get('Número de Processo')}")
            print(f"   Sample UUID: {sample.get('UUID', '')[:20]}...")

        # Test 2: Document fetch with full text
        print("\n2. Testing document fetch (full text)...")
        if results:
            sample = results[0].get("_source", {})
            processo = sample.get("Número de Processo", "")
            uuid = sample.get("UUID", "")

            if processo and uuid:
                doc = self._fetch_document(processo, uuid)
                if doc:
                    texto = doc.get("Texto", "")
                    sumario = doc.get("Sumário", "")
                    print(f"   Processo: {processo}")
                    print(f"   Full text length: {len(texto)} characters")
                    print(f"   Summary length: {len(sumario)} characters")

                    # Clean and show sample
                    clean_text = self._clean_html(texto)
                    print(f"   Cleaned text length: {len(clean_text)} characters")
                    print(f"   Sample text: {clean_text[:400]}...")

                    # Show metadata
                    print(f"   Date: {doc.get('Data')}")
                    area = doc.get('Área', {})
                    if isinstance(area, dict):
                        print(f"   Area: {area.get('Show', [''])[0]}")
                else:
                    print("   Failed to fetch document")
            else:
                print("   No processo/UUID in search results")
        else:
            print("   No results to test document fetch")

        # Test 3: Historical decision
        print("\n3. Testing historical decisions (2020)...")
        results_2020 = self._search_decisions(2020, must_have_text=True)
        print(f"   Found {len(results_2020)} decisions for 2020")

        if results_2020:
            sample = results_2020[0].get("_source", {})
            processo = sample.get("Número de Processo", "")
            uuid = sample.get("UUID", "")
            if processo and uuid:
                doc = self._fetch_document(processo, uuid)
                if doc:
                    texto = doc.get("Texto", "")
                    print(f"   Processo: {processo}")
                    print(f"   Full text length: {len(texto)} characters")

        print("\nTest complete!")


def main():
    scraper = SupremeCourtScraper()

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
