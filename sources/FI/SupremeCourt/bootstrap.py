#!/usr/bin/env python3
"""
FI/SupremeCourt -- Finnish Supreme Court (Korkein oikeus)

Fetches case law decisions with full text from two sources:
  - LawSampo SPARQL endpoint (pre-2022 cases, ~6000 judgments)
  - Finlex website RSC endpoint (2022+ cases, ~300+ judgments)

Data range: 1980-present

Strategy:
  - Pre-2022: SPARQL query on LawSampo for KKO Judgment records
  - 2022+: Finlex Next.js RSC endpoint for case listing and full text
  - Full text: LawSampo lss:html property or Finlex highlightable spans

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, List
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.SupremeCourt")

# LawSampo SPARQL endpoint
SPARQL_ENDPOINT = "http://ldf.fi/lawsampo/sparql"

# KKO court URI
KKO_COURT_URI = "http://ldf.fi/lawsampo/common_KKO"

# SPARQL query to get KKO judgments with full text
JUDGMENT_QUERY = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX lss: <http://ldf.fi/schema/lawsampo/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?judgment ?label ?ecli ?issued ?number ?finlex_url ?expression ?html_text
WHERE {{
  ?judgment a lss:Judgment .
  ?judgment dcterms:creator <{court}> .
  ?judgment dcterms:isVersionOf ?ecli .

  OPTIONAL {{ ?judgment skos:prefLabel ?label . FILTER(lang(?label) = 'fi') }}
  OPTIONAL {{ ?judgment dcterms:issued ?issued }}
  OPTIONAL {{ ?judgment lss:judgment_number ?number }}
  OPTIONAL {{ ?judgment lss:finlex_url ?finlex_url }}
  OPTIONAL {{
    ?judgment lss:is_realized_by ?expression .
    ?expression lss:html ?html_text .
    FILTER(lang(?html_text) = 'fi')
  }}
}}
ORDER BY DESC(?issued)
LIMIT {limit}
OFFSET {offset}
"""


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for FI/SupremeCourt -- Finnish Supreme Court.
    Country: FI
    URL: https://www.korkeinoikeus.fi

    Data types: case_law
    Auth: none (Open Linked Data)

    Uses LawSampo SPARQL endpoint to fetch KKO decisions with full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    # -- SPARQL helpers --------------------------------------------------------

    def _execute_sparql(self, query: str) -> List[Dict]:
        """
        Execute a SPARQL query and return results.
        """
        self.rate_limiter.wait()

        try:
            resp = self.session.post(
                SPARQL_ENDPOINT,
                data={"query": query},
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()

            bindings = data.get("results", {}).get("bindings", [])
            return bindings

        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            return []

    def _paginate_judgments(
        self,
        court_uri: str = KKO_COURT_URI,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict, None, None]:
        """
        Generator that paginates through all judgments.
        """
        offset = 0
        page = 1
        seen_ids = set()

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping")
                return

            query = JUDGMENT_QUERY.format(
                court=court_uri,
                limit=page_size,
                offset=offset,
            )

            logger.info(f"Fetching page {page} (offset={offset})...")
            results = self._execute_sparql(query)

            if not results:
                logger.info(f"No more results at page {page}")
                return

            unique_in_page = 0
            for binding in results:
                judgment_uri = binding.get("judgment", {}).get("value", "")

                # Skip duplicates (SPARQL can return multiple bindings per judgment)
                if judgment_uri in seen_ids:
                    continue
                seen_ids.add(judgment_uri)
                unique_in_page += 1

                yield binding

            logger.info(f"Page {page}: {unique_in_page} unique judgments")

            if len(results) < page_size:
                logger.info(f"Last page (got {len(results)} < {page_size})")
                return

            offset += page_size
            page += 1

    # -- Text extraction -------------------------------------------------------

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text from HTML content.
        """
        if not html_content:
            return ""

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for element in soup(["script", "style"]):
                element.decompose()

            # Get text
            text = soup.get_text(separator="\n")

            # Clean up
            text = html.unescape(text)
            # Normalize whitespace
            lines = [line.strip() for line in text.split("\n")]
            text = "\n".join(line for line in lines if line)
            # Collapse multiple newlines
            text = re.sub(r'\n{3,}', '\n\n', text)

            return text.strip()

        except Exception as e:
            logger.warning(f"Error extracting text from HTML: {e}")
            return ""

    # -- Finlex RSC methods (2022+ cases) -------------------------------------

    FINLEX_BASE = "https://www.finlex.fi/fi/oikeuskaytanto/korkein-oikeus/ennakkopaatokset"

    def _fetch_finlex_case_ids(self, year: int) -> List[str]:
        """Fetch case numbers for a given year from Finlex RSC listing."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(
                f"{self.FINLEX_BASE}/{year}",
                headers={"RSC": "1"},
                timeout=30,
            )
            content = resp.content.decode("utf-8")
            case_ids = sorted(set(re.findall(rf"KKO:{year}:(\d+)", content)), key=int)
            logger.info(f"Finlex {year}: found {len(case_ids)} cases")
            return case_ids
        except Exception as e:
            logger.error(f"Failed to list Finlex cases for {year}: {e}")
            return []

    def _fetch_finlex_case(self, year: int, number: str) -> Optional[Dict]:
        """Fetch a single case from Finlex RSC and return a raw dict."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(
                f"{self.FINLEX_BASE}/{year}/{number}",
                headers={"RSC": "1"},
                timeout=30,
            )
            content = resp.content.decode("utf-8")

            # Extract highlightable text spans (Finnish content comes first)
            raw_spans = re.findall(
                r'"highlightable","children":"((?:[^"\\]|\\.)*)"', content
            )
            decoded_spans = []
            for s in raw_spans:
                try:
                    decoded_spans.append(json.loads('"' + s + '"'))
                except Exception:
                    decoded_spans.append(s)

            # Extract ECLI
            ecli_match = re.search(r"ECLI:[A-Z:0-9]+", content)
            ecli = ecli_match.group(0) if ecli_match else f"ECLI:FI:KKO:{year}:{number}"

            # Extract date
            date_match = re.search(r'"dateTime":"([^"]+)"', content)
            date_str = date_match.group(1)[:10] if date_match else None

            return {
                "_finlex": True,
                "ecli": ecli,
                "date": date_str,
                "year": year,
                "number": number,
                "spans": decoded_spans,
            }
        except Exception as e:
            logger.error(f"Failed to fetch Finlex KKO:{year}:{number}: {e}")
            return None

    def _paginate_finlex(
        self, start_year: int = 2022, end_year: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """Yield raw case dicts from Finlex for years >= start_year."""
        if end_year is None:
            end_year = datetime.now().year
        for year in range(start_year, end_year + 1):
            case_numbers = self._fetch_finlex_case_ids(year)
            for num in case_numbers:
                case = self._fetch_finlex_case(year, num)
                if case:
                    yield case

    # -- Abstract method implementations ---------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all KKO judgments: LawSampo (pre-2022) + Finlex (2022+).
        """
        # Pre-2022 from LawSampo
        for binding in self._paginate_judgments(page_size=100):
            yield binding
        # 2022+ from Finlex
        for case in self._paginate_finlex(start_year=2022):
            yield case

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield recent judgments. LawSampo recent pages + Finlex current year.
        """
        for binding in self._paginate_judgments(page_size=50, max_pages=5):
            yield binding
        current_year = datetime.now().year
        for case in self._paginate_finlex(start_year=current_year - 1, end_year=current_year):
            yield case

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform SPARQL binding or Finlex RSC data into standard schema.

        CRITICAL: Extracts and includes FULL TEXT.
        """
        # Finlex RSC case (2022+)
        if raw.get("_finlex"):
            return self._normalize_finlex(raw)

        # LawSampo SPARQL binding (pre-2022)
        return self._normalize_lawsampo(raw)

    def _normalize_finlex(self, raw: dict) -> Optional[dict]:
        """Normalize a Finlex RSC case."""
        spans = raw.get("spans", [])
        ecli = raw.get("ecli", "")
        date = raw.get("date")
        year = raw.get("year")
        number = raw.get("number", "")

        # Build full text from spans - take only Finnish text (before Swedish)
        # Swedish section typically starts with a heading containing Swedish words
        fi_spans = []
        for s in spans:
            # Stop at Swedish section (common markers)
            if re.match(r"^(Högsta domstolen|Tingsrätten|Hovrätten|Besvärst)", s):
                break
            fi_spans.append(s)

        full_text = "\n".join(fi_spans)

        if len(full_text) < 200:
            logger.warning(f"Text too short for {ecli}: {len(full_text)} chars")
            return None

        doc_id = ecli.replace(":", "_") if ecli else f"KKO_{year}_{number}"
        url = f"https://www.finlex.fi/fi/oikeuskaytanto/korkein-oikeus/ennakkopaatokset/{year}/{number}"
        title = f"KKO:{year}:{number}"

        return {
            "_id": doc_id,
            "_source": "FI/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date,
            "url": url,
            "ecli": ecli,
            "judgment_number": number,
            "year": year,
            "judgment_uri": None,
            "finlex_url": url,
            "court": "Korkein oikeus",
            "court_en": "Supreme Court",
            "language": "fi",
        }

    def _normalize_lawsampo(self, raw: dict) -> Optional[dict]:
        """Normalize a LawSampo SPARQL binding."""
        judgment_uri = raw.get("judgment", {}).get("value", "")
        label = raw.get("label", {}).get("value", "")
        ecli = raw.get("ecli", {}).get("value", "")
        issued = raw.get("issued", {}).get("value", "")
        number = raw.get("number", {}).get("value", "")
        finlex_url = raw.get("finlex_url", {}).get("value", "")
        html_text = raw.get("html_text", {}).get("value", "")

        if not html_text:
            logger.warning(f"No HTML text for {ecli}")
            return None

        full_text = self._extract_text_from_html(html_text)

        if len(full_text) < 200:
            logger.warning(f"Text too short for {ecli}: {len(full_text)} chars")
            return None

        doc_id = ecli.replace(":", "_") if ecli else judgment_uri.split("/")[-1]

        date = None
        if issued:
            date = issued[:10]

        year = None
        if ecli:
            match = re.search(r':(\d{4}):', ecli)
            if match:
                year = int(match.group(1))
        elif date:
            year = int(date[:4])

        url = finlex_url or "https://www.korkeinoikeus.fi/fi/index/ennakkopaatokset.html"

        return {
            "_id": doc_id,
            "_source": "FI/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": label or f"KKO {number}" if number else ecli,
            "text": full_text,
            "date": date,
            "url": url,
            "ecli": ecli,
            "judgment_number": number,
            "year": year,
            "judgment_uri": judgment_uri,
            "finlex_url": finlex_url,
            "court": "Korkein oikeus",
            "court_en": "Supreme Court",
            "language": "fi",
        }

    # -- Custom commands -------------------------------------------------------

    def test_api(self):
        """Quick SPARQL connectivity test."""
        print("Testing LawSampo SPARQL endpoint...")

        # Test basic query
        test_query = """
PREFIX lss: <http://ldf.fi/schema/lawsampo/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT (COUNT(?j) as ?count)
WHERE {
  ?j a lss:Judgment .
  ?j dcterms:creator <http://ldf.fi/lawsampo/common_KKO> .
}
"""
        results = self._execute_sparql(test_query)
        if results:
            count = results[0].get("count", {}).get("value", "0")
            print(f"  KKO judgments in LawSampo: {count}")
        else:
            print("  FAILED: Could not query endpoint")
            return

        # Test fetching one judgment with full text
        print("\nFetching sample judgment with full text...")
        sample_query = JUDGMENT_QUERY.format(
            court=KKO_COURT_URI,
            limit=1,
            offset=0,
        )
        results = self._execute_sparql(sample_query)
        if results:
            binding = results[0]
            ecli = binding.get("ecli", {}).get("value", "")
            html_text = binding.get("html_text", {}).get("value", "")
            text = self._extract_text_from_html(html_text)
            print(f"  ECLI: {ecli}")
            print(f"  Text length: {len(text)} chars")
            if text:
                print(f"  Preview: {text[:200]}...")
        else:
            print("  FAILED: No sample judgment found")
            return

        print("\nAPI test passed!")

    def run_sample(self, n: int = 12) -> dict:
        """
        Fetch a sample of judgments with full text from both sources.
        """
        import itertools

        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []
        text_lengths = []

        # Mix LawSampo (pre-2022) and Finlex (2022+) sources
        lawsampo_gen = self._paginate_judgments(page_size=50, max_pages=1)
        current_year = datetime.now().year
        finlex_gen = self._paginate_finlex(start_year=current_year - 1, end_year=current_year)

        # Take 6 from each source for a balanced sample
        half = n // 2
        sources = itertools.chain(
            itertools.islice(lawsampo_gen, half + 5),
            itertools.islice(finlex_gen, half + 5),
        )

        for raw in sources:
            if saved >= n:
                break

            checked += 1
            ecli_val = raw.get("ecli", raw.get("ecli", {}).get("value", "") if isinstance(raw.get("ecli"), dict) else "")

            try:
                normalized = self.normalize(raw)

                if not normalized:
                    errors.append(f"{ecli_val}: Normalization returned None")
                    continue

                if not normalized.get("text"):
                    errors.append(f"{ecli_val}: No text content")
                    continue

                text_len = len(normalized.get("text", ""))
                if text_len < 500:
                    errors.append(f"{ecli_val}: Text too short ({text_len} chars)")
                    continue

                safe_name = re.sub(r'[^\w\-]', '_', normalized["_id"])
                sample_path = sample_dir / f"{safe_name}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                saved += 1
                text_lengths.append(text_len)
                logger.info(f"  Saved {normalized.get('ecli', '')}: {text_len} chars (year={normalized.get('year')})")

            except Exception as e:
                errors.append(f"{ecli_val}: {str(e)}")
                logger.error(f"Error processing {ecli_val}: {e}")

        stats = {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "errors": errors[:10],
            "avg_text_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
        }

        return stats


# -- CLI Entry Point -----------------------------------------------------------


def main():
    scraper = SupremeCourtScraper()

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
            print(json.dumps(stats, indent=2))
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
