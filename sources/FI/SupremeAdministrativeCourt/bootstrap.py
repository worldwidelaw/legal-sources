#!/usr/bin/env python3
"""
FI/SupremeAdministrativeCourt -- Finnish Supreme Administrative Court (Korkein hallinto-oikeus)

Fetches KHO case law with full text from two complementary lanes.

  1. Finlex (https://www.finlex.fi) -- PRIMARY, live.
     Official Ministry of Justice service. Covers ennakkopaatokset (1944-),
     lyhyet-ratkaisuselosteet (1946-2021) and muut-paatokset (2013-), including
     everything published after LawSampo froze. See finlex_web.py for why the
     page body has to be read out of the Next.js RSC payload.

  2. LawSampo SPARQL (http://ldf.fi/lawsampo/sparql) -- SECONDARY, historical.
     A frozen research snapshot: 10,114 KHO judgments, MAX(dateIssued) =
     2021-08-04. Kept because it carries clean pre-2021 full text, but it can
     never yield anything newer (issue #1503).

Records from both lanes key on the ECLI identifier, so the overlap dedupes.

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py bootstrap-fast      # Concurrent full pull (fleet entry point)
  python bootstrap.py update              # Incremental update (newest-first)
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

sys.path.insert(0, str(Path(__file__).parent))
import finlex_web  # noqa: E402  (local module, needs the path insert above)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.SupremeAdministrativeCourt")

# LawSampo SPARQL endpoint
SPARQL_ENDPOINT = "http://ldf.fi/lawsampo/sparql"

# KHO court URI
KHO_COURT_URI = "http://ldf.fi/lawsampo/common_KHO"

# SPARQL query to get KHO judgments with full text
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


class SupremeAdministrativeCourtScraper(BaseScraper):
    """
    Scraper for FI/SupremeAdministrativeCourt -- Finnish Supreme Administrative Court.
    Country: FI
    URL: https://www.kho.fi

    Data types: case_law
    Auth: none (Open Linked Data)

    Uses LawSampo SPARQL endpoint to fetch KHO decisions with full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

        # finlex.fi is a Next.js app that only serves the RSC payload to a
        # browser-shaped request; the SPARQL UA above gets a stripped shell.
        self.web_session = requests.Session()
        self.web_session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fi,en;q=0.8",
        })

        self._years_cache: Optional[Dict[str, List[int]]] = None

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
        court_uri: str = KHO_COURT_URI,
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

    # -- Abstract method implementations ---------------------------------------

    # -- Finlex web lane -------------------------------------------------------

    def _get_html(self, url: str) -> str:
        """Fetch a finlex.fi page. Returns "" on failure."""
        self.rate_limiter.wait()
        try:
            resp = self.web_session.get(url, timeout=60)
            if resp.status_code == 404:
                return ""
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Fetch failed for {url}: {e}")
            return ""

    def _discover_years(self) -> Dict[str, List[int]]:
        """
        Map each KHO collection to the years Finlex publishes for it.

        Cached on the instance so bootstrap_fast's worker threads don't each
        re-request the collection index pages.
        """
        if self._years_cache is not None:
            return self._years_cache

        years_by_collection: Dict[str, List[int]] = {}
        for collection in finlex_web.COLLECTIONS:
            html_text = self._get_html(finlex_web.collection_url(collection))
            years = finlex_web.parse_years(html_text, collection)
            if years:
                years_by_collection[collection] = years
                logger.info(
                    f"{collection}: {len(years)} years ({min(years)}-{max(years)})"
                )
            else:
                logger.warning(f"{collection}: no years discovered")

        if not years_by_collection:
            raise RuntimeError(
                "Finlex year discovery returned nothing for all three KHO "
                "collections -- the site layout changed or the host is blocking us."
            )

        self._years_cache = years_by_collection
        return years_by_collection

    def _iter_finlex_stubs(
        self, min_year: Optional[int] = None
    ) -> Generator[dict, None, None]:
        """
        Yield lightweight `{lane, collection, year, number}` stubs, newest first.

        The per-decision page fetch deliberately happens in normalize() so that
        bootstrap_fast's worker pool actually parallelises it -- base_scraper
        only threads normalize(), not fetch_all().
        """
        years_by_collection = self._discover_years()

        if min_year is not None:
            years_by_collection = {
                collection: [y for y in years if y >= min_year]
                for collection, years in years_by_collection.items()
            }

        for stub in finlex_web.iter_stubs(years_by_collection):
            collection, year = stub["collection"], stub["year"]
            html_text = self._get_html(finlex_web.collection_url(collection, year))
            numbers = finlex_web.parse_decision_numbers(html_text, collection, year)
            if not numbers:
                logger.info(f"{collection}/{year}: 0 decisions")
                continue
            logger.info(f"{collection}/{year}: {len(numbers)} decisions")
            for number in numbers:
                yield {
                    "lane": "finlex",
                    "collection": collection,
                    "year": year,
                    "number": number,
                }

    def _normalize_finlex(self, raw: dict) -> Optional[dict]:
        """Fetch and parse one finlex.fi decision page into the standard schema."""
        collection = raw["collection"]
        year = raw["year"]
        number = raw["number"]
        url = finlex_web.decision_url(collection, year, number)

        html_text = self._get_html(url)
        if not html_text:
            return None

        text = finlex_web.extract_decision_text(html_text)
        if len(text) < 200:
            logger.warning(f"Text too short for {collection}/{year}/{number}: {len(text)}")
            return None

        meta = finlex_web.parse_metadata(text)
        ecli = meta.get("ecli") or ""

        if not ecli and collection == "ennakkopaatokset":
            # Pages from the pre-ECLI era don't render an ECLI, but LawSampo
            # assigns yearbook decisions ECLI:FI:KHO:{year}:{number} all the way
            # back to 1944. Synthesising it keeps both lanes on one dedup key.
            ecli = f"ECLI:FI:KHO:{year}:{number}"

        doc_id = ecli.replace(":", "_") if ecli else f"KHO_{collection}_{year}_{number}"

        # The first rendered line is the decision's own label, e.g. "KHO:2026:1".
        first_line = text.split("\n", 1)[0].strip()
        label = first_line if len(first_line) <= 120 else ""
        title = label or (ecli or f"KHO {year}/{number}")
        keywords = meta.get("keywords") or []
        if keywords:
            title = f"{title} - {keywords[0]}"

        date = meta.get("date")
        if not date:
            # Fall back to the collection year rather than inventing a month/day.
            date = None

        return {
            "_id": doc_id,
            "_source": "FI/SupremeAdministrativeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "ecli": ecli,
            "judgment_number": label,
            "year": year,
            "diary_number": meta.get("diary_number"),
            "archival_record": meta.get("archival_record"),
            "keywords": keywords,
            "collection": collection,
            "decision_type": finlex_web.COLLECTION_LABELS.get(collection, ""),
            "court": "Korkein hallinto-oikeus",
            "court_en": "Supreme Administrative Court",
            "language": "fi",
            "provenance": "finlex.fi",
        }

    # -- Abstract method implementations ---------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield every KHO decision, Finlex first (live, all years) then LawSampo.

        LawSampo runs second so that its frozen pre-2021 snapshot only fills
        gaps; identical ECLIs dedupe against the Finlex records already written.
        """
        yielded_finlex = 0
        for stub in self._iter_finlex_stubs():
            yielded_finlex += 1
            yield stub
        logger.info(f"Finlex lane: {yielded_finlex} decisions queued")

        for binding in self._paginate_judgments(page_size=100):
            binding["lane"] = {"value": "lawsampo"}
            yield binding

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions published on or after `since`, newest first.

        LawSampo is deliberately excluded: it stopped at 2021-08-04, so
        including it would re-crawl 10K frozen rows on every refresh and write
        nothing (the failure reported in issue #1503).
        """
        min_year = since.year if since else None
        for stub in self._iter_finlex_stubs(min_year=min_year):
            yield stub

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform a stub or SPARQL binding into the standard schema.

        CRITICAL: Both lanes emit full document text.
        """
        if raw.get("lane") == "finlex":
            return self._normalize_finlex(raw)

        # Extract values from SPARQL binding
        judgment_uri = raw.get("judgment", {}).get("value", "")
        label = raw.get("label", {}).get("value", "")
        ecli = raw.get("ecli", {}).get("value", "")
        issued = raw.get("issued", {}).get("value", "")
        number = raw.get("number", {}).get("value", "")
        finlex_url = raw.get("finlex_url", {}).get("value", "")
        html_text = raw.get("html_text", {}).get("value", "")

        # Skip if no full text
        if not html_text:
            logger.warning(f"No HTML text for {ecli}")
            return None

        # Extract clean text from HTML
        full_text = self._extract_text_from_html(html_text)

        if len(full_text) < 200:
            logger.warning(f"Text too short for {ecli}: {len(full_text)} chars")
            return None

        # Build ID from ECLI
        doc_id = ecli.replace(":", "_") if ecli else judgment_uri.split("/")[-1]

        # Parse date
        date = None
        if issued:
            date = issued[:10]  # ISO format YYYY-MM-DD

        # Extract year from ECLI or date
        year = None
        if ecli:
            match = re.search(r':(\d{4}):', ecli)
            if match:
                year = int(match.group(1))
        elif date:
            year = int(date[:4])

        # URL - prefer finlex_url, fall back to KHO website
        url = finlex_url or f"https://www.kho.fi/fi/index/paatokset.html"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "FI/SupremeAdministrativeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": label or f"KHO {number}" if number else ecli,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Case-specific metadata
            "ecli": ecli,
            "judgment_number": number,
            "year": year,
            "judgment_uri": judgment_uri,
            "finlex_url": finlex_url,
            "court": "Korkein hallinto-oikeus",
            "court_en": "Supreme Administrative Court",
            "language": "fi",
            "provenance": "lawsampo",
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
  ?j dcterms:creator <http://ldf.fi/lawsampo/common_KHO> .
}
"""
        results = self._execute_sparql(test_query)
        if results:
            count = results[0].get("count", {}).get("value", "0")
            print(f"  KHO judgments in LawSampo: {count}")
        else:
            print("  FAILED: Could not query endpoint")
            return

        # Test fetching one judgment with full text
        print("\nFetching sample judgment with full text...")
        sample_query = JUDGMENT_QUERY.format(
            court=KHO_COURT_URI,
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
        Fetch a sample of judgments with full text.
        """
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []
        text_lengths = []

        # Sample from the live Finlex lane so the check exercises the path that
        # actually covers the post-2021 years.
        for stub in self._iter_finlex_stubs():
            if saved >= n:
                break

            checked += 1
            ecli = f"{stub['collection']}/{stub['year']}/{stub['number']}"

            try:
                normalized = self.normalize(stub)

                if not normalized:
                    errors.append(f"{ecli}: Normalization returned None")
                    continue

                if not normalized.get("text"):
                    errors.append(f"{ecli}: No text content")
                    continue

                text_len = len(normalized.get("text", ""))
                if text_len < 500:
                    errors.append(f"{ecli}: Text too short ({text_len} chars)")
                    continue

                # Save to sample directory
                safe_name = re.sub(r'[^\w\-]', '_', normalized["_id"])
                sample_path = sample_dir / f"{safe_name}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                saved += 1
                text_lengths.append(text_len)
                logger.info(f"  Saved {ecli}: {text_len} chars")

            except Exception as e:
                errors.append(f"{ecli}: {str(e)}")
                logger.error(f"Error processing {ecli}: {e}")

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
    scraper = SupremeAdministrativeCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
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

    elif command in ("bootstrap-fast", "bootstrap_fast"):
        stats = scraper.bootstrap_fast()
        print(
            f"\nbootstrap_fast complete: {stats.get('records_fetched', 0)} fetched, "
            f"{stats.get('records_new', 0)} new, "
            f"{stats.get('records_updated', 0)} updated"
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
