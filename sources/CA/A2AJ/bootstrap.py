#!/usr/bin/env python3
"""
CA/A2AJ -- A2AJ Canadian Legal Data Fetcher

Fetches Canadian case law from the A2AJ API (York University).
191K+ decisions from 14 courts/tribunals. Free API, no auth.

Strategy:
  - Bootstrap: For each dataset (court), search by date windows using
    sort_results=oldest_first. Narrow windows when hitting 50-result cap.
    Then fetch full text for each citation found.
  - Sample: Fetches 2 recent decisions per priority court for validation.

API: https://api.a2aj.ca
Docs: https://api.a2aj.ca/docs

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.A2AJ")

API_BASE = "https://api.a2aj.ca"

# Priority courts for sample mode (high-value, diverse coverage)
SAMPLE_COURTS = ["SCC", "FC", "FCA", "ONCA", "BCSC", "TCC"]


class A2AJScraper(BaseScraper):
    """
    Scraper for CA/A2AJ -- A2AJ Canadian Legal Data.
    Country: CA
    URL: https://a2aj.ca/canadian-legal-data/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_coverage(self, doc_type="cases"):
        """Get available datasets and document counts."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/coverage", params={"doc_type": doc_type})
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", []) if isinstance(data, dict) else data
        except Exception as e:
            logger.error(f"Failed to get coverage: {e}")
            return []

    def _search(self, query="*", dataset=None, start_date=None, end_date=None,
                sort="oldest_first", size=50, doc_type="cases"):
        """Search for documents. Returns list of result dicts (max 50)."""
        params = {
            "query": query,
            "sort_results": sort,
            "size": str(size),
            "doc_type": doc_type,
        }
        if dataset:
            params["dataset"] = dataset
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        self.rate_limiter.wait()
        try:
            resp = self.client.get("/search", params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", []) if isinstance(data, dict) else data
        except Exception as e:
            logger.error(f"Search failed (dataset={dataset}, {start_date}-{end_date}): {e}")
            return []

    def _fetch_document(self, citation, doc_type="cases"):
        """Fetch full text for a single document by citation."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/fetch", params={
                "citation": citation,
                "doc_type": doc_type,
                "output_language": "en",
            })
            resp.raise_for_status()
            data = resp.json()
            # API wraps results in {"results": [...]}
            if isinstance(data, dict) and "results" in data:
                results = data["results"]
                return results[0] if results else None
            elif isinstance(data, list) and data:
                return data[0]
            elif isinstance(data, dict):
                return data
            return None
        except Exception as e:
            logger.error(f"Fetch failed for citation '{citation}': {e}")
            return None

    def _search_date_window(self, dataset, start_date, end_date):
        """
        Search a date window for a dataset. If results hit 50 (the cap),
        split the window in half and recurse.
        Yields (citation, metadata) tuples.
        """
        results = self._search(
            query="*",
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            sort="oldest_first",
            size=50,
        )

        if not results:
            return

        # If we hit the 50-result cap, split the date window
        if len(results) >= 50:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            delta = (end_dt - start_dt).days

            if delta <= 0:
                # Can't split further; yield what we have
                for r in results:
                    citation = r.get("citation_en") or r.get("citation_fr", "")
                    if citation:
                        yield citation, r
                return

            mid_dt = start_dt + timedelta(days=delta // 2)
            mid_date = mid_dt.strftime("%Y-%m-%d")

            # First half
            yield from self._search_date_window(dataset, start_date, mid_date)
            # Second half (day after mid to avoid overlap)
            next_day = (mid_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            yield from self._search_date_window(dataset, next_day, end_date)
        else:
            for r in results:
                citation = r.get("citation_en") or r.get("citation_fr", "")
                if citation:
                    yield citation, r

    # -- BaseScraper interface ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all case law documents from all courts."""
        coverage = self._get_coverage("cases")
        if not coverage:
            logger.error("Could not get coverage data")
            return

        for court in coverage:
            dataset = court.get("dataset", "")
            doc_count = court.get("number_of_documents", 0)
            earliest = court.get("earliest_document_date", "2000-01-01")
            latest = court.get("latest_document_date", "2026-12-31")

            # Parse dates (handle datetime format)
            earliest = earliest[:10] if earliest else "1900-01-01"
            latest = latest[:10] if latest else "2026-12-31"

            logger.info(f"Processing {dataset}: {doc_count} documents ({earliest} to {latest})")

            seen_citations = set()
            fetched = 0

            for citation, meta in self._search_date_window(dataset, earliest, latest):
                if citation in seen_citations:
                    continue
                seen_citations.add(citation)

                # Fetch full text
                doc = self._fetch_document(citation)
                if doc and doc.get("unofficial_text_en"):
                    doc["_dataset"] = dataset
                    yield doc
                    fetched += 1
                    if fetched % 100 == 0:
                        logger.info(f"  {dataset}: {fetched} documents fetched")

            logger.info(f"  {dataset}: {fetched} documents fetched (done)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents added/modified since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        today_str = datetime.now().strftime("%Y-%m-%d")

        coverage = self._get_coverage("cases")
        for court in coverage:
            dataset = court.get("dataset", "")
            for citation, meta in self._search_date_window(dataset, since_str, today_str):
                doc = self._fetch_document(citation)
                if doc and doc.get("unofficial_text_en"):
                    doc["_dataset"] = dataset
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw A2AJ API response into standard schema."""
        citation = raw.get("citation_en") or raw.get("citation_fr", "")
        text = raw.get("unofficial_text_en") or raw.get("unofficial_text_fr", "")

        # Parse date
        date_str = raw.get("document_date_en") or raw.get("document_date_fr", "")
        if date_str:
            # Handle ISO datetime like "2020-02-28T00:00:00"
            date_str = date_str[:10]  # Just the date part
        else:
            date_str = None

        # Clean text - remove any stray HTML if present
        if text:
            text = re.sub(r'<[^>]+>', '', text)
            text = text.strip()

        # Extract court from dataset or citation
        dataset = raw.get("_dataset", "")
        if not dataset and citation:
            # Try to extract from citation like "2020 SCC 5"
            parts = citation.split()
            if len(parts) >= 2:
                dataset = parts[1]

        return {
            "_id": citation,
            "_source": "CA/A2AJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("name_en") or raw.get("name_fr", ""),
            "text": text,
            "date": date_str,
            "url": raw.get("url_en") or raw.get("url_fr", ""),
            "citation": citation,
            "citation_official": raw.get("citation2_en") or raw.get("citation2_fr", ""),
            "court": dataset,
            "court_description": "",
            "language": "en",
            "license": raw.get("upstream_license", ""),
            "summary": raw.get("summary", ""),
            "key_holding": raw.get("key_holding", ""),
        }

    # -- Sample mode --------------------------------------------------------

    def _fetch_sample(self) -> list:
        """Fetch sample records from priority courts for validation."""
        samples = []

        for court in SAMPLE_COURTS:
            logger.info(f"Fetching samples from {court}...")
            results = self._search(
                query="*",
                dataset=court,
                sort="newest_first",
                size=3,
            )

            if not results:
                logger.warning(f"No results from {court}")
                continue

            for r in results[:2]:
                citation = r.get("citation_en") or r.get("citation_fr", "")
                if not citation:
                    continue

                doc = self._fetch_document(citation)
                if doc:
                    doc["_dataset"] = court
                    normalized = self.normalize(doc)
                    if normalized.get("text"):
                        samples.append(normalized)
                        logger.info(
                            f"  {citation}: {len(normalized['text'])} chars"
                        )
                    else:
                        logger.warning(f"  {citation}: no full text")

            if len(samples) >= 12:
                break

        return samples


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CA/A2AJ data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode: fetch small set for validation")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = A2AJScraper()

    if args.command == "test-api":
        print("Testing A2AJ API connectivity...")
        coverage = scraper._get_coverage()
        if coverage:
            print(f"OK: {len(coverage)} datasets available")
            for c in coverage:
                print(f"  {c['dataset']}: {c['number_of_documents']} docs "
                      f"({c['earliest_document_date'][:10]} to {c['latest_document_date'][:10]})")
        else:
            print("FAIL: Could not reach API")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Running sample mode...")
            samples = scraper._fetch_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                fname = sample_dir / f"sample_{i+1:03d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to sample/")
            if samples:
                texts = [s["text"] for s in samples if s.get("text")]
                avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
                print(f"Average text length: {avg_len} chars")
                print(f"Courts covered: {set(s['court'] for s in samples)}")
                # Validate
                for s in samples:
                    assert s.get("text"), f"Missing text: {s['_id']}"
                    assert s.get("title"), f"Missing title: {s['_id']}"
                    assert s.get("date"), f"Missing date: {s['_id']}"
                print("All validation checks passed!")
            return

        # Full bootstrap
        result = scraper.bootstrap()
        print(f"Bootstrap complete: {result}")

    elif args.command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")


if __name__ == "__main__":
    main()
