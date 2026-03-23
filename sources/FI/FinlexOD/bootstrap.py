#!/usr/bin/env python3
"""
FI/FinlexOD -- Finland Finlex Open Data Fetcher

Fetches Finnish legislation via REST API and case law via SPARQL endpoint.

Strategy:
  - Legislation: Paginate REST API listing, fetch Akoma Ntoso XML per doc
  - Case law: SPARQL queries to Semantic Finlex for KKO/KHO full text
  - Parse XML with regex for robustness (namespaces make ET fragile)

Usage:
  python bootstrap.py bootstrap          # Fetch legislation + case law
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.FinlexOD")

API_BASE = "https://opendata.finlex.fi/finlex/avoindata/v1"
SPARQL_URL = "http://ldf.fi/finlex/sparql"


class FinlexODScraper(BaseScraper):
    """Scraper for FI/FinlexOD -- Finnish legislation and case law."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json, application/xml, text/xml",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with retry."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 10s")
                    time.sleep(10)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _sparql_query(self, query: str, timeout: int = 90) -> List[Dict]:
        """Execute SPARQL query and return bindings."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(
                    SPARQL_URL,
                    params={"query": query},
                    headers={"Accept": "application/json"},
                    timeout=timeout,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("results", {}).get("bindings", [])
                logger.warning(f"SPARQL returned {resp.status_code}")
            except Exception as e:
                logger.warning(f"SPARQL attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10 * (attempt + 1))
        return []

    def _parse_akn_xml(self, xml_text: str) -> Dict[str, str]:
        """Parse Akoma Ntoso XML to extract title, text, date, number."""
        result = {"title": "", "text": "", "date": "", "number": "", "year": ""}

        # Title
        title_m = re.search(r"<docTitle[^>]*>(.*?)</docTitle>", xml_text, re.DOTALL)
        if title_m:
            result["title"] = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()

        # Fallback title from shortTitle
        if not result["title"]:
            short_m = re.search(r"<shortTitle[^>]*>(.*?)</shortTitle>", xml_text, re.DOTALL)
            if short_m:
                result["title"] = re.sub(r"<[^>]+>", "", short_m.group(1)).strip()

        # Date
        date_m = re.search(r'FRBRdate date="([^"]+)" name="dateIssued"', xml_text)
        if date_m:
            result["date"] = date_m.group(1)

        # Number
        num_m = re.search(r'FRBRnumber value="([^"]+)"', xml_text)
        if num_m:
            result["number"] = num_m.group(1)

        # Year from URI
        year_m = re.search(r"/act/statute(?:-consolidated)?/(\d{4})/", xml_text)
        if year_m:
            result["year"] = year_m.group(1)

        # Body text
        body_m = re.search(r"<body[^>]*>(.*?)</body>", xml_text, re.DOTALL)
        if body_m:
            body_text = re.sub(r"<[^>]+>", " ", body_m.group(1))
            body_text = re.sub(r"\s+", " ", body_text).strip()
            result["text"] = body_text
        else:
            # Fallback: extract all text excluding meta
            text = re.sub(r"<meta>.*?</meta>", "", xml_text, flags=re.DOTALL)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            result["text"] = text

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("_type", "legislation")
        doc_id = raw.get("document_id", "")
        return {
            "_id": doc_id,
            "_source": "FI/FinlexOD",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "number": raw.get("number", ""),
            "year": raw.get("year", ""),
        }

    def _fetch_legislation(self, max_records: Optional[int] = None) -> Generator[Dict, None, None]:
        """Fetch legislation via REST API."""
        count = 0
        page = 1

        while True:
            url = f"{API_BASE}/akn/fi/act/statute/list?format=json&page={page}&limit=10"
            resp = self._request(url)
            if resp is None:
                break

            items = resp.json() if resp.status_code == 200 else []
            if not items:
                break

            # Filter to Finnish versions only
            fin_items = [i for i in items if i.get("akn_uri", "").endswith("/fin@")]
            logger.info(f"Legislation page {page}: {len(fin_items)} Finnish items")

            for item in fin_items:
                akn_uri = item.get("akn_uri", "")
                if not akn_uri:
                    continue

                # Fetch full XML
                xml_resp = self._request(akn_uri, timeout=30)
                if xml_resp is None:
                    continue

                parsed = self._parse_akn_xml(xml_resp.text)
                if not parsed["text"] or len(parsed["text"]) < 50:
                    continue

                # Extract year/number from URI for ID
                uri_m = re.search(r"/statute/(\d{4})/(\d+)/", akn_uri)
                year = uri_m.group(1) if uri_m else parsed.get("year", "")
                number = uri_m.group(2) if uri_m else parsed.get("number", "")

                raw = {
                    "document_id": f"FI-SD-{year}-{number}",
                    "_type": "legislation",
                    "title": parsed["title"] or f"Finnish Statute {year}/{number}",
                    "text": parsed["text"],
                    "date": parsed["date"],
                    "url": f"http://data.finlex.fi/eli/sd/{year}/{number}/alkup",
                    "number": number,
                    "year": year,
                }
                count += 1
                yield self.normalize(raw)

                if max_records and count >= max_records:
                    return

            if len(items) < 10:
                break
            page += 1

        logger.info(f"Legislation: {count} records fetched")

    def _fetch_case_law(self, max_records: Optional[int] = None) -> Generator[Dict, None, None]:
        """Fetch case law via SPARQL endpoint."""
        count = 0
        batch_size = 10
        seen_ids = set()

        for court, graph in [("kko", "http://data.finlex.fi/ecli/kko/"),
                              ("kho", "http://data.finlex.fi/ecli/kho/")]:
            offset = 0
            while True:
                query = f"""
PREFIX sfcl: <http://data.finlex.fi/schema/sfcl/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?judgment ?title ?text WHERE {{
  GRAPH <{graph}> {{
    ?expr a sfcl:Expression ;
          dcterms:title ?title ;
          sfcl:realizes ?judgment .
  }}
  BIND(IRI(CONCAT(STR(?expr), '/txt')) AS ?txtUri)
  ?txtUri sfcl:text ?text .
}} LIMIT {batch_size} OFFSET {offset}
"""
                bindings = self._sparql_query(query, timeout=120)
                if not bindings:
                    break

                logger.info(f"Case law {court.upper()} offset {offset}: {len(bindings)} results")

                for b in bindings:
                    judgment_uri = b.get("judgment", {}).get("value", "")
                    title = b.get("title", {}).get("value", "")
                    full_text = b.get("text", {}).get("value", "").strip()

                    if not full_text or len(full_text) < 50:
                        continue

                    # Extract ECLI-like ID from URI
                    # URI: http://data.finlex.fi/ecli/kko/1998/138
                    uri_m = re.search(r"/ecli/(kko|kho)/(\d{4})/(\d+)", judgment_uri)
                    if uri_m:
                        court_id = uri_m.group(1).upper()
                        year = uri_m.group(2)
                        num = uri_m.group(3)
                        doc_id = f"FI-{court_id}-{year}-{num}"
                    else:
                        doc_id = f"FI-{court.upper()}-{title.replace(':', '-')}"

                    if doc_id in seen_ids:
                        continue
                    seen_ids.add(doc_id)

                    raw = {
                        "document_id": doc_id,
                        "_type": "case_law",
                        "title": title,
                        "text": full_text,
                        "date": "",
                        "url": judgment_uri,
                        "number": num if uri_m else "",
                        "year": year if uri_m else "",
                    }
                    count += 1
                    yield self.normalize(raw)

                    if max_records and count >= max_records:
                        return

                if len(bindings) < batch_size:
                    break
                offset += batch_size

        logger.info(f"Case law: {count} records fetched")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation and case law."""
        yield from self._fetch_legislation()
        yield from self._fetch_case_law()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation updates."""
        yield from self._fetch_legislation(max_records=50)

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test REST API
        resp = self._request(
            f"{API_BASE}/akn/fi/act/statute/list?format=json&page=1&limit=2"
        )
        if resp is None:
            logger.error("REST API unreachable")
            return False
        items = resp.json()
        logger.info(f"REST API OK: {len(items)} items")

        # Test SPARQL
        bindings = self._sparql_query("""
            PREFIX sfcl: <http://data.finlex.fi/schema/sfcl/>
            PREFIX dcterms: <http://purl.org/dc/terms/>
            SELECT ?title WHERE {
              GRAPH <http://data.finlex.fi/ecli/kko/> {
                ?expr a sfcl:Expression ;
                      dcterms:title ?title .
              }
            } LIMIT 1
        """)
        if bindings:
            logger.info(f"SPARQL OK: {bindings[0].get('title', {}).get('value', '')}")
        else:
            logger.warning("SPARQL returned no results (may be slow)")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FI/FinlexOD data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    args = parser.parse_args()

    scraper = FinlexODScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        if args.sample:
            # In sample mode, get 10 legislation + 5 case law
            for record in scraper._fetch_legislation(max_records=10):
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                text_len = len(record.get("text", ""))
                logger.info(
                    f"[{count+1}] {record.get('title', '?')[:80]} ({text_len:,} chars)"
                )
                count += 1

            for record in scraper._fetch_case_law(max_records=5):
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                text_len = len(record.get("text", ""))
                logger.info(
                    f"[{count+1}] {record.get('title', '?')[:80]} ({text_len:,} chars)"
                )
                count += 1
        else:
            for record in scraper.fetch_all():
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
