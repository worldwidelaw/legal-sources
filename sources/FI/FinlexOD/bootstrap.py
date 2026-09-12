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

# The statute list is newest-first, so once this many consecutive already-seen
# statutes go by we are past the new tail. Kept well above the 10-item page size
# so a single page of re-ordered results cannot end the walk early.
STOP_AFTER_SEEN = 50
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
                yield raw

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
                    yield raw

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

    # ── Incremental refresh (#1502) ───────────────────────────────────

    def _checkpoint_path(self) -> Path:
        return Path(__file__).parent / "data" / "finlex_checkpoint.json"

    def _load_seen(self) -> set:
        try:
            with open(self._checkpoint_path(), encoding="utf-8") as f:
                return set(json.load(f).get("seen_ids") or [])
        except (OSError, ValueError):
            return set()

    def _save_seen(self, seen: set) -> None:
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"seen_ids": sorted(seen), "count": len(seen),
                       "updated_at": datetime.now(timezone.utc).isoformat()}, f)
        tmp.replace(path)  # atomic: a truncated checkpoint would re-yield the corpus

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Yield only statutes and decisions not seen by a previous run.

        The old implementation fetched the first 50 statutes on every refresh —
        not a full re-crawl, but an arbitrary cap that could neither reach
        anything below the 50 newest nor return any case law at all (#1502).

        Two facts drive the replacement, both verified against the live API:

        * `/act/statute/list` is ordered newest-first — page 1 is the latest
          statute, page 500 lands about three years back — so the legislation
          walk can stop once it is deep into already-seen territory.
        * `limit` is capped at 10 server-side, so every page costs a request and
          stopping early is where the entire saving comes from.

        The comparator is a seen-ID checkpoint, not a date. A statute's year is
        its enactment year rather than the day Finlex published it, so a date
        cutoff would silently skip anything published late or re-issued — the
        exact failure mode this issue is about. An ID we have never emitted is
        new regardless of the date it carries.

        Case law comes from SPARQL with no ORDER BY, so its offset order is not
        guaranteed to track recency and an early stop would be unsound. Those
        results are filtered against the checkpoint but still walked, so the
        refresh stays correct at the cost of the SPARQL pagination.
        """
        seen = self._load_seen()
        if not seen:
            logger.info(
                "No checkpoint — this first incremental run walks the whole "
                "corpus so nothing is missed; later runs stop early."
            )

        emitted = 0
        consecutive_seen = 0
        stopped_early = False

        for raw in self._fetch_legislation():
            doc_id = raw.get("document_id")
            if doc_id in seen:
                consecutive_seen += 1
                if consecutive_seen >= STOP_AFTER_SEEN:
                    logger.info(
                        f"Reached {consecutive_seen} consecutive known statutes — "
                        "stopping the newest-first walk"
                    )
                    stopped_early = True
                    break
                continue
            consecutive_seen = 0
            seen.add(doc_id)
            emitted += 1
            yield raw

        logger.info(f"Legislation: {emitted} new "
                    f"({'stopped early' if stopped_early else 'walked to the end'})")

        case_new = 0
        for raw in self._fetch_case_law():
            doc_id = raw.get("document_id")
            if doc_id in seen:
                continue
            seen.add(doc_id)
            case_new += 1
            yield raw

        logger.info(f"Case law: {case_new} new. Update yielded {emitted + case_new} records")
        self._save_seen(seen)

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
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full bootstrap (default, accepted for VPS compat)",
    )
    args = parser.parse_args()

    scraper = FinlexODScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap()
        print(json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
