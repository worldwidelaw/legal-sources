#!/usr/bin/env python3
"""
US/EyeciteCitations -- Citation-Enriched US Case Law (Eyecite + CourtListener)

Fetches US court opinions via CourtListener's public search API, extracts full
text, then runs Free Law Project's Eyecite library to parse every Blue Book
citation in each opinion. Each record contains the opinion text plus structured
citation metadata (volume, reporter, page, year, court).

This is Phase 3 of issue #549 — building the citation resolution foundation.
The extracted citation data enables:
  - Citation index: (reporter, volume, page) → document lookup
  - Citation graph: which cases cite which other cases
  - Blue Book resolution: "347 U.S. 483" → Brown v. Board of Education

Dependencies: eyecite, reporters-db, courts-db (all BSD-2 licensed)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 opinions with citations
  python bootstrap.py bootstrap             # Full bootstrap (paginated)
  python bootstrap.py update --since YYYY-MM-DD  # Incremental updates
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

from common.pdf_extract import extract_pdf_markdown

try:
    from eyecite import get_citations
    from eyecite.models import FullCaseCitation, ShortCaseCitation, SupraCitation, IdCitation
    EYECITE_AVAILABLE = True
except ImportError:
    EYECITE_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.EyeciteCitations")

SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_BASE = "https://storage.courtlistener.com/"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Sample across diverse courts for representative citation extraction
SAMPLE_COURTS = [
    "scotus",      # Supreme Court
    "ca2",         # Second Circuit
    "ca9",         # Ninth Circuit
    "cadc",        # DC Circuit
    "ca5",         # Fifth Circuit
    "ca7",         # Seventh Circuit
    "ca3",         # Third Circuit
    "ca11",        # Eleventh Circuit
    "nysupct",     # New York
    "cal",         # California
    "texsupct",    # Texas
    "illappct",    # Illinois
    "fla",         # Florida
    "mass",        # Massachusetts
    "pa",          # Pennsylvania
]


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._pieces = []

    def handle_data(self, data):
        self._pieces.append(data)

    def get_text(self):
        return "".join(self._pieces)


def strip_html(html: str) -> str:
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_citations(text: str) -> List[Dict[str, Any]]:
    """Extract and structure all citations from opinion text using eyecite."""
    if not EYECITE_AVAILABLE or not text:
        return []

    citations = []
    seen = set()

    for cite in get_citations(text):
        if isinstance(cite, FullCaseCitation):
            key = f"{cite.groups.get('volume', '')}-{cite.groups.get('reporter', '')}-{cite.groups.get('page', '')}"
            if key in seen:
                continue
            seen.add(key)

            # Get surrounding context (up to 200 chars each side)
            start = max(0, cite.span()[0] - 200)
            end = min(len(text), cite.span()[1] + 200)
            context = text[start:end].strip()

            entry = {
                "type": "full",
                "citation": cite.matched_text(),
                "volume": cite.groups.get("volume", ""),
                "reporter": cite.groups.get("reporter", ""),
                "page": cite.groups.get("page", ""),
                "year": cite.metadata.year if cite.metadata else None,
                "court": cite.metadata.court if cite.metadata else None,
                "plaintiff": cite.metadata.plaintiff if cite.metadata else None,
                "defendant": cite.metadata.defendant if cite.metadata else None,
                "context": context,
            }
            citations.append(entry)

        elif isinstance(cite, ShortCaseCitation):
            key = f"short-{cite.groups.get('volume', '')}-{cite.groups.get('reporter', '')}-{cite.groups.get('page', '')}"
            if key in seen:
                continue
            seen.add(key)
            entry = {
                "type": "short",
                "citation": cite.matched_text(),
                "volume": cite.groups.get("volume", ""),
                "reporter": cite.groups.get("reporter", ""),
                "page": cite.groups.get("page", ""),
            }
            citations.append(entry)

    return citations


class EyeciteCitationsScraper(BaseScraper):
    """Scraper for US/EyeciteCitations via CourtListener + Eyecite."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        if not EYECITE_AVAILABLE:
            logger.warning("eyecite not installed — citation extraction disabled")

    def _search_opinions(self, court: str = None, page_size: int = 20,
                         filed_after: str = None, filed_before: str = None,
                         cursor_url: str = None) -> Dict[str, Any]:
        for attempt in range(3):
            try:
                if cursor_url:
                    resp = self.session.get(cursor_url, timeout=60)
                else:
                    params = {
                        "format": "json",
                        "type": "o",
                        "page_size": min(page_size, 20),
                        "order_by": "dateFiled desc",
                    }
                    if court:
                        params["court"] = court
                    if filed_after:
                        params["filed_after"] = filed_after
                    if filed_before:
                        params["filed_before"] = filed_before
                    resp = self.session.get(SEARCH_URL, params=params, timeout=60)

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.Timeout:
                if attempt < 2:
                    logger.warning("Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
            except Exception as e:
                logger.error(f"Search API error: {e}")
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise
        return {"count": 0, "results": []}

    def _download_file(self, url: str) -> Optional[bytes]:
        try:
            resp = self.session.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            if len(resp.content) > 100:
                return resp.content
            return None
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_data: bytes) -> str:
        return extract_pdf_markdown(
            source="US/EyeciteCitations",
            source_id="",
            pdf_bytes=pdf_data,
            table="case_law",
        ) or ""

    def _get_file_url(self, opinion: Dict) -> Optional[str]:
        local_path = opinion.get("local_path")
        if local_path:
            return STORAGE_BASE + local_path
        download_url = opinion.get("download_url")
        if download_url:
            return download_url
        return None

    def _extract_text_from_url(self, url: str) -> str:
        data = self._download_file(url)
        if not data:
            return ""
        if url.endswith(".html") or url.endswith(".htm"):
            return strip_html(data.decode("utf-8", errors="replace"))
        header = data[:100].lower()
        if b"<!doctype html" in header or b"<html" in header:
            return strip_html(data.decode("utf-8", errors="replace"))
        return self._extract_pdf_text(data)

    def _process_search_result(self, result: Dict) -> Optional[Dict[str, Any]]:
        opinions = result.get("opinions", [])
        if not opinions:
            return None

        opinion = opinions[0]
        file_url = self._get_file_url(opinion)
        if not file_url:
            return None

        text = self._extract_text_from_url(file_url)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {result.get('caseName', 'unknown')}: {len(text)} chars")
            return None

        # Run eyecite citation extraction
        citations_data = extract_citations(text)
        full_citations = [c for c in citations_data if c["type"] == "full"]
        short_citations = [c for c in citations_data if c["type"] == "short"]

        logger.info(
            f"  Citations: {len(full_citations)} full + {len(short_citations)} short "
            f"in {result.get('caseName', '')[:50]}"
        )

        cl_citations = result.get("citation", [])
        citation_str = cl_citations[0] if cl_citations else ""

        return {
            "cluster_id": result.get("cluster_id"),
            "case_name": result.get("caseName", ""),
            "case_name_full": result.get("caseNameFull", ""),
            "docket_number": result.get("docketNumber", ""),
            "court_id": result.get("court_id", ""),
            "court": result.get("court", ""),
            "date_filed": result.get("dateFiled"),
            "citation": citation_str,
            "status": result.get("status", ""),
            "file_url": file_url,
            "cl_url": f"https://www.courtlistener.com{result.get('absolute_url', '')}",
            "text": text,
            # Eyecite enrichment
            "citations_extracted": citations_data,
            "citation_count": len(citations_data),
            "full_citation_count": len(full_citations),
            "short_citation_count": len(short_citations),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        cursor_url = None
        total_fetched = 0
        page = 0
        while True:
            page += 1
            logger.info(f"Fetching search results page {page}...")
            data = self._search_opinions(cursor_url=cursor_url)
            results = data.get("results", [])
            if not results:
                break
            for result in results:
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                raw = self._process_search_result(result)
                if raw:
                    total_fetched += 1
                    yield raw
            cursor_url = data.get("next")
            if not cursor_url:
                break
        logger.info(f"Total fetched: {total_fetched}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        if not since:
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cursor_url = None
        page = 0
        while True:
            page += 1
            data = self._search_opinions(filed_after=since, cursor_url=cursor_url)
            results = data.get("results", [])
            if not results:
                break
            for result in results:
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                raw = self._process_search_result(result)
                if raw:
                    yield raw
            cursor_url = data.get("next")
            if not cursor_url:
                break

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        court_id = raw.get("court_id", "")
        cluster_id = raw.get("cluster_id", "")
        doc_id = f"US-CITE-{court_id.upper()}-{cluster_id}"

        # Build citation index entries from extracted citations
        citation_index = []
        for cite in raw.get("citations_extracted", []):
            if cite.get("type") == "full":
                citation_index.append({
                    "volume": cite["volume"],
                    "reporter": cite["reporter"],
                    "page": cite["page"],
                    "year": cite.get("year"),
                    "court": cite.get("court"),
                    "plaintiff": cite.get("plaintiff"),
                    "defendant": cite.get("defendant"),
                    "lookup_key": f"{cite['volume']}-{cite['reporter'].replace(' ', '').replace('.', '')}-{cite['page']}",
                })

        return {
            "_id": doc_id,
            "_source": "US/EyeciteCitations",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date_filed"),
            "url": raw.get("cl_url", ""),
            "case_number": raw.get("docket_number", ""),
            "court": raw.get("court", ""),
            "court_id": raw.get("court_id", ""),
            "citation": raw.get("citation", ""),
            "status": raw.get("status", ""),
            "jurisdiction": "US",
            "file_url": raw.get("file_url", ""),
            # Citation enrichment (Phase 3, issue #549)
            "citation_count": raw.get("citation_count", 0),
            "full_citation_count": raw.get("full_citation_count", 0),
            "short_citation_count": raw.get("short_citation_count", 0),
            "citations_extracted": raw.get("citations_extracted", []),
            "citation_index": citation_index,
        }

    def test_connection(self) -> bool:
        try:
            data = self._search_opinions(court="scotus", page_size=5)
            count = data.get("count", 0)
            results = data.get("results", [])
            logger.info(f"Connection test: {count:,} SCOTUS opinions, got {len(results)} results")

            # Test eyecite
            if EYECITE_AVAILABLE:
                test_text = "See Brown v. Board of Education, 347 U.S. 483 (1954)."
                cites = extract_citations(test_text)
                logger.info(f"Eyecite test: found {len(cites)} citation(s) in test string")
                if cites:
                    logger.info(f"  Parsed: {cites[0]}")
            else:
                logger.warning("Eyecite not available — install with: pip install eyecite")
                return False

            return count > 0 and len(results) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def fetch_sample(self, count: int = 15) -> Generator[Dict[str, Any], None, None]:
        """Fetch samples from diverse courts for representative citation coverage."""
        per_court = max(1, count // len(SAMPLE_COURTS))
        total = 0
        for court in SAMPLE_COURTS:
            if total >= count:
                break
            logger.info(f"Sampling from {court}...")
            data = self._search_opinions(court=court, page_size=5)
            results = data.get("results", [])
            court_count = 0
            for result in results:
                if total >= count or court_count >= per_court:
                    break
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                raw = self._process_search_result(result)
                if raw:
                    total += 1
                    court_count += 1
                    yield raw
        logger.info(f"Total sampled: {total}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/EyeciteCitations — citation-enriched case law")
    subparsers = parser.add_subparsers(dest="command")

    boot_parser = subparsers.add_parser("bootstrap", help="Bootstrap data")
    boot_parser.add_argument("--sample", action="store_true", help="Sample mode")
    boot_parser.add_argument("--full", action="store_true", help="Full bootstrap")
    boot_parser.add_argument("--count", type=int, default=15, help="Sample count")

    upd_parser = subparsers.add_parser("update", help="Incremental update")
    upd_parser.add_argument("--since", required=True, help="YYYY-MM-DD")

    subparsers.add_parser("test", help="Test connectivity + eyecite")

    args = parser.parse_args()

    scraper = EyeciteCitationsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    elif args.command == "bootstrap":
        if args.sample:
            # Use diverse court sampling
            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(parents=True, exist_ok=True)
            records = []
            total_citations = 0
            for raw in scraper.fetch_sample(count=args.count):
                record = scraper.normalize(raw)
                records.append(record)
                total_citations += record.get("citation_count", 0)
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False, default=str)
                logger.info(
                    f"[{len(records)}/{args.count}] {record['title'][:60]} — "
                    f"{record.get('citation_count', 0)} citations, "
                    f"{len(record.get('text', ''))} chars"
                )

            # Summary stats
            logger.info(f"\n=== SAMPLE SUMMARY ===")
            logger.info(f"Records: {len(records)}")
            logger.info(f"Total citations extracted: {total_citations}")
            if records:
                avg_cites = total_citations / len(records)
                avg_text = sum(len(r.get("text", "")) for r in records) / len(records)
                logger.info(f"Avg citations/opinion: {avg_cites:.1f}")
                logger.info(f"Avg text length: {avg_text:.0f} chars")
                courts = set(r.get("court_id", "") for r in records)
                logger.info(f"Courts covered: {len(courts)} ({', '.join(sorted(courts))})")

                # Citation type breakdown
                all_cites = []
                for r in records:
                    all_cites.extend(r.get("citations_extracted", []))
                reporters = {}
                for c in all_cites:
                    if c.get("type") == "full":
                        rep = c.get("reporter", "?")
                        reporters[rep] = reporters.get(rep, 0) + 1
                if reporters:
                    top = sorted(reporters.items(), key=lambda x: -x[1])[:10]
                    logger.info(f"Top reporters cited: {', '.join(f'{r}({n})' for r, n in top)}")
        elif args.full:
            stats = scraper.bootstrap(sample_mode=False)
            logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")
        else:
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
            logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "update":
        count = 0
        data_dir = scraper.source_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        with open(data_dir / "updates.jsonl", "w") as f:
            for raw in scraper.fetch_updates(since=args.since):
                record = scraper.normalize(raw)
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        logger.info(f"Fetched {count} updates since {args.since}")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
