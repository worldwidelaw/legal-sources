#!/usr/bin/env python3
"""
UK/ScottishCourts -- Scottish Courts and Tribunals Service judgments.

Fetches Scottish court opinions from the official SCTS judgments search API.

Strategy:
  - Read the public web-app definition to discover supported courts.
  - Page through POST /search for judgment metadata.
  - Download linked judgment PDFs from scotcourts.gov.uk/media/.
  - Extract full text from PDFs with the shared PDF extractor.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample --sample-size 10
  python bootstrap.py bootstrap
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse

import urllib3

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.ScottishCourts")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOURCE_ID = "UK/ScottishCourts"
PUBLIC_BASE_URL = "https://www.scotcourts.gov.uk"
API_BASE_URL = "https://api.pa.web.scotcourts.gov.uk/web"
DEFINITION_ID = 1414

CITATION_RE = re.compile(
    r"\[(?P<year>\d{4})\]\s+"
    r"(?P<court>CSIH|CSOH|HCJAC|HCJ|SAC\s*\(Civ\)|SAC\s*\(Crim\)|"
    r"SC\s+[A-Z][A-Za-z]+|SC\s+[A-Z]{2,})\s+"
    r"(?P<number>\d+)",
    re.IGNORECASE,
)


def parse_iso_date(value: str) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return value[:10]


def safe_slug(value: str) -> str:
    slug = Path(urlparse(value).path).stem if value else ""
    slug = re.sub(r"[^A-Za-z0-9]+", "-", slug).strip("-")
    return slug[:120] or "record"


def extract_citation(text: str) -> Optional[str]:
    if not text:
        return None
    match = CITATION_RE.search(text[:6000])
    if not match:
        return None
    court = re.sub(r"\s+", " ", match.group("court").upper())
    court = court.replace("SAC (CIV)", "SAC (Civ)").replace("SAC (CRIM)", "SAC (Crim)")
    return f"[{match.group('year')}] {court} {match.group('number')}"


def court_division(citation: Optional[str], courts: list[str]) -> Optional[str]:
    citation = citation or ""
    if "CSIH" in citation:
        return "Court of Session - Inner House"
    if "CSOH" in citation:
        return "Court of Session - Outer House"
    if "HCJAC" in citation:
        return "High Court of Justiciary - Appeal Court"
    if "SAC (Civ)" in citation:
        return "Sheriff Appeal Court - Civil"
    if "SAC (Crim)" in citation:
        return "Sheriff Appeal Court - Criminal"
    if courts:
        return courts[0]
    return None


class ScottishCourtsScraper(BaseScraper):
    """Scraper for official Scottish court judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        headers = {
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        }
        self.client = HttpClient(base_url=API_BASE_URL, headers=headers, timeout=60, verify=False)
        self.pdf_client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/pdf,*/*",
            },
            timeout=120,
            verify=False,
        )

    def _get_definition(self) -> dict:
        self.rate_limiter.wait()
        resp = self.client.get(f"/definition/{DEFINITION_ID}")
        resp.raise_for_status()
        return resp.json()

    def _search_page(self, page: int, limit: int = 50, filters: Optional[list] = None) -> dict:
        payload = {
            "query": "",
            "filters": filters or [],
            "page": page,
            "indexType": "Judgments",
            "category": "",
            "limit": limit,
        }
        self.rate_limiter.wait()
        resp = self.client.post("/search", json_data=payload)
        resp.raise_for_status()
        return resp.json()

    def _absolute_url(self, link: str) -> str:
        if not link:
            return ""
        return urljoin(PUBLIC_BASE_URL, link)

    def _download_pdf_text(self, pdf_url: str, source_id: str) -> str:
        if not pdf_url:
            return ""
        self.rate_limiter.wait()
        try:
            resp = self.pdf_client.get(pdf_url)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("Failed to download PDF %s: %s", pdf_url, exc)
            return ""
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_bytes=resp.content,
            table="case_law",
        )

    def _fetch_record(self, row: dict) -> Optional[dict]:
        pdf_url = self._absolute_url(row.get("documentLink", ""))
        source_id = safe_slug(pdf_url)
        text = self._download_pdf_text(pdf_url, source_id)
        if not text:
            return None

        courts = row.get("court") or []
        citation = extract_citation(text)
        enriched = dict(row)
        enriched["_pdf_url"] = pdf_url
        enriched["_full_text"] = text
        enriched["_citation"] = citation
        enriched["_division"] = court_division(citation, courts)
        enriched["_slug"] = source_id
        return enriched

    def fetch_all(self) -> Generator[dict, None, None]:
        page = 1
        total_pages = None

        while True:
            data = self._search_page(page=page)
            rows = data.get("results") or []
            pagination = data.get("pagination", {}).get("page", {})
            total_pages = total_pages or pagination.get("total", 1)
            logger.info("Page %s/%s: %s records", page, total_pages, len(rows))

            if not rows:
                break

            for row in rows:
                record = self._fetch_record(row)
                if record:
                    yield record
                time.sleep(0.25)

            if page >= total_pages:
                break
            page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        page = 1
        since_date = since.date().isoformat()

        while True:
            data = self._search_page(page=page)
            rows = data.get("results") or []
            pagination = data.get("pagination", {}).get("page", {})
            total_pages = pagination.get("total", 1)
            if not rows:
                break

            all_old = True
            for row in rows:
                published = parse_iso_date(row.get("date", ""))
                if published and published < since_date:
                    continue
                all_old = False
                record = self._fetch_record(row)
                if record:
                    yield record
                time.sleep(0.25)

            if all_old or page >= total_pages:
                break
            page += 1

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("_full_text", "")
        pdf_url = raw.get("_pdf_url", "")
        title = raw.get("title", "")
        if not title or not pdf_url or not text:
            return None

        opinion_date = parse_iso_date(raw.get("additionalDate", ""))
        published_date = parse_iso_date(raw.get("date", ""))
        courts = raw.get("court") or []
        judges = raw.get("judges") or []
        sheriffdom = raw.get("sheriffdom") or []
        slug = raw.get("_slug") or safe_slug(pdf_url)

        return {
            "_id": f"UK-SCOTCOURTS-{slug}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": opinion_date or published_date,
            "published_date": published_date or None,
            "url": pdf_url,
            "pdf_url": pdf_url,
            "court": courts[0] if courts else None,
            "courts": courts,
            "division": raw.get("_division"),
            "judges": judges,
            "sheriffdom": sheriffdom,
            "citation": raw.get("_citation"),
            "jurisdiction": "Scotland",
            "jurisdiction_code": "GB-SCT",
            "source_api_url": f"{API_BASE_URL}/search",
        }

    def test_api(self) -> bool:
        definition = self._get_definition()
        config = definition.get("configurations", [{}])[0]
        court_filter = (config.get("filtering", {}).get("filters") or [{}])[0]
        courts = [option.get("value") for option in court_filter.get("options", [])]
        logger.info("Configured courts: %s", ", ".join(court for court in courts if court))

        data = self._search_page(page=1, limit=3)
        total = data.get("pagination", {}).get("count", {}).get("total", 0)
        rows = data.get("results") or []
        logger.info("Search API returned %s total judgments", total)
        if not rows:
            return False

        sample = self._fetch_record(rows[0])
        if not sample:
            return False
        logger.info(
            "PDF extraction OK: %s chars from %s",
            len(sample.get("_full_text", "")),
            sample.get("title", "")[:80],
        )
        return True


def main():
    parser = argparse.ArgumentParser(description="UK/ScottishCourts scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--sample-size", type=int, default=10, help="Sample record count")
    args = parser.parse_args()

    scraper = ScottishCourtsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        if not ok:
            raise SystemExit(1)
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))
        return

    if args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.update() if scraper.status.get("last_run") else {
            "note": "No last_run; using 30-day fetch_updates for smoke update",
            "records": sum(1 for _ in scraper.fetch_updates(since)),
        }
        logger.info("Update complete: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
