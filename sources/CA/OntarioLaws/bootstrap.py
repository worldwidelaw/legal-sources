#!/usr/bin/env python3
"""
CA/OntarioLaws -- Ontario e-Laws Data Fetcher

Fetches consolidated Ontario statutes and regulations from the official e-Laws
portal's undocumented REST API (backed by Elasticsearch).

Strategy:
  - Enumerate all current statutes/regulations via advanced-search (body search for "the")
  - Fetch full text for each document via doc-search endpoint
  - Strip HTML to plain text

API base: https://www.ontario.ca/laws/api/v2/
No auth required, just a browser-like User-Agent.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.OntarioLaws")

API_BASE = "https://www.ontario.ca/laws/api/v2"
LAWS_BASE = "https://www.ontario.ca/laws"
PAGE_SIZE = 50


class HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._text = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "td"):
            self._text.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._text.append(data)

    def get_text(self):
        text = "".join(self._text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()


def html_to_text(html: str) -> str:
    """Convert HTML content to plain text."""
    if not html:
        return ""
    extractor = HTMLTextExtractor()
    try:
        extractor.feed(html)
        return extractor.get_text()
    except Exception:
        # Fallback: regex strip
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"&nbsp;", " ", text)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"&lt;", "<", text)
        text = re.sub(r"&gt;", ">", text)
        text = re.sub(r"&#\d+;", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()


class OntarioLawsScraper(BaseScraper):
    """
    Scraper for CA/OntarioLaws -- Ontario e-Laws.
    Country: CA
    URL: https://www.ontario.ca/laws

    Data types: legislation
    Auth: none (King's Printer for Ontario, open reproduction)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )

    def _search_documents(self, doc_type: str = "statute") -> list:
        """Enumerate all current documents of a given type via advanced-search."""
        all_docs = []
        page = 1

        while True:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(
                    f"/legislation/en/advanced-search",
                    params={
                        "text": "the",
                        "selection": "current",
                        "result": doc_type,
                        "searchWithin": "body",
                        "page": page,
                        "pageSize": PAGE_SIZE,
                        "sort": "AZ",
                        "highlight": "",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Search failed for {doc_type} page {page}: {e}")
                break

            hits = data.get("hits", {}).get("hits", [])
            total = data.get("hits", {}).get("total", {}).get("value", 0)

            for hit in hits:
                source = hit.get("_source", {})
                code = source.get("code", "")
                alias = source.get("alias", {}).get("en", "")
                act_name = source.get("act", {}).get("en", "")
                title = source.get("title", {}).get("en", "")
                date_from = source.get("dateFrom", {}).get("en", "")

                if code:
                    all_docs.append({
                        "code": code,
                        "alias": alias,
                        "act_name": act_name,
                        "title": title,
                        "date_from": date_from,
                        "doc_type": doc_type,
                    })

            logger.info(f"  {doc_type} page {page}: {len(hits)} hits (total: {total}, collected: {len(all_docs)})")

            if len(hits) < PAGE_SIZE or len(all_docs) >= total:
                break
            page += 1

        return all_docs

    def _fetch_full_text(self, doc_type: str, code: str) -> Optional[dict]:
        """Fetch full document text via doc-search endpoint."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/legislation/en/doc-search/{doc_type}/{code}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch {doc_type}/{code}: {e}")
            return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        doc_type = raw.get("doc_type", "statute")
        code = raw.get("code", "")
        alias = raw.get("alias", "")

        # Parse date
        date_str = raw.get("date_from", "")
        date_iso = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                pass

        # Build URL from alias
        url = f"{LAWS_BASE}/{alias}" if alias else f"{LAWS_BASE}/{doc_type}/{code}"

        return {
            "_id": f"CA-ON-{doc_type}-{code}",
            "_source": "CA/OntarioLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "code": code,
            "doc_type": doc_type,
            "title": raw.get("title", raw.get("act_name", "")),
            "act_name": raw.get("act_name", ""),
            "text": raw.get("text", ""),
            "date": date_iso,
            "state": raw.get("state", "current"),
            "url": url,
            "chapter": raw.get("chapter", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Ontario statutes and regulations with full text."""
        logger.info("Starting Ontario e-Laws fetch...")

        for doc_type in ["statute", "regulation"]:
            logger.info(f"Enumerating {doc_type}s...")
            docs = self._search_documents(doc_type)
            logger.info(f"Found {len(docs)} {doc_type}s")

            for i, doc in enumerate(docs):
                code = doc["code"]
                logger.info(f"  [{i+1}/{len(docs)}] Fetching {doc_type}/{code}...")

                full = self._fetch_full_text(doc_type, code)
                if not full:
                    continue

                content_html = full.get("content", "")
                text = html_to_text(content_html)

                if not text or len(text) < 50:
                    logger.warning(f"  {doc_type}/{code}: text too short ({len(text)} chars), skipping")
                    continue

                doc["text"] = text

                # Extract string values from potentially dict fields (API returns {en:..., fr:...})
                title = full.get("title", doc.get("title", ""))
                act_name = full.get("actName", doc.get("act_name", ""))
                if isinstance(title, dict):
                    title = title.get("en", "")
                if isinstance(act_name, dict):
                    act_name = act_name.get("en", "")

                doc["title"] = title
                doc["act_name"] = act_name
                doc["state"] = full.get("state", "current")
                doc["chapter"] = full.get("chapter", "")
                doc["date_from"] = full.get("dateFrom", doc.get("date_from", ""))

                yield doc

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch documents updated since a given date."""
        yield from self.fetch_all()

    def test_api(self):
        """Quick API connectivity test."""
        logger.info("Testing Ontario e-Laws API...")

        # Test currency date
        self.rate_limiter.wait()
        resp = self.client.get("/legislation/en/currency-date")
        logger.info(f"Currency date: {resp.text.strip()} (HTTP {resp.status_code})")

        # Test search
        self.rate_limiter.wait()
        resp = self.client.get(
            "/legislation/en/advanced-search",
            params={
                "text": "highway",
                "selection": "current",
                "result": "statute",
                "searchWithin": "title",
                "page": 1,
                "pageSize": 5,
                "sort": "AZ",
                "highlight": "",
            },
        )
        data = resp.json()
        total = data.get("hits", {}).get("total", {}).get("value", 0)
        logger.info(f"Search for 'highway' statutes: {total} results (HTTP {resp.status_code})")

        # Test full text fetch
        self.rate_limiter.wait()
        resp = self.client.get("/legislation/en/doc-search/statute/90h08")
        data = resp.json()
        content_len = len(data.get("content", ""))
        logger.info(f"Highway Traffic Act content: {content_len} chars (HTTP {resp.status_code})")
        logger.info(f"Title: {data.get('title', '?')}")

        logger.info("API test complete!")


def main():
    scraper = OntarioLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|test-api]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        scraper.test_api()
    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        scraper.bootstrap(sample_mode=sample)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
