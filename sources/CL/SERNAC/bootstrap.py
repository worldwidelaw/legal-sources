#!/usr/bin/env python3
"""
CL/SERNAC -- Servicio Nacional del Consumidor (Consumer Protection Agency)

Fetches consumer protection doctrine: interpretive circulars and interpretive
rulings (dictámenes) from SERNAC's public portal.

Strategy:
  - Scrape two listing pages for document metadata (title, date, PDF URL)
  - Circulares Interpretativas: ~41 documents (2019-present)
  - Dictámenes Interpretativos: ~85 documents (2020-present)
  - Download PDFs and extract full text via common/pdf_extract
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch current year only
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.SERNAC")

BASE_URL = "https://www.sernac.cl"
PORTAL = "/portal/618"

# Listing pages
CIRCULARES_URL = f"{BASE_URL}{PORTAL}/w3-propertyvalue-21072.html"
DICTAMENES_URL = f"{BASE_URL}{PORTAL}/w3-propertyvalue-66262.html"

USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"


def _get(url: str, timeout: int = 60) -> str:
    """GET a URL and return decoded text."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    return resp.read().decode("utf-8", errors="replace")


def _get_bytes(url: str, timeout: int = 120) -> bytes:
    """GET a URL and return raw bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    return resp.read()


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    return " ".join(text.split())


def _parse_iso_from_class(class_str: str) -> Optional[str]:
    """Extract ISO date from class like 'iso8601-20251009T1719000300'."""
    m = re.search(r"iso8601-(\d{4})(\d{2})(\d{2})", class_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _parse_listing_page(html: str, doc_type: str) -> List[Dict[str, Any]]:
    """Parse a SERNAC listing page and extract document metadata.

    Each document is in a <div class="recuadro"> block with:
      - PDF link: articles-{aid}_archivo_01.pdf
      - Title in <a> tag title attribute
      - Date in <p class="fecha ... iso8601-YYYYMMDD...">
      - Article ID in aid-{number} class
    """
    results = []
    blocks = re.findall(r'<div class="recuadro">(.*?)</div>\s*</div>', html, re.DOTALL)

    for block in blocks:
        # Extract article ID
        aid_match = re.search(r'aid-(\d+)', block)
        if not aid_match:
            continue
        aid = aid_match.group(1)

        # Extract PDF URL
        pdf_match = re.search(r'href="(articles-\d+_archivo_01\.pdf)"', block)
        if not pdf_match:
            continue
        pdf_url = f"{BASE_URL}{PORTAL}/{pdf_match.group(1)}"

        # Extract title from the PDF link title attribute
        title_match = re.search(r'title="Ir a ([^"]+)"', block)
        if title_match:
            title = unescape(title_match.group(1))
        else:
            # Fallback: get from h3 link text
            h3_match = re.search(r'<h3[^>]*>.*?<a[^>]*>([^<]+)</a>', block, re.DOTALL)
            title = _clean_html(h3_match.group(1)) if h3_match else f"{doc_type} {aid}"

        # Extract date from ISO class
        date_match = re.search(r'class="[^"]*iso8601-(\d{8})', block)
        if date_match:
            d = date_match.group(1)
            date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        else:
            date_str = None

        # Extract description snippet
        desc_match = re.search(r'pn-xattr">\s*<p>(.*?)</p>', block, re.DOTALL)
        description = _clean_html(desc_match.group(1)) if desc_match else ""

        # Extract resolution number from description
        res_match = re.search(r'Resolución Exenta N[°º]?\s*(\d+)', description)
        resolution = f"RE-{res_match.group(1)}" if res_match else None

        results.append({
            "article_id": aid,
            "doc_type": doc_type,
            "title": title,
            "date": date_str,
            "pdf_url": pdf_url,
            "page_url": f"{BASE_URL}{PORTAL}/w3-article-{aid}.html",
            "description": description,
            "resolution": resolution,
        })

    return results


class SERNACScraper(BaseScraper):
    SOURCE_ID = "CL/SERNAC"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all SERNAC interpretive documents."""
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents modified since a date."""
        since_date = since[:10] if since else "1970-01-01"
        for doc in self._fetch_documents(sample=False):
            if doc.get("date") and doc["date"] >= since_date:
                yield doc

    def _fetch_documents(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents from both listing pages."""
        all_docs = []

        # Fetch circulars listing
        logger.info("Fetching circulares interpretativas listing...")
        try:
            html = _get(CIRCULARES_URL)
            circulares = _parse_listing_page(html, "circular")
            logger.info(f"Found {len(circulares)} circulares")
            all_docs.extend(circulares)
        except Exception as e:
            logger.error(f"Failed to fetch circulares listing: {e}")

        time.sleep(1.5)

        # Fetch dictámenes listing
        logger.info("Fetching dictámenes interpretativos listing...")
        try:
            html = _get(DICTAMENES_URL)
            dictamenes = _parse_listing_page(html, "dictamen")
            logger.info(f"Found {len(dictamenes)} dictámenes")
            all_docs.extend(dictamenes)
        except Exception as e:
            logger.error(f"Failed to fetch dictámenes listing: {e}")

        if sample:
            # Take 6 from each category for balanced sampling
            circ = [d for d in all_docs if d["doc_type"] == "circular"][:6]
            dict_ = [d for d in all_docs if d["doc_type"] == "dictamen"][:8]
            all_docs = circ + dict_

        logger.info(f"Processing {len(all_docs)} documents total...")
        fetched = 0

        for doc in all_docs:
            try:
                logger.info(f"Downloading PDF: {doc['title'][:60]}...")
                pdf_bytes = _get_bytes(doc["pdf_url"])
                text = extract_pdf_markdown(
                    source=self.SOURCE_ID,
                    source_id=f"{doc['doc_type']}-{doc['article_id']}",
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                )

                if not text or len(text.strip()) < 100:
                    logger.warning(f"Insufficient text for {doc['article_id']}: {len(text) if text else 0} chars")
                    continue

                doc["text"] = text
                fetched += 1
                yield self.normalize(doc)
                time.sleep(1.0)

            except Exception as e:
                logger.warning(f"Failed to fetch PDF for {doc['article_id']}: {e}")
                continue

        logger.info(f"Fetched {fetched}/{len(all_docs)} documents with full text")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a SERNAC document to the standard schema."""
        doc_type = raw.get("doc_type", "circular")
        aid = raw["article_id"]

        return {
            "_id": f"CL-SERNAC-{doc_type}-{aid}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("page_url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "doc_type": doc_type,
            "resolution": raw.get("resolution"),
            "description": raw.get("description", ""),
            "language": "es",
            "jurisdiction": "CL",
            "issuing_body": "Servicio Nacional del Consumidor (SERNAC)",
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            html = _get(CIRCULARES_URL, timeout=15)
            docs = _parse_listing_page(html, "circular")
            logger.info(f"Connectivity OK — found {len(docs)} circulares")
            return len(docs) > 0
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/SERNAC data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    scraper = SERNACScraper(str(source_dir))

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)
    count = 0
    limit = 15 if args.sample else 999999

    gen = (
        scraper._fetch_documents(sample=args.sample)
        if args.command == "bootstrap"
        else scraper.fetch_updates(since=datetime.now().strftime("%Y-01-01"))
    )

    for record in gen:
        if count >= limit:
            break
        text_len = len(record.get("text", ""))
        if text_len < 100:
            logger.warning(f"Skipping {record['_id']}: text too short ({text_len} chars)")
            continue

        fname = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"]) + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"[{count}/{limit}] Saved {record['_id']} ({text_len} chars)")

    logger.info(f"Done: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
