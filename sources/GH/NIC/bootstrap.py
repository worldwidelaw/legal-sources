#!/usr/bin/env python3
"""
GH/NIC -- Ghana National Insurance Commission — Directives

Fetches regulatory directives, guidelines, market reports, public notices and
news from Ghana's NIC.

Strategy:
  The Commission retired the old WordPress site at nicgh.org (it now 301s to
  nic.gov.gh) and rebuilt on a Vite SPA backed by a public Strapi v5 API. The
  old /wp-json/wp/v2 endpoints are gone (#1511). We read the Strapi collections
  directly — no auth, no HTML scraping:

    /api/regulatory-docs  Directives & guidelines, each with a PDF `file`
    /api/publications     Annual reports, quarterly market reports, market
                          research, each with a PDF `file`
    /api/notices          Public notices issued under the Insurance Act,
                          full text inline as Strapi rich-text blocks
    /api/news-articles    News & events, full text inline as rich-text blocks

  PDFs live on Azure blob storage and are extracted with pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for the full pull (fleet)
  python bootstrap.py update             # Incremental refresh
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import time
import logging
import html
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GH.NIC")

SITE_BASE = "https://nic.gov.gh"
API_BASE = (
    "https://nicwebsitebackend-gcauhtdfeph8d9e4.westeurope-01.azurewebsites.net/api"
)
USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"

MIN_TEXT_LENGTH = 150

# Strapi collection -> (record kind, site path prefix for the public URL)
COLLECTIONS = {
    "regulatory-docs": ("directive", "/regulation/guidelines"),
    "publications": ("publication", "/media/reports"),
    "notices": ("notice", "/media/notices"),
    "news-articles": ("news", "/media/news"),
}


def strip_html(raw_html: str) -> str:
    """Remove HTML tags, decode entities, collapse whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", raw_html, flags=re.S | re.I)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(p|div|h[1-6]|li|tr|td|th|blockquote)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def blocks_to_text(blocks) -> str:
    """Flatten Strapi rich-text blocks (`body`) into plain text.

    Blocks are a list of {type, children:[{type, text|children}]}; lists nest
    one level deeper. Anything unrecognised contributes its leaf `text` values.
    """
    if isinstance(blocks, str):
        return strip_html(blocks)
    if not isinstance(blocks, list):
        return ""

    def leaf_text(node) -> str:
        if isinstance(node, str):
            return node
        if not isinstance(node, dict):
            return ""
        if isinstance(node.get("children"), list):
            return "".join(leaf_text(c) for c in node["children"])
        return node.get("text", "") or ""

    parts = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        btype = block.get("type")
        if btype == "list":
            for item in block.get("children", []) or []:
                line = leaf_text(item).strip()
                if line:
                    parts.append(f"- {line}")
        else:
            line = leaf_text(block).strip()
            if line:
                parts.append(line)
    return "\n\n".join(parts).strip()


def download_pdf_text(url: str, max_pages: int = 200) -> Optional[str]:
    """Download a PDF and extract text using pdfplumber."""
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=90)
        resp.raise_for_status()
        if len(resp.content) > 50_000_000:  # Skip PDFs > 50MB
            logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
            return None
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(resp.content)
            tmp_path = f.name
        try:
            with pdfplumber.open(tmp_path) as pdf:
                pages_text = []
                for page in pdf.pages[:max_pages]:
                    page_text = page.extract_text()
                    if page_text:
                        pages_text.append(page_text)
                    # pdfplumber caches every visited page's layout for the
                    # document's lifetime; flush it or a long report peaks at
                    # several GB and OOMs a small fleet box (see #1328 family).
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                return "\n\n".join(pages_text) if pages_text else None
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        logger.warning(f"PDF extraction failed for {url}: {e}")
        return None


def api_get(collection: str, params: dict = None, timeout: int = 60) -> dict:
    """GET a Strapi collection endpoint and return the decoded payload."""
    resp = requests.get(
        f"{API_BASE}/{collection}",
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def paginate(collection: str, extra_params: dict = None, page_size: int = 100,
             max_pages: int = 100) -> Generator[dict, None, None]:
    """Paginate a Strapi collection, populating relations (file, department)."""
    page = 1
    while page <= max_pages:
        params = {
            "populate": "*",
            "pagination[page]": page,
            "pagination[pageSize]": page_size,
            "sort": "updatedAt:desc",
        }
        if extra_params:
            params.update(extra_params)
        payload = api_get(collection, params=params)
        data = payload.get("data") or []
        for item in data:
            yield item

        pagination = (payload.get("meta") or {}).get("pagination") or {}
        page_count = pagination.get("pageCount", 1)
        if page >= page_count or not data:
            break
        page += 1
        time.sleep(1.0)


class NICScraper(BaseScraper):
    """
    Scraper for GH/NIC — Ghana National Insurance Commission.
    Country: GH
    URL: https://nic.gov.gh/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _normalize_item(self, collection: str, item: dict) -> Optional[dict]:
        """Normalize one Strapi entry; PDF-backed entries get their text extracted."""
        kind, path_prefix = COLLECTIONS[collection]

        title = (item.get("title") or "").strip()
        slug = item.get("slug") or ""
        if not title:
            return None

        # Detail routes exist for news and notices; the document collections are
        # rendered as list pages, so link those to the listing.
        if kind in ("news", "notice") and slug:
            url = f"{SITE_BASE}{path_prefix}/{slug}"
        else:
            url = f"{SITE_BASE}{path_prefix}"

        pdf_url = None
        text = ""
        file_meta = item.get("file")
        if isinstance(file_meta, dict) and file_meta.get("url"):
            pdf_url = file_meta["url"]
            logger.info(f"Downloading PDF: {pdf_url}")
            text = download_pdf_text(pdf_url) or ""
            time.sleep(1.0)

        if len(text) < MIN_TEXT_LENGTH:
            inline = blocks_to_text(item.get("body"))
            if len(inline) >= MIN_TEXT_LENGTH:
                text = inline

        if len(text) < MIN_TEXT_LENGTH:
            logger.warning(
                f"Skipping {collection}/{slug or item.get('id')} '{title[:60]}': "
                f"insufficient text ({len(text)} chars)"
            )
            return None

        # `publications` carry an explicit publication year; everything else
        # dates from when the Commission published the entry.
        date_str = None
        year = item.get("year")
        if isinstance(year, int) and 1900 < year < 2100:
            date_str = f"{year}-01-01"
        else:
            published = item.get("publishedAt") or item.get("createdAt") or ""
            date_str = published[:10] or None

        record = {
            "_id": f"GH/NIC/{collection}-{item.get('documentId') or item.get('id')}",
            "_source": "GH/NIC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": url,
            "document_type": kind,
            "pdf_url": pdf_url,
            "collection": collection,
            "slug": slug or None,
            "modified": (item.get("updatedAt") or "")[:10] or None,
        }
        for optional in ("docType", "pubType", "category", "reference", "summary", "excerpt"):
            if item.get(optional):
                record[optional] = item[optional]
        return record

    def normalize(self, raw: dict) -> dict:
        """Records are already normalized by _normalize_item."""
        return raw

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield every NIC document across the four Strapi collections."""
        yielded = 0
        for collection in COLLECTIONS:
            logger.info(f"Fetching {collection}...")
            for item in paginate(collection):
                record = self._normalize_item(collection, item)
                if record:
                    yield record
                    yielded += 1
                    logger.info(
                        f"[{yielded}] {record['document_type']}: "
                        f"{record['title'][:60]} ({len(record['text'])} chars)"
                    )
        logger.info(f"fetch_all complete: {yielded} records")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Yield entries updated since `since`.

        `since` may be a datetime (what update() passes), a date or a string;
        as_date_str() reduces all of them to YYYY-MM-DD. Interpolating the raw
        datetime is what produced the malformed filter behind #1511.
        """
        cutoff = as_date_str(since)
        if not cutoff:
            yield from self.fetch_all()
            return

        params = {"filters[updatedAt][$gte]": f"{cutoff}T00:00:00.000Z"}
        for collection in COLLECTIONS:
            logger.info(f"Fetching {collection} updated since {cutoff}...")
            for item in paginate(collection, extra_params=params):
                record = self._normalize_item(collection, item)
                if record:
                    yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="GH/NIC — Ghana National Insurance Commission"
    )
    subparsers = parser.add_subparsers(dest="command")

    for name in ("bootstrap", "bootstrap-fast"):
        bp = subparsers.add_parser(name, help="Full initial fetch")
        bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
        bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
        bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = NICScraper()

    if args.command == "test":
        logger.info("Testing NIC Strapi connectivity...")
        try:
            for collection in COLLECTIONS:
                payload = api_get(collection, params={"pagination[pageSize]": 1})
                total = (
                    (payload.get("meta") or {}).get("pagination") or {}
                ).get("total", "?")
                logger.info(f"{collection}: {total} records available")

            payload = api_get(
                "regulatory-docs", params={"populate": "*", "pagination[pageSize]": 1}
            )
            data = payload.get("data") or []
            if not data:
                raise RuntimeError("regulatory-docs returned no entries")
            record = scraper._normalize_item("regulatory-docs", data[0])
            if not record:
                raise RuntimeError("could not normalize the first regulatory-doc")
            logger.info(f"Sample: {record['title'][:80]} ({len(record['text'])} chars)")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
