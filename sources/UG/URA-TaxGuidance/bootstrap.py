#!/usr/bin/env python3
"""
UG/URA-TaxGuidance -- Uganda Revenue Authority Tax Guidance

Fetches tax guidance, case digests, sector guides, compliance materials,
and tax rulings from URA. Documents discovered via WordPress REST API
and downloaded as PDFs.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UG.URA-TaxGuidance")

BASE_URL = "https://ura.go.ug"
WP_API = f"{BASE_URL}/wp-json/wp/v2/media"
DELAY = 2.0


def _make_id(source_url: str) -> str:
    """Generate a stable ID from the PDF URL."""
    name = unquote(source_url).split("/")[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(name) > 100:
        name = name[:100]
    return f"UG_URA_{name}"


def _clean_title(wp_title: str) -> str:
    """Clean up WP title (HTML entities, etc.)."""
    title = html.unescape(wp_title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _classify_doc_type(title: str, url: str) -> str:
    """Classify document type from title and URL."""
    t = title.lower()
    if "case digest" in t:
        return "case_digest"
    if "practice note" in t:
        return "practice_note"
    if "ruling" in t or "advance ruling" in t:
        return "ruling"
    if "guide" in t or "guidance" in t:
        return "guide"
    if "circular" in t:
        return "circular"
    if "waiver" in t:
        return "tax_waiver"
    if "act" in t or "law" in t or "regulation" in t or "amendment" in t:
        return "legislation"
    if "newsletter" in t:
        return "newsletter"
    if "data book" in t or "report" in t or "annual" in t:
        return "report"
    if "compliance" in t or "aeo" in t or "authorized economic" in t:
        return "compliance"
    if "crs" in t or "exchange of information" in t:
        return "crs_reporting"
    return "guidance"


def _extract_date(wp_date: str) -> Optional[str]:
    """Extract ISO date from WP date field."""
    if not wp_date:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", wp_date)
    return m.group(1) if m else None


class URATaxGuidanceScraper(BaseScraper):
    """Scraper for Uganda Revenue Authority tax guidance documents."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
            },
        )

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Discover all English PDF documents via WP REST API."""
        all_docs = []
        seen_urls = set()
        page = 1

        while page <= 30:
            try:
                resp = self.http.get(
                    WP_API,
                    params={
                        "per_page": 20,
                        "page": page,
                        "mime_type": "application/pdf",
                        "lang": "en",
                    },
                    timeout=30,
                )
                if resp.status_code != 200:
                    break
                items = resp.json()
                if not items:
                    break

                for item in items:
                    source_url = item.get("source_url", "")
                    if not source_url or source_url in seen_urls:
                        continue
                    seen_urls.add(source_url)

                    title = _clean_title(item.get("title", {}).get("rendered", ""))
                    if not title:
                        continue

                    all_docs.append({
                        "wp_id": item.get("id", 0),
                        "source_url": source_url,
                        "title": title,
                        "date": _extract_date(item.get("date", "")),
                        "doc_type": _classify_doc_type(title, source_url),
                    })

                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1
                time.sleep(1.0)
            except Exception as e:
                logger.warning("API error page %d: %s", page, e)
                break

        logger.info("Total unique English PDF documents discovered: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, source_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        pdf_url = source_url
        if pdf_url.startswith("/"):
            pdf_url = f"{BASE_URL}{pdf_url}"
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("UG/URA-TaxGuidance", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all URA guidance documents with full text."""
        all_docs = self._discover_documents()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = _make_id(doc["source_url"])
            logger.info("Processing: %s", doc["title"][:80])

            text = self._download_and_extract(doc["source_url"], doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", doc_id)
                continue

            source_url = doc["source_url"]
            if source_url.startswith("/"):
                source_url = f"{BASE_URL}{source_url}"

            yield {
                "_id": doc_id,
                "wp_id": doc["wp_id"],
                "title": doc["title"],
                "date": doc["date"],
                "doc_type": doc["doc_type"],
                "pdf_url": source_url,
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates — re-fetch all for this collection."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "UG/URA-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "doc_type": raw.get("doc_type", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UG/URA-TaxGuidance bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = URATaxGuidanceScraper()

    if args.command == "test":
        docs = scraper._discover_documents()
        print(f"OK — found {len(docs)} documents")
        types = {}
        for d in docs:
            t = d["doc_type"]
            types[t] = types.get(t, 0) + 1
        for t, c in sorted(types.items()):
            print(f"  {t}: {c}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
