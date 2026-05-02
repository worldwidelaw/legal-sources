#!/usr/bin/env python3
"""
ZA/SARB-Notices -- South African Reserve Bank Prudential Authority Publications

Fetches regulatory doctrine from SARB: Banks Directives, Banks Circulars,
Guidance Notes, PA Communications, Insurance Act Notices, and Consultation
Documents. ~950+ PDF documents.

Data access:
  - Sitemap at resbank.co.za/sitemap.xml lists all publication detail pages
  - Each detail page links to one or more PDFs at /content/dam/sarb/...
  - Text extracted via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
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
logger = logging.getLogger("legal-data-hunter.ZA.SARB-Notices")

BASE_URL = "https://www.resbank.co.za"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
DELAY = 2.0

# Sitemap URL path prefixes for regulatory publication detail pages
CATEGORY_PREFIXES = {
    "prudential-authority/pa-deposit-takers/banks-directives": "Banks Directive",
    "prudential-authority/pa-deposit-takers/banks-circulars": "Banks Circular",
    "prudential-authority/pa-deposit-takers/banks-guidance-notes": "Banks Guidance Note",
    "prudential-authority/pa-public-awareness/Communication": "PA Communication",
    "prudential-authority/pa-insurers/pa-post-insurance/act-notices": "Insurance Act Notice",
    "prudential-authority/pa-documents-issued-for-consultation": "Consultation Document",
    "pa-financial-sector-regulations-prudential-standards": "Prudential Standard",
    "cop-guidance-notice": "COP Guidance Notice",
    "national-payment-system/Directives": "NPS Directive",
}

DETAIL_PATH = "/en/home/publications/publication-detail-pages/"


class SARBNoticesScraper(BaseScraper):
    def __init__(self):
        self.source_dir = Path(__file__).parent
        self.config = self._load_config()
        self.status = self._load_status()
        self.http = HttpClient()
        self.sample_dir = self.source_dir / "sample"
        self.sample_dir.mkdir(exist_ok=True)

    def _extract_urls_from_sitemap(self) -> List[Tuple[str, str]]:
        """Parse sitemap.xml and return (url, category) tuples for regulatory pages."""
        logger.info("Fetching sitemap from %s", SITEMAP_URL)
        resp = self.http.get(SITEMAP_URL, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch sitemap: HTTP {resp.status_code}")

        xml_text = resp.text
        results = []

        # Extract all <loc> URLs
        urls = re.findall(r'<loc>([^<]+)</loc>', xml_text)
        for url in urls:
            if DETAIL_PATH not in url:
                continue
            path_after_detail = url.split(DETAIL_PATH, 1)[1]
            for prefix, category in CATEGORY_PREFIXES.items():
                if path_after_detail.startswith(prefix):
                    results.append((url, category))
                    break

        logger.info("Found %d regulatory publication pages in sitemap", len(results))
        return results

    def _parse_detail_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a detail page and extract title, date, and PDF URL(s)."""
        try:
            resp = self.http.get(url, timeout=20)
            if resp.status_code != 200:
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

        page_html = resp.text

        # Extract title from <title> tag
        title_match = re.search(r'<title>([^<]+)</title>', page_html)
        title = html.unescape(title_match.group(1).strip()) if title_match else ""
        if title.lower() in ("page not found", ""):
            logger.warning("Page not found: %s", url)
            return None

        # Extract PDF links
        pdf_links = re.findall(
            r'href="(/content/dam/[^"]+\.pdf)"',
            page_html,
            re.IGNORECASE,
        )
        # Deduplicate while preserving order
        seen = set()
        unique_pdfs = []
        for link in pdf_links:
            if link not in seen:
                seen.add(link)
                unique_pdfs.append(link)

        if not unique_pdfs:
            logger.warning("No PDF links found on %s", url)
            return None

        # Extract dates
        pub_date = None
        mod_date = None

        pub_match = re.search(
            r'Published Date:</div>\s*<div[^>]*>\s*(\d{4}-\d{2}-\d{2})',
            page_html,
        )
        if pub_match:
            pub_date = pub_match.group(1)

        mod_match = re.search(
            r'Last Modified Date:</div>\s*<div[^>]*>\s*(\d{4}-\d{2}-\d{2})',
            page_html,
        )
        if mod_match:
            mod_date = mod_match.group(1)

        # Fallback: extract year from URL path
        year_match = re.search(r'/(\d{4})/', url)
        year = year_match.group(1) if year_match else None

        date = pub_date or mod_date or (f"{year}-01-01" if year else None)

        # Extract citation year from meta
        citation_year = None
        cy_match = re.search(r'Citation_year"\s+content="(\d{4})"', page_html)
        if cy_match:
            citation_year = cy_match.group(1)

        return {
            "title": title,
            "url": url,
            "pdf_urls": [f"{BASE_URL}{p}" for p in unique_pdfs],
            "date": date,
            "year": citation_year or year,
        }

    def _make_id(self, url: str) -> str:
        """Generate a stable ID from the URL path."""
        path = url.replace(BASE_URL, "").replace(DETAIL_PATH, "")
        # Simplify to something like "banks-directives/2020/9821"
        return path.strip("/").replace("/", "-")

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Yield all regulatory documents with full text."""
        url_categories = self._extract_urls_from_sitemap()

        if sample:
            # Take a diverse sample: a few from each category
            from collections import defaultdict
            by_cat = defaultdict(list)
            for url, cat in url_categories:
                by_cat[cat].append((url, cat))
            sampled = []
            for cat, items in by_cat.items():
                sampled.extend(items[:3])
            url_categories = sampled[:15]
            logger.info("Sample mode: processing %d pages", len(url_categories))

        count = 0
        errors = 0
        for url, category in url_categories:
            time.sleep(DELAY)

            meta = self._parse_detail_page(url)
            if not meta:
                errors += 1
                continue

            # Try first PDF
            pdf_url = meta["pdf_urls"][0]
            doc_id = self._make_id(url)

            time.sleep(DELAY)
            text = extract_pdf_markdown(
                source="ZA/SARB-Notices",
                source_id=doc_id,
                pdf_url=pdf_url,
                table="doctrine",
            )

            if not text or len(text.strip()) < 50:
                # Try other PDFs if first one failed
                for alt_pdf in meta["pdf_urls"][1:]:
                    time.sleep(DELAY)
                    text = extract_pdf_markdown(
                        source="ZA/SARB-Notices",
                        source_id=doc_id,
                        pdf_url=alt_pdf,
                        table="doctrine",
                    )
                    if text and len(text.strip()) >= 50:
                        pdf_url = alt_pdf
                        break

            if not text or len(text.strip()) < 50:
                logger.warning("No text extracted from PDFs for %s", url)
                errors += 1
                continue

            record = self.normalize({
                "id": doc_id,
                "title": meta["title"],
                "text": text,
                "url": url,
                "pdf_url": pdf_url,
                "date": meta["date"],
                "year": meta["year"],
                "category": category,
            })

            count += 1
            yield record

        logger.info("Completed: %d records, %d errors", count, errors)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Incremental update - re-fetch all (sitemap doesn't support date filtering reliably)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        return {
            "_id": f"ZA-SARB-{raw['id']}",
            "_source": "ZA/SARB-Notices",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["date"],
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "year": raw.get("year"),
            "doc_category": raw["category"],
            "jurisdiction": "ZA",
            "institution": "South African Reserve Bank",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ZA/SARB-Notices scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SARBNoticesScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        urls = scraper._extract_urls_from_sitemap()
        logger.info("Sitemap returned %d regulatory publication URLs", len(urls))
        if urls:
            meta = scraper._parse_detail_page(urls[0][0])
            if meta:
                logger.info("Sample page: %s", meta["title"])
                logger.info("PDF URLs: %s", meta["pdf_urls"][:2])
                logger.info("Date: %s", meta["date"])
                print("PASS")
            else:
                print("FAIL: Could not parse detail page")
        else:
            print("FAIL: No URLs found in sitemap")
        return

    sample_mode = args.sample or not args.full
    if args.command == "bootstrap":
        sample_dir = scraper.sample_dir
        count = 0
        for record in scraper.fetch_all(sample=sample_mode):
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info("[%d] %s (%d chars)", count, record["title"][:60], len(record.get("text", "")))
            if sample_mode and count >= 15:
                break

        logger.info("Bootstrap complete: %d records saved to %s", count, sample_dir)

    elif args.command == "update":
        for record in scraper.fetch_updates():
            logger.info("Updated: %s", record["title"][:60])


if __name__ == "__main__":
    main()
