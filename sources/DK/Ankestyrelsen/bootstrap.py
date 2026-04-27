#!/usr/bin/env python3
"""
DK/Ankestyrelsen -- Danish National Appeals Board Principle Decisions

Fetches principmeddelelser (binding administrative precedent) from Ankestyrelsen.

Strategy:
  1. Parse sitemap.xml for all decision URLs (~5,800)
  2. Fetch each decision page and extract full text from HTML
  3. Extract metadata: title, dates, categories, journal number

Content structure:
  - rich-text divs contain the decision body
  - Accordion sections: "Principmeddelelsen fastslår", "Den konkrete sag",
    "Baggrund for at offentliggøre", "Reglerne", metadata box
  - Categories as span.label-default elements

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import re
import html
import json
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.Ankestyrelsen")

BASE_URL = "https://www.ast.dk"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
URL_FILTER = "/for-myndigheder/principmeddelelser/"


class AnkestyrelsenScraper(BaseScraper):
    """
    Scraper for DK/Ankestyrelsen -- Danish principle decisions.
    Country: DK
    URL: https://ast.dk/afgorelser/principafgorelser

    Data types: doctrine (binding administrative precedent)
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _fetch_sitemap_urls(self) -> list[dict]:
        """
        Parse sitemap.xml and return all principmeddelelse URLs with lastmod dates.
        Filters out year index pages (URLs ending in just a year).
        """
        logger.info("Fetching sitemap...")
        self.rate_limiter.wait()
        resp = self.client.get("/sitemap.xml")
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        urls = []
        for url_elem in root.findall("sm:url", ns):
            loc = url_elem.find("sm:loc", ns)
            if loc is None or loc.text is None:
                continue

            url = loc.text.strip()
            if URL_FILTER not in url:
                continue

            # Skip year index pages like /principmeddelelser/2025
            path_after_filter = url.split(URL_FILTER, 1)[1] if URL_FILTER in url else ""
            if re.match(r"^\d{4}/?$", path_after_filter):
                continue

            lastmod = None
            lastmod_elem = url_elem.find("sm:lastmod", ns)
            if lastmod_elem is not None and lastmod_elem.text:
                lastmod = lastmod_elem.text.strip()

            urls.append({"url": url, "lastmod": lastmod})

        logger.info(f"Found {len(urls)} decision URLs in sitemap")
        return urls

    def _parse_decision_page(self, url: str) -> Optional[dict]:
        """
        Fetch and parse a single decision page.
        Returns raw dict with extracted content or None on failure.
        """
        # Convert full URL to relative path
        path = url.replace(BASE_URL, "")
        if not path.startswith("/"):
            path = "/" + path

        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)

            if resp.status_code == 404:
                logger.debug(f"404: {path}")
                return None

            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            logger.warning(f"Error fetching {path}: {e}")
            return None

        # Extract title from og:title
        title = ""
        title_match = re.search(r'og:title"\s+content="([^"]+)"', content)
        if title_match:
            title = html.unescape(title_match.group(1)).strip()

        if not title:
            # Fallback: try <title> tag
            title_match = re.search(r"<title>([^<]+)</title>", content)
            if title_match:
                title = html.unescape(title_match.group(1)).strip()
                # Remove " - Ankestyrelsen" suffix
                title = re.sub(r"\s*-\s*Ankestyrelsen\s*$", "", title)

        # Extract categories/labels
        categories = []
        for label_match in re.finditer(r'label-default">([^<]+)</span>', content):
            cat = html.unescape(label_match.group(1)).strip()
            if cat:
                categories.append(cat)

        # Extract rich-text sections (decision body)
        text_parts = []
        rich_texts = re.findall(
            r'<div class="rich-text">(.*?)</div>(?:\s*</div>)',
            content, re.DOTALL
        )

        for rt in rich_texts:
            clean = re.sub(r"<[^>]+>", " ", rt)
            clean = html.unescape(clean)
            clean = re.sub(r"\s+", " ", clean).strip()
            if clean and len(clean) > 10:
                text_parts.append(clean)

        # If rich-text extraction got too little, try broader extraction
        if sum(len(p) for p in text_parts) < 100:
            # Try all content within accordion sections
            accordion_sections = re.findall(
                r'class="section-body"[^>]*>(.*?)</div>\s*</div>',
                content, re.DOTALL
            )
            for sec in accordion_sections:
                clean = re.sub(r"<[^>]+>", " ", sec)
                clean = html.unescape(clean)
                clean = re.sub(r"\s+", " ", clean).strip()
                if clean and len(clean) > 10:
                    text_parts.append(clean)

        # Extract metadata from the metadata rich-text section
        signature_date = None
        publication_date = None
        paragraph = None
        journal_number = None

        meta_pattern = (
            r"Dato for underskrift.*?(\d{2}\.\d{2}\.\d{4}).*?"
            r"Offentligg.*?dato.*?(\d{2}\.\d{2}\.\d{4})"
        )
        meta_match = re.search(meta_pattern, " ".join(text_parts), re.DOTALL)
        if meta_match:
            sig = meta_match.group(1)
            pub = meta_match.group(2)
            try:
                signature_date = datetime.strptime(sig, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
            try:
                publication_date = datetime.strptime(pub, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract paragraph reference
        para_match = re.search(r"Paragraf\s+(§[^D]*?)(?:Journalnummer|$)", " ".join(text_parts))
        if para_match:
            paragraph = para_match.group(1).strip()

        # Extract journal number
        jn_match = re.search(r"Journalnummer\s+(\S+)", " ".join(text_parts))
        if jn_match:
            journal_number = jn_match.group(1).strip()

        # Remove the metadata section from text_parts (it's not decision content)
        filtered_parts = []
        for part in text_parts:
            if "Dato for underskrift" in part:
                continue
            if "Postadresse:" in part or "Nytorv 7" in part:
                continue
            if len(part) < 20:
                continue
            filtered_parts.append(part)

        full_text = "\n\n".join(filtered_parts)

        # Extract year from URL
        year_match = re.search(r"/principmeddelelser/(\d{4})/", url)
        year = int(year_match.group(1)) if year_match else None

        # Generate a stable ID from the URL slug
        slug = url.rstrip("/").split("/")[-1]
        decision_id = slug

        # Extract decision number from title if possible (e.g., "1-25", "P-14-76")
        num_match = re.search(
            r"principmeddelelse\s+(\S+)\s+om",
            title, re.IGNORECASE
        )
        if not num_match:
            num_match = re.search(
                r"principafgørelse\s+(\S+)",
                title, re.IGNORECASE
            )
        decision_number = num_match.group(1) if num_match else None

        return {
            "url": url,
            "decision_id": decision_id,
            "decision_number": decision_number,
            "title": title,
            "text": full_text,
            "year": year,
            "signature_date": signature_date,
            "publication_date": publication_date,
            "paragraph": paragraph,
            "journal_number": journal_number,
            "categories": categories,
            "status": "Gældende" if "Gældende" in categories else (
                "Historisk" if "Historisk" in categories else None
            ),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from the sitemap."""
        urls = self._fetch_sitemap_urls()

        for i, entry in enumerate(urls):
            if i > 0 and i % 100 == 0:
                logger.info(f"Progress: {i}/{len(urls)} decisions fetched")

            raw = self._parse_decision_page(entry["url"])
            if raw is None:
                continue

            # Add lastmod from sitemap
            raw["_sitemap_lastmod"] = entry.get("lastmod")

            yield raw

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions modified since the given date using sitemap lastmod."""
        urls = self._fetch_sitemap_urls()
        since_str = since.strftime("%Y-%m-%d")

        for entry in urls:
            lastmod = entry.get("lastmod", "")
            if lastmod and lastmod >= since_str:
                raw = self._parse_decision_page(entry["url"])
                if raw is not None:
                    raw["_sitemap_lastmod"] = entry.get("lastmod")
                    yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw decision data into standard schema."""
        if not raw.get("text") or len(raw["text"].strip()) < 50:
            logger.debug(f"Skipping {raw.get('decision_id', '?')}: no/insufficient text")
            return None

        # Use publication date, signature date, or year as fallback
        date = raw.get("publication_date") or raw.get("signature_date")
        if not date and raw.get("year"):
            date = f"{raw['year']}-01-01"

        return {
            "_id": raw["decision_id"],
            "_source": "DK/Ankestyrelsen",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": date,
            "url": raw.get("url", ""),
            "decision_number": raw.get("decision_number"),
            "journal_number": raw.get("journal_number"),
            "paragraph": raw.get("paragraph"),
            "categories": raw.get("categories", []),
            "status": raw.get("status"),
            "year": raw.get("year"),
        }


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="DK/Ankestyrelsen scraper")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample", action="store_true", help="Sample mode (10 records)"
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AnkestyrelsenScraper()

    if args.command == "test-api":
        logger.info("Testing sitemap access...")
        urls = scraper._fetch_sitemap_urls()
        logger.info(f"Sitemap: {len(urls)} decision URLs found")
        if urls:
            logger.info(f"First URL: {urls[0]['url']}")
            logger.info("Testing page fetch...")
            raw = scraper._parse_decision_page(urls[0]["url"])
            if raw:
                logger.info(f"Title: {raw['title']}")
                logger.info(f"Text length: {len(raw.get('text', ''))}")
                logger.info(f"Categories: {raw.get('categories', [])}")
                logger.info("API test PASSED")
            else:
                logger.error("Failed to parse decision page")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        last_run = scraper.status.get("last_run")
        if last_run:
            since = datetime.fromisoformat(last_run)
        else:
            since = datetime(2020, 1, 1, tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
