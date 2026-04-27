#!/usr/bin/env python3
"""
BH/NBR-TaxGuidance -- Bahrain National Bureau for Revenue Tax Guidance

Fetches tax guidance documents (VAT, excise, DMTT guides and NBR decisions)
from the Bahrain National Bureau for Revenue at nbr.gov.bh.

Strategy:
  - Set English language via /language/en session cookie
  - Scrape guidelines_and_publications page for publication slugs
  - Scrape /decisions page for decision links
  - For each publication, extract S3 PDF URL from Adobe DC View SDK JS
  - Download PDFs and extract full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import hashlib
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen, build_opener, HTTPCookieProcessor
from urllib.error import HTTPError, URLError
from http.cookiejar import CookieJar

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BH.NBR-TaxGuidance")

BASE_URL = "https://www.nbr.gov.bh"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"


class BHNBRScraper(BaseScraper):
    SOURCE_ID = "BH/NBR-TaxGuidance"

    def __init__(self):
        self.cookie_jar = CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cookie_jar))
        self._set_english()

    def _set_english(self):
        """Set English language session cookie."""
        req = Request(f"{BASE_URL}/language/en", headers={"User-Agent": USER_AGENT})
        try:
            self.opener.open(req, timeout=15)
            logger.info("English language cookie set")
        except Exception as e:
            logger.warning(f"Failed to set English cookie: {e}")

    def _fetch_page(self, path: str, timeout: int = 30) -> Optional[str]:
        """Fetch an HTML page with session cookies."""
        url = f"{BASE_URL}{path}" if path.startswith("/") else path
        req = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            resp = self.opener.open(req, timeout=timeout)
            return resp.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError) as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _extract_publication_slugs(self, page_html: str) -> List[str]:
        """Extract publication slugs from the guidelines page."""
        pattern = r'/publications/view/([^"\']+)'
        slugs = re.findall(pattern, page_html)
        return list(dict.fromkeys(slugs))  # deduplicate preserving order

    def _extract_decision_links(self, page_html: str) -> List[Dict[str, str]]:
        """Extract decision links from the decisions page."""
        # Look for links to /media/decision* or direct PDF links
        decisions = []
        # Pattern for decision detail pages
        pattern = r'href="(/media/[^"]*decision[^"]*)"'
        links = re.findall(pattern, page_html, re.IGNORECASE)
        # Also look for direct S3 PDF links in decisions page
        s3_pattern = r'(https://nbrproduserdata[^"\']+\.pdf)'
        s3_links = re.findall(s3_pattern, page_html)

        for link in links:
            slug = link.split("/")[-1]
            decisions.append({"slug": slug, "path": link})

        for url in s3_links:
            slug = url.split("/")[-1].replace(".pdf", "")
            decisions.append({"slug": slug, "pdf_url": url})

        return decisions

    def _extract_pdf_url(self, page_html: str) -> Optional[str]:
        """Extract S3 PDF URL from Adobe DC View SDK JavaScript."""
        # Primary: Adobe DC View SDK
        pattern = r'url:\s*"(https://nbrproduserdata[^"]+\.pdf)"'
        match = re.search(pattern, page_html)
        if match:
            return match.group(1)

        # Fallback: any S3 PDF link
        pattern2 = r'(https://nbrproduserdata[^"\']+\.pdf)'
        match2 = re.search(pattern2, page_html)
        if match2:
            return match2.group(1)

        # Fallback: download link
        pattern3 = r'href="([^"]*\.pdf)"'
        match3 = re.search(pattern3, page_html)
        if match3:
            url = match3.group(1)
            if url.startswith("http"):
                return url
            return f"{BASE_URL}{url}"

        return None

    def _extract_title(self, page_html: str, slug: str) -> str:
        """Extract document title from page."""
        # Try page title
        title_match = re.search(r'<h[12][^>]*>([^<]+)</h[12]>', page_html)
        if title_match:
            return html.unescape(title_match.group(1).strip())

        # Try meta title
        meta_match = re.search(r'<title>([^<]+)</title>', page_html)
        if meta_match:
            title = html.unescape(meta_match.group(1).strip())
            title = re.sub(r'\s*[-|]\s*NBR.*$', '', title)
            if title:
                return title

        # Fallback: humanize slug
        return slug.replace("_", " ").replace("-", " ").title()

    def _list_publications(self) -> List[Dict[str, str]]:
        """Get all publication slugs from the guidelines page."""
        page = self._fetch_page("/guidelines_and_publications")
        if not page:
            # Try alternate URL
            page = self._fetch_page("/vat_guideline")
        if not page:
            logger.error("Cannot access guidelines page")
            return []

        slugs = self._extract_publication_slugs(page)
        logger.info(f"Found {len(slugs)} publications")
        return [{"slug": s, "type": "publication"} for s in slugs]

    def _list_decisions(self) -> List[Dict[str, Any]]:
        """Get all decisions from the decisions page."""
        page = self._fetch_page("/decisions")
        if not page:
            return []

        decisions = self._extract_decision_links(page)
        logger.info(f"Found {len(decisions)} decisions")
        return decisions

    def _categorize(self, slug: str, title: str) -> str:
        """Determine document category from slug/title."""
        slug_lower = slug.lower()
        title_lower = title.lower()
        if "dmtt" in slug_lower or "dmtt" in title_lower or "minimum top-up" in title_lower:
            return "DMTT"
        if "excise" in slug_lower or "excise" in title_lower:
            return "Excise"
        if "decision" in slug_lower or "decision" in title_lower:
            return "Decision"
        return "VAT"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all publications and decisions with full text."""
        publications = self._list_publications()
        decisions = self._list_decisions()

        # Process publications
        for pub in publications:
            slug = pub["slug"]
            time.sleep(1.5)

            page = self._fetch_page(f"/publications/view/{slug}")
            if not page:
                logger.warning(f"Skipping publication {slug}: page not accessible")
                continue

            pdf_url = self._extract_pdf_url(page)
            if not pdf_url:
                logger.warning(f"Skipping publication {slug}: no PDF URL found")
                continue

            title = self._extract_title(page, slug)

            # Extract text from PDF
            text = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=slug,
                pdf_url=pdf_url,
                table="doctrine",
            )
            if not text or len(text.strip()) < 100:
                logger.warning(f"Skipping {slug}: insufficient text ({len(text) if text else 0} chars)")
                continue

            yield self.normalize({
                "doc_id": slug,
                "title": title,
                "text": text,
                "url": f"{BASE_URL}/publications/view/{slug}",
                "pdf_url": pdf_url,
                "category": self._categorize(slug, title),
            })

        # Process decisions
        for dec in decisions:
            slug = dec.get("slug", "")
            time.sleep(1.5)

            pdf_url = dec.get("pdf_url")
            if not pdf_url and "path" in dec:
                # Fetch decision page to get PDF URL
                page = self._fetch_page(dec["path"])
                if page:
                    pdf_url = self._extract_pdf_url(page)

            if not pdf_url:
                logger.warning(f"Skipping decision {slug}: no PDF URL")
                continue

            title = f"NBR Decision - {slug.replace('_', ' ').title()}"

            text = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=f"decision_{slug}",
                pdf_url=pdf_url,
                table="doctrine",
            )
            if not text or len(text.strip()) < 100:
                logger.warning(f"Skipping decision {slug}: insufficient text")
                continue

            yield self.normalize({
                "doc_id": f"decision_{slug}",
                "title": title,
                "text": text,
                "url": f"{BASE_URL}/decisions",
                "pdf_url": pdf_url,
                "category": "Decision",
            })

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all (no date filtering available)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": None,  # No dates on individual publications
            "url": raw["url"],
            "category": raw.get("category", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ─── CLI Entry Point ─────────────���────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BH/NBR-TaxGuidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = BHNBRScraper()

    if args.command == "test":
        page = scraper._fetch_page("/guidelines_and_publications")
        if page:
            slugs = scraper._extract_publication_slugs(page)
            print(f"OK: Found {len(slugs)} publications")
        else:
            print("FAIL: Cannot access NBR website")
            sys.exit(1)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    for record in scraper.fetch_all():
        count += 1
        # Save to sample/
        fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        logger.info(f"[{count}] {record['title'][:60]} ({text_len} chars)")

        if count >= limit:
            logger.info(f"Sample limit reached ({limit} records)")
            break

    print(f"\nDone: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
