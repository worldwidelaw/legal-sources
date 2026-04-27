#!/usr/bin/env python3
"""
MU/BankOfMauritius -- Bank of Mauritius Guidelines & Regulatory Documents

Fetches banking/financial guidelines, regulatory guidance, and market guidelines
from the Bank of Mauritius website (Drupal 7).

Strategy:
  - Scrapes paginated listing pages from 4 sections:
    1. /financial-stability/supervision/guideline  (paginated, ~60 items)
    2. /monetary-policy/monetary-policy-framework/Guideline  (~1 item)
    3. /markets/guidelines  (~16 items)
    4. /bank-notes-coins/guidelines  (~1 item)
  - Visits each detail page to extract body text and PDF download link
  - Downloads PDFs and extracts full text via common.pdf_extract

Data:
  - Banking supervision guidelines (capital adequacy, risk management, etc.)
  - Monetary policy framework guidelines
  - Financial markets guidelines (sustainable bonds, etc.)
  - Regulatory circulars and guidance

License: Public regulatory data (Mauritius)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MU.BankOfMauritius")

BASE_URL = "https://www.bom.mu"

# Sections containing guidelines with their listing URL paths
GUIDELINE_SECTIONS = [
    {
        "path": "/financial-stability/supervision/guideline",
        "category": "Banking Supervision",
        "paginated": True,
    },
    {
        "path": "/monetary-policy/monetary-policy-framework/Guideline",
        "category": "Monetary Policy",
        "paginated": False,
    },
    {
        "path": "/markets/guidelines",
        "category": "Financial Markets",
        "paginated": False,
    },
    {
        "path": "/bank-notes-coins/guidelines",
        "category": "Banknotes & Coins",
        "paginated": False,
    },
]

# Regex to parse listing items: date + link
LISTING_ITEM_RE = re.compile(
    r'date-calendar-left">([\d]+ \w+ \d{4})</div>\s*'
    r'<div class="col-sm-8 title-calendar-right">\s*'
    r'<a href="([^"]+)"[^>]*>(.*?)</a>',
    re.DOTALL,
)

# Regex to find PDF link in detail page
PDF_LINK_RE = re.compile(
    r"href=['\"]([^'\"]*\.pdf)['\"]",
    re.IGNORECASE,
)

# Regex to extract body text from Drupal field
BODY_RE = re.compile(
    r'field-name-body.*?content:encoded">(.*?)</div>\s*</div>\s*</div>',
    re.DOTALL,
)

# Regex for pagination
PAGER_RE = re.compile(r'page=(\d+)')


def _strip_html(html_text: str) -> str:
    """Remove HTML tags and decode entities."""
    import html as html_module
    text = re.sub(r'<br\s*/?\s*>', '\n', html_text)
    text = re.sub(r'<p[^>]*>', '\n\n', text)
    text = re.sub(r'</p>', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date like '06 January 2026' to ISO format."""
    try:
        dt = datetime.strptime(date_str.strip(), "%d %B %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


class BankOfMauritiusScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )

    def _fetch_listing_page(self, path: str, page: int = 0) -> str:
        """Fetch a listing page, optionally with pagination."""
        url = path if page == 0 else f"{path}?page={page}"
        resp = self.client.get(url)
        return resp.text if resp else ""

    def _get_all_guideline_links(self) -> List[Dict[str, str]]:
        """Scrape all guideline listing pages and return detail page info."""
        all_items = []
        seen_urls = set()

        for section in GUIDELINE_SECTIONS:
            path = section["path"]
            category = section["category"]
            logger.info(f"Scanning section: {category} ({path})")

            page = 0
            while True:
                html = self._fetch_listing_page(path, page)
                if not html:
                    break

                items = LISTING_ITEM_RE.findall(html)

                # Fallback: if no standard listing items, extract links from article body
                if not items:
                    body_links = re.findall(
                        r'href="(/markets/guidelines/[^"]+|/bank-notes-coins/guidelines/[^"]+|'
                        r'/monetary-policy/[^"]+/[Gg]uideline[^"]*)"[^>]*>([^<]+)</a>',
                        html
                    )
                    for link_url, link_title in body_links:
                        link_title = link_title.strip()
                        if link_url not in seen_urls and link_title:
                            seen_urls.add(link_url)
                            all_items.append({
                                "date_str": "",
                                "url": link_url,
                                "title": link_title,
                                "category": category,
                            })
                    break

                for date_str, url, title in items:
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    all_items.append({
                        "date_str": date_str.strip(),
                        "url": url,
                        "title": _strip_html(title).strip(),
                        "category": category,
                    })

                if not section["paginated"]:
                    break

                # Check for next page
                page_nums = [int(p) for p in PAGER_RE.findall(html)]
                if not page_nums or page >= max(page_nums):
                    break
                page += 1
                time.sleep(1)

            logger.info(f"  Found {len([i for i in all_items if i['category'] == category])} items in {category}")

        logger.info(f"Total guideline items found: {len(all_items)}")
        return all_items

    def _fetch_detail_page(self, url: str) -> Dict[str, Any]:
        """Fetch a detail page and extract body text and PDF link."""
        resp = self.client.get(url)
        if not resp:
            return {"body": "", "pdf_url": None}

        html = resp.text

        # Extract body text
        body_match = BODY_RE.search(html)
        body = _strip_html(body_match.group(1)) if body_match else ""

        # Extract PDF link - look in the article/content area
        # The PDF link is typically in a tblDownloadFile table
        pdf_url = None
        pdf_matches = PDF_LINK_RE.findall(html)
        for pdf in pdf_matches:
            # Skip favicon and social media icons
            if "favicon" in pdf.lower() or "icon" in pdf.lower():
                continue
            # Prefer links from /sites/default/files/
            if "/sites/default/files/" in pdf:
                pdf_url = pdf if pdf.startswith("http") else urljoin(BASE_URL, pdf)
                break
        if not pdf_url and pdf_matches:
            # Fallback to first non-icon PDF
            for pdf in pdf_matches:
                if "favicon" not in pdf.lower() and "icon" not in pdf.lower():
                    pdf_url = pdf if pdf.startswith("http") else urljoin(BASE_URL, pdf)
                    break

        # Extract published date from meta
        pub_match = re.search(r'article:published_time.*?content="([^"]+)"', html)
        published = pub_match.group(1)[:10] if pub_match else None

        return {"body": body, "pdf_url": pdf_url, "published": published}

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all guideline documents with full text."""
        items = self._get_all_guideline_links()

        for i, item in enumerate(items):
            logger.info(f"[{i+1}/{len(items)}] Fetching: {item['title'][:60]}...")

            detail = self._fetch_detail_page(item["url"])
            time.sleep(1)

            # Determine the date
            date = _parse_date(item["date_str"]) or detail.get("published")

            # Get full text from PDF
            full_text = None
            if detail["pdf_url"]:
                source_id = hashlib.md5(item["url"].encode()).hexdigest()
                try:
                    full_text = extract_pdf_markdown(
                        source="MU/BankOfMauritius",
                        source_id=source_id,
                        pdf_url=detail["pdf_url"],
                        table="doctrine",
                    )
                except Exception as e:
                    logger.warning(f"  PDF extraction failed: {e}")

            # Use body text as fallback if PDF extraction fails
            text = full_text or detail.get("body", "")

            if not text:
                logger.warning(f"  No text extracted for: {item['title']}")
                continue

            yield self.normalize({
                "url": item["url"],
                "title": item["title"],
                "date": date,
                "text": text,
                "category": item["category"],
                "pdf_url": detail.get("pdf_url"),
                "body_summary": detail.get("body", "")[:500] if detail.get("body") else None,
            })

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield documents updated since the given date."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        full_url = raw["url"]
        if not full_url.startswith("http"):
            full_url = BASE_URL + full_url

        doc_id = hashlib.md5(raw["url"].encode()).hexdigest()

        return {
            "_id": f"MU/BankOfMauritius/{doc_id}",
            "_source": "MU/BankOfMauritius",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": full_url,
            "category": raw.get("category"),
            "pdf_url": raw.get("pdf_url"),
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="MU/BankOfMauritius scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    scraper = BankOfMauritiusScraper()

    if args.command == "test":
        logger.info("Testing connectivity to bom.mu...")
        resp = scraper.client.get("/financial-stability/supervision/guideline")
        if resp and resp.status_code == 200:
            items = LISTING_ITEM_RE.findall(resp.text)
            logger.info(f"OK — found {len(items)} guidelines on first listing page")
        else:
            logger.error(f"FAIL — status {resp.status_code if resp else 'no response'}")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        for doc in scraper.fetch_all():
            count += 1
            text_len = len(doc.get("text", ""))
            logger.info(
                f"  #{count} {doc['title'][:50]}... "
                f"({text_len} chars)"
            )

            # Save sample
            if count <= 20:
                import json
                fname = re.sub(r'[^\w\-]', '_', doc["_id"])[:80] + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)

            if count >= max_records:
                break

        logger.info(f"Done — {count} records fetched")


if __name__ == "__main__":
    main()
