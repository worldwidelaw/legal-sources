#!/usr/bin/env python3
"""
GR/ConsumerOmbudsman -- Hellenic Consumer Ombudsman Data Fetcher

Fetches recommendations and findings from the Hellenic Consumer Ombudsman
(Συνήγορος του Καταναλωτή / synigoroskatanaloti.gr).

Strategy:
  - Paginate through /systaseis-porismata?page=N (6 items/page, ~56 pages)
  - Extract recommendation links from list pages
  - For each recommendation, fetch the detail page
  - Extract text from HTML body + download attached PDF for full text
  - Full text comes from PDF attachment (pypdf) or HTML body as fallback

Endpoints:
  - List: https://www.synigoroskatanaloti.gr/systaseis-porismata?page=N
  - Detail: https://www.synigoroskatanaloti.gr/el/{slug}
  - PDF: https://www.synigoroskatanaloti.gr/sites/default/files/recommendations/...

Data:
  - ~336 recommendations across 56 pages
  - Categories: Insurance, Energy, Financial Services, Transport, Health, etc.
  - Outcomes: Accepted, Not accepted, Ex officio
  - Language: Greek
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.consumer-ombudsman")

BASE_URL = "https://www.synigoroskatanaloti.gr"
LIST_PATH = "/systaseis-porismata"
ITEMS_PER_PAGE = 6


class GreekConsumerOmbudsmanScraper(BaseScraper):
    """
    Scraper for GR/ConsumerOmbudsman -- Hellenic Consumer Ombudsman.
    Country: GR
    URL: https://www.synigoroskatanaloti.gr

    Data types: doctrine
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "el,en",
            },
            timeout=60,
        )

    def _get_max_page(self) -> int:
        """Determine the last page number from pagination."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"{LIST_PATH}?page=0")
            resp.raise_for_status()
            pages = re.findall(r'page=(\d+)', resp.text)
            if pages:
                return max(int(p) for p in pages)
            return 0
        except Exception as e:
            logger.error(f"Failed to get pagination: {e}")
            return 0

    def _scrape_list_page(self, page: int) -> List[Dict[str, Any]]:
        """Scrape a list page and return recommendation entries."""
        items = []
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"{LIST_PATH}?page={page}")
            resp.raise_for_status()
            content = resp.text

            # Extract recommendation links with titles
            # Links follow pattern: /el/{slug} with meaningful long slugs
            link_pattern = re.compile(
                r'<a\s+href="(/el/(?:apodekti|dimosiopoiisi|en-merei|mi-apodekti|aytepaggelti|'
                r'eggrafi-systasi|systasi-porisma)[a-z0-9-]*)"[^>]*>'
                r'(.*?)</a>',
                re.DOTALL | re.IGNORECASE
            )

            seen = set()
            for match in link_pattern.finditer(content):
                href = match.group(1)
                title_html = match.group(2)
                title = re.sub(r'<[^>]+>', '', title_html).strip()

                if href in seen or len(title) < 20:
                    continue
                seen.add(href)

                slug = href.split('/el/')[-1]
                items.append({
                    "slug": slug,
                    "url": href,
                    "title": title,
                })

            # If the specific pattern didn't match enough, try a broader approach
            if len(items) < 3:
                broad_pattern = re.compile(
                    r'<a\s+href="(/el/[a-z0-9-]{30,})"[^>]*>(.*?)</a>',
                    re.DOTALL | re.IGNORECASE
                )
                for match in broad_pattern.finditer(content):
                    href = match.group(1)
                    title_html = match.group(2)
                    title = re.sub(r'<[^>]+>', '', title_html).strip()

                    # Filter out non-recommendation pages
                    skip_slugs = ['systaseis-porismata', 'nea', 'nomothesia', 'gsis',
                                  'diasynoriakes', 'ti-einai', 'politiki', 'kodikas',
                                  'submit-complaint', 'contact-form', 'faqs']
                    if any(s in href for s in skip_slugs):
                        continue
                    if href in seen or len(title) < 30:
                        continue
                    seen.add(href)

                    slug = href.split('/el/')[-1]
                    items.append({
                        "slug": slug,
                        "url": href,
                        "title": title,
                    })

            logger.info(f"Page {page}: found {len(items)} recommendations")
            return items

        except Exception as e:
            logger.error(f"Failed to scrape page {page}: {e}")
            return []

    def _fetch_detail(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a detail page and extract content + PDF."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(item["url"])
            resp.raise_for_status()
            content = resp.text

            result = {"slug": item["slug"], "title": item["title"]}

            # Extract date from the page
            date_match = re.search(
                r'class="[^"]*date[^"]*"[^>]*>(\d{2}/\d{2}/\d{4})',
                content, re.IGNORECASE
            )
            if date_match:
                raw_date = date_match.group(1)
                try:
                    dt = datetime.strptime(raw_date, "%d/%m/%Y")
                    result["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    result["date"] = raw_date

            # Extract category (thematic unit)
            # Look for field labels like "Ενέργεια & Ύδρευση", "Ασφάλειες" etc.
            cat_match = re.search(
                r'field--name-field-category[^"]*"[^>]*>.*?'
                r'<div[^>]*class="[^"]*field__item[^"]*"[^>]*>(.*?)</div>',
                content, re.DOTALL | re.IGNORECASE
            )
            if cat_match:
                result["category"] = re.sub(r'<[^>]+>', '', cat_match.group(1)).strip()

            # Extract outcome (Αποδεκτές / Μη αποδεκτές / Αυτεπάγγελτες)
            outcome_match = re.search(
                r'field--name-field-category.*?field__item[^>]*>(.*?)</div>',
                content, re.DOTALL | re.IGNORECASE
            )
            if outcome_match:
                outcome_text = re.sub(r'<[^>]+>', '', outcome_match.group(1)).strip()
                result["outcome"] = outcome_text

            # Extract the body text from the HTML page
            body_text = ""
            article_match = re.search(r'<article[^>]*>(.*?)</article>', content, re.DOTALL)
            if article_match:
                article_html = article_match.group(1)
                # Get the main body field
                body_field = re.search(
                    r'field--name-body.*?field__item[^>]*>(.*?)</div>\s*</div>',
                    article_html, re.DOTALL | re.IGNORECASE
                )
                if body_field:
                    body_text = re.sub(r'<[^>]+>', ' ', body_field.group(1))
                    body_text = re.sub(r'\s+', ' ', body_text).strip()
                else:
                    # Fallback: get all text from article
                    body_text = re.sub(r'<[^>]+>', '\n', article_html)
                    body_text = re.sub(r'\n+', '\n', body_text).strip()
                    # Remove very short lines (nav items, etc.)
                    lines = [l.strip() for l in body_text.split('\n') if len(l.strip()) > 30]
                    body_text = '\n'.join(lines)

            result["html_text"] = body_text

            # Find PDF attachment
            pdf_match = re.search(
                r'href="(/sites/default/files/[^"]*\.pdf)"',
                content, re.IGNORECASE
            )
            if pdf_match:
                pdf_url = pdf_match.group(1)
                result["pdf_url"] = pdf_url

                # Download and extract text from PDF
                if HAS_PYPDF:
                    try:
                        self.rate_limiter.wait()
                        pdf_resp = self.client.get(pdf_url)
                        pdf_resp.raise_for_status()
                        reader = PdfReader(io.BytesIO(pdf_resp.content))
                        pdf_text = ""
                        for page in reader.pages:
                            page_text = page.extract_text()
                            if page_text:
                                pdf_text += page_text + "\n"
                        pdf_text = pdf_text.strip()
                        if pdf_text and len(pdf_text) > 100:
                            result["pdf_text"] = pdf_text
                            logger.debug(f"Extracted {len(pdf_text)} chars from PDF")
                    except Exception as e:
                        logger.warning(f"Failed to extract PDF text for {item['slug']}: {e}")

            # Use PDF text as primary, HTML text as fallback
            result["text"] = result.get("pdf_text", "") or result.get("html_text", "")

            if not result.get("text"):
                logger.warning(f"No text extracted for {item['slug']}")

            return result

        except Exception as e:
            logger.error(f"Failed to fetch detail for {item['slug']}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        slug = raw.get("slug", "unknown")
        date = raw.get("date", "")

        return {
            "_id": f"GR/ConsumerOmbudsman/{slug}",
            "_source": "GR/ConsumerOmbudsman",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": f"{BASE_URL}{raw.get('url', '/el/' + slug)}",
            "category": raw.get("category", ""),
            "outcome": raw.get("outcome", ""),
            "pdf_url": f"{BASE_URL}{raw['pdf_url']}" if raw.get("pdf_url") else None,
            "language": "el",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all recommendations (raw dicts)."""
        max_page = self._get_max_page()
        logger.info(f"Total pages: {max_page + 1}")

        for page in range(0, max_page + 1):
            items = self._scrape_list_page(page)
            for item in items:
                detail = self._fetch_detail(item)
                if detail:
                    yield detail

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield recently added recommendations (first few pages)."""
        for page in range(0, 5):
            items = self._scrape_list_page(page)
            for item in items:
                detail = self._fetch_detail(item)
                if detail:
                    if since and detail.get("date") and detail["date"] < since:
                        return
                    yield detail


def main():
    scraper = GreekConsumerOmbudsmanScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
