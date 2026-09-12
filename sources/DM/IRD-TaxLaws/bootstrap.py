#!/usr/bin/env python3
"""
DM/IRD-TaxLaws -- Dominica Inland Revenue Division Tax Laws & Guides

Scrapes tax doctrine content from ird.gov.dm sub-pages covering corporate
income tax, excise tax, PAYE, personal income tax, VAT, withholding tax,
travel tax, licensing, past taxes, and international tax. Also fetches
IRD-hosted PDF guides/tables.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DM.IRD-TaxLaws")

BASE_URL = "https://ird.gov.dm"

# All sub-pages to scrape for doctrine content
PAGES = [
    # Tax Laws
    ("/tax-laws/corporate-income-tax", "Corporate Income Tax"),
    ("/tax-laws/excise-tax", "Excise Tax"),
    ("/tax-laws/pay-as-you-earn-p-a-y-e", "Pay As You Earn (PAYE)"),
    ("/tax-laws/personal-income-tax", "Personal Income Tax"),
    ("/tax-laws/travel-tax", "Travel Tax"),
    ("/tax-laws/value-added-tax", "Value Added Tax (VAT)"),
    ("/tax-laws/withholding-tax", "Withholding Tax"),
    # Licenses
    ("/tax-laws/licenses/drivers-license", "Driver's License"),
    ("/tax-laws/licenses/firearms-license", "Firearms License"),
    ("/tax-laws/licenses/huckster-license", "Huckster License"),
    ("/tax-laws/licenses/liquor-license", "Liquor License"),
    ("/tax-laws/licenses/motor-vehicle-license", "Motor Vehicle License"),
    ("/tax-laws/licenses/professional-license", "Professional License"),
    ("/tax-laws/licenses/store-parlour-license", "Store Parlour License"),
    # Past Taxes
    ("/tax-laws/past-taxes/entertainment-act", "Entertainment Act (Past Tax)"),
    ("/tax-laws/past-taxes/hotel-occupancy-tax", "Hotel Occupancy Tax (Past Tax)"),
    ("/tax-laws/past-taxes/sales-tax", "Sales Tax (Past Tax)"),
    ("/tax-laws/past-taxes/stabilization-levy", "Stabilization Levy (Past Tax)"),
    # International Tax
    ("/international-tax/automatic-exchange-of-information-aeoi", "AEOI - Automatic Exchange of Information"),
    ("/international-tax/country-by-country-reporting", "Country by Country Reporting"),
    ("/international-tax/fatca", "FATCA"),
    ("/international-tax/mutual-agreement-procedure-map", "Mutual Agreement Procedure (MAP) Guidelines"),
]


def _strip_tags(html_content: str) -> str:
    """Remove HTML tags and clean whitespace."""
    import html as html_mod
    content = html_content
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<br\s*/?\s*>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'</?(p|div|h[1-6]|li|tr|td|th|ul|ol|table|blockquote|section|header|footer|nav|aside)[^>]*>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<[^>]+>', '', content)
    content = html_mod.unescape(content)
    lines = [line.strip() for line in content.split('\n')]
    lines = [line for line in lines if line]
    content = '\n'.join(lines)
    content = re.sub(r'\n{3,}', '\n\n', content)
    return content.strip()


def _clean_html(html: str) -> str:
    """Extract main content from HTML, targeting the article-details div."""
    # Joomla article pages use div.article-details
    m = re.search(
        r'<div[^>]*class="article-details[^"]*"[^>]*>(.*?)(?=<div[^>]*class="sp-column)',
        html, re.DOTALL | re.IGNORECASE
    )
    if m:
        return _strip_tags(m.group(1))

    # Fallback: try <article> tag
    m = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL | re.IGNORECASE)
    if m:
        return _strip_tags(m.group(1))

    # Last resort: col-lg-9 main column (but strip footer/contact)
    m = re.search(
        r'<div[^>]*class="col-lg-9[^"]*"[^>]*>(.*?)(?=<div[^>]*class="(?:col-sm-col|container\b))',
        html, re.DOTALL | re.IGNORECASE
    )
    if m:
        return _strip_tags(m.group(1))

    return _strip_tags(html)


def _extract_category_links(html: str) -> List[Dict[str, str]]:
    """Extract article links from a Joomla category listing page."""
    links = []
    if 'category-list' not in html:
        return links
    m = re.search(r'<div[^>]*class="[^"]*category-list[^"]*"[^>]*>(.*?)(?=<div[^>]*class="(?:sp-column|container\b))', html, re.DOTALL | re.IGNORECASE)
    if not m:
        return links
    for link_m in re.finditer(r'<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>', m.group(1), re.DOTALL | re.IGNORECASE):
        href = link_m.group(1).strip()
        title = re.sub(r'<[^>]+>', '', link_m.group(2)).strip()
        if href and title and not href.startswith('#') and not href.endswith(('.pdf', '.PDF')):
            links.append({"path": href, "title": title})
    return links


class DominicaIRDScraper(BaseScraper):
    """Scraper for DM/IRD-TaxLaws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/pdf,*/*",
            })
        return self.session

    def _fetch_page(self, path: str) -> str:
        """Fetch an IRD page."""
        self.rate_limiter.wait()
        sess = self._get_session()
        url = BASE_URL + path
        resp = sess.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _extract_ird_pdfs(self, html: str) -> List[Dict[str, str]]:
        """Find PDFs hosted on ird.gov.dm (not dominica.gov.dm)."""
        pdfs = []
        seen = set()
        pattern = r'<a\s+[^>]*href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>'
        for m in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
            href = m.group(1).strip()
            title = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            # Skip PDFs on dominica.gov.dm (already in DM/Legislation)
            if 'dominica.gov.dm' in href:
                continue
            url = href if href.startswith('http') else urljoin(BASE_URL, href)
            if url in seen:
                continue
            seen.add(url)
            if title:
                pdfs.append({"url": url, "title": title})
        return pdfs

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title", "Unknown")
        slug = raw.get("slug", "")
        url = raw.get("url", "")
        doc_type = raw.get("_doc_type", "doctrine")

        doc_id = hashlib.md5(slug.encode() if slug else url.encode()).hexdigest()[:12]

        return {
            "_id": f"DM/IRD-TaxLaws/{doc_id}",
            "_source": "DM/IRD-TaxLaws",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": url,
            "slug": slug,
            "category": raw.get("category", ""),
        }

    def _categorize(self, path: str) -> str:
        if '/licenses/' in path:
            return "Licenses"
        if '/past-taxes/' in path:
            return "Past Taxes"
        if '/international-tax/' in path:
            return "International Tax"
        return "Tax Laws"

    def _process_page(self, path, title, html, pdf_records):
        """Process an HTML page and collect PDF links. Returns a record or None."""
        text = _clean_html(html)
        ird_pdfs = self._extract_ird_pdfs(html)
        pdf_records.extend(ird_pdfs)

        if not text or len(text) < 30:
            return None

        return {
            "title": f"IRD Guide: {title}",
            "slug": path,
            "url": BASE_URL + path if not path.startswith('http') else path,
            "text": text,
            "date": "",
            "category": self._categorize(path),
            "_doc_type": "doctrine",
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        limit = 15 if sample else None
        count = 0
        pdf_records = []
        seen_paths = set()

        for path, title in PAGES:
            if limit and count >= limit:
                break
            if path in seen_paths:
                continue
            seen_paths.add(path)

            logger.info(f"Fetching: {title} ({path})")
            try:
                html = self._fetch_page(path)
            except Exception as e:
                logger.warning(f"Failed to fetch {path}: {e}")
                continue

            # Check if this is a category listing page
            cat_links = _extract_category_links(html)
            if cat_links:
                logger.info(f"  Category page with {len(cat_links)} sub-articles")
                # Also collect PDFs from the category page itself
                pdf_records.extend(self._extract_ird_pdfs(html))
                for link in cat_links:
                    if limit and count >= limit:
                        break
                    sub_path = link["path"]
                    if sub_path in seen_paths:
                        continue
                    seen_paths.add(sub_path)
                    logger.info(f"  Following sub-article: {link['title']}")
                    try:
                        sub_html = self._fetch_page(sub_path)
                    except Exception as e:
                        logger.warning(f"  Failed to fetch {sub_path}: {e}")
                        continue
                    rec = self._process_page(sub_path, link["title"], sub_html, pdf_records)
                    if rec:
                        yield rec
                        count += 1
                        logger.info(f"  [{count}] {link['title']} ({len(rec['text'])} chars)")
                continue

            rec = self._process_page(path, title, html, pdf_records)
            if rec:
                yield rec
                count += 1
                logger.info(f"  [{count}] {title} ({len(rec['text'])} chars)")
            else:
                logger.warning(f"Skipping {title} - insufficient content")

        # Fetch IRD-hosted PDFs
        seen_urls = set()
        for pdf_entry in pdf_records:
            if limit and count >= limit:
                break
            url = pdf_entry["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)

            logger.info(f"Fetching PDF: {pdf_entry['title']}")
            self.rate_limiter.wait()
            sess = self._get_session()
            try:
                resp = sess.get(url, timeout=60)
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning(f"Failed to download PDF {url}: {e}")
                continue

            if len(pdf_bytes) < 100:
                continue

            text = extract_pdf_markdown(
                source="DM/IRD-TaxLaws",
                source_id=hashlib.md5(url.encode()).hexdigest()[:12],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text) < 50:
                logger.warning(f"Skipping PDF {pdf_entry['title']} - no text")
                continue

            yield {
                "title": pdf_entry["title"],
                "slug": url.replace(BASE_URL, ""),
                "url": url,
                "text": text,
                "date": "",
                "category": "Guide/Document",
                "_doc_type": "doctrine",
            }
            count += 1
            logger.info(f"  [{count}] PDF: {pdf_entry['title']} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = DominicaIRDScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing IRD website access...")
        html = scraper._fetch_page("/tax-laws/corporate-income-tax")
        text = _clean_html(html)
        print(f"Corporate Income Tax page: {len(text)} chars")
        print(f"Preview: {text[:200]}")
        pdfs = scraper._extract_ird_pdfs(html)
        print(f"IRD-hosted PDFs: {len(pdfs)}")
        print("Test PASSED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
