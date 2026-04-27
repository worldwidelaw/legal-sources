#!/usr/bin/env python3
"""
BM/OTC-TaxGuidance -- Bermuda Office of Tax Commissioner Guidance

Fetches tax guidance from Bermuda's gov.bm website. Two types of content:
  1. HTML guidance pages (payroll tax, land tax, stamp duty, etc.)
  2. PDF guidance notes linked from those pages

Strategy:
  - Scrape predefined list of OTC tax guidance pages
  - Extract main body text from HTML (strip navigation/headers/footers)
  - Identify substantive PDF guidance notes (not forms/calculators)
  - Download and extract text from PDFs

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
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
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BM.OTC-TaxGuidance")

BASE_URL = "https://www.gov.bm"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# Tax guidance pages to scrape (slug -> category)
GUIDANCE_PAGES = [
    ("payroll-tax", "Payroll Tax"),
    ("calculating-payroll-tax-2026", "Payroll Tax"),
    ("calculating-payroll-tax-2025", "Payroll Tax"),
    ("progressive-payroll-tax-2018-19-FAQs", "Payroll Tax"),
    ("land-tax", "Land Tax"),
    ("attention-land-property-owners", "Land Tax"),
    ("financial-services-tax-", "Financial Services Tax"),
    ("corporate-services-tax", "Corporate Services Tax"),
    ("stamp-duty-tax-legal-transactions", "Stamp Duty"),
    ("hotel-occupancy-tax", "Hotel Occupancy Tax"),
    ("betting-duty", "Betting Duty"),
    ("passenger-departure-tax", "Passenger Departure Tax"),
    ("foreign-currency-tax", "Foreign Currency Purchase Tax"),
    ("timesharing-taxes", "Timesharing Taxes"),
    ("tax-decisions-could-affect-your-business", "Tax Decisions"),
    ("registering-changing-or-closing-tax-account", "Registration"),
    ("types-taxes-bermuda", "Overview"),
    ("taxes-business-bermuda", "Business Taxes"),
    ("paying-taxes-and-bills", "Payment"),
]

# PDF guidance notes to fetch (path relative to gov.bm, doc_id, title, category)
GUIDANCE_PDFS = [
    (
        "/sites/default/files/Payroll-Tax-Guidance-Notes.pdf",
        "payroll-tax-guidance-notes",
        "Payroll Tax Guidance Notes",
        "Payroll Tax",
    ),
    (
        "/sites/default/files/2024-06/Taxi-Owner-Operator-Guidance-Notes-Payroll-Tax.pdf",
        "taxi-owner-operator-guidance-notes",
        "Taxi Owner/Operator Payroll Tax Guidance Notes",
        "Payroll Tax",
    ),
    (
        "/sites/default/files/PENSION_SCHEME_APPROVAL_FAQs.pdf",
        "pension-scheme-approval-faqs",
        "Pension Scheme Tax Approval FAQs",
        "Payroll Tax",
    ),
    (
        "/sites/default/files/Land%20Valuation%20and%20Tax%20Act%201967_0.pdf",
        "land-valuation-tax-act-1967",
        "Land Valuation and Tax Act 1967",
        "Land Tax",
    ),
    (
        "/sites/default/files/Land%20Valuation%20and%20Tax%20Amendment%20Act%202015%20Synopsis_1.pdf",
        "land-tax-amendment-2015-synopsis",
        "Land Valuation and Tax Amendment Act 2015 Synopsis",
        "Land Tax",
    ),
    (
        "/sites/default/files/Important%20information%20on%20your%20New%20Annual%20Rental%20Value_1.pdf",
        "arv-information-notice",
        "Important Information on Annual Rental Value (ARV)",
        "Land Tax",
    ),
    (
        "/sites/default/files/OTC-MINIMUM-WAGE-POLICY-STATEMENT.pdf",
        "otc-minimum-wage-policy",
        "OTC Minimum Wage Policy Statement",
        "Policy",
    ),
]


class BMOTCScraper(BaseScraper):
    SOURCE_ID = "BM/OTC-TaxGuidance"

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)

    def _fetch_page(self, path: str, timeout: int = 30) -> Optional[str]:
        """Fetch an HTML page."""
        url = f"{BASE_URL}/{path}" if not path.startswith("http") else path
        url = url.replace(" ", "%20")
        req = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            resp = urlopen(req, timeout=timeout)
            return resp.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError) as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _extract_body_text(self, page_html: str) -> str:
        """Extract main body text from an HTML page, stripping tags."""
        # Try to isolate main content area
        # Drupal typically uses <main> or <article> or div.field--name-body
        body_match = re.search(
            r'<(?:main|article)[^>]*>(.*?)</(?:main|article)>',
            page_html, re.DOTALL
        )
        if body_match:
            content = body_match.group(1)
        else:
            # Fallback: look for content region
            body_match = re.search(
                r'class="[^"]*field--name-body[^"]*"[^>]*>(.*?)</div>\s*</div>',
                page_html, re.DOTALL
            )
            if body_match:
                content = body_match.group(1)
            else:
                # Last fallback: everything between content markers
                content = page_html

        # Remove script and style blocks
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL)
        # Remove nav, header, footer
        content = re.sub(r'<nav[^>]*>.*?</nav>', '', content, flags=re.DOTALL)
        content = re.sub(r'<header[^>]*>.*?</header>', '', content, flags=re.DOTALL)
        content = re.sub(r'<footer[^>]*>.*?</footer>', '', content, flags=re.DOTALL)

        # Convert common block elements to newlines
        content = re.sub(r'<(?:br|hr)\s*/?>', '\n', content)
        content = re.sub(r'<(?:p|div|li|h[1-6]|tr|blockquote)[^>]*>', '\n', content)
        content = re.sub(r'</(?:p|div|li|h[1-6]|tr|blockquote)>', '\n', content)

        # Strip all remaining HTML tags
        content = re.sub(r'<[^>]+>', '', content)

        # Decode HTML entities
        content = html.unescape(content)

        # Clean up whitespace
        lines = [line.strip() for line in content.split('\n')]
        lines = [line for line in lines if line]
        text = '\n'.join(lines)

        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()

    def _extract_title(self, page_html: str, slug: str) -> str:
        """Extract page title."""
        # Try h1
        h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', page_html)
        if h1_match:
            return html.unescape(h1_match.group(1).strip())

        # Try <title>
        title_match = re.search(r'<title>([^<]+)</title>', page_html)
        if title_match:
            title = html.unescape(title_match.group(1).strip())
            # Remove site suffix
            title = re.sub(r'\s*[|–-]\s*Government of Bermuda.*$', '', title)
            if title:
                return title

        # Fallback: humanize slug
        return slug.replace("-", " ").replace("_", " ").title()

    def _find_pdf_links(self, page_html: str) -> List[Dict[str, str]]:
        """Find PDF links on a page (for discovery, not used in main flow)."""
        pattern = r'href="(/sites/default/files/[^"]+\.pdf)"'
        links = re.findall(pattern, page_html)
        results = []
        for link in links:
            filename = link.split("/")[-1].replace("%20", " ").replace(".pdf", "")
            results.append({"path": link, "filename": filename})
        return results

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all guidance pages and PDF documents."""
        # 1. Fetch HTML guidance pages
        for slug, category in GUIDANCE_PAGES:
            time.sleep(1.5)
            page = self._fetch_page(slug)
            if not page:
                logger.warning(f"Skipping page {slug}: not accessible")
                continue

            title = self._extract_title(page, slug)
            text = self._extract_body_text(page)

            if len(text) < 200:
                logger.warning(f"Skipping {slug}: insufficient text ({len(text)} chars)")
                continue

            yield self.normalize({
                "doc_id": f"page_{slug}",
                "title": title,
                "text": text,
                "url": f"{BASE_URL}/{slug}",
                "category": category,
                "doc_type": "html_page",
            })

        # 2. Fetch PDF guidance notes
        for pdf_path, doc_id, title, category in GUIDANCE_PDFS:
            time.sleep(1.5)
            pdf_url = f"{BASE_URL}{pdf_path}"

            text = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=doc_id,
                pdf_url=pdf_url,
                table="doctrine",
            )
            if not text or len(text.strip()) < 100:
                logger.warning(f"Skipping PDF {doc_id}: insufficient text ({len(text) if text else 0} chars)")
                continue

            yield self.normalize({
                "doc_id": f"pdf_{doc_id}",
                "title": title,
                "text": text,
                "url": pdf_url,
                "category": category,
                "doc_type": "pdf_guidance",
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
            "date": None,
            "url": raw["url"],
            "category": raw.get("category", ""),
        }


# ─── CLI Entry Point ─────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BM/OTC-TaxGuidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = BMOTCScraper()

    if args.command == "test":
        page = scraper._fetch_page("payroll-tax")
        if page:
            text = scraper._extract_body_text(page)
            print(f"OK: Payroll tax page has {len(text)} chars of content")
        else:
            print("FAIL: Cannot access gov.bm")
            sys.exit(1)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    for record in scraper.fetch_all():
        count += 1
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
