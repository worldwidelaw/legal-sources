#!/usr/bin/env python3
"""
GD/IRD-TaxGuidance -- Grenada Inland Revenue Division Tax Guidance

Fetches official tax guidance pages from the Grenada IRD website (ird.gd).
Covers all 11 tax types plus international tax/EOI guidance pages.
Each page is a doctrine record with the full guidance text.

Endpoint:
  - Tax pages: https://ird.gd/taxes/{slug}
  - International tax: https://ird.gd/international-tax-eoi/{slug}

Data:
  - ~15 guidance pages (doctrine)
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html as html_mod
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GD.IRD-TaxGuidance")

BASE_URL = "https://ird.gd"

# All pages to scrape: (section, slug, title)
PAGES = [
    ("taxes", "income-tax", "Income Tax"),
    ("taxes", "excise-tax", "Excise Tax"),
    ("taxes", "gaming-tax", "Gaming Tax"),
    ("taxes", "property-tax", "Property Tax"),
    ("taxes", "withholding-tax", "Withholding Tax"),
    ("taxes", "value-added-tax", "Value Added Tax"),
    ("taxes", "annual-stamp-tax", "Annual Stamp Tax"),
    ("taxes", "property-transfer-tax", "Property Transfer Tax"),
    ("taxes", "motor-vehicle-licence", "Motor Vehicle Licence"),
    ("taxes", "refreshment-house-licence", "Refreshment House Licence"),
    ("taxes", "other-licences", "Other Licences"),
    ("international-tax-eoi", "overview", "International Tax/EOI Overview"),
    ("international-tax-eoi", "fatca-foreign-account-tax-compliance-act", "FATCA - Foreign Account Tax Compliance Act"),
    ("international-tax-eoi", "crs-common-reporting-standard", "CRS - Common Reporting Standard"),
    ("international-tax-eoi", "tax-treaty-agreement", "Tax Treaty Agreement"),
]

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t]+")
MULTI_NL_RE = re.compile(r"\n{3,}")


def extract_content(html_text: str) -> str:
    """Extract main content from IRD page HTML."""
    # Primary: find sp-page-builder content
    match = re.search(
        r'id="sp-page-builder"[^>]*>(.*?)(?:</div>\s*</div>\s*</div>\s*</section>)',
        html_text,
        re.DOTALL,
    )
    if not match:
        # Fallback: find sppb-section content
        match = re.search(
            r'class="sppb-section[^"]*"[^>]*>(.*?)(?:<footer|<div[^>]*class="sp-scroll-up)',
            html_text,
            re.DOTALL,
        )

    if not match:
        return ""

    content = match.group(1)

    # Remove script and style tags with content
    content = re.sub(r"<script[^>]*>.*?</script>", "", content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r"<style[^>]*>.*?</style>", "", content, flags=re.DOTALL | re.IGNORECASE)

    # Replace block tags with newlines for readability
    content = re.sub(r"<(?:br|/p|/div|/li|/tr|/h[1-6])[^>]*>", "\n", content, flags=re.IGNORECASE)
    content = re.sub(r"<(?:li)[^>]*>", "\n- ", content, flags=re.IGNORECASE)

    # Strip remaining tags
    content = TAG_RE.sub(" ", content)
    content = html_mod.unescape(content)

    # Normalize whitespace
    content = WS_RE.sub(" ", content)
    content = MULTI_NL_RE.sub("\n\n", content)

    # Strip lines and remove empty lines at edges
    lines = [line.strip() for line in content.split("\n")]
    content = "\n".join(lines).strip()

    return content


class GDIRDTaxGuidanceScraper(BaseScraper):
    """Scraper for GD/IRD-TaxGuidance -- Grenada IRD Tax Guidance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36 "
                          "LegalDataHunter/1.0",
        })

    def _get(self, url: str) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        slug = raw.get("slug", "")
        return {
            "_id": f"GD/IRD-TaxGuidance/{slug}",
            "_source": "GD/IRD-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("url", ""),
            "slug": slug,
            "section": raw.get("section", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        pages = PAGES if not sample else PAGES[:15]
        count = 0

        for section, slug, title in pages:
            url = f"{BASE_URL}/{section}/{slug}"
            logger.info(f"  [{count+1}] Fetching: {title} ({url})")

            try:
                resp = self._get(url)
            except Exception as e:
                logger.warning(f"    Failed to fetch {url}: {e}")
                continue

            text = extract_content(resp.text)

            if not text or len(text.strip()) < 100:
                logger.warning(f"    Skipping {slug} - insufficient text ({len(text.strip()) if text else 0} chars)")
                continue

            record = self.normalize({
                "title": f"Grenada IRD - {title}",
                "text": text,
                "url": url,
                "slug": slug,
                "section": section,
            })
            yield record
            count += 1
            logger.info(f"    OK ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No incremental update — static guidance pages."""
        logger.info("No incremental update support; use full refresh.")
        return
        yield


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = GDIRDTaxGuidanceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing connectivity to ird.gd...")
        resp = scraper._get(BASE_URL)
        print(f"Status: {resp.status_code}, Length: {len(resp.text)}")
        print("OK" if resp.status_code == 200 else "FAILED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
