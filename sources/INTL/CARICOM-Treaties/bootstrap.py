#!/usr/bin/env python3
"""
INTL/CARICOM-Treaties -- CARICOM Treaty Collection

Fetches treaties from the Caribbean Community (CARICOM) Secretariat website.

Strategy:
  - Scrape the treaty listing page at /treaties/
  - Each treaty has a detail page with a "Download File" PDF button
  - Download PDFs and extract full text using pdfplumber
  - ~40 treaties total

Endpoints:
  - Listing: https://caricom.org/treaties/
  - Detail:  https://caricom.org/treaties/{slug}/
  - PDFs:    https://caricom.org/wp-content/uploads/{filename}.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.CARICOM-Treaties")

BASE_URL = "https://caricom.org"
SOURCE_ID = "INTL/CARICOM-Treaties"

# PDFs that appear in header/footer/navigation on every page — skip these
COMMON_PDFS = [
    "plenipotentiary-reps",
    "Data-Protection-and-Privacy",
    "Procurement-Plan",
    "Blank-Application",
    "Rotation-of-Chairmanship",
    "revised_treaty-text",
    "charter_of_civil",
]


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


class CARICOMTreatiesScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.http = HttpClient()

    def _get(self, url: str) -> Optional[str]:
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
        return None

    def _get_bytes(self, url: str) -> Optional[bytes]:
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.content
        except Exception as e:
            logger.warning(f"Failed to fetch bytes {url}: {e}")
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        try:
            import pdfplumber
            pages_text = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _list_treaties(self) -> List[Dict[str, Any]]:
        """Scrape the treaty listing page for treaty links and titles."""
        url = f"{BASE_URL}/treaties/"
        html = self._get(url)
        if not html:
            logger.error("Could not fetch treaty listing page")
            return []

        treaties = []
        seen_urls = set()

        # Find treaty links: <a href="https://caricom.org/treaties/{slug}/">Title</a>
        pattern = re.compile(
            r'<a[^>]*href="(https://caricom\.org/treaties/[^"]+/)"[^>]*>(.*?)</a>',
            re.DOTALL,
        )

        for m in pattern.finditer(html):
            treaty_url = m.group(1)
            title = _strip_html(m.group(2))

            # Skip the main listing page itself, and skip duplicates
            if treaty_url.rstrip("/") == f"{BASE_URL}/treaties":
                continue
            if treaty_url in seen_urls:
                continue
            if not title or len(title) < 5:
                continue
            seen_urls.add(treaty_url)

            slug = treaty_url.rstrip("/").split("/")[-1]
            doc_id = hashlib.sha256(slug.encode()).hexdigest()[:16]

            treaties.append({
                "doc_id": doc_id,
                "slug": slug,
                "treaty_url": treaty_url,
                "title": title,
            })

        return treaties

    def _find_treaty_pdf(self, html: str) -> Optional[str]:
        """Find the treaty-specific PDF on a detail page.

        The treaty PDF is in a link with class "more-link button".
        Fall back to any non-common PDF if that pattern isn't found.
        """
        # Primary: "more-link button" pattern
        m = re.search(
            r'class="more-link button"[^>]*href="([^"]*\.pdf)"', html
        )
        if m:
            return m.group(1)

        # Fallback: any PDF not in the common list
        all_pdfs = re.findall(r'href="([^"]*\.pdf)"', html)
        for pdf_url in all_pdfs:
            if not any(common in pdf_url for common in COMMON_PDFS):
                return pdf_url

        return None

    def _fetch_treaty(self, treaty: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a treaty's detail page, find and download PDF, extract text."""
        logger.info(f"Fetching treaty: {treaty['title'][:80]}...")

        html = self._get(treaty["treaty_url"])
        if not html:
            logger.warning(f"Could not fetch detail page: {treaty['treaty_url']}")
            return None

        pdf_url = self._find_treaty_pdf(html)
        if not pdf_url:
            logger.warning(f"No treaty PDF found on {treaty['treaty_url']}")
            return None

        if pdf_url.startswith("/"):
            pdf_url = f"{BASE_URL}{pdf_url}"

        pdf_bytes = self._get_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF: {pdf_url}")
            return None

        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(
                f"Insufficient text for {treaty['doc_id']}: {len(text)} chars"
            )
            return None

        return {
            "_id": treaty["doc_id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": treaty["title"],
            "text": text,
            "date": None,
            "url": treaty["treaty_url"],
            "pdf_url": pdf_url,
            "language": "en",
        }

    def fetch_all(
        self, sample: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        treaties = self._list_treaties()
        logger.info(f"Found {len(treaties)} treaties")

        if sample:
            treaties = treaties[:15]
            logger.info(f"Sample mode: processing {len(treaties)} treaties")

        count = 0
        for treaty in treaties:
            record = self._fetch_treaty(treaty)
            if record:
                count += 1
                yield record
            time.sleep(1)

        logger.info(f"Fetched {count}/{len(treaties)} treaties with full text")

    def fetch_updates(
        self, since: str
    ) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw


def bootstrap(sample: bool = False):
    scraper = CARICOMTreatiesScraper()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in scraper.fetch_all(sample=sample):
        count += 1
        fname = f"{record['_id']}.json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(
            f"[{count}] Saved {fname} — {record['title'][:60]}... "
            f"({len(record['text'])} chars)"
        )

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
    return count


def test():
    scraper = CARICOMTreatiesScraper()
    html = scraper._get(f"{BASE_URL}/treaties/")
    if html and "caricom.org/treaties/" in html:
        logger.info("PASS: CARICOM treaty listing accessible")
        return True
    else:
        logger.error("FAIL: Could not access CARICOM treaties page")
        return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="INTL/CARICOM-Treaties bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        success = test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        sys.exit(0 if count > 0 else 1)
    elif args.command == "update":
        count = bootstrap(sample=False)
        sys.exit(0 if count > 0 else 1)
