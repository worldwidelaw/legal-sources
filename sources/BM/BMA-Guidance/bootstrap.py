#!/usr/bin/env python3
"""
BM/BMA-Guidance -- Bermuda Monetary Authority Regulatory Guidance

Fetches regulatory guidance notes, codes of conduct, and policy documents
from the BMA document centre across multiple categories (banking, insurance,
investment, AML/ATF, digital assets, etc.).

Documents are PDFs hosted on cdn.bma.bm. Full text extracted via
common.pdf_extract.

Endpoint:
  - Category pages: https://www.bma.bm/document-centre/policy-and-guidance-{category}
  - PDFs: https://cdn.bma.bm/documents/{timestamp}-{title}.pdf

Data:
  - ~150+ guidance documents across 15 categories
  - Full text extracted from PDFs
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
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BM.BMA-Guidance")

BASE_URL = "https://www.bma.bm"

# Category pages to scrape - two URL patterns observed
CATEGORY_PAGES = [
    ("banking", f"{BASE_URL}/document-centre/policy-and-guidance-banking"),
    ("insurance", f"{BASE_URL}/document-centre/policy-and-guidance-insurance"),
    ("investment", f"{BASE_URL}/document-centre/policy-and-guidance-investment-business"),
    ("credit-unions", f"{BASE_URL}/document-centre/policy-and-guidance-credit-unions"),
    ("digital-assets", f"{BASE_URL}/document-centre/policy-and-guidance-digital-asset-business"),
    ("money-service", f"{BASE_URL}/document-centre/policy-and-guidance-money-service-business"),
    ("fund-admin", f"{BASE_URL}/fund-administration-policy-and-guidance"),
    ("trust", f"{BASE_URL}/document-centre/policy-and-guidance-trust-business"),
    ("enforcement", f"{BASE_URL}/document-centre/policy-and-guidance-enforcement"),
    ("conduct", f"{BASE_URL}/document-centre/policy-and-guidance-conduct-of-business"),
    ("outsourcing", f"{BASE_URL}/document-centre/policy-and-guidance-outsourcing"),
    ("aml-atf", f"{BASE_URL}/document-centre/policy-and-guidance-aml-atf"),
    ("investment-funds", f"{BASE_URL}/document-centre/policy-and-guidance-investment-funds"),
    ("corporate-service", f"{BASE_URL}/document-centre/policy-and-guidance-corporate-service-provider"),
]

# Also try alternative URL patterns
ALT_CATEGORY_PAGES = [
    ("general", f"{BASE_URL}/documents-centre/documents-policy-and-guidance"),
    ("conduct-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-conduct-of-business"),
    ("banking-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-banking"),
    ("insurance-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-insurance"),
    ("investment-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-investment-business"),
    ("credit-unions-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-credit-unions"),
    ("investment-funds-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-investment-funds"),
    ("money-service-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-money-service-business"),
    ("trust-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/trust-business"),
    ("digital-assets-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/fintech/documents-digital-asset-business"),
    ("enforcement-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-enforcement"),
    ("outsourcing-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/documents-outsourcing"),
    ("aml-atf-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/aml-atf"),
    ("corporate-service-alt", f"{BASE_URL}/documents-centre/documents-policy-and-guidance/corporate-service-provider"),
]

# Match PDF links from cdn.bma.bm or bma.bm/viewPDF
PDF_RE = re.compile(
    r'href=["\']'
    r'(https?://(?:cdn\.bma\.bm|www\.bma\.bm/viewPDF)/documents/[^"\']+\.pdf)'
    r'["\']',
    re.IGNORECASE,
)
# Also match title text near the link
TITLE_RE = re.compile(r"<[^>]*class=['\"][^'\"]*document-title[^'\"]*['\"][^>]*>(.*?)</", re.DOTALL | re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(s: str) -> str:
    text = TAG_RE.sub(" ", s)
    text = html_mod.unescape(text)
    return WS_RE.sub(" ", text).strip()


def doc_id_from_url(pdf_url: str) -> str:
    """Derive a stable doc ID from the PDF URL."""
    fname = unquote(pdf_url.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").replace(".PDF", "")
    # Remove timestamp prefix (YYYY-MM-DD-HH-MM-SS-)
    fname = re.sub(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-", "", fname)
    return fname


def title_from_url(pdf_url: str) -> str:
    """Extract a readable title from the PDF URL."""
    doc_id = doc_id_from_url(pdf_url)
    # Replace hyphens with spaces, clean up
    title = doc_id.replace("-", " ").replace("  ", " ").strip()
    return title


class BMAGuidanceScraper(BaseScraper):
    """Scraper for BM/BMA-Guidance -- Bermuda Monetary Authority guidance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })

    def _get(self, url: str, **kwargs) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120, **kwargs)
        resp.raise_for_status()
        return resp

    def _list_documents(self) -> List[Tuple[str, str, str]]:
        """Scrape all (title, pdf_url, category) across all category pages."""
        results = []
        seen_urls = set()

        all_pages = CATEGORY_PAGES + ALT_CATEGORY_PAGES

        for category, page_url in all_pages:
            logger.info(f"Fetching category '{category}' from {page_url}")
            try:
                resp = self._get(page_url)
            except Exception as e:
                logger.warning(f"  Failed to fetch {category}: {e}")
                continue

            page_count = 0
            for match in PDF_RE.finditer(resp.text):
                pdf_url = html_mod.unescape(match.group(1))
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                title = title_from_url(pdf_url)
                # Strip -alt suffix from category name
                cat = category.replace("-alt", "")
                results.append((title, pdf_url, cat))
                page_count += 1

            logger.info(f"  Found {page_count} new documents in '{category}'")

        logger.info(f"Total unique documents found: {len(results)}")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        return {
            "_id": f"BM/BMA-Guidance/{doc_id}",
            "_source": "BM/BMA-Guidance",
            "_type": "doctrine",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("pdf_url", ""),
            "doc_id": doc_id,
            "category": raw.get("category", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        docs = self._list_documents()
        if sample:
            docs = docs[:25]  # extra in case some fail

        for title, pdf_url, category in docs:
            if limit and count >= limit:
                break

            doc_id = doc_id_from_url(pdf_url)
            logger.info(f"  [{count+1}] [{category}] {title[:60]}...")

            try:
                text = extract_pdf_markdown(
                    source="BM/BMA-Guidance",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="doctrine",
                )
            except Exception as e:
                logger.warning(f"    PDF extraction failed for {doc_id}: {e}")
                text = None

            if not text or len(text.strip()) < 50:
                logger.warning(f"    Skipping {doc_id} - no/short text")
                continue

            record = self.normalize({
                "title": title,
                "text": text,
                "pdf_url": pdf_url,
                "doc_id": doc_id,
                "category": category,
            })
            yield record
            count += 1
            logger.info(f"    OK ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No update mechanism — refresh category pages for new documents."""
        logger.info("No incremental update support; use full refresh.")
        return
        yield


if __name__ == "__main__":
    scraper = BMAGuidanceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        import requests
        try:
            resp = requests.get(
                CATEGORY_PAGES[0][1],
                headers={"User-Agent": "LegalDataHunter/1.0"},
                timeout=30,
            )
            print(f"Connection OK: {resp.status_code}")
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
