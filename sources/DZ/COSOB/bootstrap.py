#!/usr/bin/env python3
"""
DZ/COSOB -- Algeria Securities & Exchange Commission (COSOB)

Fetches regulatory texts (laws, decrees, arrêtés, regulations, instructions,
AML/CFT texts, guidelines) from COSOB website.

Strategy:
  - Scrape 6 category sub-pages under /reglementations/
  - Each page has PDF links with descriptive anchor text
  - Download PDFs and extract full text using pdfplumber
  - ~115+ regulatory documents total

Endpoints:
  - Regulations: https://cosob.dz/reglementations/{category}/
  - PDFs:        https://cosob.dz/wp-content/uploads/{YYYY}/{MM}/{filename}.pdf

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
from typing import Generator, Optional, Dict, Any, List, Tuple
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
logger = logging.getLogger("legal-data-hunter.DZ.COSOB")

BASE_URL = "https://cosob.dz"
SOURCE_ID = "DZ/COSOB"

# Category pages to scrape: (url_path, category_label, data_type)
CATEGORIES = [
    ("reglementations/lois-ordonnaces/", "Lois et Ordonnances", "legislation"),
    ("reglementations/decrets/", "Décrets", "legislation"),
    ("reglementations/arretes/", "Arrêtés", "legislation"),
    ("reglementations/reglements-et-instructions/", "Règlements et Instructions", "doctrine"),
    ("reglementations/lignes-directrices/", "Lignes Directrices", "doctrine"),
    ("reglementations/textes-anti-blanchiment/", "Textes Anti-Blanchiment", "legislation"),
]


def _strip_html(text: str) -> str:
    """Remove HTML tags, decode entities, and fix spacing artifacts."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    # Replace non-breaking spaces with regular spaces
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    # Fix single-letter-then-space at start (e.g., "D écret" → "Décret")
    text = re.sub(r"^(\S) (\S)", r"\1\2", text)
    return text


def _title_from_url(url: str) -> str:
    """Extract a fallback title from a PDF URL filename."""
    filename = unquote(url.split("/")[-1])
    name = filename.rsplit(".", 1)[0]
    name = name.replace("-", " ").replace("_", " ")
    return name


class COSOBScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        super().__init__()
        self.http = HttpClient()

    def _get(self, url: str) -> Optional[str]:
        """Fetch a URL, return text or None."""
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
        return None

    def _get_bytes(self, url: str) -> Optional[bytes]:
        """Fetch a URL, return bytes or None."""
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.content
        except Exception as e:
            logger.warning(f"Failed to fetch bytes {url}: {e}")
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        try:
            import pdfplumber
            pages_text = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    # Release per-page layout + cached textmap to cap peak RSS
                    # on large PDFs (prevents OOM exit 137 on the fleet).
                    page.flush_cache()
                    try:
                        page.get_textmap.cache_clear()
                    except AttributeError:
                        pass
            return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _extract_pdfs_from_page(self, html: str, category: str, data_type: str) -> List[Dict[str, Any]]:
        """Parse a category page and extract PDF document metadata.

        The COSOB site uses Visual Composer. Each document appears as a
        vce-text-block div (containing the title/description) followed by
        a vce-button anchor linking to the PDF. We interleave both element
        types by position and pair the last seen text-block with the next
        PDF link.
        """
        docs = []
        seen_urls = set()

        # Collect text-blocks and PDF hrefs with their positions
        elements = []

        for m in re.finditer(
            r'<div[^>]*class="[^"]*vce-text-block[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL
        ):
            clean = _strip_html(m.group(1))
            if clean and len(clean) > 10:
                elements.append(("text", clean, m.start()))

        for m in re.finditer(r'href="([^"]*\.pdf)"', html):
            elements.append(("pdf", m.group(1), m.start()))

        elements.sort(key=lambda x: x[2])

        # Walk elements: last text before a PDF becomes that PDF's title
        last_text = ""
        for typ, content, _pos in elements:
            if typ == "text":
                last_text = content
            elif typ == "pdf":
                pdf_url = content
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                if pdf_url.startswith("/"):
                    pdf_url = f"{BASE_URL}{pdf_url}"

                title = last_text if last_text else _title_from_url(pdf_url)
                doc_id = hashlib.sha256(pdf_url.encode()).hexdigest()[:16]

                docs.append({
                    "doc_id": doc_id,
                    "pdf_url": pdf_url,
                    "title": title,
                    "category": category,
                    "data_type": data_type,
                })
                last_text = ""  # consume the title

        return docs

    def _fetch_document(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download a PDF and extract full text, return normalized record."""
        logger.info(f"Fetching PDF: {doc['title'][:80]}...")

        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            logger.warning(f"Could not download PDF: {doc['pdf_url']}")
            return None

        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {doc['doc_id']}: {len(text)} chars")
            return None

        return {
            "_id": doc["doc_id"],
            "_source": SOURCE_ID,
            "_type": doc["data_type"],
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": doc["title"],
            "text": text,
            "date": None,
            "url": doc["pdf_url"],
            "category": doc["category"],
            "language": "fr",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all COSOB regulatory documents."""
        all_docs = []
        seen_urls = set()

        for cat_path, cat_label, data_type in CATEGORIES:
            url = f"{BASE_URL}/{cat_path}"
            logger.info(f"Listing category: {cat_label} ({url})")

            html = self._get(url)
            if not html:
                logger.warning(f"Could not fetch listing: {url}")
                continue

            docs = self._extract_pdfs_from_page(html, cat_label, data_type)

            # Deduplicate across categories (some PDFs appear on multiple pages)
            new_docs = []
            for d in docs:
                if d["pdf_url"] not in seen_urls:
                    seen_urls.add(d["pdf_url"])
                    new_docs.append(d)

            logger.info(f"  Found {len(new_docs)} unique PDFs in {cat_label}")
            all_docs.extend(new_docs)

        logger.info(f"Total unique documents found: {len(all_docs)}")

        if sample:
            all_docs = all_docs[:15]
            logger.info(f"Sample mode: processing {len(all_docs)} documents")

        count = 0
        for doc in all_docs:
            record = self._fetch_document(doc)
            if record:
                count += 1
                yield record
            time.sleep(1)

        logger.info(f"Fetched {count}/{len(all_docs)} documents with full text")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents updated since a given date."""
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Already normalized during fetch."""
        return raw


def bootstrap(sample: bool = False):
    """Run bootstrap: fetch all documents and save to sample/."""
    scraper = COSOBScraper()
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
    """Quick connectivity test."""
    scraper = COSOBScraper()
    html = scraper._get(f"{BASE_URL}/reglementations/reglements-et-instructions/")
    if html and ".pdf" in html:
        logger.info("PASS: COSOB site accessible, PDF links found")
        return True
    else:
        logger.error("FAIL: Could not access COSOB regulation pages")
        return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse
    parser = argparse.ArgumentParser(description="DZ/COSOB bootstrap")
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
